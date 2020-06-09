import datetime
import re


class InvalidRequestParameter(Exception):
    pass


class TableNameBadFormat(Exception):
    pass


def check_date(date):
    try:
        datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S.%fZ')
    except ValueError:
        return False
    return True


def date_checker_start_end(func):
    def wrapper(*args, **kwargs):
        if check_date(kwargs['start']) is False \
                or check_date(kwargs['end']) is False:
            raise ValueError
            return {}
        return func(**kwargs)
    wrapper.__name__ = func.__name__
    return wrapper


# Round a datetime to the closest  5 minute
def round_time(dt=None, round_to=60):
    if dt is None:
        dt = datetime.datetime.utcnow()
    seconds = (dt.replace(tzinfo=None) - dt.min).seconds
    rounding = (seconds + round_to / 2) // round_to * round_to
    return dt + (datetime.timedelta(
        0,
        rounding - seconds,
        -dt.microsecond) - datetime.timedelta(minutes=5))


def validate_request_params(kwargs, regex=r'[a-zA-Z0-9_,]'):
    recomp = re.compile(regex)
    for key, value in kwargs.items():
        if recomp.match(value):
            continue
        raise InvalidRequestParameter(f'Parameter {key}: {value} is invalid.')
    return kwargs


def request_params(args):
    now = round_time().strftime('%Y-%m-%d %H:%M:%S.%fZ')
    args = args.to_dict()
    validated = {
        'start': args.pop('start', now),
        'end': args.pop('end', now)
    }
    validated.update(
        validate_request_params(args)
    )
    return validated


def assert_url_params(func):
    def wrapper(*args, **kwargs):
        regex = re.compile(r'[a-zA-Z_]')
        if regex.match(kwargs['table']):
            return func(**kwargs)
        raise TableNameBadFormat(f'Table name {kwargs["table"]} unproperly formatted.')
    wrapper.__name__ = func.__name__
    return wrapper
