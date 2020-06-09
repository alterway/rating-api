import os
from base64 import b64decode

from flask import request

from kubernetes import client, config
from kubernetes.client.rest import ApiException

from rating.api.config import envvar


class InvalidToken(Exception):
    pass


def get_token_from_request(request):
    if request.form:
        return request.form.get('token')
    elif request.args:
        return request.args.to_dict().get('token')
    else:
        return request.get_json().get('token')


def require_admin(func):
    def wrapper(*args, **kwargs):
        token = get_token_from_request(request)
        admin_api_key = envvar('RATING_ADMIN_API_KEY')
        if token == admin_api_key:
            return func(**kwargs)
        raise InvalidToken('Internal token unrecognized')
    wrapper.__name__ = func.__name__
    return wrapper


def register_admin_key():
    config.load_incluster_config()
    api = client.CoreV1Api()
    namespace = envvar('RATING_NAMESPACE')
    secret_name = f'{namespace}-admin'
    try:
        secret_encoded_bytes = api.read_namespaced_secret(secret_name, namespace).data
    except ApiException as exc:
        raise exc
    rating_admin_api_key = list(secret_encoded_bytes.keys())[0]
    os.environ[rating_admin_api_key] = b64decode(
        secret_encoded_bytes[rating_admin_api_key]).decode('utf-8')
    return os.environ[rating_admin_api_key]
