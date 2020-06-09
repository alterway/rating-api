import bisect
import errno
import logging
import os
import shutil
import sys
import time
from datetime import datetime as dt

import yaml


def envvar(name):
    """Return the value of an environment variable, or die trying."""
    try:
        return os.environ[name]
    except KeyError:
        sys.exit(1)


class Config:
    SQLALCHEMY_DATABASE_URI = envvar('POSTGRES_DATABASE_URI')
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    JSON_ADD_STATUS = False
    JSON_DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S'


class ConfigurationMissing(Exception):
    pass


class Lockfile():

    def __init__(self, path):
        self.lock_path = '{}/.lock'.format(path)
        self.delay = 1
        self.timeout = 60

    def __enter__(self):
        while True:
            try:
                self.fd = os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_RDWR)
            except FileExistsError:
                try:
                    time.sleep(self.delay)
                    if time.time() - os.path.getmtime(self.lock_path) >= self.timeout:
                        os.unlink(self.lock_path)
                        logging.info('removed stale lock file')
                except FileNotFoundError:
                    logging.info('lock file does not exist')
            else:
                break

    def __exit__(self, *args):
        os.close(self.fd)
        os.unlink(self.lock_path)


def delete_configuration(timestamp):
    rating_rates_dir = envvar('RATING_RATES_DIR')

    with Lockfile(rating_rates_dir):
        try:
            shutil.rmtree('{}/{}'.format(rating_rates_dir, timestamp))
        except OSError as err:
            logging.error(
                f'An error happened while removing {timestamp} configuration directory.')
            if err.errno == errno.ENOENT:
                logging.error(
                    f'Configuration directory {timestamp} does not exist.')
            sys.exit(1)
        else:
            logging.info(f'removed {timestamp} configuration')
            return timestamp


def write_configuration(config, timestamp=0):
    rating_rates_dir = envvar('RATING_RATES_DIR')

    with Lockfile(rating_rates_dir):
        config_dir = f'{rating_rates_dir}/{timestamp}'
        if os.path.exists(config_dir):
            logging.error(
                f'Configuration for timestamp {timestamp} already exist, aborting.')
            sys.exit(1)
        os.makedirs(config_dir)
        for config_name, configuration in config.items():
            with open(f'{config_dir}/{config_name}.yaml', 'w+') as f:
                yaml.safe_dump({config_name: configuration}, f, default_flow_style=False)


def create_new_config(content):
    timestamp = dt.strptime(content.pop('timestamp'), '%Y-%m-%dT%H:%M:%SZ')
    write_configuration(content, timestamp=int(timestamp.timestamp()))
    return timestamp


def update_config(content):
    rating_rates_dir = envvar('RATING_RATES_DIR')
    ts = dt.strptime(content.pop('timestamp'), '%Y-%m-%dT%H:%M:%SZ')
    timestamp = int(ts.timestamp())
    with Lockfile(rating_rates_dir):
        config_dir = f'{rating_rates_dir}/{timestamp}'
        if not os.path.exists(config_dir):
            raise ConfigurationMissing
        for config_name, configuration in content.items():
            with open(f'{config_dir}/{config_name}.yaml', 'w+') as f:
                yaml.safe_dump({config_name: configuration}, f, default_flow_style=False)
    return timestamp


def retrieve_directories(path=envvar('RATING_RATES_DIR'), tenant_id=None):
    dir_list = os.listdir(path)
    if '.lock' in dir_list:
        dir_list.remove('.lock')

    if 'lost+found' in dir_list:
        dir_list.remove('lost+found')
    return sorted(dir_list, key=float)


def retrieve_config_as_dict(timestamp, tenant_id=None):
    rating_rates_dir = envvar('RATING_RATES_DIR')
    config = {}
    with Lockfile(f'{rating_rates_dir}/{timestamp}'):
        for file in ['metrics.yaml', 'rules.yaml']:
            with open(f'{rating_rates_dir}/{timestamp}/{file}', 'r') as f:
                config_type = os.path.splitext(file)[0]
                config[config_type] = yaml.safe_load(f)
    return config


def retrieve_configurations(tenant_id):
    rating_rates_dir = envvar('RATING_RATES_DIR')
    configurations = []

    with Lockfile(rating_rates_dir):
        for timestamp in retrieve_directories(rating_rates_dir):
            config_dict = retrieve_config_as_dict(timestamp)
            config_dict['valid_from'] = timestamp
            configurations.append(config_dict)
    for idx in range(len(configurations) - 1):
        configurations[idx]['valid_to'] = configurations[idx + 1]['valid_from']
    configurations[-1]['valid_to'] = dt(2100, 1, 1, 1, 1).strftime('%s')
    return configurations


def get_closest_configs_bisect(timestamp, timestamps):
    return bisect.bisect_right(timestamps, timestamp) - 1


def format_labels_prometheus(labels):
    if not labels:
        return ''
    string_labels = '{'
    n_labels = len(labels)
    for key, value in labels.items():
        n_labels -= 1
        string_labels += f'{key}=\"{value}\"'
        if n_labels > 0:
            string_labels += ', '
    string_labels += '}'
    return string_labels


def generate_metrics_from_rules(rules):
    for ruleset in rules['rules']:
        labels = format_labels_prometheus(
            ruleset.get('labelSet')
        )
        for rule in ruleset.get('ruleset'):
            metric, price = rule['metric'], rule['price']
            yield f'{metric}{labels} {price}'


def generate_rules_export():
    timestamp = int(time.time())
    closest_config = retrieve_closest_config(timestamp)
    return generate_metrics_from_rules(closest_config['rules'])


def retrieve_closest_config(timestamp):
    timestamp_tuple = tuple(int(ts) for ts in retrieve_directories())
    closest = get_closest_configs_bisect(timestamp, timestamp_tuple)
    return retrieve_config_as_dict(timestamp_tuple[closest])
