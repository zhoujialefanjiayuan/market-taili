import os
import bottle
import logging

import peewee
import sys

from marketsys import db
from marketsys.plugins import packing_plugin
from marketsys.plugins import UserPlugin


from marketsys.utils import env_detect
from marketsys.error import register_error_handler

os.chdir(os.path.dirname(__file__))
app = application = bottle.default_app()


def load_config():
    app.config.load_config('config/base.ini')

    cur_env = env_detect().lower()

    if os.path.exists('config/%s.ini' % cur_env):
        app.config.load_config('config/%s.ini' % cur_env)

    # load config map in k8s
    config_map_path = os.environ.get('APP_CONFIG_MAP_PATH')
    secret_path = os.environ.get('APP_SECRET_PATH')
    for path in (config_map_path, secret_path):
        if not (path and os.path.exists(path)):
            continue

        config_map = {}
        for entry in os.scandir(path):
            if entry.is_file() and not entry.name.startswith('.'):
                with open(entry.path, encoding='utf-8') as f:
                    config_map[entry.name] = f.read().strip()
        if config_map:
            app.config.update(**config_map)


def load_controllers():
    for c in os.listdir('controllers'):
        head, _ = os.path.splitext(c)
        if not head.startswith('_'):
            __import__('marketsys.controllers.' + head)


def set_logger():
    default_format = ('[%(asctime)s] [%(levelname)s] '
                      '[%(module)s: %(lineno)d] %(message)s')
    logging.basicConfig(
        stream=sys.stdout,
        level=logging.INFO,
        format=default_format,
        datefmt='%Y-%m-%d %H:%M:%S %z',
    )


def install_plugins():
    app.install(packing_plugin)
    app.install(UserPlugin('marketsys.models:Operator'))

def base_config():
    load_config()
    set_logger()
    db.init()


def init_app():
    base_config()
    load_controllers()
    install_plugins()

    register_error_handler()

    return app
