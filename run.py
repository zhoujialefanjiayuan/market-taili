from bottle import run

from marketsys.app import init_app
from marketsys.models import *
from marketsys.db import db

app = application = init_app()

if __name__ == '__main__':
    run(app, host='0.0.0.0', debug=True, reloader=True, port= 1120)
