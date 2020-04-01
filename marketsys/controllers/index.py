import bottle
from bottle import get
from marketsys.models import *
from marketsys.db import db


app = bottle.default_app()


@get('/createtables')
def index():
    #db.create_tables([Session,])
    db.create_tables([DPD])
    return

