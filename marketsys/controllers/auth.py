from datetime import datetime, date

import hashlib

import bcrypt
import bottle
import logging
from bottle import post, abort,route,request

from marketsys.db import db
from marketsys.models import Operator, Session
from marketsys.utils import plain_forms, plain_query
from marketsys.validator import login_validator, reset_password_validator

app = bottle.default_app()


@route('/api/login',method =['POST','OPTIONS'])
def login():
    args = request.json
    cs = Operator.select().where(Operator.username == args['username'])
    if not cs.exists():
        abort(403, 'username password do not match')
    cs = cs.get()
    if (cs.password !=
            hashlib.md5(args['password'].lower().encode('utf-8')).hexdigest()):
        logging.info('%s %s login failed', cs.username, cs.id)
        abort(403, 'username password do not match')
    logging.info('%s %s login success', cs.username, cs.id)

    session = cs.logged_in(expire_days=7)
    cs.last_active_at = datetime.now()
    cs.save()

    return {
        'jwt': session.jwt_token(),
        'username':args['username'],
        'promise':[] if cs.promise== None else cs.promise.split(',')
    }


@route('/api/reset-password',method =['POST','OPTIONS'])
def reset_password(op):
    form = request.json
    password = form['old_password'].lower()
    new_password = form['new_password'].lower()

    password = bytes(password, 'utf-8')
    if hashlib.md5(password).hexdigest() == op.password:
        new_password = bytes(new_password, 'utf-8')
        op.password = hashlib.md5(new_password).hexdigest()
        op.save()
    else:
        logging.info('%s %s login failed', op.username, op.id)
        abort(403, 'password is not correct')

    sessions = Session.filter(
        Session.op == op.id,
        Session.expire_at > date.today(),
    )

    with db.atomic():
        for session in sessions:
            session.expire_at = date.today()
            session.save()
    return {"statu":200}

