import logging

import json
import jwt
import inspect
from datetime import datetime, timedelta

import bottle
from bottle import request, abort

from marketsys.models import Session, Operator

app = bottle.default_app()


def gen_jwt_token(user_id, exp=7):
    secret = app.config['user.secret']
    payload = {
        'user_id': user_id,
        'exp': datetime.now() + timedelta(days=exp)
    }

    return 'Bearer ' + jwt.encode(payload, secret).decode('utf-8')


def get_jwt_user():
    secret = app.config['op.secret']
    algorithms = app.config['op.algorithms'].strip(',').split(',')
    try:
        # Authorization: Bearer <token>
        token = request.headers['Authorization'][7:]
        print(token)
        return jwt.decode(token, secret, algorithms=algorithms)
    except Exception as e:
        print(e)
        return None


class UserPlugin(object):
    def __init__(self, cls, keyword='op'):
        self.cls = bottle.load(cls)
        self.keyword = keyword

    def apply(self, callback, route):
        _callback = route['callback']
        # Test if the original callback accepts a 'admin' keyword.
        # Ignore it if it does not need a database handle.
        argspec = inspect.signature(_callback)
        if self.keyword not in argspec.parameters:
            return callback

        def wrapper(*args, **kwargs):
            jwt_user = get_jwt_user()
            if jwt_user is None:
                abort(401, 'Invalid user')

            session = (Session.select(Session, Operator)
                       .join(Operator)
                       .where(Session.id == jwt_user.get('session_id')))
            if not session:
                abort(401, 'Invalid user')
            session = session.get()

            if datetime.now() > session.expire_at:
                logging.info('admin %s expire', session.op_id)
                abort(401, 'Account Expired')


            # Add the connection handle as a keyword argument.
            kwargs[self.keyword] = session.op

            return callback(*args, **kwargs)

        # Replace the route callback with the wrapped one.
        return wrapper

#
# class UserRolePlugin(object):
#     def __init__(self, role, cls, keyword='op'):
#         if not role:
#             abort(422, 'Permission den')
#
#         self.role = role
#         self.cls = bottle.load(cls)
#         self.keyword = keyword
#
#     def apply(self, callback, route):
#         _callback = route['callback']
#         # Test if the original callback accepts a 'admin' keyword.
#         # Ignore it if it does not need a database handle.
#         argspec = inspect.signature(_callback)
#         if self.keyword not in argspec.parameters:
#             return callback
#
#         def wrapper(*args, **kwargs):
#             jwt_user = get_jwt_user()
#             if jwt_user is None:
#                 abort(401, 'Invalid user')
#
#             session = (Session.select(Session, Operator)
#                        .join(Operator)
#                        .where(Session.id == jwt_user.get('session_id')))
#             if not session:
#                 abort(401, 'Invalid user')
#             session = session.get()
#
#             if datetime.now() > session.expire_at:
#                 logging.info('admin %s expire', session.op_id)
#                 abort(401, 'Account Expired')
#
#             admin_permission = session.op.role.permission
#             if not admin_permission:
#                 abort(403, 'Permission Denied')
#             callback_permission = getattr(callback, 'permission', None)
#             if (callback_permission and callback_permission not in
#                     json.loads(admin_permission)):
#                 abort(403, 'Permission Denied')
#
#             if session.op.role.id != self.role:
#                 abort(403, 'Current User Role Denied')
#
#             # Add the connection handle as a keyword argument.
#             kwargs[self.keyword] = session.op
#
#             return callback(*args, **kwargs)
#
#         # Replace the route callback with the wrapped one.
#         return wrapper
