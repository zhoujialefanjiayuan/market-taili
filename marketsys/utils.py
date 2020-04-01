import json
import os
import re
import sys
from datetime import datetime, date

from bottle import request
from decimal import Decimal



#时间转字符串
from marketsys import snowflake


def json_default(dt_fmt='%Y-%m-%d %H:%M:%S', date_fmt='%Y-%m-%d',
                 decimal_fmt=str):
    def _default(obj):
        if isinstance(obj, datetime):
            return obj.strftime(dt_fmt)
        elif isinstance(obj, date):
            return obj.strftime(date_fmt)
        elif isinstance(obj, Decimal):
            return decimal_fmt(obj)
        else:
            raise TypeError('%r is not JSON serializable' % obj)

    return _default

#自定义json_dump，可以序列化时间
def json_dumps(obj, dt_fmt='%Y-%m-%d %H:%M:%S', date_fmt='%Y-%m-%d',
               decimal_fmt=str, ensure_ascii=False):
    return json.dumps(obj, ensure_ascii=ensure_ascii,
                      default=json_default(dt_fmt, date_fmt, decimal_fmt))

#dict  访问对象化
class PropDict(dict):
    """A dict that allows for object-like property access syntax."""

    def __getattr__(self, name):
        return self[name] if name in self else None


def _plain_args(d, list_fields=None):
    list_fields = list_fields or ()

    result = PropDict((key, getattr(d, key)) for key in d)
    for key in list_fields:
        result[key] = d.getall(key)

    return result


def plain_forms(list_fields=None):
    """ Plain POST data. """
    return _plain_args(request.forms, list_fields)



def plain_query(list_fields=None):
    """ Plain GET data """
    return _plain_args(request.query, list_fields)


def plain_params(list_fields=None):
    """ Plain all data """
    return _plain_args(request.params, list_fields)


id_generator = snowflake.generator(1, 1)


def idg():
    return next(id_generator)


def get_permission(role_group, role_action):
    return '%s^%s' % (role_group.strip(), role_action.strip())


def mask(s, start=0, end=None, fill_with='*'):
    """ 将指定范围内的字符替换成指定字符，范围规则与 list 切片一致 """
    sl = list(s)
    if end is None:
        end = len(sl)
    sl[start:end] = fill_with * len(sl[start:end])
    return ''.join(sl)


def env_detect():
    env = os.environ.get('APP_ENV')
    if env is None:
        if os.path.basename(sys.argv[0]) in ('utrunner.py', 'nose', 'nose2'):
            env = 'TESTING'
        else:
            env = 'DEV'
    return env


number_strip_re = re.compile(r'\d+')


def number_strip(m):
    # 印尼的号码有86开头，所以去掉中国 的 86 要跟着 + 一起
    m = m.replace('+86', '')

    # 以下规则，都不考虑中国号码

    # 先取纯数字
    number = ''.join(number_strip_re.findall(m))

    if number.startswith('62'):
        number = number[2:]

    # 所有开头的 0 都不要
    return number.lstrip('0')


def request_ip():
    return (request.environ.get('HTTP_X_FORWARDED_FOR') or
            request.environ.get('REMOTE_ADDR'))


def datetime_format(date_str):
    t = None
    if date_str:
        t = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S+%f')
        t = t.strftime('%Y-%m-%d %H:%M:%S')
    return t
