from voluptuous import (Schema, message, TrueInvalid, Required,
                        Length, Any, All, Coerce, Optional, REMOVE_EXTRA)
from voluptuous.validators import truth
from voluptuous.error import Invalid
import ipaddress


from datetime import datetime


class IPInvalid(Invalid):
    """The value is not valid IP."""


@message('expected an IP address', cls=IPInvalid)
def IPAddr(v):
    """Verify that the value is an IP or not.
    Support validate IPv4 or IPv6
    """
    try:
        if not v:
            raise IPInvalid("expected an IP address")
        if not isinstance(v, str):
            raise IPInvalid("expected an string type")

        # this function only available in python 3
        ipaddress.ip_address(v)
        return v
    except:
        raise ValueError


def ListCoerce(t):
    return lambda vs: [t(v) for v in vs] if vs else []


def Strip(chars=None):
    return lambda v: v.strip(chars) if v else v


def Cut(end, start=0):
    return lambda v: v[start:end]


@message('Invalid digit', cls=TrueInvalid)
@truth
def IsDigit(s):
    return s.isdigit()


def Date(fmt='%Y-%m-%d'):
    return lambda v: datetime.strptime(v, fmt).date()


def Datetime(fmt='%Y-%m-%d %H:%M:%S'):
    return lambda v: datetime.strptime(v, fmt)


class ValidatorSchema(Schema):
    pass

reset_password_validator = ValidatorSchema({
    Required('old_password'): Length(min=6, msg='password format error'),
    Required('new_password'): Length(min=6, msg='password format error'),
})

login_validator = ValidatorSchema({
    Required('username'): Length(min=2, max=20,
                                 msg='username format error'),
    Required('password'): Length(min=6, msg='password format error'),
})

number_validator = ValidatorSchema({
    Required('phone_number'): Length(max=50, msg='phone number too long'),
    Required('content'): Length(max=512, msg='message too long'),
})

repayment_check_validator = ValidatorSchema({
    'user_id': IsDigit(),
    'app_id': IsDigit(),
    'remit_at': Datetime(),
    'due_at': Datetime(),
    'proof_path': str,
    'app': str
})
