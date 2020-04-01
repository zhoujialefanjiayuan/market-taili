import json
from _pydecimal import Decimal
from datetime import datetime, timedelta
from enum import Enum

import bottle
import jwt
from peewee import (DateTimeField, BigIntegerField, CharField,
                    DateField, DecimalField, IntegerField,
                    SmallIntegerField, ForeignKeyField, TextField, R,
                    DoesNotExist, BooleanField)
from playhouse.signals import Model

from marketsys.db import db
from marketsys.utils import idg, request_ip

app = bottle.default_app()

class ModelBase(Model):
    created_at = DateTimeField(constraints=[R('DEFAULT CURRENT_TIMESTAMP')])
    updated_at = DateTimeField(constraints=[R('DEFAULT CURRENT_TIMESTAMP'),
                                            R('ON UPDATE CURRENT_TIMESTAMP')])

    def update_dict(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
        return self

    class Meta:
        database = db
        only_save_dirty = True



class Operator(ModelBase):
    id = BigIntegerField(default=idg, primary_key=True)
    username = CharField(max_length=32)
    password = CharField(max_length=64)
    email = CharField(max_length=32, null=True)
    phone = CharField(max_length=16, null=True)
    last_active_at = DateTimeField(null=True)
    promise = CharField(max_length=524, null=True)
    class Meta:
        db_table = 'operator'

    def logged_in(self, expire_days=8):
        headers = bottle.request.headers
        expire_at = datetime.now() + timedelta(days=expire_days)
        try:
            session = Session.get(op=self)
            session.expire_at = datetime.now() + timedelta(days=expire_days)
            session.save()
        except:
            session = Session.create(op=self,
                                     ip=request_ip(),
                                     ua=headers.get('User-Agent'),
                                     expire_at=expire_at)
        return session
    def logged_sendmessage(self,phone_number,content,statu = "ok"):
        sendmessage = Sendmessage_log.create(op=self,
                               phone_number = phone_number,
                               content = content,
                               statu = statu,
                               )
        return sendmessage

class Session(ModelBase):
    id = BigIntegerField(primary_key=True)
    op = ForeignKeyField(Operator, related_name='sessions')
    ip = CharField(max_length=64)
    ua = TextField()
    expire_at = DateTimeField()

    class Meta:
        db_table = 'session'
        auto_increment = True

    def jwt_token(self):
        token = jwt.encode({
            'session_id': str(self.id),
            'cs_id': str(self.op.id),
        }, app.config['op.secret'])
        return token.decode('utf-8')

class Sendmessage_log(Model):
    id = BigIntegerField(default=idg, primary_key=True)
    phone_number = CharField(max_length=50)
    content = CharField(max_length=524)
    created_at = DateTimeField(constraints=[R('DEFAULT CURRENT_TIMESTAMP')])
    statu = CharField(max_length=524)
    op = ForeignKeyField(Operator, related_name='sendmessage_log')

    class Meta:
        database = db
        only_save_dirty = True
        db_table = 'sendmessage_log'


#注册认证申请表
class RegistAuthApp(Model):
    #id = BigIntegerField(primary_key=True)
    created_at = DateTimeField(constraints=[R('DEFAULT CURRENT_TIMESTAMP')])
    register_num =  BigIntegerField(default=0)
    auth_num =  BigIntegerField(default=0)
    application_pepolenum = BigIntegerField(default=0) #申请人数
    remit_num =  BigIntegerField(default=0)
    app_name = CharField(max_length=100)
    day = DateTimeField()
    class Meta:
        database = db
        only_save_dirty = True
        db_table = 'regist_auth_app'


#放款，还款，逾期金额历史汇总
class RemitRepayDue(Model):
    #id = BigIntegerField(primary_key=True)
    created_at = DateTimeField(constraints=[R('DEFAULT CURRENT_TIMESTAMP')])
    repay_amount =  BigIntegerField(default=0)
    due_amount =  BigIntegerField(default=0)
    remit_amount =  BigIntegerField(default=0) #总的申请数
    day = DateTimeField(unique=True)
    class Meta:
        database = db
        only_save_dirty = True
        db_table = 'remit_repay_due'

class SupplierBalance(Model):
    created_at = DateTimeField(constraints=[R('DEFAULT CURRENT_TIMESTAMP')])
    supplier_day = CharField(max_length=100,unique = True)
    supplier = CharField(max_length=100)
    day = CharField(max_length=100)
    balance = DecimalField(default = 0.0,decimal_places=6,max_digits=17)
    class Meta:
        database = db
        only_save_dirty = True
        db_table = 'supplier_balance'


class BalanceTenMinute(Model):
    created_at = DateTimeField(constraints=[R('DEFAULT CURRENT_TIMESTAMP')])
    supplier = CharField(max_length=100)
    balance = DecimalField(default=0.0, decimal_places=6, max_digits=17)
    themoment = CharField(max_length=100)
    class Meta:
        database = db
        only_save_dirty = True
        db_table = 'balance_ten_minute'


#逾期率
class DPD(Model):
    created_at = DateTimeField(constraints=[R('DEFAULT CURRENT_TIMESTAMP')])
    dptp1_rate = DecimalField(default = 0.0,decimal_places=1,max_digits=5)
    dptp1_3_rate = DecimalField(default = 0.0,decimal_places=1,max_digits=5)
    dptp1_15_rate = DecimalField(default = 0.0,decimal_places=1,max_digits=5)
    dptp1_num = DecimalField(default = 0.0,decimal_places=1,max_digits=5)
    dptp1_3_num = DecimalField(default = 0.0,decimal_places=1,max_digits=5)
    dptp1_15_num = DecimalField(default = 0.0,decimal_places=1,max_digits=5)
    app_name =  CharField(max_length=50)
    class Meta:
        database = db
        only_save_dirty = True
        db_table = 'dpd'