import datetime
import os

import pandas
from bottle import request,route,post

from marketsys.api.chuanglan_sendmessage import send_sms
from marketsys.models import Sendmessage_log
from marketsys.plugins.paginator_plugin import page_plugin
from marketsys.serializers import sendmessagelog_serializer

from marketsys.utils import plain_forms
from marketsys.validator import number_validator


@route('/api/single_sendmessage',method =['POST','OPTIONS'])
def single_sendmessage(op):
    args = request.json
    phone_number = args['phone_number']
    content = args['content']
    response = send_sms(phone_number,content)
    if response['code'] == "0":
        op.logged_sendmessage(phone_number,content)
        return {"statu": 200, "send": "ok"}
    else:
        op.logged_sendmessage(phone_number,content,response['error'])
        return {"statu":200,"send":"fail"}

@route('/api/search_sendmessagelog',apply = [page_plugin],method =['POST','OPTIONS'])
def search_sendmessagelog(op):
    args = request.json
    phone = args['phone_number']
    start_senttime = args['start_senttime']
    end_senttime = args['end_senttime']
    context = args['context']
    logs = Sendmessage_log.select()
    if phone:
        logs = logs.where(Sendmessage_log.phone_number == phone)
    if start_senttime:
        start_senttime = datetime.datetime.strptime(start_senttime,'%Y-%m-%d %H:%M:%S')
        logs = logs.where(Sendmessage_log.created_at >= start_senttime)
    if end_senttime:
        end_senttime = datetime.datetime.strptime(end_senttime,'%Y-%m-%d %H:%M:%S')
        logs = logs.where(Sendmessage_log.created_at <= end_senttime)
    if context:
        logs = logs.where(Sendmessage_log.content.contains(context))
    logs = logs.order_by(Sendmessage_log.created_at.desc())
    return logs,sendmessagelog_serializer


@route('/api/getexcel',method =['POST','OPTIONS'])
def getexceldata(op):
    print("form",request.forms)
    filename_s = request.forms['filename']

    filedata = request.files['file']

    filename = str(datetime.datetime.now()).split('.')[0].replace(' ','-').replace(':','-') + filename_s
    filepath = os.path.dirname(os.path.dirname(__file__))+'/excelfile/'+ filename
    filedata.save(filepath,overwrite=True)

    filetype = filename_s.split('.')[-1]
    if filetype == 'csv':
        df = pandas.read_csv(filepath)
    else:
        df = pandas.read_excel(filepath)
    re_df = df.head(10)
    data = re_df.values.tolist()
    data_list = []
    for i in data:
        print(i)
        # i = str(i[0]).split('\t')
        obj = {}
        obj['phone_number'] = i[0]
        obj['content'] = i[1]
        data_list.append(obj)
    return {'statu':200,'data':data_list,'filename':filename}

@post('/api/send_allmessage')
def send_allmessage(op):
    filename = request.json['filename']
    filepath = os.path.dirname(os.path.dirname(__file__)) + '/excelfile/' + filename
    df = pandas.read_excel(filepath)
    data = df.values
    for i in data:
        send_sms(str(i[0]),str(i[1]))
    return {'statu':200}


