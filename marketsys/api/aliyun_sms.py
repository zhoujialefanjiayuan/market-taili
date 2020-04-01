#!/usr/bin/env python
#coding=utf-8


from aliyunsdkcore.client import AcsClient
from aliyunsdkcore.request import CommonRequest
phones = ['18689227306']
AccessKeyId='LTAI4Fc52iKbkm9zXb27vz1U'
AccessKeySecret = 'GJeLj9qLYxxJVziTo7ufPKba7icZSU'
def sendsms(phonenumber,supplier,balance):
    client = AcsClient(AccessKeyId, AccessKeySecret, 'cn-hangzhou')
    request = CommonRequest()
    request.set_accept_format('json')
    request.set_domain('dysmsapi.aliyuncs.com')
    request.set_method('POST')
    request.set_protocol_type('https') # https | http
    request.set_version('2017-05-25')
    request.set_action_name('SendSms')

    request.add_query_param('RegionId', "cn-hangzhou")
    request.add_query_param('PhoneNumbers', phonenumber)
    request.add_query_param('SignName', "深圳泰利网络科技有限公司")
    request.add_query_param('TemplateCode', "SMS_184826212")
    request.add_query_param('TemplateParam', {'channel':supplier,'money':str(balance) +'元'})

    response = client.do_action_with_exception(request)
    return

