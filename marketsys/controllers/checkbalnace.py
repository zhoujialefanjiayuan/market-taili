import datetime
import time

import pandas as pd
from bottle import get
from marketsys.api import checkbalance
from marketsys.api.aliyun_sms import sendsms
from marketsys.db import marketsys_conn
from marketsys.models import SupplierBalance, BalanceTenMinute

#监控余额短信提示
phones = ['18689227306']

@get('/api/checkbalance')
def supplier_balance():
    yesterday = str(datetime.datetime.today() - datetime.timedelta(days=1))[:10]
    print(yesterday)
    balancedata = SupplierBalance.select().order_by(SupplierBalance.id.desc()).limit(6)
    data = []
    suppliers = []
    for supplier in balancedata:
        obj = {}
        obj['supplier'] = supplier.supplier
        obj['day'] = supplier.day
        obj['balance'] = str(supplier.balance)
        data.append(obj)
        suppliers.append(supplier.supplier)
    markdb,_ = marketsys_conn()
    sql = 'select * from supplier_balance order by id desc limit %d'%(len(data)*100)
    alldata = pd.read_sql(sql,markdb)
    moment_sql = 'select * from balance_ten_minute order by id desc limit %d'%(len(data)*100)
    moment_data = pd.read_sql(moment_sql, markdb)

    chartdata = alldata[alldata.supplier == suppliers[0] ]
    momentchart = moment_data[moment_data.supplier == suppliers[0] ]

    chartdata = chartdata.rename(columns={'balance':suppliers[0]}).drop(['id','created_at','supplier_day','supplier'],axis=1)
    momentchart = momentchart.rename(columns={'balance':suppliers[0]}).drop(['id','created_at','supplier'],axis=1)
    for supplier in suppliers[1:]:
        chartdata = pd.merge(chartdata,alldata[alldata.supplier == supplier][['balance','day']],how='outer',on='day').rename(columns={'balance':supplier})
        momentchart = pd.merge(momentchart,moment_data[moment_data.supplier == supplier][['balance','themoment']],how='outer',on='themoment').rename(columns={'balance':supplier})
    chartdata= chartdata.fillna(0).applymap(lambda x:str(x))
    momentchart= momentchart.fillna(0).applymap(lambda x:str(x))
    print(chartdata)
    print(momentchart)
    markdb.close()
    return {'table':data,'chart':chartdata.sort_values('day').to_dict(orient="list"),
            'momentchart':momentchart.sort_values('themoment').to_dict(orient="list")}


#由定时任务每十分钟调用
@get('/api/tenminute/uiopjnghds')
def tenminute_balance():
    allbalance = checkbalance()
    themoment = str(datetime.datetime.today())[0:16]
    for i in allbalance:
        i['themoment'] = themoment
    BalanceTenMinute.insert_many(allbalance).execute()
    return

#由定时任务每天调用一次,23:55:00
@get('/api/oneday/uiopjnghdskhbkf')
def oneday_balance():
    #清空balance_ten_minute表
    #BalanceTenMinute.delete()
    today = str(datetime.datetime.today())[:10]
    yesterday =  str(datetime.datetime.today() - datetime.timedelta(days=1))[:10]
    yesterdaydata = SupplierBalance.select().where(SupplierBalance.day==yesterday)
    yesterday_balance = {}
    for i in yesterdaydata:
        yesterday_balance[i.supplier] = float(i.balance)
    print(yesterday_balance)
    allbalance = checkbalance()
    for i in allbalance:
        i['supplier_day'] = i['supplier'] + ',' + today
        i['day'] = today
        dist_balance_for_tendays = (yesterday_balance.get(i['supplier'],0) - i['balance'])*10
        if dist_balance_for_tendays > i['balance']:
            for phone in phones:
                sendsms(phone,i['supplier'],i['balance'])
            time.sleep(120)
    SupplierBalance.insert_many(allbalance).execute()
    return