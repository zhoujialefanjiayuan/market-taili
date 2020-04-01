import json
import os

import pandas  as pd
from bottle import post, request, default_app, abort, get,static_file,response

from marketsys.constant_mapping import ApplicationStatus
from marketsys.db import account_center_conn, login_conn, bill_conn, marketsys_conn, redis_conn
from marketsys.plugins import packing_plugin

# app = default_app()
# accountserveice = accountserveice()

# baseurl = app.config['service.accountjava_service.base_url']


def single_to_double_for_timestr(timestr):
    if timestr == '':
        return ''
    fisrt = timestr.split(' ')
    second = fisrt[0].split('-')
    second[1] = '0'+ second[1] if int(second[1]) < 10 else second[1]
    second[2] = '0'+ second[2] if int(second[2]) < 10 else second[2]
    return  '-'.join(second) +' ' + fisrt[1]


@post('/api/getuser-by-user_id')
def getuser_byuserid(op):
    userid = request.json['userid']
    if not userid:
        return abort(500, "not params")
    account_center_db,account_center_cursor = account_center_conn()
    sql = '''
    select
        ua.id, 
        ua.mobile_no,
        ua.created_at,
        ua.is_app_frozen,
        i.created_at,
        MAX(uap.credit_max)
        from user_account ua
        left outer join   identity i on ua.id = i.user_id
        left outer join  user_account_product uap on ua.id = uap.user_id
    where ua.id = %s'''%userid
    account_center_cursor.execute(sql)
    data = account_center_cursor.fetchall()[0]
    if data[0] is None:
        account_center_db.close()
        return abort(500,"user don't exist")

    obj = {'userid':data[0],'mobile_no':data[1],'created_at':str(data[2]),'is_frozen':data[3],
           'authentication_at':str(data[4]) if data[4] else data[4],'authentication_money':data[5]}
    account_center_db.close()
    #查询最后活跃时间
    login_db, login_db_cursor= login_conn()
    sql2 = 'select max(updated_at) from login_user_log where user_id ='+ str(obj['userid'])
    login_db_cursor.execute(sql2)
    time = login_db_cursor.fetchall()[0][0]
    obj['last_active_time'] = str(time) if time else None
    login_db.close()

    #查询借款次数，和最后的状态
    bill_db, bii_cursor = bill_conn()
    sql3 = 'select status,max(created_at),count(id) from application where user_id =' + str(obj['userid'])
    bii_cursor.execute(sql3)
    data = bii_cursor.fetchall()
    bill_db.close()
    application_times = data[0][2]
    if application_times == 0:
        obj['status'] = None
    else:
        status = data[0][0]
        obj['status'] = ApplicationStatus.visible_dict()[status]
    obj['application_times'] = application_times
    return  {'result':[obj],'total':1,'every':20}


@post('/api/getuser-by-mobile_no')
def getuser_bymobileno(op):
    mobile_no = request.json['mobile_no']
    if not mobile_no:
        return abort(500, "not params")
    account_center_db, account_center_cursor = account_center_conn()
    sql = '''
        select
            ua.id, 
            ua.mobile_no,
            ua.created_at,
            ua.is_app_frozen,
            i.created_at,
            MAX(uap.credit_max)
            from user_account ua
            left outer join  identity i on ua.id = i.user_id
            left outer join  user_account_product uap on ua.id = uap.user_id
        where ua.mobile_no = %s''' % mobile_no
    account_center_cursor.execute(sql)
    data = account_center_cursor.fetchall()[0]
    if data[0] == None:
        return abort(500,"user don't exist")
    obj = {'userid': data[0], 'mobile_no': data[1], 'created_at': str(data[2]), 'is_frozen': data[3],
           'authentication_at': str(data[4]) if data[4] else data[4], 'authentication_money': data[5]}
    account_center_db.close()
    # 查询最后活跃时间
    login_db, login_db_cursor = login_conn()
    sql2 = 'select max(updated_at) from login_user_log where user_id =' + str(obj['userid'])
    login_db_cursor.execute(sql2)
    time = login_db_cursor.fetchall()[0][0]
    obj['last_active_time'] = str(time) if time else None
    login_db.close()
    # 查询借款次数，和最后的状态
    bill_db, bii_cursor = bill_conn()
    sql3 = 'select status,max(created_at),count(id) from application where user_id =' + str(obj['userid'])
    bii_cursor.execute(sql3)
    data = bii_cursor.fetchall()
    bill_db.close()
    application_times = data[0][2]
    if  application_times == 0:
        obj['status'] = None
    else:
        status = data[0][0]
        obj['status'] = ApplicationStatus.visible_dict()[status]
    obj['application_times'] = application_times
    return {'result':[obj],'total':1,'every':10}


@get('/api/condition')
def condition(op):
    account_center_db, account_center_cursor = account_center_conn()
    sql = 'select credit_max from user_account_product group by credit_max'
    account_center_cursor.execute(sql)
    authentication_money = account_center_cursor.fetchall()
    authentication_money_set = set()
    for i in authentication_money:
        authentication_money_set.add(i[0])
    authentication_money_list = list(authentication_money_set)
    authentication_money_list.sort()
    account_center_db.close()
    return {'authentication_money':authentication_money_list,
            "application_times":[0,1,2,3,"3次以上"],
            "is_frozen":['是','否'],
            # [(120, '无借款'), (70, "还款中"), (90, '逾期')]}
            "status":['无借款',"还款中",'逾期']}

@post('/api/search_userdetil')
def getUser_detil(op):
    params = request.json
    created_at_start = single_to_double_for_timestr(params['created_at_start'])
    created_at_end = single_to_double_for_timestr(params['created_at_end'])
    authentication_at_start = single_to_double_for_timestr(params['authentication_at_start'])
    authentication_at_end = single_to_double_for_timestr(params['authentication_at_end'])
    last_active_time_start = single_to_double_for_timestr(params['last_active_time_start'])
    last_active_time_end = single_to_double_for_timestr(params['last_active_time_end'])
    get_authentication_money = params['authentication_money']
    get_application_times = params['application_times']
    get_status = params['status']
    get_is_frozen = params['is_frozen']
    page = params['page']
    if get_is_frozen == '是':
        get_is_frozen = 1
    if get_is_frozen == '否':
        get_is_frozen = 0
    if get_status == "无借款":
        get_status = [0,10,20,30,60]
    if get_status == "还款中":
        get_status = [40,50,70,80,120,130]
    if get_status == "逾期":
        get_status = [90,100,110]

    rs = redis_conn()
    userdata = rs.get('userdata')
    if not userdata:
        # pandas读取数据并缓存,按照每月缓存：
        basesql = '''
            select
                ua.id as userid, 
                ua.mobile_no,
                ua.created_at as created_at,
                ua.is_app_frozen as is_frozen,
                i.created_at as authentication_at,
                MAX(ifnull(uap.credit_max,0)) as authentication_money
                from user_account ua
                left outer join  identity i on ua.id = i.user_id
                left outer join  user_account_product uap on ua.id = uap.user_id
                group by ua.id order by ua.created_at desc
                '''
        account_center_db, account_center_cursor = account_center_conn()
        alldata = pd.read_sql(basesql, account_center_db)
        account_center_db.close()
        login_db, login_db_cursor = login_conn()
        sql2 = 'select mobile_no,max(updated_at) as active_at from login_user group by mobile_no'
        active_data = pd.read_sql(sql2, login_db)
        login_db.close()

        bill_db, bii_cursor = bill_conn()
        sql3 = 'select user_id as userid,status,max(created_at) as created,count(id) as application_times from application where status != 20 group by user_id '
        app_data = pd.read_sql(sql3, bill_db)
        bill_db.close()
        data = pd.merge(pd.merge(alldata,active_data,how='left'),app_data).fillna(0)
        data['created_at'] = data['created_at'].apply(lambda x:str(x))
        data['authentication_at'] = data['authentication_at'].apply(lambda x:str(x))
        data['created'] = data['created'].apply(lambda x:str(x))
        data['active_at'] = data['active_at'].apply(lambda x:str(x))
        print(data.head(1).to_dict(orient='split'))
        rs.set('userdata', json.dumps(data.to_dict(orient='split')), 18000)
        userdata = data
    else:
        userdata = json.loads(userdata)
        userdata = pd.DataFrame(**userdata)
    if created_at_start:
        userdata = userdata[userdata['created_at'] >= created_at_start]
    if created_at_end:
        created_at_end
        userdata = userdata[userdata['created_at'] <= created_at_end]
    if authentication_at_start:
        userdata = userdata[userdata['authentication_at'] >= authentication_at_start]
    if authentication_at_end:
        userdata = userdata[userdata['authentication_at'] <= authentication_at_end]
    if get_authentication_money:
        userdata = userdata[userdata['authentication_money'] == get_authentication_money]
    if get_is_frozen != '':
        userdata = userdata[userdata['is_frozen'] == get_is_frozen]
    #"application_times": [0, 1, 2, 3, "3次以上"]
    if get_application_times != '':
        if get_application_times == "3次以上":
            userdata = userdata[userdata['application_times'] > 3]
        else:
            get_application_times = int(get_application_times)
            userdata = userdata[userdata['application_times'] == get_application_times]
    if last_active_time_start:
        userdata = userdata[userdata['active_at'] >= last_active_time_start]
    if last_active_time_end:
        userdata = userdata[userdata['active_at'] <= last_active_time_end]
    if get_status:
        userdata = userdata[userdata['status'].isin(get_status)]
    if userdata.empty:
        return abort(500, "data don't exist")
    total = userdata.shape[0]
    send_data = userdata.loc[(page-1)*200:page*200]
    print(send_data)
    send_data['status'] = send_data['status'].apply(lambda x : ApplicationStatus.visible_dict()[x])
    return  {'result': send_data.to_dict(orient='record'), 'total': total, 'every': 200}

@get('/api/channel')
def getchannel(op):
    marketsys_db, _ = marketsys_conn()
    r = pd.read_sql('select * from channel_regist_auth_ap_remit', marketsys_db)['channel']
    marketsys_db.close()
    return r.values.tolist()

@post('/api/channeldata')
def channeldata(op):
    params = request.json
    authtime_start = single_to_double_for_timestr(params['authtime_start'])
    authtime_end = single_to_double_for_timestr(params['authtime_end'])
    channel = params['channel']
    page = params['page']
    account_center_db, cursor = marketsys_conn()
    if authtime_start == '' and authtime_end =='':
        b = pd.read_sql('select * from channel_bill_sub;', account_center_db)
        channel_bill_sub_data = b.fillna(0).to_dict(orient='index')
        channel_installment = {}
        for i in channel_bill_sub_data.values():
            name = 'none' if i['channel'] == None else i['channel']
            if name not in channel_installment:
                channel_installment[name] = {'stage_num1': 0, 'stage_num2': 0, 'stage_num3': 0, 'stage_num4': 0}
                channel_installment[name]['stage_num' + str(i['stage_num'])] += i['ispay']
            else:
                channel_installment[name]['stage_num' + str(i['stage_num'])] += i['ispay']

        r = pd.read_sql('select * from channel_regist_auth_ap_remit', account_center_db)
        channel_regist_auth_ap_remit = r.fillna(0).to_dict(orient='index')
        out_data = []
        for i in channel_regist_auth_ap_remit.values():
            if i['channel'] == channel or channel == '' :
                try:
                    i.update(channel_installment[i['channel']])
                except:
                    i.update({'stage_num1': 0, 'stage_num2': 0, 'stage_num3': 0, 'stage_num4': 0})
                i.update({'computer_check': (i['ap_success'] - i['human_check'])})
                for j in i.keys():
                    regesit = i['regesit']
                    if j != 'channel' and j != 'regesit':
                        if regesit == 0:
                            i[j] = str(i[j]) + ' (0.00%)'
                        else:
                            i[j] = str(i[j]) + ' (' + str('%.2f%%'%(i[j] * 100/regesit)) + ')'
                out_data.append(i)

    else:
        rs = redis_conn()
        bill_sub_channel_data = rs.get('bill_sub_channel_data')
        if not bill_sub_channel_data:
            basesql1 = 'select * from bill_sub_channel '
            bill_sub_channel_data = pd.read_sql(basesql1, account_center_db)
            bill_sub_channel_data['due_at'] = bill_sub_channel_data['due_at'].apply(lambda x : str(x))
            bill_sub_channel_data['created_at'] = bill_sub_channel_data['created_at'].apply(lambda x : str(x))
            rs.set('bill_sub_channel_data',json.dumps(bill_sub_channel_data.to_dict(orient='split')),18000)
        else:
            bill_sub_channel_data = json.loads(bill_sub_channel_data)
            bill_sub_channel_data = pd.DataFrame(**bill_sub_channel_data)

        if authtime_start:
            #authtime_start = datetime.strptime(authtime_start, "'%Y-%m-%d %H:%M:%S'")
            bill_sub_channel_data = bill_sub_channel_data[bill_sub_channel_data['created_at'] >= authtime_start]
        if authtime_end:
            bill_sub_channel= bill_sub_channel_data[bill_sub_channel_data['created_at'] <= authtime_end]
        if bill_sub_channel.empty == True:
            return abort(500, "data don't exist")
        bill_id = bill_sub_channel.groupby(['channel', 'is_installment', 'stage_num'])[['bill_id']].count().reset_index()
        bill_id['on_time_repay'] = bill_sub_channel.groupby(['channel', 'is_installment', 'stage_num'])[['on_time_repay']].sum().reset_index()['on_time_repay']
        billsub_data = bill_id.fillna(0).to_dict(orient='index')

        channel_installment = {}
        for i in billsub_data.values():

            name = 'none' if i['channel'] == None else i['channel']
            if name not in channel_installment:
                channel_installment[name] = {'stage_num1': 0, 'stage_num2': 0, 'stage_num3': 0, 'stage_num4': 0}
                channel_installment[name]['stage_num' + str(i['stage_num'])] += i['on_time_repay']
            else:
                channel_installment[name]['stage_num' + str(i['stage_num'])] += i['on_time_repay']

        # 创建表ap_num_humancheck

        ap_num_humancheck_data = rs.get('ap_num_humancheck_data')
        if not ap_num_humancheck_data:
            basesql2 = 'select count(ap_id) as ap_num,count(is_human) as human_check,count(ap_success) as ap_success, sum(badloan) as badloan, channel,created_at from ap_humancheck group by channel'
            ap_num_humancheck_data = pd.read_sql(basesql2, account_center_db)
            ap_num_humancheck_data['created_at'] = ap_num_humancheck_data['created_at'].apply(lambda x:str(x))
            rs.set('ap_num_humancheck_data', json.dumps(ap_num_humancheck_data.to_dict(orient='split')), 18000)
        else:
            ap_num_humancheck_data = json.loads(ap_num_humancheck_data)
            ap_num_humancheck_data = pd.DataFrame(**ap_num_humancheck_data)
        print(ap_num_humancheck_data.head())
        if authtime_start:
            ap_num_humancheck_data = ap_num_humancheck_data[ap_num_humancheck_data['created_at'] >= authtime_start]
        if authtime_end:
            ap_num_humancheck_data = ap_num_humancheck_data[ap_num_humancheck_data['created_at'] <= authtime_end]
            ap_num_humancheck_data.drop(columns='created_at', inplace=True)
        # basesql2 +=' group by channel'
        ap_num_humancheck = ap_num_humancheck_data

        # basesql3 = 'select count(id) as regesit,count(is_auth) as is_auth,channel from channel_auth_table '
        # if authtime_start:
        #     if 'where' in basesql3:
        #         basesql3 += ' and created_at >= str_to_date("{}","%Y-%m-%d %H:%i:%s")'.format(authtime_start)
        #     else:
        #         basesql3 += ' where created_at >= str_to_date("{}","%Y-%m-%d %H:%i:%s")'.format(authtime_start)
        # if authtime_end:
        #     if 'where' in basesql3:
        #         basesql3 += ' and created_at <= str_to_date("{}","%Y-%m-%d %H:%i:%s")'.format(authtime_end)
        #     else:
        #         basesql3 += ' where created_at <= str_to_date("{}","%Y-%m-%d %H:%i:%s")'.format(authtime_end)
        # basesql3 += ' group by channel'
        # channel_regesit_auth = pd.read_sql(basesql3, account_center_db)

        channel_regesit_auth_data = rs.get('channel_regesit_auth_data')
        if not channel_regesit_auth_data:
            basesql3 = 'select count(id) as regesit,count(is_auth) as is_auth,channel,created_at from channel_auth_table group by channel '
            channel_regesit_auth_data = pd.read_sql(basesql3, account_center_db)
            channel_regesit_auth_data['created_at'] = channel_regesit_auth_data['created_at'].apply(lambda x: str(x))
            rs.set('channel_regesit_auth_data', json.dumps(channel_regesit_auth_data.to_dict(orient='split')), 18000)
        else:
            channel_regesit_auth_data = json.loads(channel_regesit_auth_data)
            channel_regesit_auth_data = pd.DataFrame(**channel_regesit_auth_data)
        if authtime_start:
            channel_regesit_auth_data = channel_regesit_auth_data[channel_regesit_auth_data['created_at'] >= authtime_start]
        if authtime_end:
            channel_regesit_auth_data = channel_regesit_auth_data[channel_regesit_auth_data['created_at'] <= authtime_end]
            channel_regesit_auth_data.drop(columns='created_at', inplace=True)
        channel_regesit_auth_data = channel_regesit_auth_data.merge(ap_num_humancheck, on='channel',
                                                               how='left').fillna(0).to_dict(orient='index')
        out_data = []
        for i in channel_regesit_auth_data.values():
            if i['channel'] == channel or channel == '':
                try:
                    i.update(channel_installment[i['channel']])
                except:
                    i.update({'stage_num1': 0, 'stage_num2': 0, 'stage_num3': 0, 'stage_num4': 0})
                i.update({'computer_check': (i['ap_success'] - i['human_check'])})
                for j in i.keys():
                    regesit = i['regesit']
                    if j != 'channel' and j != 'regesit':
                        if regesit == 0:
                            i[j] = str(i[j]) + '(0.00%)'
                        else:
                            i[j] = str(i[j]) + str('%.2f%%'%(i[j]*100/regesit))
                out_data.append(i)
        rs.close()
    account_center_db.close()

    total = len(out_data)
    page = int(page)
    return {'result':out_data[(page-1)*20:page*20],'total':total,'every':20}

#搜索后数据下载接口
@post('/api/download/search_userdetil',skip=[packing_plugin])
def getUser_detil(op):
    params = request.json
    created_at_start = single_to_double_for_timestr(params['created_at_start'])
    created_at_end = single_to_double_for_timestr(params['created_at_end'])
    authentication_at_start = single_to_double_for_timestr(params['authentication_at_start'])
    authentication_at_end = single_to_double_for_timestr(params['authentication_at_end'])
    last_active_time_start = single_to_double_for_timestr(params['last_active_time_start'])
    last_active_time_end = single_to_double_for_timestr(params['last_active_time_end'])
    get_authentication_money = params['authentication_money']
    get_application_times = params['application_times']
    get_status = params['status']
    get_is_frozen = params['is_frozen']
    page = params['page']
    if get_is_frozen == '是':
        get_is_frozen = 1
    if get_is_frozen == '否':
        get_is_frozen = 0
    if get_status == "无借款":
        get_status = [0,10,20,30,60]
    if get_status == "还款中":
        get_status = [40,50,70,80,120,130]
    if get_status == "逾期":
        get_status = [90,100,110]
    rs = redis_conn()
    userdata = rs.get('userdata')
    userdata = json.loads(userdata)
    userdata = pd.DataFrame(**userdata)
    if created_at_start:
        userdata = userdata[userdata['created_at'] >= created_at_start]
    if created_at_end:
        created_at_end
        userdata = userdata[userdata['created_at'] <= created_at_end]
    if authentication_at_start:
        userdata = userdata[userdata['authentication_at'] >= authentication_at_start]
    if authentication_at_end:
        userdata = userdata[userdata['authentication_at'] <= authentication_at_end]
    if get_authentication_money:
        userdata = userdata[userdata['authentication_money'] == get_authentication_money]
    if get_is_frozen != '':
        userdata = userdata[userdata['is_frozen'] == get_is_frozen]
    #"application_times": [0, 1, 2, 3, "3次以上"]
    if get_application_times != '':
        if get_application_times == "3次以上":
            userdata = userdata[userdata['application_times'] > 3]
        else:
            get_application_times = int(get_application_times)
            userdata = userdata[userdata['application_times'] == get_application_times]
    if last_active_time_start:
        userdata = userdata[userdata['active_at'] >= last_active_time_start]
    if last_active_time_end:
        userdata = userdata[userdata['active_at'] <= last_active_time_end]
    if get_status:
        userdata = userdata[userdata['status'].isin(get_status)]

    if userdata.empty:
        return abort(500, "data don't exist")
    #total = userdata.shape[0]
    if page == -1:
        send_data = userdata
    else:
        send_data = userdata.loc[:page*200]
    send_data['status'] = send_data['status'].apply(lambda x : ApplicationStatus.visible_dict()[x])
    nowpath = os.getcwd()
    if not os.path.exists(nowpath+'/downloaddata/account_data.xls'):
        os.mknod(nowpath+'/downloaddata/account_data.xls',0o777)
    send_data.to_excel(nowpath+'/downloaddata/account_data.xls')
    return '/api/download/account'
