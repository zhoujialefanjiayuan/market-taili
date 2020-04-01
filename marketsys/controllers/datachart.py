
from datetime import date, timedelta, datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np
from bottle import get

from marketsys.db import account_center_conn, bill_conn, marketsys_conn, remittance_conn
from marketsys.models import RemitRepayDue, DPD

app_names = ["IkiDana", "IkiModel"]


@get('/api/datachart/pepole_num/<whereday>/<the_app_name>')
def pepole_num(whereday, the_app_name, op):
    # the_app_name 默认为 all
    if whereday == '1':
        startmon = date.today() - timedelta(days=date.today().day - 1)
        endmon = startmon + relativedelta(months=1)
    else:
        thedate = date(*[int(i) for i in whereday.split('-')])
        startmon = thedate
        endmon = startmon + relativedelta(months=1)
    if date.today() < startmon:
        return
    startmon -= timedelta(days=1)
    data = between_data(str(startmon), str(endmon))
    days = data[['day', 'app_name']].apply(lambda x: str(x[0]) + "," + x[1], axis=1).values
    # print(days)
    max_time = endmon
    min_time = startmon
    while min_time < max_time:
        for app_name in app_names:
            if str(min_time) + "," + app_name in days:
                continue
            else:
                new_row = pd.DataFrame(**{
                    'columns': ['day', 'app_name', 'register_num', 'application_pepolenum', 'auth_num', 'remit_num', ],
                    'data': [[min_time, app_name, 0, 0, 0, 0]], 'index': [1]})
                data = data.append(new_row)
        min_time += timedelta(days=1)

    data = data.sort_values('day')
    if date.today() < endmon:
        basedata = data.set_index('day').loc[startmon + timedelta(days=1):date.today()]
    else:
        basedata = data.set_index('day').loc[startmon + timedelta(days=1):]

    if the_app_name != "all":
        basedata = basedata[basedata.app_name == the_app_name]

    else:
        basedata = basedata.groupby('day')[["register_num", "application_pepolenum", "auth_num", "remit_num"]].sum()
    # 计算转化率
    basedata['auth_rate'] = basedata[['auth_num', 'register_num']].apply(
        lambda x: int(round(0 if x['register_num'] == 0 else x['auth_num'] / x['register_num'], 3) * 1000), axis=1)
    basedata['remit_rate'] = basedata[['remit_num', 'application_pepolenum']].apply(
        lambda x: int(
            round(0 if x['application_pepolenum'] == 0 else x['remit_num'] / x['application_pepolenum'], 3) * 1000),
        axis=1)
    basedata['app_rate'] = basedata[['application_pepolenum', 'register_num']].apply(
        lambda x: int(round(0 if x['register_num'] == 0 else x['application_pepolenum'] / x['register_num'], 3) * 1000),
        axis=1)
    senddict = dict()
    senddict.update(basedata.to_dict(orient='list'))
    senddict['application_num'] = senddict['application_pepolenum']
    senddict['xaxis'] = [str(i).split(' ')[0] for i in basedata.index.tolist()]
    senddict['tablename'] = '注册/认证/申贷/放款人数统计(基于当天注册量)'
    senddict['app_names'] = app_names
    return senddict


# 不基于当天注册用户
@get('/api/datachart/notbase_pepole_num/<whereday>/<the_app_name>')
def pepole_num(whereday, the_app_name):
    # the_app_name 默认为 all
    if whereday == '1':
        startmon = date.today() - timedelta(days=date.today().day - 1)
        endmon = startmon + relativedelta(months=1)
    else:
        thedate = date(*[int(i) for i in whereday.split('-')])
        startmon = thedate
        endmon = startmon + relativedelta(months=1)
    if date.today() < startmon:
        return
    startmon -= timedelta(days=1)
    data = history_between_data(str(startmon), str(endmon))
    data["day"] = data['app_name_day'].apply(lambda x: date(*[int(i) for i in x.split(',')[1].split('-')]))
    data["app_name"] = data['app_name_day'].apply(lambda x: x.split(',')[0])
    days = data['app_name_day'].values
    data.drop(['app_name_day'], axis=1, inplace=True)
    max_time = endmon
    min_time = startmon
    while min_time < max_time:
        for app_name in app_names:
            if app_name + ',' + str(min_time) in days:
                continue
            else:
                new_row = pd.DataFrame(
                    **{'columns': ['register_num', 'app_num', 'auth_num', 'remit_num', 'day', 'app_name'],
                       'data': [[0, 0, 0, 0, min_time, app_name]], 'index': [1]})
                data = data.append(new_row)
        min_time += timedelta(days=1)
    data = data.sort_values('day')
    # print(data)
    # print('*********')
    if date.today() < endmon:
        basedata = data.set_index('day').loc[startmon + timedelta(days=1):date.today()]
    else:
        basedata = data.set_index('day').loc[startmon + timedelta(days=1):]
    # print(basedata)
    # print("----------")
    if the_app_name != "all":
        basedata = basedata[basedata.app_name == the_app_name]
        # print(basedata)
    else:
        basedata = basedata.groupby('day')[["register_num", "app_num", "auth_num", "remit_num"]].sum()
    # 计算转化率
    basedata['auth_rate'] = basedata[['auth_num', 'register_num']].apply(
        lambda x: int(round(0 if x['register_num'] == 0 else x['auth_num'] / x['register_num'], 3) * 1000), axis=1)
    basedata['remit_rate'] = basedata[['remit_num', 'app_num']].apply(
        lambda x: int(round(0 if x['app_num'] == 0 else x['remit_num'] / x['app_num'], 3) * 1000),
        axis=1)
    basedata['app_rate'] = basedata[['app_num', 'register_num']].apply(
        lambda x: int(round(0 if x['register_num'] == 0 else x['app_num'] / x['register_num'], 3) * 1000),
        axis=1)
    senddict = dict()
    senddict.update(basedata.to_dict(orient='list'))
    senddict['application_num'] = senddict['app_num']
    senddict['xaxis'] = [str(i).split(' ')[0] for i in basedata.index.tolist()]
    senddict['tablename'] = '注册/认证/申贷/放款人数统计'
    senddict['app_names'] = app_names
    return senddict


def history_data(today, yesterday):
    basesql1 = '''
                        select
                            u.id as register_num,
                            b.id as application_pepolenum,
                            i.user_id as auth_num,
                            u.app_name as app_name,
                            date(u.created_at) as day
                            from user_account u left join bill.application b on u.id = b.user_id
                            left join identity i on i.user_id = u.id
                            where date(u.created_at) < str_to_date("{}","%Y-%m-%d");
                            '''.format(today)
    account_center_db, _ = account_center_conn()
    # 注册/认证/申请用户
    register_num = pd.read_sql(basesql1, account_center_db)
    # print(register_num)
    # basesql3 = '''
    #                          select
    #                              count(a.user_id) as application_num,
    #                              date(a.created_at) as day
    #                              from application a where a.status != 20 and date(a.created_at) < str_to_date("{}","%Y-%m-%d")
    #                              group by date(a.created_at);
    #                              '''.format(today)
    # # 借款申请
    bill_db, _ = bill_conn()
    # application_num = pd.read_sql(basesql3, bill_db)
    basesql4 = ''' select
                 user_id as remit_num, 
                 date(created_at) as remit_day
                 from bill where date(created_at) < str_to_date("{}","%Y-%m-%d");
                 '''.format(today)
    # 放款人数
    remit_num = pd.read_sql(basesql4, bill_db)
    remit_num["user_id_day"] = remit_num[["remit_num", "remit_day"]].apply(lambda x: str(x[0]) + "," + str(x[1]),
                                                                           axis=1)
    bill_db.close()
    account_center_db.close()
    if register_num.empty:
        return pd.DataFrame(
            {'columns': ['register_num', 'day', 'auth_num', 'remit_num', 'application_pepolenum', "app_name"]})
    register_num["user_id_day"] = register_num[["register_num", "day"]].apply(lambda x: str(x[0]) + "," + str(x[1]),
                                                                              axis=1)
    data = pd.merge(register_num, remit_num, how='left', on="user_id_day")
    enddata = data.groupby(["day", "app_name"])[
        ["register_num", "application_pepolenum", "auth_num", "remit_num"]].count()
    # day  app_name  register_num  application_pepolenum  auth_num  remit_num
    return enddata.reset_index()


def between_data(before, today):
    basesql1 = '''
                           select
                             u.id as register_num,
                             b.id as application_pepolenum,
                             i.user_id as auth_num,
                             u.app_name as app_name,
                             date(u.created_at) as day
                               from user_account u left join bill.application b on u.id = b.user_id and  date_format(u.created_at,'%Y-%m-%d') = date_format(b.created_at,'%Y-%m-%d')
                               left join identity i on i.user_id = u.id
                               where date(u.created_at) <= str_to_date("{}","%Y-%m-%d")
                               and date(u.created_at) >= str_to_date("{}","%Y-%m-%d") group by u.id;
                               '''.format(today, before)
    account_center_db, _ = account_center_conn()
    # 注册用户
    # print("0000000",basesql1)
    register_num = pd.read_sql(basesql1, account_center_db)
    # register_num.rename(columns={'id':'register_num'},inplace=True)
    bill_db, _ = bill_conn()
    basesql4 = '''
                 select
                     user_id as remit_num, 
                     date(created_at) as remit_day 
                     from bill where date(created_at) < str_to_date("{}","%Y-%m-%d")
                     and date(created_at) > str_to_date("{}","%Y-%m-%d");
                     '''.format(today, before)
    # 放款人数
    remit_num = pd.read_sql(basesql4, bill_db)
    bill_db.close()
    account_center_db.close()
    if register_num.empty:
        return pd.DataFrame(
            **{'columns': ['register_num', 'day', 'auth_num', 'remit_num', 'application_pepolenum', "app_name"]})

    register_num["user_id_day"] = register_num[["register_num", "day"]].apply(lambda x: str(x[0]) + "," + str(x[1]),
                                                                              axis=1)
    if remit_num.empty:
        remit_num["user_id_day"] = pd.DataFrame(columns=['put'])
    else:
        remit_num["user_id_day"] = remit_num[["remit_num", "remit_day"]].apply(lambda x: str(x[0]) + "," + str(x[1]),
                                                                               axis=1)
    data = pd.merge(register_num, remit_num, how='left', on="user_id_day")
    # print(data[np.logical_not(pd.isnull(data['remit_num']))])
    enddata = data.groupby(["day", "app_name"])[
        ["register_num", "application_pepolenum", "auth_num", "remit_num"]].count()

    return enddata.reset_index()


# 历史注册/认证/数据，不基于天注册用户为准
def history_between_data(before, today):
    # 注册量
    registersql = '''
                           select
                             u.id as register_num,
                             u.app_name as app_name,
                             date(u.created_at) as day
                               from user_account u 
                               where date(u.created_at) <= str_to_date("{}","%Y-%m-%d")
                               and date(u.created_at) >= str_to_date("{}","%Y-%m-%d");
                               '''.format(today, before)
    account_center_db, _ = account_center_conn()
    # 注册用户
    register_num = pd.read_sql(registersql, account_center_db)
    authsql = """select user_id as auth_num,app_name,date(created_at) as day from 
                    identity where date(created_at) <= str_to_date("{}","%Y-%m-%d")
                    and date(created_at) >= str_to_date("{}","%Y-%m-%d");""".format(today, before)
    # 认证用户
    auth_num = pd.read_sql(authsql, account_center_db)

    # 申请人数
    bill_db, _ = bill_conn()
    appsql = """select id as app_num,app_name,date(created_at) as day from 
                    application where date(created_at) <= str_to_date("{}","%Y-%m-%d")
                    and date(created_at) >= str_to_date("{}","%Y-%m-%d") group by user_id;""".format(today, before)
    app_num = pd.read_sql(appsql, bill_db)
    remitsql = '''
                 select
                     user_id as remit_num,app_name, 
                     date(created_at) as day 
                     from bill where date(created_at) < str_to_date("{}","%Y-%m-%d")
                     and date(created_at) > str_to_date("{}","%Y-%m-%d");
                     '''.format(today, before)
    # 放款人数
    remit_num = pd.read_sql(remitsql, bill_db)
    bill_db.close()
    account_center_db.close()
    if not register_num.empty:
        register_num = register_num.groupby(["day", "app_name"])[["register_num"]].count().reset_index()
        register_num["app_name_day"] = register_num[["app_name", "day"]].apply(lambda x: str(x[0]) + "," + str(x[1]),
                                                                               axis=1)
    else:
        register_num = register_num.reindex(columns=['register_num', "day", "app_name", 'app_name_day'])
    if not auth_num.empty:
        auth_num = auth_num.groupby(["day", "app_name"])[["auth_num"]].count().reset_index()
        auth_num["app_name_day"] = auth_num[["app_name", "day"]].apply(lambda x: str(x[0]) + "," + str(x[1]), axis=1)
    else:
        auth_num = auth_num.reindex(columns=['auth_num', "day", "app_name", 'app_name_day'])
    if not app_num.empty:
        app_num = app_num.groupby(["day", "app_name"])[["app_num"]].count().reset_index()
        app_num["app_name_day"] = app_num[["app_name", "day"]].apply(lambda x: str(x[0]) + "," + str(x[1]), axis=1)
    else:
        app_num = app_num.reindex(columns=['app_num', "day", "app_name", 'app_name_day'])
        # print(app_num)
    if not remit_num.empty:
        remit_num = remit_num.groupby(["day", "app_name"])[["remit_num"]].count().reset_index()
        # print(remit_num)
        remit_num["app_name_day"] = remit_num[["app_name", "day"]].apply(lambda x: str(x[0]) + "," + str(x[1]), axis=1)
    else:
        remit_num = remit_num.reindex(columns=['remit_num', "day", "app_name", 'app_name_day'])
    data = pd.merge(register_num, auth_num, how='outer', on="app_name_day")
    data = pd.merge(data, app_num, how='outer', on="app_name_day")
    data = pd.merge(data, remit_num, how='outer', on="app_name_day")

    return data.fillna(0)[["register_num", "app_num", "auth_num", "remit_num", "app_name_day"]]


# 注册、认证、申贷、放款人数统计比率
@get('/api/datachart/rate/<whereday>')
def rate(whereday, op):
    if whereday == '1':
        startmon = date.today() - timedelta(days=date.today().day - 1)
        endmon = startmon + relativedelta(months=1)
    else:
        thedate = date(*[int(i) for i in whereday.split('-')])
        startmon = thedate
        endmon = startmon + relativedelta(months=1)
    sql = 'select day,register_num,  auth_num,  application_num,  remit_num,application_pepolenum from regist_auth_app where day >= str_to_date("{}","%Y-%m-%d %H:%i:%s") and day < str_to_date("{}","%Y-%m-%d")'.format(
        startmon, endmon)
    marketsys_db, _ = marketsys_conn()
    basedata = pd.read_sql(sql, marketsys_db).sort_values('day')
    if basedata.empty:
        return
    basedata['auth_rate'] = None
    basedata['remit_rate'] = None
    basedata['app_rate'] = None
    basedata['auth_rate'] = basedata[['auth_num', 'register_num']].apply(
        lambda x: int(round(0 if x['register_num'] == 0 else x['auth_num'] / x['register_num'], 3) * 1000), axis=1)
    basedata['remit_rate'] = basedata[['remit_num', 'application_num']].apply(
        lambda x: int(round(0 if x['application_num'] == 0 else x['remit_num'] / x['application_num'], 3) * 1000),
        axis=1)
    basedata['app_rate'] = basedata[['application_pepolenum', 'register_num']].apply(
        lambda x: int(round(0 if x['register_num'] == 0 else x['application_pepolenum'] / x['register_num'], 3) * 1000),
        axis=1)
    basedata = basedata.set_index('day')
    senddict = dict()
    senddict.update(basedata.to_dict(orient='list'))
    senddict['xaxis'] = [str(i).split(' ')[0] for i in basedata.index.to_list()]
    senddict['tablename'] = '注册转化率/借款率/放款率'
    marketsys_db.close()
    return senddict


@get('/api/datachart/remit_repay/<whereday>')
def remit_repay(whereday, op):
    # '1' 默认为今天
    if whereday == '1':
        startmon = date.today() - timedelta(days=date.today().day - 1)
        endmon = startmon + relativedelta(months=1)
    else:
        thedate = date(*[int(i) for i in whereday.split('-')])
        startmon = thedate
        endmon = startmon + relativedelta(months=1)

    if date.today() < startmon:
        return

    remit_sql = 'select user_id,amount,created_at from remittance where status=2 and created_at < "{}" and created_at > "{}"'.format(
        endmon, startmon)
    redb, remit_cursor = remittance_conn()
    # 放款数据
    remit_data = pd.read_sql(remit_sql, redb)
    repay_sql = 'select user_id,amount,created_at from repayment_history where history_type != 1 and history_type!=4 and created_at < "{}" and created_at > "{}"'.format(
        endmon, startmon)
    bill_sub_sql = 'select user_id,principal+if(finished_at = null,if(now() > origin_due_at-(TIMESTAMPDIFF(day, created_at, origin_due_at ) / ifnull(stage_num, 1)),interest,0),interest_paid)+if(late_fee>0,late_fee+20000,0)-principal_paid-late_fee_paid-interest_paid as due_amount,due_at from bill_sub where overdue_days>0 and due_at < "{}" and due_at > "{}"'.format(
        endmon, startmon)
    bidb, repay_cursor = bill_conn()
    # 还款数据
    bidb_data = pd.read_sql(repay_sql, bidb)
    # 逾期数据
    due_data = pd.read_sql(bill_sub_sql, bidb)

    if not bidb_data.empty:
        bidb_data["created_at"] = bidb_data["created_at"].map(lambda x: str(x)[:10])
        alldata = bidb_data.groupby("created_at")[["amount"]].sum()
        alldata.rename(columns={"amount": "repay_amount"}, inplace=True)
        alldata['repay_num'] = bidb_data.groupby("created_at")[["user_id"]].count()
    else:
        alldata = pd.DataFrame(**{'columns': ['repay_amount', 'repay_num']})
    index = alldata.index.tolist()
    # 这个月缺损的天
    themonth = []
    start = startmon
    end = endmon
    while start < end:
        look = str(start)
        if look in index:
            start += timedelta(days=1)
            continue
        else:
            themonth.append(str(start))
            start += timedelta(days=1)
    for ind in themonth:
        new_row = pd.DataFrame(**{'columns': ['repay_amount', 'repay_num'],
                                  'data': [[0, 0]], 'index': [ind]})
        alldata = alldata.append(new_row)

    if not remit_data.empty:
        remit_data["created_at"] = remit_data["created_at"].map(lambda x: str(x)[:10])
        collect = remit_data.groupby("created_at")[["user_id"]].count()
        collect.rename(columns={"user_id": "remit_num"}, inplace=True)
        collect['remit_amount'] = remit_data.groupby("created_at")["amount"].sum()
        alldata = alldata.join(collect).fillna(0)
    else:
        alldata['remit_num'] = pd.DataFrame(**{'columns': ['remit_num'],
                                               'data': [[0]], 'index': alldata.index})
        alldata['remit_amount'] = pd.DataFrame(**{'columns': ['remit_amount'],
                                                  'data': [[0]], 'index': alldata.index})
    if not due_data.empty:
        due_data["due_at"] = due_data["due_at"].map(lambda x: str(x)[:10])
        collect = due_data.groupby("due_at")[["user_id"]].count()
        collect.rename(columns={"user_id": "due_num"}, inplace=True)
        collect['due_amount'] = due_data.groupby("due_at")["due_amount"].sum()
        alldata = alldata.join(collect).fillna(0)
    else:
        alldata['due_num'] = pd.DataFrame(**{'columns': ['remit_num'],
                                             'data': [[0]], 'index': alldata.index})
        alldata['due_amount'] = pd.DataFrame(**{'columns': ['remit_amount'],
                                                'data': [[0]], 'index': alldata.index})
    alldata.sort_index(inplace=True)

    # 获取历史金额
    if date.today() < endmon:
        query = RemitRepayDue.select().where(RemitRepayDue.day >= startmon, RemitRepayDue.day < date.today()).dicts()
        linenum = len(query)
        if linenum == 0:
            remit_amount_sql = 'select sum(amount) from remittance where created_at < "{}"'.format(date.today())
            repay_amount_sql = 'select sum(amount) from repayment_history where created_at < "{}"'.format(date.today())
            due_amount_sql = 'select sum(principal+if(finished_at = null,if(now() > origin_due_at-(TIMESTAMPDIFF(day, created_at, origin_due_at ) / ifnull(stage_num, 1)),interest,0),interest_paid)+if(late_fee>0,late_fee+20000,0)-principal_paid-late_fee_paid-interest_paid) from bill_sub where created_at <"{}"'.format(
                str(date.today()))
            remit_cursor.execute(remit_amount_sql)
            remit_amount = remit_cursor.fetchall()[0][0]
            repay_cursor.execute(repay_amount_sql)
            repay_amount = repay_cursor.fetchall()[0][0]
            repay_cursor.execute(due_amount_sql)
            due_amount = repay_cursor.fetchall()[0][0]
            RemitRepayDue.create(remit_amount=remit_amount, repay_amount=repay_amount,
                                 due_amount=due_amount, day=(date.today() - timedelta(days=1)))
            alldata.loc[str(date.today() - timedelta(days=1)), 'sum_remit_amount'] = remit_amount
            alldata.loc[str(date.today() - timedelta(days=1)), 'sum_repay_amount'] = repay_amount
            alldata.loc[str(date.today() - timedelta(days=1)), 'sum_due_amount'] = due_amount
        else:
            for the_one in query:
                remit_amount = the_one["remit_amount"]
                repay_amount = the_one["repay_amount"]
                due_amount = the_one["due_amount"]
                day = str(the_one["day"])[:10]
                # print(day,"-------")
                alldata.loc[day, 'sum_remit_amount'] = remit_amount
                alldata.loc[day, 'sum_repay_amount'] = repay_amount
                alldata.loc[day, 'sum_due_amount'] = due_amount

            thelasttime = query[-1]["day"] + timedelta(days=1)
            while thelasttime < datetime.today():
                thetime = thelasttime
                thelasttime += timedelta(days=1)
                remit_amount_sql = 'select sum(amount) from remittance where created_at < "{}"'.format(thelasttime)
                repay_amount_sql = 'select sum(amount) from repayment_history where created_at < "{}"'.format(
                    thelasttime)
                due_amount_sql = 'select sum(principal+if(finished_at = null,if(now() > origin_due_at-(TIMESTAMPDIFF(day, created_at, origin_due_at ) / ifnull(stage_num, 1)),interest,0),interest_paid)+if(late_fee>0,late_fee+20000,0)-principal_paid-late_fee_paid-interest_paid) from bill_sub where created_at <"{}"'.format(
                    thelasttime)
                remit_cursor.execute(remit_amount_sql)
                remit_amount = remit_cursor.fetchall()[0][0]
                repay_cursor.execute(repay_amount_sql)
                repay_amount = repay_cursor.fetchall()[0][0]
                repay_cursor.execute(due_amount_sql)
                due_amount = repay_cursor.fetchall()[0][0]
                if thelasttime < datetime.today():
                    RemitRepayDue.create(remit_amount=remit_amount, repay_amount=repay_amount,
                                         due_amount=due_amount, day=thetime)
                alldata.loc[thetime, 'sum_remit_amount'] = remit_amount
                alldata.loc[thetime, 'sum_repay_amount'] = repay_amount
                alldata.loc[thetime, 'sum_due_amount'] = due_amount

    else:
        query = RemitRepayDue.select().where(RemitRepayDue.day >= startmon, RemitRepayDue.day < endmon).dicts()
        linenum = len(query)
        if linenum == 0:
            alldata['sum_remit_amount'] = pd.DataFrame(**{'columns': ['remit_amount'],
                                                          'data': [[0]], 'index': alldata.index})
            alldata['sum_repay_amount'] = pd.DataFrame(**{'columns': ['repay_amount'],
                                                          'data': [[0]], 'index': alldata.index})
            alldata['sum_due_amount'] = pd.DataFrame(**{'columns': ['due_amount'],
                                                        'data': [[0]], 'index': alldata.index})
        else:
            for the_one in query:
                remit_amount = the_one["remit_amount"]
                repay_amount = the_one["repay_amount"]
                due_amount = the_one["due_amount"]
                day = str(the_one["day"])[:10]
                alldata.loc[day, 'sum_remit_amount'] = remit_amount
                alldata.loc[day, 'sum_repay_amount'] = repay_amount
                alldata.loc[day, 'sum_due_amount'] = due_amount

    alldata = alldata.fillna(value=0).applymap(lambda x: str(int(x) if type(x) == float else x))

    if date.today() < endmon:
        alldata = alldata.loc[:str(date.today() - timedelta(days=1))]
    alldata['sum_remit_amount'] = alldata['sum_remit_amount'].map(lambda x: int(int(x) / 14000))
    alldata['sum_repay_amount'] = alldata['sum_repay_amount'].map(lambda x: int(int(x) / 14000))
    alldata['sum_due_amount'] = alldata['sum_due_amount'].map(lambda x: int(int(x) / 14000))
    alldata['remit_amount'] = alldata['remit_amount'].map(lambda x: int(int(x) / 14000))
    alldata['repay_amount'] = alldata['repay_amount'].map(lambda x: int(int(x) / 14000))
    alldata['due_amount'] = alldata['due_amount'].map(lambda x: int(int(x) / 14000))
    alldata.rename(index={i: i[-2:] for i in alldata.index.tolist()}, inplace=True)
    send_data = alldata.to_dict(orient='list')
    send_data['xaxis'] = alldata.index.tolist()
    send_data['xaxis'][0] = str(startmon)
    send_data['table_name'] = "放款/还款/逾期数据"
    redb.close()
    bidb.close()
    return send_data


@get('/api/datachart/due_at/<whereday>/<app_name>')
def due_at(whereday, app_name, op):
    billdb, _ = bill_conn()
    if whereday == '1':
        startmon = date.today() - timedelta(days=date.today().day - 1)
        endmon = startmon + relativedelta(months=1)
    else:
        thedate = date(*[int(i) for i in whereday.split('-')])
        startmon = thedate
        endmon = startmon + relativedelta(months=1)
    if date.today() < startmon:
        return
    endmon = date.today() + timedelta(days=1) if date.today() < endmon else endmon
    # interest  利息是按期收取，即进入当期后，才会算利息
    # if(now() > b.origin_due_at-(TIMESTAMPDIFF(day, b.created_at, b.origin_due_at ) / ifnull(b.stage_num, 1),b.interest,0)
    sql = """select b.id,date(b.due_at) as day,if(b.finished_at <= b.due_at,1,0) as is_normal_repay,
    b.user_id,m.is_first_success_loan, 
    b.principal+if(b.finished_at = null,if(now() > b.origin_due_at-(TIMESTAMPDIFF(day, b.created_at, b.origin_due_at ) / ifnull(b.stage_num, 1)),b.interest,0),b.interest_paid)+if(b.late_fee>0,b.late_fee+20000,0)  as all_amount,
    if(b.finished_at <= b.due_at,b.principal_paid+b.late_fee_paid+b.interest_paid,0) as repay_amount  
    from bill_sub b left join bill m on m.id = b.bill_id where b.due_at < '{}' and b.due_at > '{}'""".format(endmon,
                                                                                                             startmon)
    data = pd.read_sql(sql, billdb)
    # #print(data)
    # data['isrepay'] = ((data.due_amount <=0 ) & (data.isrepay == 1)).map(lambda x:1 if x==True else 0)

    if app_name == 'all':
        ispay_amount_num = data.groupby(["day", "is_first_success_loan"])[
            ["is_normal_repay", 'all_amount', 'repay_amount']].sum()
        all_num = data.groupby(["day", "is_first_success_loan"])[["is_normal_repay"]].count()

        all_num.rename(columns={"is_normal_repay": "sum"}, inplace=True)

        senddata = pd.concat((all_num, ispay_amount_num), axis=1, join="inner")
        senddata = senddata.reset_index()
    else:
        # 获取app_name余userid对应数据
        app_useridsql = 'select user_id,app_name from identity'
        acount_db, _ = account_center_conn()
        appdata = pd.read_sql(app_useridsql, acount_db)
        data = pd.merge(data, appdata, how='left', on='user_id')
        ispay_amount_num = data.groupby(["day", "is_first_success_loan", 'app_name'])[
            ["is_normal_repay", 'all_amount', 'repay_amount']].sum()
        all_num = data.groupby(["day", "is_first_success_loan", 'app_name'])[["is_normal_repay"]].count()
        all_num.rename(columns={"is_normal_repay": "sum"}, inplace=True)
        senddata = pd.concat((all_num, ispay_amount_num), axis=1, join="inner")
        senddata = senddata.reset_index()
        senddata = senddata[senddata.app_name == app_name]
        acount_db.close()

    new_user = senddata[senddata.is_first_success_loan == 1]
    old_user = senddata[senddata.is_first_success_loan == 0]
    new_days = new_user['day'].values
    old_days = old_user['day'].values
    max_time = endmon
    min_time = startmon
    while min_time < max_time:
        if min_time not in old_days:
            new_row = pd.DataFrame(**{
                'columns': ['day', 'is_first_success_loan', 'sum', 'is_normal_repay', 'all_amount', 'repay_amount'],
                'data': [[min_time, 0, 0, 0, 0, 0]], 'index': [1]})
            old_user = old_user.append(new_row)

        if min_time not in new_days:
            new_row = pd.DataFrame(**{
                'columns': ['day', 'is_first_success_loan', 'sum', 'is_normal_repay', 'all_amount', 'repay_amount'],
                'data': [[min_time, 1, 0, 0, 0, 0]], 'index': [1]})
            new_user = new_user.append(new_row)
        min_time += timedelta(days=1)

    print(old_user)
    new_user['all_amount'] = new_user['all_amount'].map(lambda x: int(x / 14000))
    new_user['repay_amount'] = new_user['repay_amount'].map(lambda x: int(x / 14000))

    old_user['all_amount'] = old_user['all_amount'].map(lambda x: int(x / 14000))
    old_user['repay_amount'] = old_user['repay_amount'].map(lambda x: int(x / 14000))
    print(old_user)
    # 逾期数
    new_user['due'] = new_user[['sum', 'is_normal_repay']].apply(lambda x: x[0] - x[1], axis=1)
    old_user['due'] = old_user[['sum', 'is_normal_repay']].apply(lambda x: x[0] - x[1], axis=1)
    #DPTP.select().limit(100)
    # 未还款金额
    new_user['due_amount'] = new_user[['all_amount', 'repay_amount']].apply(lambda x: x[0] - x[1], axis=1)
    old_user['due_amount'] = old_user[['all_amount', 'repay_amount']].apply(lambda x: x[0] - x[1], axis=1)
    # 首逾率
    #new_user['new_first_duerate'] = new_user[['due_amount', 'all_amount']].apply(
       # lambda x: str(round(0 if x[1] == 0 else x[0] / x[1], 3)), axis=1)
    #old_user['old_first_duerate'] = old_user[['due_amount', 'all_amount']].apply(
       # lambda x: str(round(0 if x[1] == 0 else x[0] / x[1], 3)), axis=1)

    new_user = new_user.sort_values('day').drop(['is_first_success_loan'], axis=1)
    old_user = old_user.sort_values('day').drop(['is_first_success_loan'], axis=1)

    # 统计该月每日催收回款
    due_repay_sql = "select  date(b.finished_at) as day,b.user_id as bomber_num,m.is_first_success_loan,b.principal_paid+b.late_fee_paid+b.interest_paid as bomber_repay_amount  from bill_sub b left join bill m on m.id = b.bill_id where b.finished_at < '{}' and b.finished_at > '{}' and b.overdue_days > 0".format(
        endmon, startmon)
    due_repay = pd.read_sql(due_repay_sql, billdb)

    if app_name == 'all':
        due_repay['bomber_repay_amount'] = due_repay['bomber_repay_amount'].map(lambda x: int(x / 14000))
        due_repay_data = pd.concat((due_repay.groupby(['day', 'is_first_success_loan'])[['bomber_repay_amount']].sum(),
                                    due_repay.groupby(['day', 'is_first_success_loan'])[['bomber_num']].count()),
                                   axis=1)
        due_repay_data.reset_index(inplace=True)
    else:
        due_repay = pd.merge(due_repay, appdata, how='left', left_on='bomber_num', right_on='user_id')
        due_repay['bomber_repay_amount'] = due_repay['bomber_repay_amount'].map(lambda x: int(x / 14000))
        due_repay_data = pd.concat(
            (due_repay.groupby(['day', 'is_first_success_loan', 'app_name'])[['bomber_repay_amount']].sum(),
             due_repay.groupby(['day', 'is_first_success_loan', 'app_name'])[['bomber_num']].count()),
            axis=1)
        due_repay_data.reset_index(inplace=True)
        due_repay_data = due_repay_data[due_repay_data.app_name == app_name]

    if due_repay_data.empty:
        due_repay_data['bomber_repay_amount'] = []
    # print(due_repay_data)

    new_user_due_repay_data = due_repay_data[due_repay_data.is_first_success_loan == 1].drop('is_first_success_loan',
                                                                                             axis=1)
    old_user_due_repay_data = due_repay_data[due_repay_data.is_first_success_loan == 0].drop('is_first_success_loan',
                                                                                             axis=1)

    old_user = pd.merge(old_user, old_user_due_repay_data, how='left', on='day').fillna(0)
    new_user = pd.merge(new_user, new_user_due_repay_data, how='left', on='day').fillna(0)

    # #首逾率
    old_user['comeback_money_rate'] = old_user[['bomber_repay_amount', 'due_amount']].apply(
        lambda x: str(round(0 if x[1] == 0 else x[0] / x[1], 3)), axis=1)
    new_user['comeback_money_rate'] = new_user[['bomber_repay_amount', 'due_amount']].apply(
        lambda x: str(round(0 if x[1] == 0 else x[0] / x[1], 3)), axis=1)

    old_user = old_user.applymap(lambda x: str(int(x) if type(x) == float else x))
    new_user = new_user.applymap(lambda x: str(int(x) if type(x) == float else x))

    tosend = {}
    tosend['old'] = old_user.to_dict(orient="list")
    tosend['new'] = new_user.to_dict(orient="list")
    tosend['table_name'] = "逾期/回款数据"
    billdb.close()
    return tosend


@get('/api/datachart/tableintroduce')
def getcahrtintroduce(op):
    table1 = dict()
    table1['tablename'] = "注册/认证/申贷/放款人数"
    table1['introduce'] = """
    认证转化率 = 认证人数/注册人数
    放款转化率 = 放款人数/申贷人数
    借款转化率 = 申贷人数/注册人数
    """
    table2 = dict()
    table2['tablename'] = "放款/还款/逾期"
    table2['introduce'] = """
    放款金额：每天放款金额数目，放款数：每天放款笔数
    还款金额：每天还款金额数目，还款数：每天还款笔数
    逾期金额：每天逾期金额数目，逾期数：每天逾期笔数
    历史放款总额：到当天为止，整个项目放款金额数目
    历史还款总额：到当天为止，整个项目还款金额数目
    历史逾期总额：到当天为止，整个项目逾期金额数目
    """

    table3 = dict()
    table3['tablename'] = "新老用户数据"
    table3['introduce'] = """
    新用户：第一次借款用户，老用户：非第一次借款用户

    到期总额 = 正常还款金额 + 逾期金额
    到期总额：当日到期总金额数目
    逾期金额：当日到期且逾期总金额数目
    正常还款金额：当日到期且还款总金额数目

    到期用户数量 = 正常还款数量 + 逾期数量
    到期用户数量：当日到期用户总数
    正常还款数量：正常还款的用户总数
    逾期数量：逾期用户总数

    用户首逾率 = 逾期金额（当日到期且逾期总金额数目）/ 到期总额（当日到期总金额数目）
    用户回款率 = 当日催回金额 / 当日逾期金额
    """
    table4 = dict()
    table4['tablename'] = "DTP逾期率"
    table4['introduce'] = """
    以下数据基于bill_sub表
    DPD1计数比 = 当天逾期1天人数/当天到期人数
    DPD1-3计数比 = 当天逾期1天到3天人数/最近3天到期人数
    DPD1-15计数比 = 当天逾期1天到15天人数/最近15天到期人数
    DPD1本金比 = 当天逾期1天本金/当天到期本金
    DPD1-3本金比 = 当天逾期1天到15天本金/最近15天到期本金
    DPD1-15本金比 = 当天逾期1天到15天本金/最近15天到期本金
    """
    

    return [table1, table2, table3,table4]


@get('/api/dptp/agafa')
def dptp():
    today = date.today()
    yesterday =  today - timedelta(days=1)
    third_day =  today - timedelta(days=4)
    fifteen_day =  today - timedelta(days=16)
    dptp1_sql = 'select principal,due_at,overdue_days,ifnull(stage_num,"IkiModel") as app_name from bill_sub where due_at = "{} 22:00:00"'.format(yesterday)
    dptp1_3_sql = 'select principal,due_at,overdue_days,ifnull(stage_num,"IkiModel") as app_name from bill_sub where due_at <= "{} 22:00:00" and due_at > "{} 22:00:00"'.format(yesterday,third_day)
    dptp1_15_sql = 'select principal,due_at,overdue_days,ifnull(stage_num,"IkiModel") as app_name from bill_sub where due_at <= "{} 22:00:00" and due_at > "{} 22:00:00"'.format(yesterday,fifteen_day)
    billdb, _ = bill_conn()
    dptp1_15 = pd.read_sql(dptp1_15_sql,billdb)
    dptp1_15['app_name'] = dptp1_15['app_name'].map(lambda x:'IkiModel' if x =='IkiModel' else 'IkiDana')
    dptp1 = pd.read_sql(dptp1_sql,billdb)
    dptp1['app_name'] = dptp1['app_name'].map(lambda x: 'IkiModel' if x == 'IkiModel' else 'IkiDana')
    dptp1_3 = pd.read_sql(dptp1_3_sql,billdb)
    dptp1_3['app_name'] = dptp1_3['app_name'].map(lambda x: 'IkiModel' if x == 'IkiModel' else 'IkiDana')
    billdb.close()
    for i in ["IkiDana","IkiModel"]:
        dptp1_principal_sum = dptp1[dptp1.app_name == i]['principal'].sum()
        dptp1_3_principal_sum = dptp1_3[dptp1_3.app_name == i]['principal'].sum()
        dptp1_15_principal_sum = dptp1_15[dptp1_15.app_name == i]['principal'].sum()
        dptp1_pepole_sum = dptp1[dptp1.app_name == i]['overdue_days'].count()
        dptp1_3_pepole_sum = dptp1_3[dptp1_3.app_name == i]['overdue_days'].count()
        dptp1_15_pepole_sum = dptp1_15[dptp1_15.app_name == i]['overdue_days'].count()
        if dptp1_principal_sum == 0:
            dptp1_rate = 0.0
        else:
            dptp1_rate =  round( dptp1[(dptp1.overdue_days > 0) & (dptp1.app_name == i)]['principal'].sum()
                            /dptp1_principal_sum*100,1)
        if dptp1_3_principal_sum == 0:
            dptp1_3_rate = 0.0
        else:
            dptp1_3_rate =  round(dptp1_3[(dptp1_3.overdue_days > 0)  & (dptp1_3.app_name == i)]['principal'].sum()
                              /dptp1_3_principal_sum*100,1)
        if dptp1_15_principal_sum == 0:
            dptp1_15_rate = 0.0
        else:
            dptp1_15_rate =  round(dptp1_15[(dptp1_15.overdue_days > 0) & (dptp1_15.app_name == i)]['principal'].sum()
                               /dptp1_15_principal_sum*100,1)
        if dptp1_pepole_sum == 0:
            dptp1_num == 0.0
        else:
            dptp1_num =  round(dptp1[(dptp1.overdue_days > 0) & (dptp1.app_name == i)]['overdue_days'].count()/
                           dptp1_pepole_sum*100,1)
        if dptp1_3_pepole_sum == 0:
            dptp1_3_num == 0.0
        else:
            dptp1_3_num =  round(dptp1_3[(dptp1_3.overdue_days > 0) & (dptp1_3.app_name == i)]['overdue_days'].count()
                             /dptp1_3_pepole_sum*100,1)
        if dptp1_15_pepole_sum == 0:
            dptp1_15_num = 0.0
        else:
            dptp1_15_num =  round(dptp1_15[(dptp1_15.overdue_days > 0)& (dptp1_15.app_name == i)]['overdue_days'].count()
                              /dptp1_15_pepole_sum*100,1)
        print({'dptp1_rate':0.0 if np.isnan(dptp1_rate) else float(dptp1_rate),
                    'dptp1_3_rate':0.0 if np.isnan(dptp1_3_rate) else float(dptp1_3_rate),
                    'dptp1_15_rate':0.0 if np.isnan(dptp1_15_rate) else float(dptp1_15_rate),
                    'dptp1_num':0.0 if np.isnan(dptp1_num) else float(dptp1_num),
                    'dptp1_3_num':0.0 if np.isnan(dptp1_3_num) else float(dptp1_3_num),
                    'dptp1_15_num':0.0 if np.isnan(dptp1_15_num) else float(dptp1_15_num),'app_name':i})
        DPD.insert({'dptp1_rate':0.0 if np.isnan(dptp1_rate) else float(dptp1_rate),
                    'dptp1_3_rate':0.0 if np.isnan(dptp1_3_rate) else float(dptp1_3_rate),
                    'dptp1_15_rate':0.0 if np.isnan(dptp1_15_rate) else float(dptp1_15_rate),
                    'dptp1_num':0.0 if np.isnan(dptp1_num) else float(dptp1_num),
                    'dptp1_3_num':0.0 if np.isnan(dptp1_3_num) else float(dptp1_3_num),
                    'dptp1_15_num':0.0 if np.isnan(dptp1_15_num) else float(dptp1_15_num),
                    'app_name':i,'created_at':str(yesterday)}).execute()
    return


@get('/api/dptp_rate/<whereday>')
def dptp_rate(whereday):
    marketdb, _ = marketsys_conn()
    if whereday == '1':
        startmon = date.today() - timedelta(days=date.today().day - 1)
        endmon = startmon + relativedelta(months=1)
    else:
        thedate = date(*[int(i) for i in whereday.split('-')])
        startmon = thedate
        endmon = startmon + relativedelta(months=1)
    sql = 'select * from dpd where created_at < "{}" and created_at >"{}"'.format(endmon,startmon)
    data = pd.read_sql(sql,marketdb)
    data['created_at'] = data['created_at'].map(lambda x:str(x))
    IkiDana_data = data[data.app_name=='IkiDana'].sort_values('created_at')
    IkiModela_data = data[data.app_name=='IkiModel'].sort_values('created_at')
    marketdb.close()
    return {'IkiDana':IkiDana_data.to_dict(orient="list"),'IkiModel':IkiModela_data.to_dict(orient="list")}