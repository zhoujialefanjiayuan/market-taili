import os
import re

from bottle import post, abort, get, request, response, static_file
import pandas as pd
from marketsys.db import bill_conn, remittance_conn, account_center_conn
from marketsys.plugins import packing_plugin

table_dict = {"user_account": account_center_conn,
              "identity": account_center_conn,
              "bill": bill_conn,
              "application": bill_conn,
              "repayment_history": bill_conn,
              "remittance": remittance_conn}

everypage = 100

@get('/api/columns/<table>')
def get_columns(table,op):
    conn = table_dict.get(table)
    if conn == None:
        abort(500,"table don't exist")
    db,cursor = conn()
    sql = 'describe ' + table
    cursor.execute(sql)
    columns_data = cursor.fetchall()
    columns = []
    for i in columns_data:
        columns.append(i[0])
    db.close()
    return  {"columns":columns}

#'-10未申请，0-10审核中、20用户放弃申请、30审核拒绝、40-50放款中、60放款失败、
# 70还款中、80正常还款、90逾期未还款、100逾期已还款、110坏账、120还清、130部分还'
user_status={
    "-10":"未申请",
    "20":"未申请",
    "0":"审核中",
    "10":"审核中",
    "30":"审核拒绝",
    "40":"放款中",
    "50":"放款中",
    "60":"放款失败",
    "70":"还款中",
    "80":"正常还款",
    "90":"逾期未还款",
    "110":"逾期未还款",
    "100":"逾期已还款",
    "120":"逾期已还款",
    "130":"逾期已还款",
}
@post('/api/datasearch/')
def datasearch():
    params = request.json
    table = params['table']
    conditions = params['conditions']
    primary = params['primary']
    page = int(params['page'])
    type = params['type']
    conn = table_dict.get(table)
    if conn == None:
        abort(500, "table don't exist")
    db, cursor = conn()
    #查看字段类型
    sql_columns = 'describe ' + table
    cursor.execute(sql_columns)
    columns_data = cursor.fetchall()
    all_columns = []
    bit_columns = []
    for i in columns_data:
        all_columns.append(i[0])
        if i[1] == "bit(1)":
            bit_columns.append(i[0])
    #选择数据
    allnum = 0
    if len(conditions) == 1 and str(conditions[0]['file']) == "":
        searchsql = 'select * from {} limit {},{}'.format(table, (page-1)*everypage,everypage)
        cursor.execute('select count({}) from {} '.format(primary,table))
        thenum = cursor.fetchall()
        if len(thenum) != 0:
            allnum = thenum[0][0]
    else:
        basesql = 'select * from ' + table + ' '
        for condition in conditions:
            start = str(condition['start'])
            file = str(condition['file'])
            if file == "":
                continue
            end = str(condition['end'])
            if start == "" and  end == "":
                abort(500,'need input filter params')
            elif start == "" and end != "":
                if 'where' in basesql:
                    basesql += ' and {file} <= "{end}" '.format(file=file,end=end)
                else:
                    basesql += ' where {file} <= "{end}" '.format(file=file, end=end)
            elif start != "" and end == "":
                 if start[0] == ">":
                     if 'where' in basesql:
                         basesql += ' and {file} > "{start}" '.format(file=file, start=start[1:])
                     else:
                         basesql += ' where {file} > "{start}" '.format(file=file, start=start[1:])
                 elif start[0] == "<":
                     if 'where' in basesql:
                         basesql += ' and {file} < "{start}" '.format(file=file, start=start[1:])
                     else:
                         basesql += ' where {file} < "{start}" '.format(file=file, start=start[1:])
                 else:
                     if 'where' in basesql:
                         if file in bit_columns:
                             basesql += ' and {file} = "{start}" '.format(file=file, start=start)
                         else:
                            basesql += ' and {file} like "%{start}%" '.format(file=file, start=start)
                     else:
                         if file in bit_columns:
                             basesql += ' where {file} = "{start}" '.format(file=file, start=start)
                         else:
                             basesql += ' where {file} like "%{start}%" '.format(file=file, start=start)
            else:
                if 'where' in basesql:
                    basesql += ' and {file} >= "{start}" and {file} <= "{end}" '.format(file = file,start = start,end = end)
                else:
                    basesql += ' where {file} >= "{start}" and {file} <= "{end}" '.format(file=file, start=start, end=end)
        searchsql = basesql
    if len(type) != 0:
        orderfile = ".".join(type)
        if orderfile == "created":
            searchsql = searchsql.replace("*","count({}) as num,date_format(created_at,'%Y-%m-%d') as created".format(primary)).split("limit")[0] \
                        + " group by created "
        elif orderfile == "app_name":
            if "app_name" in all_columns:
                searchsql = searchsql.replace("*", "count({}) as num,app_name".format(
                                                  primary)).split("limit")[0] + " group by app_name"
            else:
                abort(500,"the table don't have the field named app_name ")
        else:
            if "app_name" in all_columns:
                searchsql = searchsql.replace("*", "count({}) as num,date_format(created_at,'%Y-%m-%d') as created,app_name".format(
                                                  primary)).split("limit")[0] + " group by created,app_name"
            else:
                abort(500,"the table don't have the fieled named app_name ")

        #按照主键倒序
        searchsql += ' order by %s desc'%primary

        print("内部", searchsql)
        cursor.execute(searchsql)
        send_data = cursor.fetchall()
        result = []
        for send in send_data[(page - 1) * everypage:page * everypage]:
            obj={}
            obj["num"] = send[0]
            obj[type[0]] = send[1]
            try:
                obj[type[1]] = send[2]
                result.append(obj)
            except:
                result.append(obj)
        return {'result': result, 'total': len(send_data), 'every': everypage,"type":1}

    # 按照主键倒序
    if "limit" not in searchsql:
        searchsql += ' order by %s desc'%primary
    else:
        if table == 'user_account':
            searchsql = 'select * from {}  order by created_at desc '
        else:
            searchsql = 'select * from {}  order by created_at desc limit {},{}'.format(table, (page-1)*everypage,everypage)

    print("外部", searchsql)
    data = pd.read_sql(searchsql,db)
    total = allnum if allnum > data.shape[0] else data.shape[0]
    if total == 0 :
        return {'result':[],'total':0,'every':everypage}

    #确定用户状态
    if table == 'user_account':
        # 获取application是否申请借款
        userstatus = params['user_status']
        billdb, _ = bill_conn()
        application_sql = "select user_id as id,status as user_status,max(id) from application group by user_id"
        application = pd.read_sql(application_sql,billdb)
        data = pd.merge(data, application, how='left', on='id').fillna('-10').drop(columns=['max(id)'],axis=1)
        if userstatus !="":
            userstatus = userstatus.split(',')
            data = data[data.user_status.isin(userstatus)]

    if "limit" not in searchsql:
        senddata = data.loc[(page - 1) * everypage:page * everypage]
    else:
        senddata = data
    for column in bit_columns:
        senddata[column] = senddata[column].apply(lambda x: 1 if x== b'\x01' else 0 )
    senddata = senddata.applymap(lambda x:str(x))
    if table == 'user_account':
        senddata['user_status'] = senddata['user_status'].map(user_status)
    senddata = senddata.to_dict(orient='index').values()
    db.close()
    return {'result':[i for i in senddata],'total':total,'every':everypage,"type":0}


@get('/api/ddl/<table>')
def get_columns(table,op):
    conn = table_dict.get(table)
    if conn == None:
        abort(500,"table don't exist")
    db,cursor = conn()
    sql = 'show create table '+ table
    cursor.execute(sql)
    ddl_data = cursor.fetchall()[0][1].split(";")[0]
    lines = ddl_data.split('\n')
    showstr = ''
    for line in lines:
        feiled = re.match('`.*`',line.strip())
        if feiled:
            feiled = feiled.group()
        else:
            continue
        if len(feiled) == 0:
            continue
        comment = re.findall("COMMENT \'.*\'",line)
        comment = '' if len(comment) == 0 else comment[0]
        showstr += feiled + ' : ' + comment + '\n'
    db.close()
    return  {"ddl":showstr}

@get('/api/userdetail/<userid>')
def userdetail(userid,op):
    db,cursor = account_center_conn()
    data_dict = {'bankcard':{},
                 'contact':{},
                 'job':{},
                 'profile':{},
                 'identity':{},
                 }
    for j in data_dict:
        # 查看字段类型
        sql_columns = 'describe ' + j
        cursor.execute(sql_columns)
        columns_data = cursor.fetchall()
        all_columns = []
        time_columns = []
        bit_columns = []
        for i in columns_data:
            all_columns.append(i[0])
            if i[1] == "datetime":
                time_columns.append(i[0])
            if i[1] == "bit(1)":
                bit_columns.append(i[0])
        sql = 'select * from %s where user_id =%s'%(j,userid)
        cursor.execute(sql)
        fetch = cursor.fetchall()
        if len(fetch) == 0:
            data_dict[j]= dict(zip(all_columns, ['']*len(all_columns)))
        else:
            for one in fetch:
                obj = dict(zip(all_columns, [str(o) for o in one]))
                for t in time_columns:
                    obj[t] = str(obj[t])
                data_dict[j]=obj
                break
    db.close()
    return data_dict


@get('/api/billdetail/<userid>/<bill_id>')
def billdetail(userid,bill_id,op):
    db,cursor = bill_conn()
    print(type(userid))
    sql_columns = 'describe bill_sub'
    cursor.execute(sql_columns)
    columns_data = cursor.fetchall()
    all_columns = []
    time_columns = []
    bit_columns = []
    for i in columns_data:
        all_columns.append(i[0])
        if i[1] == "datetime":
            time_columns.append(i[0])
        if  "decimal" in i[1]:
            bit_columns.append(i[0])
    sql = 'select *  from bill_sub where user_id ='+ userid
    print(sql)
    cursor.execute(sql)
    fetch = cursor.fetchall()
    billdetail_data = []
    historybill_data = []
    for i in fetch:
        obj = dict(zip(all_columns, i))
        for k in obj:
            if k in bit_columns:
                obj[k] = float(obj[k])
            else:
                obj[k] = str(obj[k])
        if bill_id == str(obj["bill_id"]):
            billdetail_data.append(obj)
        else:
            historybill_data.append(obj)
    db.close()
    return  {'billdetail_data':billdetail_data,'historybill_data':historybill_data}


@post('/api/downloaddata/datasearch',skip=[packing_plugin])
def datasearch(op):
    params = request.json
    table = params['table']
    conditions = params['conditions']
    primary = params['primary']
    page = int(params['page'])
    type = params['type']
    conn = table_dict.get(table)
    if conn == None:
        abort(500, "table don't exist")
    db, cursor = conn()
    #查看字段类型
    sql_columns = 'describe ' + table
    cursor.execute(sql_columns)
    columns_data = cursor.fetchall()
    all_columns = []
    bit_columns = []
    for i in columns_data:
        all_columns.append(i[0])
        if i[1] == "bit(1)":
            bit_columns.append(i[0])
    #选择数据
    allnum = 0
    if len(conditions) == 1 and str(conditions[0]['file']) == "":
        searchsql = 'select * from {} limit {}'.format(table, page*everypage)
        cursor.execute('select count({}) from {} '.format(primary,table))
        thenum = cursor.fetchall()
        if len(thenum) != 0:
            allnum = thenum[0][0]
    else:
        basesql = 'select * from ' + table + ' '
        for condition in conditions:
            start = str(condition['start'])
            file = str(condition['file'])
            if file == "":
                continue
            end = str(condition['end'])
            if start == "" and  end == "":
                abort(500,'need input filter params')
            elif start == "" and end != "":
                if 'where' in basesql:
                    basesql += ' and {file} <= "{end}" '.format(file=file,end=end)
                else:
                    basesql += ' where {file} <= "{end}" '.format(file=file, end=end)
            elif start != "" and end == "":
                 if start[0] == ">":
                     if 'where' in basesql:
                         basesql += ' and {file} > "{start}" '.format(file=file, start=start[1:])
                     else:
                         basesql += ' where {file} > "{start}" '.format(file=file, start=start[1:])
                 elif start[0] == "<":
                     if 'where' in basesql:
                         basesql += ' and {file} < "{start}" '.format(file=file, start=start[1:])
                     else:
                         basesql += ' where {file} < "{start}" '.format(file=file, start=start[1:])
                 else:
                     if 'where' in basesql:
                         if file in bit_columns:
                             basesql += ' and {file} = "{start}" '.format(file=file, start=start)
                         else:
                            basesql += ' and {file} like "%{start}%" '.format(file=file, start=start)
                     else:
                         if file in bit_columns:
                             basesql += ' where {file} = "{start}" '.format(file=file, start=start)
                         else:
                             basesql += ' where {file} like "%{start}%" '.format(file=file, start=start)
            else:
                if 'where' in basesql:
                    basesql += ' and {file} >= "{start}" and {file} <= "{end}" '.format(file = file,start = start,end = end)
                else:
                    basesql += ' where {file} >= "{start}" and {file} <= "{end}" '.format(file=file, start=start, end=end)
        searchsql = basesql
    if len(type) != 0:
        orderfile = ".".join(type)
        if orderfile == "created":
            searchsql = searchsql.replace("*","count({}) as num,date_format(created_at,'%Y-%m-%d') as created".format(primary)).split("limit")[0] \
                        + " group by created "
        elif orderfile == "app_name":
            if "app_name" in all_columns:
                searchsql = searchsql.replace("*", "count({}) as num,app_name".format(
                                                  primary)).split("limit")[0] + " group by app_name"
            else:
                abort(500,"the table don't have the field named app_name ")
        else:
            if "app_name" in all_columns:
                searchsql = searchsql.replace("*", "count({}) as num,date_format(created_at,'%Y-%m-%d') as created,app_name".format(
                                                  primary)).split("limit")[0] + " group by created,app_name"
            else:
                abort(500,"the table don't have the fieled named app_name ")

        #按照主键倒序
        searchsql += ' order by %s desc'%primary

        print("内部", searchsql)
        cursor.execute(searchsql)
        send_data = cursor.fetchall()
        result = []
        for send in send_data[(page - 1) * everypage:page * everypage]:
            obj={}
            obj["num"] = send[0]
            obj[type[0]] = send[1]
            try:
                obj[type[1]] = send[2]
                result.append(obj)
            except:
                result.append(obj)
        return {'result': result, 'total': len(send_data), 'every': everypage,"type":1}

    # 按照主键倒序

    if "limit" not in searchsql:
        searchsql += ' order by %s desc'%primary
    else:
        searchsql = 'select * from {}  order by created_at desc limit {}'.format(table, page*everypage)
    if page == -1:
        searchsql = searchsql.split('limit')[0]
    print("外部", searchsql)
    data = pd.read_sql(searchsql,db)
    total = allnum if allnum > data.shape[0] else data.shape[0]
    if total == 0 :
        return abort(501,'not data')

        # 确定用户状态
    if table == 'user_account':
        # 获取application是否申请借款
        userstatus = params['user_status']
        billdb, _ = bill_conn()
        application_sql = "select user_id as id,status as user_status,max(id) from application group by user_id"
        application = pd.read_sql(application_sql, billdb)
        data = pd.merge(data, application, how='left', on='id').fillna('-10').drop(columns=['max(id)'], axis=1)
        if userstatus != "":
            userstatus = userstatus.split(',')
            data = data[data.user_status.isin(userstatus)]
    #区分是否有筛选条件
    if page == -1:
        senddata = data
    else:
        senddata = data.loc[:page * 200]
    for column in bit_columns:
        senddata[column] = senddata[column].apply(lambda x: 1 if x== b'\x01' else 0 )
    senddata = senddata.applymap(lambda x:str(x))
    if table == 'user_account':
        senddata['user_status'] = senddata['user_status'].map(user_status)
    db.close()
    # if os.path.exists('../downloaddata/%s_data.xls'%table):
    #     os.rmdir('../downloaddata/%s_data.xls'%table)
    nowpath = os.getcwd()
    if not os.path.exists(nowpath+'/downloaddata/%s_data.xls'%table):
        os.mknod(nowpath+'/downloaddata/%s_data.xls'%table,0o777)
    senddata.to_excel(nowpath+'/downloaddata/%s_data.xls'%table)
    return "/api/download/%s"%table



@get('/api/download/<table>',skip =[packing_plugin])
def down(table):
    nowpath = os.getcwd()
    response['Content-Type'] = 'application/octet-stream'
    response['Content-Disposition'] = "attachment;filename='{}_data.xls'".format(table)  # 这里改成自己需要的文件名
    return static_file('%s_data.xls' % table, nowpath + '/downloaddata/')