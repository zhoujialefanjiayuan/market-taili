import datetime
import json
import requests

balanceurl = 'https://api.nxcloud.com/api/common/getBalance'
pullurl = 'https://api.nxcloud.com/api/sms/getSmsReport'
detailurl = 'https://api.nxcloud.com/api/sms/getSmsCdr'
sendsmsurl = 'https://api.nxcloud.com/api/sms/mtsend'

appkey = 'EddRRC6f'
secretkey = 'rTVzqCin'


appkey1 = 'V4Ix0BpY'
secretkey1 = 'XNxq8AHu'

def checkbalance_niuxing():
    re = requests.post(balanceurl, data={"appkey": appkey, "secretkey": secretkey})
    re.close()
    #人民币
    return float(json.loads(re.text)['balance'])


operator=['INDONESIAN','SMARTFREN','TELEKOMUNIKASI','3 INDONESIA','EXCELCOM','NATRINDO']

def yanzheng_pull(start_date,end_date):
    re_data = []
    for i in operator:
        re = requests.post(pullurl, data={"appkey": appkey, "secretkey": secretkey,
                                         'start_date':start_date,'end_date':end_date,
                                         'operator':i})
        re_data.append(json.loads(re.text))
    return re_data

def yingxiao_pull(start_date,end_date):
    re_data = []
    for i in operator:
        re = requests.post(pullurl, data={"appkey": appkey1, "secretkey": secretkey1,
                                         'start_date':start_date,'end_date':end_date,
                                         'operator':i})
        re_data.append(json.loads(re.text))

    return re_data


#查询当天发送记录
def yanzheng_detail_pull(date,page):
    date = ''.join(date.split('-'))
    re = requests.post(detailurl, data={"appkey": appkey, "secretkey": secretkey,
                                         'date':date,'page_size':'50',"page":str(page)})
    data = re.text
    if data =='':
        return {}
    else:
        return json.loads(re.text)

def yingxiao_detail_pull(date,page):
    date = ''.join(date.split('-'))
    re = requests.post(detailurl, data={"appkey":appkey1, "secretkey":secretkey1,
                                         'date':date,'page_size':'50',"page":str(page)})
    data = re.text
    if data =='':
        return {}
    else:
        return json.loads(re.text)

def today():
    todaydate = str(datetime.date.today())
    yingxiao_total = yingxiao_detail_pull(todaydate,1)['info']['total']
    yanzheng_total = yanzheng_detail_pull(todaydate,1)['info']['total']
    if yingxiao_total == 0 and yanzheng_total ==0:
        return False
    else:
        return True


def sendsms():
    content = 'please click, http://pjanusa.vip/'.encode()
    re = requests.post(sendsmsurl,data={"appkey": appkey, "secretkey": secretkey,
                                         'phone':'62081291465680','content':content})
    return  re.text

if __name__ == '__main__':
    print(checkblance())