import datetime
import hashlib

import requests
import json

account = 'cs_lfe1bd'
pwd = 'YVSHaybz'




def timestr():
    return str(datetime.datetime.now()).split('.')[0].replace('-','',-1).replace(' ','').replace(':','',-1)

def sign(times):
    thestr = account+pwd+times
    m = hashlib.md5()
    m.update(thestr.encode())
    return  m.hexdigest()


def checkbalance_tianyihong():
    thetimes = timestr()
    thesign = sign(thetimes)
    blanceurl = 'http://sms.skylinelabs.cc:20003/getbalanceV2?account={}&sign={}&datetime={}'.format(account,thesign,thetimes)
    re = requests.get(blanceurl)
    re.close()
    #欧元
    return float(json.loads(re.text)['balance'])*7.7


#只可以查询当天发送记录
def detail_pull():
    thetimes = timestr()
    thesign = sign(thetimes)
    detailurl = 'http://sms.skylinelabs.cc:20003/getsentrcdV2?account={}&sign={}&datetime={}&begintime=000000&endtime=235959&startindex=0'.format(
        account,thesign,thetimes
    )
    print(detailurl)
    re = requests.get(detailurl)
    return json.loads(re.text)

def today():
    result = detail_pull()['array']
    return True if len(result)>0 else False

if __name__ == '__main__':
    print(checkblance())