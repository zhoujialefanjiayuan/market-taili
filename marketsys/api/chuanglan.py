import requests
import  json
account  = 'IM6137117'
password = 'duey43itLl617d'
balanceurl = 'http://intapi.253.com/balance/json'
pullurl = 'http://intapi.253.com/pull/report'
detailurl = 'http://intapi.253.com/pull/mo'

def checkbalance_chuanglan():
    re = requests.post(balanceurl,json={"account":account,"password":password})
    re.close()
    #人民币
    return float(json.loads(re.text)['balance'])

def pull(count):
    re = requests.post(pullurl,json={"account":account,"password":password,"count":count})
    return json.loads(re.text)['result']
"""
{
"code": 0,"error": "",
"result": [
{
"batchSeq": "I2170326_1808142031_0000000",
"mobile": "8618037170702",
"msgid": "18081420311005732392",
"reportTime": "1808142031",
"status": "DELIVRD"
},]}"""


#不存在当天限制
def detail(count):
    re = requests.post(detailurl,json={"account":account,"password":password,"count":count})
    return json.loads(re.text)

def  today():
    result = detail(100)['result']
    return  True if len(result)>0 else False

"""
{
"code": 0,
"error": "",
"result": [
{
"destcode": "10690141880",
"moTime": "1534732783355",
"mobile": "18037170702",
"msg": "test message"
},
{
"destcode": "10690141881",
"moTime": "1534732787359",
"mobile": "18037170702",
"msg": "test message content"
}
]
}"""



if __name__ == '__main__':
    print(checkblance())
