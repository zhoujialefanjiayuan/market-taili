import json

import requests

account = 'ikidana'
psw = 'changeMe123'
def checkbalance_advance():
    getcookies = requests.post(url='https://in.advance.ai/guardian/login',data={'username':account,'password':psw})
    SESSION = getcookies.cookies.get_dict()['SESSION']
    balance = requests.get('https://in.advance.ai/guardian/deposit/customer-balance',headers={'Cookie':'SESSION='+SESSION})
    getcookies.close()
    balance.close()
    #印尼盾
    return float(json.loads(balance.text)['data']['balance'])/2041