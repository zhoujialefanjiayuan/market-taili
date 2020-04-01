
import http.client
import json
import bottle


app = bottle.default_app()

def send_sms(phone,text):
    if '62' != phone[0:2]:
        phone = '62' + phone
    if app.config['sendmessage.chosesupport'] == "chuanglan":

    # 服务地址
        host = app.config['sendmessage.chuanglan.host']
        # 端口号
        port = app.config['sendmessage.chuanglan.port']

        # 智能匹配模版短信接口的URI
        sms_send_uri = app.config['sendmessage.chuanglan.sms_send_uri']

        # 创蓝账号
        # account  = "I5453745"
        account = app.config['sendmessage.chuanglan.account']

        # 创蓝密码
        # password = "W6E2fsCbtG1d1e"
        password = app.config['sendmessage.chuanglan.password']
        params = {'account': account, 'password' : password, 'msg': text, 'mobile':str(phone), 'report' : 'false'}
        params=json.dumps(params)
        headers = {"Content-type": "application/json"}
        conn = http.client.HTTPConnection(host, port=port, timeout=30)
        conn.request("POST", sms_send_uri, params, headers)
        response = conn.getresponse()
        response_str = json.loads(response.read().decode())
        conn.close()
        return  response_str