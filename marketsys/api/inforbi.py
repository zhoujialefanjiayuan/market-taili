
import time
from os import path

from selenium import webdriver
from selenium.webdriver.chrome.options import Options




def getinforbip_balance():
    options = Options()
    options.add_argument("--headless")  # 使用headless 无界面形态
    options.add_argument('--disable-gpu')  # 禁用gpu
    options.add_argument("--no-sandbox")
    nowpath = path.abspath('.')
    print("%s/chromedriver"%nowpath)
    driver = webdriver.Chrome(executable_path="%s/chromedriver"%nowpath, options=options)

    account = 'ikidana'
    psw = 'NXJ93YMU@mqzLaH6'

    url = 'http://portal.infobip.com'
    driver.get(url)
    time.sleep(1)
    username = driver.find_element_by_id("username")
    username.send_keys(account)
    time.sleep(0.5)
    ps = driver.find_element_by_id("password")
    ps.send_keys(psw)
    time.sleep(0.5)
    botton = driver.find_element_by_id("login-btn")
    botton.click()
    time.sleep(1)
    balance = driver.find_element_by_xpath('//*[@class="orange"]').get_attribute('innerText')
    b = balance.split('R')[1]
    intbalance = b.strip().replace(',','',-1).split('.')[0]
    driver.close()
    #印尼盾转化
    if balance[0] == '-':
        return -float(intbalance)/2041
    return float(intbalance)/2041

if __name__ == '__main__':
    print(getinforbip_balance())


