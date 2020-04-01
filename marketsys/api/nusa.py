import time

from selenium import webdriver
from selenium.webdriver.chrome.options import Options


def checkbalance_nusa():
    options = Options()
    options.add_argument("--headless")  # 使用headless 无界面形态
    options.add_argument('--disable-gpu')  # 禁用gpu
    options.add_argument("--no-sandbox")
    driver = webdriver.Chrome(executable_path="./chromedriver", options=options)
    account = 'ikidana'
    psw = '467661568cx#CX'
    url = 'https://app.nusasms.com'
    driver.get(url)
    time.sleep(1)
    username = driver.find_element_by_id("username")
    username.send_keys(account)
    time.sleep(0.5)
    ps = driver.find_element_by_id("password")
    ps.send_keys(psw)
    time.sleep(0.5)
    botton = driver.find_element_by_xpath('//*[@tabindex="3"]')
    botton.click()
    time.sleep(1)
    balance = driver.find_element_by_xpath('//span//*[@class="label label-success"]').get_attribute('innerText')
    b = float(balance.split(' ')[1].split(',')[0])/2 #27.160,00
    driver.close()
    return b