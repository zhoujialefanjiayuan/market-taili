from marketsys.api.advance import checkbalance_advance
from marketsys.api.chuanglan import checkbalance_chuanglan
from marketsys.api.inforbi import getinforbip_balance
from marketsys.api.niuxing import checkbalance_niuxing
from marketsys.api.nusa import checkbalance_nusa
from marketsys.api.tianyihong import checkbalance_tianyihong



def checkbalance():
    balance_data = []
    balance_data.append({'supplier':'advance','balance':checkbalance_advance()})
    balance_data.append({'supplier':'chuanglan','balance':checkbalance_chuanglan()})
    #balance_data.append({'supplier':'inforbi','balance':getinforbip_balance()})
    balance_data.append({'supplier':'niuxing','balance':checkbalance_niuxing()})
    balance_data.append({'supplier':'tianyihong','balance':checkbalance_tianyihong()})
    #balance_data.append({'supplier':'nusa','balance':checkbalance_nusa()})
    return balance_data