
import pymysql
import redis
from bottle import hook, default_app,request,response
from peewee import MySQLDatabase
from playhouse.pool import PooledMySQLDatabase

app = default_app()


class BeginPooledMySQL(PooledMySQLDatabase):
    def begin(self):
        # db api 并没有自动加 begin 语句，所以在此要手动加上
        self.get_conn().begin()


class BeginMySQLDatabase(MySQLDatabase):
    def begin(self):
        # db api 并没有自动加 begin 语句，所以在此要手动加上
        self.get_conn().begin()


# 此处 autocommit 仅表示 peewee 是否在每一句后面自动加一句 commit
# timeout 指连接池里没有可用链接时，等待多少秒
# db = BeginPooledMySQL(
#     None,
#     autocommit=False,
#     max_connections=15,
#     timeout=5,
#     stale_timeout=60)

# !!! 连接池的 _in_use 有问题，暂时不使用

db = BeginMySQLDatabase(None, autocommit=False)
readonly_db = BeginMySQLDatabase(None, autocommit=False)


def init():
    app.config.setdefault('db.read_timeout', 20)
    app.config.setdefault('db.write_timeout', 20)
    db.init(
        app.config['db.database'],
        host=app.config['db.host'],
        user=app.config['db.user'],
        port=int(app.config['db.port']),
        charset=app.config['db.charset'],
        password=app.config['db.password'],
        autocommit=True,  # 连接mysql 时是否使用autocommit 模式
        read_timeout=app.config['db.read_timeout'],
        write_timeout=app.config['db.write_timeout'],
    )


@hook('before_request')
def validate():
    db.get_conn()
    #readonly_db.get_conn()
    REQUEST_METHOD = request.environ.get('REQUEST_METHOD')
    HTTP_ACCESS_CONTROL_REQUEST_METHOD = request.environ.get('HTTP_ACCESS_CONTROL_REQUEST_METHOD')
    if REQUEST_METHOD == 'OPTIONS':
        request.environ['REQUEST_METHOD'] = HTTP_ACCESS_CONTROL_REQUEST_METHOD


@hook('after_request')
def enable_cors():
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET,POST,PUT,DELETE,OPTIONS'
    response.headers['Access-Control-Allow-Credentials'] = 'true'
    allow_headers = 'Referer, Accept, Origin, User-Agent,X-Requested-With, Content-Type, X-File-Name'
    response.headers['Access-Control-Allow-Headers'] = allow_headers
    if not db.is_closed():
        db.close()
    #if not readonly_db.is_closed():
    #   readonly_db.close()


def account_center_conn():
    account_center_conn = pymysql.connect(
        db=app.config['readonly-db.database.account_center'],
        host=app.config['readonly-db.host'],
        user=app.config['readonly-db.user'],
        password=app.config['readonly-db.password'],
        charset=app.config['readonly-db.charset'],
        port=int(app.config['readonly-db.port']),
        autocommit=True,  # 连接mysql 时是否使用autocommit 模式
    )
    cursor = account_center_conn.cursor()
    return account_center_conn,cursor

def bill_conn():
    bill_conn = pymysql.connect(
        db=app.config['readonly-db.database.bill'],
        host=app.config['readonly-db.host'],
        user=app.config['readonly-db.user'],
        password=app.config['readonly-db.password'],
        charset=app.config['readonly-db.charset'],
        port=int(app.config['readonly-db.port']),
        autocommit=True,  # 连接mysql 时是否使用autocommit 模式
    )
    cursor = bill_conn.cursor()
    return bill_conn,cursor



def login_conn():
    login_conn = pymysql.connect(
        db=app.config['readonly-db.database.login'],
        host=app.config['readonly-db.host'],
        user=app.config['readonly-db.user'],
        password=app.config['readonly-db.password'],
        charset=app.config['readonly-db.charset'],
        port=int(app.config['readonly-db.port']),
        autocommit=True,  # 连接mysql 时是否使用autocommit 模式
    )
    cursor = login_conn.cursor()
    return login_conn,cursor


def marketsys_conn():
    marketsys_conn = pymysql.connect(
        db=app.config['readonly-db.database.marketsys'],
        host=app.config['readonly-db.host'],
        user=app.config['readonly-db.user'],
        password=app.config['readonly-db.password'],
        charset=app.config['readonly-db.charset'],
        port=int(app.config['readonly-db.port']),
        autocommit=True,  # 连接mysql 时是否使用autocommit 模式
    )
    cursor = marketsys_conn.cursor()
    return marketsys_conn,cursor

def remittance_conn():
    remittance_conn = pymysql.connect(
        db=app.config['python-db.database.remittance'],
        host=app.config['python-db.host'],
        user=app.config['python-db.user'],
        password=app.config['python-db.password'],
        charset=app.config['python-db.charset'],
        port=int(app.config['python-db.port']),
        autocommit=True,  # 连接mysql 时是否使用autocommit 模式
    )
    cursor = remittance_conn.cursor()
    return remittance_conn,cursor

#redis  连接
def redis_conn():
    rs = redis.StrictRedis(host="localhost",port=6379,db=0)
    return rs
