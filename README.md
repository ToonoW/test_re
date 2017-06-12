# 规则引擎processor Core

## 环境变量
<pre>
# debug开关
DEBUG=True

# 代码目录路径
SYS_PATH="/gw_re_processor"

# mongo
MONGO_GIZWITS_DATA="mongodb://mongo.gwdev.com:27017/gizwits_data"
MONGO_GIZWITS_CORE="mongodb://mongo.gwdev.com:27017/gizwits_core"

# rules_engine_api mysql
MYSQL_HOST="mysql.gwdev.com"
MYSQL_PORT=3306
MYSQL_USER="root"
MYSQL_PWD="root"
MYSQL_DB="rules_engine"

# la连接的m2m
M2M_MQ_URL="amqp://guest:guest@m2mprod.gwdev.com:5672/"
EXCHANGE="amq.topic"

# graylog
LOG_GRAYLOG={"level": "INFO", "class": "graypy.GELFRabbitHandler", "url": "amqp://guest:guest@localhost:5672/%2f"}

# inner api
HOST_GET_BINDING="innerapi.gwdev.com"
INNER_API_TOKEN="6a13dd13db814217b987f649aa5763c2"

# 监听队列超时时间
LISTEN_TIMEOUT=20
</pre>

# 手动启动方式
<pre>
python start.py [options]

# celery setting
BROKER_URL = "redis://redis:6379/0"
CELERY_RESULT_BACKEND="redis://redis:6379/0"

# python start.py -h 输出如下
Usage:
  start.py [options]

Options:
  -h --help                      Show this screen.
  --version                      Show version.
  --queue=<queue>                binding queue [default: all]     # 可选值:all, alert, fault, online, offline, bind, unbind, data, 其中all表示选中除data外的全部
  --product_key=<product_key>    binding product_key [default: *]
  --only-tmp-consumer            start as a tmp consumer          # 开启此参数,则无视其他参数所有参数,只启动一个兼容旧sms与phone服务的consumer
  --only-http-consumer           start as a http consumer         # 开启此参数,则无视除--only-tmp-consumer外的其他参数所有参数,只启动一个兼容开能私有云回调服务的consumer
  --only-gdmshttp-consumer       start as a gdms_http consumer    # 开启此参数,则无视除--only-tmp-consumer, --only-http-consumer外的其他参数所有参数,只启动一个兼容gdms回调服务的consumer
  --main=<main>                  set num of main core             # 设置当前进程为该核心启动的协程数量, 以下参数用法相似
  --sel=<sel>                    set num of sel core
  --cal=<cal>                    set num of cal core
  --que=<que>                    set num of que core
  --log=<log>                    set num of log core
  --tri=<log>                    set num of log core
</pre>
