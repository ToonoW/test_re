#!/usr/bin/env python
# coding=utf-8

# import dotenv
# dotenv.read_dotenv()

from getenv import env
from logstash.formatter import LogstashFormatterVersion1

# debug
DEBUG = env('DEBUG', False)

# project path
SYS_PATH = env('SYS_PATH', '.')

# M2M settings
M2M_MQ_URL = env(
    'M2M_MQ_URL', 'amqp://guest:guest@m2mprod.gwdev.com:5672/mqtt')
PREFETCH_COUNT = env('PREFETCH_COUNT', 100)
EXCHANGE = env('EXCHANGE', 'amq.topic')

# reapi
REAPI_HOST = env('REAPI_HOST', 'reapi.gwdev.com')
REAPI_TOKEN = env('REAPI_TOKEN', 'cWExcmVhcGl0ZXN0MQ==')

TOPIC_MAP = {
    'device_schedule': 'schedule',
    'device_online': 'online',
    'device_offline': 'offline',
    'device_bind': 'bind',
    'device_unbind': 'unbind',
    'device_status_kv': 'data',
    'device_reset': 'reset',
    'attr_fault': 'fault',
    'attr_alert': 'alert'
}

IS_USE_GEVENT = env("IS_USE_GEVENT", False)

KV_ROUTING_KEY = env("KV_ROUTING_KEY", "products.{}.events.device.#.status.kv")

ROUTING_KEY = {
    'all': 'products.{}.events.device.*',
    # 'enterprises': 'enterprises.{}.events',
    'alert': 'products.{}.events.device.attr_fault',
    'fault': 'products.{}.events.device.attr_alert',
    'online': 'products.{}.events.device.online',
    'offline': 'products.{}.events.device.offline',
    'bind': 'products.{}.events.device.bind',
    'unbind': 'products.{}.events.device.unbind',
    'reset': 'products.{}.events.device.reset',
    # 'raw': 'products.{}.events.device.status.raw',
    'data': KV_ROUTING_KEY,
    # 'changed': 'products.{}.events.datapoints.changed',
    'schedule': 'rules_engine_schedule',
    'schedule_wait': 'rules_engine_schedule_wait'
}

PUBLISH_ROUTING_KEY = {
    'schedule': 'rules_engine_schedule',
    'notification': 'gw_notification_message',
    'http': 'gw_http_message',
    'gdms_http': 'gw_gdms_http_message',
    'tmp': 'gw_tmp_message',
    'email': 'gw_email_message',
    'sms': 'gw_sms_message',
    'devctrl': 'gw_devctrl_message',
    'es': 'gw_es_message'
}

DEBUG_ROUTING_KEY = {
    'schedule': 'rules_engine_debug',
    'notification': 'rules_engine_debug',
    'http': 'rules_engine_debug',
    'gdms_http': 'rules_engine_debug',
    'tmp': 'rules_engine_debug',
    'log': 'rules_engine_debug',
    'email': 'rules_engine_debug',
    'sms': 'rules_engine_debug',
    'devctrl': 'rules_engine_debug',
    'es': 'rules_engine_debug'
}

# where to send msg
MSG_TO = {
    # 'internal': 'redis',
    'internal': 'default',
    'external': 'rabbitmq'
}

PRODUCT_WHITELIST = env("PRODUCT_WHITELIST", ["pk"])

# processor core_map
CORE_MAP = {
    'v1': {
        'sel': 'SelectorCore',
        'cal': 'CalculatorCore',
        'script': 'ScriptCore',
        'json': 'JsonCore',
        'que': 'QueryCore',
        'tri': 'TriggerCore'
    },
    'v2': {
        'sel': 'SelectorCore',
        'cal': 'CalculatorCore',
        'script': 'ScriptCore',
        'json': 'JsonCore',
        'que': 'QueryCore',
        'tri': 'TriggerCore'
    },
    'v3': {
        'input': 'InputCore',
        'function': 'FuncCore',
        'output': 'OutputCore'
    }
}

# schedule settings
SCHEDULE_FILE_DIR = env('SCHEDULE_FILE_DIR', '/schedule')
DEVICE_HASH_GROUP = env('DEVICE_HASH_GROUP', 100)

###########databases settings################
# mongo
MONGO_GIZWITS_DATA = env("MONGO_GIZWITS_DATA",
                         "mongodb://localhost:27017/gizwits_data")
MONGO_GIZWITS_CORE = env("MONGO_GIZWITS_CORE",
                         "mongodb://localhost:27017/gizwits_core")

# mysql
MYSQL_HOST = env("MYSQL_HOST", "localhost")
MYSQL_PORT = env("MYSQL_PORT", 3306)
MYSQL_USER = env("MYSQL_USER", "root")
MYSQL_PWD = env("MYSQL_PWD", "root")
MYSQL_DB = env("MYSQL_DB", "rules_engine")
MYSQL_TABLE = {
    'rule': {
        'table': 't_rulesengine_rule',
    },
    'action': {
        'table': 't_rulesengine_action'
    },
    'schedule': {
        'table': 't_rulesengine_schedule',
    },
    'device_status': {
        'table': 't_rulesengine_device_status'
    },
    'limit': {
        'table': 't_rulesengine_limit'
    },
    'product_delay_setting': {
        'table': 't_rulesengine_product_delay_setting'
    }
}

# redis
REDIS_HOST = env("REDIS_HOST", 'localhost')
REDIS_PORT = env("REDIS_PORT", 6379)
REDIS_DB = env("REDIS_DB", 0)
REDIS_PWD = env("REDIS_PWD", '')
REDIS_EXPIRE = env("REDIS_EXPIRE", 3600)
REDIS_LOCK_EXPRIE = env("REDIS_LOCK_EXPRIE", 60)
LISTEN_TIMEOUT = env("LISTEN_TIMEOUT", 20)

LA_REDIS_URL = env("LA_REDIS_URL", "redis://localhost:6380/0")

#############################################

# host_get_bindings
HOST_GET_BINDING = env('HOST_GET_BINDING', 'innerapi.gwdev.com')
INNER_API_TOKEN = env('INNER_API_TOKEN', '6a13dd13db814217b987f649aa5763c2')

# host_run_script
SCRIPT_HOST = env('SCRIPT_HOST', 'script.gwdev.com')
SCRIPT_API_TOKEN = env('SCRIPT_API_TOKEN', '6a13dd13db814217b987f649aa5763c2')

# ES tmp url
ES_URL = 'https://admin:go4xpg@119.29.166.125:9200/product.air_cleaner.v1/data'

# sequence settings
SEQUENCE_EXPIRE = env("SEQUENCE_EXPIRE", 86400)
SEQUENCE_MAX_LEN = env("SEQUENCE_MAX_LEN", 50)

# product notification interval
NOTIFICATION_INTERVAL_EXPIRE = env(
    "NOTIFICATION_INTERVAL_EXPIRE", 10800)  # 缓存 3 小时过期

# default limit settings
MSG_LIMIT = env("MSG_LIMIT", 100)
TRIGGLE_LIMIT = env("TRIGGLE_LIMIT", 100)

# CELERY
BROKER_URL = env("CELERY_BROKER_URL", 'redis://redis:6379/0')
CELERY_RESULT_BACKEND = env("CELERY_RESULT_BACKEND",
                            'redis://redis:6379/0')
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_ACCEPT_CONTENT = ['application/json']


CELERY_DEFAULT_ROUTING_KEY = env(
    "CELERY_DEFAULT_ROUTING_KEY", "celery_recore_routing_key")
CELERY_DEFAULT_EXCHANGE = env(
    "CELERY_DEFAULT_EXCHANGE", "celery_recore_exchange")
CELERY_DEFAULT_QUEUE = env("CELERY_DEFAULT_QUEUE", "celery_recore_queue")

# opt settings
THERMAL_THRESHOLD = env('THERMAL_THRESHOLD', 100)

IS_NO_ACK = env("IS_NO_ACK",  True)

USE_DEBUG = env("USE_DEBUG", True)

# logging
LOGGING = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'standard': {
            'format': "[%(asctime)s] %(levelname)s [%(name)s:%(lineno)s] %(message)s",
            'datefmt': "%d/%b/%Y %H:%M:%S"
        },
    },
    'handlers': {
        "console": env("LOG_CONSOLE", {"level": "INFO", "class": "logging.StreamHandler", "formatter": "standard"}),
        "graylog": env("LOG_GRAYLOG", {"level": "INFO", "class": "graypy.GELFHandler", "url": "amqp://guest:guest@localhost:5672/%2f"}),
        "file": env("LOG_FILE", {"level": "INFO", "backupCount": 1, "class": "logging.handlers.RotatingFileHandler", "filename": "processor.log", "maxBytes": 50000000, "formatter": "standard"}),
        "debug": env("DEBUG_FILE", {"level": "INFO", "backupCount": 1, "class": "logging.handlers.RotatingFileHandler", "filename": "debug.log", "maxBytes": 50000000, "formatter": "standard"}),
        'logstash': {
            'level': 'INFO',
            'class': 'logstash.LogstashHandler',
            'host': env('LOGSTASH_IP', '193.112.141.15'),
            'port': env('LOGSTASH_PORT', 12213),
            'version': 1,
        },
        'logstash_file': {
            'level': 'INFO',
            'backupCount': 1,
            'class': 'logstash.FileLogstashHandler',
            'filename': 'logstash_file.log',
            'maxBytes': 50000000,
            'encoding': 'utf-8',
        },
    },
    'loggers': {
        'processor': {
            'handlers': ['console', 'file'],
            'level': 'INFO',
        },
        'processor_gray': {
            'handlers': ['graylog', 'file'],
            'level': 'INFO'
        },
        'debug_gray': {
            'handlers': ['graylog', 'file'],
            'level': 'INFO'
        },
        'debug_info': {
            'handlers': ['debug'],
            'level': 'INFO'
        },
        'logstash': {
            'handlers': ['logstash', 'logstash_file'],
            'level': 'INFO',
        }
    }
}

# task element index
INDEX = {
    'sel': {
        'left': 1,
        'opt': 2,
        'right': 3
    },
    'cal': {
        'exp': 1,
        'name': 2
    },
    'script': {
        'script_id': 1,
        'params': 2,
        'name': 3
    },
    'json': {
        'source': 1,
        'params': 2,
        'refresh': 3,
        'content': 4,
        'name': 5
    },
    'que': {
        'type': 1,
        'target': 2,
        'pass': 3
    },
    'tri': {
        'action_type': 1,
        'params': 2,
        'extern_params': 3,
        'action_content': 4,
        'action_id': 5
    },
    'tri_in_db': {
        'allow_time': 1,
        'task_list': 2,
        'action_type': 3,
        'params': 4,
        'action_content': 5
    }
}


# logstash日志相关
LOGSTASH_NODE = env('LOGSTASH_NODE', 'default')
LOGSTASH_SWITCH = env('LOGSTASH_SWITCH', {
    'enter_re': False,
    'rule_ready': True,
    'make_action': True,
    'process_action': True,
})
