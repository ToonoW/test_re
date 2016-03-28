#!/usr/bin/env python
# coding=utf-8

#import dotenv
#dotenv.read_dotenv()

from getenv import env

# project path
SYS_PATH = env('SYS_PATH', '.')

# M2M settings
M2M_MQ_URL = env('M2M_MQ_URL', 'amqp://guest:guest@m2mprod.gwdev.com:5672/mqtt')

EXCHANGE = env('EXCHANGE', 'amq.topic')

TOPIC_MAP = {
    'device_online': 'online',
    'device_offline': 'offline',
    'device_bind': 'bind',
    'device_unbind': 'unbind',
    'device_status_kv': 'data',
    'attr_fault': 'fault',
    'attr_alert': 'alert'
}

ROUTING_KEY = {
    'all': '*.{}.events.device.#',
    'enterprises': 'enterprises.{}.events',
    'alert': 'products.{}.events.device.attr_fault',
    'fault': 'products.{}.events.device.attr_alert',
    'online': 'products.{}.events.device.online',
    'offline': 'products.{}.events.device.offline',
    'bind': 'products.{}.events.device.bind',
    'unbind': 'products.{}.events.device.unbind',
    'raw': 'products.{}.events.device.status.raw',
    'data': 'products.{}.events.device.status.kv',
    'changed': 'products.{}.events.datapoints.changed'
}

PUBLISH_ROUTING_KEY = {
    'notification': 'gw_notification_message',
    'http': 'rules_engine_http'
}

# where msg to send
MSG_TO = {
    'internal': 'redis',
    'external': 'rabbitmq'
}

#
TRANSCEIVER = {
    'send': {
        MSG_TO['internal']: 'redis_publish',
        MSG_TO['external']: 'mq_publish'
    },
    'begin': {
        MSG_TO['internal']: 'redis_listen',
        MSG_TO['external']: 'mq_listen'
    },
    'unpack': {
        MSG_TO['internal']: 'redis_unpack',
        MSG_TO['external']: 'mq_unpack'
    },
}

# databases settings
# mongo
MONGO_DATABASES = env("MONGO_GIZWITS_DATA", "mongodb://localhost:27017/gizwits_data")

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
    }
}

# redis
REDIS_HOST = env("REDIS_HOST", 'localhost')
REDIS_PORT = env("REDIS_PORT", 6379)
REDIS_DB = env("REDIS_DB", 0)
REDIS_PWD = env("REDIS_PWD", '')
REDIS_EXPIRE = env("REDIS_EXPIRE", 3600)
REDIS_BRPOP_TIMEOUT = 20

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
        'file': {
            'level': 'INFO',
            'when': 'D',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': '/mnt/workspace/gw_re_pocessor/processor.log',
            'formatter': 'standard',
        },
        "console": env("LOG_CONSOLE", {"level": "INFO", "class": "logging.StreamHandler", "formatter": "standard"}),
        #"graylog": env("LOG_GRAYLOG", {"level": "INFO", "class": "graypy.GELFRabbitHandler", "url": "amqp://guest:guest@localhost:5672/%2f"}),
    },
    'loggers': {
        'processor': {
            'handlers': ['file'],
            'level': 'INFO',
        },
        'processor_gray': {
            #'handlers': ['graylog'],
            'handlers': ['console'],
            'level': 'INFO'
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
    'que': {
        'type': 1,
        'target': 2
    },
    'tri': {
        'action_type': 1,
        'params': 2,
        'action_content': 3
    },
    'tri_in_db': {
        'allow_time': 1,
        'task_list': 2,
        'action_type': 3,
        'params': 4,
        'action_content': 5
    },
    'log': {}
}

# processor core_map
CORE_MAP = {
    'internal': {
        'sel': 'SelectorCore',
        'cal': 'CalculatorCore',
        'que': 'QueryCore',
        'tri': 'TriggerCore',
        'log': 'LoggerCore'
    }
}

CONTAINER_MAP = {
    'internal': {
        'queue': ['BaseRedismqConsumer'],
        'processor': ['CommonProcessor'],
        'transceiver': ['InternalTransceiver']
    },
    'main': {
        'queue': ['BaseRabbitmqConsumer', 'BaseRedismqConsumer'],
        'processor': ['CommonProcessor'],
        'transceiver': ['MainTransceiver']
    },
    'output': {
        'queue': ['BaseRabbitmqConsumer', 'BaseRedismqConsumer'],
        'processor': ['CommonProcessor'],
        'transceiver': ['OutputTransceiver']
    }
}
