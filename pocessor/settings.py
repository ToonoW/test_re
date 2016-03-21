#!/usr/bin/env python
# coding=utf-8

import dotenv
dotenv.read_dotenv()

from getenv import env
import dj_database_url


# M2M settings
M2M_MQ_URL = env('M2M_MQ_URL', 'amqp://guest:guest@m2mprod.gwdev.com:5672/mqtt')
M2M_HOST = env('M2M_HOST', 'm2mprod.gwdev.com')
M2M_PORT = env('M2M_PORT', 5672)
VHOST = env('VHOST', '/')
M2M_USER = env('M2M_USER', 'guest')
M2M_PWD = env('M2M_PWD', 'guest')

EXCHANGE = 'amq.topic'

# databases settings
MONGO_DATABASES = env("MONGO_GIZWITS_DATA", "mongodb://localhost:27017/gizwits_data")

MYSQL_DATABASES = dj_database_url.parse(env("MYSQL_DATABASES", "mysql://root:root@localhost:3306/rules_engine"))

# logging
LOGGING = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'standard': {
            'format': "[%(asctime)s] %(levelname)s [%(name)s:%(lineno)s] %(message)s",
            'datefmt': "%d/%b/%Y %H:%M:%S"
        },
        'biz_log': {
            'format': ' %(message)s'
        }
    },
    'handlers': {
        "console": env("LOG_CONSOLE", {"level": "INFO", "class": "logging.StreamHandler", "formatter": "standard"}),
        #"graylog": env("LOG_GRAYLOG", {"level": "INFO", "class": "graypy.GELFRabbitHandler", "url": "amqp://guest:guest@localhost:5672/%2f"}),
    },
    'loggers': {
        'file': {
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'level': 'INFO',
            'when': 'D',
            'filename': '/mnt/workspace/gw_re_pocessor/gizwits_data.log',
            'formatter': 'standard',
        },
        'pocessor': {
            'handlers': ['console'],
            'propagate': True,
            'level': 'WARN',
        },
        'pocessor_gray': {
            #'handlers': ['graylog'],
            'handlers': ['console'],
            'level': 'INFO'
        }
    }
}
