#!/usr/bin/env python
# coding=utf-8

import redis, MySQLdb

from pymongo import MongoClient
from re_processor import settings


redis_pool = redis.ConnectionPool(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    db=settings.REDIS_DB,
    password=settings.REDIS_PWD)

cache = redis.Redis(connection_pool=redis_pool)
cache_la = redis.from_url(settings.LA_REDIS_URL)

def get_redis():
    return cache

def get_redis_la():
    return cache_la

mongo_conn_data = MongoClient(settings.MONGO_GIZWITS_DATA)
mongo_conn_core = MongoClient(settings.MONGO_GIZWITS_CORE)

def get_mongodb(name='data'):
    if 'data' == name:
        return mongo_conn_data.get_default_database()
    elif 'core' == name:
        return mongo_conn_core.get_default_database()
    else:
        return False


class MysqlConnection(object):
    '''
    operate mysql
    '''
    def __init__(self):
        self.conn = MySQLdb.connect(
            host=settings.MYSQL_HOST,
            port=settings.MYSQL_PORT,
            user=settings.MYSQL_USER,
            passwd=settings.MYSQL_PWD,
            db=settings.MYSQL_DB
            )
        self.conn.autocommit(True)

    def reconnect(self):
        self.conn = MySQLdb.connect(
            host=settings.MYSQL_HOST,
            port=settings.MYSQL_PORT,
            user=settings.MYSQL_USER,
            passwd=settings.MYSQL_PWD,
            db=settings.MYSQL_DB
            )
        self.conn.autocommit(True)


    def __del__(self):
        self.conn.close()

mysql_conn = MysqlConnection()

def get_mysql():
    try:
        mysql_conn.conn.ping()
    except:
        print 'reconnect mysql'
        mysql_conn.reconnect()
    return mysql_conn.conn.cursor()
