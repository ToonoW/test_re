#!/usr/bin/env python
# coding=utf-8

import redis
import MySQLdb

from pymongo import MongoClient
from re_processor import settings


redis_pool = redis.ConnectionPool(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    db=settings.REDIS_DB,
    password=settings.REDIS_PWD)

mongo_conn = MongoClient(settings.MONGO_DATABASES)

def get_mongodb():
    return mongo_conn.get_default_database()


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

    def __del__(self):
        self.conn.close()

mysql_conn = MysqlConnection()

def get_mysql():
    return mysql_conn.cursor()
