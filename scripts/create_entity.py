# coding: utf-8
import os
import sys; sys.path.append('..')

from src import conf_reader
from src.third.sqlalchemy import create_engine
from src.rwtypes.writers.db.MessageEntity import Base

conf = conf_reader.load_yaml_conf()

db_user = conf.get('specs').get('mysqldb').get('user')
db_passwd = conf.get('specs').get('mysqldb').get('passwd')
db_database = conf.get('specs').get('mysqldb').get('database')

if db_user and db_passwd and db_database:
	engine = create_engine("mysql://%s:%s@localhost/%s" % (db_user, db_passwd, db_database))

Base.metadata.create_all(engine)
