import shlex
import time
import Queue
from copy import deepcopy
from subprocess import call
import unittest

import sys; sys.path.append('..')
import __meta__; __meta__.load_paths()
from rwtypes.readers.db.DBReader import DBReader

from third.sqlalchemy import create_engine, Table, Column, Integer, String, MetaData
from third.sqlalchemy.orm import sessionmaker


def get_queue(maxsize=1024):
    return Queue.Queue(maxsize=maxsize)


def create_database(db_name):
    call(shlex.split("mysql -e 'create database %s'" % db_name))


def drop_database(db_name):
    call(shlex.split("mysql -e 'drop database %s'" % db_name))


def db_periodic_reading(f):
    def wrapper(self):
        engine = create_engine(self.connection, echo=False)
        metadata = MetaData()
        table = Table('users', metadata, Column(u'id', Integer()),
                                         Column(u'name', String(7)))
        metadata.create_all(engine)
        conn = engine.connect()
        conn.execute(table.insert(), [
            {'id': 0, 'name': 'spam'},
            {'id': 1, 'name': 'egg'},
            {'id': 2, 'name': 'bacon'},
        ])
        f(self)
    return wrapper

class TestDBReader(unittest.TestCase):
    def setUp(self):
        self.db_name = "dbreader_test"
        drop_database(self.db_name)
        self.user = ''
        self.password = ''
        self.host = "localhost"
        self.connection_format = 'mysql+mysqldb://%s:%s@%s/%s'
        self.connection = self.connection_format % (self.user,
                                                    self.password,
                                                    self.host,
                                                    self.db_name)
        self.base_conf = {'database' : self.db_name,
                          'host': self.host,
                          'user': self.user,
                          'passwd': self.password,
                          'connection': self.connection_format}
        create_database(self.db_name)

    def tearDown(self):
        drop_database(self.db_name)

    @db_periodic_reading
    def test_happy_periodic_reading(self):
        q = get_queue()
        conf = deepcopy(self.base_conf)
        conf.update({'period': 1,
                     'columns': ['id', 'name'],
                     'query': 'select id, name from users'})

        myreader = DBReader(q, conf=conf)
        myreader.start()

        # waits for processing messages
        time.sleep(3.1)

        message = q.get()
        self.assertEqual({'id': 0, 'name': 'spam'}, message.content)
        self.assertEqual(0, message.checkpoint)

        message = q.get()
        self.assertEqual({'id': 1, 'name': 'egg'}, message.content)
        self.assertEqual(1, message.checkpoint)

        message = q.get()
        self.assertEqual({'id': 2, 'name': 'bacon'}, message.content)
        self.assertEqual(2, message.checkpoint)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestDBReader))
    return suite


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
