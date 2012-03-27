import shlex
from subprocess import call
import unittest

import sys; sys.path.append('..')
import __meta__; __meta__.load_paths()
from rwtypes.readers.db.DBReader import DBReader

from third.sqlalchemy import create_engine, Table, Column, Integer, String, MetaData
from third.sqlalchemy.orm import sessionmaker 


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
    return wrapper

class TestDBReader(unittest.TestCase):
    def setUp(self):
        self.user = ''
        self.password = ''
        self.host = "localhost"
        self.db_name = "dbreader_test"
        self.connection = 'mysql+mysqldb://%s:%s@%s/%s' % (self.user,
                                                           self.password,
                                                           self.host,
                                                           self.db_name)
        create_database(self.db_name)

    def tearDown(self):
        drop_database(self.db_name)

    @db_periodic_reading
    def test_happy_periodic_reading(self):
        pass
        
        

def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestDBReader))
    return suite


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
