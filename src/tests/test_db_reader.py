import shlex
from subprocess import call
import unittest

import sys; sys.path.append('..')
import __meta__; __meta__.load_paths()
from rwtypes.readers.db.DBReader import DBReader


def create_database(db_name):
    call(shlex.split("mysql -e 'create database %s'" % db_name))


def drop_database(db_name):
    call(shlex.split("mysql -e 'drop database %s'" % db_name))


class TestDBReader(unittest.TestCase):
    def __init__(self):
        unittest.TestCase.__init__(self)
        self.db_name = "dbreader_test"

    def setUp(self):
        create_database(self.db_name)

    def tearDown(self):
        drop_database(self.db_name)

    def test_(self):
        pass

def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestDBReader))
    return suite


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
