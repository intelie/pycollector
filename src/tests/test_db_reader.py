import unittest

import sys; sys.path.append('..')
import __meta__; __meta__.load_paths()
from rwtypes.readers.db.DBReader import DBReader


class TestDBReader(unittest.TestCase):
    pass


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestDBReader))
    return suite


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
