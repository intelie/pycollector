import unittest

import sys; sys.path.append('..')
from __exceptions import ConfigurationError
from rwtypes.readers.log.LogUtils import LogUtils


class TestLogConfReader(unittest.TestCase):
    def test_raise_exception_if_conf_contains_groupby_without_a_match_property(self):
        conf = {'groupby': {'column': 'spam'}}
        self.assertRaises(ConfigurationError, LogUtils.validate_conf, conf)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestLogConfReader))
    return suite


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
