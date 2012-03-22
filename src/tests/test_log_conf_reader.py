import unittest

import sys; sys.path.append('..')
from __exceptions import ConfigurationError
from rwtypes.readers.log.LogConfReader import LogConfReader


class TestLogConfReader(unittest.TestCase):
    def test_raise_exception_if_conf_contains_groupby_without_a_match_property(self):
        conf = {'sums': [{'groupby': {'column': 'spam'}}]}
        self.assertRaises(ConfigurationError, LogConfReader.validate_conf, conf)

        conf = {'counts': [{'groupby': {'column': 'spam'}}]}
        self.assertRaises(ConfigurationError, LogConfReader.validate_conf, conf)

    def test_raise_exception_if_conf_contains_groupby_without_column_property(self):
        conf = {'sums': [{'groupby': {'match': '(.*)'}}]}
        self.assertRaises(ConfigurationError, LogConfReader.validate_conf, conf)

        conf = {'counts': [{'groupby': {'match': '(.*)'}}]}
        self.assertRaises(ConfigurationError, LogConfReader.validate_conf, conf)

    def test_raise_exception_if_match_doesnt_contain_an_invalid_regexp(self):
        conf = {'sums': [{'groupby': {'match': '(.*', 'column': 'spam'}}]}
        self.assertRaises(ConfigurationError, LogConfReader.validate_conf, conf)

        conf = {'counts': [{'groupby': {'match': '.*)', 'column': 'spam'}}]}
        self.assertRaises(ConfigurationError, LogConfReader.validate_conf, conf)

def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestLogConfReader))
    return suite


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
