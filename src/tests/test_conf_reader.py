import unittest

import sys; sys.path.append('..')
from conf_reader import read_yaml_conf
from __exceptions import ConfigurationError


class TestConfReader(unittest.TestCase):
    def setUp(self):
        self.reader = {'type': 'stdin'}
        self.writer = {'type': 'stdout'}
        self.base = {'specs': [], 'conf': []}
        self.base['conf'] = [{'reader': self.reader,
                             'writer': self.writer}]

    def tearDown(self):
        # back with initial stage
        self.setUp()

    def test_raise_exception_if_checkpoint_path_is_missing(self):
        self.reader['checkpoint_enabled'] = True
        self.reader['type'] = 'stdin'
        self.writer['type'] = 'stdout'
        self.writer['checkpoint_enabled'] = True
        self.assertRaises(ConfigurationError, read_yaml_conf, (self.base))

    def test_raise_exception_if_conf_is_empty(self):
        self.base['conf'] = None
        self.assertRaises(ConfigurationError, read_yaml_conf, (self.base))

    def test_raise_exception_if_checkpoint_and_not_blockable(self):
        # without blockable in reader
        self.tearDown()
        self.reader['checkpoint_enabled'] = True
        self.assertRaises(ConfigurationError, read_yaml_conf, (self.base))

        # without blockable in writer
        self.tearDown()
        self.writer['checkpoint_enabled'] = True
        self.assertRaises(ConfigurationError, read_yaml_conf, (self.base))

    def test_checkpoint_in_reader_and_writer(self):
        self.reader['checkpoint_enabled'] = True
        self.reader['checkpoint_path'] = '/var/log/42.log'
        self.reader['blockable'] = True
        self.assertRaises(ConfigurationError, read_yaml_conf, (self.base))

        self.tearDown()
        self.writer['checkpoint_enabled'] = True
        self.writer['checkpoint_path'] = '/var/log/42.log'
        self.writer['blockable'] = True
        self.assertRaises(ConfigurationError, read_yaml_conf, (self.base))

    def test_raise_exception_if_type_is_missing(self):
        self.reader.pop('type')
        self.assertRaises(ConfigurationError, read_yaml_conf, (self.base))

        self.tearDown()
        self.writer.pop('type')
        self.assertRaises(ConfigurationError, read_yaml_conf, (self.base))

    def test_raise_exception_if_type_is_unknown(self):
        self.reader['type'] = 'sbrubles'
        self.assertRaises(ConfigurationError, read_yaml_conf, (self.base))

        self.tearDown()
        self.writer['type'] = 'nietzsche'
        self.assertRaises(ConfigurationError, read_yaml_conf, (self.base))
        
    
def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestConfReader))
    return suite


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
