import unittest

import sys; sys.path.append('..')
from conf_reader import read_yaml_conf
from __exceptions import ConfigurationError


class TestConfReader(unittest.TestCase):
    def setUp(self):
        self.reader = {'type' : 'stdin'}
        self.writer = {'type' : 'stdout'}
        self.base = {'specs': [], 'conf' : []}
        self.base['conf'] = [{'reader' : self.reader,
                             'writer' : self.writer}]

    def test_raise_exception_if_checkpoint_path_is_missing(self):
        self.base['conf'][0]['reader']['checkpoint_enabled'] = True
        self.base['conf'][0]['reader']['type'] = 'stdin'
        self.base['conf'][0]['writer']['type'] = 'stdout'
        self.assertRaises(ConfigurationError, read_yaml_conf, (self.base))

if __name__ == '__main__':
    unittest.main()
