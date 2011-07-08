# -*- coding: utf-8 -*-


import unittest
import sys; sys.path.append("../src")

from log_lines_manager import LogLinesManager
from exception import *


class TestConsolidation(unittest.TestCase):
    def testCountInitialization(self):
        conf = {'log_filename' : 'test.log',
                'events_conf' : [{'eventtype' : 'access',
                                  'regexps' : [],
                                  'consolidation_conf' : {'field' : 'access'}}, 
                                 {'eventtype' : 'users',
                                  'regexps' : [], 
                                  'consolidation_conf' : {'field' : 'users'}}]}
        log_manager = LogLinesManager(conf)
        expected_counts = {'access' : 0, 'users' : 0}
        counts = log_manager.counts
        self.assertDictEqual(expected_counts, counts)

    def testMatching(self):
        conf = {'log_filename' : 'test.log',
                'events_conf' : [{'eventtype' : 'access',
                                  'regexps' : ['.*access.*'],
                                  'consolidation_conf' : {'field' : 'access'}}, 
                                 {'eventtype' : 'users',
                                  'regexps' : ['.*users.*'], 
                                  'consolidation_conf' : {'field' : 'users'}}]}
        log_manager = LogLinesManager(conf)
        log_manager.process_line('access test')
        log_manager.process_line('users test')
        expected_counts = {'access' : 1, 'users' : 1}
        counts = log_manager.counts
        self.assertDictEqual(expected_counts, counts)


if __name__ == "__main__":
    unittest.main()
