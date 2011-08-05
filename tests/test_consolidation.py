# -*- coding: utf-8 -*-


import unittest
import sys; sys.path.append("../src")

from log_lines_processor import LogLinesProcessor
from exception import *


class TestConsolidation(unittest.TestCase):
    def testCountInitialization(self):
        conf = {'log_filename' : 'test.log',
                'events_conf' : [{'eventtype' : 'access',
                                  'regexps' : [],
                                  'consolidation_conf' : {'field' : 'accesses'}}, 
                                 {'eventtype' : 'user',
                                  'regexps' : [], 
                                  'consolidation_conf' : {'field' : 'users'}}]}
        line_processor = LogLinesProcessor(conf)
        expected_consolidated = {0 : {'eventtype': 'access', 'accesses' : 0},
                                 1 : {'eventtype': 'user', 'users' : 0}}
        consolidated = line_processor.consolidated
        self.assertDictEqual(expected_consolidated, consolidated)

    def testMatchingWithGlobalFields(self):
        conf = {'log_filename' : 'test.log',
                'global_fields' : {'global-key' : 'global-value'},
                'events_conf' : [{'eventtype' : 'access',
                                  'regexps' : ['.*access.*'],
                                  'consolidation_conf' : {'field' : 'accesses'}}, 
                                 {'eventtype' : 'user',
                                  'regexps' : ['.*users.*'], 
                                  'consolidation_conf' : {'field' : 'users'}}]}
        line_processor = LogLinesProcessor(conf)
        line_processor.process('access test')
        line_processor.process('users test')
        expected_consolidated = {0 : {'eventtype': 'access', 'global-key' : 'global-value', 'accesses' : 1},
                                 1 : {'eventtype': 'user', 'global-key' : 'global-value', 'users' : 1}}
        consolidated = line_processor.consolidated
        self.assertDictEqual(expected_consolidated, consolidated)

    def testNoConsolidationConfs(self):
        conf = {'log_filename' : 'test.log',
                'events_conf' : [{'eventtype' : 'my-type', 'regexps' : []}]}
        line_processor = LogLinesProcessor(conf)
        expected_consolidated = {}
        consolidated = line_processor.consolidated
        self.assertDictEqual(expected_consolidated, consolidated)

    def testConsolidationWithUserDefinedFields(self):
        conf = {'log_filename' : 'test.log',
                'events_conf' : [{'eventtype' : 'real-numbers',
                                  'regexps' : ['^(\d{1,}\.\d{1,}).*$'],
                                  'consolidation_conf' : {'field' : 'real', 
                                                          'user_defined_fields' : {'a' : 'b', 
                                                                                   'c' : 'd'}}}]}
        line_processor = LogLinesProcessor(conf)
        line_processor.process('234')
        line_processor.process('546.544')
        line_processor.process('324.34')
        expected_consolidated = {0: {'eventtype' : 'real-numbers', 'real' : 2, 'a' : 'b', 'c' : 'd'}}
        consolidated = line_processor.consolidated
        self.assertDictEqual(expected_consolidated, consolidated)

    def testConsolidationDisabled(self):
        conf = {'log_filename' : 'test.log', 
                'events_conf' : [{'eventtype' : 'my-type',
                                 'regexps' : ['.*'],
                                 'consolidation_conf' : {'enable' : False}}]}
        line_processor = LogLinesProcessor(conf)
        expected_consolidated = {}
        consolidated = line_processor.consolidated
        self.assertDictEqual(expected_consolidated, consolidated)
        self.assertDictEqual(expected_consolidated, consolidated)
        self.assertEqual(line_processor.event_queue, [])
        line_processor.process('tsc tsc')


if __name__ == "__main__":
    unittest.main()
