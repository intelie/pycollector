# -*- coding: utf-8 -*-


import unittest
import sys; sys.path.append("../src")

from event import LogLinesManager
from exception import *


class TestOneEventPerLine(unittest.TestCase):
    def testMostSimpleEvent(self):
        conf = {'events_conf' : [{'eventtype' : 'my-type',
                                  'regexps' : ['.*foo.*']}]}
        log_manager = LogLinesManager(conf)
        line = 'go there and get me some food.'
        log_manager.process_line(line)
        event = log_manager.to_send
        expected_event = {'eventtype' : 'my-type', 'line' : line}
        self.assertDictEqual(expected_event, event)


    def testWithGlobalDefinedConf(self):
        conf = {'global_fields': {'global' : 'global-value'},
                'events_conf': [{'eventtype': 'acessos',
                                 'regexps': ['^lex.*$']}]}
        log_manager = LogLinesManager(conf)
        line = 'lex parsimoniae'
        log_manager.process_line(line)
        event = log_manager.to_send
        expected_subset = {'global' : 'global-value'}
        self.assertDictContainsSubset(expected_subset, event)


    def testWithUserDefinedConf(self):
        conf = {'events_conf': [{'eventtype': 'acessos',
                                 'regexps': ['^lex.*$'],
                                 'one_event_per_line_conf': {
                                    'user_defined_fields': {
                                        'passo': 'passo 1',
                                        'produto': 'antivirus'}}}]}
        log_manager = LogLinesManager(conf)
        line = 'lex parsimoniae'
        log_manager.process_line(line)
        event = log_manager.to_send
        expected_subset = {'passo' : 'passo 1', 
                           'produto' : 'antivirus'}
        self.assertDictContainsSubset(expected_subset, event)


    def testWithUserDefinedAndGlobalConfs(self):
        conf = {
           'global_fields': {
               'host': 'my-machine',
               'log_type': 'apache',
           },
           'events_conf': [{
                'eventtype': 'acessos',
                'regexps': ['^lex.*$'],
                'one_event_per_line_conf': {
                    'user_defined_fields': {
                    'passo': 'passo 1',
                    'produto': 'antivirus'}
                }
            }],
        }        
        log_manager = LogLinesManager(conf)
        line = 'lex parsimoniae'
        log_manager.process_line(line)
        event = log_manager.to_send
        expected_subset = {'host' : 'my-machine', 'log_type' : 'apache', 
                           'passo' : 'passo 1', 'produto' : 'antivirus'}
        self.assertDictContainsSubset(expected_subset, event)


    def testWithoutRegexps(self):
        conf = {'events_conf' : [{'eventtype' : 'my-type'}]}
        self.assertRaises(RegexpNotFound, LogLinesManager, conf)


    def testWithMultipleRegexps(self):
        conf = {'events_conf' : [{'eventtype' : 'my-type',
                                  'regexps' : ['^this won\'t match$', 
                                               '^.*marine!$']}]}
        log_manager = LogLinesManager(conf)
        line = 'go, go, go, marine!'
        log_manager.process_line(line)
        event = log_manager.to_send
        expected_event = {'eventtype' : 'my-type', 'line' : line}
        self.assertDictEqual(expected_event, event)


    def testWithEmptyListOfRegexps(self):
        conf = {'events_conf' : [{'eventtype' : 'my-type',
                                  'regexps' : []}]}
        log_manager = LogLinesManager(conf)
        line = 'go, go, go, marine!'
        log_manager.process_line(line)
        event = log_manager.to_send
        expected_event = None
        self.assertEqual(expected_event, event)


    def testEventWithRegexpGroups(self):
        conf = {'events_conf' : [{'eventtype' : 'my-type',
                                  'regexps' : ['.*(?P<who>you).*(?P<verb>can).*']}]}
        log_manager = LogLinesManager(conf)
        line = 'There\'s nothing you can do that can\'t be done'
        log_manager.process_line(line)
        event = log_manager.to_send
        expected_subset = {'who' : 'you', 'verb' : 'can'}
        self.assertDictContainsSubset(expected_subset, event)


    def testWithTwoEventsConfs(self):
        conf = {'events_conf' : [{'eventtype' : 'suspense',
                                  'regexps' : ['^pânico \d$']},
                                 {'eventtype' : 'comédia dramática',
                                  'regexps' : ['^.*(?P<title>a vida é bela).*$']}]}
        log_manager = LogLinesManager(conf)
        line = 'a vida é bela'
        log_manager.process_line(line)
        event = log_manager.to_send
        expected_event = {'eventtype' : 'comédia dramática', 
                          'title' : 'a vida é bela', 
                          'line' : line}
        self.assertDictEqual(expected_event, event)
        

if __name__ == "__main__":
    unittest.main()
