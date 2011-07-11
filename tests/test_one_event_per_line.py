#!/usr/bin/env python
# -*- coding: utf-8 -*-


import unittest
import sys; sys.path.append("../src")

from log_lines_processor import LogLinesProcessor
from exception import *


class TestOneEventPerLine(unittest.TestCase):
    def testMostSimpleEvent(self):
        conf = {'log_filename': 'test.log',
                'events_conf' : [{'eventtype' : 'my-type',
                                  'regexps' : ['.*foo.*']}]}
        line_processor = LogLinesProcessor(conf)
        line = 'go there and get me some food.'
        line_processor.process(line)
        event = line_processor.event_queue[0]
        expected_event = {'eventtype' : 'my-type', 'line' : line}
        self.assertDictEqual(expected_event, event)

    def testWithGlobalDefinedConf(self):
        conf = {'log_filename': 'test.log',
                'global_fields': {'global' : 'global-value'},
                'events_conf': [{'eventtype': 'acessos',
                                 'regexps': ['^lex.*$']}]}
        line_processor = LogLinesProcessor(conf)
        line = 'lex parsimoniae'
        line_processor.process(line)
        event = line_processor.event_queue[0]
        expected_subset = {'global' : 'global-value'}
        self.assertDictContainsSubset(expected_subset, event)

    def testWithUserDefinedConf(self):
        conf = {'log_filename': 'test.log',
                'events_conf': [{'eventtype': 'acessos',
                                 'regexps': ['^lex.*$'],
                                 'one_event_per_line_conf': {
                                    'user_defined_fields': {
                                        'passo': 'passo 1',
                                        'produto': 'antivirus'}}}]}
        line_processor = LogLinesProcessor(conf)
        line = 'lex parsimoniae'
        line_processor.process(line)
        event = line_processor.event_queue[0]
        expected_subset = {'passo' : 'passo 1', 
                           'produto' : 'antivirus'}
        self.assertDictContainsSubset(expected_subset, event)

    def testWithUserDefinedAndGlobalConfs(self):
        conf = {
           'log_filename': 'test.log',
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
        line_processor = LogLinesProcessor(conf)
        line = 'lex parsimoniae'
        line_processor.process(line)
        event = line_processor.event_queue[0]
        expected_subset = {'host' : 'my-machine', 'log_type' : 'apache', 
                           'passo' : 'passo 1', 'produto' : 'antivirus'}
        self.assertDictContainsSubset(expected_subset, event)


    def testWithMultipleRegexps(self):
        conf = {'log_filename': 'test.log',
                'events_conf' : [{'eventtype' : 'my-type',
                                  'regexps' : ['^this won\'t match$', 
                                               '^.*marine!$']}]}
        line_processor = LogLinesProcessor(conf)
        line = 'go, go, go, marine!'
        line_processor.process(line)
        event = line_processor.event_queue[0]
        expected_event = {'eventtype' : 'my-type', 'line' : line}
        self.assertDictEqual(expected_event, event)

    def testWithEmptyListOfRegexps(self):
        conf = {'log_filename': 'test.log',
                'events_conf' : [{'eventtype' : 'my-type',
                                  'regexps' : []}]}
        line_processor = LogLinesProcessor(conf)
        line = 'go, go, go, marine!'
        line_processor.process(line)
        self.assertEqual(len(line_processor.event_queue), 0)

    def testEventWithRegexpGroups(self):
        conf = {'log_filename': 'test.log',
                'events_conf' : [{'eventtype' : 'my-type',
                                  'regexps' : ['.*(?P<who>you).*(?P<verb>can).*']}]}
        line_processor = LogLinesProcessor(conf)
        line = 'There\'s nothing you can do that can\'t be done'
        line_processor.process(line)
        event = line_processor.event_queue[0]
        expected_subset = {'who' : 'you', 'verb' : 'can'}
        self.assertDictContainsSubset(expected_subset, event)

    def testWithTwoEventsConfs(self):
        conf = {'log_filename': 'test.log',
                'events_conf' : [{'eventtype' : 'suspense',
                                  'regexps' : ['^pânico \d$']},
                                 {'eventtype' : 'comédia dramática',
                                  'regexps' : ['^.*(?P<title>a vida é bela).*$']}]}
        line_processor = LogLinesProcessor(conf)
        line = 'a vida é bela'
        line_processor.process(line)
        event = line_processor.event_queue[0]
        expected_event = {'eventtype' : 'comédia dramática', 
                          'title' : 'a vida é bela', 
                          'line' : line}
        self.assertDictEqual(expected_event, event)
        

if __name__ == "__main__":
    unittest.main()

