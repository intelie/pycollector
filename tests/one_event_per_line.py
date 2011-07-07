# -*- coding: utf-8 -*-


import unittest
import sys; sys.path.append("../src")

from event import create_event
from exception import *


class TestOneEventPerLine(unittest.TestCase):
    def testMostSimpleEvent(self):
        line = 'go there and get me some food.'
        conf = {'events_conf' : [{'eventtype' : 'my-type',
                                  'regexps' : ['.*foo.*']}]}
        event = create_event(line, conf)
        expected_event = {'eventtype' : 'my-type', 'line' : line}
        self.assertDictEqual(expected_event, event)


    def testWithGlobalDefinedConf(self):
        line = 'lex parsimoniae'
        conf = {'global_fields': {'global' : 'global-value'},
                'events_conf': [{'eventtype': 'acessos',
                                 'regexps': ['^lex.*$']}]}
        event = create_event(line, conf)
        expected_subset = {'global' : 'global-value'}
        self.assertDictContainsSubset(expected_subset, event)


    def testWithUserDefinedConf(self):
        line = 'lex parsimoniae'
        conf = {'events_conf': [{'eventtype': 'acessos',
                                 'regexps': ['^lex.*$'],
                                 'one_event_per_line_conf': {
                                    'user_defined_fields': {
                                        'passo': 'passo 1',
                                        'produto': 'antivirus'}}}]}
        event = create_event(line, conf)
        expected_subset = {'passo' : 'passo 1', 
                           'produto' : 'antivirus'}
        self.assertDictContainsSubset(expected_subset, event)


    def testWithUserDefinedAndGlobalConfs(self):
        line = 'lex parsimoniae'
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
        event = create_event(line, conf)
        expected_subset = {'host' : 'my-machine', 'log_type' : 'apache', 
                           'passo' : 'passo 1', 'produto' : 'antivirus'}
        self.assertDictContainsSubset(expected_subset, event)


    def testWithoutRegexps(self):
        line = 'imagination is more important than knowledge'
        conf = {'events_conf' : [{'eventtype' : 'my-type'}]}
        self.assertRaises(RegexpNotFound, create_event, line, conf)


    def testWithMultipleRegexps(self):
        line = 'go, go, go, marine!'
        conf = {'events_conf' : [{'eventtype' : 'my-type',
                                  'regexps' : ['^this won\'t match$', 
                                               '^.*marine!$']}]}
        event = create_event(line, conf)
        expected_event = {'eventtype' : 'my-type', 'line' : line}
        self.assertDictEqual(expected_event, event)


    def testWithEmptyListOfRegexps(self):
        line = 'go, go, go, marine!'
        conf = {'events_conf' : [{'eventtype' : 'my-type',
                                  'regexps' : []}]}
        event = create_event(line, conf)
        expected_event = None
        self.assertEqual(expected_event, event)


    def testEventWithRegexpGroups(self):
        line = 'There\'s nothing you can do that can\'t be done'
        conf = {'events_conf' : [{'eventtype' : 'my-type',
                                  'regexps' : ['.*(?P<who>you).*(?P<verb>can).*']}]}
        event = create_event(line, conf)
        expected_subset = {'who' : 'you', 'verb' : 'can'}
        self.assertDictContainsSubset(expected_subset, event)


    def testWithTwoEventsConfs(self):
        line = 'a vida é bela'
        conf = {'events_conf' : [{'eventtype' : 'suspense',
                                  'regexps' : ['', '^pânico \d$']},
                                 {'eventtype' : 'comédia dramática',
                                  'regexps' : ['^.*(?P<title>a vida é bela).*$']}]}
        event = create_event(line, conf)    
        expected_event = {'eventtype' : 'comédia dramática', 
                          'title' : 'a vida é bela', 
                          'line' : line}
        self.assertDictEqual(expected_event, event)


if __name__ == "__main__":
    unittest.main()
