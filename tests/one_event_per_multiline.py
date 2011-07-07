# -*- coding: utf-8 -*-


import unittest
import sys; sys.path.append("../src")

from event import create_event


class TestOneEventPerMultiLine(unittest.TestCase):
    def testMostSimpleEvent(self):
        lines = ['john was the base guitarrist and vocalist', 
                 'george was the solo guitarrist', 
                 'ringo was the drummer']
        conf = {'events_conf' : [{'eventtype' : 'my-type',
                                  'regexps' : ['^.*guitarrist.*$']}],
                                  'consolidation_conf' : {'field': 'count'}}
        event = create_event(line, conf)
        expected_event = {'eventtype' : 'my-type', 'count': 2}
        self.assertDictEqual(expected_event, event)


    def testMultipleRegexps(self):
        lines = ['bono is a singer', 
                 'john nash is a mathematician',
                 'einstein was a physicist',
                 'eric clapton is a musician']
        conf = {'events_conf' : [{'eventtype' : 'my-type',
                                  'regexps' : ['^.*mathematician.*$',
                                               '^.*physicist.*$']}],
                                  'consolidation_conf' : {'field': 'scientists'}}
        event = create_event(line, conf)
        expected_event = {'eventtype' : 'my-type', 'scientists': 2}
        self.assertDictEqual(expected_event, event)


    def testWithUserDefinedConf(self):
        lines = ['bono is a singer', 
                 'john nash is a mathematician',
                 'einstein was a physicist',
                 'eric clapton is a musician']
        description = 'mathematicians or physicists.'
        conf = {'events_conf' : [{'eventtype' : 'my-type',
                                  'regexps' : ['^.*mathematician.*$',
                                               '^.*physicist.*$']}],
                                  'consolidation_conf' : {'field': 'scientists', 
                                                          'user_defined_fields' : {'description' : description}}}
        event = create_event(line, conf)
        expected_event = {'eventtype' : 'my-type', 
                          'scientists': 2, 
                          'description' : description}
        self.assertDictEqual(expected_event, event)


    def testWithDisabledConsolidation(self):
        lines = ['bono is a singer', 
                 'eric clapton is a musician']
        conf = {'events_conf' : [{'eventtype' : 'my-type',
                                  'regexps' : ['^.*mathematician.*$',
                                               '^.*physicist.*$']}],
                                  'consolidation_conf' : {'enable': False}}
        event = create_event(line, conf)
        expected_event = None
        self.assertDictEqual(expected_event, event)


    def testWithGlobalDefinedConf(self):
        lines = ['bono is a singer', 
                 'einstein was a physicist',
                 'eric clapton is a musician']
        conf = {'global_fields' : {'college' : 'princeton'},
                'events_conf' : [{'eventtype' : 'my-type',
                                  'regexps' : ['^.*mathematician.*$',
                                               '^.*physicist.*$']}],
                                  'consolidation_conf' : {'field': 'scientists'}}
        event = create_event(line, conf)
        expected_event = {'eventtype' : 'my-type', 
                          'scientists': 1,
                          'college' : 'princeton'}
        self.assertDictEqual(expected_event, event)


if __name__ == "__main__":
    unittest.main()
