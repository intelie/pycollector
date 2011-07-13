# -*- coding: utf-8 -*-


import unittest
import sys; sys.path.append("../src")

from exception import *
import conf_util


class TestOneEventPerLine(unittest.TestCase):
    def testWithoutEventsConf(self):
        conf = {'log_filename' : 'test.log'}
        self.assertRaises(EventsConfNotFound, conf_util.validate_conf, conf)

    def testWithoutFilename(self):
        conf = {'events_conf' : [{'eventtype': 'teste', 'regexps' : ['^.*$']}]}
        self.assertRaises(LogFilenameNotFound, conf_util.validate_conf, conf)

    def testWithoutEventtype(self):
        conf = {'log_filename' : 'test.log', 'events_conf' : [{'regexps' : ['^.*$']}]}
        self.assertRaises(EventtypeNotFound, conf_util.validate_conf, conf)

    def testWithoutRegexps(self):
        conf = {'log_filename': 'test.log', 'events_conf' : [{'eventtype' : 'my-type'}]}
        self.assertRaises(RegexpNotFound, conf_util.validate_conf, conf)

    #TODO: check for fieldname in consolidation_conf

if __name__ == "__main__":
    unittest.main()
