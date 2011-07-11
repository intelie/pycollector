# -*- coding: utf-8 -*-


import unittest
import sys; sys.path.append("../src")

from log_file_manager import LogFileManager
from exception import *


class TestOneEventPerLine(unittest.TestCase):
    def testWithoutEventsConf(self):
        conf = {'log_filename' : 'test.log'}
        self.assertRaises(EventsConfNotFound, LogFileManager.validate_conf, conf)

    def testWithoutFilename(self):
        conf = {'events_conf' : [{'eventtype': 'teste', 'regexps' : ['^.*$']}]}
        self.assertRaises(LogFilenameNotFound, LogFileManager.validate_conf, conf)

    def testWithoutEventtype(self):
        conf = {'log_filename' : 'test.log', 'events_conf' : [{'regexps' : ['^.*$']}]}
        self.assertRaises(EventtypeNotFound, LogFileManager.validate_conf, conf)

    def testWithoutRegexps(self):
        conf = {'log_filename': 'test.log', 'events_conf' : [{'eventtype' : 'my-type'}]}
        self.assertRaises(RegexpNotFound, LogFileManager, conf)

    #TODO: check for fieldname in consolidation_conf

if __name__ == "__main__":
    unittest.main()
