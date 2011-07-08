# -*- coding: utf-8 -*-


import unittest
import sys; sys.path.append("../src")

from log_lines_manager import LogLinesManager
from exception import *


class TestOneEventPerLine(unittest.TestCase):
    def testWithoutEventsConf(self):
        conf = {'log_filename' : 'test.log'}
        self.assertRaises(EventsConfNotFound, LogLinesManager.validate_conf, conf)

    def testWithoutFilename(self):
        conf = {'events_conf' : [{'eventtype': 'teste', 'regexps' : ['^.*$']}]}
        self.assertRaises(LogFilenameNotFound, LogLinesManager.validate_conf, conf)

    def testWithoutEventtype(self):
        conf = {'log_filename' : 'test.log', 'events_conf' : [{'regexps' : ['^.*$']}]}
        self.assertRaises(EventtypeNotFound, LogLinesManager.validate_conf, conf)


if __name__ == "__main__":
    unittest.main()
