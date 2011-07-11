# -*- coding: utf-8 -*-


import unittest
import sys; sys.path.append("../src")

from log_lines_processor import LogLinesProcessor
from exception import *


class TestOneEventPerLine(unittest.TestCase):
    def testWithoutEventsConf(self):
        conf = {'log_filename' : 'test.log'}
        self.assertRaises(EventsConfNotFound, LogLinesProcessor.validate_conf, conf)

    def testWithoutFilename(self):
        conf = {'events_conf' : [{'eventtype': 'teste', 'regexps' : ['^.*$']}]}
        self.assertRaises(LogFilenameNotFound, LogLinesProcessor.validate_conf, conf)

    def testWithoutEventtype(self):
        conf = {'log_filename' : 'test.log', 'events_conf' : [{'regexps' : ['^.*$']}]}
        self.assertRaises(EventtypeNotFound, LogLinesProcessor.validate_conf, conf)


if __name__ == "__main__":
    unittest.main()
