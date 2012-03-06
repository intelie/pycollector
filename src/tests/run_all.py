#!/usr/bin/env python
#
# File: run_all.py
# Description: test suite to run all tests at once
#

import unittest

import test_reader
import test_writer
import test_conf_reader

suite_reader = test_reader.suite()
suite_writer = test_writer.suite()
suite_conf_reader = test_conf_reader.suite()

suite = unittest.TestSuite()
suite.addTests(suite_reader)
suite.addTests(suite_writer)
suite.addTests(suite_conf_reader)

unittest.TextTestRunner(verbosity=2).run(suite)
