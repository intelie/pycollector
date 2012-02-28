import unittest

import test_queue
import test_reader
import test_writer

suite_queue = test_queue.suite()
suite_reader = test_reader.suite()
suite_writer = test_writer.suite()

suite = unittest.TestSuite()
suite.addTests(suite_queue)
suite.addTests(suite_reader)
suite.addTests(suite_writer)

unittest.TextTestRunner(verbosity=2).run(suite)

