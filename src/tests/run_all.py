#!/usr/bin/env python
#
# File: run_all.py
# Description: test suite to run all tests at once
#

import os
import unittest


suite = unittest.TestSuite()

# gettting only test_*.py
tests = filter(lambda x: x.startswith('test_') and x.endswith('.py'), 
               os.listdir('.'))
# removing .py
tests = map(lambda x: x[:-3], tests)

# adding tests
for test in tests:
    exec('import %s' % test)
    exec('suite_%s = %s.suite()' % (test[5:], test))
    exec('suite.addTests(suite_%s)' % test[5:])

# running, =)
unittest.TextTestRunner(verbosity=2).run(suite)
