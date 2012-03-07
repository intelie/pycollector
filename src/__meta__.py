#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
    File: __meta__.py
    Description: centralizes paths and default values
"""

import os
import sys
import pprint

try:
    BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    PATHS = {
        'BASE_PATH' : BASE_PATH,
        'SRC_PATH' : os.path.join(BASE_PATH, "src"),
        'CONF_PATH' : os.path.join(BASE_PATH, "conf"),
        'HELPERS_PATH' : os.path.join(BASE_PATH, "src", "helpers"),
        'THIRD_PATH' : os.path.join(BASE_PATH, "src", "third"),
        'RWTYPES_PATH' : os.path.join(BASE_PATH, "src", "rwtypes"),}
    DEFAULTS = {
        'LOGS_PATH' : os.path.join(BASE_PATH, "logs"),
        'PID_FILE_PATH': os.path.join(BASE_PATH, "pycollector.pid"),
        'LOG_SEVERITY' : "DEBUG",
        'LOG_ROTATING' : 'midnight',
        'LOG_FORMATTER' : '%(asctime)s - %(filename)s (%(lineno)d) [(%(threadName)-10s)] %(levelname)s - %(message)s',}
except Exception, e:
    print e


def load_paths():
    import sys
    sys.path = PATHS.values() + sys.path


if __name__ == "__main__":
    pprint.pprint({'PATHS' : PATHS})
    pprint.pprint({'DEFAULTS' : DEFAULTS})
