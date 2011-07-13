#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
    File: collector.py
    Description: This module orchestrates log file managers
"""

__version__ = "0.1"
__author__ = "Ronald Kaiser"
__email__ = "ronald at intelie dot com dot br"


import sys; sys.path.append('../conf')

import helpers.kronos as kronos

from log_file_manager import LogFileManager, LogFileManagerThreaded
import sample_conf


class Collector:
    def __init__(self, conf):
        self.conf = conf
        self.log_file_managers = []
        self.scheduler = kronos.ThreadedScheduler()
        for log_file_conf in conf:
            self.log_file_managers.append(LogFileManagerThreaded(log_file_conf))

    def run(self):
        for log_file_manager in self.log_file_managers:
            log_file_manager.start()


if __name__ == '__main__':
    c = Collector(sample_conf.conf)
    c.run()
