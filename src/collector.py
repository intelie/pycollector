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

from log_file_manager import LogFileManagerThreaded
import sample_conf


class Collector:
    def __init__(self, conf):
        self.conf = conf
        self.log_threads = []
        for file_conf in conf:
            self.log_threads.append(LogFileManagerThreaded(file_conf))

    def start(self):
        #TODO: log instead of print
        print "Starting collector..."
        for thread in self.log_threads:
            try:
                #TODO: log instead of print
                print "Starting thread for log file: %s." % thread.log_file_manager.filename 
                thread.start()
                #TODO: log instead of print
                print "Log file manager thread for %s started." % thread.log_file_manager.filename
            except:
                #TODO: log instead of print
                print "Couldn't start thread for log file: %s" % thread.log_file_manager.filename
        #TODO: log instead of print
        print "Collector started."



if __name__ == '__main__':
    c = Collector(sample_conf.conf)
    c.start()
