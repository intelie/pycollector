#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
    File: collector.py
    Description: This module starts log file managers
"""

__author__ = "Ronald Kaiser"
__email__ = "ronald at intelie dot com dot br"


import logging, logging.config

from conf_util import *
from log_file_manager import LogFileManagerThreaded


class Collector:
    def __init__(self, conf, logging_conf=None, to_log=False):
        self.to_log = to_log

        if to_log:
            logging.config.fileConfig(logging_conf.LOGGING_CONF_FILENAME, 
                                      disable_existing_loggers=False)
            self.logger = logging.getLogger()

        self.log_threads = []

        for f_conf in conf:
            try:
                validate_conf(f_conf) 
            except Exception, e:
                if to_log:
                    self.logger.info("Configuration error.")
                    self.logger.debug(e)
            try:
                self.log_threads.append(LogFileManagerThreaded(f_conf, logging_conf, to_log))
            except Exception, e:
                if to_log:
                    self.logger.error(e)

        if to_log:
            self.logger.info("Collector instantiated with success.")


    def start(self):
        if self.to_log:
            self.logger.info("Starting collector...")

        for thread in self.log_threads:
            try:
                if self.to_log:
                    self.logger.debug("Starting thread for log file: %s." % \
                                      thread.log_file_manager.filename)
                thread.start()
                if self.to_log:
                    self.logger.debug("Thread for %s started." % \
                                      thread.log_file_manager.filename)
            except Exception, e:
                if to_log:
                    self.logger.error("Couldn't start thread for log file: %s" % \
                                      thread.log_file_manager.filename)
                    self.logger.error(e)

        if self.to_log:
            self.logger.debug("Threads started: %s" % self.log_threads)
            self.logger.info("Collector started.")


if __name__ == '__main__':
    import sys; sys.path.append('../conf')

    import sample_conf
    import logging_conf

    c = Collector(sample_conf.conf, logging_conf, True)
    c.start()
