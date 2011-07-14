#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
    File: collector.py
    Description: This module starts log file managers
"""

__version__ = "0.1"
__author__ = "Ronald Kaiser"
__email__ = "ronald at intelie dot com dot br"


import logging, logging.config

from log_file_manager import LogFileManagerThreaded


class Collector:
    def __init__(self, conf, logging_conf=None, to_log=True):
        self.to_log = to_log
        self.log_threads = []

        for file_conf in conf:
            self.log_threads.append(LogFileManagerThreaded(file_conf))

        if to_log:
            logging.config.fileConfig(logging_conf.LOGGING_CONF_FILENAME)
            self.logger = logging.getLogger()
            self.logger.info("Collector instantiated.")

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

    c = Collector(sample_conf.conf, logging_conf)
    c.start()
