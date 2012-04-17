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
import sys

class LogCollector:
    def __init__(self, conf, logging_conf=None, to_log=False):
        self.to_log = to_log
        if to_log:
            self.logger = logging.getLogger()
            self.logger.setLevel(logging_conf.SEVERITY)
            filename = logging_conf.LOGGING_PATH + 'collector.log'
            log_handler = logging.handlers.TimedRotatingFileHandler(filename, 
                                                                    when=logging_conf.ROTATING)
            formatter = logging.Formatter(logging_conf.FORMATTER)
            log_handler.setFormatter(formatter)
            self.logger.addHandler(log_handler)

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
            self.logger.info("LogCollector instantiated with success.")

    def start(self):
        if self.to_log:
            self.logger.info("Starting LogCollector...")

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
                if self.to_log:
                    self.logger.error("Couldn't start thread for log file: %s" % \
                                      thread.log_file_manager.filename)
                    self.logger.error(e)

        if self.to_log:
            self.logger.debug("Threads started: %s" % self.log_threads)
            self.logger.info("LogCollector started.")

    def stop(self):
        print 'stopping'
        for thread in self.log_threads:
            thread.stop()
           
