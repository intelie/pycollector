#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
    File: collector.py
    Description: This module starts the thread
"""


import logging, logging.config


class Collector:
    def __init__(self, conf, to_log=False):
        self.conf = conf
        self.to_log = to_log
        if to_log: 
            self.set_logging()
            self.logger.info("Collector instantiated with success.")

    def set_logging(self):
        try:
            self.logger = logging.getLogger()
            self.logger.setLevel(self.conf.SEVERITY)
            filename = self.conf.LOGGING_PATH + 'collector.log'
            log_handler = logging.handlers.TimedRotatingFileHandler(filename, 
                                                                    when=self.conf.ROTATING)
            formatter = logging.Formatter(self.conf.FORMATTER)
            log_handler.setFormatter(formatter)
            self.logger.addHandler(log_handler)
        except Exception, e:
            print e

    def start(self):
        if self.to_log:
            self.logger.info("Starting collector...")

        import Queue

        from readers.myreader import MyReader
        from writers.mywriter import MyWriter

        q = Queue.Queue()

        myreader = MyReader(queue=q, blockable=True, periodic=True, interval=1)
        mywriter = MyWriter(queue=q, periodic=True, interval=1)

        myreader.start()
        if self.to_log:
            self.logger.info("Reader started")

        mywriter.start()
        if self.to_log:
            self.logger.info("Writer started")

        if self.to_log:
            self.logger.info("Collector started.")

        while True:
            pass
