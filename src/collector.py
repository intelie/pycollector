"""
    File: collector.py
    Description: This module starts the reader/writer threads
"""

import time
import Queue
import logging, logging.config

from helpers import inspect_shell

from reader.myreader import MyReader
from writer.mywriter import MyWriter


SEVERITY_DEFAULT = "DEBUG"
LOGGING_PATH_DEFAULT = "../logs/"
ROTATING_DEFAULT = "midnight"
FORMATTER_DEFAULT = "%(asctime)s - %(filename)s (%(lineno)d) [(%(threadName)-10s)] %(levelname)s - %(message)s"


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
            try:
                severity = self.conf.SEVERITY
            except AttributeError:
                severity = SEVERITY_DEFAULT

            self.logger.setLevel(severity)

            try:
                logging_path = self.conf.LOGGING_PATH
            except AttributeError:
                logging_path = LOGGING_PATH_DEFAULT

            filename = logging_path + 'collector.log'

            try:
                rotating = self.conf.ROTATING
            except AttributeError:
                rotating = ROTATING_DEFAULT

            log_handler = logging.handlers.TimedRotatingFileHandler(filename, 
                                                                    when=rotating)

            try:
                formatter = self.conf.FORMATTER
            except AttributeError:
                formatter = FORMATTER_DEFAULT

            formatter = logging.Formatter(formatter)
            log_handler.setFormatter(formatter)
            self.logger.addHandler(log_handler)
        except Exception, e:
            print e

    def start(self):
        if self.to_log:
            self.logger.info("Starting collector...")

        q = Queue.Queue(maxsize=100000)

        self.mywriter = MyWriter(queue=q)
        self.myreader = MyReader(queue=q, writer=self.mywriter)

        self.mywriter.start()
        if self.to_log:
            self.logger.info("Writer started")

        self.myreader.start()
        if self.to_log:
            self.logger.info("Reader started")

        if self.to_log:
            self.logger.info("Collector started.")

        while True:
            processed = self.mywriter.processed
            time.sleep(1)
            print "Messages processed: %s" % str(self.mywriter.processed - processed)

