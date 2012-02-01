#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    File: collector.py
    Description: This module starts the reader/writer threads
"""

import time
import Queue
import logging, logging.config
import sys; sys.path.extend(['rwtypes', 'third'])

from helpers import inspect_shell
from util import conf_reader 
from rwtypes import rwtypes


SEVERITY_DEFAULT = "DEBUG"
LOGGING_PATH_DEFAULT = "../logs/"
ROTATING_DEFAULT = "midnight"
FORMATTER_DEFAULT = "%(asctime)s - %(filename)s (%(lineno)d) [(%(threadName)-10s)] %(levelname)s - %(message)s"


class Collector:
    def __init__(self, daemon_conf=None, to_log=False):

        #supports inspecting
        global c
        c = self

        self.daemon_conf = daemon_conf
        self.to_log = to_log
        self.conf = conf_reader.read_conf('../conf/conf.yaml')
        self.prepare_readers_writers()
        if to_log: 
            self.set_logging()

    def set_logging(self):
        try:
            self.logger = logging.getLogger()
            try:
                severity = self.daemon_conf.SEVERITY
            except AttributeError:
                severity = SEVERITY_DEFAULT

            self.logger.setLevel(severity)

            try:
                logging_path = self.daemon_conf.LOGGING_PATH
            except AttributeError:
                logging_path = LOGGING_PATH_DEFAULT
            filename = logging_path + 'collector.log'

            try:
                rotating = self.daemon_conf.ROTATING
            except AttributeError:
                rotating = ROTATING_DEFAULT

            log_handler = logging.handlers.TimedRotatingFileHandler(filename, 
                                                                    when=rotating)

            try:
                formatter = self.daemon_conf.FORMATTER
            except AttributeError:
                formatter = FORMATTER_DEFAULT

            formatter = logging.Formatter(formatter)
            log_handler.setFormatter(formatter)
            self.logger.addHandler(log_handler)
        except Exception, e:
            print e

    def prepare_readers_writers(self):
        self.pairs = []
        queue_maxsize = 100000
        for pair in self.conf:
            reader_conf = pair['reader']
            writer_conf = pair['writer']

            if 'queue_maxsize' in reader_conf:
                queue_maxsize = reader_conf['queue_maxsize']

            queue = Queue.Queue(maxsize = queue_maxsize)

            writer_type = writer_conf['type'] 
            writer_type = rwtypes.get_writer_type(writer_type)
            exec('import %s' % writer_type['module'])
            exec('writer_class = %s.%s' % (writer_type['module'], writer_type['class']))
            writer = writer_class(queue, writer_conf)


            reader_conf = pair['reader']
            reader_type = reader_conf['type']
            reader_type = rwtypes.get_reader_type(reader_type)
            exec('import %s' % reader_type['module'])
            exec('reader_class = %s.%s' % (reader_type['module'], reader_type['class']))

            if not 'interval' in writer_conf:
                reader = reader_class(queue, reader_conf, writer)
            else:
                reader = reader_class(queue, reader_conf)

            self.pairs.append((writer, reader))


    def start(self):
        for (writer, reader) in self.pairs:
            writer.start()
            reader.start()
        
        while True: time.sleep(3600) 


#supports inspecting
c = None

if __name__ == '__main__':
    c = Collector()
    c.start()
