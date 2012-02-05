#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    File: collector.py
    Description: This module starts the reader/writer threads
"""

import os
import time
import Queue
import threading
import logging, logging.config
import sys; sys.path.extend(['rwtypes', 'third'])

import web
from util import conf_reader 
from rwtypes import rwtypes


SEVERITY_DEFAULT = "DEBUG"
LOGGING_PATH_DEFAULT = os.path.join(os.path.dirname(__file__), "logs", "")
ROTATING_DEFAULT = "midnight"
FORMATTER_DEFAULT = "%(asctime)s - %(filename)s (%(lineno)d) [(%(threadName)-10s)] %(levelname)s - %(message)s"


class Collector:
    def __init__(self,
                 daemon_conf=None,
                 to_log=False,
                 server=True,
                 default_queue_maxsize=100000):
        self.daemon_conf = daemon_conf
        self.to_log = to_log
        self.server = server

        #TODO: fix this relative path, use os.path
        self.conf = conf_reader.read_conf('../conf/conf.yaml')

        self.default_queue_maxsize = default_queue_maxsize
        self.prepare_readers_writers()
        self.web_server = web.Server(self)
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
        queue_maxsize = self.default_queue_maxsize
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
        try:
            for (writer, reader) in self.pairs:
                writer.start()
                reader.start()
        except Exception, e:
            print "Cannot start pair"
            print e

        try:
            if self.server:
                self.web_server.start()
        except Exception, e:
            print "Cannot start server"
            print e

        
        while True: time.sleep(3600) 


if __name__ == '__main__':
    c = Collector()
    c.start()
