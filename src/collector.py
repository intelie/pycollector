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


import __meta__
import web
import conf_reader 
from rwtypes import rwtypes


class Collector:
    def __init__(self,
                 conf=None,
                 to_log=False,
                 server=True,
                 default_queue_maxsize=1000):
        self.conf = conf
        self.to_log = to_log
        self.server = server

        log_filename = os.path.join(__meta__.PATHS['CONF_PATH'])
        self.conf = conf_reader.read_conf(log_filename)

        self.default_queue_maxsize = default_queue_maxsize
        self.prepare_readers_writers()
        self.web_server = web.Server(self)
        if to_log: 
            self.set_logging()

    def set_logging(self):
        try:
            self.logger = logging.getLogger()
            try:
                severity = self.conf.SEVERITY
            except AttributeError:
                severity = __meta__.DEFAULTS['LOG_SEVERITY']

            self.logger.setLevel(severity)

            try:
                log_file_path = self.conf.LOGGING_PATH
            except AttributeError:
                log_file_path = __meta__.DEFAULTS['LOG_FILE_PATH']

            try:
                rotating = self.conf.ROTATING
            except AttributeError:
                rotating = __meta__.DEFAULTS['LOG_ROTATING'] 

            log_handler = logging.handlers.TimedRotatingFileHandler(log_file_path, 
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

    def prepare_readers_writers(self):
        self.pairs = []
        queue_maxsize = self.default_queue_maxsize
        for pair in self.conf:
            reader_conf = pair['reader']
            writer_conf = pair['writer']

            if 'queue_maxsize' in reader_conf:
                queue_maxsize = reader_conf['queue_maxsize']

            queue = Queue.Queue(maxsize = queue_maxsize)

            if not 'type' in writer_conf:
                print 'Missing writer type in conf.yaml'
                exit(-1)
            writer_type = writer_conf['type'] 
            writer_type = rwtypes.get_writer_type(writer_type)

            exec('import %s' % writer_type['module'])
            exec('writer_class = %s.%s' % (writer_type['module'], writer_type['class']))
            writer = writer_class(queue, writer_conf)


            reader_conf = pair['reader']
            if not 'type' in reader_conf:
                print 'Missing writer type in conf.yaml'
                exit(-1)
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
                writer.setDaemon(True)
                writer.start()
                reader.setDaemon(True)
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
        

if __name__ == '__main__':
    c = Collector()
    c.start()
