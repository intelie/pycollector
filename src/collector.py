#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
    File: collector.py
    Description: This module starts the reader/writer threads
"""

import os
import time
import Queue
import logging
import threading
import logging, logging.config


import __meta__
import web
from rwtypes import rwtypes


class Collector:
    def __init__(self,
                 conf,
                 daemon_conf=None,
                 server=True,
                 default_queue_maxsize=1000):
        self.log = logging.getLogger()
        self.conf = conf
        self.daemon_conf = daemon_conf
        self.server = server
        self.default_queue_maxsize = default_queue_maxsize
        self.prepare_readers_writers()
        self.web_server = web.Server(self)

    def prepare_readers_writers(self):
        #TODO: refactoring
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

    def start_pairs(self):
        try:
            for (writer, reader) in self.pairs:
                writer.setDaemon(True)
                writer.start()
                reader.setDaemon(True)
                reader.start()
            self.log.info('Readers/writers started.')
        except Exception, e:
            self.log.error("Cannot start pair.")
            self.log.error(e)

    def start_server(self):
        try:
            self.web_server.start()
            self.log.info('Web started.')
        except Exception, e:
            self.log.error("Cannot start server")
            self.log.error(e)

    def start(self):
        self.start_pairs()
        if self.server:
            self.start_server()
        self.log.info("Collector started.")
        

if __name__ == '__main__':
    c = Collector()
    c.start()
