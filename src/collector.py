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


import web
import __meta__
from rwtypes import rwtypes
from __queue import CustomQueue


class Collector:
    def __init__(self,
                 conf, 
                 daemon_conf=None,
                 enable_server=True,
                 server_port=8442,
                 default_queue_maxsize=1000):

        self.log = logging.getLogger()
        self.conf = conf
        self.daemon_conf = daemon_conf
        self.server_port = server_port
        self.enable_server = enable_server
        self.server = web.Server(self, self.server_port)
        self.default_queue_maxsize = default_queue_maxsize
        self.prepare_readers_writers()

    def instantiate(self, queue, conf):
        """Instantiate a reader or writer"""
        rwtype = rwtypes.get_type(conf['type'])
        exec('import %s' % rwtype['module']) 
        exec('clazz = %s.%s' % (rwtype['module'], rwtype['class']))
        return clazz(queue, conf)

    def prepare_readers_writers(self):
        """Append reader/writer references in self.pairs"""
        self.pairs = []
        for pair in self.conf:
            maxsize = pair['reader'].get('queue_maxsize', self.default_queue_maxsize)
            queue = CustomQueue(maxsize=maxsize)

            writer = self.instantiate(queue, pair['writer'])
            reader = self.instantiate(queue, pair['reader'])

            if reader.checkpoint_enabled:
                reader.last_checkpoint = writer.last_checkpoint
            self.pairs.append((writer, reader))

    def start_pairs(self):
        """Starts readers and writers threads"""
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
        """Starts the web server. 
        Available by default in http://localhost:8442.
        """
        try:
            self.server.start()
            self.log.info('Web started.')
        except Exception, e:
            self.log.error("Cannot start server")
            self.log.error(e)

    def start(self):
        """Starts the collector"""
        self.start_pairs()
        if self.enable_server:
            self.start_server()
        self.log.info("Collector started.")
        while True: 
            self.log.debug("Threads alive: %s" % threading.enumerate())
            if not self.server.is_alive():
                self.log.debug("Restarting web server...")
                self.start_server()
            time.sleep(60) 

    def __str__(self):
        return str(self.__dict__)


if __name__ == '__main__':
    c = Collector()
    c.start()
