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
                 server=True,
                 server_port=8442,
                 default_queue_maxsize=1000):

        self.log = logging.getLogger()
        self.server_port = server_port
        self.conf = conf
        self.daemon_conf = daemon_conf
        self.server = server
        self.default_queue_maxsize = default_queue_maxsize
        self.prepare_readers_writers()
        self.web_server = web.Server(self, self.server_port)

    def instantiate(self, queue, conf):
        rwtype = rwtypes.get_type(conf['type'])
        exec('import %s' % rwtype['module']) 
        exec('clazz = %s.%s' % (rwtype['module'], rwtype['class']))
        return clazz(queue, conf)

    def prepare_readers_writers(self):
        self.pairs = []
        for pair in self.conf:
            maxsize = pair['reader'].get('queue_maxsize', self.default_queue_maxsize)
            queue = CustomQueue(maxsize=maxsize)

            writer = self.instantiate(queue, pair['writer'])
            if not 'interval' in pair['writer']:
                queue.callback = writer.process

            reader = self.instantiate(queue, pair['reader'])

            if reader.checkpoint_enabled:
                reader.last_checkpoint = writer.last_checkpoint
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
        while True: 
            self.log.debug("Threads alive: %s" % threading.enumerate())
            if not self.web_server.is_alive():
                self.log.debug("Restarting webserver...")
                self.start_server()
            time.sleep(60) 

    def __str__(self):
        return str(self.__dict__)


if __name__ == '__main__':
    c = Collector()
    c.start()
