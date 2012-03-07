#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
    File: collector.py
    Description: This module starts the reader/writer threads
"""

import os
import sys
import Queue
import logging
import threading
import logging, logging.config

if __name__ == '__main__':
    try:
        import __meta__
        sys.path = __meta__.PATHS.values() + sys.path
    except ImportError, e:
        print e

import web
from rwtypes import rwtypes
from __queue import CustomQueue


class Collector:
    def __init__(self,
                 conf,
                 daemon_conf=None,
                 enable_server=True,
                 server_port=8442,
                 default_queue_maxsize=1000):

        self.log = logging.getLogger('pycollector')
        self.conf = conf
        self.daemon_conf = daemon_conf
        self.server_port = server_port
        self.enable_server = enable_server
        self.server = web.Server(daemon_conf['LOGS_PATH'], self, self.server_port)
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
                writer.start()
                reader.start()
            self.log.info('Readers/writers started.')
        except Exception, e:
            self.log.error("Cannot start pair.")
            self.log.error(e)
            sys.exit(-1)

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
            sys.exit(-1)

    def start(self):
        """Starts the collector"""
        self.start_pairs()
        if self.enable_server:
            self.start_server()
        self.log.info("Collector started.")

    def __str__(self):
        return str(self.__dict__)


if __name__ == '__main__':
    import conf_reader as cr
    import daemon_util as du
    du.set_logging()
    try:
        conf = cr.read_yaml_conf()
        daemon_conf = cr.read_daemon_conf()
    except ConfigurationError, e:
        print e.msg
        sys.exit(-1)

    Collector(conf=conf, daemon_conf=daemon_conf).start()
