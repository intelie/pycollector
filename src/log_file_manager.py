#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
    File: log_file_manager.py
    Description: This module knows how to tail log files and send events 
    (one per log line or consolidated ones).
"""


import time
import threading
import logging, logging.config
import sys; sys.path.append('../conf')

import helpers.filetail as filetail
import helpers.kronos as kronos 
from helpers.stomp_sender import send_message_via_stomp
import helpers.simplejson as json

from conf_util import *
from daemon_conf import ACTIVEMQ_SERVER, ACTIVEMQ_PORT
from log_lines_processor import LogLinesProcessor
import os


class LogFileManager:
    def __init__(self, conf, logging_conf=None, to_log=False):
        self.conf = conf
        self.to_log = to_log
        self.logging_conf = logging_conf
        self.logger = None
        self.filename = conf['log_filename']
        self._stopped = False
        if to_log:
            self.set_logging()
        self.scheduler = kronos.ThreadedScheduler()
        self.line_processor = LogLinesProcessor(self.conf, self.logger, to_log)
        self.default_task_period = 1 #minute

    def set_logging(self):
        logger = self.filename.split(os.sep)[-1].split('.log')[0] + '.lc.log'
        self.logger = logging.getLogger(logger)
        self.logger.setLevel(self.logging_conf.SEVERITY)
        filename = self.logging_conf.LOGGING_PATH + logger
        self.log_handler = logging.handlers.TimedRotatingFileHandler(filename, 
                                                                     when=self.logging_conf.ROTATING)
        formatter = logging.Formatter(self.logging_conf.FORMATTER)
        self.log_handler.setFormatter(formatter)
        self.logger.addHandler(self.log_handler)

    def send_simple_event(self):
        while True:
            if (len(self.line_processor.event_queue) > 0):
                event = self.line_processor.event_queue.pop(0)
                self.send_2_activemq(event)

    def send_consolidated_event(self, conf_index):
        self.send_2_activemq(self.line_processor.consolidated[conf_index])
        field = self.conf['events_conf'][conf_index]['consolidation_conf']['field']
        self.line_processor.consolidated[conf_index][field] = 0

    def send_2_activemq(self, message_data):
        body = message_data.copy()
        header = { 'destination' : '/queue/events',
                   'timestamp': int(time.time() * 1000),
                   'eventtype' : body['eventtype']}
        del body['eventtype']
        body = json.dumps(body)
        try:
            if self.to_log:
                self.logger.info("Sending message:")
                self.logger.debug("Header: %s." % header)
                self.logger.debug("Body: %s." % body)
            send_message_via_stomp([(ACTIVEMQ_SERVER, ACTIVEMQ_PORT )], header, body)
            if self.to_log:
                self.logger.info("Message sent.")
        except Exception, e:
            if self.to_log:
                self.logger.info("Message couldn't be sent.")
                self.logger.error(e)

    def schedule_tasks(self):
        self.schedule_consolidated_events_tasks()
        self.schedule_simple_events_task()
        self.scheduler.start()

    def tail(self):
        self.tail = filetail.Tail(self.filename, only_new=True)
        if self.to_log:
            self.logger.info("Scheduling tasks for: %s..." % self.filename)
        self.schedule_tasks()
        if self.to_log:
            self.logger.debug("Tasks for %s scheduled." % self.filename)
            self.logger.debug("Starting tailing for %s..." % self.filename)
        while not self._stopped:
            line = self.tail.nextline() 
            
            try:
                self.line_processor.process(line)
                if self.to_log:
                    self.logger.debug("Line processed with success.")
            except Exception, e:
                if self.to_log:
                    self.logger.info("Couldn't process line.")
                    self.logger.debug(e)

    def stop(self):
        self._stopped = True
        self.tail.stop()
                    
    def schedule_consolidated_events_tasks(self):
        events_conf = self.conf['events_conf']
        for index, event_conf in enumerate(events_conf):
            if is_consolidation_enabled(event_conf):
                period = 60*event_conf['consolidation_conf'].get('period', self.default_task_period)
                self.scheduler.add_interval_task(self.send_consolidated_event,
                                                 "consolidation task",
                                                 0,
                                                 period,
                                                 kronos.method.threaded,
                                                 [index],
                                                 None)

    def schedule_simple_events_task(self):
        pass
        # self.scheduler.add_single_task(self.send_simple_event,
                                       # "simple event task",
                                       # 0,
                                       # kronos.method.threaded,
                                       # [],
                                       # None)


class LogFileManagerThreaded(threading.Thread):
    def __init__(self, conf, logging_conf=None, to_log=False):
        self.log_file_manager = LogFileManager(conf, logging_conf, to_log)
        threading.Thread.__init__(self)

    def run(self):
        self.log_file_manager.tail()
        
    def stop(self):
        self.log_file_manager.stop()
