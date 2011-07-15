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
from activemq_conf import ACTIVEMQ_SERVER, ACTIVEMQ_PORT
from log_lines_processor import LogLinesProcessor


class LogFileManager:
    def __init__(self, conf, logging_conf=None, to_log=True):
        validate_conf(conf)
        self.conf = conf
        self.to_log = to_log
        self.logging_conf = logging_conf
        if to_log:
            self.set_logging()
        self.filename = conf['log_filename']
        self.scheduler = kronos.ThreadedScheduler()
        self.line_processor = LogLinesProcessor(self.conf, self.logger)
        self.default_task_period = 1 #minute

    def set_logging(self):
        logger = self.conf['log_filename'].split('/')[-1].split('.log')[0] + '.lc.log'
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
        body = message_data
        headers = { 'destination' : '/queue/events',
                    'timestamp': int(time.time() * 1000),
                    'eventtype' : 'test'}
        body = json.dumps(body)
        try:
            if self.to_log:
                self.logger.debug("Headers: %s." % headers)
                self.logger.debug("Body: %s." % body)
                self.logger.info("Sending message...")
            send_message_via_stomp([(ACTIVEMQ_SERVER, ACTIVEMQ_PORT )], headers, body)
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
        t = filetail.Tail(self.filename, only_new=True)
        if self.to_log:
            self.logger.info("Scheduling tasks for: %s..." % self.filename)
        self.schedule_tasks()
        if self.to_log:
            self.logger.debug("Tasks scheduled.")
            self.logger.debug("Starting tailing...")
        while True:
            line = t.nextline()
            try:
                self.line_processor.process(line)
                if self.to_log:
                    self.logger.debug("Line processed with success: %s" % line)
            except Exception, e:
                if self.to_log:
                    self.logger.info("Couldn't process line")
                    self.logger.debug(e)

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
        self.scheduler.add_single_task(self.send_simple_event,
                                       "simple event task",
                                       0,
                                       kronos.method.threaded,
                                       [],
                                       None)


class LogFileManagerThreaded(threading.Thread):
    def __init__(self, conf, logging_conf, to_log=True):
        self.log_file_manager = LogFileManager(conf, logging_conf, to_log)
        threading.Thread.__init__(self)

    def run(self):
        self.log_file_manager.tail()
