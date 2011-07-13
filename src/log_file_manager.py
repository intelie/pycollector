#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
    File: log_file_manager.py
    Description: This module knows how to tail log files and send events 
    (one per log line or consolidated ones).
"""


import time
import threading

import helpers.filetail as filetail
import helpers.kronos as kronos 
from helpers.stomp_sender import send_message_via_stomp
import helpers.simplejson as json

from exception import * 
from log_lines_processor import LogLinesProcessor
from sample_conf import conf


class LogFileManager:
    def __init__(self, conf):
        self.validate_conf(conf)
        self.conf = conf
        self.filename = conf['log_filename']
        self.scheduler = kronos.ThreadedScheduler()
        self.line_processor = LogLinesProcessor(self.conf)
        self.default_task_period = 1 #minute

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
                    'eventtype' : message_data['eventtype']}
        body = json.dumps(body)
        send_message_via_stomp([("localhost", 61613 )], headers, body)
        print "message sent"

    def schedule_consolidated_events_tasks(self):
        events_conf = self.conf['events_conf']
        for index, event_conf in enumerate(events_conf):
            #TODO: make it a boolean function
            if is_consolidation_enabled(event_conf):
                period = event_conf['consolidation_conf'].get('period', self.default_task_period)
                period *= 60 #conversion to minutes
                self.scheduler.add_interval_task(self.send_consolidated_event, "consolidation task",
                                                 0, period, kronos.method.threaded, [index], None)

    def schedule_simple_events_task(self):
        self.scheduler.add_single_task(self.send_simple_event, "simple event task",
                                       0, kronos.method.threaded, [], None)

    def schedule_tasks(self):
        self.schedule_consolidated_events_tasks()
        self.schedule_simple_events_task()
        self.scheduler.start()

    def tail(self):
        t = filetail.Tail(self.filename, only_new=True)
        self.schedule_tasks()
        while True:
            line = t.nextline()
            self.line_processor.process(line)


class LogFileManagerThreaded(threading.Thread):
    def __init__(self, conf):
        threading.Thread.__init__(self)
        self.log_file_manager = LogFileManager(conf)

    def run(self):
        self.log_file_manager.tail()

