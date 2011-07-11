#!/usr/bin/env python
# -*- coding: utf-8 -*-


from threading import Timer

import helpers.filetail as filetail
import helpers.kronos as kronos 

from exception import * 
from log_lines_processor import LogLinesProcessor
from sample_conf import conf


class LogFileManager():
    def __init__(self, conf):
        self.validate_conf(conf)
        self.conf = conf
        self.filename = conf['log_filename']
        self.scheduler = kronos.ThreadedScheduler()
        self.line_processor = LogLinesProcessor(self.conf)
        self.default_task_period = 1 #minute

    @staticmethod
    def validate_conf(conf):
        if not conf.has_key('log_filename'):
            raise LogFilenameNotFound()
        if not conf.has_key('events_conf'):
            raise EventsConfNotFound()
        for event_conf in conf['events_conf']:
            if not event_conf.has_key('eventtype'):
                raise EventtypeNotFound()
            if not event_conf.has_key('regexps'):
                raise RegexpNotFound()

    def send_consolidated(self, conf_index):
        print self.line_processor.consolidated[conf_index] #TODO: instead of print, send
        field = self.conf['events_conf'][conf_index]['consolidation_conf']['field']
        self.line_processor.consolidated[conf_index][field] = 0

    def schedule_tasks(self):
        events_conf = self.conf['events_conf']
        for index, event_conf in enumerate(events_conf):

            #TODO: make it a boolean function
            if (event_conf.has_key('consolidation_conf') and not event_conf['consolidation_conf'].has_key('enable')) or \
                (event_conf.has_key('consolidation_conf') and event_conf['consolidation_conf'].has_key('enable') and \
                 event_conf['consolidation_conf']['enable'] == True):
                period = event_conf['consolidation_conf'].get('period', self.default_task_period)
                period *= 60 #conversion to minutes
                self.scheduler.add_interval_task(self.send_consolidated, "consolidation task",
                                                 0, period, kronos.method.threaded,
                                                 [index], None)
        print "Tasks scheduled."
        self.scheduler.start()


    def tail(self):
        t = filetail.Tail(self.filename, only_new=True)
        self.schedule_tasks()
        while True:
            line = t.nextline()
            self.line_processor.process(line)


if __name__ == '__main__':
    test = LogFileManager(conf[0])
    test.tail()
