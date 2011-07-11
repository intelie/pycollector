#!/usr/bin/env python
# -*- coding: utf-8 -*-


import helpers.filetail as filetail

from exception import * 


class LogFileManager():
    def __init__(self, conf):
        self.validate_conf(conf)
        self.conf = conf
        self.filename = conf['log_filename']

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

    def tail(self):
        t = filetail.Tail(self.filename, only_new=True)
        line_processor = LogLinesProcessor(self.conf)
        while True:
            line = t.nextline()
            line_processor.process_line(line)
            #todo: send event and check for time period to send consolidated events
