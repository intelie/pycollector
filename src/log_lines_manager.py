#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
    File: log_lines_manager.py
    Description: This module know how to deal with log lines and prepares events
    to be sent.
"""


import re

from exception import *


class LogLinesManager:
    def __init__(self, conf):
        self.validate_conf(conf)
        self.conf = conf
        self.counts = {}
        self.init_counts()
        self.event_queue = []

    def init_counts(self):
        events_conf = self.conf['events_conf']
        for event_conf in events_conf:
            if event_conf.has_key('consolidation_conf'):                
                self.counts.update({event_conf['consolidation_conf']['field'] : 0})

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

    def process_line(self, line):
        event = {}
        no_match = True
        already_match = False
        events_conf = self.conf['events_conf']
        for event_conf in events_conf:
            regexps = event_conf['regexps']
            for regexp in regexps:
                match = re.match(regexp, line)
                if match:
                    no_match = False
                    event.update({'eventtype' : event_conf['eventtype']})
                    event.update({'line' : line})
                    event.update(match.groupdict())
                    if event_conf.has_key('one_event_per_line_conf') and \
                       event_conf['one_event_per_line_conf'].has_key('user_defined_fields'):
                        event.update(event_conf['one_event_per_line_conf']['user_defined_fields'])
                    elif event_conf.has_key('consolidation_conf'):
                        if event_conf['consolidation_conf'].has_key('enable') and \
                            event_conf['consolidation_conf']['enable'] == False:
                            pass
                        else:
                            self.counts[event_conf['consolidation_conf']['field']] += 1                        
                    already_match = True
                    break
            if already_match:
                break

        if no_match:
            return 
        elif 'global_fields' in self.conf:
            event.update(self.conf['global_fields'])
        self.event_queue.append(event)

