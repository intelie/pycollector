#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
    File: log_lines_manager.py
    Description: This module knows how to deal with log lines and prepares events
    to be sent.
"""


import re

from exception import *


class LogLinesProcessor:
    def __init__(self, conf):
        self.conf = conf
        self.consolidated = {}
        self.init_counts()
        self.event_queue = []

    def has_global_fields(self):
        return self.conf.has_key('global_fields')

    def init_counts(self):
        events_conf = self.conf['events_conf']
        for (index, event_conf) in enumerate(events_conf):
            if not event_conf.has_key('consolidation_conf'):
                pass
            if (event_conf.has_key('consolidation_conf') and not event_conf['consolidation_conf'].has_key('enable')) or \
                (event_conf.has_key('consolidation_conf') and event_conf['consolidation_conf'].has_key('enable') and \
                 event_conf['consolidation_conf']['enable'] == True):
                event = {}
                event.update({'eventtype' : event_conf['eventtype']})
                event.update({event_conf['consolidation_conf']['field'] : 0})
                if self.has_global_fields():
                    event.update(self.conf['global_fields'])
                if event_conf['consolidation_conf'].has_key('user_defined_fields'):
                    event.update(event_conf['consolidation_conf']['user_defined_fields'])
                self.consolidated.update({index : event})
                
    def create_event(self, line, groups_matched, conf_index):
        event = {}
        conf = self.conf['events_conf'][conf_index]
        event.update({'eventtype' : conf['eventtype']})
        event.update(groups_matched)
        if conf.has_key('one_event_per_line_conf') and \
           conf['one_event_per_line_conf'].has_key('user_defined_fields'):
            event.update(conf['one_event_per_line_conf']['user_defined_fields'])
        if conf.has_key('consolidation_conf'):
            if conf['consolidation_conf'].has_key('enable') and \
                conf['consolidation_conf']['enable'] == False:
                pass
            else:
                self.consolidated[conf_index][conf['consolidation_conf']['field']] += 1
        else:
            event.update({'line' : line})
        if self.has_global_fields():
            event.update(self.conf['global_fields'])
        return event

    def process(self, line):
        events_conf = self.conf['events_conf']
        for index, event_conf in enumerate(events_conf):
            regexps = event_conf['regexps']
            for regexp in regexps:
                match = re.match(regexp, line)
                if match:
                    event = self.create_event(line, match.groupdict(), index)
                    self.event_queue.append(event)
                    return
