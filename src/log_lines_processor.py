#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
    File: log_lines_processor.py
    Description: This module processes log lines and prepares events to be sent.
"""


import re
import logging
import time

from exception import *
from conf_util import *
from hyperloglog import *


class LogLinesProcessor:
    def __init__(self, conf, logger=None, to_log=False):
        self.logger = logger
        self.to_log = to_log
        self.conf = conf
        self.consolidated = {}
        self.init_counts()
        self.event_queue = []
        
        events_conf = self.conf['events_conf']
        for event_conf in events_conf:
            event_conf['compiled'] = map(re.compile, event_conf['regexps'])
        
    def init_counts(self):
        events_conf = self.conf['events_conf']
        for index in range(len(events_conf)):
            self.reset_count(index)
            
    def reset_count(self, index):
        event_conf = self.conf['events_conf'][index]
        if is_consolidation_enabled(event_conf):
            event = {'eventtype' : event_conf['eventtype'], '_ignore' : [], 
                     event_conf['consolidation_conf']['field'] : 0}

            if has_global_fields(self.conf):
                event.update(self.conf['global_fields'])

            if event_conf['consolidation_conf'].has_key('user_defined_fields'):
                event.update(event_conf['consolidation_conf']['user_defined_fields'])

            for unique_name, definition in event_conf['consolidation_conf'].get('unique_fields', {}).items():
                event['_ignore'].append(unique_name)
                event[unique_name] = HyperLogLog(definition['log2m'])

            self.consolidated.update({index : event})

    def prepare_event(self, line, groups_matched, conf_index):
        conf = self.conf['events_conf'][conf_index]
        event = {'eventtype' : conf['eventtype']}
        event.update(groups_matched)

        if conf.has_key('one_event_per_line_conf') and \
           conf['one_event_per_line_conf'].has_key('user_defined_fields'):
            event.update(conf['one_event_per_line_conf']['user_defined_fields'])

        if is_consolidation_enabled(conf):
            self.consolidated[conf_index][conf['consolidation_conf']['field']] += 1
            for unique_name, definition in conf['consolidation_conf'].get('unique_fields', {}).items():
                self.consolidated[conf_index][unique_name].offer([groups_matched.get(x, None) for x in definition['fields']])
                    
                
        else:
            event.update({'line' : line})

        if has_global_fields(self.conf):
            event.update(self.conf['global_fields'])

        return event

    def process(self, line):
        try:
            events_conf = self.conf['events_conf']
            for index, event_conf in enumerate(events_conf):
                for regexp in event_conf['compiled']:
                    line = line.rstrip('\r\n')
                    
                    match = regexp.search(line)
                    if match:
                        if self.to_log:
                            self.logger.debug("Line: [%s] matching with regexp: %s" % (line, regexp))
                            self.logger.debug("Groups matched: %s" % match.groupdict())
                        event = self.prepare_event(line, match.groupdict(), index)

                        self.logger.debug(event_conf)
                        if is_one_event_per_line_enabled(event_conf):
                            self.event_queue.append(event)
                        return
        except Exception, e:
            if self.to_log:
                self.logger.error(e)
            
                
                
        if self.to_log:
            self.logger.debug("Line: [%s] didn't match with any regexp." % line)
