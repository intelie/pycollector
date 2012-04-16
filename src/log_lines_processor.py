#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
    File: log_lines_processor.py
    Description: This module processes log lines and prepares events to be sent.
"""


import re
import logging

from exception import *
from conf_util import *


class LogLinesProcessor:
    def __init__(self, conf, logger=None, to_log=False):
        self.logger = logger
        self.to_log = to_log
        self.conf = conf
        self.consolidated = {}
        self.init_counts()
        self.event_queue = []

    def init_counts(self):
        events_conf = self.conf['events_conf']
        for (index, event_conf) in enumerate(events_conf):
            if is_consolidation_enabled(event_conf):
                event = {'eventtype' : event_conf['eventtype'], 
                         event_conf['consolidation_conf']['field'] : 0}

                if has_global_fields(self.conf):
                    event.update(self.conf['global_fields'])

                if event_conf['consolidation_conf'].has_key('user_defined_fields'):
                    event.update(event_conf['consolidation_conf']['user_defined_fields'])

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
        else:
            event.update({'line' : line})

        if has_global_fields(self.conf):
            event.update(self.conf['global_fields'])

        return event

    def process(self, line):
        try:
            events_conf = self.conf['events_conf']
            for index, event_conf in enumerate(events_conf):
                regexps = event_conf['regexps']
                for regexp in regexps:
                    line = line.rstrip('\r\n')
                    match = re.match(regexp, line)
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

