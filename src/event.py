# -*- coding: utf-8 -*-


import re

from exception import *


class LogLinesManager:
    def __init__(self, conf):
        self.validate_conf(conf)
        self.conf = conf
        self.to_send = None


    def validate_conf(self, conf):
        for event_conf in conf['events_conf']:
            if not 'regexps' in event_conf:   
                raise RegexpNotFound(event_conf['eventtype'])


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
                    if 'one_event_per_line_conf' in event_conf and 'user_defined_fields' in event_conf['one_event_per_line_conf']:
                        event.update(event_conf['one_event_per_line_conf']['user_defined_fields'])
                    already_match = True
                    break
            if already_match:
                break

        if no_match:
            self.to_send = None
            return 
        elif 'global_fields' in self.conf:
            event.update(self.conf['global_fields'])
        self.to_send = event

