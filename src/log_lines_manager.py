# -*- coding: utf-8 -*-


import re

from exception import *


class LogLinesManager:
    def __init__(self, conf):
        self.validate_conf(conf)
        self.conf = conf
        self.to_send = None

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
                    already_match = True
                    break
            if already_match:
                break

            self.to_send = None
        if no_match:
            return 
        elif 'global_fields' in self.conf:
            event.update(self.conf['global_fields'])
        self.to_send = event

