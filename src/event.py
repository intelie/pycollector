# -*- coding: utf-8 -*-


import re

from exception import *


def create_event(data, conf):
    if isinstance(data, str):
        line = data
    elif isinstance(data, list):
        lines = data
    
    event = {}
    events_conf = conf['events_conf']
    no_match = True
    if line:
        for event_conf in events_conf:
            if 'regexps' in event_conf:        
                regexps = event_conf['regexps']                
            else:
                raise RegexpNotFound(event_conf['eventtype'])
            for regexp in regexps:
                match = re.match(regexp, line)
                if match:
                    no_match = False
                    event.update({'eventtype' : event_conf['eventtype']})
                    event.update({'line' : line})
                    event.update(match.groupdict())
                    if 'one_event_per_line_conf' in event_conf and 'user_defined_fields' in event_conf['one_event_per_line_conf']:
                        event.update(event_conf['one_event_per_line_conf']['user_defined_fields'])                            
                    break
    if no_match:
        return None
    elif 'global_fields' in conf:
        event.update(conf['global_fields'])
    return event
