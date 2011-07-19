"""
    File: one_event_per_line_conf.py
    Description: collecting one event per line.
"""

conf = [{
           'log_filename': '/var/log/apache/access.log',
           #optional
           'global_fields': { 
               'host': 'WebServer-VendasOnline',
               'log_type': 'apache-access-log',
           },
           'events_conf': [{
                'eventtype': 'VendasOnline',
                'regexps': ['^.*GET /sale/wizardControl.html?prd_code1=9052.*$'],
                'one_event_per_line_conf': {
                    'user_defined_fields': {
                        'provedor': 'Oi'
                    }
                }
            }]
    }]
