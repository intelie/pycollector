"""
    File: pattern_conf.py
    Description: example of log configuration.
"""

conf = [{
           'log_filename': '/var/log/syslog',
           'global_fields': { 
               'host': 'WebServer-VendasOnline',
               'log_type': 'apache-access-log',
           },
           'events_conf': [{
                'eventtype': 'Welcome',
                'regexps': ['(?P<test>\\d+:\\d+:\\d+)'],
                'consolidation_conf': {
                    'period' : 1/60.0,
                    'enable': True,
                    'field': 'acessos',
                    'unique_fields': [['test', 4]],
                    'user_defined_fields': {
                        'provedor': 'Oi'}
                }
            }],
}]
