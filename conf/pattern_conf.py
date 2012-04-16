"""
    File: pattern_conf.py
    Description: example of log configuration.
"""

conf = [{
           'log_filename': 'C:\inetpub\logs\LogFiles\W3SVC1\u_ex120416.log',
           'global_fields': { 
               'host': 'WebServer-VendasOnline',
               'log_type': 'apache-access-log',
           },
           'events_conf': [{
                'eventtype': 'Welcome',
                'regexps': ['^.*GET /welcome\.png.*$'],
                'consolidation_conf': {
                    'period' : 1/60.0,
                    'enable': True,
                    'field': 'acessos',
                    'user_defined_fields': {
                        'provedor': 'Oi'}
                },
                'one_event_per_line_conf': {
                    'user_defined_fields': {
                        'provedor': 'Oi'}
                }
            }],
}]
