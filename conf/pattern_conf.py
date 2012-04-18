"""
    File: pattern_conf.py
    Description: example of log configuration.
"""

conf = [{
           'log_filename': 'C:\inetpub\logs\LogFiles\W3SVC1\u_ex%y%m%d.log',
           'global_fields': { 
               'host': 'WebServer-VendasOnline',
               'log_type': 'apache-access-log',
           },
           'events_conf': [{
                'eventtype': 'Welcome',
                'regexps': ['^.*GET /welcome\.png.*$'],
                'consolidation_conf': {
                    'period' : 5/60.0,
                    'enable': True,
                    'field': 'acessos',
                    'user_defined_fields': {
                        'provedor': 'Oi'}
                }
            }],
}]
