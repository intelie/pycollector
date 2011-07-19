"""
    File: pattern_conf.py
    Description: example of log configuration.
"""

conf = [{
           'log_filename': '/home/kaiser/oi_vendas_online.access.log',
           'global_fields': { 
               'host': 'WebServer-VendasOnline',
               'log_type': 'apache-access-log',
           },
           'events_conf': [{
                'eventtype': 'VendasOnline',
                'regexps': ['^.*GET /sale/wizardControl.html\?prd_code1=9052.*$'],
                'consolidation_conf': {
                    'period' : 1,
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
