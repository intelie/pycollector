"""
    File: consolidation_conf.py
    Description: collecting and consolidating events periodically.
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
                'regexps': ['^.*GET /sale/wizardControl.html?prd_code1=9052.*$'], #one or more regexps
                'consolidation_conf': {
                    'period' : 1,
                    'enable': True,
                    'field': 'acessos',  #consolidation field
                    'user_defined_fields': {
                        'provedor': 'Oi'
                    }
                }
            }]
    }]
