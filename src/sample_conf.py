"""
    File: sample_conf.py
    Description: example of log configuration.
"""


conf = [{
           'log_filename': 'test.log',
           'global_fields': {
               'host': 'my_machine',
               'log_type': 'apache',
           },
           'events_conf': [{
                'eventtype': 'acessos',
                'regexps': ['.*'],
                'consolidation_conf': {
                    'period' : 1, #period in minutes
                    'enable': True, #just to disable
                    'field': 'count',
                    'user_defined_fields': {
                        'passo': 'passo 1',
                        'produto': 'antivirus'}
                },
                'one_event_per_line_conf': {
                    'user_defined_fields': {
                        'passo': 'passo 1',
                        'produto': 'antivirus'}
                }
            }],
       }]
