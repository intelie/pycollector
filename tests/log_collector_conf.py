"""
    File: log_collector_conf.py
    Description: example of log configuration.
"""


conf = [{
           'log_filename': '/var/log/access.log',
           'global_fields': {
               'host': 'my_machine',
               'log_type': 'apache',
           },
           'events_conf': [{
                'eventtype': 'acessos',
                'regexps': [
                            '^.*(?P<action>GET) /sale/wizardControl.html?prd_code1=(?P<code>[\d]+)&_origem=(?P<origem>vitrine).*$',
                            '^.*POST /sale/wizardStepIdentificationNewUser!nextStep.html?step=register.*$'],
                'consolidation_conf': {
                    'enable': False, #just to disable
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
