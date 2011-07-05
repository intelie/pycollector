#
# File: log_collector_conf.py
# Description: example of log configuration.
#

conf = [{
           'log_filename': '/var/log/access.log',
           'global_fields': {
               'host': 'my_machine',
               'log_type': 'apache',
           },
           'event_config':
           [{
               'event_type': 'acessos',
                'patterns': [
                        {'regexp': '^.*GET /sale/wizardControl.html?prd_code1=9052&_origem=vitrine.*$',
                        'group_names': ["group1", "group2"]},
                        {'regexp': '^.*POST /sale/wizardStepIdentificationNewUser!nextStep.html?step=register.*$',
                        'group_names': ["group1", "group2"]}],
                'consolidation_conf': {
                    'enable_consolidation': True,
                    'consolidation_field': 'count',
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
