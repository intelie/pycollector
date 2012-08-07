"""
    File: pattern_conf.py
    Description: log configuration for "Vendas Online".
"""

conf = [
{
           'log_filename': '/var/log/syslog', 
           'global_fields': {
               'host': 'crm-web-1.adm.infra',
               'log_type': 'ApacheAccess',
           },
           'events_conf': [{
                'eventtype': 'VendasOnline',
                'regexps': ['^(?P<test>.*)$'],
                'consolidation_conf': {
                    'period' : 1.0/60,
                    'enable': True,
                    'field': 'accesses',
                    'unique_fields': {'users': { 'fields': ['test'], 'log2m': 10 } },
                    'user_defined_fields': {
                        'provider': 'iG',
                        'step': 'Passo 1'}
                },
           }]
},
]
