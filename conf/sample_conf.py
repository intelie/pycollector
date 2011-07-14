"""
    File: sample_conf.py
    Description: example of log configuration.
"""

conf = [{
           'log_filename': '/home/kaiser/test1.log',
           'global_fields': {
               'host': 'my_machine',
               'log_type': 'apache',
           },
           'events_conf': [{
                'eventtype': 'test1',
                'regexps': ['^test1 (?P<numbertest1>\d+)$', '^line1 (?P<numbertest1>\d+)$'],
                'consolidation_conf': {
                    'period' : 1,
                    'enable': True,
                    'field': 'count1',
                    'user_defined_fields': {
                        'user-defined-consolidated1': 'user-defined-consolidated-value1'}
                },
                'one_event_per_line_conf': {
                    'user_defined_fields': {
                        'user-defined1': 'user-defined-value1'}
                }
            }],
        },
        {
           'log_filename': '/home/kaiser/test2.log',
           'global_fields': {
               'host': 'my_machine',
               'log_type': 'apache',
           },
           'events_conf': [{
                'eventtype': '/home/kaiser/test2.log',
                'regexps': ['^test2 (?P<numbertest2>\d+)$', '^line2 (?P<numbertest2>\d+)$'],
                'consolidation_conf': {
                    'period' : 2,
                    'enable': True,
                    'field': 'count2',
                    'user_defined_fields': {
                        'user-defined-consolidated2': 'user-defined-consolidated-value2'}
                },
                'one_event_per_line_conf': {
                    'user_defined_fields': {
                        'user-defined2': 'user-defined-value2'}
                }
            }]
}]
