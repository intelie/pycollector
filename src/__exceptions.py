#
# File: exceptions.py
# Description: pycollector exceptions 
#

class ConfigurationError(Exception):
    """Raised when there is something missing or wrong in conf.yaml"""
    def __init__(self, msg=''):
        self.msg = msg
