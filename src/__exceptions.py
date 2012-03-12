#
# File: exceptions.py
# Description: pycollector exceptions 
#

class GenericError(Exception):
    def __init__(self, msg=''):
        self.msg = msg

    def __str__(self):
        return str(self.msg)

class ConfigurationError(GenericError):
    """Raised when there is something missing or wrong in conf.yaml"""

class ParsingError(GenericError):
    """Error parsing log line"""
