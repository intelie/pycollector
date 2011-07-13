#!/usr/bin/env python
#-*- coding:utf-8 -*-

"""
    File: exception.py 
    Description: This module defines exceptions for log configurations.
"""


class ConfException(Exception):
    def __init__(self, conf):
        self.conf = conf

    def __str__(self):
        return repr(self.conf)

class RegexpNotFound(ConfException):
    pass

class EventsConfNotFound(ConfException):
    pass

class LogFilenameNotFound(ConfException):
    pass

class EventtypeNotFound(ConfException):
    pass
