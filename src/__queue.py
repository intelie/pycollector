"""
    File: __queue.py
    Description: Customized Queue based on Queue builtin module
"""

import Queue


class CustomQueue(Queue.Queue):
    def __str__(self):
        return "%s/%s" % (self.qsize(), self.maxsize)
