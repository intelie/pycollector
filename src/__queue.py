"""
    File: __queue.py
    Description: Customized Queue based on Queue builtin module
"""

import Queue
import thread


class CustomQueue(Queue.Queue):
    def __init__(self, maxsize=0, callback=None):
        Queue.Queue.__init__(self, maxsize)
        self.callback = callback

    def put(self, item, block=True, timeout=None):
        Queue.Queue.put(self, item, block, timeout)

        if not self.callback is None:
            thread.start_new_thread(self.callback, (item,))

    def __str__(self):
        return "%s/%s" % (self.qsize(), self.maxsize)
