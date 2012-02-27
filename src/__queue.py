"""
    File: __queue.py
    Description: Customized Queue based on Queue builtin module
"""

import time
import Queue
import thread


class CustomQueue(Queue.Queue):
    def __init__(self, maxsize=0, callback=None):
        Queue.Queue.__init__(self, maxsize)
        self.callback = callback
        thread.start_new_thread(self.deliverer, ())

    def deliverer(self):
        if self.callback == None:
            return
        while True:
            while self.qsize() > 0:
                self.callback()
            time.sleep(1)

    def __str__(self):
        return "%s/%s" % (self.qsize(), self.maxsize)
