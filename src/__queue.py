"""
    File: __queue.py
    Description: Customized Queue based on Queue builtin module
"""

import time
import Queue
import thread
import logging


class CustomQueue(Queue.Queue):
    def __init__(self, maxsize=0, callback=None):
        self.log = logging.getLogger() 
        Queue.Queue.__init__(self, maxsize)
        self.set_callback(callback)

    def set_callback(self, callback):
        self.callback = callback
        thread.start_new_thread(self.deliverer, ())

    def deliverer(self):
        while True:
            try:
                while self.qsize() > 0:
                    self.callback()
            except Exception, e:
                self.log.error(e)
            time.sleep(1)

    def __str__(self):
        return "%s/%s" % (self.qsize(), self.maxsize)
