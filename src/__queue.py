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
        self.n = 0
        self.deliverer_running = False

    def deliverer(self):
        if not self.callback is None:
            while self.n > 0:
                self.callback()
                self.n -= 1
        self.deliverer_running = False

    def put(self, item, block=True, timeout=None):
        Queue.Queue.put(self, item, block, timeout)

        self.n += 1

        if not self.deliverer_running:
            self.deliverer_running = True
            thread.start_new_thread(self.deliverer, ())

    def __str__(self):
        return "%s/%s" % (self.qsize(), self.maxsize)
