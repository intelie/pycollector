
import time
import threading

import helpers.kronos as kronos 


class Reader(threading.Thread):
    def __init__(self, queue, blockable=False, periodic=False, interval=0):
        self.queue = queue
        self.blockable = blockable
        self.scheduler = kronos.ThreadedScheduler()
        if periodic == True:
            self.scheduler.add_interval_task(self.process,
                                 "periodic task",
                                 0,
                                 interval,
                                 kronos.method.threaded,
                                 [],
                                 None)
        else:
            self.scheduler.add_single_task(self.process,
                                           "single task",
                                           0,
                                           kronos.method.threaded,
                                           [],
                                           None)
        threading.Thread.__init__(self)

    def store(self, msg):
        if self.queue.qsize() > 0 and self.blockable:
            while self.queue.qsize() > 0:
                pass
        else:
            self.queue.put(msg)

    def read(self):
        """Subclasses should implement."""
        pass

    def process(self):
        msg = self.read()
        if msg != None:
            self.store(msg)

    def run(self):
        self.scheduler.start()


if __name__ == "__main__":
    class MyReader(Reader):
        def read(self):
            print "know thyself"

    reader = MyReader(periodic=True, interval=1)
    reader.start()

    reader = MyReader(periodic=True, interval=4)
    reader.start()
    while True:
        pass
