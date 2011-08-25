
import time
import threading

import helpers.kronos as kronos


class Writer(threading.Thread):
    def __init__(self, queue, periodic=True, interval=1):
        self.queue = queue
        self.periodic = periodic
        self.interval = interval
        self.scheduler = kronos.ThreadedScheduler()
        if self.periodic:
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

    def process(self):
        if self.queue.qsize() > 0:
            msg = self.queue.get()
            self.write(msg)

    def write(self, msg):
        """Subclasses should implement."""
        pass

    def run(self):
        self.scheduler.start()


if __name__ == "__main__":
    class MyWriter(Writer):
        def write(self):
            print "know thyself"

    writer = MyWriter(periodic=True, interval=1)
    writer.start()

    while True:
        pass

