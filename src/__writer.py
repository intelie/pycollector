import time
import threading

import helpers.kronos as kronos


class Writer(threading.Thread):
    def __init__(self, queue, periodic=True, interval=1):
        self.queue = queue
        self.periodic = periodic
        self.interval = interval
        self.setup()
        self.scheduler = kronos.ThreadedScheduler()
        if self.periodic:
            self.scheduler.add_interval_task(self.process,
                                 "periodic task",
                                 0,
                                 self.interval,
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
            ok = self.write(msg)
            if not ok:
                print "Message [%s] can't be sent" % msg
        else:
            print "No messages in the queue"

    def setup(self):
        """Subclasses should implement."""
        #TODO: make a setup via decorators?
        pass

    def write(self, msg):
        """Subclasses should implement."""
        pass

    def run(self):
        self.scheduler.start()

