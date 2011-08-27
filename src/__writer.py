import time
import threading

import helpers.kronos as kronos


class Writer(threading.Thread):
    def __init__(self, queue, blockable=False, periodic=True, interval=1):
        self.queue = queue
        self.periodic = periodic
        self.interval = interval
        self.blockable = blockable
        self.setup()
        self.schedule_tasks()
        threading.Thread.__init__(self)

    def reschedule_tasks(self):
        self.schedule_tasks()
        self.scheduler.start()

    def schedule_tasks(self):
        self.scheduler = kronos.ThreadedScheduler()
        if self.periodic:
            self.schedule_interval_task()

    def schedule_interval_task(self):
        self.scheduler.add_interval_task(self.process,
                                         "periodic task",
                                         0,
                                         self.interval,
                                         kronos.method.threaded,
                                         [],
                                         None)

    def process(self):
        if self.queue.qsize() > 0:
            msg = self.queue.get()
            if not self._write(msg):
                if self.blockable:
                    self.scheduler.stop()
                    while not self._write(msg):
                        print "Rewriting..."
                    self.reschedule_tasks()
                else:
                    print "Message [%s] can't be sent" % msg
        else:
            print "No messages in the queue"

    def setup(self):
        """Subclasses should implement."""
        pass

    def _write(self, msg):
        try:
            return self.write(msg)
        except Exception, e:
            print "Can't write"
            print e
            return False

    def write(self, msg):
        """Subclasses should implement."""
        pass

    def run(self):
        self.scheduler.start()

