
import time
import threading

import helpers.kronos as kronos


class Writer(threading.Thread):
    def __init__(self, queue, conf={}, blockable=False, interval=None, retry_interval=0):
        """queue: source of messages to write somewhere
           conf: additional configurations
           blockable: stops if a message were not delivered
           interval: interval to read from queue"""

        self.queue = queue
        self.interval = interval
        self.retry_interval = self.interval or 10 
        self.blockable = blockable
        self.processed = 0
        self.blocked = False
        if conf:
            self.set_conf(conf)
        self.setup()
        self.schedule_tasks()
        threading.Thread.__init__(self)

    def set_conf(self, conf):
        """Turns configuration properties
            into instance properties."""
        try:
            for item in conf:
                if isinstance(conf[item], str):
                    exec("self.%s = '%s'" % (item, conf[item]))
                else:
                    exec("self.%s = %s" % (item, conf[item]))
        except Exception, e:
            print "Invalid configuration item: %s" % item
            print e


    def reschedule_tasks(self):
        self.schedule_tasks()
        self.scheduler.start()

    def schedule_tasks(self):
        self.scheduler = kronos.ThreadedScheduler()
        if self.interval:
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
        """Method called to process (write) a message.
            It is called in the end of each interval 
            in the case of a periodic task.
            It it's a async writer it is called by a Reader as
            a callback.
            So, it may be called by subclasses."""

        if self.queue.qsize() > 0:
            msg = self.queue.get()
            if not self._write(msg):
                if self.blockable:
                    self.scheduler.stop()
                    self.blocked = True
                    while not self._write(msg):
                        print "Rewriting..."
                        time.sleep(self.retry_interval)
                    self.blocked = False
                    self.reschedule_tasks()
                else:
                    print "Message [%s] can't be written" % msg
            else:
                print "Message %s written" % msg
                self.processed += 1
        else:
            print "No messages in the queue"

    def setup(self):
        """Subclasses should implement."""
        pass

    def _write(self, msg):
        """Method that calls write method defined by subclasses.
        Shouldn't be called by subclasses."""
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
        """Starts the writer"""
        self.scheduler.start()

