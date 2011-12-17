
import time
import threading

import helpers.kronos as kronos


class Reader(threading.Thread):
    def __init__(self, queue, conf=None, writer=None, interval=None):
        """queue: stores read messages
           conf: aditional configurations
           writer: if the writer is async, you should pass an instance
           interval: period of reads"""
        self.queue = queue
        self.interval = interval
        self.writer = writer
        self.setup()

        self.schedule_tasks()
        threading.Thread.__init__(self)

    def schedule_tasks(self):
        self.scheduler = kronos.ThreadedScheduler()
        if self.interval:
            self.schedule_interval_task()
        else:
            self.schedule_single_task()

    def schedule_interval_task(self):
        self.scheduler.add_interval_task(self._process,
                                         "periodic task",
                                         0,
                                         self.interval,
                                         kronos.method.threaded,
                                         [],
                                         None)

    def schedule_single_task(self):
        self.scheduler.add_single_task(self._process,
                                       "single task",
                                       0,
                                       kronos.method.threaded,
                                       [],
                                       None)

    def _writer_callback(self):
        """Callback to writer for non periodic tasks.
           Shouldn't be called by subclasses."""
        if self.writer and not self.writer.interval:
            self.writer.process()

    def _store(self, msg):
        """Internal method to store read messages.
           Shouldn't be called by subclasses."""
        if self.queue.qsize() < self.queue.maxsize:
            self.queue.put(msg)
            self._writer_callback()
        else:
            print "discarding message [%s], full queue" % msg

    def _process(self):
        """Method called internally to process (read) a message.
           It is called in the end of each interval 
           in the case of a periodic task.
           Shouldn't be called by subclasses"""
        msg = self._read()
        if not msg:
            print "discarding message due to an error"
        elif msg and self.interval:
            self.store(msg)

    def _read(self):
        """Internal method that calls read() method. 
           Shouldn't be called by subclasses."""
        try:
            return self.read()
        except Exception, e:
            print "Can't read"
            print e
            return False

    def store(self, msg):
        """Stores a read message. 
           This may be called by subclasses."""
        try:
            self._store(msg)
        except Exception, e:
            print "Can't store in queue"
            print e
            return False

    def setup(self):
        """Subclasses should implement."""
        pass

    def read(self):
        """Subclasses should implement."""
        pass

    def run(self):
        """Starts the reader"""
        self.scheduler.start()

