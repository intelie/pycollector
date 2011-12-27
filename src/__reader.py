# -*- coding:utf-8 -*-

import time
import threading

import helpers.kronos as kronos


class Reader(threading.Thread):
    def __init__(self,
                 queue,        # stores read messages
                 conf={},      # additional configurations
                 writer=None,  # if writer is async, it should be provided
                 interval=None # interval of readings
                 ):
        if conf:
            self.set_conf(conf)
        self.interval = interval
        self.processed = 0
        self.queue = queue
        self.writer = writer
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
            print "Invalid configuration item: %s " % item

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
            self.processed += 1
        else:
            print "discarding message [%s], full queue" % msg

    def _process(self):
        """Method called internally to process (read) a message.
           It is called in the end of each interval 
           in the case of a periodic task.
           Shouldn't be called by subclasses"""
        if not self._read():
            print "Message can't be read"

    def _read(self):
        """Internal method that calls read() method. 
           Shouldn't be called by subclasses."""
        try:
            return self.read()
        except Exception, e:
            print e

    def store(self, msg):
        """Stores a read message. 
           This should be called by subclasses."""
        try:
            self._store(msg)
        except Exception, e:
            print "Can't store in queue, message %s" % msg
            print e
            return False
        try:
            self._writer_callback()
        except Exception, e:
            print "Error when calling writer_callback"
            print e

    def setup(self):
        """Subclasses should implement."""
        pass

    def read(self):
        """Subclasses should implement."""
        pass

    def run(self):
        """Starts the reader"""
        self.scheduler.start()

