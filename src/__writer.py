# -*- coding: utf-8 -*-

import os
import time
import threading

import helpers.kronos as kronos


class Writer(threading.Thread):
    def __init__(self, 
                 queue,                 # source of messages to be written
                 conf={},               # additional configurations
                 blockable=False,       # stops if a message were not delivered
                 interval=None,         # interval to read from queue
                 retry_interval=0,      # retry interval (in seconds) to writing
                 retry_timeout=None,    # if timeout is reached, discard message
                 checkpoint_path=None,  # filepath to write checkpoint
                 checkpoint_interval=60 # interval of checkpoint writing
                 ):
        self.conf = conf
        self.queue = queue
        self.interval = interval
        self.retry_interval = self.interval or 10
        self.blockable = blockable
        self.processed = 0
        self.discarded = 0
        self.blocked = False
        self.checkpoint_path = checkpoint_path
        self.checkpoint_interval = checkpoint_interval

        if conf:
            self.set_conf(conf)

        self.setup()

        self.last_checkpoint = None
        self.current_checkpoint = None

        if self.checkpoint_path:
            self.last_checkpoint = self._read_checkpoint()

        self.schedule_tasks()
        self.schedule_checkpoint_writing()

        threading.Thread.__init__(self)

    def schedule_checkpoint_writing(self):
        if self.checkpoint_interval and self.checkpoint_path:
            self.scheduler.add_interval_task(self._write_checkpoint,
                                             "checkpoint writing",
                                             0,
                                             self.checkpoint_interval,
                                             kronos.method.threaded,
                                             [],
                                             None)

    def _read_checkpoint(self):
        """Read checkpoint file from disk."""
        try:
            if os.path.exists(self.checkpoint_path):
                return open(self.checkpoint_path, 'r+').read()
            else:
                return None
        except Exception, e:
            print 'Error reading writer checkpoint'
            print e

    def _write_checkpoint(self):
        """Write checkpoint in disk."""
        try:
            f = open(self.checkpoint_path, 'w+')
            f.write(self.last_checkpoint.__str__())
            f.close()
        except Exception, e:
            print 'Error writing checkpoint in %s' % self.checkpoint_path
            print e 

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
            If it's an async writer it is called by a Reader as
            a callback.
            So, it may be called by subclasses."""

        if self.queue.qsize() > 0:
            msg = self.queue.get()

            self.current_checkpoint = msg.checkpoint 

            if not self._write(msg.content):
                if self.blockable:
                    self.scheduler.stop()
                    self.blocked = True
                    time_passed = 0
                    while not self._write(msg):
                        if (self.retry_timeout and self.time_passed > self.retry_timeout):
                            self.discarded += 1
                            break
                        time.sleep(self.retry_interval)
                        time_passed += self.retry_interval 
                        print "Rewriting..."
                    self.blocked = False
                    self.reschedule_tasks()
                else:
                    self.discarded += 1
                    print "Message [%s] can't be written" % msg
            else:
                print "Message [%s] written" % msg
                self.processed += 1
                if self.checkpoint_path:
                    self._update_last_checkpoint()
        else:
            print "No messages in the queue to write"

    def _write(self, msg):
        """Method that calls write method defined by subclasses.
        Shouldn't be called by subclasses."""
        try:
            return self.write(msg)
        except Exception, e:
            print "Can't write message %s" % msg
            print e
            return False

    def setup(self):
        """Subclasses should implement."""
        pass

    def write(self, msg):
        """Subclasses should implement."""
        pass

    def _update_last_checkpoint(self):
        try:
            self.last_checkpoint = self.current_checkpoint
        except Exception, e:
            print 'error updating last_checkpoint'
            print e

    def _set_checkpoint(self, msg):
        """Wrapper method to set_checkpoint (user defined)
           to get exceptions"""
        try:
            self.set_checkpoint(msg)
        except Exception, e:
            print 'Error in setting checkpoint'
            print e

    def set_checkpoint(self, checkpoint):
        """Subclasses should call it from write() method"""
        self.current_checkpoint = checkpoint

    def run(self):
        """Starts the writer"""
        self.scheduler.start()

    def __str__(self):
        return str(self.__dict__)
