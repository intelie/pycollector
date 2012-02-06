# -*- coding: utf-8 -*-


import os
import time
import threading

import helpers.kronos as kronos


class Writer(threading.Thread):
    def __init__(self, 
                 queue,                   # source of messages to be written
                 conf={},                 # additional configurations
                 blockable=False,         # stops if a message were not delivered
                 interval=None,           # interval to read from queue
                 retry_interval=0,        # retry interval (in seconds) to writing
                 retry_timeout=None,      # if timeout is reached, discard message
                 checkpoint_enabled=False,# deafult is to not deal with checkpoints
                 checkpoint_interval=60   # interval of checkpoint writing
                 ):

        self.conf = conf
        self.processed = 0
        self.discarded = 0
        self.queue = queue
        self.interval = interval
        self.retry_interval = retry_interval 
        self.blockable = blockable
        self.blocked = False 
        self.checkpoint_enabled = checkpoint_enabled

        self.set_conf(conf)
        self.setup()
   
        self.schedule_tasks()

        if self.checkpoint_enabled: 
            self.last_checkpoint = self._read_checkpoint()
            if not hasattr(self, 'checkpoint_interval'):
                self.checkpoint_interval = checkpoint_interval
            if not hasattr(self, 'checkpoint_path'):
                print 'Error. Please, configure a checkpoint_path'
                exit(-1)
            self.schedule_checkpoint_writing()

        threading.Thread.__init__(self)

    def schedule_checkpoint_writing(self):
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
            read = open(self.checkpoint_path, 'r+').read()
            if read:
                return read 
            else:
                return ''
        except Exception, e:
            print 'Error reading writer checkpoint'
            print e

    def _write_checkpoint(self):
        """Write checkpoint in disk."""
        try:
            lc = self.last_checkpoint
            f = open(self.checkpoint_path, 'w+')
            f.write(lc.__str__() or '')
            f.close()
            print 'checkpoint [%s] written' % lc
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

            wrote = False

            if not self._write(msg.content):
                if self.blockable:
                    self.scheduler.stop()
                    self.blocked = True
                    time_passed = 0

                    while True:
                        if self._write(msg):
                            wrote = True
                            break
                        elif self.retry_timeout and \
                             time_passed > self.retry_timeout:
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
                wrote = True

            if wrote:
                print "Message [%s] written" % msg
                self.processed += 1
                if self.checkpoint_enabled:
                    self._set_checkpoint(msg.checkpoint)
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

    def _set_checkpoint(self, checkpoint):
        try:
            self.last_checkpoint = checkpoint 
        except Exception, e:
            print 'error updating last_checkpoint'
            print e

    def run(self):
        """Starts the writer"""
        self.scheduler.start()

    def __str__(self):
        return str(self.__dict__)
