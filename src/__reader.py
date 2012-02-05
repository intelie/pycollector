# -*- coding:utf-8 -*-

import time
import threading

import helpers.kronos as kronos


class Reader(threading.Thread):
    def __init__(self,
                 queue,                   # stores read messages
                 conf={},                 # additional configurations
                 writer=None,             # if writer is async, it must be provided
                 interval=None,           # interval of readings
                 checkpoint_enabled=False,# default is to not deal with checkpoint 
                 checkpoint_interval=60   # interval between checkpoints
                 ):

        self.conf = conf
        self.processed = 0
        self.discarded = 0
        self.queue = queue
        self.writer = writer
        self.interval = interval
        self.checkpoint_enabled = checkpoint_enabled

        self.set_conf(conf)
        self.setup()

        self.schedule_tasks()

        if self.checkpoint_enabled:
            self.last_checkpoint = ''
            if writer and \
                writer.last_checkpoint:
                self.last_checkpoint = writer.last_checkpoint
            if not hasattr(self, 'checkpoint_interval'):
                self.checkpoint_interval = checkpoint_interval
            if not hasattr(self, 'checkpoint_path'):
                print 'Error. Please, configure a checkpoint_path'
                exit(-1)
            self.schedule_checkpoint_writing()

        threading.Thread.__init__(self)

    def schedule_checkpoint_writing(self):
        if self.checkpoint_enabled:
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
            return open(self.checkpoint_path, 'r+').read()
        except Exception, e:
            print 'Error reading checkpoint'
            print e

    def _write_checkpoint(self):
        """Write checkpoint in disk."""
        try:
            if self.checkpoint_enabled:
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
        try:
            self.writer.process()
        except Exception, e:
            print 'Error when executing writer_callback'
            print e

    def _store(self, msg):
        """Internal method to store read messages.
           Shouldn't be called by subclasses."""
        try: 
            if not self.queue.full():
                self.queue.put(msg)
                self.processed += 1
            else:
                self.discarded += 1
                print "discarding message [%s], full queue" % msg
        except Exception, e:
            print "can't store in queue, message %s" % msg
            print e

        if self.checkpoint_enabled:
            self._set_checkpoint(msg.checkpoint)

        if self.writer and not self.writer.interval:
            self._writer_callback()


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

    def _set_checkpoint(self, checkpoint):
        """Wrapper method to set_checkpoint (user defined)
           to get exceptions"""
        try:
            self.last_checkpoint = checkpoint
        except Exception, e:
            print 'Error setting checkpoint'
            print e

    def store(self, msg):
        """Stores a read message. 
           This should be called by subclasses."""
        self._store(msg)

    def setup(self):
        """Subclasses should implement."""
        pass

    def read(self):
        """Subclasses should implement."""
        pass

    def run(self):
        """Starts the reader"""
        self.scheduler.start()

    def __str__(self):
        return str(self.__dict__) 
