# -*- coding: utf-8 -*-

import os
import time
import pickle
import logging
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
        self.log = logging.getLogger()
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

        if self.checkpoint_enabled: 
            self.last_checkpoint = self._read_checkpoint()
            if not hasattr(self, 'checkpoint_interval'):
                self.checkpoint_interval = checkpoint_interval
            if not hasattr(self, 'checkpoint_path'):
                self.log.error('Error. Please, configure a checkpoint_path')
                self.log.info('Aborting.')
                exit(-1)

        self.setup()

        self.scheduler = kronos.ThreadedScheduler()
        self.schedule_tasks()
        if self.checkpoint_enabled:
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
            if not os.path.exists(self.checkpoint_path):
                self.log.info('No checkpoint found in %s.' % self.checkpoint_path)
                return ''
            f = open(self.checkpoint_path, 'rb')
            read = pickle.load(f)
            f.close()
            if read:
                return read 
            else:
                return ''
            self.log.info("Checkpoint read from %s" % self.checkpoint_path)
        except Exception, e:
            self.log.error('Error reading checkpoint in %s' % self.checkcpoint_path)
            self.log.error(e)

    def _write_checkpoint(self):
        """Write checkpoint in disk."""
        try:
            lc = self.last_checkpoint
            f = open(self.checkpoint_path, 'w')
            pickle.dump(lc, f)
            f.close()
            self.log.info('Checkpoint written: %s' % lc)
        except Exception, e:
            self.log.error('Error writing checkpoint in %s' % self.checkpoint_path)
            self.log.error(e)

    def set_conf(self, conf):
        """Turns configuration properties
            into instance properties."""
        try:
            for item in conf:
                if isinstance(conf[item], str):
                    exec("self.%s = '%s'" % (item, conf[item]))
                else:
                    exec("self.%s = %s" % (item, conf[item]))
            self.log.info("Configuration settings added with success into writer.")
        except Exception, e:
            self.log.error("Invalid configuration item: %s" % item)
            self.log.error(e)

    def reschedule_tasks(self):
        try:
            self.scheduler = kronos.ThreadedScheduler()
            self.schedule_tasks()
            if self.checkpoint_enabled:
                self.schedule_checkpoint_writing()
            self.scheduler.start()
            self.log.info("Success in rescheduling")
        except Exception, e:
            self.log.error("Error while rescheduling tasks")
            self.log.error(e)

    def schedule_tasks(self):
        try:
            if self.interval:
                self.schedule_interval_task()
        except Exception, e:
            self.log.error("Error while scheduling tasks")
            self.log.error(e)

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
                    self.log.warning("Writer blocked.")

                    time_passed = 0
                    while True:
                        self.log.info("Trying to rewrite message...")
                        if self._write(msg.content):
                            wrote = True
                            self.log.info("Rewriting done with success.")
                            break   
                        elif self.retry_timeout and \
                             time_passed > self.retry_timeout:
                            self.discarded += 1
                            self.log.info("Retry timeout reached. Discarding message...")
                            break
                        time.sleep(self.retry_interval)
                        time_passed += self.retry_interval 
                    self.blocked = False
                    self.log.info("Writer unblocked.")
                    self.reschedule_tasks()
                else:
                    self.discarded += 1
                    self.log.info("Since it's not blockable, discarding message: %s" % msg)
            else:
                wrote = True

            if wrote:
                self.log.debug("Message written: %s" % msg)
                self.processed += 1
                if self.checkpoint_enabled:
                    self._set_checkpoint(msg.checkpoint)
        else:
            self.log.info("No messages in the queue to write.")

    def _write(self, msg):
        """Method that calls write method defined by subclasses.
        Shouldn't be called by subclasses."""
        try:
            return self.write(msg)
        except Exception, e:
            self.log.error("Can't write message: %s" % msg)
            self.log.error(e)
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
            self.log.debug("Last checkpoint: %s" %checkpoint)
        except Exception, e:
            self.log.error('Error updating last_checkpoint')
            self.log.error(e)

    def run(self):
        """Starts the writer"""
        self.scheduler.start()

    def __str__(self):
        return str(self.__dict__)
