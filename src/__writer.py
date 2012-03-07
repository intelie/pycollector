# -*- coding: utf-8 -*-

"""
    File: __writer.py
    Description: This module implements the logic of a Writer.

    A Writer is responsible to collect messages from the internal
    queue and deliver it to somewhere.

    In order to implement your Writer, you must extend this class
    and write 2 methods, like this:

    ==========
    from __writer import Writer

    class MyWriter(Writer):
        def setup(self):
            ... # Provide here whatever initializations are necessary.
            ... # Properties defined in a conf.yaml will be available
            ... # here as instance variables (e.g., self.foo)

            ... # You may define a list of required confs, e.g.:
            ... self.required_confs = ['foo', 'bar']

            ... # and call validate_conf to check if it was loaded properly
            ... self.validate_conf()

            ... # If they are not found in your conf, an exception is raised

        def write(self, msg):
            ... # Your code goes here.
            ... # You must return a boolean indicating whether the
            ... # message was delivered correctly or not.
    ==========

    There are basically 2 flavors of Writer: asynchronous and synchronous.

    Synchronous Writers are defined with a 'period' in conf.yaml
    Asynchronous Writers are defined WITHOUT a 'period' in conf.yaml.

    For synchronous Writers, write() is called after each 'period'.
    For asynchronous Writers, write() is called as soon a message arrives in
    the queue.
"""

import os
import time
import pickle
import logging
import threading

import helpers.kronos as kronos
from __exceptions import ConfigurationError


class Writer(threading.Thread):
    def __init__(self,
                 queue,                   # source of messages to be written
                 conf={},                 # additional configurations
                 blockable=True,          # stops if a message was not delivered
                 period=None,             # period to read from queue
                 retry_period=1,          # retry period (in seconds) to writing
                 retry_timeout=None,      # if timeout is reached, discard message
                 checkpoint_enabled=False,# deafult is to not deal with checkpoints
                 checkpoint_period=60,    # period of checkpoint writing
                 health_check_period=300, # period to log status
                 thread_name="Writer"     # thread name to easily recognize in log
                 ):
        self.log = logging.getLogger('pycollector')
        self.conf = conf
        self.processed = 0
        self.discarded = 0
        self.queue = queue
        self.blocked = False
        self.period = period
        self.blockable = blockable
        self.retry_period = retry_period
        self.retry_timeout = retry_timeout
        self.thread_name = thread_name
        self.checkpoint_enabled = checkpoint_enabled
        self.health_check_period = health_check_period
        self.set_conf(conf)

        if self.checkpoint_enabled:
            self.last_checkpoint = self._read_checkpoint()
            if not hasattr(self, 'checkpoint_period'):
                self.checkpoint_period = checkpoint_period

        self.setup()

        self.scheduler = kronos.ThreadedScheduler()
        self.schedule_tasks()
        if self.checkpoint_enabled:
            self.schedule_checkpoint_writing()

        threading.Thread.__init__(self, name=self.thread_name)

    def validate_conf(self):
        """Validate if required confs are present.
           required_confs are supposed to be set in setup() method.
        """
        for item in self.required_confs:
            if not hasattr(self, item):
                raise ConfigurationError("%s not defined in your conf.yaml" % item)


    def schedule_checkpoint_writing(self):
        self.scheduler.add_interval_task(self._write_checkpoint,
                                         "checkpoint writing",
                                         0,
                                         self.checkpoint_period,
                                         kronos.method.threaded,
                                         [],
                                         None)

    def _read_checkpoint(self):
        """Read checkpoint file from disk."""
        if not os.path.exists(self.checkpoint_path):
            self.log.info('No checkpoint found in %s.' % self.checkpoint_path)
            open(self.checkpoint_path, 'w').close()
            self.log.info('Created checkpoint file in %s.' % self.checkpoint_path)
            return ''
        try:
            f = open(self.checkpoint_path, 'rb')
            read = pickle.load(f)
            f.close()
            if read:
                return read
            else:
                return ''
            self.log.info("Checkpoint read from %s" % self.checkpoint_path)
        except Exception, e:
            self.log.error('Error reading checkpoint in %s' % self.checkpoint_path)
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
        """Turns configuration properties into instance properties."""
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
            if self.period:
                self.schedule_periodic_task()
            else:
                self.schedule_single_task()
            self.log.info("Tasks scheduled with success")
        except Exception, e:
            self.log.error("Error while scheduling tasks")
            self.log.error(e)

    def schedule_single_task(self):
        self.scheduler.add_single_task(self.async_process,
                                       "single task",
                                       0,
                                       kronos.method.threaded,
                                       [],
                                       None)

    def schedule_periodic_task(self):
        self.scheduler.add_interval_task(self.process,
                                         "periodic task",
                                         0,
                                         self.period,
                                         kronos.method.threaded,
                                         [],
                                         None)

    def retry_writing(self, msg):
        """Blocks writer till a message is written or a timeout is reached"""
        wrote = False
        time_passed = 0
        while True:
            self.log.info("Trying to rewrite message...")
            if self._write(msg.content):
                wrote = True
                self.processed += 1
                self.log.debug("Message written: %s" % msg)
                self.log.info("Rewriting done with success.")
                break
            elif self.retry_timeout and \
                 time_passed > self.retry_timeout:
                 self.discarded += 1
                 self.log.debug("Message: %s discarded due to timeout" % msg)
                 self.log.info("Retry timeout reached. Discarding message...")
                 break
            time.sleep(self.retry_period)
            time_passed += self.retry_period
        return wrote

    def async_process(self):
        """Method that processes (write) a message.
           It waits for new messages from the queue,
           and as soon as one arrives, it tries to deliver it.
        """
        while True:
            try:
                #blocks if none
                msg = self.queue.get()
                wrote = False
                if not self._write(msg.content):
                    if self.blockable:
                        self.blocked = True
                        self.log.warning("Writer blocked.")
                        wrote = self.retry_writing(msg)
                        self.blocked = False
                        self.log.info("Writer unblocked.")
                    else:
                        self.discarded += 1
                        self.log.info("Since it's not blockable, discarding message: %s" % msg)
                else:
                    wrote = True
                    self.processed += 1
                    self.log.debug("Message written: %s" % msg)

                if wrote and self.checkpoint_enabled:
                    self._set_checkpoint(msg.checkpoint)

            except Exception, e:
                self.log.error("Couldn't process message")
                self.log.error(e)

    def process(self):
        """Method called to process (write) a message.
            It is called in the end of each period
            in the case of a periodic task.
            If it's an async writer it is called by a Reader as
            a callback.  So, it may be called by subclasses.
        """ 
        try:
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
                                self.log.debug("Message written: %s" % msg)
                                self.log.info("Rewriting done with success.")
                                break
                            elif self.retry_timeout and \
                                 time_passed > self.retry_timeout:
                                self.discarded += 1
                                self.log.info("Retry timeout reached. Discarding message...")
                                break
                            time.sleep(self.retry_period)
                            time_passed += self.retry_period
                        self.blocked = False
                        self.log.info("Writer unblocked.")
                        self.reschedule_tasks()
                    else:
                        self.discarded += 1
                        self.log.info("Since it's not blockable, discarding message: %s" % msg)
                else:
                    wrote = True
                    self.log.debug("Message written: %s" % msg)

                if wrote:
                    self.processed += 1
                    if self.checkpoint_enabled:
                        self._set_checkpoint(msg.checkpoint)
            else:
                self.log.debug("No messages in the queue to write.")
        except Exception, e:
            self.log.error("Couldn't process message")
            self.log.error(e)

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

    def write(self, msg):
        """Subclasses should implement."""

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
        while True:
            self.log.debug("running")
            time.sleep(self.health_check_period)

    def __str__(self):
        return str(self.__dict__)
