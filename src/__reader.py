# -*- coding:utf-8 -*-

import time
import pickle
import logging
import threading

import helpers.kronos as kronos

from __exceptions import ConfigurationError


class Reader(threading.Thread):
    def __init__(self,
                 queue,                   # stores read messages
                 conf={},                 # additional configurations
                 writer=None,             # if writer is async, it must be provided
                 interval=None,           # interval of readings
                 blockable=True,          # stops if a message was not stored
                 retry_interval=0,        # retry interval (in seconds) to store
                 retry_timeout=None,      # if timeout is reached, discard message
                 checkpoint_enabled=False,# default is to not deal with checkpoint 
                 checkpoint_interval=60   # interval between checkpoints
                 ):
        self.log = logging.getLogger()
        self.conf = conf
        self.processed = 0
        self.discarded = 0
        self.queue = queue
        self.blocked = False
        self.writer = writer
        self.interval = interval
        self.blockable = blockable
        self.retry_timeout = retry_timeout
        self.retry_interval = retry_interval
        self.checkpoint_enabled = checkpoint_enabled
        self.set_conf(conf)

        if self.checkpoint_enabled:
            self.last_checkpoint = ''
            if writer and \
                writer.last_checkpoint:
                self.last_checkpoint = writer.last_checkpoint
            if not hasattr(self, 'checkpoint_interval'):
                self.checkpoint_interval = checkpoint_interval
            if not hasattr(self, 'checkpoint_path'):
                self.log.error('Please, configure a checkpoint_path.')
                self.log.info('Aborting.')
                exit(-1)

        self.setup()

        if hasattr(self, 'required_confs'):
            self.validate_conf()

        self.scheduler = kronos.ThreadedScheduler()
        self.schedule_tasks()
        if self.checkpoint_enabled:
            self.schedule_checkpoint_writing()

        threading.Thread.__init__(self)

    def validate_conf(self):
        for item in self.required_confs:
            if not hasattr(self, item):
                self.log.error('%s not defined in conf.yaml' % item)
                self.log.info('Aborting')
                raise ConfigurationError()

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
        """Turns configuration properties 
           into instance properties."""
        try:
            for item in conf:
                if isinstance(conf[item], str):
                    exec("self.%s = '%s'" % (item, conf[item]))
                else:
                    exec("self.%s = %s" % (item, conf[item]))
            self.log.info("Configuration settings added with success into reader.")
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
            else:
                self.schedule_single_task()
            self.log.info("Tasks scheduled with success")
        except Exception, e:
            self.log.error("Error while scheduling task")
            selg.log.error(e)

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
            self.log.debug("Calling writer_callback")
            self.writer.process()
        except Exception, e:
            self.log.error('Error when executing writer_callback')
            self.log.error(e)

    def _store(self, msg):
        """Internal method to store read messages.
           Shouldn't be called by subclasses."""
        success = False
        try: 
            if not self.queue.full():
                self.queue.put(msg)
                self.processed += 1
                self.log.debug("Stored message: %s" % msg)
                success = True
            else:
                if self.blockable:
                    self.blocked = True
                    self.scheduler.stop()
                    self.log.warning("Reader blocked.")

                    time_passed = 0
                    while True:
                        self.log.info("Waiting for a slot in queue...")
                        if not self.queue.full():
                            self.queue.put(msg)
                            self.processed += 1
                            self.log.debug("Stored message: %s" % msg )
                            success = True
                            break
                        elif self.retry_timeout and \
                            time_passed > self.retry_timeout:
                            self.discarded += 1 
                            self.log.debug("Discarded message: %s, full queue" % msg) 
                            break
                        time.sleep(self.retry_interval)
                        time_passed += self.retry_interval

                    self.blocked = False
                    self.log.info("Reader unblocked.")
                    self.reschedule_tasks()
                else:
                    self.discarded += 1
                    self.log.debug("Discarded message: %s, full queue" % msg)
        except Exception, e:
            self.log.error("Can't store in queue, message %s" % msg)
            self.log.error(e)

        if success:
            if self.checkpoint_enabled:
                self._set_checkpoint(msg.checkpoint)

            if self.writer and not self.writer.interval:
                self._writer_callback()
        return success

    def _process(self):
        """Method called internally to process (read) a message.
           It is called in the end of each interval 
           in the case of a periodic task.
           Shouldn't be called by subclasses"""
        if not self._read():
            self.log.info("Message can't be read")

    def _read(self):
        """Internal method that calls read() method. 
           Shouldn't be called by subclasses."""
        try:
            return self.read()
        except Exception, e:
            self.log.error(e)

    def _set_checkpoint(self, checkpoint):
        """Wrapper method to set_checkpoint (user defined)
           to get exceptions"""
        try:
            self.last_checkpoint = checkpoint
            self.log.debug("Last checkpoint: %s" % checkpoint)
        except Exception, e:
            self.log.error('Error setting checkpoint')
            self.log.error(e)

    def store(self, msg):
        """Stores a read message. 
           This should be called by subclasses."""
        return self._store(msg)

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
