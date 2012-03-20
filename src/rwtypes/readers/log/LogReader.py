import copy
import logging
import datetime
import traceback

from third import filetail

from __reader import Reader
from __message import Message
from __exceptions import ParsingError
from LogUtils import LogUtils


class LogReader(Reader):
    """Conf:
        - logpath (required): path of log file,
            e.g. /tmp/my.log
        - delimiter (optional): character to split log lines,
            e.g. '\t'
        - columns (optional): list of columns for each log line,
            e.g. ['date', 'hour', 'message']"""

    def set_current_datetime(self):
        """Set datetime from the current line"""
        if self.use_datetime_column:
            self.current_datetime = LogUtils.get_datetime(self.current_line,
                                                          self.datetime_column)
        else:
            self.current_datetime = LogUtils.get_datetime(self.current_line,
                                                          self.date_column,
                                                          self.time_column)

    def do_sums(self):
        for i, s in enumerate(self.current_sums):
            current_value = int(self.current_line[s['column_name']])
            current_start_time = s['current']['interval_started_at']
            period = s['interval_duration_sec']

            # starting interval
            if current_start_time == 0:
                start = LogUtils.get_starting_minute(self.current_datetime)
                s['current']['interval_started_at'] = start
                s['current']['value'] = current_value
            else:
                (start, end) = LogUtils.get_interval(current_start_time, period)
                # not in current interval
                if not (start <= self.current_datetime < end):
                    previous = [{'interval_started_at' : s['current']['interval_started_at'],
                                 'value' : s['current']['value']}]
                    zeros = LogUtils.get_missing_intervals(end, period, self.current_datetime)
                    previous.extend([{'interval_started_at' : z, 'value' : 0} for z in zeros])
                    s['previous'] = previous
                    new_start, new_end = LogUtils.get_interval(self.current_datetime, period)
                    s['current']['interval_started_at'] = new_start
                    s['current']['value'] = current_value
                # in current interval
                else:
                    s['previous'] = []
                    s['current']['value'] += current_value
        return True

    def store_sums(self):
        for s in self.current_sums:
            for p in s['previous']:
                content = {'interval_duration_sec' : s['interval_duration_sec'],
                          'interval_started_at' : p['interval_started_at'],
                          'column_name' : s['column_name'],
                          'value' : p['value']}
                content = copy.deepcopy(content)
                if self.checkpoint_enabled:
                    self.store(Message(checkpoint=self.current_checkpoint,
                                       content=content))
                else:
                    self.store(Message(content=content))

        if self.checkpoint_enabled:
            self.current_checkpoint['sums'] = self.current_sums

    def do_counts(self):
        for i, c in enumerate(self.current_counts):
            current_value = self.current_line[c['column_name']]
            current_start_time = c['current']['interval_started_at']
            period = c['interval_duration_sec']

            # starting interval
            if current_start_time == 0:
                start = LogUtils.get_starting_minute(self.current_datetime)
                c['current']['interval_started_at'] = start
                c['current']['value'] = 1 if current_value == c['column_value'] else 0
            else:
                (start, end) = LogUtils.get_interval(current_start_time, period)
                # not in current interval
                if not (start <= self.current_datetime < end):
                    previous = [{'interval_started_at' : c['current']['interval_started_at'],
                                 'value' : c['current']['value']}]
                    zeros  = LogUtils.get_missing_intervals(end, period, self.current_datetime)
                    previous.extend([{'interval_started_at' : z, 'value' : 0} for z in zeros])
                    c['previous'] = previous
                    new_start, new_end = LogUtils.get_interval(self.current_datetime, period)
                    c['current']['interval_started_at'] = new_start
                    c['current']['value'] = 1 if current_value == c['column_value'] else 0
                # in current interval
                else:
                    c['previous'] = []
                    if current_value == c['column_value']: c['current']['value'] += 1
        return True

    def store_counts(self):
        for c in self.current_counts:
            for p in c['previous']:
                content = {'interval_started_at' : p['interval_started_at'],
                           'interval_duration_sec' : c['interval_duration_sec'],
                           'column_name' : c['column_name'],
                           'column_value' : c['column_value'],
                           'value' : p['value']}
                content = copy.deepcopy(content) 
                if self.checkpoint_enabled:
                    self.store(Message(checkpoint=self.current_checkpoint,
                                       content=content))
                else:
                    self.store(Message(content=content))

        if self.checkpoint_enabled:
            self.current_checkpoint['counts'] = self.current_counts

    def recover_checkpoint(self):
        """Seek file if previous checkpoint was found."""
        self.current_checkpoint = self.last_checkpoint or {}
        if 'bytes_read' in self.last_checkpoint:
            self.log.info("Detected checkpoint, seeking file: %s to %s position" %
                          (self.logpath,
                           self.last_checkpoint['bytes_read']))
            self.tail.seek_bytes(self.current_checkpoint['bytes_read'])

    def setup(self):
        # starts the logger
        self.log = logging.getLogger('pycollector')

        # check for required confs
        self.required_confs = ['logpath']
        self.validate_conf()

        # failure recovering from checkpoint
        if self.checkpoint_enabled: self.recover_checkpoint()

        # initializations
        self.to_split = True if hasattr(self, 'delimiter') else False
        self.to_dictify = True if hasattr(self, 'columns') else False
        self.to_sum = True if hasattr(self, 'sums') else False
        self.to_count = True if hasattr(self, 'counts') else False
        self.use_datetime_column = True if hasattr(self, 'datetime_column') else False

        if hasattr(self, 'sums'):
            self.current_sums = LogUtils.initialize_sums(self.sums)

        if hasattr(self, 'counts'):
            self.current_counts = LogUtils.initialize_counts(self.counts)

        # starts tail
        self.tail = filetail.Tail(self.logpath, max_sleep=1, store_pos=True)

    def process_line(self):
        try:
            if self.to_sum or self.to_count:
                self.set_current_datetime()
                if self.to_sum: self.do_sums() and self.store_sums()
                if self.to_count: self.do_counts() and self.store_counts()
                return

            if self.checkpoint_enabled:
                checkpoint = copy.deepcopy(self.current_checkpoint)
                self.store(Message(checkpoint=checkpoint,
                                   content=self.current_line))
            else:
                self.store(Message(content=self.current_line))
        except Exception, e:
            self.log.error("Error processing line")
            self.log.error(traceback.format_exc())

    def get_line(self):
        """Returns a boolean indicating whether the log line
           was successfully read or not"""
        try:
            self.bytes_read, self.current_line = self.tail.nextline()
            if self.checkpoint_enabled:
                self.current_checkpoint['bytes_read'] = self.bytes_read

            if self.to_split and self.to_dictify:
                self.current_line = LogUtils.dictify_line(self.current_line,
                                                      self.delimiter,
                                                      self.columns)
            elif self.to_split:
                self.current_line = LogUtils.split_line(self.current_line,
                                                    self.delimiter)
        except ParsingError, e:
            self.log.error(e.msg)
            return False
        except Exception, e:
            self.log.error(traceback.format_exc())
            return False
        return True

    def read(self):
        if self.period:
            self.get_line() and self.process_line()
            return
        while True:
            self.get_line() and self.process_line()
