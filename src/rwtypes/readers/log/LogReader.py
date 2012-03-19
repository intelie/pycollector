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
        try:
            if hasattr(self, 'datetime_column'):
                self.current_datetime = LogUtils.get_datetime(self.current_line,
                                                          self.datetime_column)
            else:
                self.current_datetime = LogUtils.get_datetime(self.current_line,
                                                          self.date_column,
                                                          self.time_column)
        except Exception, e:
            raise ParsingError("Can't set datetime from line %s" % self.current_line)

    def do_sums(self):
        for i, s in enumerate(self.current_sums):
            current_value_to_sum = int(self.current_line[s['column_name']])
            current_start_time = s['current']['interval_started_at']
            sum_period = s['interval_duration_sec']

            # starting interval
            if current_start_time == 0:
                start = self.get_starting_minute(self.current_datetime)
                s['current']['interval_started_at'] = start
                s['current']['value'] = current_value_to_sum
            else:
                (start, end) = self.get_interval(current_start_time, sum_period)
                # not in interval
                if not (start <= self.current_datetime < end):
                    previous = []
                    previous.append({'interval_started_at' : s['interval_started_at'],
                                     'value' : s['value']})
                    zeros = LogUtils.get_missing_intervals(end, sum_period, self.current_datetime)
                    previous.extend([{'interval_started_at' : z, 'value' : 0} for z in zeros])
                    new_start, new_end = LogUtils.get_interval(self.current_datetime, sum_period)
                    s['current']['interval_started_at'] = new_start
                    s['current']['value'] = current_value_to_sum
                # in interval
                else:
                    s['previous'] = []
                    s['current']['value'] += current_value_to_sum

    def store_sums(self):
        for s in self.current_sums:
            to_send = []
            if len(s['remaining']) > 0:
                event = s['remaining']
                event['interval_duration_sec'] = s['interval_duration_sec']
                event['column_name'] = s['column_name']
                to_send.append(copy.deepcopy(event))
            for z in s['zeros']:
                event = {}
                event['interval_started_at'] = z
                event['interval_duration_sec'] = s['interval_duration_sec']
                event['column_name'] = s['column_name']
                event['value'] = 0
                to_send.append(copy.deepcopy(event))
            contents = to_send
            if self.checkpoint_enabled:
                self.current_checkpoint['sums'] = self.current_sums
                for content in contents:
                    self.store(Message(content=content,
                                       checkpoint=self.current_checkpoint))
            else:
                for content in contents:
                    self.store(Message(content=content))

    def do_counts(self):
        # TODO: merge with do_sums in one function
        for i, s in enumerate(self.current_counts):
            current_value = self.current_line[s['column_name']]
            last_start_time = s['interval_started_at']
            count_period = s['interval_duration_sec']

            # starting interval
            if last_start_time == 0:
                start = self.get_starting_minute(self.current_datetime)
                s['interval_started_at'] = start
                # TODO: apply regexp
                if current_value == s['column_value']:
                    s['value'] += 1
            else:
                (start, end) = self.get_interval(last_start_time, count_period)
                # not in interval
                if not (start <= self.current_datetime < end):
                    s['remaining']['interval_started_at'] = s['interval_started_at']
                    s['remaining']['value'] = s['value']
                    s['zeros'] = self.get_missing_intervals(end, count_period, self.current_datetime)
                    new_start, new_end = self.get_interval(self.current_datetime, count_period)
                    s['interval_started_at'] = new_start
                    if current_value == s['column_value']:
                        s['value'] = 1
                    else:
                        s['value'] = 0
                # in interval
                else:
                    s['remaining'] = {}
                    s['zeros'] = []
                    # TODO: apply regexp
                    if current_value == s['column_value']:
                        s['value'] += 1

    def store_counts(self):
        # TODO: merge with store_sums in one function
        for s in self.current_counts:
            to_send = []
            if len(s['remaining']) > 0:
                event = s['remaining']
                event['interval_duration_sec'] = s['interval_duration_sec']
                event['column_name'] = s['column_name']
                event['column_value'] = s['column_value']
                to_send.append(copy.deepcopy(event))
            for z in s['zeros']:
                event = {}
                event['interval_started_at'] = z
                event['interval_duration_sec'] = s['interval_duration_sec']
                event['column_name'] = s['column_name']
                event['column_value'] = s['column_value']
                event['value'] = 0
                to_send.append(copy.deepcopy(event))
            contents = to_send
            if self.checkpoint_enabled:
                self.current_checkpoint['counts'] = self.current_counts
                for content in contents:
                    self.store(Message(content=content,
                                       checkpoint=self.current_checkpoint))
            else:
                for content in contents:
                    self.store(Message(content=content))

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
