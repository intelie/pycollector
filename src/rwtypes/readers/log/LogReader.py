import copy
import logging
import datetime
import traceback

from third import filetail
from helpers.dateutil import parser

from __reader import Reader
from __message import Message
from __exceptions import ParsingError


class LogReader(Reader):
    """Conf:
        - logpath (required): path of log file,
            e.g. /tmp/my.log
        - delimiter (optional): character to split log lines,
            e.g. '\t'
        - columns (optional): list of columns for each log line,
            e.g. ['date', 'hour', 'message']"""

    @classmethod
    def get_missing_intervals(cls, start_datetime, period, event_datetime):
        try:
            intervals = []
            (start, end) = cls.get_interval(start_datetime, period)
            while event_datetime >= end:
                intervals.append(start)
                (start, end) = cls.get_interval(end, period)
            return intervals
        except Exception, e:
            raise ParsingError("Error calculating missing intervals.")

    @classmethod
    def get_interval(cls, dt, period):
        try:
            start = cls.get_starting_minute(dt)
            end = start + datetime.timedelta(0, period)
            return (start, end)
        except Exception, e:
            raise ParsingError("Can't get period from datetime: %s and period: %s" % (dt, period))

    @classmethod
    def get_starting_minute(cls, dt):
        """Removes seconds from datetime object"""
        try:
            return dt - datetime.timedelta(0, dt.second)
        except Exception, e:
            raise ParsingError("Can't get starting minute from %s" % dt)

    @classmethod
    def get_datetime(cls, dictified_line, column1, column2=None):
        """Get datetime object from dictionary and date time columns"""
        try:
            dt = dictified_line[column1]
            if column2 != None:
                dt += ' %s' % dictified_line[column2]
            return parser.parse(dt, fuzzy=True)
        except Exception, e:
            raise ParsingError("Error parsing datetime for %s" % dictified_line)

    @classmethod
    def dictify_line(cls, line, delimiter, columns):
        """Dictify a log line mapping columns with values separated
        by a delimiter"""
        err = ''
        try:
            splitted = cls.split_line(line, delimiter)
            if len(splitted) != len(columns):
                err += "Different number of columns."
                raise Exception
            return dict(zip(columns, splitted))
        except Exception, e:
            raise ParsingError("Error parsing line: %s. %s" % (line, err))

    @classmethod
    def split_line(cls, line, delimiter):
        """Return a list of values splited by a delimiter"""
        try:
            return line.strip().split(delimiter)
        except Exception, e:
            raise ParsingError("Error parsing line: %s" % line)

    @classmethod
    def initialize_sums(self, conf):
        sums = []
        for s in conf:
            common = {'interval_started_at' : 0,
                      'interval_duration_sec' : s['period']*60,
                      'column_name' : s['column'],}
            if 'group_by' in s:
                to_add = {'group_by': s['group_by'],
                          'grouped_values': {},
                          'grouped_remaining': {},
                          'grouped_zeros': {}}
            else:
                to_add = {'value' : 0,
                          'remaining': {},
                          'zeros' : []}
            common.update(to_add)
            sums.append(common)
        print sums
        return sums


    @classmethod
    def initialize_counts(self, conf):
        return [{'interval_started_at': 0,
                 'interval_duration_sec': s['period']*60,
                 'column_name': s['column'],
                 'column_value': s['match'],
                 'remaining': {},
                 'zeros': [],
                 'value' : 0} for s in conf]

    def get_line(self):
        """Returns a boolean indicating whether the log line
           was successfully read or not"""
        try:
            self.bytes_read, self.current_line = self.tail.nextline()
            if self.checkpoint_enabled:
                self.current_checkpoint['bytes_read'] = self.bytes_read

            if self.to_split and self.to_dictify:
                self.current_line = self.dictify_line(self.current_line,
                                                      self.delimiter,
                                                      self.columns)
            elif self.to_split:
                self.current_line = self.split_line(self.current_line,
                                                    self.delimiter)
        except ParsingError, e:
            self.log.error(e.msg)
            return False
        except Exception, e:
            self.log.error(traceback.format_exc())
            return False
        return True

    def set_current_datetime(self):
        """Set datetime from the current line"""
        try:
            if hasattr(self, 'datetime_column'):
                self.current_datetime = self.get_datetime(self.current_line,
                                                          self.datetime_column)
            else:
                self.current_datetime = self.get_datetime(self.current_line,
                                                          self.date_column,
                                                          self.time_column)
        except Exception, e:
            raise ParsingError("Can't set datetime from line %s" % self.current_line)

    def do_sums(self):
        for i, s in enumerate(self.current_sums):
            current_value_to_sum = int(self.current_line[s['column_name']])
            last_start_time = s['interval_started_at']
            sum_period = s['interval_duration_sec']

            # starting interval
            if last_start_time == 0:
                start = self.get_starting_minute(self.current_datetime)
                s['interval_started_at'] = start
                self.current_sums[i]['value'] += current_value_to_sum
            else:
                (start, end) = self.get_interval(last_start_time, sum_period)
                # not in interval
                if not (start <= self.current_datetime < end):
                    s['remaining']['interval_started_at'] = s['interval_started_at']
                    s['remaining']['value'] = s['value']
                    s['zeros'] = self.get_missing_intervals(end, sum_period, self.current_datetime)
                    new_start, new_end = self.get_interval(self.current_datetime, sum_period)
                    s['interval_started_at'] = new_start
                    s['value'] = current_value_to_sum
                # in interval
                else:
                    s['remaining'] = {}
                    s['zeros'] = []
                    self.current_sums[i]['value'] += current_value_to_sum

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

    def process_line(self):
        try:
            if self.to_sum:
                self.set_current_datetime()
                self.do_sums()
                self.store_sums()
            if self.to_count:
                self.set_current_datetime()
                self.do_counts()
                self.store_counts()
            if not self.to_sum and not self.to_count:
                if self.checkpoint_enabled:
                    checkpoint = copy.deepcopy(self.current_checkpoint)
                    self.store(Message(checkpoint=checkpoint,
                                       content=self.current_line))
                else:
                    self.store(Message(content=self.current_line))
        except Exception, e:
            self.log.error("Error processing line")
            self.log.error(traceback.format_exc())

    def set_checkpoint(self):
        if self.checkpoint_enabled:
            self.current_checkpoint = self.last_checkpoint or {}
            if 'bytes_read' in self.last_checkpoint:
                self.log.info("Detected checkpoint, seeking file: %s to %s position" %
                              (self.logpath,
                               self.last_checkpoint['bytes_read']))
                self.tail.seek_bytes(self.current_checkpoint['bytes_read'])

    def setup(self):
        self.log = logging.getLogger('pycollector')

        self.required_confs = ['logpath']
        self.validate_conf()

        self.to_dictify = False
        self.to_split = False
        self.to_sum = False
        self.to_count = False
        if hasattr(self, 'delimiter'):
            self.to_split = True
        if hasattr(self, 'columns'):
            self.to_dictify = True
        if hasattr(self, 'sums'):
            self.to_sum = True
        if hasattr(self, 'counts'):
            self.to_count = True

        self.tail = filetail.Tail(self.logpath, max_sleep=1, store_pos=True)
        self.set_checkpoint()

        if hasattr(self, 'sums'):
            self.current_sums = self.initialize_sums(self.sums)

        if hasattr(self, 'counts'):
            self.current_counts = self.initialize_counts(self.counts)

    def read(self):
        if self.period and self.get_line():
            self.process_line()
        else:
            while True:
                if self.get_line():
                    self.process_line()
