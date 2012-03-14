import copy
import pprint
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
        return [{'interval_started_at': 0,
                 'interval_duration_sec': s['period']*60,
                 'column_name': s['column'],
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

    def do_sums(self):
        if not hasattr(self, 'sums'):
            return

        for s in self.sums:
            if hasattr(self, 'datetime_column'):
                dt = self.get_datetime(self.current_line,
                                       self.datetime_column)
            else:
                dt = self.get_datetime(self.current_line,
                                       self.date_column,
                                       self.time_column)



            # to be continued...

    def process_line(self):
        try:
            self.do_sums()
        except Exception, e:
            self.log.error("Error processing sums")
            self.log.error(traceback.format_exc())

        # store it
        try:
            if self.checkpoint_enabled:
                self.store(Message(checkpoint=copy.deepcopy(self.current_checkpoint),
                                   content=self.current_line))
            else:
                self.store(Message(content=self.current_line))
        except Exception, e:
            self.log.error("Error storing line in queue.")
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
        if hasattr(self, 'delimiter'):
            self.to_split = True
        if hasattr(self, 'columns'):
            self.to_dictify = True

        self.tail = filetail.Tail(self.logpath, max_sleep=1, store_pos=True)
        self.set_checkpoint()

        if hasattr(self, 'sums'):
            self.current_sums = self.initialize_sums(self.sums)

    def read(self):
        if self.period and self.get_line():
            self.process_line()
        else:
            while True:
                if self.get_line():
                    self.process_line()
