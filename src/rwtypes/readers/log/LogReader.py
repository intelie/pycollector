import copy
import pprint
import logging
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
    def get_datetime(cls, datetime_string):
        try:
            return parser.parse(datetime_string, fuzzy=True)
        except Exception, e:
            raise ParsingError("Error parsing datetime for %s" % datetime_string)

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
        if self.checkpoint_enabled:
            self.current_checkpoint = self.last_checkpoint or {}
            if 'bytes_read' in self.last_checkpoint:
                self.log.info("Detected checkpoint, seeking file: %s to %s position" %
                              (self.logpath,
                               self.last_checkpoint['bytes_read']))
                self.tail.seek_bytes(self.current_checkpoint['bytes_read'])

    def process_line(self):
        # TODO: transform the data


        # store it
        try:
            if self.checkpoint_enabled:
                self.store(Message(checkpoint=copy.deepcopy(self.current_checkpoint),
                                   content=self.current_line))
            else:
                self.store(Message(content=self.current_line))
        except Exception, e:
            self.log.error("Error processing line.")
            self.log.error(traceback.format_exc())

    def read(self):
        if self.period and self.get_line():
            self.process_line()
        else:
            while True:
                if self.get_line():
                    self.process_line()
