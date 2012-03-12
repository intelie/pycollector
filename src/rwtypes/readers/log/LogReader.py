import logging
import traceback
from third import filetail

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
            self.tail.seek_bytes(self.last_checkpoint or 0)

    @classmethod
    def dictify_line(cls, line, delimiter, columns):
        """Dictify a log line mapping columns with values separated
        by a delimiter"""
        try:
            return dict(zip(columns, cls.split_line(line, delimiter)))
        except Exception, e:
            raise ParsingError("Error parsing line: %s" % line)

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
        self.checkpoint, self.current_line = self.tail.nextline()
        try:
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
            self.log.error(e)
            return False
        return True

    def process_line(self):
        # do any transformation to the log line
        # ...

        # store it
        self.store(Message(checkpoint=self.checkpoint, 
                           content=self.current_line))

    def read(self):
        if self.period and self.get_line():
            self.process_line()
        else:
            while True:
                if self.get_line():
                    self.process_line()
