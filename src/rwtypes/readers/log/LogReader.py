import logging
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

        self.tail = filetail.Tail(self.logpath, max_sleep=1, store_pos=True)

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

    def process_line(self):
        try:
            # get new line
            self.checkpoint, line = self.tail.nextline()
            to_store = line

            # parse
            if hasattr(self, 'delimiter') and \
                hasattr(self, 'columns'):
                to_store = self.dictify_line(to_store,
                                             self.delimiter,
                                             self.columns)
            elif hasattr(self, 'delimiter'):
                to_store = self.split_line(to_store,
                                           self.delimiter)

            # store in queue
            self.store(Message(content=to_store, checkpoint=self.checkpoint))
            return True

        except ParsingError, e:
            self.log.error(e.msg)
            return False
        except Exception, e:
            self.log.error(e)
            return False

    def read(self):
        if self.period:
            return self.process_line()
        else:
            while True:
                self.process_line()
