import logging
from third import filetail

from __reader import Reader
from __message import Message


class LogReader(Reader):
    """Conf:
        - logpath (required): path of log file,
            e.g. /tmp/my.log
        - delimiter (optional): character to split log lines,
            e.g. '\t'
        - columns (optional): list of columns for each log line,
            e.g. ['date', 'hour', 'message']"""

    def setup(self):
        self.log = logging.getLogger()
        self.tail = filetail.Tail(self.logpath, max_sleep=1)

        #TODO: implement checkpoints

        #TODO: set with log line number
        self.checkpoint = 0 

    def process_line(self):
        try:
            to_store = self.tail.nextline()

            if hasattr(self, 'delimiter'):
                to_store = to_store.strip().split(self.delimiter)

                if hasattr(self, 'columns'):
                    to_store = dict(zip(self.columns, to_store))

            if self.store(Message(content=to_store,
                               checkpoint=self.checkpoint)):

                #TODO: update with log line number
                self.checkpoint += 1
            return True

        except Exception, e:
            self.log.error('error reading line: %s' % line)
            return False
    
    def read(self):
        if self.interval:
            return self.process_line()
        else:            
            while True:
                self.process_line()
