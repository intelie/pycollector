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
        
    def read(self):
        checkpoint = 0 
        while True:
            try:
                line = self.tail.nextline()

                if hasattr(self, 'delimiter'):
                    values = line.strip().split(self.delimiter)

                    if hasattr(self, 'columns'):
                        column_values = dict(zip(self.columns, values))

                        #XXX: if we have columns, save it as a dict
                        checkpoint += 1
                        self.store(Message(content=column_values,
                                           checkpoint=checkpoint))
                        continue

                    #XXX: if we have just a delimiter, save the list
                    self.store(Message(content=values,
                                       checkpoint=checkpoint))
                    checkpoint += 1
                    continue

                self.store(Message(content=line,
                                   checkpoint=checkpoint))
                checkpoint += 1

            except Exception, e:
                self.log.error('error reading line: %s' % line)
                self.log.error(e)
