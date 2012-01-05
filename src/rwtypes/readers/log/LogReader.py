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
        self.tail = filetail.Tail(self.logpath, max_sleep=1)
        
    def read(self):
        while True:
            try:
                line = self.tail.nextline()

                #XXX: remove \n?
                line = line.strip()

                if hasattr(self, 'delimiter'):
                    values = line.split(self.delimiter)

                    if hasattr(self, 'columns'):
                        column_values = dict(zip(self.columns, values))

                        #XXX: if we have columns, save it as a dict
                        self.store(Message(content=column_values))
                        continue

                    #XXX: if we have just a delimiter, save the list
                    self.store(Message(content=values))
                    continue

                self.store(Message(content=line))

            except Exception, e:
                print 'error reading line: %s' % line
                print e
