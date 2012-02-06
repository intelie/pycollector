from third import filetail

from __reader import Reader
from __message import Message


class AzionAnalytics(Reader):
    """Conf:
        - logpath (required): path of log file,
            e.g. /tmp/my.log
        - delimiter (required): character to split log lines,
            e.g. '\t'
        - columns (required): list of columns for each log line,
            e.g. ['date', 'hour', 'message']"""

    def setup(self):
        self.tail = filetail.Tail(self.logpath, max_sleep=1)
        self.client = logpath.split('.')[1] 
        
    def read(self):
        while True:
            try:
                line = self.tail.nextline()

                if hasattr(self, 'delimiter'):
                    values = line.strip().split(self.delimiter)

                    if hasattr(self, 'columns'):
                        column_values = dict(zip(self.columns, values))

                        #XXX: if we have columns, save it as a dict
                        self.store(Message(content=column_values))

            except Exception, e:
                print 'error reading line: %s' % line
                print e
