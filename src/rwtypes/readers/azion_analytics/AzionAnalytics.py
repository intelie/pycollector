import datetime

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
        if not hasattr(self, 'delimiter'):
            print 'configure a delimiter in conf.yaml.'
            exit(-1)

        if not hasattr(self, 'columns'):
            print 'columns not defined in conf.yaml'
            exit(-1)

        if not hasattr(self, 'logpath'):
            print 'logpath not defined in conf.yaml'
            exit(-1)

        self.tail = filetail.Tail(self.logpath, max_sleep=1)
        self.client = self.logpath.split('.')[1]
        self.time_format = "%d/%b/%Y:%H:%M:%S"
        if self.count:
            self.agg_count = {}
            for key, value in self.count.items():
                self.agg_count[key] = {'value': 0, 
                                       'start_time' : 0,
                                       'content' : value[0],
                                       'interval': value[1]*60}

    def read(self):
        cur_time = 0
        while True:
            try:
                line = self.tail.nextline()

                values = line.strip().split(self.delimiter)
                column_values = dict(zip(self.columns, values))

                try:
                    cur_time = datetime.datetime.strptime(column_values['time_local'][1:21], 
                                                          self.time_format)
                    cur_minute = cur_time - datetime.timedelta(0, cur_time.second)
                except ValueError:
                    print 'Cannot parse date, skipping line'
                    continue

                for column, metadata in self.agg_count.items():
                    if column_values[column] == metadata['content']:

                        #first occurrence
                        if metadata['start_time'] == 0:
                            metadata['start_time'] = cur_minute
                            metadata['value'] = 1
                            continue

                        time_passed = cur_time - metadata['start_time']

                        #in the interval
                        if time_passed.seconds <= metadata['interval']:
                            metadata['value'] += 1

                        #closing interval
                        else:
                            time = metadata['start_time'].strftime(self.time_format)
                            msg = Message(content={'count_' + column : metadata['value'],
                                                   'client' : self.client,
                                                   'time': time})
                            self.store(msg)

                            #TODO: fill empty periods with zeros


                            metadata['value'] = 1
                            metadata['start_time'] = cur_minute

            except Exception, e:
                print 'error reading line: %s' % line
                print e
