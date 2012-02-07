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
        self.check_conf(['delimiter', 'columns', 'logpath'])
        self.tail = filetail.Tail(self.logpath, max_sleep=1)
        self.client = self.logpath.split('.')[1]
        self.time_format = "%d/%b/%Y:%H:%M:%S"
        self.start_counters()

    def start_counters(self):
        if self.count:
            self.agg_count = {}
            for key, value in self.count.items():
                d = {'value' : 0,
                     'start_time' : 0,
                     'content' : value[0],
                     'interval' : value[1]*60}
                self.agg_count[key] = d

    def check_conf(self, items):
        for item in items:
            if not hasattr(self, item):
                print '%s not defined in conf.yaml.' % item
                exit(-1)
    
    def dictify_line(self, line):
        return dict(zip(self.columns, line.strip().split(self.delimiter)))

    def get_datetime(self, date_format):
        return datetime.datetime.strptime(date_format, self.time_format)

    def get_minute(self, date):
        return date - datetime.timedelta(0, date.second)

    def get_empty_periods(self, beginning, end, period):
        empty_periods = []
        while beginning + period <= end:
            empty_periods.append(beginning)
            beginning = beginning + period
        return empty_periods

    def send_empty_periods(self, empty_periods):
        for empty_period in empty_periods:
            content = {'count' : 0,
                       'client' : self.client,
                       'time' : empty_period}
            msg = Message(content=content)
            self.store(msg)

    def read(self):
        cur_time = 0
        while True:
            try:
                line = self.tail.nextline()
                line_data = self.dictify_line(line)
                try:
                    cur_time = self.get_datetime(line_data['time_local'][1:21])
                    cur_minute = self.get_minute(cur_time)
                except ValueError:
                    print 'Cannot parse date, skipping line'
                    continue

                for column, metadata in self.agg_count.items():
                    #match occurred
                    if line_data[column] == metadata['content']:

                        #first occurrence
                        if metadata['start_time'] == 0:
                            metadata['start_time'] = cur_minute
                            metadata['value'] = 1
                            continue

                        time_passed = cur_time - metadata['start_time']

                        #in the interval
                        if time_passed.seconds <= metadata['interval']:
                            metadata['value'] += 1
                            continue

                        #closing interval
                        else:
                            time = metadata['start_time'].strftime(self.time_format)
                            content={'count_' + column : metadata['value'],
                                     'client' : self.client,
                                     'time' : time}
                            msg = Message(content=content)
                            self.store(msg)
                            
                            #empty periods
                            next_interval = metadata['start_time'] + datetime.timedelta(0, metadata['interval'])
                            empty_periods = self.get_empty_periods(next_interval, cur_minute, datetime.timedelta(0, metadata['interval']))
                            self.send_empty_periods(empty_periods)

                            metadata['value'] = 1
                            metadata['start_time'] = cur_minute

            except Exception, e:
                print 'error reading line: %s' % line
                print e
