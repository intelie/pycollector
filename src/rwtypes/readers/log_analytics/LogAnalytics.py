import logging
import datetime

from third import filetail

from __reader import Reader
from __message import Message


class LogAnalytics(Reader):
    """Conf:
        - logpath (required): path of log file,
            e.g. /tmp/my.log
        - delimiter (required): character to split log lines,
            e.g. '\t'
        - columns (required): list of columns for each log line,
            e.g. ['date', 'hour', 'message']"""

    def setup(self):
        self.log = logging.getLogger()
        self.check_conf(['delimiter', 'columns', 'logpath'])
        self.tail = filetail.Tail(self.logpath, max_sleep=1)
        self.client = self.logpath.split('.')[1]
        self.time_format = "%d/%b/%Y:%H:%M:%S"
        self.start_counts()
        self.start_sums()

    def start_counts(self):
        if self.counts:
            self.agg_counts = {}
            for key, value in self.counts.items():
                d = {'value' : 0,
                     'start_time' : 0,
                     'content' : value[0],
                     'interval' : value[1]*60}
                self.agg_counts[key] = d

#    def start_sums(self):
#        if self.sums:
#            self.agg_sums = {}
#            for key, value in self.sums.items():
#                d = {'value' : 0,
#                     'start_time' : 0,
#                     'content' : value[0],
#                     'interval' : value[1]*60}
#                self.agg_sums[key] = d

    def check_conf(self, items):
        for item in items:
            if not hasattr(self, item):
                self.log.error('%s not defined in conf.yaml.' % item)
                self.log.info('Aborting.')
                exit(-1)
    
    def dictify_line(self, line):
        return dict(zip(self.columns, line.strip().split(self.delimiter)))

    def get_datetime(self, date_format):
        return datetime.datetime.strptime(date_format, self.time_format)

    def get_minute(self, date):
        return date - datetime.timedelta(0, date.second)

    def add_interval(self, date, seconds):
        return date + datetime.timedelta(0, seconds)

    def get_empty_periods(self, beginning, end, period):
        empty_periods = []
        while beginning + period <= end:
            empty_periods.append(beginning)
            beginning = beginning + period
        return empty_periods

    def store_empty_periods(self, pos, column, metadata, empty_periods):
        for empty_period in empty_periods:
            checkpoint = self.generate_checkpoint(pos)
            content = {'value' : 0,
                       'client' : self.client,
                       'interval_duration_sec' : metadata['interval'],
                       'field_name' : column,
                       'field_value' : metadata['content'],
                       'interval_started_at' : empty_period.strftime(self.time_format),
                       'aggregation_type' : 'count',}
            msg = Message(checkpoint=checkpoint,
                          content=content)
            self.store(msg)

    def set_first_count(self, metadata, time):
        metadata['value'] = 1 
        metadata['start_time'] = time

#    def set_first_sum(self, metadata, time, value):
#        metadata['value'] = value 
#        metadata['start_time'] = time

    def generate_checkpoint(self, pos):
        checkpoint = {'pos' : pos}
        checkpoint.update({'counts' : self.agg_counts})
        return checkpoint

    def read(self):

        #recover after failure
        if self.checkpoint_enabled and self.last_checkpoint:
            self.agg_counts = self.last_checkpoint['counts']
#            self.agg_sums = self.last_checkpoint['sums']
            self.tail.seek_bytes(self.last_checkpoint['pos'])
        cur_time = 0
        while True:
            try:
                self.current_line = self.tail.nextline()
                cur_pos = self.tail.pos
                line_data = self.dictify_line(self.current_line)
                try:
                    cur_time = self.get_datetime(line_data['time_local'][1:21])
                    cur_minute = self.get_minute(cur_time)
                except ValueError:
                    self.log.error('Cannot parse date %s, skipping line' % line_data['time_local'])
                    continue

                for column, metadata in self.agg_counts.items():
                    #match occurred
                    if line_data[column] == metadata['content']:

                        #first occurrence
                        if metadata['start_time'] == 0:
                            self.set_first_count(metadata, cur_minute)
                            continue

                        time_passed = cur_time - metadata['start_time']

                        #in the interval
                        if time_passed.seconds <= metadata['interval']:
                            metadata['value'] += 1
                            continue

                        #closing interval
                        else:
                            time = metadata['start_time'].strftime(self.time_format)
                            checkpoint = self.generate_checkpoint(cur_pos)
                            content={'value' : metadata['value'],
                                     'client' : self.client,
                                     'interval_duration_sec' : metadata['interval'],
                                     'field_name' : column,
                                     'field_value' : metadata['content'], 
                                     'interval_started_at' : time,
                                     'aggregation_type': 'count'}
                            msg = Message(checkpoint=checkpoint,
                                          content=content)
                            self.store(msg)
                            
                            #empty periods
                            next_interval = self.add_interval(metadata['start_time'], 
                                                              metadata['interval'])
                            empty_periods = self.get_empty_periods(next_interval,
                                                                   cur_minute, 
                                                                   datetime.timedelta(0, metadata['interval']))
                            self.store_empty_periods(cur_pos, column, metadata, empty_periods)
                            self.set_first_count(metadata, cur_minute)


                #for column, metadata in self.agg_sum.items():
#                    try:
#                        value = int(line_data[column].strip())
#                    except Exception, e:
#                        self.log.error("Cannot get value from %s" % column)
#                        self.log.error(e)
#
#                    if metadata['start_time'] == 0:
                        
            except Exception, e: 
                self.log.debug('Error reading line: %s' % self.current_line)
                self.log.error(e)
