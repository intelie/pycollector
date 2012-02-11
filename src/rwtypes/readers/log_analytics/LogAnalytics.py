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

    def check_conf(self, items):
        for item in items:
            if not hasattr(self, item):
                self.log.error('%s not defined in conf.yaml.' % item)
                self.log.info('Aborting.')
                exit(-1)

    def start_counts(self):
        self.agg_counts = {}
        if hasattr(self, 'counts'):
            for key, value in self.counts.items():
                d = {'value' : 0,
                     'interval_started_at' : 0,
                     'field_name' : key,
                     'field_value' : value[0],
                     'interval_duration_sec' : value[1]*60}
                self.agg_counts[key] = d

    def start_sums(self):
        self.agg_sums = {}
        if hasattr(self, 'sums'):
            for key, value in self.sums.items():
                d = {'value' : 0,
                     'field_name' : key,
                     'interval_started_at' : 0,
                     'interval_duration_sec' : value[0]*60}
                self.agg_sums[key] = d

    def dictify_line(self, line):
        return dict(zip(self.columns, line.strip().split(self.delimiter)))

    def get_datetime(self, date_format):
        return datetime.datetime.strptime(date_format, self.time_format)

    def get_minute(self, date):
        return date - datetime.timedelta(0, date.second)

    def add_interval(self, date, seconds):
        return date + datetime.timedelta(0, seconds)

    def generate_checkpoint(self, pos):
        checkpoint = {'pos' : pos}
        checkpoint.update({'counts' : self.agg_counts,
                           'sums' : self.agg_sums})
        return checkpoint 

#    def get_empty_periods(self, beginning, end, period):
#        empty_periods = []
#        while beginning + period <= end:
#            empty_periods.append(beginning)
#            beginning = beginning + period
#        return empty_periods

#    def store_empty_periods(self, pos, column, metadata, empty_periods):
#        for empty_period in empty_periods:
#            checkpoint = self.generate_checkpoint(pos)
#            content = {'value' : 0,
#                       'client' : self.client,
#                       'interval_duration_sec' : metadata['interval'],
#                       'field_name' : column,
#                       'field_value' : metadata['content'],
#                       'interval_started_at' : empty_period.strftime(self.time_format),
#                       'aggregation_type' : 'count',}
#            msg = Message(checkpoint=checkpoint,
#                          content=content)
#            self.store(msg)

    def get_count_message(column, agg):
        formatted_time = agg['interval_started_at'].strftime(self.time_format)
        checkpoint = self.generate_checkpoint(self.current_position)
        content={'field_name' : column,
                 'field_value' : agg['field_value'], 
                 'client' : self.client,
                 'value' : agg['value'],
                 'interval_duration_sec' : agg['interval_duration_sec'],
                 'interval_started_at' : formatted_time,
                 'aggregation_type': 'count'}
        return Message(checkpoint=checkpoint, content=content)

    def recover_from_previous_failure(self):
        try:
            if self.checkpoint_enabled and self.last_checkpoint:
                self.agg_counts = self.last_checkpoint['counts']
                self.agg_sums = self.last_checkpoint['sums']
                self.tail.seek_bytes(self.last_checkpoint['pos'])
        except Exception, e
            self.log.error("Can't recover from previous checkpoint: %s" % self.last_checkpoint)
            self.error(e)

    def read(self):
        self.recover_from_previous_failure()
        self.current_time = 0
        while True:
            try:
                self.current_line = self.tail.nextline()
                self.currrent_position = self.tail.pos
                self.log_line_data = self.dictify_line(self.current_line)
                self.current_time = self.get_datetime(line_data['time_local'][1:21])
                self.current_minute = self.get_minute(self.current_time)
                self.log.debug('Line parsed with success: %s' % self.current_line)
            except ValueError:
                self.log.error('Cannot parse line: %s' % self.current_line)
                self.log.error(e)
                continue

            try:
                for column, agg in self.agg_counts.items():

                    #skip to next count if did not match
                    if not log_line_data[column] == agg_count['field_value']:
                        continue

                    #first occurrence
                    if agg_count['interval_started_at'] == 0:
                        agg_count['value'] += 1
                        agg_count['started_interval_at'] = self.current_time
                        continue

                    #not the first one
                    time_passed = self.current_time - agg_count['interval_started_at']t

                    #in the interval?
                    if time_passed.seconds <= agg_count['interval_duration_sec']:
                        agg_count['field_value'] += 1
                        continue
                    #end of interval
                    else:
                        self.store(self.get_count_message(agg_count))
                        
                        #empty periods
                        next_interval = self.add_interval(agg_count['start_time'], 
                                                          metadata['interval'])
                        empty_periods = self.get_empty_periods(next_interval,
                                                               cur_minute, 
                                                               datetime.timedelta(0, metadata['interval']))
                        self.store_empty_periods(cur_pos, column, metadata, empty_periods)
                        self.set_first_count(metadata, cur_minute)
            except Exception, e:
                self.log.error('Cannot apply count in %s' % self.current_line )
                self.log.error(e)


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
