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

    def start_sums(self):
        self.agg_sums= {}
        if hasattr(self, 'sums'):
            for key, value in self.sums.items():
                d = {'value' : 0,
                     'field_name' : key,
                     'interval_started_at' : 0,
                     'interval_duration_sec' : value*60}
                self.agg_sums[key] = d

    def start_counts(self):
        self.agg_counts = {}
        if hasattr(self, 'counts'):
            for key, value in self.counts.items():
                d = {'value' : 0,
                     'field_name' : key,
                     'field_value' : value[0],
                     'interval_started_at' : 0,
                     'interval_duration_sec' : value[1]*60}
                self.agg_counts[key] = d

    def check_conf(self, items):
        for item in items:
            if not hasattr(self, item):
                self.log.error('%s not defined in conf.yaml.' % item)
                self.log.info('Aborting.')
                exit(-1)
    
    def dictify_line(self):
        return dict(zip(self.columns, self.current_line.strip().split(self.delimiter)))

    def get_current_time(self):
        return datetime.datetime.strptime(self.log_line_data['time_local'][1:21], self.time_format)

    def get_minute(self):
        return self.current_time - datetime.timedelta(0, self.current_time.second)

    def add_interval(self, date, seconds):
        return date + datetime.timedelta(0, seconds)

    def get_empty_periods(self, beginning, end, period):
        empty_periods = []
        while beginning + period <= end:
            empty_periods.append(beginning)
            beginning = beginning + period
        return empty_periods
    
    def get_count_message(self, column, agg):
        formatted_time = agg['interval_started_at'].strftime(self.time_format)
        checkpoint = self.generate_checkpoint()
        content = {'field_name' : column, 
                   'field_value' : agg['field_value'],
                   'client' : self.client,
                   'value' : agg['value'],
                   'interval_duration_sec' : agg['interval_duration_sec'],
                   'interval_started_at' : formatted_time,
                   'aggregation_type' : 'count'}
        return Message(checkpoint=checkpoint, content=content)

    def store_empty_periods(self, pos, column, agg, empty_periods):
        for empty_period in empty_periods:
            checkpoint = self.generate_checkpoint()
            content = {'value' : 0,
                       'field_name' : column,
                       'field_value' : agg['field_value'],
                       'client' : self.client,
                       'interval_duration_sec' : agg['interval_duration_sec'],
                       'interval_started_at' : empty_period.strftime(self.time_format),
                       'aggregation_type' : 'count',}
            msg = Message(checkpoint=checkpoint, content=content)
            self.store(msg)

    def recover_from_previous_failure(self):
        try:
            if self.checkpoint_enabled and \
                self.last_checkpoint:
                self.agg_counts = self.last_checkpoint['count']
                self.agg_sums = self.last_checkpoint['sums']
                self.tail.seek_bytes(self.last_checkpoint['pos'])
        except Exception, e:
            self.log.error("Can't recover from previous checkpoint: %s" % 
                           self.last_checkpoint)
            self.log.error(e)

    def generate_checkpoint(self):
        checkpoint = {'pos' : self.current_position}
        checkpoint.update({'counts' : self.agg_counts,
                           'sums' : self.agg_sums,})
        return checkpoint

    def read(self):
        self.recover_from_previous_failure()
        self.current_time = 0
        while True:
            try:
                self.current_line = self.tail.nextline()
                self.current_position = self.tail.pos
                self.log_line_data = self.dictify_line()
                self.current_time = self.get_current_time()
                self.current_minute = self.get_minute()
                self.log.debug('Line parsed with success: %s' % self.current_line)
            except ValueError:
                self.log.error('Cannot parse line: %s' % self.current_line)
                self.log.error(e)
                continue

            try:
                for column, agg in self.agg_counts.items():
                    if not self.log_line_data[column] == agg['field_value']:
                        continue

                    if agg['interval_started_at'] == 0:
                        agg['value'] += 1
                        agg['interval_started_at'] = self.current_minute
                        continue

                    time_passed = self.current_time - agg['interval_started_at']

                    if time_passed.seconds <= agg['interval_duration_sec']:
                        agg['value'] += 1
                        continue
                    else:
                        self.store(self.get_count_message(column, agg))
                        next_interval = self.add_interval(agg['interval_started_at'], 
                                                          agg['interval_duration_sec'])
                        empty_periods = self.get_empty_periods(next_interval,
                                                               self.current_time, 
                                                               datetime.timedelta(0, agg['interval_duration_sec']))
                        self.store_empty_periods(self.current_position, column, agg, empty_periods)

                        agg['value'] = 1 
                        agg['interval_started_at'] = self.current_minute
                    
                self.log.debug('Applied counts successfully.')
            except Exception, e:
                self.log.error('Cannot apply count in %s' % self.current_line)
                self.log.error(e)
