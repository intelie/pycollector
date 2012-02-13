import logging
import datetime

from third import filetail

from __reader import Reader
from __message import Message


#TODO: it still needs refactoring

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
        if 'time_local' in self.log_line_data:
            return datetime.datetime.strptime(self.log_line_data['time_local'][1:21], self.time_format)
        else:
            #wowza format
            date = self.log_line_data['date']
            time = self.log_line_data['time']
            dt = "%s %s" % (date, time)
            return datetime.datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")
            

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
    
    def get_sum_message(self, column, agg):
        formatted_time = agg['interval_started_at'].strftime(self.time_format)
        checkpoint = self.generate_checkpoint()
        content = {'field_name' : column, 
                   'client' : self.client,
                   'value' : agg['value'],
                   'interval_duration_sec' : agg['interval_duration_sec'],
                   'interval_started_at' : formatted_time,
                   'aggregation_type' : 'sum'}
        return Message(checkpoint=checkpoint, content=content)

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

    def store_empty_sum_periods(self, column, agg, empty_periods):
        for empty_period in empty_periods:
            checkpoint = self.generate_checkpoint()
            content = {'value' : 0,
                       'field_name' : column,
                       'client' : self.client,
                       'interval_duration_sec' : agg['interval_duration_sec'],
                       'interval_started_at' : empty_period.strftime(self.time_format),
                       'aggregation_type' : 'sum',}
            msg = Message(checkpoint=checkpoint, content=content)
            self.store(msg)

    def store_empty_count_periods(self, column, agg, empty_periods):
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
        if self.checkpoint_enabled and \
            self.last_checkpoint:
            self.agg_counts = self.last_checkpoint['counts']
            self.agg_sums = self.last_checkpoint['sums']
            self.tail.seek_bytes(self.last_checkpoint['pos'])

    def generate_checkpoint(self):
        checkpoint = {'pos' : self.current_position}
        checkpoint.update({'counts' : self.agg_counts,
                           'sums' : self.agg_sums,})
        return checkpoint

    def do_agg_counts(self):
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
                self.store_empty_count_periods(column, agg, empty_periods)

                agg['value'] = 1 
                agg['interval_started_at'] = self.current_minute

    def do_agg_sums(self):
        for column, agg in self.agg_sums.items():

            #business filter 
            do_sum = False
            if (column == 'cs-bytes' or column == 'sc-bytes'):
                if self.log_line_data['x-event'] == 'disconnect':
                    do_sum = True
                elif self.log_line_data['x-category'] == 'stream' and \
                      self.log_line_data['x-event'] == 'destroy' and \
                      self.log_line_data['c_proto'].find('rtmp') < 0 and \
                      self.log_line_data['x-suri'][:4].find('rtpm') < 0:
                    do_sum = True
            if not do_sum:
                continue

            try:
                current_value = int(self.log_line_data[column])
            except ValueError:
                self.log.error("Can't cast value: %s, for column: %" % (self.log_line_data[column], column))
                self.log.error(e)

            if agg['interval_started_at'] == 0:
                agg['value'] = current_value 
                agg['interval_started_at'] = self.current_minute
                continue

            time_passed = self.current_time - agg['interval_started_at']

            if time_passed.seconds <= agg['interval_duration_sec']:
                agg['value'] += current_value 
                continue
            else:
                self.store(self.get_sum_message(column, agg)) 
                next_interval = self.add_interval(agg['interval_started_at'],
                                                  agg['interval_duration_sec'])
                empty_periods = self.get_empty_periods(next_interval,
                                                       self.current_time,
                                                       datetime.timedelta(0, agg['interval_duration_sec']))
                self.store_empty_sum_periods(column, agg, empty_periods)

                agg['value'] = current_value 
                agg['interval_started_at'] = self.current_minute

    def read(self):
        try:
            self.recover_from_previous_failure()
        except Exception, e:
            self.log.error("Can't recover from previous checkpoint")
        self.current_time = 0
        while True:
            try:
                self.current_line = self.tail.nextline()
                if self.current_line.startswith("#"):
                    self.log.debug("Skipping comment line: %s" % self.current_line)
                    continue
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
                self.do_agg_counts()
                self.log.debug('Applied counts successfully.')
            except Exception, e:
                self.log.error('Cannot apply counts in %s' % self.current_line)
                self.log.error(e)
            try:
                self.do_agg_sums()
                self.log.debug('Applied sums successfully.')
            except Exception, e:
                self.log.error('Cannot apply sums in %s' % self.current_line)
                self.log.error(e)
