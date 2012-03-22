import re
import copy
import logging
import datetime
import traceback

from third import filetail

from __reader import Reader
from __message import Message
from __exceptions import ParsingError
from LogConfReader import LogConfReader
from LogUtils import LogUtils


class LogReader(Reader):
    """Conf:
        - logpath (required): path of log file,
            e.g. /tmp/my.log
        - delimiter (optional): character to split log lines,
            e.g. '\t'
        - columns (optional): list of columns for each log line,
            e.g. ['date', 'hour', 'message']"""

    def set_current_datetime(self):
        """Set datetime from the current line"""
        if self.use_datetime_column:
            self.current_datetime = LogUtils.get_datetime(self.current_line,
                                                          self.datetime_column)
        else:
            self.current_datetime = LogUtils.get_datetime(self.current_line,
                                                          self.date_column,
                                                          self.time_column)

    def do_aggregation_with_groupby(self, kind, cache):
        """Input: string ('sums' or 'counts')
                  dict {'column_name': 'to_sum_column',
                        'interval_duration_sec': 60,
                        'groupby': {'column': 'host'},
                        'groups': {}}

           Output: side effect in cache structure"""
        try:
            current_value = self.current_line[cache['column_name']]
            if kind == 'sums':
                current_value = int(current_value)
            groupby_value = self.current_line[cache['groupby']['column']]
            groupby_value = re.match(cache['groupby']['match'], groupby_value).groups()[0]
            period = cache['interval_duration_sec']

            # starting interval
            if not groupby_value in cache['groups']:
                for group in cache['groups']:
                    current_start_time = cache['groups'][group]['current']['interval_started_at']
                    (start, end) = LogUtils.get_interval(current_start_time, period)
                    if not (start <= self.current_datetime < end):
                        closed = [{'interval_started_at': cache['groups'][group]['current']['interval_started_at'],
                                     'value': cache['groups'][group]['current']['value']}]
                        zeros = LogUtils.get_missing_intervals(end, period, self.current_datetime)
                        closed.extend([{'interval_started_at': z, 'value': 0} for z in zeros])
                        cache['groups'][group]['closed'] = closed
                        cache['groups'][group]['current']['interval_started_at'] = LogUtils.get_starting_minute(self.current_datetime)
                        cache['groups'][group]['current']['value'] = 0

                start = LogUtils.get_starting_minute(self.current_datetime)
                if kind == 'sums':
                    cache['groups'][groupby_value] = {'current':
                                                        {'interval_started_at': start,
                                                         'value': current_value},
                                                      'closed': []}
                elif kind == 'counts':
                    value = 1 if current_value == cache['column_value'] else 0
                    cache['groups'][groupby_value] = {'current':
                                                        {'interval_started_at': start,
                                                         'value': value},
                                                      'closed': []}
            else:
                current_start_time = cache['groups'][groupby_value]['current']['interval_started_at']
                (start, end) = LogUtils.get_interval(current_start_time, period)
                # not in interval
                if not (start <= self.current_datetime < end):
                    for group in cache['groups']:
                        if group == groupby_value:
                            closed = [{'interval_started_at': cache['groups'][groupby_value]['current']['interval_started_at'],
                                         'value': cache['groups'][groupby_value]['current']['value']}]
                            zeros = LogUtils.get_missing_intervals(end, period, self.current_datetime)
                            closed.extend([{'interval_started_at': z, 'value': 0} for z in zeros])
                            cache['groups'][groupby_value]['closed'] = closed
                            new_start, new_end = LogUtils.get_interval(self.current_datetime, period)
                            cache['groups'][groupby_value]['current']['interval_started_at'] = new_start
                            if kind == 'sums':
                                cache['groups'][groupby_value]['current']['value'] = current_value
                            elif kind == 'counts':
                                cache['groups'][groupby_value]['current']['value'] = 1 if current_value == cache['column_value'] else 0
                        else:
                            closed = [{'interval_started_at': cache['groups'][group]['current']['interval_started_at'],
                                         'value': cache['groups'][group]['current']['value']}]
                            zeros = LogUtils.get_missing_intervals(end, period, self.current_datetime)
                            closed.extend([{'interval_started_at': z, 'value': 0} for z in zeros])
                            cache['groups'][group]['closed'] = closed
                            new_start, new_end = LogUtils.get_interval(self.current_datetime, period)
                            cache['groups'][group]['current']['interval_started_at'] = new_start
                            if kind == 'sums':
                                cache['groups'][group]['current']['value'] = 0
                            elif kind == 'counts':
                                cache['groups'][group]['current']['value'] = 0
                # in interval
                else:
                    for group in cache['groups']:
                        cache['groups'][group]['closed'] = []
                    if kind == 'sums':
                        cache['groups'][groupby_value]['current']['value'] += current_value
                    elif kind == 'counts' and \
                         current_value == cache['column_value']:
                         cache['groups'][groupby_value]['current']['value'] += 1
            return True
        except Exception, e:
            traceback.print_exc()

    def do_aggregation(self, kind):
        cache = self.current_sums if kind == 'sums' else self.current_counts
        for i, c in enumerate(cache):
            if 'groupby' in c:
                self.do_aggregation_with_groupby(kind, c)
            else:
                current_value = self.current_line[c['column_name']]
                if kind == 'sums':
                    current_value = int(current_value)
                current_start_time = c['current']['interval_started_at']
                period = c['interval_duration_sec']

                # starting interval
                if current_start_time == 0:
                    start = LogUtils.get_starting_minute(self.current_datetime)
                    c['current']['interval_started_at'] = start
                    if kind == 'sums':
                        c['current']['value'] = current_value
                    elif kind == 'counts':
                        c['current']['value'] = 1 if current_value == c['column_value'] else 0
                else:
                    (start, end) = LogUtils.get_interval(current_start_time, period)
                    # not in interval
                    if not (start <= self.current_datetime < end):
                        closed = [{'interval_started_at': c['current']['interval_started_at'],
                                     'value': c['current']['value']}]
                        zeros = LogUtils.get_missing_intervals(end, period, self.current_datetime)
                        closed.extend([{'interval_started_at': z, 'value': 0 } for z in zeros])
                        c['closed'] = closed
                        new_start, new_end = LogUtils.get_interval(self.current_datetime, period)
                        c['current']['interval_started_at'] = new_start
                        if kind == 'sums':
                            c['current']['value'] = current_value
                        elif kind == 'counts':
                            c['current']['value'] = 1 if current_value == c['column_value'] else 0
                    # in interval
                    else:
                        c['closed'] = []
                        if kind == 'sums':
                            c['current']['value'] += current_value
                        elif kind == 'counts' and \
                             current_value == c['column_value']:
                             c['current']['value'] += 1
        return True

    def store_aggregation_with_groupby(self, kind, cache):
        try:
            for group in cache['groups']:
                for p in cache['groups'][group]['closed']:
                    content = {'interval_duration_sec': cache['interval_duration_sec'],
                               'interval_started_at': p['interval_started_at'],
                               'column_name': cache['column_name'],
                               'value': p['value'],
                               cache['groupby']['column']: group}
                    if kind == "counts":
                        content.update({'column_value': cache['column_value']})
                    content = copy.deepcopy(content)
                    try:
                        if self.checkpoint_enabled:
                            self.store(Message(checkpoint=self.current_checkpoint,
                                               content=content))
                        else:
                            self.store(Message(content=content))
                    except Exception, e:
                        traceback.print_exc()
        except Exception, e:
            traceback.print_exc()

    def store_aggregation(self, kind):
        cache = self.current_sums if kind == 'sums' else self.current_counts
        for c in cache:
            if 'groupby' in c:
                self.store_aggregation_with_groupby(kind, c)
            else:
                for p in c['closed']:
                    content = {'interval_duration_sec': c['interval_duration_sec'],
                               'interval_started_at': p['interval_started_at'],
                               'column_name' : c['column_name'],
                               'value' : p['value']}
                    if kind == "counts":
                        content.update({'column_value': c['column_value']})
                    content = copy.deepcopy(content)
                    if self.checkpoint_enabled:
                        self.store(Message(checkpoint=self.current_checkpoint,
                                           content=content))
                    else:
                        self.store(Message(content=content))

        if self.checkpoint_enabled:
            self.current_checkpoint[kind] = cache

    def recover_checkpoint(self):
        """Seek file if previous checkpoint was found."""
        self.current_checkpoint = self.last_checkpoint or {}
        if 'bytes_read' in self.last_checkpoint:
            self.log.info("Detected checkpoint, seeking file: %s to %s position" %
                          (self.logpath,
                           self.last_checkpoint['bytes_read']))
            self.tail.seek_bytes(self.current_checkpoint['bytes_read'])

    def setup(self):
        # starts the logger
        self.log = logging.getLogger('pycollector')

        # validate conf
        LogConfReader.validate_conf(self.conf)

        # check for required confs
        self.required_confs = ['logpath']
        self.check_required_confs()

        # starts tail
        self.tail = filetail.Tail(self.logpath, max_sleep=1, store_pos=True)

        # failure recovering from checkpoint
        if self.checkpoint_enabled: self.recover_checkpoint()

        # initializations
        self.to_split = True if hasattr(self, 'delimiter') else False
        self.to_dictify = True if hasattr(self, 'columns') else False
        self.to_sum = True if hasattr(self, 'sums') else False
        self.to_count = True if hasattr(self, 'counts') else False
        self.use_datetime_column = True if hasattr(self, 'datetime_column') else False

        if hasattr(self, 'sums'):
            self.current_sums = LogUtils.initialize_sums(self.sums)

        if hasattr(self, 'counts'):
            self.current_counts = LogUtils.initialize_counts(self.counts)

    def process_line(self):
        try:
            if self.to_sum or self.to_count:
                self.set_current_datetime()
                try:
                    if self.to_sum:
                        self.do_aggregation('sums') and \
                         self.store_aggregation('sums')
                except Exception, e:
                    self.log.error("Error while summing.")
                    self.log.error(traceback.format_exc())
                try:
                    if self.to_count:
                        self.do_aggregation('counts') and \
                        self.store_aggregation('counts')
                except Exception, e:
                    self.log.error("Error while counting.")
                    self.log.error(traceback.format_exc())
                return

            if self.checkpoint_enabled:
                checkpoint = copy.deepcopy(self.current_checkpoint)
                self.store(Message(checkpoint=checkpoint,
                                   content=self.current_line))
            else:
                self.store(Message(content=self.current_line))
        except Exception, e:
            self.log.error("Error processing line")
            self.log.error(traceback.format_exc())

    def get_line(self):
        """Returns a boolean indicating whether the log line
           was successfully read or not"""
        try:
            self.bytes_read, self.current_line = self.tail.nextline()
            if self.checkpoint_enabled:
                self.current_checkpoint['bytes_read'] = self.bytes_read

            if self.to_split and self.to_dictify:
                self.current_line = LogUtils.dictify_line(self.current_line,
                                                      self.delimiter,
                                                      self.columns)
            elif self.to_split:
                self.current_line = LogUtils.split_line(self.current_line,
                                                    self.delimiter)
        except ParsingError, e:
            self.log.error(e.msg)
            return False
        except Exception, e:
            self.log.error(traceback.format_exc())
            return False
        return True

    def read(self):
        if self.period:
            self.get_line() and self.process_line()
            return
        while True:
            self.get_line() and self.process_line()
