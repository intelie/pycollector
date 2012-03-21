import datetime

from helpers.dateutil import parser

from __exceptions import ParsingError


class LogUtils:
    @classmethod
    def get_missing_intervals(cls, start_datetime, period, event_datetime):
        """Input: datetime object (starting datetime),
                  int (period in seconds),
                  datetime object (event datetime)
           Output: tuple with datetime objects"""
        try:
            intervals = []
            (start, end) = cls.get_interval(start_datetime, period)
            while event_datetime >= end:
                intervals.append(start)
                (start, end) = cls.get_interval(end, period)
            return intervals
        except Exception, e:
            raise ParsingError("Error calculating missing intervals.")

    @classmethod
    def get_interval(cls, dt, period):
        """Input: datetime object (starting datetime),
                  int (period in seconds)
           Output: tuple with 2 datetime objects: (start, end)"""
        try:
            start = cls.get_starting_minute(dt)
            end = start + datetime.timedelta(0, period)
            return (start, end)
        except Exception, e:
            raise ParsingError("Can't get period from datetime: %s and period: %s" % (dt, period))

    @classmethod
    def get_starting_minute(cls, dt):
        """Input: datetime object
           Ouput: datetime object with seconds set to zero"""
        try:
            return dt - datetime.timedelta(0, dt.second)
        except Exception, e:
            raise ParsingError("Can't get starting minute from %s" % dt)

    @classmethod
    def get_datetime(cls, dictified_line, column1, column2=None):
        """Input: dict (log line mapped to its columns names),
                  string (log column name with date/time data),
                  string (if log contains date and time data in
                          different columns, use this parameter)
           Output: datetime object"""
        try:
            dt = dictified_line[column1]
            if column2 != None:
                dt += ' %s' % dictified_line[column2]
            return parser.parse(dt, fuzzy=True)
        except Exception, e:
            raise ParsingError("Error parsing datetime for %s" % dictified_line)

    @classmethod
    def dictify_line(cls, line, delimiter, columns):
        """Input: string (raw log line)
                  string (usually, a delimiter character)
                  list (columns names)
           Output: dict"""
        err = ''
        try:
            splitted = cls.split_line(line, delimiter)
            if len(splitted) != len(columns):
                err += "Different number of columns."
                raise Exception
            return dict(zip(columns, splitted))
        except Exception, e:
            raise ParsingError("Error parsing line: %s. %s" % (line, err))

    @classmethod
    def split_line(cls, line, delimiter):
        """Input: string (raw log line)
                  string (usually, a delimiter character)
           Output: list"""
        try:
            return line.strip().split(delimiter)
        except Exception, e:
            raise ParsingError("Error parsing line: %s" % line)

    @classmethod
    def initialize_sums(cls, conf):
        """Input: dict (sums configuration)
           Output: dict"""
        return [{'column_name' : c['column'],
                 'interval_duration_sec' : c['period']*60,
                 'groupby' : c['groupby'],
                 'groups' : {}}
                 if 'groupby' in c else
                {'column_name' : c['column'],
                 'interval_duration_sec' : c['period']*60,
                 'current' : {'interval_started_at' : 0,
                              'value' : 0},
                 'previous' : []}
                 for c in conf]

    @classmethod
    def initialize_counts(cls, conf):
        """Input: dict (counts configuration)
           Output: dict"""
        return [{'column_name': c['column'],
                 'column_value': c['match'],
                 'interval_duration_sec' : c['period']*60,
                 'groupby': c['groupby'],
                 'groups' : {}}
                if 'groupby' in c else
                {'column_name': c['column'],
                 'column_value': c['match'],
                 'interval_duration_sec' : c['period']*60,
                 'current' : {'interval_started_at' : 0,
                              'value' : 0},
                 'previous' : []}
                for c in conf]

