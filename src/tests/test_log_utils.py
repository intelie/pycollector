import os
import unittest
import datetime

import sys; sys.path.append('..')
from __exceptions import ParsingError
from rwtypes.readers.log.LogUtils import LogUtils


class TestLogUtils(unittest.TestCase):
    def setUp(self):
        # write log file
        self.reader_checkpoint = '/tmp/rcheckpoint'
        open(self.reader_checkpoint, 'w').close()
        self.logpath = '/tmp/a.log'
        self.f = open(self.logpath, 'w')
        self.f.write('a\tb\tc\n')
        self.f.write('x\ty\tz\n')
        self.f.close()

    def tearDown(self):
        os.remove(self.logpath)
        os.remove(self.reader_checkpoint)

    def test_dictify_line(self):
        line = 'a\tb\tc'
        delimiter = '\t'
        columns = ['c0', 'c1', 'c2']
        expected = {'c0' : 'a', 'c1' : 'b', 'c2' : 'c'}
        result = LogUtils.dictify_line(line, delimiter, columns)
        self.assertEqual(expected, result)

    def test_dictify_line_with_lower_number_of_columns_should_raise_exception(self):
        line = 'foo:bar'
        delimiter = ':'
        columns = ['c0', 'c1', 'c2']
        self.assertRaises(ParsingError, LogUtils.dictify_line, line, delimiter, columns)

    def test_dictify_line_with_greater_number_of_columns_should_raise_exception(self):
        line = 'foo:bar:spam:spam'
        delimiter = ':'
        columns = ['c0', 'c1', 'c2']
        self.assertRaises(ParsingError, LogUtils.dictify_line, line, delimiter, columns)

    def test_split_line(self):
        line = 'a\tb\tc'
        delimiter = '\t'
        expected = ['a', 'b', 'c']
        result = LogUtils.split_line(line, delimiter)
        self.assertEqual(expected, result)

    def test_getting_datetime_from_dictified_line_and_datetime_column(self):
        dictified_line = {'a':1,
                          'b': 2,
                          'c': '[30/Jan/2012:18:01:03 +0000]'}
        column = 'c'
        result = LogUtils.get_datetime(dictified_line, column)
        self.assertEqual(2012, result.year)
        self.assertEqual(1, result.month)
        self.assertEqual(30, result.day)
        self.assertEqual(18, result.hour)
        self.assertEqual(1, result.minute)
        self.assertEqual(3, result.second)

    def test_getting_datetime_from_dictified_line_with_date_and_time_columns(self):
        dictified_line = {'a' : 234,
                          'date' : '2012-01-30',
                          'time' : '18:00:09'}
        result = LogUtils.get_datetime(dictified_line, 'date', 'time')
        self.assertEqual(2012, result.year)
        self.assertEqual(1, result.month)
        self.assertEqual(30, result.day)
        self.assertEqual(18, result.hour)
        self.assertEqual(0, result.minute)
        self.assertEqual(9, result.second)

    def test_getting_starting_minute_from_datetime(self):
        dt = datetime.datetime(2012, 12, 1, 13, 37, 7)
        result = LogUtils.get_starting_minute(dt)
        self.assertEqual(2012, result.year)
        self.assertEqual(12, result.month)
        self.assertEqual(1, result.day)
        self.assertEqual(13, result.hour)
        self.assertEqual(37, result.minute)
        self.assertEqual(0, result.second)

    def test_get_interval_from_datetime_and_period(self):
        dt = datetime.datetime(2012, 12, 1, 13, 42, 3)
        period = 5*60 # 5 minutes
        result = LogUtils.get_interval(dt, period)
        start = datetime.datetime(2012, 12, 1, 13, 42, 0)
        end = datetime.datetime(2012, 12, 1, 13, 47, 0)
        expected = (start, end)
        self.assertEqual(expected, result)

    def test_initialize_sums_without_groupby(self):
        sums_conf = [{'column': 'bytes_sent',
                      'period':  1,}]
        expected = [{'column_name' : 'bytes_sent',
                     'interval_duration_sec' : 60,
                     'current' : {'interval_started_at' : 0,
                                  'value' : 0},
                     'previous' : []}]
        result = LogUtils.initialize_sums(sums_conf)
        self.assertEqual(expected, result)

    def test_initialize_counts_without_groupby(self):
        counts_conf = [{'column': 'method',
                      'match' : 'GET',
                      'period':  1,}]
        expected = [{'column_name' : 'method',
                     'column_value' : 'GET',
                     'interval_duration_sec' : 60,
                     'current' : {'interval_started_at' : 0,
                                  'value' : 0},
                     'previous' : []}]
        result = LogUtils.initialize_counts(counts_conf)
        self.assertEqual(expected, result)

    def test_get_missing_intervals(self):
        expected = [datetime.datetime(2012, 1, 1, 1, 1, 0),
                    datetime.datetime(2012, 1, 1, 1, 2, 0),
                    datetime.datetime(2012, 1, 1, 1, 3, 0),
                    datetime.datetime(2012, 1, 1, 1, 4, 0)]

        start = datetime.datetime(2012, 1, 1, 1, 1, 0)
        period = 60 # seconds
        event = datetime.datetime(2012, 1, 1, 1, 5, 0)
        result = LogUtils.get_missing_intervals(start, period, event)
        self.assertEqual(expected, result)

    def test_get_missing_intervals_with_seconds(self):
        expected = [datetime.datetime(2012, 1, 1, 1, 1, 0),
                    datetime.datetime(2012, 1, 1, 1, 2, 0),
                    datetime.datetime(2012, 1, 1, 1, 3, 0)]

        start = datetime.datetime(2012, 1, 1, 1, 1, 0)
        period = 60 # seconds
        event = datetime.datetime(2012, 1, 1, 1, 4, 34)
        result = LogUtils.get_missing_intervals(start, period, event)
        self.assertEqual(expected, result)

    def test_get_missing_intervals_should_return_empty_list(self):
        expected = []

        start = datetime.datetime(2012, 1, 1, 1, 1, 0)
        period = 60 # seconds
        event = datetime.datetime(2012, 1, 1, 1, 1, 34)
        result = LogUtils.get_missing_intervals(start, period, event)
        self.assertEqual(expected, result)

    def test_get_missing_intervals_with_2_minutes_period(self):
        expected = [datetime.datetime(2012, 1, 1, 1, 1, 0),
                    datetime.datetime(2012, 1, 1, 1, 3, 0)]

        start = datetime.datetime(2012, 1, 1, 1, 1, 0)
        period = 120 # seconds
        event = datetime.datetime(2012, 1, 1, 1, 5, 34)
        result = LogUtils.get_missing_intervals(start, period, event)
        self.assertEqual(expected, result)

    def test_get_missing_intervals_with_2_minutes_period_should_return_empty_list(self):
        expected = []

        start = datetime.datetime(2012, 1, 1, 1, 1, 0)
        period = 120 # seconds
        event = datetime.datetime(2012, 1, 1, 1, 2, 42)
        result = LogUtils.get_missing_intervals(start, period, event)
        self.assertEqual(expected, result)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestLogUtils))
    return suite


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
