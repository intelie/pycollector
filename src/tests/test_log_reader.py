import os
import time
import Queue
import unittest
import datetime

import sys; sys.path.append('..')
from __message import Message
from __exceptions import ParsingError
from rwtypes.readers.log.LogReader import LogReader


def get_queue(maxsize=1024):
    return Queue.Queue(maxsize=maxsize)


class TestLogReader(unittest.TestCase):
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
        result = LogReader.dictify_line(line, delimiter, columns)
        self.assertEqual(expected, result)

    def test_dictify_line_with_lower_number_of_columns_should_raise_exception(self):
        line = 'foo:bar'
        delimiter = ':'
        columns = ['c0', 'c1', 'c2']
        self.assertRaises(ParsingError, LogReader.dictify_line, line, delimiter, columns)

    def test_dictify_line_with_greater_number_of_columns_should_raise_exception(self):
        line = 'foo:bar:spam:spam'
        delimiter = ':'
        columns = ['c0', 'c1', 'c2']
        self.assertRaises(ParsingError, LogReader.dictify_line, line, delimiter, columns)

    def test_split_line(self):
        line = 'a\tb\tc'
        delimiter = '\t'
        expected = ['a', 'b', 'c']
        result = LogReader.split_line(line, delimiter)
        self.assertEqual(expected, result)

    def test_reading_log_and_saving_into_queue(self):
        # starting reader
        q = get_queue()
        conf = {'logpath' : self.logpath,
                'checkpoint_path' : self.reader_checkpoint,
                'checkpoint_enabled' : True}
        myreader = LogReader(q, conf=conf)
        myreader.start()

        # time to process log lines
        time.sleep(0.1)

        msg = q.get()
        self.assertEqual('a\tb\tc\n', msg.content)

        msg = q.get()
        self.assertEqual('x\ty\tz\n', msg.content)

    def test_reading_log_with_delimiters_and_saving_into_queue(self):
        # starting reader
        q = get_queue()
        conf = {'logpath' : self.logpath, 'delimiter': '\t'}
        myreader = LogReader(q, conf=conf)
        myreader.start()

        # time to process log lines
        time.sleep(0.1)

        msg = q.get()
        self.assertEqual(['a', 'b', 'c'], msg.content)

        msg = q.get()
        self.assertEqual(['x', 'y', 'z'], msg.content)

    def test_reading_log_with_delimiters_and_columns_and_saving_into_queue(self):
        q = get_queue()
        conf = {'logpath' : self.logpath,
                'delimiter': '\t',
                'columns' : ['col0', 'col1', 'col2']}
        myreader = LogReader(q, conf=conf)
        myreader.start()

        # time to process log lines
        time.sleep(0.1)

        msg = q.get()
        self.assertEqual({'col0' : 'a', 'col1': 'b', 'col2': 'c'}, msg.content)

        msg = q.get()
        self.assertEqual({'col0' : 'x', 'col1': 'y', 'col2': 'z'}, msg.content)

    def test_saving_checkpoint_in_bytes_read(self):
        # starting reader
        f = open(self.reader_checkpoint, 'rb')
        q = get_queue()
        conf = {'logpath' : self.logpath,
                'checkpoint_path' : self.reader_checkpoint,
                'checkpoint_enabled' : True}
        myreader = LogReader(q, conf=conf)
        myreader.start()

        # time to process log lines
        time.sleep(0.1)

        msg = q.get()
        self.assertEqual(6, msg.checkpoint['bytes_read'])

        msg = q.get()
        self.assertEqual(12, msg.checkpoint['bytes_read'])

    def test_getting_datetime_from_dictified_line_and_datetime_column(self):
        dictified_line = {'a':1, 
                          'b': 2, 
                          'c': '[30/Jan/2012:18:01:03 +0000]'}
        column = 'c'
        result = LogReader.get_datetime(dictified_line, column)
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
        result = LogReader.get_datetime(dictified_line, 'date', 'time')
        self.assertEqual(2012, result.year)
        self.assertEqual(1, result.month)
        self.assertEqual(30, result.day)
        self.assertEqual(18, result.hour)
        self.assertEqual(0, result.minute)
        self.assertEqual(9, result.second)

    def test_getting_starting_minute_from_datetime(self):
        dt = datetime.datetime(2012, 12, 1, 13, 37, 7)
        result = LogReader.get_starting_minute(dt)
        self.assertEqual(2012, result.year)
        self.assertEqual(12, result.month)
        self.assertEqual(1, result.day)
        self.assertEqual(13, result.hour)
        self.assertEqual(37, result.minute)
        self.assertEqual(0, result.second)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestLogReader))
    return suite


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
