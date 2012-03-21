import os
import time
import Queue
import unittest
import datetime

import sys; sys.path.append('..')
from __message import Message
from rwtypes.readers.log.LogReader import LogReader


def get_queue(maxsize=1024):
    return Queue.Queue(maxsize=maxsize)


def log_to_sum(g):
    def wrapper(self):
        logpath = '/tmp/sum.log'
        f = open(logpath, 'w')
        f.write('foo\tbar\t[30/Jan/2012:18:07:09 +0000]\t5\n')
        f.write('foo\tbar\t[30/Jan/2012:18:08:09 +0000]\t7\n')
        f.write('foo\tbar\t[30/Jan/2012:18:08:09 +0000]\t11\n')
        f.write('foo\tbar\t[30/Jan/2012:18:11:09 +0000]\t13\n')
        f.write('foo\tbar\t[30/Jan/2012:18:12:00 +0000]\t13\n')
        f.close()
        g(self)
        os.remove(logpath)
    return wrapper


def log_to_sum_with_date_and_time_columns(g):
    def wrapper(self):
        logpath = '/tmp/sum.log'
        f = open(logpath, 'w')
        f.write('foo\tbar\t30/Jan/2012\t18:07:09\t5\n')
        f.write('foo\tbar\t30/Jan/2012\t18:08:09\t7\n')
        f.write('foo\tbar\t30/Jan/2012\t18:08:09\t11\n')
        f.write('foo\tbar\t30/Jan/2012\t18:11:09\t13\n')
        f.write('foo\tbar\t30/Jan/2012\t18:12:00\t13\n')
        f.close()
        g(self)
        os.remove(logpath)
    return wrapper


def log_to_sum_2_columns(g):
    def wrapper(self):
        logpath = '/tmp/sum.log'
        f = open(logpath, 'w')
        f.write('foo\tbar\t[30/Jan/2012:18:07:09 +0000]\t5\t4\n')
        f.write('foo\tbar\t[30/Jan/2012:18:08:09 +0000]\t7\t2\n')
        f.write('foo\tbar\t[30/Jan/2012:18:08:09 +0000]\t11\t4\n')
        f.write('foo\tbar\t[30/Jan/2012:18:11:09 +0000]\t13\t2\n')
        f.write('foo\tbar\t[30/Jan/2012:18:12:00 +0000]\t13\t4\n')
        f.close()
        g(self)
        os.remove(logpath)
    return wrapper


def log_to_count(g):
    def wrapper(self):
        logpath = '/tmp/count.log'
        f = open(logpath, 'w')
        f.write('foo\tbar\t[30/Jan/2012:18:07:09 +0000]\tGET\n')
        f.write('foo\tbar\t[30/Jan/2012:18:08:09 +0000]\tGET\n')
        f.write('foo\tbar\t[30/Jan/2012:18:08:09 +0000]\tGET\n')
        f.write('foo\tbar\t[30/Jan/2012:18:11:09 +0000]\tPUT\n')
        f.write('foo\tbar\t[30/Jan/2012:18:12:00 +0000]\tDELETE\n')
        f.close()
        g(self)
        os.remove(logpath)
    return wrapper


def log_to_count_with_date_and_time_columns(g):
    def wrapper(self):
        logpath = '/tmp/count.log'
        f = open(logpath, 'w')
        f.write('foo\tbar\t30/Jan/2012\t18:07:09\tGET\n')
        f.write('foo\tbar\t30/Jan/2012\t18:08:09\tGET\n')
        f.write('foo\tbar\t30/Jan/2012\t18:08:09\tGET\n')
        f.write('foo\tbar\t30/Jan/2012\t18:11:09\tPUT\n')
        f.write('foo\tbar\t30/Jan/2012\t18:12:00\tDELETE\n')
        f.close()
        g(self)
        os.remove(logpath)
    return wrapper


def log_to_count_2_columns(g):
    def wrapper(self):
        logpath = '/tmp/count.log'
        f = open(logpath, 'w')
        f.write('foo\tbar\t[30/Jan/2012:18:07:09 +0000]\tGET\t200\n')
        f.write('foo\tbar\t[30/Jan/2012:18:08:09 +0000]\tGET\t200\n')
        f.write('foo\tbar\t[30/Jan/2012:18:08:09 +0000]\tGET\t404\n')
        f.write('foo\tbar\t[30/Jan/2012:18:11:09 +0000]\tPUT\t404\n')
        f.write('foo\tbar\t[30/Jan/2012:18:12:00 +0000]\tDELETE\t200\n')
        f.close()
        g(self)
        os.remove(logpath)
    return wrapper


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

    def test_reading_log_and_saving_into_queue(self):
        # starting reader
        q = get_queue()
        conf = {'logpath': self.logpath,
                'checkpoint_path': self.reader_checkpoint,
                'checkpoint_enabled': True}
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
        conf = {'logpath': self.logpath, 'delimiter': '\t'}
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
        conf = {'logpath': self.logpath,
                'delimiter': '\t',
                'columns': ['col0', 'col1', 'col2']}
        myreader = LogReader(q, conf=conf)
        myreader.start()

        # time to process log lines
        time.sleep(0.1)

        msg = q.get()
        self.assertEqual({'col0': 'a',
                          'col1': 'b',
                          'col2': 'c'}, msg.content)

        msg = q.get()
        self.assertEqual({'col0': 'x',
                          'col1': 'y',
                          'col2': 'z'}, msg.content)

    def test_saving_checkpoint_in_bytes_read(self):
        # starting reader
        f = open(self.reader_checkpoint, 'rb')
        q = get_queue()
        conf = {'logpath': self.logpath,
                'checkpoint_path': self.reader_checkpoint,
                'checkpoint_enabled': True}
        myreader = LogReader(q, conf=conf)
        myreader.start()

        # time to process log lines
        time.sleep(0.1)

        msg = q.get()
        self.assertEqual(6, msg.checkpoint['bytes_read'])

        msg = q.get()
        self.assertEqual(12, msg.checkpoint['bytes_read'])


    ########################## SUMS TESTS ##########################

    @log_to_sum
    def test_summing_without_groupby(self):
        q = get_queue()
        conf = {'logpath': '/tmp/sum.log',
                'columns': ['c0', 'c1', 'datetime', 'primes'],
                'delimiter': '\t',
                'datetime_column': 'datetime',
                'sums': [{'column': 'primes', 'period': 1}]}
        myreader = LogReader(q, conf=conf)
        myreader.start()

        # time to process
        time.sleep(0.1)

        # get messages
        messages = []
        while q.qsize() > 0: messages.append(q.get())

        result = map(lambda x: (x.content['interval_started_at'].minute,
                                x.content['value']), messages)
        self.assertIn((7, 5), result)
        self.assertIn((8, 18), result)
        self.assertIn((9, 0), result)
        self.assertIn((10, 0), result)
        self.assertIn((11, 13), result)

    @log_to_sum_with_date_and_time_columns
    def test_summing_with_date_and_time_columns_without_groupby(self):
        q = get_queue()
        conf = {'logpath': '/tmp/sum.log',
                'columns': ['c0', 'c1', 'date', 'time', 'primes'],
                'delimiter': '\t',
                'date_column': 'date',
                'time_column': 'time',
                'sums': [{'column': 'primes', 'period': 1}]}
        myreader = LogReader(q, conf=conf)
        myreader.start()

        # time to process
        time.sleep(0.1)

        # get messages
        messages = []
        while q.qsize() > 0: messages.append(q.get())

        result = map(lambda x: (x.content['interval_started_at'].minute,
                                x.content['value']), messages)
        self.assertIn((7, 5), result)
        self.assertIn((8, 18), result)
        self.assertIn((9, 0), result)
        self.assertIn((10, 0), result)
        self.assertIn((11, 13), result)

    @log_to_sum_2_columns
    def test_summing_2_columns_without_groupby(self):
        q = get_queue()
        conf = {'logpath': '/tmp/sum.log',
                'columns': ['c0', 'c1', 'datetime',
                             'primes', 'evens'],
                'delimiter': '\t',
                'datetime_column': 'datetime',
                'sums': [{'column': 'primes',
                          'period': 1},
                         {'column': 'evens',
                          'period': 1}]}
        myreader = LogReader(q, conf=conf)
        myreader.start()

        # time to process
        time.sleep(0.1)

        messages = []
        while q.qsize() > 0: messages.append(q.get())

        # it should generate messages for the
        # minutes: 7, 8, 9, 10, 11 (x 2, since there are 2 sums)
        self.assertEqual(10, len(messages))

        evens = filter(lambda x: x.content['column_name'] == 'evens', messages)
        primes = filter(lambda x: x.content['column_name'] == 'primes', messages)

        # assert that all minutes were delivered for each sum
        self.assertEqual(5, len(primes))
        self.assertEqual(5, len(evens))

        result = map(lambda x: (x.content['interval_started_at'].minute,
                                x.content['value']), primes)
        self.assertIn((7, 5), result)
        self.assertIn((8, 18), result)
        self.assertIn((9, 0), result)
        self.assertIn((10, 0), result)
        self.assertIn((11, 13), result)

        result = map(lambda x: (x.content['interval_started_at'].minute,
                                x.content['value']), evens)
        self.assertIn((7, 4), result)
        self.assertIn((8, 6), result)
        self.assertIn((9, 0), result)
        self.assertIn((10, 0), result)
        self.assertIn((11, 2), result)


    ########################## COUNT TESTS ##########################

    @log_to_count
    def test_counting_without_groupby(self):
        q = get_queue()
        conf = {'logpath': '/tmp/count.log',
                'columns': ['c0', 'c1', 'datetime', 'method'],
                'delimiter': '\t',
                'datetime_column': 'datetime',
                'counts': [{'column': 'method',
                            'match': 'GET',
                            'period': 1}]}
        myreader = LogReader(q, conf=conf)
        myreader.start()

        # time to process
        time.sleep(0.1)

        messages = []
        while q.qsize() > 0: messages.append(q.get())

        result = map(lambda x: (x.content['interval_started_at'].minute,
                                x.content['value']), messages)

        self.assertIn((7, 1), result)
        self.assertIn((8, 2), result)
        self.assertIn((9, 0), result)
        self.assertIn((10, 0), result)
        self.assertIn((11, 0), result)


    @log_to_count_with_date_and_time_columns
    def test_counting_with_date_and_time_columns_without_groupby(self):
        q = get_queue()
        conf = {'logpath': '/tmp/count.log',
                'columns': ['c0', 'c1', 'date', 'time', 'method'],
                'delimiter': '\t',
                'date_column': 'date',
                'time_column': 'time',
                'counts': [{'column': 'method',
                            'match': 'GET',
                            'period': 1}]}
        myreader = LogReader(q, conf=conf)
        myreader.start()

        # time to process
        time.sleep(0.1)

        messages = []
        while q.qsize() > 0: messages.append(q.get())

        result = map(lambda x: (x.content['interval_started_at'].minute,
                                x.content['value']), messages)

        self.assertIn((7, 1), result)
        self.assertIn((8, 2), result)
        self.assertIn((9, 0), result)
        self.assertIn((10, 0), result)
        self.assertIn((11, 0), result)

    @log_to_count_2_columns
    def test_counting_2_columns_without_groupby(self):

        q = get_queue()
        conf = {'logpath': '/tmp/count.log',
                'columns': ['c0', 'c1', 'datetime',
                             'method', 'status'],
                'delimiter': '\t',
                'datetime_column': 'datetime',
                'counts': [{'column': 'method',
                            'match': 'GET',
                            'period': 1,},
                           {'column': 'status',
                            'match': '200',
                            'period': 1,}]}
        myreader = LogReader(q, conf=conf)
        myreader.start()

        # time to process
        time.sleep(0.1)

        messages = []
        while q.qsize() > 0: messages.append(q.get())

        # it should generate messages for the
        # minutes: 7, 8, 9, 10, 11 (x 2, since there are 2 counts)
        self.assertEqual(10, len(messages))

        status = filter(lambda x: x.content['column_name'] == 'status', messages)
        methods = filter(lambda x: x.content['column_name'] == 'method', messages)

        # assert that all minutes were delivered for each count
        self.assertEqual(5, len(status))
        self.assertEqual(5, len(methods))

        result = map(lambda x: (x.content['interval_started_at'].minute,
                                x.content['value']), methods)
        self.assertIn((7, 1), result)
        self.assertIn((8, 2), result)
        self.assertIn((9, 0), result)
        self.assertIn((10, 0), result)
        self.assertIn((11, 0), result)

        result = map(lambda x: (x.content['interval_started_at'].minute,
                                x.content['value']), status)
        self.assertIn((7, 1), result)
        self.assertIn((8, 1), result)
        self.assertIn((9, 0), result)
        self.assertIn((10, 0), result)
        self.assertIn((11, 0), result)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestLogReader))
    return suite


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
