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
        f.write('first_column\tsecond_column\t[30/Jan/2012:18:07:09 +0000]\t5\n')
        f.write('first_column\tsecond_column\t[30/Jan/2012:18:08:09 +0000]\t7\n')
        f.write('first_column\tsecond_column\t[30/Jan/2012:18:08:09 +0000]\t11\n')
        f.write('first_column\tsecond_column\t[30/Jan/2012:18:11:09 +0000]\t13\n')
        f.write('first_column\tsecond_column\t[30/Jan/2012:18:12:00 +0000]\t13\n')
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

    def xtest_initialize_sums_with_groupby(self):
        sums_conf = [{'column' : 'bytes_sent',
                      'period' : 1,
                      'group_by': {
                          'column' : 'host',
                          'match' : '(?P<host_name>.*)',
                         }}]
        result = LogReader.initialize_sums(sums_conf)
        expected = [{'interval_started_at': 0,
                     'column_name': 'bytes_sent',
                     'group_by': {
                         'column_name' : 'host',
                         'match' : '(?P<host_name>.*)'
                         },
                     'interval_duration_sec' : 60,
                     'grouped_values': {},
                     'grouped_remaining': {},
                     'grouped_zeros': {}}]
        self.assertEqual(expected, result)

    @log_to_sum
    def xtest_summing_without_groupby(self):
        q = get_queue()
        conf = {'logpath' : '/tmp/sum.log',
                'columns' : ['c0', 'c1', 'datetime', 'primes'],
                'delimiter' : '\t',
                'datetime_column': 'datetime',
                'sums' : [{'column' : 'primes', 'period': 1}]}
        myreader = LogReader(q, conf=conf)
        myreader.start()

        # time to process
        time.sleep(0.1)

        content = q.get().content
        self.assertEqual(7, content['interval_started_at'].minute)
        self.assertEqual(5, content['value'])

        content = q.get().content
        self.assertEqual(8, content['interval_started_at'].minute)
        self.assertEqual(18, content['value'])

        content = q.get().content
        self.assertEqual(9, content['interval_started_at'].minute)
        self.assertEqual(0, content['value'])

        content = q.get().content
        self.assertEqual(10, content['interval_started_at'].minute)
        self.assertEqual(0, content['value'])

        content = q.get().content
        self.assertEqual(11, content['interval_started_at'].minute)
        self.assertEqual(13, content['value'])

    def xtest_summing_with_groupby(self):
        logpath = '/tmp/sum.log'
        f = open(logpath, 'w')
        f.write('g0\tsecond_column\t[30/Jan/2012:18:07:09 +0000]\t5\n')
        f.write('g1\tsecond_column\t[30/Jan/2012:18:08:09 +0000]\t7\n')
        f.write('g1\tsecond_column\t[30/Jan/2012:18:08:09 +0000]\t11\n')
        f.write('g0\tsecond_column\t[30/Jan/2012:18:11:09 +0000]\t13\n')
        f.write('g1\tsecond_column\t[30/Jan/2012:18:12:00 +0000]\t13\n')
        f.close()

        q = get_queue()
        conf = {'logpath' : logpath,
                'columns' : ['c0', 'c1', 'datetime', 'primes'],
                'delimiter' : '\t',
                'datetime_column': 'datetime',
                'sums' : [{'column' : 'primes',
                           'period': 1,
                           'group_by': {
                               'column' : 'c0',
                               'match' : '(?P<host>.*)' }}]}

        myreader = LogReader(q, conf=conf)
        myreader.start()

        # time to process
        time.sleep(0.1)

        messages = []
        while q.qsize() > 0:
            messages.append(q.get())

        self.assertEqual(9, len(messages))

        checks = 0
        for m in messages:
            content = m.content
            if content['interval_started_at'].minute == 7 and \
                content['host'] == 'g0':
                self.assertEqual(5, content['value'])
                checks += 1
                continue

            if content['interval_started_at'].minute == 8 and \
                content['host'] == 'g0':
                self.assertEqual(0, content['value'])
                checks += 1
                continue

            if content['interval_started_at'].minute == 8 and \
                content['host'] == 'g1':
                self.assertEqual(18, content['value'])
                checks += 1
                continue

            if (content['interval_started_at'].minute == 9 or \
                content['interval_started_at'].minute == 10) and \
                (content['host'] == 'g0' or content['host'] == 'g1'):
                self.assertEqual(0, content['value'])
                checks += 1
                continue

            if content['interval_started_at'].minute == 11 and \
                content['host'] == 'g0':
                self.assertEqual(13, content['value'])
                checks += 1
                continue

            if content['interval_started_at'].minute == 11 and \
                content['host'] == 'g1':
                self.assertEqual(0, content['value'])
                checks += 1
                continue

        self.assertEqual(9, checks)


        os.remove(logpath)


    def xtest_summing_2_columns_without_groupby(self):
        logpath = '/tmp/sum.log'
        f = open(logpath, 'w')
        f.write('first_column\tsecond_column\t[30/Jan/2012:18:07:09 +0000]\t5\t4\n')
        f.write('first_column\tsecond_column\t[30/Jan/2012:18:08:09 +0000]\t7\t2\n')
        f.write('first_column\tsecond_column\t[30/Jan/2012:18:08:09 +0000]\t11\t4\n')
        f.write('first_column\tsecond_column\t[30/Jan/2012:18:11:09 +0000]\t13\t2\n')
        f.write('first_column\tsecond_column\t[30/Jan/2012:18:12:00 +0000]\t13\t4\n')
        f.close()

        q = get_queue()
        conf = {'logpath' : logpath,
                'columns' : ['c0', 'c1', 'datetime', 'primes', 'evens'],
                'delimiter' : '\t',
                'datetime_column': 'datetime',
                'sums' : [{'column': 'primes',
                           'period': 1},
                          {'column': 'evens',
                           'period': 1}]}
        myreader = LogReader(q, conf=conf)
        myreader.start()

        # time to process
        time.sleep(0.1)

        messages = []
        while q.qsize() > 0:
            messages.append(q.get())

        # it should generate messages for the
        # minutes: 7, 8, 9, 10, 11 (x 2, since there are 2 sums)
        self.assertEqual(10, len(messages))

        even_messages = []
        prime_messages = []
        for m in messages:
            if m.content['column_name'] == 'primes':
                prime_messages.append(m)
            else:
                even_messages.append(m)

        # assert that all minutes were delivered for each sum
        self.assertEqual(5, len(prime_messages))
        self.assertEqual(5, len(even_messages))

        checks = 0
        for message in prime_messages:
            content = message.content
            if content['interval_started_at'].minute == 7:
                self.assertEqual(5, content['value'])
                checks += 1

            if content['interval_started_at'].minute == 8:
                self.assertEqual(18, content['value'])
                checks += 1

            if content['interval_started_at'].minute == 9:
                self.assertEqual(0, content['value'])
                checks += 1

            if content['interval_started_at'].minute == 10:
                self.assertEqual(0, content['value'])
                checks += 1

            if content['interval_started_at'].minute == 11:
                self.assertEqual(13, content['value'])
                checks += 1

        self.assertEqual(5, checks)

        checks = 0
        for message in even_messages:
            content = message.content
            if content['interval_started_at'].minute == 7:
                self.assertEqual(4, content['value'])
                checks += 1

            if content['interval_started_at'].minute == 8:
                self.assertEqual(6, content['value'])
                checks += 1

            if content['interval_started_at'].minute == 9:
                self.assertEqual(0, content['value'])
                checks += 1

            if content['interval_started_at'].minute == 10:
                self.assertEqual(0, content['value'])
                checks += 1

            if content['interval_started_at'].minute == 11:
                self.assertEqual(2, content['value'])
                checks += 1

        self.assertEqual(5, checks)
        os.remove(logpath)

    def xtest_counting_without_groupby(self):
        logpath = '/tmp/count.log'
        f = open(logpath, 'w')
        f.write('first_column\tsecond_column\t[30/Jan/2012:18:07:09 +0000]\tGET\n')
        f.write('first_column\tsecond_column\t[30/Jan/2012:18:08:09 +0000]\tGET\n')
        f.write('first_column\tsecond_column\t[30/Jan/2012:18:08:09 +0000]\tGET\n')
        f.write('first_column\tsecond_column\t[30/Jan/2012:18:11:09 +0000]\tPUT\n')
        f.write('first_column\tsecond_column\t[30/Jan/2012:18:12:00 +0000]\tDELETE\n')
        f.close()

        q = get_queue()
        conf = {'logpath' : logpath,
                'columns' : ['c0', 'c1', 'datetime', 'method'],
                'delimiter' : '\t',
                'datetime_column': 'datetime',
                'counts' : [{'column' : 'method',
                             'match': 'GET',
                             'period': 1}]}
        myreader = LogReader(q, conf=conf)
        myreader.start()

        content = q.get().content
        self.assertEqual(7, content['interval_started_at'].minute)
        self.assertEqual(1, content['value'])

        content = q.get().content
        self.assertEqual(8, content['interval_started_at'].minute)
        self.assertEqual(2, content['value'])

        content = q.get().content
        self.assertEqual(9, content['interval_started_at'].minute)
        self.assertEqual(0, content['value'])

        content = q.get().content
        self.assertEqual(10, content['interval_started_at'].minute)
        self.assertEqual(0, content['value'])

        content = q.get().content
        self.assertEqual(11, content['interval_started_at'].minute)
        self.assertEqual(0, content['value'])

        os.remove(logpath)

    def xtest_counting_2_columns_without_groupby(self):
        logpath = '/tmp/count.log'
        f = open(logpath, 'w')
        f.write('first_column\tsecond_column\t[30/Jan/2012:18:07:09 +0000]\tGET\t200\n')
        f.write('first_column\tsecond_column\t[30/Jan/2012:18:08:09 +0000]\tGET\t200\n')
        f.write('first_column\tsecond_column\t[30/Jan/2012:18:08:09 +0000]\tGET\t404\n')
        f.write('first_column\tsecond_column\t[30/Jan/2012:18:11:09 +0000]\tPUT\t404\n')
        f.write('first_column\tsecond_column\t[30/Jan/2012:18:12:00 +0000]\tDELETE\t200\n')
        f.close()

        q = get_queue()
        conf = {'logpath' : logpath,
                'columns' : ['c0', 'c1', 'datetime', 'method', 'status'],
                'delimiter' : '\t',
                'datetime_column': 'datetime',
                'counts' : [{'column': 'method',
                             'match': 'GET',
                             'period': 1,},
                            {'column': 'status',
                             'match' : '200',
                             'period' : 1,}]}
        myreader = LogReader(q, conf=conf)
        myreader.start()

        # give time to process messages
        time.sleep(0.1)

        messages = []
        while q.qsize() > 0:
            messages.append(q.get())

        # it should generate messages for the
        # minutes: 7, 8, 9, 10, 11 (x 2, since there are 2 counts)
        self.assertEqual(10, len(messages))

        status_messages = []
        method_messages = []
        for m in messages:
            if m.content['column_name'] == 'status':
                status_messages.append(m)
            else:
                method_messages.append(m)

        # assert that all minutes were delivered for each count
        self.assertEqual(5, len(status_messages))
        self.assertEqual(5, len(method_messages))

        checks = 0
        for message in method_messages:
            content = message.content
            if content['interval_started_at'].minute == 7:
                self.assertEqual(1, content['value'])
                checks += 1

            if content['interval_started_at'].minute == 8:
                self.assertEqual(2, content['value'])
                checks += 1

            if content['interval_started_at'].minute == 9:
                self.assertEqual(0, content['value'])
                checks += 1

            if content['interval_started_at'].minute == 10:
                self.assertEqual(0, content['value'])
                checks += 1

            if content['interval_started_at'].minute == 11:
                self.assertEqual(0, content['value'])
                checks += 1

        self.assertEqual(5, checks)

        checks = 0
        for message in status_messages:
            content = message.content
            if content['interval_started_at'].minute == 7:
                self.assertEqual(1, content['value'])
                checks += 1

            if content['interval_started_at'].minute == 8:
                self.assertEqual(1, content['value'])
                checks += 1

            if content['interval_started_at'].minute == 9:
                self.assertEqual(0, content['value'])
                checks += 1

            if content['interval_started_at'].minute == 10:
                self.assertEqual(0, content['value'])
                checks += 1

            if content['interval_started_at'].minute == 11:
                self.assertEqual(0, content['value'])
                checks += 1
        self.assertEqual(5, checks)

        os.remove(logpath)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestLogReader))
    return suite


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
