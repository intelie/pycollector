import os
import time
import Queue
import unittest

import sys; sys.path.append('..')
from __message import Message
from rwtypes.readers.log.LogReader import LogReader


def get_queue(maxsize=1024):
    return Queue.Queue(maxsize=maxsize)


class TestLogReader(unittest.TestCase):
    def setUp(self):
        # write log file
        self.logpath = '/tmp/a.log'
        self.f = open(self.logpath, 'w')
        self.f.write('a\tb\tc\n')
        self.f.write('x\ty\tz\n')
        self.f.close()

    def tearDown(self):
        os.remove(self.logpath)

    def test_dictify_line(self):
        line = 'a\tb\tc'
        delimiter = '\t'
        columns = ['c0', 'c1', 'c2']
        expected = {'c0' : 'a', 'c1' : 'b', 'c2' : 'c'}
        result = LogReader.dictify_line(line, delimiter, columns)
        self.assertEqual(expected, result)

    def test_split_line(self):
        line = 'a\tb\tc'
        delimiter = '\t'
        expected = ['a', 'b', 'c']
        result = LogReader.split_line(line, delimiter)
        self.assertEqual(expected, result)

    def test_reading_log_and_saving_into_queue(self):
        # starting reader
        q = get_queue()
        conf = {'logpath' : self.logpath}
        myreader = LogReader(q, conf=conf)
        myreader.start()

        # time to process log lines
        time.sleep(0.2)

        msg = q.get()
        self.assertEqual(6, msg.checkpoint)
        self.assertEqual('a\tb\tc\n', msg.content)

        msg = q.get()
        self.assertEqual(12, msg.checkpoint)
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
        self.assertEqual(6, msg.checkpoint)
        self.assertEqual(['a', 'b', 'c'], msg.content)

        msg = q.get()
        self.assertEqual(12, msg.checkpoint)
        self.assertEqual(['x', 'y', 'z'], msg.content)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestLogReader))
    return suite


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
