import unittest
import Queue

import sys; sys.path.append('..')
from __writer import *


def get_queue():
    q = Queue.Queue()
    q.maxsize = 1024
    return q


class TestWriter(unittest.TestCase):
    def testPeriodicSchedulingRemovingFromQueue(self):
        class MyWriter(Writer):
            def setup(self):
                self.periodic = True
                self.interval = 1
            def write(self, msg):
                return msg

        q = get_queue()
        q.put(1); q.put(2)
        mywriter = MyWriter(q)
        mywriter.start()
        time.sleep(2)
        self.assertEqual(q.qsize(), 0)

    def testSingleSchedulingRemovingFromQueue(self):
        class MyWriter(Writer):
            def setup(self):
                self.periodic = False
            def write(self, msg):
                return msg

        q = get_queue()
        q.put(1); q.put(2); q.put(3)
        mywriter = MyWriter(q)
        mywriter.start()
        time.sleep(3)
        self.assertEqual(q.qsize(), 2)


if __name__ == "__main__":
    unittest.main()

