import unittest
import time
import Queue

import sys; sys.path.append('..')
from __reader import *
from __writer import *


def get_queue():
    q = Queue.Queue()
    q.maxsize = 1024
    return q


class TestReader(unittest.TestCase):
    def testPeriodicSchedulingAddingToQueue(self):
        class MyReader(Reader):
            def setup(self):
                self.interval = 1

            def read(self):
                return "life is beautiful"

        q = get_queue()
        myreader = MyReader(q)
        myreader.start()
        time.sleep(3)
        size = q.qsize()
        self.assertTrue(size >= 3)

    def testSingleSchedulingAddingToQueue(self):
        class MyReader(Reader):
            def read(self):
                while True:
                    self.store("love is all you need")
                    time.sleep(1)

        q = get_queue()
        myreader = MyReader(q)
        myreader.start()
        time.sleep(3)
        size = q.qsize()
        self.assertTrue(size >= 3)


if __name__ == "__main__":
    unittest.main()

