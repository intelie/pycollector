import unittest
import Queue

import sys; sys.path.append('..')
from __reader import *


def get_queue():
    q = Queue.Queue()
    q.maxsize = 1024
    return q


class TestReader(unittest.TestCase):
    def testPeriodicSchedulingAddingToQueue(self):
        class MyReader(Reader):
            def setup(self):
                self.periodic = True
                self.interval = 1
            def read(self):
                return "life is beautiful"

        q = get_queue()
        myreader = MyReader(q)
        myreader.start()
        time.sleep(3)
        self.assertEqual(q.qsize(), 3)

    def testSingleSchedulingAddingToQueue(self):
        class MyReader(Reader):
            def setup(self):
                self.periodic = False
            def read(self):
                return "love is all you need"

        q = get_queue()
        myreader = MyReader(q)
        myreader.start()
        time.sleep(3)
        self.assertEqual(q.qsize(), 1)


if __name__ == "__main__":
    unittest.main()

