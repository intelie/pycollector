import unittest
import time
import Queue

import sys; sys.path.append('..')
from __reader import Reader
from __writer import Writer


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
                self.store(msg="life is beautiful")
                return True

        q = get_queue()
        myreader = MyReader(q)
        myreader.start()
        time.sleep(3)
        size = q.qsize()
        self.assertTrue(size >= 3)

    def testSingleSchedulingAddingToQueue(self):
        class MyReader(Reader):
            def read(self):
                n = 0
                while n < 3:
                    self.store(msg="love is all you need")
                    n += 1

        q = get_queue()
        myreader = MyReader(q)
        myreader.start()
        time.sleep(1)
        self.assertEqual(3, q.qsize())
        self.assertEqual(3, myreader.processed)



if __name__ == "__main__":
    unittest.main()

