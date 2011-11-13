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
                self.interval = 1

            def write(self, msg):
                return msg

        q = get_queue()
        q.put(1); q.put(2)
        mywriter = MyWriter(q)
        mywriter.start()
        time.sleep(2)
        self.assertEqual(q.qsize(), 0)

    #TODO: test callback from reader.


if __name__ == "__main__":
    unittest.main()

