import unittest
import Queue

import sys; sys.path.append('..')
from __writer import *


def get_queue():
    q = Queue.Queue()
    q.maxsize = 1024
    return q


class TestWriter(unittest.TestCase):
    def test_periodic_scheduling_removing_from_queue(self):
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
        self.assertEqual(0, q.qsize())

    #TODO: test callback from reader.

    def test_writer_processed_messages(self):
        class MyWriter(Writer):
            def write(self, msg):
                return True

        q = get_queue()
        q.put(1); q.put(2)
        mywriter = MyWriter(q)
        mywriter.start()
        mywriter.process()
        mywriter.process()
        self.assertEqual(2, mywriter.processed)


if __name__ == "__main__":
    unittest.main()

