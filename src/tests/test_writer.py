import time
import unittest
import Queue

import sys; sys.path.append('..')
from __writer import Writer


def get_queue():
    return Queue.Queue(maxsize=1024)


class TestWriter(unittest.TestCase):
    def test_periodic_scheduling_removing_from_queue(self):
        class MyWriter(Writer):
            def setup(self):
                self.interval = 1

            def write(self, msg):
                return msg

        q = get_queue()
        q.put(1)
        q.put(2)
        mywriter = MyWriter(q)
        mywriter.start()
        time.sleep(2)
        self.assertEqual(0, q.qsize())

    def test_processed_messages_counting(self):
        class MyWriter(Writer):
            def write(self, msg):
                return True

        q = get_queue()
        q.put(1)
        q.put(2)
        mywriter = MyWriter(q)
        mywriter.start()
        mywriter.process()
        mywriter.process()
        self.assertEqual(2, mywriter.processed)

    def test_checkpoint_saving(self):
        class MyWriter(Writer):
            def write(self, msg):
                return True

            def set_checkpoint(self, msg):
                self.checkpoint = msg
        
        q = get_queue()
        q.put(1)
        q.put(2)
        q.put(3)
        mywriter = MyWriter(q)
        mywriter.start()
        mywriter.process() #processes message 1
        self.assertEqual(1, mywriter.checkpoint)
        mywriter.process() #processes message 2
        self.assertEqual(2, mywriter.checkpoint)


if __name__ == "__main__":
    unittest.main()

