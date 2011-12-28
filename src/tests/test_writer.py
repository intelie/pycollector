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
            def setup(self):
                self.save_checkpoint = True

            def write(self, msg):
                return True

        q = get_queue()
        q.put(1)
        q.put(2)
        mywriter = MyWriter(q)
        mywriter.start()

        #should process message 1
        mywriter.process()

        #default checkpoint should be the msg
        self.assertEqual(1, mywriter.last_checkpoint)

        #should process message 2
        mywriter.process()
        self.assertEqual(2, mywriter.last_checkpoint)


if __name__ == "__main__":
    unittest.main()

