import os
import time
import unittest
import Queue

import sys; sys.path.append('..')
from __writer import Writer
from __message import Message


def get_queue(maxsize=1024):
    return Queue.Queue(maxsize=maxsize)


class TestWriter(unittest.TestCase):
    def test_periodic_scheduling_removing_from_queue(self):
        class MyWriter(Writer):
            def setup(self):
                self.interval = 1

            def write(self, msg):
                return msg

        q = get_queue()
        q.put(Message(content=1))
        q.put(Message(content=2))

        mywriter = MyWriter(q)
        mywriter.start()

        #waits interval to write messages
        time.sleep(2)

        self.assertEqual(0, q.qsize())

    def test_processed_messages_counting(self):
        class MyWriter(Writer):
            def write(self, msg):
                return True

        q = get_queue()
        q.put(Message(content=1))
        q.put(Message(content=2))

        mywriter = MyWriter(q)
        mywriter.start()

        #force processing messages
        mywriter.process()
        mywriter.process()

        self.assertEqual(2, mywriter.processed)

    def test_checkpoint_saving(self):
        checkpoint_path = '/tmp/wcheckpoint'
        class MyWriter(Writer):
            def setup(self):
                self.checkpoint_enabled = True
                self.checkpoint_path = checkpoint_path 
                self.checkpoint_interval = 1

            def write(self, msg):
                return True

        q = get_queue()
        q.put(Message(content='foo', checkpoint='foo'))
        q.put(Message(content='bar', checkpoint='bar'))

        mywriter = MyWriter(q)
        mywriter.start()

        #should process message 1
        mywriter.process()

        #waits for checkpoint_interval
        time.sleep(2)

        self.assertEqual('foo', mywriter.last_checkpoint)

        #should process message 2
        mywriter.process()

        #waits for checkpoint_interval
        time.sleep(2)

        self.assertEqual(2, mywriter.processed)
        self.assertEqual('bar', mywriter.last_checkpoint)

        f = open(checkpoint_path, 'r+')
        self.assertEqual('bar', f.read().strip())
        f.close()

        os.remove(checkpoint_path)

    def test_restore_last_checkpoint(self):
        checkpoint_path = '/tmp/wcheckpoint'
        f = open(checkpoint_path, 'w+')
        f.write('foo')
        f.close()

        class MyWriter(Writer):
            def setup(self):
                self.checkpoint_enabled = True
                self.checkpoint_path = checkpoint_path

            def write(self, msg):
                return True

        q = get_queue()
        mywriter = MyWriter(q)
        self.assertEqual('foo', mywriter.last_checkpoint)

        os.remove(checkpoint_path)

    def test_store_discarded_messages_due_to_some_fail(self):
        class MyWriter(Writer):
            def write(self, msg):
                if msg == 'foo2' or msg == 'foo3':
                    return False
                return True

        q = get_queue(3)
        q.put(Message(content='foo1'))
        q.put(Message(content='foo2'))
        q.put(Message(content='foo3'))

        mywriter = MyWriter(q)
        mywriter.start()

        #force processing messages
        mywriter.process()
        mywriter.process()
        mywriter.process()

        self.assertEqual(2, mywriter.discarded)


if __name__ == "__main__":
    unittest.main()

