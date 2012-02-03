import os
import unittest
import time
import Queue

import sys; sys.path.append('..')
from __reader import Reader
from __writer import Writer
from __message import Message


def get_queue(maxsize=1024):
    q = Queue.Queue()
    q.maxsize = maxsize
    return q


class TestReader(unittest.TestCase):
    def test_periodic_scheduling_adding_to_queue(self):
        class MyReader(Reader):
            def setup(self):
                self.interval = 1

            def read(self):
                self.store(Message(content="life is beautiful"))
                return True

        q = get_queue()
        myreader = MyReader(q)
        myreader.start()
        time.sleep(3)
        size = q.qsize()
        self.assertTrue(size >= 3)

    def test_single_scheduling_adding_to_queue(self):
        class MyReader(Reader):
            def read(self):
                n = 0
                while n < 3:
                    self.store(Message(content="love is all you need"))
                    n += 1
                return True

        q = get_queue()
        myreader = MyReader(q)
        myreader.start()
        time.sleep(1)
        self.assertEqual(3, q.qsize())
        self.assertEqual(3, myreader.processed)

    def test_checkpoint_saving(self):
        checkpoint_path = '/tmp/rcheckpoint'
        class MyReader(Reader):
            def setup(self):
                self.checkpoint_path = checkpoint_path

            def read(self):
                self.store(Message(content='foo', checkpoint='foo'))
                self.store(Message(content='bar', checkpoint='bar'))
                return True

        q = get_queue()
        myreader = MyReader(q)
        myreader.start()
        time.sleep(1)

        self.assertEqual(2, myreader.processed)
        self.assertEqual('bar', myreader.last_checkpoint)

        f = open(checkpoint_path, 'r+')
        self.assertEqual('bar', f.read().strip())
        f.close()

        os.remove(checkpoint_path)

    def test_restore_checkpoint_from_writer_when_starting(self):

        writer_checkpoint_path = '/tmp/wcheckpoint'
        f = open(writer_checkpoint_path, 'w+')
        f.write('42')
        f.close()
                
        class MyWriter(Writer):
            def setup(self):
                self.checkpoint_path = writer_checkpoint_path

        q = get_queue()
        myreader = Reader(q, writer=MyWriter(q))
        self.assertEqual('42', myreader.last_checkpoint)

        os.remove(writer_checkpoint_path)

    def test_store_discarded_messages_due_to_full_queue(self):
        class MyReader(Reader):
            def read(self):
                while(True):
                    self.store(Message(content='know thyself'))
                return True

        q = get_queue(5)
        myreader = MyReader(q)
        myreader.start()

        #waits to get a full queue
        time.sleep(0.001)

        self.assertTrue(myreader.discarded > 0)


if __name__ == "__main__":
    unittest.main()

