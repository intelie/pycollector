import os
import pickle
import unittest
import time
import Queue

import sys; sys.path.append('..')
from __reader import Reader
from __writer import Writer
from __message import Message


def get_queue(maxsize=1024):
    return Queue.Queue(maxsize=maxsize)


class TestReader(unittest.TestCase):
    def test_periodic_scheduling_adding_to_queue(self):
        class MyReader(Reader):
            def read(self):
                self.store(Message(content="life is beautiful"))
                return True

        q = get_queue()

        conf = {'interval' : 1}
        myreader = MyReader(q, conf=conf)
        myreader.start()
 
        #waits to process messages
        time.sleep(3.5)

        self.assertEqual(4, q.qsize())

    def test_single_scheduling_adding_to_queue(self):
        class MyReader(Reader):
            def read(self):
                n = 0
                phrase = "love is all you need"
                while n < 3:
                    m = Message(content=phrase)
                    self.store(m)
                    n += 1
                return True

        q = get_queue()
        
        myreader = MyReader(q)
        myreader.start()

        #waits to process messages
        time.sleep(1)

        self.assertEqual(3, q.qsize())
        self.assertEqual(3, myreader.processed)

    def test_checkpoint_saving(self):
        checkpoint_path = '/tmp/rcheckpoint'
        class MyReader(Reader):
            def read(self):
                m1 = Message(content='foo', checkpoint='foo')
                self.store(m1)
                m2 = Message(content='bar', checkpoint='bar')
                self.store(m2)
                return True

        q = get_queue()

        conf = {'checkpoint_enabled' : True,
                'checkpoint_path' : checkpoint_path}
        myreader = MyReader(q, conf=conf)
        myreader.start()

        #waits to process messages
        time.sleep(1)

        self.assertEqual(2, myreader.processed)
        self.assertEqual('bar', myreader.last_checkpoint)

        f = open(checkpoint_path, 'rb')
        self.assertEqual('bar', pickle.load(f))
        f.close()

        os.remove(checkpoint_path)

    def test_restore_checkpoint_from_writer_when_starting(self):
        writer_checkpoint_path = '/tmp/wcheckpoint'
        f = open(writer_checkpoint_path, 'w')
        pickle.dump('42', f)
        f.close()
                
        q = get_queue()

        conf = {'checkpoint_enabled' : True,
                'checkpoint_path' : writer_checkpoint_path}
        mywriter = Writer(q, conf=conf)

        conf = {'checkpoint_enabled' : True,
                'checkpoint_path' : '/tmp/test'}
        myreader = Reader(q, conf=conf, writer=mywriter)

        self.assertEqual('42', myreader.last_checkpoint)

        os.remove(writer_checkpoint_path)

    def test_store_number_of_discarded_messages_due_to_full_queue(self):
        class MyReader(Reader):
            def read(self):
                while(True):
                    self.store(Message(content='know thyself'))
                return True

        q = get_queue(5)
        
        myreader = MyReader(q)
        myreader.start()

        #waits to get a full queue
        time.sleep(0.01)

        self.assertTrue(myreader.discarded > 0)

    def test_blockable_reader(self):
        result = []
        expected = range(1, 5)

        class MyReader(Reader):
            def setup(self):
                self.current = 1

            def read(self):
                if self.current > 4:
                    return False
                self.store(Message(content=self.current))
                self.current += 1
                return True

        q = get_queue(3)
        myreader = MyReader(q, conf={'interval' : 1,
                                     'blockable' : True})
        myreader.start()

        #waits to get a full queue
        time.sleep(3.5)

        #remove an item from queue
        result.append(q.get().content)

        #waits for another read
        time.sleep(1)

        #remove rest of items from the queue
        while q.qsize() > 0: result.append(q.get().content)

        self.assertEqual(expected, result)


if __name__ == "__main__":
    unittest.main()

