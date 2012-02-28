import os
import pickle
import unittest
import time
import Queue

import sys; sys.path.append('..')
from __reader import Reader
from __message import Message
from __exceptions import ConfigurationError

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

    def test_store_number_of_discarded_messages_due_to_full_queue(self):
        class MyReader(Reader):
            def read(self):
                while(True):
                    self.store(Message(content='know thyself'))
                return True

        q = get_queue(5)

        conf = {'blockable' : False} 
        myreader = MyReader(q, conf=conf)
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

    def test_when_required_confs_are_missing_get_exception(self):
        class MyReader(Reader):
            def setup(self):
                self.required_confs = ['a', 'b']

        q = get_queue(3)
        self.assertRaises(ConfigurationError,  MyReader,  q, {'a' : 'foo'})

def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestReader))
    return suite
             

if __name__ == "__main__":
    unittest.main()

