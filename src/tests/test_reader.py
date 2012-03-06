import os
import time
import Queue
import pickle
import unittest
from mock import MagicMock

import sys; sys.path.append('..')
from __reader import Reader
from __message import Message
from __exceptions import ConfigurationError


def get_queue(maxsize=1024):
    return Queue.Queue(maxsize=maxsize)


class TestReader(unittest.TestCase):
    def test_initialization_calls(self):
        # mocking
        q = get_queue()
        myreader = Reader(q)
        myreader.set_conf = MagicMock()
        myreader.setup = MagicMock()
        myreader.schedule_tasks = MagicMock()

        conf = {'foo' : 'bar'}
        myreader.__init__(q, conf=conf)

        # after __init__, methods should be called
        myreader.set_conf.assert_called_with(conf)
        myreader.setup.assert_called_with()
        myreader.schedule_tasks.assert_called_with()

    def test_scheduling_checkpoint_when_it_is_enabled(self):
        # mocking
        q = get_queue()
        myreader = Reader(q)
        myreader.schedule_checkpoint_writing = MagicMock()

        conf = {'checkpoint_enabled' : True,
                'checkpoint_path' : '/tmp/checkpoint'}
        myreader.__init__(q, conf=conf)

        # after __init__, methods, should be called
        myreader.schedule_checkpoint_writing.assert_called_with()

    def test_confs_are_loaded_before_setup_is_called(self):
        class MyReader(Reader):
            def setup(self):
                if self.foo != 42:
                    raise Exception()

        q = get_queue()

        # raises an exception due to a lack of
        # conf providing a foo
        self.assertRaises(AttributeError, MyReader, (q))

        # now, no exception should be raised, =)
        conf = {'foo': 42}
        MyReader(q, conf=conf)

    def test_periodic_scheduling_adding_to_queue(self):
        class MyReader(Reader):
            def read(self):
                self.store(Message(content="life is beautiful"))
                return True

        q = get_queue()

        conf = {'period' : 1}
        myreader = MyReader(q, conf=conf)
        myreader.start()

        # waits to process messages
        time.sleep(3.5)

        self.assertEqual(4, q.qsize())

    def test_periodic_scheduling_calling_read_method(self):
        q = get_queue()
        conf = {'period' : 1}

        myreader = Reader(q, conf=conf)
        myreader.read = MagicMock(return_value=True)
        myreader.start()

        # waits reader period
        time.sleep(1)

        myreader.read.assert_called_with()

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

        # waits to process messages
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

        # waits to process messages
        time.sleep(1)

        self.assertEqual(2, myreader.processed)
        self.assertEqual('bar', myreader.last_checkpoint)

        # checkpoint has the right content?
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

        # waits to get a full queue
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
        myreader = MyReader(q, conf={'period' : 1,
                                     'blockable' : True})
        myreader.start()

        # waits to get a full queue
        time.sleep(3.5)

        # remove an item from queue
        result.append(q.get().content)

        # waits for another read
        time.sleep(1)

        # remove rest of items from the queue
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

