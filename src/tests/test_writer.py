import os
import time
import Queue
import pickle
import unittest
from mock import MagicMock

import sys; sys.path.append('..')
from __writer import Writer
from __message import Message
from __exceptions import ConfigurationError


def get_queue(maxsize=1024):
    return Queue.Queue(maxsize=maxsize)


class TestWriter(unittest.TestCase):
    def test_initialization_calls(self):
        # mocking
        q = get_queue()
        mywriter = Writer(q)
        mywriter.set_conf = MagicMock()
        mywriter.setup = MagicMock()
        mywriter.schedule_tasks = MagicMock()

        conf = {'foo' : 'bar'}
        mywriter.__init__(q, conf=conf)

        # asserting that methods are called
        mywriter.set_conf.assert_called_with(conf)
        mywriter.setup.assert_called_with()
        mywriter.schedule_tasks.assert_called_with()

    def test_scheduling_checkpoint_when_it_is_enabled(self):
        # mocking
        q = get_queue()
        mywriter = Writer(q)
        mywriter.schedule_checkpoint_writing = MagicMock()

        conf = {'checkpoint_enabled' : True,
                'checkpoint_path' : '/tmp/checkpoint'}
        mywriter.__init__(q, conf=conf)

        mywriter.schedule_checkpoint_writing.assert_called_with()

    def test_confs_are_loaded_before_setup_is_called(self):
        class MyWriter(Writer):
            def setup(self):
                if self.foo != 42:
                    raise Exception()

        q = get_queue()

        # raises an exception due to a lack of
        # conf providing a foo
        self.assertRaises(AttributeError, MyWriter, (q))

        # now, no exception should be raised, =)
        conf = {'foo': 42}
        MyWriter(q, conf=conf)

    def test_periodic_scheduling_removing_from_queue(self):
        class MyWriter(Writer):
            def write(self, msg):
                return msg

        q = get_queue()
        q.put(Message(content=1))
        q.put(Message(content=2))

        conf = {'period' : 1}
        mywriter = MyWriter(q, conf=conf)
        mywriter.start()

        #waits period to write messages
        time.sleep(2)

        self.assertEqual(0, q.qsize())

    def test_periodic_scheduling_calling_write_method(self):
        q = get_queue()
        q.put(Message(content=1))
        conf = {'period' : 1}

        mywriter = Writer(q, conf=conf)
        mywriter.write = MagicMock(return_value=True)
        mywriter.start()

        # waits writer period
        time.sleep(1)

        mywriter.write.assert_called_with(1)

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
            def write(self, msg):
                return True

        q = get_queue()

        # populates the queue with some messages
        q.put(Message(content='foo', checkpoint='foo'))
        q.put(Message(content='bar', checkpoint='bar'))

        conf = {'checkpoint_enabled' : True,
                'checkpoint_path' : checkpoint_path,
                'checkpoint_period' : 1}

        mywriter = MyWriter(q, conf=conf)

        # after a start, the messages should be consumed
        mywriter.start()

        # waits for checkpoint_period
        time.sleep(2)

        self.assertEqual(2, mywriter.processed)
        self.assertEqual('bar', mywriter.last_checkpoint)

        f = open(checkpoint_path, 'r+')
        self.assertEqual('bar', pickle.load(f))
        f.close()

        os.remove(checkpoint_path)

    def test_restore_last_checkpoint(self):
        checkpoint_path = '/tmp/wcheckpoint'
        f = open(checkpoint_path, 'w')
        pickle.dump('foo', f)
        f.close()

        class MyWriter(Writer):
            def write(self, msg):
                return True

        q = get_queue()

        conf = {'checkpoint_enabled' : True,
                'checkpoint_path' : checkpoint_path}
        mywriter = MyWriter(q, conf=conf)
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

        mywriter = MyWriter(q, conf={'blockable': False})
        mywriter.start()

        #force processing messages
        mywriter.process()
        mywriter.process()
        mywriter.process()

        self.assertEqual(2, mywriter.discarded)

    def test_when_required_confs_are_missing_get_exception(self):
        class MyWriter(Writer):
            def setup(self):
                self.required_confs = ['jack', 'white']
                self.validate_conf()

        q = get_queue()
        self.assertRaises(ConfigurationError, MyWriter, q, {'jack': 'bauer'})

    def test_creating_file_if_checkpoint_does_not_exist(self):
        checkpoint_path = '/tmp/checkpoint'
        if os.path.exists(checkpoint_path):
            os.remove(checkpoint_path)

        q = get_queue()
        conf = {'checkpoint_enabled' : True,
                'checkpoint_path' : checkpoint_path}

        mywriter = Writer(q, conf=conf)

        # after __init__, checkpoint file should be created
        self.assertTrue(os.path.exists(checkpoint_path))

    def test_default_values_in_initialization(self):
        q = get_queue()

        mywriter  = Writer(q)
        self.assertEqual(None, mywriter.period)
        self.assertEqual(True, mywriter.blockable)
        self.assertEqual(False, mywriter.checkpoint_enabled)
        self.assertEqual(None, mywriter.retry_timeout)
        self.assertEqual(1, mywriter.retry_period)
        self.assertEqual(300, mywriter.health_check_period)
        self.assertEqual(0, mywriter.processed)
        self.assertEqual(0, mywriter.discarded)

        # if checkpoint
        checkpoint_path = '/tmp/checkpoint'
        if os.path.exists(checkpoint_path):
            os.remove(checkpoint_path)
        conf = {'checkpoint_enabled' : True,
                'checkpoint_path' : '/tmp/checkpoint'}
        mywriter = Writer(q, conf=conf)
        self.assertEqual(60, mywriter.checkpoint_period)
        self.assertEqual('', mywriter.last_checkpoint)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestWriter))
    return suite


if __name__ == "__main__":
    unittest.main()

