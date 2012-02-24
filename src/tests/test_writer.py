import os
import time
import pickle
import unittest
import Queue

import sys; sys.path.append('..')
from __writer import Writer
from __message import Message
from __exceptions import ConfigurationError


def get_queue(maxsize=1024):
    return Queue.Queue(maxsize=maxsize)


class TestWriter(unittest.TestCase):
    def test_periodic_scheduling_removing_from_queue(self):
        class MyWriter(Writer):
            def write(self, msg):
                return msg

        q = get_queue()
        q.put(Message(content=1))
        q.put(Message(content=2))

        conf = {'interval' : 1}
        mywriter = MyWriter(q, conf=conf)
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
            def write(self, msg):
                return True

        q = get_queue()
        q.put(Message(content='foo', checkpoint='foo'))
        q.put(Message(content='bar', checkpoint='bar'))

        conf = {'checkpoint_enabled' : True,
                'checkpoint_path' : checkpoint_path,
                'checkpoint_interval' : 1}
        mywriter = MyWriter(q, conf=conf)
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
        self.assertEqual('bar', pickle.load(f))
        f.close()

        os.remove(checkpoint_path)

    def test_restore_last_checkpoint(self):
        checkpoint_path = '/tmp/wcheckpoint'
        f = open(checkpoint_path, 'w+')
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

        q = get_queue()
        self.assertRaises(ConfigurationError, MyWriter, q, {'jack': 'bauer'})


if __name__ == "__main__":
    unittest.main()

