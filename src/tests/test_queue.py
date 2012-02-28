import random
import time
import unittest

import sys; sys.path.append('..')
from __queue import CustomQueue


class TestQueue(unittest.TestCase):
    def test_callback_should_not_interfere_on_processing_messages_order(self):
        processed = []

        def callback():
            item = cq.get()
            #simulates random processing time
            time.sleep(int(random.random()*5))
            processed.append(item)

        cq = CustomQueue(2<<3, callback)

        #adds messages to queue
        for i in range(2<<2):
            cq.put(i)

        #waits processing
        while len(processed) < 2<<2:
            time.sleep(1)

        # guarantees order of processed msgs
        # probability of wrong answer = (1/6.0)**8 ~ 6 x 10^-6
        self.assertEqual(range(0, 8), processed)


    def test_callback_should_not_interfere_on_new_puts(self):
        def callback():
            cq.get()
            cq.task_done()
            #delays processing
            time.sleep(1000)

        cq = CustomQueue(2<<3, callback)

        #adds messages to queue
        for i in range(2<<3):
            cq.put(i)

        #waits to first callback
        time.sleep(1)

        # assert all other messages were put in
        # the queue while the first is being processed
        self.assertEqual(15, cq.qsize())

def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestQueue))
    return suite


if __name__ == "__main__":
    unittest.main()

