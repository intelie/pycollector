"""
 File: timeout.py
 Description: @timeout decorator to stop the execution of a function
 Author: Ronald Kaiser <raios dot catodicos at gmail dot com>
 Github: https://github.com/cathoderay/timeout
 Thanks to: Felipe Cruz
 Last changes: March, 2012

 Usage example:

     from timeout import timeout

     @timeout(5)
     def process():
        while True:
            print 'processing...'
            time.sleep(1)
"""


import time
import signal


class TimedOutException(Exception):
    pass


def timeout(value):
    def wrap(fn):
        def signal_handler(signum, frame):
            raise TimedOutException

        def wrapper(*args, **kwargs):
            try:
                if value != None:
                    signal.signal(signal.SIGALRM, signal_handler)
                    signal.alarm(value)
                return fn(*args, **kwargs)
            except TimedOutException:
                print "timed out!" #remove this line if convenient
                return None
        return wrapper
    return wrap
