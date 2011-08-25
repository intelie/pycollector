
import time
import threading

import helpers.kronos as kronos 


class Reader(threading.Thread):
    def __init__(self, periodic=False, interval=0):
        self.scheduler = kronos.ThreadedScheduler()
        if periodic == True:
            self.scheduler.add_interval_task(self.read,
                                 "periodic task",
                                 0,
                                 interval,
                                 kronos.method.threaded,
                                 [],
                                 None)
        else:
            self.scheduler.add_single_task(self.read,
                                           "single task",
                                           0,
                                           kronos.method.threaded,
                                           [],
                                           None)
        threading.Thread.__init__(self)

    def read(self):
        """Subclasses should implement."""
        pass

    def run(self):
        self.scheduler.start()


if __name__ == "__main__":
    class MyReader(Reader):
        def read(self):
            print "know thyself"

    reader = MyReader(periodic=True, interval=1)
    reader.start()

    reader = MyReader(periodic=True, interval=4)
    reader.start()
    while True:
        pass
