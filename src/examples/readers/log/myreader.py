import filetail

from __reader import *


class MyReader(Reader):
    def setup(self):
        self.tail = filetail.Tail("/tmp/access.log", max_sleep=1)

    def read(self):
        while True:
            line = self.tail.nextline()
            self.store(line)
