from __reader import *


class MyReader(Reader):
    def setup(self):
        self.periodic = True
        self.interval = 10

    def read(self):
        return raw_input('[READER] message: ')

