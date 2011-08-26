from __reader import *


class MyReader(Reader):
    def read(self):
        return raw_input('[READER] message: ')

