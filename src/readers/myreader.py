from reader import Reader


class MyReader(Reader):
    def read(self):
        return raw_input('[READER] message: ')
