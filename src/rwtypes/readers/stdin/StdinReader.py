from __reader import Reader


class StdinReader(Reader):
    def read(self):
        while True:
            try:
                self.store(raw_input('\n[READER] message: '))
            except Exception, e:
                print e
