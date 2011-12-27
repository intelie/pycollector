import sys

from __reader import Reader


class StdinReader(Reader):
    def read(self):
        while True:
            try:
                print '\n[READER] message: '
                self.store(sys.stdin.readline())
            except Exception, e:
                print e
