import sys
import logging

from __reader import Reader
from __message import Message

class StdinReader(Reader):
    """Conf: none"""

    def read(self):
        while True:
            try:
                print '\n[READER] message: '
                self.store(Message(content=sys.stdin.readline()))
            except Exception, e:
                self.log.error('error reading')
                self.log.error(e)
