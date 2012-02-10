import os
import time
import logging
import socket

from __reader import Reader
from __message import Message

class AdhocReader(Reader):
    """Start writing your reader here"""
    def setup(self):
        self.log = logging.getLogger()

    def read(self):
        try:
            date = time.strftime("%b %d %H:%M:%S")
            msg = "<14>%s %s pycollector: %s" % (date, socket.gethostname(), os.popen('acpi').read())
            self.store(Message(content=msg))
            return True
        except Exception, e:
            self.log.error('error reading')
            self.log.error(e)
            return False
