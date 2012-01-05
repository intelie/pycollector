import time
import os
import socket

from __reader import Reader
from __message import Message

class AdhocReader(Reader):
    """Start writing your reader here"""

    def read(self):
        try:
            date = time.strftime("%b %d %H:%M:%S")
            msg = "<14>%s %s pycollector: %s" % (date, socket.gethostname(), os.popen('acpi').read())
            self.store(Message(content=msg))
            return True
        except Exception, e:
            print 'error reading'
            print e
            return False
