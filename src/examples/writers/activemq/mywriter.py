import sys; sys.path.append("../")

from writer import *


class MyWriter(Writer):
    def write(self, msg):
        print "\n[WRITER] message: %s" % msg

