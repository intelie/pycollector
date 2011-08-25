from writer import Writer


class MyWriter(Writer):
    def write(self, msg):
        print "\n[WRITER] message: %s" % msg
