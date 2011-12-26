from __writer import Writer

class StdoutWriter(Writer):
    def write(self, msg):
        try:
            print "\n[WRITER] message: %s" % msg
            return True
        except Exception, e:
            print e
            return False
