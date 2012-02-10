import logging

from __writer import Writer


class StdoutWriter(Writer):
    """Conf: none"""
    def setup(self):
        self.log = logging.getLogger()

    def write(self, msg):
        try:
            print "\n[WRITER] message: %s" % msg
            return True
        except Exception, e:
            self.log.error("Can't write in stdout")
            self.log.error(e) 
            return False
