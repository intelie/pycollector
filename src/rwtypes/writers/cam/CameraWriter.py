import time

from __writer import Writer


class CameraWriter(Writer):
    def write(self, msg):
        try:
            msg.save('/tmp/%s.jpg' % time.time())
            return True
        except Exception, e:
            print 'error writing img'
            print e
            return False
