import time

from __writer import Writer


class CameraWriter(Writer):
    def setup(self):
        if not hasattr(self, 'path'):
            self.path = '/tmp'
        elif self.path[-1] == '/':
            self.path = self.path[:-1]

    def write(self, msg):
        try:
            msg.save('%s/%s-%s.jpg' % (self.path, self.prefix, time.time()))
            return True
        except Exception, e:
            print 'error writing img'
            print e
            return False
