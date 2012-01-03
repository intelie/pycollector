import time

from __writer import Writer


class CameraWriter(Writer):
    """Conf:
        - path (optional): where the images will be saved,
            e.g. /home/user/images (default is /tmp)
        - prefix (optional): prefix added to each image filename. 
    """
    def setup(self):
        if not hasattr(self, 'path'):
            self.path = '/tmp'
        elif self.path[-1] == '/':
            self.path = self.path[:-1]

    def write(self, msg):
        try:
            if hasattr(self, 'prefix'):
                msg.save('%s/%s-%s.jpg' % (self.path, self.prefix, time.time()))
            else:
                msg.save('%s/%s.jpg' % (self.path, time.time()))
            return True
        except Exception, e:
            print 'error writing img'
            print e
            return False
