import SimpleCV

from __reader import Reader


class CameraReader(Reader):
    """Conf: 
        - interval (required): period of reads
        - cam_number (optional): id number of your cam"""

    def setup(self):
        if hasattr(self, 'cam_number'):
            self.cam = SimpleCV.Camera(self.cam_number)
        else:
            self.cam = SimpleCV.Camera()

    def read(self):
        try:
            image = self.cam.getImage()
            self.store(image)
            return True
        except Exception, e:
            print 'Error in storing image'
            print e
            return False
