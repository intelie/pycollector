import SimpleCV

from __reader import Reader


class CameraReader(Reader):
    def setup(self):
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
