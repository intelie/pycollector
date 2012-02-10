import logging

import SimpleCV

from __reader import Reader
from __message import Message


class CameraReader(Reader):
    """Conf: 
        - interval (required): period of reads
        - cam_number (optional): id number of your cam"""

    def setup(self):
        self.log = logging.getLogger()
        if hasattr(self, 'cam_number'):
            self.cam = SimpleCV.Camera(self.cam_number)
        else:
            self.cam = SimpleCV.Camera()

    def read(self):
        try:
            image = self.cam.getImage()
            self.store(Message(content=image))
            return True
        except Exception, e:
            self.log.error('error in storing image')
            self.log.error(e)
            return False
