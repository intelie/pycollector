import time
import serial
import logging

from __reader import Reader
from __message import Message


class ArduinoReader(Reader):
    """Conf:
        - interface: serial where strings should be read, 
            e.g. /dev/ttyUSB0 (default)
        - bps: write rate of your interface, 
            e.g. 9600 (default)"""

    def setup(self):
        self.log = logging.getLogger()
        if not hasattr(self, 'interface'):
            self.interface = '/dev/ttyUSB0'
        if not hasattr(self, 'bps'):
            self.bps = 9600
        self.arduino = serial.Serial(self.interface, self.bps)

    def read(self):
        while True:
            try:
                msg = "%s,%s" % (time.time(), self.arduino.readline())
                self.store(Message(content=msg))
            except Exception, e:
                self.log.error('error reading') 
                self.log.error(e)
