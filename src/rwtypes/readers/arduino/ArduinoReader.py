import serial

from __reader import Reader

class ArduinoReader(Reader):
    def setup(self):
        self.arduino = serial.Serial('/dev/ttyUSB0', 9600)

    def read(self):
        while True:
            try:
                msg = self.arduino.readline()
                self.store(msg)
            except Exception, e:
                print 'error reading'
                print e
