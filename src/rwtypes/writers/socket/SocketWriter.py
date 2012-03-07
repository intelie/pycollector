import socket
import logging

from __writer import Writer

class SocketWriter(Writer):
    """Conf: 
        - host (required): hostname destination
        - port (required): port destination"""
        
    def setup(self):
        self.log = logging.getLogger('pycollector')
        self.sock = socket.socket()
        if hasattr(self, 'host') and hasattr(self, 'port'):
            self.sock.connect((self.host, int(self.port)))
        else:
            self.log.error('provide a host, port in conf.yaml')

    def write(self, msg):
        try:
            self.sock.send(msg)
            return True
        except Exception, e:
            self.log.error('error writing in socket')
            self.log.error(e)
            return False
