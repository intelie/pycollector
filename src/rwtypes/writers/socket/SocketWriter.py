import socket

from __writer import Writer

class SocketWriter(Writer):
    """Conf: 
        - host (required): hostname destination
        - port (required): port destination"""
        
    def setup(self):
        self.sock = socket.socket()
        if hasattr(self, 'host') and hasattr(self, 'port'):
            self.sock.connect((self.host, int(self.port)))
        else:
            print "provide host, port in conf file"

    def write(self, msg):
        try:
            self.sock.send(msg)
            return True
        except Exception, e:
            print e
            return False
