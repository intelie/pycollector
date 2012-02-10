import sys
import xmpp
import logging

from __writer import *


class GtalkWriter(Writer):
    """Conf:
        - login (required): gmail username
        - passwd (required): gmail password
        - destination (required): xmpp destination, 
            e.g. myfriend@gmail.com"""

    def setup(self):
        self.log = logging.getLogger()
        self.con = xmpp.Client('gmail.com')
        self.con.connect(server=('talk.google.com', 5222))
        if hasattr(self, 'login') and hasattr(self, 'passwd'):
            self.con.auth(self.login, self.passwd, "botty")
            self.con.sendInitPresence()
        else:
            self.log.error('provide a user/pass in conf.yaml')
            sys.exit(-1)

        if not hasattr(self, 'destination'):
            self.log.error('provide a destination in conf file')
            sys.exit(-1)


    def write(self, msg):
        try:
            m = xmpp.Message(self.destination, msg)
            m.setAttr('type', 'chat')
            self.con.send(m) 
            return True
        except:
            self.log.error('error sending message')
            self.log.error(e)
            return False

