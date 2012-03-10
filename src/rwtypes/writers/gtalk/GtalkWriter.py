import sys
import xmpp
import logging
import traceback

from __writer import *


class GtalkWriter(Writer):
    def setup(self):
        self.log = logging.getLogger('pycollector')
        self.con = xmpp.Client('gmail.com')
        self.con.connect(server=('talk.google.com', 5222))

        self.required_confs = ['login', 'passwd', 'destination']
        self.validate_conf()

        self.con.auth(self.login, self.passwd, "botty")
        self.con.sendInitPresence()

    def write(self, msg):
        try:
            m = xmpp.Message(self.destination, msg)
            m.setAttr('type', 'chat')
            self.con.send(m) 
            return True
        except:
            self.log.error('error sending message')
            self.log.error(traceback.format_exc())
            return False

