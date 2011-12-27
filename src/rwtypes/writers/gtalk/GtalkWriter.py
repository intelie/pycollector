import xmpp

from __writer import *


class GtalkWriter(Writer):
    def setup(self):
        self.con = xmpp.Client('gmail.com')
        self.con.connect(server=('talk.google.com', 5222))
        if self.login and self.passwd:
            self.con.auth(self.login, self.passwd, "botty")
            self.con.sendInitPresence()
        else:
            print 'Provide a user/pass in conf file'

    def write(self, msg):
        if self.destination:
            m = xmpp.Message(self.destination, msg)
        else:
            print 'Provide a destination in conf file'
        m.setAttr('type', 'chat')
        self.con.send(m) 
        return True

