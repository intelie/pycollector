import xmpp

from __writer import *


class MyWriter(Writer):
    def setup(self):
        self.interval = 5
        
        login = "cathoderaybot"
        passwd = "c4th0d3r4y"
        self.con = xmpp.Client('gmail.com')
        self.con.connect(server=('talk.google.com', 5222))
        self.con.auth(login, passwd, "botty")
        self.con.sendInitPresence()

    def write(self, msg):
        destiny = "raios.catodicos@gmail.com"
        m = xmpp.Message(destiny, msg)
        m.setAttr('type', 'chat')
        self.con.send(m) 
        return True

