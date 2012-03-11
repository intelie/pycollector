import sys
import xmpp
import logging
import traceback

from __message import Message
from __reader import Reader


class GtalkReader(Reader):
    def setup(self):
        self.log = logging.getLogger('pycollector')
        self.con = xmpp.Client('gmail.com')
        self.con.connect(server=('talk.google.com', 5222))

        self.required_confs = ['login', 'passwd']
        self.validate_conf()

        self.con.auth(self.login, self.passwd, "botty")
        self.con.sendInitPresence()

    def read(self):
        if self.period:
            self.log.error("This reader was intended to be async, without 'period'. Change your conf.yaml")
            return

        def process_message(sess, mess):
            text = mess.getBody()
            self.store(Message(content=text))

        self.con.RegisterHandler('message', process_message)
        while True:
            try:
                self.con.Process(1)
            except:
                self.log.error('error reading message')
                self.log.error(traceback.format_exc())
