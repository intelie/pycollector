import time
import json

from __writer import *
from lib import stomp_sender


class MyWriter(Writer):
    def setup(self):
        self.interval = 10

    def write(self, msg):
        #TODO: do exception handling
        headers = { 'destination': '/queue/events',
                    'timestamp': int(time.time())*1000}
        body = {'message' : msg}
        body = json.dumps(body)
        try:
            stomp_sender.send_message_via_stomp([('localhost', 61613)], headers, body)
            return True
        except Exception, e:
            return False
