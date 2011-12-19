import time
import json


from lib import stomp_sender
from __writer import *


class ActivemqWriter(Writer):
    def write(self, msg):        
        headers = {'destination' : self.destination,
                   'timestamp' : int(time.time()*1000)}
        body = json.dumps(msg)
        try:
            stomp_sender.send_message_via_stomp([(self.host, self.port)], headers, body)
            return True
        except Exception, e:
            print e
            return False