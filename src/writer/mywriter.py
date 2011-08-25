import time
import json

from writer import *
from lib import stomp_sender


class MyWriter(Writer): #activemqwriter
    def write(self, msg):
        #TODO: do exception handling
        headers = { 'destination': '/queue/events',
                    'timestamp': int(time.time())*1000}
        body = {'message' : msg}
        body = json.dumps(body)
 
        print headers
        print body
        try:
            stomp_sender.send_message_via_stomp([('localhost', 61613)], headers, body)
            print "sent!"
            return True
        except Exception, e:
            print e
            print "can't send"
            return False
