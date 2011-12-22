import datetime
import time
import json


from third.stomp import stomp_sender
from __writer import Writer


class ActivemqWriter(Writer):
    def write(self, msg):
        try:
            headers = {'destination' : self.destination,
                       'eventtype' : self.eventtype
                       'timestamp' : int(time.time()*1000)}

            for item in msg:
                if isinstance(msg[item], datetime.datetime):
                    msg[item] = msg[item].isoformat()

            body = json.dumps(msg)

            stomp_sender.send_message_via_stomp([(self.host, self.port)], headers, body)
            return True
        except Exception, e:
            print e
            return False
