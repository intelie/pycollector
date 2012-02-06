import socket
import datetime
import time
import calendar
import json


from third.stomp import stomp_sender
from __writer import Writer


class ActivemqWriter(Writer):
    """Conf:
        - host (required): activemq host
        - port (required): activemq port
        - destination (required): queue destination,
            e.g. /queue/events
        - eventtype (required): header eventtype
        - additional_properties: dict with additional fields"""

    def write(self, msg):
        try:
            headers = {'destination' : self.destination,
                       'timestamp' : int(time.time()*1000)}

            if isinstance(msg, str):
                msg = {'msg' : msg}

            if hasattr(self, 'eventtype'):
                headers.update({'eventtype' : self.eventtype})

            if hasattr(self, 'additional_properties'):
                for prop in self.additional_properties:
                    msg.update({prop : self.additional_properties[prop]})

            if not hasattr(self, 'remove_host') or \
                hasattr(self, 'remove_host') and not self.remove_host:
                msg['host'] = socket.gethostname()

            body = json.dumps(msg)

            stomp_sender.send_message_via_stomp([(self.host, self.port)], headers, body)
            return True
        except Exception, e:
            print e
            return False
