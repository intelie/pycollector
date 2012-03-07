import time
import json
import socket
import datetime
import logging
import calendar


from third.stomp import stomp_sender
from __writer import Writer


class ActivemqWriter(Writer):
    """Conf:
        - host (required): activemq host
        - port (required): activemq port
        - destination (required): queue destination,
            e.g. /queue/events
        - eventtype (optional): header eventtype
        - additional_properties: dict with additional fields"""

    def setup(self):
        self.log = logging.getLogger('pycollector')
        self.check_conf(['host', 'port', 'destination', 'eventtype'])

    def check_conf(self, items):
        for item in items:
            if not hasattr(self, item):
                self.log.error('%s not defined in conf.yaml.' % item)
                exit(-1)

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
            self.log.error("Can't write")
            self.log.error(e)
            return False
