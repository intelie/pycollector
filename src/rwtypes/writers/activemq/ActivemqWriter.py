import time
import json
import socket
import logging
import traceback

import connection_adapter 
from third import stomp
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
        self.conn = connection_adapter.ConnectionAdapter([(self.host, self.port)])

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

            if self.conn.connection is None or not self.conn.is_connected():
                self.log.debug('Connecting to STOMP server %s on port %s.' % (self.host, self.port))
                self.conn.connect()
                      
            self.conn.send(body, headers,
                                 destination=headers['destination'])
            
            return True
        except stomp.exception.NotConnectedException, e:
            self.log.debug('Not connected to STOMP server %s on port %s.' % (self.host, self.port))
            self.conn.try_to_reconnect()
            return False
        except socket.error, e:
            self.log.warn('Socket error while trying to send message to STOMP server %s on port %s: %s.' % (self.host, self.port, e))
            return False
        except Exception, e:
            self.log.error('Error while trying to send message to STOMP server %s on port %s.\n%s' % (self.host, self.port, traceback.format_exc()))
            return False
