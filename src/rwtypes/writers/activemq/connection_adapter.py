# coding: utf-8

import logging
import time
import sys
import socket

from third import stomp


logger = logging.getLogger("pycollector")


class ConnectionAdapter(object):
    """
      Adapter to send messages to ActiveMQ through STOMP.
    """
    def __init__(self, broker):
        self.broker = broker
        self.connection = None
        self.conn_sleep_delay = 3 # Seconds to wait after losing a connection

    def send(self, message, headers={}, **keyword_headers):
        self.connection.send(message, headers, **keyword_headers)
            
    def try_to_reconnect(self):
        logger.debug('Trying to reconnect to STOMP server...')
        time.sleep(self.conn_sleep_delay)
        try:
            self.connect()
        except stomp.exception.NotConnectedException:
            self.try_to_reconnect()
        except socket.error, e:
            logger.warn('Socket error while trying to reconnect to STOMP server: %s.' % e)

    def connect(self):
        """Attempts to connect to broker(s).
        stomp will automatically try to connect to other brokers
        if some of them are offline.
        """
        self.connection = stomp.Connection(self.broker, prefer_localhost=False,
                                      try_loopback_connect=False)
        self.connection.set_listener('', ErrorListener(self.connection))
        self.connection.start()
        self.connection.connect()
        
    def is_connected(self):
        if self.connection is None or not self.connection.is_connected:
            return False
        return True


class ErrorListener(stomp.ConnectionListener):
    def __init__(self, connection):
        self.connection = connection

    def on_error(self, headers, message):
        logger.error('received an error %s' % message)
        if self.connection.is_connected:
            # This necessary because of an activemq bug - https://issues.apache.org/activemq/browse/AMQ-1376
            logger.error('TCP is connect but has some errors')
            self.connection.stop()


if __name__ == '__main__':
    print 'Testing...'
    brokers = [('127.0.0.1', 61613)]
    conn = ConnectionAdapter(brokers, 0)
    while True:
        try:
            string_to_send = 'testing ' + str(time.time())
            to_send = {'message': string_to_send,
                       'headers': {'destination': '/queue/events'}}
            print 'sent: %s' % string_to_send
            conn.send(**to_send)
            time.sleep(1)
        except KeyboardInterrupt:
            break
    sys.exit()
