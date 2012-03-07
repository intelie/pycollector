# -*- coding:utf-8 -*-

"""
    File: web.py
    Description: going to be a simple web server with realtime data.
"""

import os
import threading

if __name__ == "__main__":
    try:
        import sys
        import __meta__
        sys.path = __meta__.PATHS.values() + sys.path
    except ImportError, e:
        print e

import cherrypy


class Home:
    def __init__(self, collector=None):
        self.collector = collector

    def get_html_for_pairs(self, pairs):
        html = ''
        for pair in pairs:
            writer, reader = pair
            for i, item in enumerate([reader, writer]):
                item_type = "reader"
                if i == 1:
                    item_type = "writer"
                html += """
                  <table>
                  <tr><td>%s</td><td>%s</td></tr>
                  <tr><td>processed</td><td>%s</td></tr>
                  <tr><td>discarded</td><td>%s</td></tr>
                  <tr><td>conf</td><td>%s</td></tr>
                  </table>
                """ % (item_type,
                       item.__class__.__name__,
                       item.processed,
                       item.discarded,
                       item.conf)
            html += """
                  <table>
                  <tr><td>queue maxsize</td><td>%s</td></tr>
                  <tr><td>messages in queue</td><td>%s</td></tr>
                  </table>
            """ % (item.queue.maxsize,
                   item.queue.qsize())
            html += "</br>"
        return html

    def index(self):
        if not self.collector or \
            not hasattr(self.collector, 'pairs'):
            return "Unavailable"
        data = {}
        data.update({'number_of_pairs': len(self.collector.pairs) or 'Unavailable'})
        data.update({'pairs' : self.get_html_for_pairs(self.collector.pairs) or 'Unavailable'})
        html = """
        <html>
            <head>
            <title>pycollector</title>

            <style type="text/css">
            table {
                border: 1px solid #000}
            </style>

            </head>

            <body>
                <h1>pycollector</h1>
                <p>Collecting units: %(number_of_pairs)s</p>
                <h3><b>Collecting Units</b><h3>
                %(pairs)s
            </body>
        </html>
        """ % data
        return html

    index.exposed = True

    @cherrypy.expose
    def coll(self):
        return str(self.collector)

    @cherrypy.expose
    def queues(self):
        msg = ''
        for pair in self.collector.pairs:
            msg += str(pair[0].queue)
            msg += ' '
        return msg


class Server(threading.Thread):
    def __init__(self, logs_path='/tmp', collector=None, server_port=8442):
        threading.Thread.__init__(self)
        self.collector = collector
        self.conf = {
            'server.socket_port' : server_port,
            'log.access_file' : os.path.join(logs_path, 'web-access.log'),
            'log.error_file' : os.path.join(logs_path, 'web.log'),
            'log.screen' : False,
        }
        cherrypy.config.update(self.conf)

    def run(self):
        cherrypy.quickstart(Home(self.collector),
                            config={'global' : self.conf})

if __name__ == '__main__':
    Server().start()

