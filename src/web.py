# -*- coding:utf-8 -*-

"""
    File: web.py
    Description: going to be a simple web server with pycollector realtime metrics. 
"""

import sys; sys.path.append('helpers')
import threading

import cherrypy


class Home:
    def __init__(self, collector=None):
        self.collector = collector

    def index(self):
        return "pycollector"

    index.exposed = True


class Server(threading.Thread):
    def __init__(self, collector=None):
        threading.Thread.__init__(self)
        self.collector = collector
        cherrypy.config.update({'server.socket_port' : 8442})

    def run(self):
        cherrypy.quickstart(Home(self.collector))

if __name__ == '__main__':
    Server().start()
