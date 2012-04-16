#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pythoncom
import win32serviceutil
import win32service
import win32event
import servicemanager
import socket
import sys
import time
import os
import sys, os 
sys.path.append(os.path.join(os.path.dirname(__file__), "../src"))  
sys.path.append(os.path.join(os.path.dirname(__file__), "../conf"))  

class LogCollectSvc (win32serviceutil.ServiceFramework):
    _svc_name_ = "LogCollect"
    _svc_display_name_ = "Log Collect Service"

    def __init__(self,args):
        win32serviceutil.ServiceFramework.__init__(self,args)
        self.hWaitStop = win32event.CreateEvent(None,0,0,None)
        self._stopped = False
        socket.setdefaulttimeout(60)

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)
        self.collector.stop()
        self._stopped = True

    def SvcDoRun(self):
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                              servicemanager.PYS_SERVICE_STARTED,
                              (self._svc_name_,''))
        self.main()

    def main(self):
        import daemon_conf
        from pattern_conf import conf
        from logcollector import LogCollector
        print "Configuration file in use: pattern_conf"
        self.collector = LogCollector(conf)
        self.collector.start()
        while not self._stopped:
            time.sleep(1)

if __name__ == '__main__':
    win32serviceutil.HandleCommandLine(LogCollectSvc)