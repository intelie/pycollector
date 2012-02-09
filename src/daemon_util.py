#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import shlex
import logging
from subprocess import call, Popen, PIPE

import __meta__
import conf_reader


def write_pid(pidfile):
    f = open(pidfile, 'w+')
    f.write(str(os.getpid()))
    f.close()
    os.system("chmod 644 %s" % pidfile)


def remove_pidfile(pidfile):
    return call(shlex.split("rm -rf %s" % pidfile))


def get_pid(pidfile):
    f = open(pidfile, 'r')
    pid = int(f.read())
    f.close()
    return pid


def kill_pids(pids):
    """Input: a list of ints (pids) | Output: the returning code from kill."""
    pids = map(lambda x: str(x), pids)
    return call(shlex.split("kill -9 %s" % ' '.join(pids)))


def is_running(ps="""ps aux --cols=1000 |
                     grep -E 'pycollector .*start' |
                     grep -v 'grep' | awk {'print $2'}"""):
    cmd = Popen(ps, stdout=PIPE, shell=True)
    pids = cmd.stdout.read().split('\n')
    pids = filter(lambda x: x.isdigit(), pids)
    pids = map(lambda x: int(x), pids)

    cur_pid = os.getpid()
    if cur_pid in pids: 
        pids.remove(cur_pid)

    if len(pids) > 0: 
        return (True, pids)
    return (False, pids)


def log_dir_exists(path):
    return os.path.exists(path)


def suggest_log_path_creation(path):
    path = os.path.split(path)[0]
    choice = raw_input('Do you want me to create the directory: %s ? [Y|n] ' % path)
    if choice == "" or choice.upper() == 'Y':
        return_code = call(shlex.split('mkdir %s' % path))
        if return_code != 0:
            print "Can't create directory."
            return False
        else:
            print "Directory created with success."
            return True
    return True


def set_logging():
    try:
        daemon_conf = conf_reader.read_daemon_conf()
        log_path = daemon_conf['LOG_FILE_PATH']
        log_dir = os.path.split(log_path)[0]
        if not (log_dir_exists(log_dir) or suggest_log_path_creation(log_path)):
            exit(-1)

        logger = logging.getLogger()
        logger.setLevel(daemon_conf['LOG_SEVERITY'])
        rotating = daemon_conf['LOG_ROTATING']
        handler = logging.handlers.TimedRotatingFileHandler(log_path, when=rotating)
        formatter = logging.Formatter(daemon_conf['LOG_FORMATTER'])
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.addHandler(logging.StreamHandler())
        return logger
    except Exception, e:
        print "Cannot set logging."
        print e


def start(collector, to_daemon=True):
    if is_running()[0]:
        print "Daemon already running."
        sys.exit(-1)
    
    if to_daemon:
        d = daemon.DaemonContext(working_directory=os.getcwd())
        d.open()

    write_pid(collector.daemon_conf['PID_FILE_PATH'])
    collector.start()


def status():
    pid_path = conf_reader.read_daemon_conf()['PID_FILE_PATH']

    if not is_running()[0]:
        print "Status: NOT RUNNING."
        if os.path.exists(pid_path):
            print "WARNING: Pidfile in %s seems to be obsolete. Please, remove it mannualy." % pid_path
            sys.exit(-1)
        sys.exit(0)
    print "Status: RUNNING."
    sys.exit(0)


def stop():
    running, pids = is_running()
    if not running:
        print "Daemon was not running."
        sys.exit(-1)

    pid_path = conf_reader.read_daemon_conf()['PID_FILE_PATH']

    if not pid_path or not os.path.exists(pid_path):
        print "WARNING: Seems to be running, but can't get pidfile in %s. Kill it manually." % pid_path
        sys.exit(-1)

    try:
        print "Stopping daemon..."
        pid = get_pid(pid_path)
    except Exception, e:
        print "Can't read pidfile. Daemon not stopped."
        print e
        sys.exit(-1)

    try:
        if kill_pids([pid]) != 0:
            print "Can't stop daemon. PID tried: %s" % pid
            sys.exit(-1)
    except Exception, e:
        print "Can't stop daemon. PID tried: %s" % pid
        print e
        sys.exit(-1)
    else:
        try:
            if remove_pidfile(pid_path) == 0:
                print "Daemon stopped."
            else:
                print "Daemon stopped, but can't remove pidfile.\nRemove manually the file %s." % pid_path
        except Exception, e:
                print "Daemon stopped, but can't remove pidfile.\nRemove manually the file %s." % pid_path
    sys.exit(0)
    
