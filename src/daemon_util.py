#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import shlex
import pprint
import logging
import traceback
from subprocess import call, Popen, PIPE

from helpers import daemon

import __meta__
import ascii
from __exceptions import ConfigurationError
import conf_reader


def log_tail():
    file_path = os.path.join(conf_reader.read_daemon_conf()['LOGS_PATH'], 'pycollector.log')
    try:
        os.system("tail -42f %s" % file_path)
    except KeyboardInterrupt:
        sys.exit(0)


def write_pid(pidfile):
    f = open(pidfile, 'w')
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
                     grep -E '[0-9] python.*pycollector .*start' |
                     grep -v 'grep' | awk '{print $2}'"""):

    cmd = Popen(ps, stdout=PIPE, shell=True)
    output = cmd.stdout.read()
    pids = output.split('\n')
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
        log_path = os.path.join(daemon_conf['LOGS_PATH'], 'pycollector.log')
        log_dir = os.path.split(log_path)[0]
        if not (log_dir_exists(log_dir) or suggest_log_path_creation(log_path)):
            exit(-1)

        logger = logging.getLogger('pycollector')
        exec("logger.setLevel(logging.%s)" % daemon_conf['LOG_SEVERITY'])
        rotating = daemon_conf['LOG_ROTATING']
        formatter = logging.Formatter(daemon_conf['LOG_FORMATTER'])

        file_handler = logging.handlers.TimedRotatingFileHandler(log_path, when=rotating)
        stream_handler = logging.StreamHandler()

        file_handler.setFormatter(formatter)
        stream_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)
        return logger
    except Exception, e:
        print "Cannot set logging."
        traceback.print_exc()


def start(collector_clazz, to_daemon=True, enable_server=True, server_port=8442):
    if is_running()[0]:
        print "Daemon already running."
        sys.exit(-1)

    #starts daemon context
    if to_daemon:
        d = daemon.DaemonContext(working_directory=os.getcwd())
        d.open()

    try:
        #guarantees that libs are available in daemon context
        sys.path.append(__meta__.PATHS.values())

        log = set_logging()
        log.info(ascii.ascii)
        log.info("Starting collector...")

        try:
            collector = collector_clazz(conf_reader.read_yaml_conf(),
                                        conf_reader.read_daemon_conf(),
                                        enable_server=enable_server,
                                        server_port=server_port)
        except ConfigurationError, e:
            log.error(e.msg)
            sys.exit(-1)

        log.info("daemon_conf.py settings (missing values are replaced by defaults):")
        log.info(pprint.pformat(collector.daemon_conf))

        log.info("conf.yaml settings:")
        log.info(pprint.pformat(collector.conf))

        write_pid(collector.daemon_conf['PID_FILE_PATH'])
        collector.start()
    except Exception, e:
        log.error(traceback.format_exc())

    #finishes daemon context
    if to_daemon:
        d.close()


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

def force_stop():
    running, pids = is_running()
    try:
        if len(pids) == 0:
            print "No instance running."
            sys.exit(0)
        print "Force stopping..."
        if kill_pids(pids) != 0:
            print "Couldn't stop."
            sys.exit(-1)
        else:
            print "Done."
            sys.exit(0)
    except Exception, e:
        traceback.print_exc()
        sys.exit(-1)

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
        traceback.print_exc()
        sys.exit(-1)

    try:
        if kill_pids(pids) != 0:
            print "Can't stop daemon. PID tried: %s" % pid
            sys.exit(-1)
    except Exception, e:
        print "Can't stop daemon. PID tried: %s" % pid
        traceback.print_exc()
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

