#!/usr/bin/env python
# -*- coding: utf-8 -*-


import os
import subprocess


def write_pid(pidfile):
    
    f = open(pidfile, 'w+')
    f.write(str(os.getpid()))
    f.close()
    os.system("chmod 644 %s" % pidfile)


def remove_pidfile(pidfile):
    return subprocess.call(["rm", "-rf", "%s" % pidfile],
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)


def get_pid(pidfile):
    f = open(pidfile, 'r')
    pid = int(f.read())
    f.close()


def kill_pids(pids):
    """Input: a list of ints (pids) | Output: the returning code from kill."""

    cmd = ["kill", "-9"]
    pids = map(lambda x: str(x), pids)
    cmd.extend(pids)
    return subprocess.call(cmd,
                           stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)


def is_running(ps="""ps aux | grep "collectord .*--start" | grep -v grep | awk {'print $2'}"""):
    ps = subprocess.Popen(ps,
                          shell=True,
                          stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE)

    pids = ps.stdout.read().split('\n')
    pids = filter(lambda x: x.isdigit(), pids)
    pids = map(lambda x: int(x), pids)

    cur_pid = os.getpid()
    if cur_pid in pids: 
        pids.remove(cur_pid)

    if len(pids) > 0: 
        return (True, pids)
    return (False, pids)


def dir_exists(path):
    if not os.path.exists(path):
        print "Logging path configured doesn't exist."
        choice = raw_input('Do you want me to create the directory: %s ? [Y|n] ' % \
                            path)
        if choice == "" or choice.upper() == 'Y':
            return_code = subprocess.call(['mkdir', path],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
            if return_code != 0:
                print "Can't create directory."
                return False
            else:
                print "Directory created with success."
                return True


def get_pattern_conf(filename):
    if filename:
        if filename.endswith('.py'):
            filename = filename[:-3]
        try:
            exec('from %s import conf as pattern_conf' % filename)
            print "Configuration file in use: %s" % filename
            return pattern_conf
        except Exception, e:
            print "Can't import configuration file. Aborting."
            print e
            exit(-1)
    else:
        try:
            from pattern_conf import conf
            print "Configuration file in use: pattern_conf"
            return conf
        except Exception, e:
            print "Can't import configuration file."
            print e
            exit(-1)
