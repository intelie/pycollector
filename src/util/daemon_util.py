#!/usr/bin/env python
# -*- coding: utf-8 -*-


import os
import shlex
from subprocess import call, Popen, PIPE


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


def dir_exists(path):
    if not os.path.exists(path):
        print "Logging path configured doesn't exist."
        choice = raw_input('Do you want me to create the directory: %s ? [Y|n] ' % \
                            path)
        if choice == "" or choice.upper() == 'Y':
            return_code = call(shlex.split('mkdir %s' % path))
            if return_code != 0:
                print "Can't create directory."
                return False
            else:
                print "Directory created with success."
                return True
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
