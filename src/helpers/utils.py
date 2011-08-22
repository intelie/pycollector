#!/usr/bin/env python
# -*- coding: utf-8 -*-


import subprocess


def is_running():
    ps = subprocess.Popen("""ps aux | grep "collectord .*--start" | grep -v grep | awk {'print $2'}""", 
                          shell=True,
                          stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE)
    pids = ps.stdout.read().split('\n')
    current_pid = os.getpid()
    pids = filter(lambda x: x.isdigit(), pids)
    pids = map(lambda x: int(x), pids)
    if current_pid in pids:
        pids.remove(current_pid)
    if len(pids) > 0:
        return (True, pids)
    return (False, pids)


def check_logging_path(daemon_conf):
    if not os.path.exists(daemon_conf.LOGGING_PATH):
        print "Logging path configured doesn't exist."
        choice = raw_input('Do you want me to create the directory: %s ? [Y|n] ' % \
                            daemon_conf.LOGGING_PATH)
        if choice == "" or choice.upper() == 'Y':
            return_code = subprocess.call(['mkdir', daemon_conf.LOGGING_PATH], 
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
            if return_code != 0:
                print "Can't create directory."
                sys.exit(-1)
            else:
                print "Logging directory created with success!"


def write_pid(pidfile):
    f = open(pidfile, 'w+')
    f.write(str(os.getpid()))
    f.close()
    os.system("chmod 644 %s" % pidfile)


def remove_pidfile(pidfile):
    return subprocess.call(["rm", "-rf", "%s" % pidfile],
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)


def get_pid_from_pidfile(pidfile):
    f = open(pidfile, 'r')
    pid = int(f.read())
    f.close()


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


def kill_pids(pids):
    """Input: a list of ints (pids).
    Output: the returning code from kill."""

    cmd = ["kill", "-9"]
    pids = map(lambda x: str(x), pids)
    cmd.extend(pids)
    return subprocess.call(cmd,
                           stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)
