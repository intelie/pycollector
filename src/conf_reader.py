import os
import sys
import logging

import __meta__
sys.path.extend(__meta__.PATHS.values())

import daemon_conf
from helpers import yaml


log = logging.getLogger()


def read_yaml_conf():
    f = open(os.path.join(__meta__.PATHS["CONF_PATH"], "conf.yaml"), 'r+')
    file_conf = yaml.load(f.read())
    
    conf = file_conf['conf']
    if not conf:
        log.error("No configurations found, check your conf.yaml.")
        log.info('Nothing to do. Aborting.')
        exit(-1)
    specs = file_conf['specs']

    new_conf = []

    for pair in conf:
        reader = pair['reader']
        writer = pair['writer']
        new_reader = reader
        new_writer = writer
        if 'spec' in reader:
            spec = reader['spec']
            new_reader.pop('spec')
            if not specs or (not spec in specs):
                log.error('Cannot find spec %s in specs session.' % spec)
                log.info('Aborting.')
                exit(-1)
            new_reader.update(specs[spec])

        if not 'type' in new_reader:
            log.error('Missing reader type in conf.yaml.')
            log.info('Aborting.')
            exit(-1)

        if 'spec' in writer: 
            spec = writer['spec']
            new_writer.pop('spec')
            if not specs or (not spec in specs):
                log.error("Cannot find spec '%s' in specs session." % spec)
                log.info('Aborting.')
                exit(-1)
            new_writer.update(specs[spec])

        if not 'type' in new_writer:
            log.error('Missing writer type in conf.yaml.')
            log.info('Aborting.')
            exit(-1)

        new_pair = {'reader': new_reader, 'writer' : new_writer}
        new_conf.append(new_pair)
    return new_conf


def read_daemon_conf():
    conf = {}
    for key, value in __meta__.DEFAULTS.items():
        try:
            exec("conf['%s'] = daemon_conf.%s" % (key, key))
        except AttributeError:
            conf[key] = __meta__.DEFAULTS[key]
    return conf



if __name__ == '__main__':
    print read_daemon_conf()
