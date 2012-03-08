import os
import sys
import logging

try:
    import __meta__
    __meta__.load_paths()
except Exception, e:
    print e
    sys.exit(-1)

import daemon_conf
from helpers import yaml
from __exceptions import ConfigurationError


log = logging.getLogger('pycollector')
default_yaml_filepath = os.path.join(__meta__.PATHS["CONF_PATH"], "conf.yaml")


def load_yaml_conf(file_path=default_yaml_filepath):
    f = open(file_path, 'r+')
    return yaml.load(f.read())


def has_type(conf):
    if not 'type' in conf:
        raise ConfigurationError("Missing 'type' in conf.yaml")

def has_checkpoint_path(conf):
    if ('checkpoint_enabled' in conf and \
        not ('checkpoint_path' in conf)):
         raise(ConfigurationError("Missing checkpoint_path for '%s' in conf.yaml" % conf['type']))


def has_blockable_and_period(conf):
    if 'blockable' in conf and \
        conf['blockable'] == True \
        and 'period' in conf:
        raise(ConfigurationError("'blockable' and 'period' are incompatibles for reader. Check your conf.yaml"))


def read_yaml_conf(file_conf=load_yaml_conf()):
    if not 'conf' in file_conf:
        raise ConfigurationError("'conf' section missing in your conf.yaml.")

    if not file_conf['conf']:
        raise ConfigurationError("'conf' section is empty in your conf.yaml.")

    specs = file_conf['specs']
    conf = file_conf['conf']

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
                raise ConfigurationError("Cannot find spec '%s' in specs session" % specs)
            new_reader.update(specs[spec])

        has_type(new_reader)
        has_checkpoint_path(new_reader)
        has_blockable_and_period(new_reader)

        if 'spec' in writer:
            spec = writer['spec']
            new_writer.pop('spec')
            if not specs or (not spec in specs):
                raise ConfigurationError("Cannot find spec '%s' in specs session" % spec)
            new_writer.update(specs[spec])

        has_type(new_writer)
        has_checkpoint_path(new_writer)

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
