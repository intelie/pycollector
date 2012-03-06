import os
import sys
import logging

try:
    import __meta__
    sys.path = __meta__.PATHS.values() + sys.path
except Exception, e:
    print e
    sys.exit(-1)

import daemon_conf
from helpers import yaml
from __exceptions import ConfigurationError


log = logging.getLogger()
default_yaml_filepath = os.path.join(__meta__.PATHS["CONF_PATH"], "conf.yaml")


def load_yaml_conf(file_path=default_yaml_filepath):
    f = open(file_path, 'r+')
    return yaml.load(f.read())


def read_yaml_conf(file_conf=load_yaml_conf()):
    if not 'conf' in file_conf:
        raise ConfigurationError("No configuration found, check your conf.yaml.")
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

        if not 'type' in new_reader:
            raise ConfigurationError("Missing reader 'type' in conf.yaml")

        if ('checkpoint_enabled' in new_reader and \
            not ('checkpoint_path' in new_reader)):
             raise(ConfigurationError("Missing checkpoint_path for reader %s" % new_reader['type']))

        if 'spec' in writer:
            spec = writer['spec']
            new_writer.pop('spec')
            if not specs or (not spec in specs):
                raise ConfigurationError("Cannot find spec '%s' in specs session" % spec)
            new_writer.update(specs[spec])

        if not 'type' in new_writer:
            raise ConfigurationError("Missing writer 'type' paired with a '%s' reader in conf.yaml" % reader['type'])

        if ('checkpoint_enabled' in new_writer and \
            not ('checkpoint_path' in new_writer)):
             raise(ConfigurationError("Missing checkpoint_path for reader %s" % new_writer['type']))

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
