from helpers import yaml

#todo: refactor! experiment

def read_conf(file_path='../../conf/conf.yaml'):
    f = open(file_path, 'r+')
    file_conf = yaml.load(f.read())
    
    conf = file_conf['conf']
    if not conf:
        print """No configurations found, check your conf.yaml. 
        Nothing to do. Killing myself."""
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
            if not spec in specs:
                print "spec '%s' not found in conf" % spec
                exit(-1)
            new_reader.update(specs[spec])

        if 'spec' in writer: 
            spec = writer['spec']
            new_writer.pop('spec')
            if not spec in specs:
                print "spec '%s' not found in conf" % spec
                exit(-1)
            new_writer.update(specs[spec])
        new_pair = {'reader': new_reader, 'writer' : new_writer}
        new_conf.append(new_pair)
    return new_conf

if __name__ == '__main__':
    print read_conf()
