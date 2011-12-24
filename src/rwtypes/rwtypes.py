import sys 
import os

reader_types = {
    'db' : {
        'module' : 'readers.db.DBReader',
        'class' : 'DBReader'
        },
    'log' : {
        'module' : 'readers.log.LogReader',
        'class' : 'LogReader'        
        },
    'stdin' : {
        'module' : 'readers.stdin.StdinReader',
        'class' : 'StdinReader'
        }
}


writer_types = {
    'activemq' : {
        'module' : 'writers.activemq.ActivemqWriter',
        'class' : 'ActivemqWriter'
        },
    'gtalk' : {
        'module' : 'writers.gtalk.GtalkWriter',
        'class' : 'GtalkWriter'
        }
}


def get_reader_type(type):
    return reader_types[type]


def get_writer_type(type):
    return writer_types[type]


def get_all_types():
    return reader_types.values() + writer_types.values() 


if __name__ == '__main__':
    print get_reader_type('db')
    print get_writer_type('activemq')
