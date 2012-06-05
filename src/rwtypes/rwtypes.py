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
    'custom_log' : {
        'module' : 'readers.log.CustomLogReader',
        'class' : 'CustomLogReader'
        },
    'stdin' : {
        'module' : 'readers.stdin.StdinReader',
        'class' : 'StdinReader'
        },
    'camr' : {
        'module' : 'readers.cam.CameraReader',
        'class' : 'CameraReader'
        },
    'adhoc' : {
        'module' : 'readers.adhoc.AdhocReader',
        'class' : 'AdhocReader'
        },
    'arduino' : {
        'module' : 'readers.arduino.ArduinoReader',
        'class' : 'ArduinoReader'
        },
    'log_analytics' : {
        'module' : 'readers.log_analytics.LogAnalytics',
        'class' : 'LogAnalytics'
        },
    'mtr' : {
        'module' : 'readers.mtr.MtrReader',
        'class' : 'MtrReader'
        },
    'gtalkr' : {
        'module' : 'readers.gtalk.GtalkReader',
        'class' : 'GtalkReader'
        }
}


writer_types = {
    'activemq' : {
        'module' : 'writers.activemq.ActivemqWriter',
        'class' : 'ActivemqWriter'
        },
    'gtalkw' : {
        'module' : 'writers.gtalk.GtalkWriter',
        'class' : 'GtalkWriter'
        },
    'stdout' : {
        'module' : 'writers.stdout.StdoutWriter',
        'class' : 'StdoutWriter'
    },
    'file' : {
        'module' : 'writers.file.FileWriter',
        'class' : 'FileWriter'
    },
    'socket' : {
        'module' : 'writers.socket.SocketWriter',
        'class' : 'SocketWriter'
    },
    'camw' : {
        'module' : 'writers.cam.CameraWriter',
        'class' : 'CameraWriter'
    }
}


def get_reader_type(type):
    return reader_types[type]

def get_reader_keys():
    return reader_types.keys()

def get_writer_keys():
    return writer_types.keys()

def get_writer_type(type):
    return writer_types[type]

def get_type(type):
    try:
        return reader_types[type]
    except:
        pass
    try:
        return writer_types[type]
    except:
        return None

def get_all_types():
    return reader_types.values() + writer_types.values()


if __name__ == '__main__':
    print get_reader_type('db')
    print get_writer_type('activemq')
    print get_type('db')
