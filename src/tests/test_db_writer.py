import Queue
import Queue
import sys; sys.path.append('..')
import __meta__; __meta__.load_paths()
from rwtypes.writers.db.DBWriter import DBWriter
from rwtypes.writers.db.MessageEntity import MessageEntity
from third.sqlalchemy import create_engine
from third.sqlalchemy.orm import sessionmaker
from dateutil.tz import tzlocal
import datetime
import logging

test_messages = [
    {'interval_started_at': datetime.datetime(2012, 9, 29, 19, 5, tzinfo=tzlocal()), 'value': 0},
    {'interval_started_at': datetime.datetime(2012, 7, 19, 19, 5, tzinfo=tzlocal()), 'value': 0},
    {'interval_started_at': datetime.datetime(2012, 5, 25, 19, 5, tzinfo=tzlocal()), 'value': 0},
    {'interval_started_at': datetime.datetime(2012, 3, 25, 19, 5, tzinfo=tzlocal()), 'value': 0},
    {'interval_started_at': datetime.datetime(2012, 1, 10, 19, 5, tzinfo=tzlocal()), 'value': 0}
]

def get_queue(maxsize=1024):
    return Queue.Queue(maxsize=maxsize)


conf = {'database': 'test',
        'host': 'localhost',
        'user': 'root',
        'passwd': 'root',
        'connection': 'mysql://%s:%s@%s/%s'}

writer = DBWriter(get_queue(), conf=conf)
writer.log.addHandler(logging.StreamHandler())

for message in test_messages:
    writer.write(message)


#engine = create_engine(conf['connection'] % (conf['user'],
#                                                conf['passwd'],
#                                                conf['host'],
#                                                conf['database']),
#                                                echo=False)

#Session = sessionmaker(bind=engine)
#session = Session()

#get_messages = session.query(writer.message_class).all()

#for msg in get_messages:
#    print msg

#[session.delete(x) for x in session.query(MessageEntity).all()]
#session.commit()
