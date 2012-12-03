import Queue
import sys; sys.path.append('..')
import __meta__; __meta__.load_paths()
from rwtypes.writers.db.DBWriter import DBWriter
from rwtypes.writers.db.MessageEntity import MessageEntity
from third.sqlalchemy import create_engine
from third.sqlalchemy.orm import sessionmaker

test_messages = [
    "Lorem Ipsum is simply dummy text of the printing and typesetting industry.",
    "Lorem Ipsum has been the industry's standard dummy text ever since the 1500s.",
    "It is a long established fact that a reader will be distracted by the readable content of a page when looking at its layout."
]

def get_queue(maxsize=1024):
    return Queue.Queue(maxsize=maxsize)


conf = {'database': 'test',
        'host': 'localhost',
        'user': 'root',
        'passwd': 'root',
        'connection': 'mysql://%s:%s@%s/%s'}

writer = DBWriter(get_queue(), conf=conf)

for message in test_messages:
    writer.write(message)


engine = create_engine("mysql://%s:%s@%s/%s" % (conf['user'],
                                                conf['passwd'],
                                                conf['host'],
                                                conf['database']),
                                                echo=False)

Session = sessionmaker(bind=engine)
session = Session()

get_messages = session.query(MessageEntity).all()

for msg in get_messages:
    if not msg.message in test_messages:
        print "FAIL!"
        sys.exit(1)
    else:
        print msg.message

[session.delete(x) for x in session.query(MessageEntity).all()]
session.commit()
