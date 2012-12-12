# coding: utf-8

import sys; sys.path.append('..')
import __meta__; __meta__.load_paths()
from third.sqlalchemy.ext.declarative import declarative_base
from third.sqlalchemy import Column, Integer, BigInteger, String

Base = declarative_base()

class MessageEntity(Base):
    __tablename__ = 'message'

    id = Column(Integer, primary_key=True)
    interval_started_at = Column(BigInteger()) #1323202860000
    aggregation_type = Column(String(15))
    service = Column(String(15))
    value = Column(Integer())
    column_value = Column(String(15))
    host = Column(String(100))
    client = Column(String(15))
    service_type = Column(String(15))
    interval_duration_sec = Column(Integer())
    column_name = Column(String(50))


    def __init__(self, msg):
        for k,v in msg.items():
            setattr(self, k, v)

        if 'column_values' in msg.keys():
            self.column_value = str(msg['column_value'])

    def __str__(self):
        return u"<Message #%s [%s\t%s => %s]>" % (self.id, self.host, self.column_name, self.column_value)

