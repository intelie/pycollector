# coding: utf-8

import sys; sys.path.append('..')
import __meta__; __meta__.load_paths()
from third.sqlalchemy.ext.declarative import declarative_base
from third.sqlalchemy import Column, Float, Integer, String, DateTime

Base = declarative_base()

class MessageEntity(Base):
    __tablename__ = 'message'

    id = Column(Integer, primary_key=True)
    interval_started_at = Column(DateTime)
    value = Column(Integer)

    def __init__(self, message):
        self.interval_started_at = message['interval_started_at']
        self.value = message['value']

    def __str__(self):
        return u"<Message [interval_started_at: %s | value: %d]>" % (self.interval_started_at, self.value)

