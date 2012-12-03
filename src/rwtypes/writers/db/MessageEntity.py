# coding: utf-8

import sys; sys.path.append('..')
import __meta__; __meta__.load_paths()
from third.sqlalchemy.ext.declarative import declarative_base
from third.sqlalchemy import Column, Integer, String

Base = declarative_base()

class MessageEntity(Base):
    __tablename__ = 'message'

    id = Column(Integer, primary_key=True)
    message = Column(String(140))

    def __init__(self, msg):
        self.message = str(msg)

    def __str__(self):
        return u"<Message('%s')>" % self.message


