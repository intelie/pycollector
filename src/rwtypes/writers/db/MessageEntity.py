# coding: utf-8

import sys; sys.path.append('..')
import __meta__; __meta__.load_paths()
from third.sqlalchemy.ext.declarative import declarative_base
from third.sqlalchemy import Column, Float, Integer, String, DateTime

Base = declarative_base()

class MessageEntity(Base):
    __tablename__ = 'message'

    id = Column(Integer, primary_key=True)

    def __init__(self, msg):

        # metaclass hack - generate table on the fly
        __type_to_field = {
                            type(1): Integer,
                            type(1.0): Float,
                            type('1'): String,
                        }

        __property_hash = {str(k): __type_to_field.get(type(v), DateTime) for (k,v) in msg.items()}

        for k,v in __property_hash.items():
            if v in [Integer, Float, DateTime]:
                __property_hash[k] = v()
            if v == String:
                __property_hash[k] = v(1024)


    def __str__(self):
        return str(self.__dict__)


