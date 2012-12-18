# coding: utf-8
import os
import sys; sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
import __meta__; __meta__.load_paths()
from third.sqlalchemy import create_engine
from MessageEntity import Base

engine = create_engine("mysql://root:root@localhost/test")

Base.metadata.create_all(engine)
