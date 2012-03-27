# -*- coding: utf-8 -*-

import datetime
import calendar
import logging

from third.sqlalchemy import create_engine
from third.sqlalchemy.orm import sessionmaker

from __reader import Reader
from __message import Message


class DBReader(Reader):
    """Conf: 
        - period (required): period of reads in seconds
        - user (required): database username,
        - passwd (required): database password
        - host (required): database hostname
        - database (required): database name
        - connection (required): connection string,
            e.g. 'mysql+mysqldb://%s:%s@%s/%s' (see sqlalchemy doc)
        - query (required): sql statement,
            e.g. select name, description from user
        - columns (required): list of columns in the same order from 'query',
            e.g. [name, description]

        Note: if checkpoint is used, query must ensure sorted data"""

    pass
