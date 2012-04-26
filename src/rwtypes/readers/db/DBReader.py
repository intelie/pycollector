# -*- coding: utf-8 -*-

import datetime
import calendar
import logging
import traceback

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

    def start_session(self):
        engine = create_engine(self.connection % (self.user,
                                                  self.passwd,
                                                  self.host,
                                                  self.database),
                                                  echo=False)
        Session = sessionmaker(bind=engine)
        self.session = Session()

    def close_session(self):
        self.session.close()

    def do_query(self):
        try:
            self.start_session()
            self.results = self.session.query(*self.columns).from_statement(self.query).all()
            self.results = zip(range(len(self.results)), self.results)
            self.close_session()
            return True
        except Exception, e:
            self.log.error("Error during query execution: %s" % self.query)
            self.log.error(traceback.format_exc())
            return False

    def store_results(self):
        messages = [Message(checkpoint=result[0],
                            content=dict(zip(self.columns, result[1])))
                    for result in self.results]
        for message in messages: self.store(message)

    def set_current_checkpoint(self):
        self.current_checkpoint = self.last_checkpoint or {'pos': 0}

    def setup(self):
        self.log = logging.getLogger()
        self.required_confs = ['columns', 'query', 'user',
                               'passwd', 'host', 'database']
        self.check_required_confs()
        if self.checkpoint_enabled:
            self.set_current_checkpoint()

    def read(self):
        if self.period:
            self.do_query() and self.store_results()
            return

