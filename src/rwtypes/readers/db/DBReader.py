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

        Note: if checkpoint is used, query must guarantee sorted data"""

    def setup(self):
        self.log = logging.getLogger()
        engine = create_engine(self.connection %(self.user, 
                                                 self.passwd, 
                                                 self.host, 
                                                 self.database), 
                                                 echo=True)
        Session = sessionmaker(bind=engine)
        self.session = Session()
        self.set_current_checkpoint()

    def set_current_checkpoint(self):
        if self.checkpoint_enabled:
            self.current_checkpoint = self.last_checkpoint or 0 
        else:
            self.current_checkpoint = 0 

    def read(self):
        try:
            self.set_current_checkpoint()
            data = self.session.query(*self.columns).from_statement(self.query).all()
            current_len = len(data)
            if current_len == 0:
                self.log.info('No data for query: %s' % self.query)
                self.current_checkpoint = 0
                return True

            if current_len < self.current_checkpoint:
                self.current_checkpoint = 0

            if self.checkpoint_enabled:
                data = data[(self.current_checkpoint):]

            if len(data) == 0:
                self.log.info("No new data based on checkpoint.")
                return True
            
            for datum in data:
                to_send = { column : datum[i] for i, column in enumerate(self.columns)}

                #XXX: Specific use, deals with datetime and None
                to_add = {}
                for item in to_send:
                    if isinstance(to_send[item], datetime.datetime):
                        t = to_send[item]
                        to_send[item] = t.isoformat()
                        time_tuple = (t.year,
                                      t.month,
                                      t.day,
                                      t.hour,
                                      t.minute,
                                      t.second,
                                      t.microsecond)
                        to_add['%s_ts' % item] = calendar.timegm(time_tuple)*1000
                    elif to_send[item] == None:
                       to_send[item] = 'NULL'

                to_send.update(to_add)
                self.current_checkpoint += 1
                if not self.store(Message(content=to_send, checkpoint=self.current_checkpoint)):
                    return False
            return True
        except Exception, e:
            self.log.error('Error reading from database')
            self.log.error(e)
            return False
