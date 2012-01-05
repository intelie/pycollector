# -*- coding: utf-8 -*-

import datetime
import calendar

from third.sqlalchemy import create_engine
from third.sqlalchemy.orm import sessionmaker

from __reader import Reader
from __message import Message


class DBReader(Reader):
    """Conf: 
        - interval (required): period of reads in seconds
        - user (required): database username,
        - passwd (required): database password
        - host (required): database hostname
        - database (required): database name
        - connection (required): connection string,
            e.g. 'mysql+mysqldb://%s:%s@%s/%s' (see sqlalchemy doc)
        - query (required): sql statement,
            e.g. select name, descriptrion from user
        - columns (required): list of columns in the same order from 'query',
            e.g. [name, description]

        Note: if checkpoint is used, query must guarantee sorted data"""

    def setup(self):
        engine = create_engine(self.connection %(self.user, 
                                                 self.passwd, 
                                                 self.host, 
                                                 self.database), 
                                                 echo=True)
        Session = sessionmaker(bind=engine)
        self.session = Session()

    def read(self):
        try:
            data = self.session.query(*self.columns).from_statement(self.query).all()

            if len(data) == 0:
                print '[dbreader] no data for query: %s' % self.query
                return True

            #getting only new data (based on checkpoint)
            if self.last_checkpoint:
                data = data[:(len(data) - int(self.last_checkpoint))]

            if len(data) <= 0:
                print "[dbreader] no new data based on checkpoint"
                return True
            else:
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

                    if not self.last_checkpoint:
                        self.store(Message(content=to_send, checkpoint= 1))
                    else:
                        self.store(Message(content=to_send, checkpoint=str(int(self.last_checkpoint) + 1)))
            return True
        except Exception, e:
            print '[dbreader] error reading from database'
            print e
            return False
