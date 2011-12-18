from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from __reader import *


class DBReader(Reader):
    def setup(self):
        engine = create_engine(self.connection %(self.user, 
                                                 self.passwd, 
                                                 self.host, 
                                                 self.database), 
                                                 echo=True)
        Session = sessionmaker(bind=engine)
        self.session = Session()

    def read(self):
        data = self.session.query(*self.columns).from_statement(self.query).all()

        if not data:
            print "No data for query: '%s'" % self.query
            return False
        else:
            for datum in data:
                self.store(datum)
        return True
