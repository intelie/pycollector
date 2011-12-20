from third.sqlalchemy import create_engine
from third.sqlalchemy.orm import sessionmaker

from __reader import Reader


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
        try:
            data = self.session.query(*self.columns).from_statement(self.query).all()

            if not data:
                print "No data for query: '%s'" % self.query
            else:
                for datum in data:
                    to_send = { column : datum[i] for i, column in enumerate(self.columns)}
                    self.store(to_send)
            return True
        except Exception, e:
            print e
            return False
