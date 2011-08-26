from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from __reader import *


class MyReader(Reader):
    def setup(self):
        self.periodic = True
        self.interval = 5

        user = "root"
        passwd = ""
        host = "localhost"
        database = "holmes"
        engine = create_engine('mysql+mysqldb://%s:%s@%s/%s' % (user, passwd, host, database), echo=True)
        Session = sessionmaker(bind=engine)
        self.session = Session()

    def read(self):
        statement = "select * from user"
        columns = ('username', 'name', 'password')
        return self.session.query(*columns).from_statement(statement).all()

