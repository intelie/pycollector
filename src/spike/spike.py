#spike solution: testing sqlalchemy...


from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


user = "root"
passwd = ""
host = "localhost"
database = "holmes"

#getting data from mysql (must install python-mysqldb)
columns = ('entity_type_id', 'name', 'description')
statement = "SELECT entity_type_id, name, description FROM entity_type"

engine = create_engine('mysql+mysqldb://%s:%s@%s/%s' % (user, passwd, host, database), echo=True)
Session = sessionmaker(bind=engine)
session = Session()
print session.query(*columns).from_statement(statement).all()


