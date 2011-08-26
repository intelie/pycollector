
#For a complete list of connection strings, take a look at:
#http://www.sqlalchemy.org/docs/core/engines.html#sqlalchemy.create_engine
connect_string = "mysql+mysqldb"

DB_USER = "root"
DB_PASSWD = ""
DB_HOST = "localhost"
DB_NAME = "holmes"


COLUMNS = ('entity_type_id', 'name', 'description')
STATEMENT = "SELECT entity_type_id, name, description FROM entity_type"

