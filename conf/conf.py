conf = [{
     'reader' : {
        'type' : 'db',
        'user' : 'root',
        'passwd' : '',
        'host' : 'localhost',
        'database' : 'test',
        'connection-string' : 'mysql+mysqldb://%s:%s@%s/%s',

        'query' : 'select username, password from user',
        'columns' : ['username', 'password'],
        'interval' : 1
     },

     'writer' : {
         'type' : 'activemq'
     }
}]

