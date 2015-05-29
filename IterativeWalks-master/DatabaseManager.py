__author__ = 'ashish'

from sqlalchemy import create_engine

class DatabaseManager:

    def __init__(self, db_user, db_password, db_address, db_port, db_name):

        # Create database connection
        self.connection_string = 'mysql+mysqldb://'+ db_user + ':' + db_password + '@'+ db_address + ':' + db_port + '/' + db_name

    def Connect(self):
        engine = create_engine(self.connection_string)
        self.connection = engine.connect()
        self.connection.autocommit = False

    def Execute(self,Query):
        transaction = self.connection.begin()
        try:
            result = self.connection.execute(Query)
            transaction.commit()
        except:
            transaction.rollback()
            raise
        return result

    def Close(self):
        self.connection.close()

