__author__ = 'Monali'

from sqlalchemy import create_engine

class CreateDB:

    def __init__(self, db_user, db_password, db_address, db_port):

        # Create database connection
        self.connection_string = 'mysqldb://'+ db_user + ':' + db_password + '@'+ db_address + ':' + db_port + '/'

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
