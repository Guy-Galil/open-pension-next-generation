
import sqlalchemy 
from sqlalchemy import MetaData, Table


class schema_comparator(object):

    def __init__(self):
        super().__init__()
        self.connect()

    db_config = {"host": "localhost", "database": "PENSSION",
                 "user": "penssion", "password": "H454dn%40"}
    conn = None

    def connect(self):
        #try:
        print('Connecting to MySQL database...')

        self.conn = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                             format(self.db_config["user"], self.db_config["password"], 
                                                    self.db_config["host"], self.db_config["database"]))
        print('Connection established.')
        #except Error as error:
            #print(error)

    def close(self):
        if self.conn is not None :
            self.conn.dispose()
            print('Connection closed.')

    def compare_tables(self):
        metadata_obj = MetaData()
        metadata_obj.reflect(bind=self.conn)
        for table in metadata_obj.sorted_tables:
           print("-------------------------------------")
           print(table.name)
           print("-------------------------------------")
           for table1 in metadata_obj.sorted_tables:
                if table.name == table1.name:
                    continue
                for c in table.columns:
                    if c.name  not in table1.columns :
                        print("{0} , {1}, {2}, not in {3}".format(table.name,c.name, c.type, table1.name))
                    else:
                        print("{0} , {1}, {2}, is in {3}".format(table.name,c.name, c.type, table1.name))

def main():  # Main
        comp = schema_comparator()
        comp.compare_tables()
        comp.close()

if __name__ == '__main__':
    main()           