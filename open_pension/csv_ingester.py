from datetime import datetime
import pandas as pd
import os, os.path, sys, glob
import sqlalchemy 


class csv_ingester(object):

    def __init__(self):
        super().__init__()
        self.connect()

    db_config = {"host": os.environ.get('PDB_HOST',"localhost"), "database": os.environ.get('PENSSION_TMP_DB',"PENSSION"),
                 "user": os.environ.get('PDB_USER',"penssion"), "password": os.environ.get('PDB_PASSWORD',"H454dn%40")}
    conn = None

    def connect(self):
        #try:
        print('Connecting to database...')

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

    def verify_kupa_name(self, kupa_no):
        meta_data = sqlalchemy.MetaData(bind=self.conn)
        sqlalchemy.MetaData.reflect(meta_data)
        table = meta_data.tables["kupot"]
        query = sqlalchemy.select([table.c[1]]).where((table.c[0] == kupa_no))
        result = self.conn.execute(query).fetchall()
        if len(result) == 0 :
            print ("Kupa {0} is not in list".format(kupa_no))
            insert = sqlalchemy.insert(table).values(kupa_no=kupa_no, kupa_name="unknown",
                last_retrieved=datetime.now())
            self.conn.execute(insert)  
        for record in result:
            print("\n", record[0])
            update = sqlalchemy.update(table).values({"last_retrieved":"{0}".format(datetime.now())}).where(table.c[0] == kupa_no)
            self.conn.execute(update)

    def ingest(self):
        for dir in glob.glob('*'):
            table_name = dir.replace("-", "_").capitalize() #file.split(".")[0].replace("-", "_")
            for file in glob.glob(dir+'/*.csv'):
                file_name = file.split("/")[1]
                file_array = file_name.split("-")
                kupa_no = file_array[0]
                month = "{0}-{1}".format(file_array[1], file_array[2])
                df = pd.read_csv(file,sep=',',quotechar='\"',encoding='utf8')
                if df.size > 0 :
                    df.to_sql(table_name, self.conn, if_exists='append', index=False)  
                    print("File is {0} headrs are:".format(file))      
                    print(df.columns.values.tolist())
                    self.verify_kupa_name(kupa_no)

def main():  # Main
        csv_dir = os.environ.get('CSV_DIR',"scrapers/data")
        os.chdir(csv_dir)
        ingester = csv_ingester()
        ingester.ingest()
        ingester.close()

if __name__ == '__main__':
    main()           