import pandas as pd
import pyodbc
import sqlalchemy

class DataFactory:
    def __init__(self, source_conn, sink_conn, database):
        #get a source connection via DSN
        #create DSN via ODBC Data Source Administrator
        self.source_conn = pyodbc.connect('DSN=' + source_conn + ';DATABASE=' + database + ';')
        #sink connection via sqlalchemy
        self.sink_conn = sqlalchemy.create_engine(sink_conn)
        
    def get_data(self, query):
        #get data from source
        return pd.read_sql(query, self.source_conn)
    
    def get_tables(self):
        #get all tables from source
        #chance schema to your schema
        return pd.read_sql("SELECT * FROM information_schema.tables WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = 'Jandre'", self.source_conn)
    
    def write_data(self, df, table_name):
        #write data to sink
        df.to_sql(table_name, self.sink_conn, if_exists='replace', index=False)
        
    def close_connections(self):
        #close connections
        self.source_conn.close()
        self.sink_conn.dispose()
        