import datafactory_cli
import sqlalchemy



#sink connection  is a sqlite3 database
sink_conn = 'sqlite:///data.db'

app = datafactory_cli.DataFactory(source_conn='Dev', sink_conn=sink_conn, database='Omnia_D365_DW')

#get tables from source
tables = app.get_tables()
tables_list = tables['TABLE_NAME'].tolist()


for table in tables_list:
    print(table)
    df = app.get_data("SELECT top 10 * FROM " + table)
    app.write_data(df, table)
    
    

app.close_connections()