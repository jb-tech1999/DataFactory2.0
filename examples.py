"""
Examples demonstrating DataFactory 2.0 capabilities
Shows how to use different source and sink connectors
"""
import datafactory_cli
from connectors import (
    ODBCSourceConnector, PostgreSQLSourceConnector, MySQLSourceConnector,
    CSVSourceConnector, JSONSourceConnector,
    SQLiteSinkConnector, PostgreSQLSinkConnector, MySQLSinkConnector,
    CSVSinkConnector, JSONSinkConnector
)


# Example 1: ODBC (SQL Server) to SQLite (Original functionality)
def example_odbc_to_sqlite():
    """Copy data from SQL Server via ODBC to SQLite database"""
    print("Example 1: ODBC to SQLite")
    
    # Create source and sink connectors
    source = ODBCSourceConnector(dsn='Dev', database='Omnia_D365_DW', schema='Jandre')
    sink = SQLiteSinkConnector(database_path='data.db')
    
    # Create DataFactory instance
    app = datafactory_cli.DataFactory(source_connector=source, sink_connector=sink)
    
    # Get tables from source
    tables = app.get_tables()
    tables_list = tables['TABLE_NAME'].tolist()
    
    # Copy data
    for table in tables_list[:5]:  # Limit to first 5 tables for example
        print(f"Processing table: {table}")
        df = app.get_data(f"SELECT TOP 10 * FROM {table}")
        app.write_data(df, table)
    
    app.close_connections()
    print("Complete!\n")


# Example 2: PostgreSQL to MySQL
def example_postgresql_to_mysql():
    """Copy data from PostgreSQL to MySQL database"""
    print("Example 2: PostgreSQL to MySQL")
    
    # Create source and sink connectors
    source = PostgreSQLSourceConnector(
        host='localhost',
        port=5432,
        database='source_db',
        user='postgres',
        password='password',
        schema='public'
    )
    
    sink = MySQLSinkConnector(
        host='localhost',
        port=3306,
        database='target_db',
        user='root',
        password='password'
    )
    
    # Create DataFactory instance
    app = datafactory_cli.DataFactory(source_connector=source, sink_connector=sink)
    
    # Get tables from source
    tables = app.get_tables()
    tables_list = tables['table_name'].tolist()
    
    # Copy data
    for table in tables_list:
        print(f"Processing table: {table}")
        df = app.get_data(f"SELECT * FROM {table}")
        app.write_data(df, table)
    
    app.close_connections()
    print("Complete!\n")


# Example 3: CSV to SQLite
def example_csv_to_sqlite():
    """Load data from CSV file to SQLite database"""
    print("Example 3: CSV to SQLite")
    
    # Create source and sink connectors
    source = CSVSourceConnector(file_path='data/source_data.csv')
    sink = SQLiteSinkConnector(database_path='data.db')
    
    # Create DataFactory instance
    app = datafactory_cli.DataFactory(source_connector=source, sink_connector=sink)
    
    # Get data from CSV
    df = app.get_data()
    
    # Write to SQLite
    app.write_data(df, 'imported_csv_data')
    
    app.close_connections()
    print("Complete!\n")


# Example 4: PostgreSQL to CSV
def example_postgresql_to_csv():
    """Export data from PostgreSQL to CSV files"""
    print("Example 4: PostgreSQL to CSV")
    
    # Create source and sink connectors
    source = PostgreSQLSourceConnector(
        host='localhost',
        port=5432,
        database='source_db',
        user='postgres',
        password='password',
        schema='public'
    )
    
    sink = CSVSinkConnector(directory='output_csv')
    
    # Create DataFactory instance
    app = datafactory_cli.DataFactory(source_connector=source, sink_connector=sink)
    
    # Get tables from source
    tables = app.get_tables()
    tables_list = tables['table_name'].tolist()
    
    # Export each table to CSV
    for table in tables_list:
        print(f"Exporting table: {table}")
        df = app.get_data(f"SELECT * FROM {table}")
        app.write_data(df, table)
    
    app.close_connections()
    print("Complete!\n")


# Example 5: JSON to PostgreSQL
def example_json_to_postgresql():
    """Load data from JSON file to PostgreSQL database"""
    print("Example 5: JSON to PostgreSQL")
    
    # Create source and sink connectors
    source = JSONSourceConnector(file_path='data/source_data.json')
    
    sink = PostgreSQLSinkConnector(
        host='localhost',
        port=5432,
        database='target_db',
        user='postgres',
        password='password'
    )
    
    # Create DataFactory instance
    app = datafactory_cli.DataFactory(source_connector=source, sink_connector=sink)
    
    # Get data from JSON
    df = app.get_data()
    
    # Write to PostgreSQL
    app.write_data(df, 'imported_json_data')
    
    app.close_connections()
    print("Complete!\n")


# Example 6: MySQL to JSON
def example_mysql_to_json():
    """Export data from MySQL to JSON files"""
    print("Example 6: MySQL to JSON")
    
    # Create source and sink connectors
    source = MySQLSourceConnector(
        host='localhost',
        port=3306,
        database='source_db',
        user='root',
        password='password'
    )
    
    sink = JSONSinkConnector(directory='output_json')
    
    # Create DataFactory instance
    app = datafactory_cli.DataFactory(source_connector=source, sink_connector=sink)
    
    # Get tables from source
    tables = app.get_tables()
    tables_list = tables['table_name'].tolist()
    
    # Export each table to JSON
    for table in tables_list:
        print(f"Exporting table: {table}")
        df = app.get_data(f"SELECT * FROM {table}")
        app.write_data(df, table)
    
    app.close_connections()
    print("Complete!\n")


# Example 7: Legacy compatibility mode
def example_legacy_mode():
    """Using legacy interface for backward compatibility"""
    print("Example 7: Legacy Mode (Backward Compatibility)")
    
    # Using the old-style constructor (still works!)
    app = datafactory_cli.LegacyDataFactory(
        source_conn='Dev',
        sink_conn='sqlite:///data.db',
        database='Omnia_D365_DW',
        schema='Jandre'
    )
    
    # Get tables from source
    tables = app.get_tables()
    tables_list = tables['TABLE_NAME'].tolist()
    
    # Copy data using old interface
    for table in tables_list[:3]:  # Limit to first 3 tables
        print(f"Processing table: {table}")
        df = app.get_data(f"SELECT TOP 10 * FROM {table}")
        app.write_data(df, table)
    
    app.close_connections()
    print("Complete!\n")


if __name__ == '__main__':
    print("DataFactory 2.0 - Examples\n")
    print("=" * 60)
    print("\nUncomment the example you want to run:\n")
    
    # Uncomment the example you want to run:
    # example_odbc_to_sqlite()
    # example_postgresql_to_mysql()
    # example_csv_to_sqlite()
    # example_postgresql_to_csv()
    # example_json_to_postgresql()
    # example_mysql_to_json()
    # example_legacy_mode()
    
    print("Please edit examples.py and uncomment the example you want to run.")
