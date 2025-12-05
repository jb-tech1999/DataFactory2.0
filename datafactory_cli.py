import pandas as pd
import pyodbc
import sqlalchemy
from connectors import (
    SourceConnector, SinkConnector,
    ODBCSourceConnector, PostgreSQLSourceConnector, MySQLSourceConnector,
    CSVSourceConnector, JSONSourceConnector,
    SQLiteSinkConnector, PostgreSQLSinkConnector, MySQLSinkConnector,
    CSVSinkConnector, JSONSinkConnector
)


class DataFactory:
    def __init__(self, source_connector: SourceConnector, sink_connector: SinkConnector):
        """
        Initialize DataFactory with source and sink connectors
        
        Args:
            source_connector: A SourceConnector instance for reading data
            sink_connector: A SinkConnector instance for writing data
        """
        self.source_connector = source_connector
        self.sink_connector = sink_connector
        
        # Connect to source and sink
        self.source_connector.connect()
        self.sink_connector.connect()
        
    def get_data(self, query: str = None) -> pd.DataFrame:
        """Get data from source using a query (optional for file-based sources)"""
        return self.source_connector.get_data(query)
    
    def get_tables(self) -> pd.DataFrame:
        """Get all tables from source"""
        return self.source_connector.get_tables()
    
    def write_data(self, df: pd.DataFrame, table_name: str):
        """Write data to sink"""
        self.sink_connector.write_data(df, table_name)
        
    def close_connections(self):
        """Close all connections"""
        self.source_connector.close()
        self.sink_connector.close()


# Legacy compatibility: maintain backward compatibility with old interface
class LegacyDataFactory:
    def __init__(self, source_conn, sink_conn, database, schema='Jandre'):
        """
        Legacy constructor for backward compatibility
        
        Args:
            source_conn: DSN name for ODBC connection
            sink_conn: SQLAlchemy connection string
            database: Database name
            schema: Schema name (default: 'Jandre')
        """
        # Create connectors using legacy parameters
        source = ODBCSourceConnector(dsn=source_conn, database=database, schema=schema)
        
        # Determine sink type from connection string
        if sink_conn.startswith('sqlite:'):
            # Extract database path from sqlite connection string
            db_path = sink_conn.replace('sqlite:///', '')
            sink = SQLiteSinkConnector(database_path=db_path)
        else:
            # For other connection strings, we'll need to parse them
            # For now, default to SQLite
            db_path = sink_conn.replace('sqlite:///', '')
            sink = SQLiteSinkConnector(database_path=db_path)
        
        # Initialize with new connector-based approach
        self.df = DataFactory(source_connector=source, sink_connector=sink)
        
    def get_data(self, query):
        return self.df.get_data(query)
    
    def get_tables(self):
        return self.df.get_tables()
    
    def write_data(self, df, table_name):
        self.df.write_data(df, table_name)
        
    def close_connections(self):
        self.df.close_connections()
        