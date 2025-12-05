"""
Connectors module for DataFactory
Provides source and sink connectors for various data systems
"""
import pandas as pd
import pyodbc
import sqlalchemy
from abc import ABC, abstractmethod
from typing import Optional


class SourceConnector(ABC):
    """Abstract base class for source connectors"""
    
    @abstractmethod
    def connect(self):
        """Establish connection to the source"""
        pass
    
    @abstractmethod
    def get_data(self, query: str) -> pd.DataFrame:
        """Get data from source using a query"""
        pass
    
    @abstractmethod
    def get_tables(self) -> pd.DataFrame:
        """Get list of tables from source"""
        pass
    
    @abstractmethod
    def close(self):
        """Close the connection"""
        pass


class SinkConnector(ABC):
    """Abstract base class for sink connectors"""
    
    @abstractmethod
    def connect(self):
        """Establish connection to the sink"""
        pass
    
    @abstractmethod
    def write_data(self, df: pd.DataFrame, table_name: str):
        """Write data to sink"""
        pass
    
    @abstractmethod
    def close(self):
        """Close the connection"""
        pass


# Source Connectors

class ODBCSourceConnector(SourceConnector):
    """ODBC source connector for SQL Server and other ODBC sources"""
    
    def __init__(self, dsn: str, database: str, schema: str = 'dbo'):
        self.dsn = dsn
        self.database = database
        self.schema = schema
        self.connection = None
    
    def connect(self):
        """Establish ODBC connection"""
        self.connection = pyodbc.connect(f'DSN={self.dsn};DATABASE={self.database};')
    
    def get_data(self, query: str) -> pd.DataFrame:
        """Get data from ODBC source"""
        return pd.read_sql(query, self.connection)
    
    def get_tables(self) -> pd.DataFrame:
        """Get all tables from ODBC source"""
        query = f"SELECT * FROM information_schema.tables WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = '{self.schema}'"
        return pd.read_sql(query, self.connection)
    
    def close(self):
        """Close ODBC connection"""
        if self.connection:
            self.connection.close()


class PostgreSQLSourceConnector(SourceConnector):
    """PostgreSQL source connector"""
    
    def __init__(self, host: str, port: int, database: str, user: str, password: str, schema: str = 'public'):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.schema = schema
        self.engine = None
        self.connection = None
    
    def connect(self):
        """Establish PostgreSQL connection"""
        connection_string = f'postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}'
        self.engine = sqlalchemy.create_engine(connection_string)
        self.connection = self.engine.connect()
    
    def get_data(self, query: str) -> pd.DataFrame:
        """Get data from PostgreSQL"""
        return pd.read_sql(query, self.connection)
    
    def get_tables(self) -> pd.DataFrame:
        """Get all tables from PostgreSQL"""
        query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{self.schema}' AND table_type = 'BASE TABLE'"
        return pd.read_sql(query, self.connection)
    
    def close(self):
        """Close PostgreSQL connection"""
        if self.connection:
            self.connection.close()
        if self.engine:
            self.engine.dispose()


class MySQLSourceConnector(SourceConnector):
    """MySQL source connector"""
    
    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.engine = None
        self.connection = None
    
    def connect(self):
        """Establish MySQL connection"""
        connection_string = f'mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}'
        self.engine = sqlalchemy.create_engine(connection_string)
        self.connection = self.engine.connect()
    
    def get_data(self, query: str) -> pd.DataFrame:
        """Get data from MySQL"""
        return pd.read_sql(query, self.connection)
    
    def get_tables(self) -> pd.DataFrame:
        """Get all tables from MySQL"""
        query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{self.database}' AND table_type = 'BASE TABLE'"
        return pd.read_sql(query, self.connection)
    
    def close(self):
        """Close MySQL connection"""
        if self.connection:
            self.connection.close()
        if self.engine:
            self.engine.dispose()


class CSVSourceConnector(SourceConnector):
    """CSV file source connector"""
    
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.df = None
    
    def connect(self):
        """Load CSV file"""
        self.df = pd.read_csv(self.file_path)
    
    def get_data(self, query: str = None) -> pd.DataFrame:
        """Get data from CSV (query parameter not used for CSV)"""
        return self.df
    
    def get_tables(self) -> pd.DataFrame:
        """Get table information (returns file name as table)"""
        import os
        table_name = os.path.basename(self.file_path)
        return pd.DataFrame({'TABLE_NAME': [table_name]})
    
    def close(self):
        """Close CSV connector (no-op for CSV)"""
        pass


class JSONSourceConnector(SourceConnector):
    """JSON file source connector"""
    
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.df = None
    
    def connect(self):
        """Load JSON file"""
        self.df = pd.read_json(self.file_path)
    
    def get_data(self, query: str = None) -> pd.DataFrame:
        """Get data from JSON (query parameter not used for JSON)"""
        return self.df
    
    def get_tables(self) -> pd.DataFrame:
        """Get table information (returns file name as table)"""
        import os
        table_name = os.path.basename(self.file_path)
        return pd.DataFrame({'TABLE_NAME': [table_name]})
    
    def close(self):
        """Close JSON connector (no-op for JSON)"""
        pass


# Sink Connectors

class SQLiteSinkConnector(SinkConnector):
    """SQLite sink connector"""
    
    def __init__(self, database_path: str):
        self.database_path = database_path
        self.engine = None
    
    def connect(self):
        """Establish SQLite connection"""
        self.engine = sqlalchemy.create_engine(f'sqlite:///{self.database_path}')
    
    def write_data(self, df: pd.DataFrame, table_name: str):
        """Write data to SQLite"""
        df.to_sql(table_name, self.engine, if_exists='replace', index=False)
    
    def close(self):
        """Close SQLite connection"""
        if self.engine:
            self.engine.dispose()


class PostgreSQLSinkConnector(SinkConnector):
    """PostgreSQL sink connector"""
    
    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.engine = None
    
    def connect(self):
        """Establish PostgreSQL connection"""
        connection_string = f'postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}'
        self.engine = sqlalchemy.create_engine(connection_string)
    
    def write_data(self, df: pd.DataFrame, table_name: str):
        """Write data to PostgreSQL"""
        df.to_sql(table_name, self.engine, if_exists='replace', index=False)
    
    def close(self):
        """Close PostgreSQL connection"""
        if self.engine:
            self.engine.dispose()


class MySQLSinkConnector(SinkConnector):
    """MySQL sink connector"""
    
    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.engine = None
    
    def connect(self):
        """Establish MySQL connection"""
        connection_string = f'mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}'
        self.engine = sqlalchemy.create_engine(connection_string)
    
    def write_data(self, df: pd.DataFrame, table_name: str):
        """Write data to MySQL"""
        df.to_sql(table_name, self.engine, if_exists='replace', index=False)
    
    def close(self):
        """Close MySQL connection"""
        if self.engine:
            self.engine.dispose()


class CSVSinkConnector(SinkConnector):
    """CSV file sink connector"""
    
    def __init__(self, directory: str):
        self.directory = directory
    
    def connect(self):
        """Ensure directory exists"""
        import os
        os.makedirs(self.directory, exist_ok=True)
    
    def write_data(self, df: pd.DataFrame, table_name: str):
        """Write data to CSV file"""
        import os
        file_path = os.path.join(self.directory, f'{table_name}.csv')
        df.to_csv(file_path, index=False)
    
    def close(self):
        """Close CSV connector (no-op for CSV)"""
        pass


class JSONSinkConnector(SinkConnector):
    """JSON file sink connector"""
    
    def __init__(self, directory: str):
        self.directory = directory
    
    def connect(self):
        """Ensure directory exists"""
        import os
        os.makedirs(self.directory, exist_ok=True)
    
    def write_data(self, df: pd.DataFrame, table_name: str):
        """Write data to JSON file"""
        import os
        file_path = os.path.join(self.directory, f'{table_name}.json')
        df.to_json(file_path, orient='records', indent=2)
    
    def close(self):
        """Close JSON connector (no-op for JSON)"""
        pass
