#  DataFactory 2.0

DataFactory 2.0 is a flexible data integration tool inspired by Microsoft Azure Data Factory. It provides a simple way to copy data between different data sources and destinations using a connector-based architecture.

## Features

- **Multiple Source Connectors**: Connect to various data sources
  - ODBC (SQL Server, etc.)
  - PostgreSQL
  - MySQL
  - CSV files
  - JSON files

- **Multiple Sink Connectors**: Write data to various destinations
  - SQLite
  - PostgreSQL
  - MySQL
  - CSV files
  - JSON files

- **Extensible Architecture**: Easy to add new connectors
- **Backward Compatibility**: Maintains compatibility with the original interface

## Setup

In a terminal change to desired folder and run following commands:

```bash
git clone https://github.com/jb-tech1999/DataFactory2.0
```

Change into folder and create a new Python virtual environment:

**Windows:**
```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

**Linux/Mac:**
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Usage

### New Connector-Based Approach (Recommended)

```python
import datafactory_cli
from connectors import ODBCSourceConnector, SQLiteSinkConnector

# Create source and sink connectors
source = ODBCSourceConnector(dsn='Dev', database='MyDatabase', schema='dbo')
sink = SQLiteSinkConnector(database_path='data.db')

# Create DataFactory instance
app = datafactory_cli.DataFactory(source_connector=source, sink_connector=sink)

# Get tables and copy data
tables = app.get_tables()
for table in tables['TABLE_NAME'].tolist():
    df = app.get_data(f"SELECT * FROM {table}")
    app.write_data(df, table)

app.close_connections()
```

### More Examples

#### PostgreSQL to MySQL

```python
from connectors import PostgreSQLSourceConnector, MySQLSinkConnector

source = PostgreSQLSourceConnector(
    host='localhost', port=5432, database='source_db',
    user='postgres', password='password', schema='public'
)
sink = MySQLSinkConnector(
    host='localhost', port=3306, database='target_db',
    user='root', password='password'
)

app = datafactory_cli.DataFactory(source_connector=source, sink_connector=sink)
# ... transfer data ...
app.close_connections()
```

#### CSV to SQLite

```python
from connectors import CSVSourceConnector, SQLiteSinkConnector

source = CSVSourceConnector(file_path='data.csv')
sink = SQLiteSinkConnector(database_path='database.db')

app = datafactory_cli.DataFactory(source_connector=source, sink_connector=sink)
df = app.get_data()
app.write_data(df, 'imported_data')
app.close_connections()
```

#### Database to JSON

```python
from connectors import PostgreSQLSourceConnector, JSONSinkConnector

source = PostgreSQLSourceConnector(
    host='localhost', port=5432, database='mydb',
    user='user', password='pass', schema='public'
)
sink = JSONSinkConnector(directory='output_json')

app = datafactory_cli.DataFactory(source_connector=source, sink_connector=sink)
tables = app.get_tables()
for table in tables['table_name'].tolist():
    df = app.get_data(f"SELECT * FROM {table}")
    app.write_data(df, table)
app.close_connections()
```

### Legacy Compatibility Mode

For backward compatibility with the original interface:

```python
import datafactory_cli

app = datafactory_cli.LegacyDataFactory(
    source_conn='Dev',  # ODBC DSN name
    sink_conn='sqlite:///data.db',
    database='MyDatabase',
    schema='dbo'
)

tables = app.get_tables()
for table in tables['TABLE_NAME'].tolist():
    df = app.get_data(f"SELECT * FROM {table}")
    app.write_data(df, table)

app.close_connections()
```

## Available Connectors

### Source Connectors

- **ODBCSourceConnector**: Connect to any ODBC data source (SQL Server, etc.)
- **PostgreSQLSourceConnector**: Connect to PostgreSQL databases
- **MySQLSourceConnector**: Connect to MySQL databases
- **CSVSourceConnector**: Read data from CSV files
- **JSONSourceConnector**: Read data from JSON files

### Sink Connectors

- **SQLiteSinkConnector**: Write to SQLite databases
- **PostgreSQLSinkConnector**: Write to PostgreSQL databases
- **MySQLSinkConnector**: Write to MySQL databases
- **CSVSinkConnector**: Write to CSV files
- **JSONSinkConnector**: Write to JSON files

## Configuration

### ODBC Setup (Windows)

For ODBC connections, create a DSN connection:
1. Open ODBC Data Source Administrator
2. Create a new System or User DSN
3. Configure with your database details
4. Use the DSN name in your code

### Database Connectors

For PostgreSQL and MySQL, ensure the database server is accessible and you have the correct credentials.

## Requirements

See `requirements.txt` for all dependencies. Key requirements:
- pandas: Data manipulation
- sqlalchemy: Database connections
- pyodbc: ODBC connections
- pymysql: MySQL support
- psycopg2-binary: PostgreSQL support

## Examples

See `examples.py` for comprehensive examples demonstrating all connector combinations.

## Contributing

Feel free to add new connectors by extending the `SourceConnector` or `SinkConnector` abstract base classes in `connectors.py`.

