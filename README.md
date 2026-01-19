#  DataFactory 2.0

DataFactory 2.0 is a flexible data integration tool inspired by Microsoft Azure Data Factory. It provides a simple way to copy data between different data sources and destinations using a connector-based architecture.

## Features

- **Multiple Source Connectors**: Connect to various data sources
  - ODBC (SQL Server, etc.)
  - PostgreSQL
  - MySQL
  - CSV files
  - JSON files
  - **Excel files** (NEW in 2.0)

- **Multiple Sink Connectors**: Write data to various destinations
  - SQLite
  - PostgreSQL
  - MySQL
  - CSV files
  - JSON files
  - **Parquet files** (NEW in 2.0)

- **REST API**: Full-featured API for job management and monitoring
  - Create, read, update, delete jobs
  - Execute jobs on-demand or on schedule
  - View job history and logs
  - Monitor scheduled jobs

- **Job Management**: Define and manage data integration jobs
  - Store job definitions in SQLite
  - Track execution history
  - Detailed logging for each execution
  - Schedule jobs with cron expressions

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

### Quick Start with API (Recommended)

Start the API server:

```bash
python api.py
```

The API will be available at `http://localhost:8000`. Visit `http://localhost:8000/docs` for interactive API documentation.

### New Connector-Based Approach

#### Excel to Parquet

```python
import datafactory_cli
from connectors import ExcelSourceConnector, ParquetSinkConnector

# Create source and sink connectors
source = ExcelSourceConnector(file_path='data.xlsx', sheet_name='Sheet1')
sink = ParquetSinkConnector(directory='output_parquet')

# Create DataFactory instance
app = datafactory_cli.DataFactory(source_connector=source, sink_connector=sink)

# Get and transfer data
df = app.get_data()
app.write_data(df, 'output_data')
app.close_connections()
```

#### Database to Parquet

```python
from connectors import PostgreSQLSourceConnector, ParquetSinkConnector

source = PostgreSQLSourceConnector(
    host='localhost', port=5432, database='mydb',
    user='user', password='pass', schema='public'
)
sink = ParquetSinkConnector(directory='output_parquet')

app = datafactory_cli.DataFactory(source_connector=source, sink_connector=sink)
tables = app.get_tables()
for table in tables['table_name'].tolist():
    df = app.get_data(f"SELECT * FROM {table}")
    app.write_data(df, table)
app.close_connections()
```

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
- **ExcelSourceConnector**: Read data from Excel files (.xlsx, .xls)

### Sink Connectors

- **SQLiteSinkConnector**: Write to SQLite databases
- **PostgreSQLSinkConnector**: Write to PostgreSQL databases
- **MySQLSinkConnector**: Write to MySQL databases
- **CSVSinkConnector**: Write to CSV files
- **JSONSinkConnector**: Write to JSON files
- **ParquetSinkConnector**: Write to Parquet files

## API Usage

DataFactory 2.0 provides a comprehensive REST API for managing data integration jobs.

### Starting the API

```bash
python api.py
```

The API runs on `http://localhost:8000` by default. Interactive documentation is available at `http://localhost:8000/docs`.

### API Endpoints

#### Job Management

- `POST /jobs` - Create a new job
- `GET /jobs` - List all jobs with last run information
- `GET /jobs/{job_id}` - Get job details
- `PUT /jobs/{job_id}` - Update job configuration
- `DELETE /jobs/{job_id}` - Delete a job

#### Job Execution

- `POST /jobs/{job_id}/execute` - Execute a job immediately

#### History & Logs

- `GET /history` - Get execution history for all jobs
- `GET /jobs/{job_id}/history` - Get execution history for a specific job
- `GET /logs/{history_id}` - Get logs for a specific execution

#### Scheduler

- `GET /scheduler/jobs` - List scheduled jobs
- `POST /scheduler/pause` - Pause the scheduler
- `POST /scheduler/resume` - Resume the scheduler

#### System

- `GET /health` - Health check
- `GET /connectors` - List available connectors

### Example: Creating a Job via API

```python
import requests

job_data = {
    "job_name": "excel_to_parquet",
    "source_type": "excel",
    "source_config": {
        "file_path": "/path/to/data.xlsx",
        "sheet_name": "Sheet1"
    },
    "sink_type": "parquet",
    "sink_config": {
        "directory": "/path/to/output"
    },
    "schedule": "0 2 * * *"  # Run daily at 2 AM
}

response = requests.post("http://localhost:8000/jobs", json=job_data)
print(response.json())
```

### Example: Executing a Job

```python
import requests

job_id = 1
response = requests.post(f"http://localhost:8000/jobs/{job_id}/execute")
result = response.json()
print(f"Status: {result['status']}")
print(f"Records processed: {result['records_processed']}")
```

### Job Scheduling

Jobs can be scheduled using cron expressions. For example:

- `"0 2 * * *"` - Run daily at 2 AM
- `"0 */4 * * *"` - Run every 4 hours
- `"0 0 * * 0"` - Run weekly on Sunday at midnight
- `"0 0 1 * *"` - Run monthly on the 1st at midnight

## Job Management System

DataFactory 2.0 includes a complete job management system with:

- **Job Definitions**: Store source, sink, and configuration
- **Execution History**: Track every job execution
- **Detailed Logging**: Log all operations to SQLite
- **Scheduling**: Run jobs on a schedule using cron expressions
- **Status Tracking**: Monitor job success/failure and record counts

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
- openpyxl: Excel file support
- pyarrow: Parquet file support
- fastapi: REST API framework
- uvicorn: ASGI server
- APScheduler: Job scheduling

## Examples

See `examples.py` for comprehensive examples demonstrating all connector combinations.

## Contributing

Feel free to add new connectors by extending the `SourceConnector` or `SinkConnector` abstract base classes in `connectors.py`.

