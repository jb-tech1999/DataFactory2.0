# DataFactory 2.0 - Quick Start Guide

## Installation

1. Clone the repository:
```bash
git clone https://github.com/jb-tech1999/DataFactory2.0
cd DataFactory2.0
```

2. Create and activate virtual environment:

**Windows:**
```bash
python -m venv venv
venv\Scripts\activate
```

**Linux/Mac:**
```bash
python -m venv venv
source venv/bin/activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Quick Start: Using the API

### 1. Start the API Server

```bash
python api.py
```

The API will be available at `http://localhost:8000`

### 2. View Interactive Documentation

Open your browser and visit:
- API Documentation: `http://localhost:8000/docs`
- Alternative Documentation: `http://localhost:8000/redoc`

### 3. Create Your First Job

**Using curl:**

```bash
curl -X POST "http://localhost:8000/jobs" \
  -H "Content-Type: application/json" \
  -d '{
    "job_name": "my_first_job",
    "source_type": "excel",
    "source_config": {
      "file_path": "/path/to/data.xlsx",
      "sheet_name": "Sheet1"
    },
    "sink_type": "parquet",
    "sink_config": {
      "directory": "/path/to/output"
    }
  }'
```

**Using Python:**

```python
import requests

job_data = {
    "job_name": "my_first_job",
    "source_type": "excel",
    "source_config": {
        "file_path": "/path/to/data.xlsx",
        "sheet_name": "Sheet1"
    },
    "sink_type": "parquet",
    "sink_config": {
        "directory": "/path/to/output"
    }
}

response = requests.post("http://localhost:8000/jobs", json=job_data)
job_id = response.json()["job_id"]
print(f"Created job: {job_id}")
```

### 4. Execute Your Job

```bash
curl -X POST "http://localhost:8000/jobs/1/execute"
```

Or in Python:

```python
response = requests.post("http://localhost:8000/jobs/1/execute")
result = response.json()
print(f"Status: {result['status']}")
print(f"Records: {result['records_processed']}")
```

### 5. View Job History

```bash
curl "http://localhost:8000/jobs/1/history"
```

### 6. View Logs

```bash
curl "http://localhost:8000/logs/1"
```

## Quick Start: Using Python Directly

### Example 1: Excel to Parquet

```python
import datafactory_cli
from connectors import ExcelSourceConnector, ParquetSinkConnector

# Create connectors
source = ExcelSourceConnector(file_path='data.xlsx', sheet_name='Sheet1')
sink = ParquetSinkConnector(directory='output')

# Create DataFactory and transfer data
app = datafactory_cli.DataFactory(source_connector=source, sink_connector=sink)
df = app.get_data()
app.write_data(df, 'output_data')
app.close_connections()

print(f"Transferred {len(df)} rows")
```

### Example 2: CSV to Parquet

```python
from connectors import CSVSourceConnector, ParquetSinkConnector

source = CSVSourceConnector(file_path='data.csv')
sink = ParquetSinkConnector(directory='output')

app = datafactory_cli.DataFactory(source_connector=source, sink_connector=sink)
df = app.get_data()
app.write_data(df, 'data')
app.close_connections()
```

### Example 3: Database to Parquet

```python
from connectors import PostgreSQLSourceConnector, ParquetSinkConnector

source = PostgreSQLSourceConnector(
    host='localhost',
    port=5432,
    database='mydb',
    user='user',
    password='password',
    schema='public'
)
sink = ParquetSinkConnector(directory='backup')

app = datafactory_cli.DataFactory(source_connector=source, sink_connector=sink)
df = app.get_data("SELECT * FROM my_table")
app.write_data(df, 'my_table_backup')
app.close_connections()
```

## Job Scheduling

You can schedule jobs to run automatically using cron expressions:

```python
import requests

job_data = {
    "job_name": "daily_backup",
    "source_type": "excel",
    "source_config": {"file_path": "/data/daily.xlsx"},
    "sink_type": "parquet",
    "sink_config": {"directory": "/backup"},
    "schedule": "0 2 * * *"  # Run daily at 2 AM
}

response = requests.post("http://localhost:8000/jobs", json=job_data)
```

### Common Cron Expressions

- `"0 * * * *"` - Every hour
- `"0 */4 * * *"` - Every 4 hours
- `"0 2 * * *"` - Daily at 2 AM
- `"0 0 * * 0"` - Weekly on Sunday
- `"0 0 1 * *"` - Monthly on the 1st

## Available Connectors

### Sources
- **excel**: Excel files (.xlsx, .xls)
- **csv**: CSV files
- **json**: JSON files
- **odbc**: ODBC data sources (SQL Server, etc.)
- **postgresql**: PostgreSQL databases
- **mysql**: MySQL databases

### Sinks
- **parquet**: Parquet files (efficient columnar storage)
- **csv**: CSV files
- **json**: JSON files
- **sqlite**: SQLite databases
- **postgresql**: PostgreSQL databases
- **mysql**: MySQL databases

## Testing

Run the test suites to verify everything works:

```bash
# Test original connectors
python test_datafactory.py

# Test new connectors (Excel, Parquet)
python test_new_connectors.py

# Test job management
python test_job_manager.py

# Test API (requires API to be running)
python api.py &  # Start API in background
python test_api.py
```

## Next Steps

1. Check out `examples_new_features.py` for more detailed examples
2. Read the full README.md for comprehensive documentation
3. Explore the interactive API docs at `http://localhost:8000/docs`
4. Create your own custom connectors by extending `SourceConnector` or `SinkConnector`

## Troubleshooting

### Port Already in Use
If port 8000 is already in use, you can change it:

```python
# In api.py, change the last line:
uvicorn.run(app, host="0.0.0.0", port=8080)  # Use port 8080 instead
```

### Database Locked
If you get "database is locked" errors, make sure only one instance of the API is running.

### Module Not Found
Make sure you've installed all requirements:

```bash
pip install -r requirements.txt
```

## Support

For issues and questions, please visit:
https://github.com/jb-tech1999/DataFactory2.0/issues
