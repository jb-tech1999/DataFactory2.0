# DataFactory 2.0 - Changelog

## Version 2.0 - Multi-Source and Multi-Sink Support

### New Features

#### Multiple Source Connectors
- **ODBCSourceConnector**: Connect to SQL Server and other ODBC data sources (original functionality, refactored)
- **PostgreSQLSourceConnector**: Connect to PostgreSQL databases
- **MySQLSourceConnector**: Connect to MySQL/MariaDB databases
- **CSVSourceConnector**: Read data from CSV files
- **JSONSourceConnector**: Read data from JSON files

#### Multiple Sink Connectors
- **SQLiteSinkConnector**: Write to SQLite databases (original functionality, refactored)
- **PostgreSQLSinkConnector**: Write to PostgreSQL databases
- **MySQLSinkConnector**: Write to MySQL/MariaDB databases
- **CSVSinkConnector**: Write to CSV files
- **JSONSinkConnector**: Write to JSON files

### Architecture Improvements
- Implemented connector-based architecture with abstract base classes
- Separated source and sink logic into independent connector classes
- Maintained backward compatibility through `LegacyDataFactory` wrapper
- Improved code organization and maintainability

### Security Enhancements
- Updated pymysql to 1.1.1 to fix SQL injection vulnerability
- Added URL encoding for database passwords to handle special characters
- Passed CodeQL security scan with no issues

### Code Quality
- Added comprehensive examples demonstrating all connector combinations
- Created test suite for validating functionality
- Added .gitignore for better repository management
- Improved code documentation and comments

### Breaking Changes
None - Full backward compatibility maintained via `LegacyDataFactory` class

### Migration Guide

#### Old Approach (Still Supported)
```python
import datafactory_cli

app = datafactory_cli.LegacyDataFactory(
    source_conn='Dev',
    sink_conn='sqlite:///data.db',
    database='MyDatabase'
)
```

#### New Recommended Approach
```python
import datafactory_cli
from connectors import ODBCSourceConnector, SQLiteSinkConnector

source = ODBCSourceConnector(dsn='Dev', database='MyDatabase', schema='dbo')
sink = SQLiteSinkConnector(database_path='data.db')
app = datafactory_cli.DataFactory(source_connector=source, sink_connector=sink)
```

### Dependencies Added
- pymysql==1.1.1 (MySQL support)
- psycopg2-binary==2.9.9 (PostgreSQL support)

### Files Added
- `connectors.py`: All source and sink connector implementations
- `examples.py`: Comprehensive examples for all connector combinations
- `test_datafactory.py`: Test suite
- `.gitignore`: Git ignore rules
- `CHANGELOG.md`: This file

### Files Modified
- `datafactory_cli.py`: Refactored to use connector pattern
- `app.py`: Updated to demonstrate new connector-based approach
- `README.md`: Comprehensive documentation of new features
- `requirements.txt`: Added new dependencies and fixed encoding
