"""
DataFactory 2.0 - Main Application
This example demonstrates the new connector-based approach
"""
import datafactory_cli
from connectors import ODBCSourceConnector, SQLiteSinkConnector


# Option 1: New connector-based approach (Recommended)
print("Using new connector-based approach...")

# Create source connector (ODBC for SQL Server)
source = ODBCSourceConnector(
    dsn='Dev',
    database='Omnia_D365_DW',
    schema='Jandre'  # Change this to your schema
)

# Create sink connector (SQLite database)
sink = SQLiteSinkConnector(database_path='data.db')

# Create DataFactory instance with connectors
app = datafactory_cli.DataFactory(source_connector=source, sink_connector=sink)

# Get tables from source
tables = app.get_tables()
tables_list = tables['TABLE_NAME'].tolist()

# Copy data from source to sink
for table in tables_list:
    print(f"Processing table: {table}")
    df = app.get_data(f"SELECT TOP 10 * FROM {table}")
    app.write_data(df, table)

# Close all connections
app.close_connections()

print("Data transfer complete!")


# Option 2: Legacy approach (for backward compatibility)
# Uncomment below to use the old interface:
#
# app = datafactory_cli.LegacyDataFactory(
#     source_conn='Dev',
#     sink_conn='sqlite:///data.db',
#     database='Omnia_D365_DW',
#     schema='Jandre'
# )
#
# tables = app.get_tables()
# tables_list = tables['TABLE_NAME'].tolist()
#
# for table in tables_list:
#     print(table)
#     df = app.get_data("SELECT TOP 10 * FROM " + table)
#     app.write_data(df, table)
#
# app.close_connections()