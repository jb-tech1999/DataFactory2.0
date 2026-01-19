"""
Examples demonstrating DataFactory 2.0 new features
Shows Excel, Parquet, Job Management, and API usage
"""
import datafactory_cli
from connectors import (
    ExcelSourceConnector, ParquetSinkConnector,
    CSVSourceConnector, JSONSourceConnector
)
from job_manager import JobManager
import pandas as pd
import os


# Example 1: Excel to Parquet (New Feature!)
def example_excel_to_parquet():
    """Load data from Excel and save to Parquet format"""
    print("Example 1: Excel to Parquet")
    
    # Create source and sink connectors
    source = ExcelSourceConnector(
        file_path='data/sales_data.xlsx',
        sheet_name='Sales'  # Optional: specify sheet name
    )
    sink = ParquetSinkConnector(directory='output_parquet')
    
    # Create DataFactory instance
    app = datafactory_cli.DataFactory(source_connector=source, sink_connector=sink)
    
    # Get data from Excel
    df = app.get_data()
    print(f"Loaded {len(df)} rows from Excel")
    
    # Write to Parquet
    app.write_data(df, 'sales_data')
    print("Data written to Parquet format")
    
    app.close_connections()
    print("Complete!\n")


# Example 2: Multiple Excel Sheets to Parquet
def example_excel_sheets_to_parquet():
    """Process multiple sheets from an Excel file"""
    print("Example 2: Multiple Excel Sheets to Parquet")
    
    source = ExcelSourceConnector(file_path='data/financial_report.xlsx')
    sink = ParquetSinkConnector(directory='output_parquet')
    
    app = datafactory_cli.DataFactory(source_connector=source, sink_connector=sink)
    
    # Get all sheet names
    sheets = app.get_tables()
    print(f"Found {len(sheets)} sheets")
    
    # Process each sheet
    for sheet_name in sheets['TABLE_NAME'].tolist():
        print(f"Processing sheet: {sheet_name}")
        
        # Create new source for this specific sheet
        sheet_source = ExcelSourceConnector(
            file_path='data/financial_report.xlsx',
            sheet_name=sheet_name
        )
        sheet_source.connect()
        
        # Get and write data
        df = sheet_source.get_data()
        sink.write_data(df, sheet_name)
        
        sheet_source.close()
        print(f"  Saved {len(df)} rows")
    
    app.close_connections()
    print("Complete!\n")


# Example 3: Database to Parquet (Efficient Storage)
def example_database_to_parquet():
    """Export database tables to efficient Parquet format"""
    print("Example 3: Database to Parquet")
    
    from connectors import PostgreSQLSourceConnector
    
    source = PostgreSQLSourceConnector(
        host='localhost',
        port=5432,
        database='analytics_db',
        user='user',
        password='password',
        schema='public'
    )
    
    sink = ParquetSinkConnector(directory='backup_parquet')
    
    app = datafactory_cli.DataFactory(source_connector=source, sink_connector=sink)
    
    # Get all tables
    tables = app.get_tables()
    
    # Export each table to Parquet
    for table in tables['table_name'].tolist():
        print(f"Exporting table: {table}")
        df = app.get_data(f"SELECT * FROM {table}")
        app.write_data(df, table)
        print(f"  Exported {len(df)} rows")
    
    app.close_connections()
    print("Complete!\n")


# Example 4: Using Job Manager Programmatically
def example_job_manager():
    """Create and execute jobs using JobManager"""
    print("Example 4: Job Manager")
    
    # Initialize job manager
    jm = JobManager(db_path='jobs.db')
    
    # Create a job
    job_id = jm.create_job(
        job_name='daily_excel_export',
        source_type='excel',
        source_config={
            'file_path': '/data/daily_report.xlsx',
            'sheet_name': 'Data'
        },
        sink_type='parquet',
        sink_config={
            'directory': '/output/parquet'
        },
        schedule='0 2 * * *'  # Run daily at 2 AM
    )
    
    print(f"Created job with ID: {job_id}")
    
    # List all jobs
    jobs = jm.list_jobs()
    print(f"Total jobs: {len(jobs)}")
    
    # Execute a job
    result = jm.execute_job(job_id)
    if result['status'] == 'success':
        print(f"Job executed successfully!")
        print(f"Records processed: {result['records_processed']}")
    else:
        print(f"Job failed: {result.get('error')}")
    
    # Get job history
    history = jm.get_job_history(job_id)
    print(f"Job has been executed {len(history)} time(s)")
    
    # Get logs for last execution
    if history:
        logs = jm.get_job_logs(history[0]['history_id'])
        print(f"Logs for last execution ({len(logs)} entries):")
        for log in logs[:5]:  # Show first 5 logs
            print(f"  [{log['level']}] {log['message']}")
    
    print("Complete!\n")


# Example 5: Using the API with requests
def example_api_usage():
    """Demonstrate API usage with Python requests library"""
    print("Example 5: API Usage")
    print("Note: Make sure the API is running (python api.py)")
    
    import requests
    
    api_url = "http://localhost:8000"
    
    # Create a job via API
    job_data = {
        "job_name": "api_excel_job",
        "source_type": "excel",
        "source_config": {
            "file_path": "/data/source.xlsx",
            "sheet_name": "Sheet1"
        },
        "sink_type": "parquet",
        "sink_config": {
            "directory": "/output"
        }
    }
    
    response = requests.post(f"{api_url}/jobs", json=job_data)
    if response.status_code == 201:
        result = response.json()
        job_id = result['job_id']
        print(f"Job created with ID: {job_id}")
        
        # Execute the job
        exec_response = requests.post(f"{api_url}/jobs/{job_id}/execute")
        if exec_response.status_code == 200:
            exec_result = exec_response.json()
            print(f"Job executed: {exec_result['status']}")
            print(f"Records: {exec_result.get('records_processed', 0)}")
        
        # Get job history
        history_response = requests.get(f"{api_url}/jobs/{job_id}/history")
        if history_response.status_code == 200:
            history = history_response.json()
            print(f"Job history: {history['count']} execution(s)")
    
    print("Complete!\n")


# Example 6: Scheduled Jobs
def example_scheduled_jobs():
    """Create jobs that run on a schedule"""
    print("Example 6: Scheduled Jobs")
    
    jm = JobManager(db_path='jobs.db')
    
    # Job 1: Daily backup at 2 AM
    jm.create_job(
        job_name='daily_backup',
        source_type='excel',
        source_config={'file_path': '/data/daily.xlsx'},
        sink_type='parquet',
        sink_config={'directory': '/backup'},
        schedule='0 2 * * *'
    )
    
    # Job 2: Hourly sync
    jm.create_job(
        job_name='hourly_sync',
        source_type='csv',
        source_config={'file_path': '/data/live.csv'},
        sink_type='parquet',
        sink_config={'directory': '/sync'},
        schedule='0 * * * *'
    )
    
    # Job 3: Weekly report
    jm.create_job(
        job_name='weekly_report',
        source_type='excel',
        source_config={'file_path': '/data/weekly.xlsx'},
        sink_type='parquet',
        sink_config={'directory': '/reports'},
        schedule='0 0 * * 0'  # Sunday at midnight
    )
    
    print("Created 3 scheduled jobs")
    print("Jobs will run automatically when the API is running")
    print("Complete!\n")


# Example 7: Monitoring and Logging
def example_monitoring():
    """Monitor job executions and view logs"""
    print("Example 7: Monitoring and Logging")
    
    jm = JobManager(db_path='jobs.db')
    
    # Get all jobs with their last run status
    jobs = jm.list_jobs_with_last_run()
    
    print("Job Status Summary:")
    print("-" * 80)
    print(f"{'Job Name':<25} {'Last Run':<20} {'Status':<10} {'Records'}")
    print("-" * 80)
    
    for job in jobs:
        job_name = job['job_name']
        last_run = job.get('last_run')
        
        if last_run:
            started = last_run['started_at'][:19]  # Trim to readable format
            status = last_run['status']
            records = last_run.get('records_processed', 'N/A')
            print(f"{job_name:<25} {started:<20} {status:<10} {records}")
        else:
            print(f"{job_name:<25} {'Never run':<20} {'-':<10} {'-'}")
    
    print("-" * 80)
    
    # Get recent history
    all_history = jm.get_all_history(limit=10)
    print(f"\nRecent Executions (last {len(all_history)}):")
    for hist in all_history[:5]:
        print(f"  {hist['job_name']}: {hist['status']} - {hist.get('records_processed', 0)} records")
    
    print("Complete!\n")


# Example 8: Data Pipeline
def example_data_pipeline():
    """Create a complete data pipeline"""
    print("Example 8: Complete Data Pipeline")
    
    # Step 1: Excel to Parquet
    print("Step 1: Converting Excel to Parquet...")
    source1 = ExcelSourceConnector(file_path='data/raw_data.xlsx')
    sink1 = ParquetSinkConnector(directory='pipeline/stage1')
    
    app1 = datafactory_cli.DataFactory(source_connector=source1, sink_connector=sink1)
    df1 = app1.get_data()
    app1.write_data(df1, 'raw_data')
    app1.close_connections()
    print(f"  Processed {len(df1)} rows")
    
    # Step 2: Parquet to CSV (for reporting)
    print("Step 2: Converting to CSV for reporting...")
    # Note: Would need a ParquetSourceConnector for this
    # For now, showing the concept
    df2 = pd.read_parquet('pipeline/stage1/raw_data.parquet')
    df2.to_csv('pipeline/stage2/report.csv', index=False)
    print(f"  Exported {len(df2)} rows to CSV")
    
    # Step 3: Archive to JSON
    print("Step 3: Creating JSON archive...")
    df3 = pd.read_parquet('pipeline/stage1/raw_data.parquet')
    df3.to_json('pipeline/archive/data.json', orient='records', indent=2)
    print(f"  Archived {len(df3)} rows to JSON")
    
    print("Pipeline complete!\n")


if __name__ == '__main__':
    print("=" * 80)
    print("DataFactory 2.0 - New Features Examples")
    print("=" * 80)
    print()
    print("This file demonstrates the new features in DataFactory 2.0:")
    print("- Excel source connector")
    print("- Parquet sink connector")
    print("- Job management system")
    print("- REST API")
    print("- Job scheduling")
    print("- Logging and monitoring")
    print()
    print("Uncomment the examples you want to run:")
    print()
    
    # Uncomment the examples you want to run:
    # example_excel_to_parquet()
    # example_excel_sheets_to_parquet()
    # example_database_to_parquet()
    # example_job_manager()
    # example_api_usage()
    # example_scheduled_jobs()
    # example_monitoring()
    # example_data_pipeline()
    
    print("Please edit this file and uncomment the examples you want to run.")
