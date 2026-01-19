"""
Job Manager for DataFactory 2.0
Manages job definitions, execution, and logging
"""
import os
import json
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional
import pandas as pd
from connectors import SourceConnector, SinkConnector
import datafactory_cli


class JobManager:
    """Manages data factory jobs and their execution history"""
    
    def __init__(self, db_path: str = 'jobs.db'):
        """
        Initialize JobManager with SQLite database
        
        Args:
            db_path: Path to SQLite database for job storage
        """
        self.db_path = db_path
        self._init_database()
    
    def _init_database(self):
        """Initialize database tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Jobs table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS jobs (
                job_id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_name TEXT UNIQUE NOT NULL,
                source_type TEXT NOT NULL,
                source_config TEXT NOT NULL,
                sink_type TEXT NOT NULL,
                sink_config TEXT NOT NULL,
                query TEXT,
                schedule TEXT,
                enabled INTEGER DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        ''')
        
        # Job execution history table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS job_history (
                history_id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id INTEGER NOT NULL,
                job_name TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                status TEXT NOT NULL,
                records_processed INTEGER,
                error_message TEXT,
                FOREIGN KEY (job_id) REFERENCES jobs (job_id)
            )
        ''')
        
        # Job logs table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS job_logs (
                log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                history_id INTEGER NOT NULL,
                job_id INTEGER NOT NULL,
                timestamp TEXT NOT NULL,
                level TEXT NOT NULL,
                message TEXT NOT NULL,
                FOREIGN KEY (history_id) REFERENCES job_history (history_id),
                FOREIGN KEY (job_id) REFERENCES jobs (job_id)
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def create_job(self, job_name: str, source_type: str, source_config: Dict,
                   sink_type: str, sink_config: Dict, query: str = None,
                   schedule: str = None) -> int:
        """
        Create a new job definition
        
        Args:
            job_name: Unique name for the job
            source_type: Type of source connector
            source_config: Configuration for source connector
            sink_type: Type of sink connector
            sink_config: Configuration for sink connector
            query: Optional query to execute
            schedule: Optional cron schedule string
        
        Returns:
            job_id of created job
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        now = datetime.now().isoformat()
        
        cursor.execute('''
            INSERT INTO jobs (job_name, source_type, source_config, sink_type, 
                            sink_config, query, schedule, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (job_name, source_type, json.dumps(source_config), sink_type,
              json.dumps(sink_config), query, schedule, now, now))
        
        job_id = cursor.lastrowid
        conn.commit()
        conn.close()
        
        return job_id
    
    def get_job(self, job_id: int) -> Optional[Dict]:
        """Get job definition by ID"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM jobs WHERE job_id = ?', (job_id,))
        row = cursor.fetchone()
        conn.close()
        
        if not row:
            return None
        
        return {
            'job_id': row[0],
            'job_name': row[1],
            'source_type': row[2],
            'source_config': json.loads(row[3]),
            'sink_type': row[4],
            'sink_config': json.loads(row[5]),
            'query': row[6],
            'schedule': row[7],
            'enabled': bool(row[8]),
            'created_at': row[9],
            'updated_at': row[10]
        }
    
    def get_job_by_name(self, job_name: str) -> Optional[Dict]:
        """Get job definition by name"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM jobs WHERE job_name = ?', (job_name,))
        row = cursor.fetchone()
        conn.close()
        
        if not row:
            return None
        
        return {
            'job_id': row[0],
            'job_name': row[1],
            'source_type': row[2],
            'source_config': json.loads(row[3]),
            'sink_type': row[4],
            'sink_config': json.loads(row[5]),
            'query': row[6],
            'schedule': row[7],
            'enabled': bool(row[8]),
            'created_at': row[9],
            'updated_at': row[10]
        }
    
    def list_jobs(self) -> List[Dict]:
        """List all jobs"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM jobs ORDER BY job_name')
        rows = cursor.fetchall()
        conn.close()
        
        jobs = []
        for row in rows:
            jobs.append({
                'job_id': row[0],
                'job_name': row[1],
                'source_type': row[2],
                'source_config': json.loads(row[3]),
                'sink_type': row[4],
                'sink_config': json.loads(row[5]),
                'query': row[6],
                'schedule': row[7],
                'enabled': bool(row[8]),
                'created_at': row[9],
                'updated_at': row[10]
            })
        
        return jobs
    
    def update_job(self, job_id: int, **kwargs) -> bool:
        """Update job definition"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Build update query dynamically
        allowed_fields = ['job_name', 'source_type', 'source_config', 'sink_type',
                         'sink_config', 'query', 'schedule', 'enabled']
        
        updates = []
        values = []
        
        for key, value in kwargs.items():
            if key in allowed_fields:
                if key in ['source_config', 'sink_config'] and isinstance(value, dict):
                    value = json.dumps(value)
                updates.append(f'{key} = ?')
                values.append(value)
        
        if not updates:
            conn.close()
            return False
        
        updates.append('updated_at = ?')
        values.append(datetime.now().isoformat())
        values.append(job_id)
        
        query = f"UPDATE jobs SET {', '.join(updates)} WHERE job_id = ?"
        cursor.execute(query, values)
        
        success = cursor.rowcount > 0
        conn.commit()
        conn.close()
        
        return success
    
    def delete_job(self, job_id: int) -> bool:
        """Delete job definition"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('DELETE FROM jobs WHERE job_id = ?', (job_id,))
        success = cursor.rowcount > 0
        
        conn.commit()
        conn.close()
        
        return success
    
    def execute_job(self, job_id: int) -> Dict:
        """
        Execute a job and log the results
        
        Returns:
            Dictionary with execution results
        """
        job = self.get_job(job_id)
        if not job:
            return {'status': 'error', 'message': 'Job not found'}
        
        if not job['enabled']:
            return {'status': 'error', 'message': 'Job is disabled'}
        
        # Create history record
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        started_at = datetime.now().isoformat()
        cursor.execute('''
            INSERT INTO job_history (job_id, job_name, started_at, status)
            VALUES (?, ?, ?, ?)
        ''', (job_id, job['job_name'], started_at, 'running'))
        
        history_id = cursor.lastrowid
        conn.commit()
        
        def log_message(level: str, message: str):
            """Log a message to the database"""
            cursor.execute('''
                INSERT INTO job_logs (history_id, job_id, timestamp, level, message)
                VALUES (?, ?, ?, ?, ?)
            ''', (history_id, job_id, datetime.now().isoformat(), level, message))
            conn.commit()
        
        try:
            log_message('INFO', f'Starting job: {job["job_name"]}')
            
            # Create source connector
            source = self._create_source_connector(job['source_type'], job['source_config'])
            log_message('INFO', f'Created source connector: {job["source_type"]}')
            
            # Create sink connector
            sink = self._create_sink_connector(job['sink_type'], job['sink_config'])
            log_message('INFO', f'Created sink connector: {job["sink_type"]}')
            
            # Create DataFactory instance
            df_app = datafactory_cli.DataFactory(source_connector=source, sink_connector=sink)
            
            # Execute data transfer
            query = job.get('query') or 'SELECT * FROM source'
            log_message('INFO', f'Executing query: {query}')
            
            df = df_app.get_data(query)
            records_processed = len(df)
            
            log_message('INFO', f'Retrieved {records_processed} records')
            
            # Write data
            table_name = job['job_name'].replace(' ', '_').lower()
            df_app.write_data(df, table_name)
            log_message('INFO', f'Data written to sink: {table_name}')
            
            # Close connections
            df_app.close_connections()
            log_message('INFO', 'Job completed successfully')
            
            # Update history
            completed_at = datetime.now().isoformat()
            cursor.execute('''
                UPDATE job_history 
                SET completed_at = ?, status = ?, records_processed = ?
                WHERE history_id = ?
            ''', (completed_at, 'success', records_processed, history_id))
            conn.commit()
            
            return {
                'status': 'success',
                'history_id': history_id,
                'records_processed': records_processed,
                'started_at': started_at,
                'completed_at': completed_at
            }
            
        except Exception as e:
            error_message = str(e)
            log_message('ERROR', f'Job failed: {error_message}')
            
            # Update history
            completed_at = datetime.now().isoformat()
            cursor.execute('''
                UPDATE job_history 
                SET completed_at = ?, status = ?, error_message = ?
                WHERE history_id = ?
            ''', (completed_at, 'failed', error_message, history_id))
            conn.commit()
            
            return {
                'status': 'failed',
                'history_id': history_id,
                'error': error_message,
                'started_at': started_at,
                'completed_at': completed_at
            }
        
        finally:
            conn.close()
    
    def _create_source_connector(self, source_type: str, config: Dict) -> SourceConnector:
        """Create source connector from configuration"""
        from connectors import (
            ODBCSourceConnector, PostgreSQLSourceConnector, MySQLSourceConnector,
            CSVSourceConnector, JSONSourceConnector, ExcelSourceConnector
        )
        
        if source_type == 'odbc':
            return ODBCSourceConnector(**config)
        elif source_type == 'postgresql':
            return PostgreSQLSourceConnector(**config)
        elif source_type == 'mysql':
            return MySQLSourceConnector(**config)
        elif source_type == 'csv':
            return CSVSourceConnector(**config)
        elif source_type == 'json':
            return JSONSourceConnector(**config)
        elif source_type == 'excel':
            return ExcelSourceConnector(**config)
        else:
            raise ValueError(f'Unknown source type: {source_type}')
    
    def _create_sink_connector(self, sink_type: str, config: Dict) -> SinkConnector:
        """Create sink connector from configuration"""
        from connectors import (
            SQLiteSinkConnector, PostgreSQLSinkConnector, MySQLSinkConnector,
            CSVSinkConnector, JSONSinkConnector, ParquetSinkConnector
        )
        
        if sink_type == 'sqlite':
            return SQLiteSinkConnector(**config)
        elif sink_type == 'postgresql':
            return PostgreSQLSinkConnector(**config)
        elif sink_type == 'mysql':
            return MySQLSinkConnector(**config)
        elif sink_type == 'csv':
            return CSVSinkConnector(**config)
        elif sink_type == 'json':
            return JSONSinkConnector(**config)
        elif sink_type == 'parquet':
            return ParquetSinkConnector(**config)
        else:
            raise ValueError(f'Unknown sink type: {sink_type}')
    
    def get_job_history(self, job_id: int, limit: int = 50) -> List[Dict]:
        """Get execution history for a job"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM job_history 
            WHERE job_id = ? 
            ORDER BY started_at DESC 
            LIMIT ?
        ''', (job_id, limit))
        
        rows = cursor.fetchall()
        conn.close()
        
        history = []
        for row in rows:
            history.append({
                'history_id': row[0],
                'job_id': row[1],
                'job_name': row[2],
                'started_at': row[3],
                'completed_at': row[4],
                'status': row[5],
                'records_processed': row[6],
                'error_message': row[7]
            })
        
        return history
    
    def get_all_history(self, limit: int = 100) -> List[Dict]:
        """Get execution history for all jobs"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM job_history 
            ORDER BY started_at DESC 
            LIMIT ?
        ''', (limit,))
        
        rows = cursor.fetchall()
        conn.close()
        
        history = []
        for row in rows:
            history.append({
                'history_id': row[0],
                'job_id': row[1],
                'job_name': row[2],
                'started_at': row[3],
                'completed_at': row[4],
                'status': row[5],
                'records_processed': row[6],
                'error_message': row[7]
            })
        
        return history
    
    def get_job_logs(self, history_id: int) -> List[Dict]:
        """Get logs for a specific job execution"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM job_logs 
            WHERE history_id = ? 
            ORDER BY timestamp
        ''', (history_id,))
        
        rows = cursor.fetchall()
        conn.close()
        
        logs = []
        for row in rows:
            logs.append({
                'log_id': row[0],
                'history_id': row[1],
                'job_id': row[2],
                'timestamp': row[3],
                'level': row[4],
                'message': row[5]
            })
        
        return logs
    
    def get_job_with_last_run(self, job_id: int) -> Optional[Dict]:
        """Get job with last execution information"""
        job = self.get_job(job_id)
        if not job:
            return None
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT started_at, completed_at, status, records_processed
            FROM job_history 
            WHERE job_id = ? 
            ORDER BY started_at DESC 
            LIMIT 1
        ''', (job_id,))
        
        row = cursor.fetchone()
        conn.close()
        
        if row:
            job['last_run'] = {
                'started_at': row[0],
                'completed_at': row[1],
                'status': row[2],
                'records_processed': row[3]
            }
        else:
            job['last_run'] = None
        
        return job
    
    def list_jobs_with_last_run(self) -> List[Dict]:
        """List all jobs with their last execution information"""
        jobs = self.list_jobs()
        
        for job in jobs:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT started_at, completed_at, status, records_processed
                FROM job_history 
                WHERE job_id = ? 
                ORDER BY started_at DESC 
                LIMIT 1
            ''', (job['job_id'],))
            
            row = cursor.fetchone()
            conn.close()
            
            if row:
                job['last_run'] = {
                    'started_at': row[0],
                    'completed_at': row[1],
                    'status': row[2],
                    'records_processed': row[3]
                }
            else:
                job['last_run'] = None
        
        return jobs
