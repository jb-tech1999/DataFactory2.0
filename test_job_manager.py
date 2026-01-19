"""
Test script for Job Manager
"""
import os
import sys
import pandas as pd
from job_manager import JobManager


def test_job_creation():
    """Test creating a job"""
    print("Testing job creation...")
    
    # Create test data
    os.makedirs('/tmp/datafactory_test', exist_ok=True)
    test_data = pd.DataFrame({
        'id': [1, 2, 3],
        'value': ['A', 'B', 'C']
    })
    csv_path = '/tmp/datafactory_test/source.csv'
    test_data.to_csv(csv_path, index=False)
    
    # Create job manager
    jm = JobManager(db_path='/tmp/datafactory_test/test_jobs.db')
    
    # Create a job
    job_id = jm.create_job(
        job_name='test_csv_to_parquet',
        source_type='csv',
        source_config={'file_path': csv_path},
        sink_type='parquet',
        sink_config={'directory': '/tmp/datafactory_test/output'}
    )
    
    assert job_id > 0, "Job ID should be positive"
    
    # Verify job was created
    job = jm.get_job(job_id)
    assert job is not None, "Job should exist"
    assert job['job_name'] == 'test_csv_to_parquet', "Job name mismatch"
    assert job['source_type'] == 'csv', "Source type mismatch"
    assert job['sink_type'] == 'parquet', "Sink type mismatch"
    
    print(f"✓ Job created successfully with ID: {job_id}")
    return True


def test_job_listing():
    """Test listing jobs"""
    print("Testing job listing...")
    
    jm = JobManager(db_path='/tmp/datafactory_test/test_jobs.db')
    
    jobs = jm.list_jobs()
    assert len(jobs) > 0, "Should have at least one job"
    
    print(f"✓ Found {len(jobs)} job(s)")
    return True


def test_job_execution():
    """Test executing a job"""
    print("Testing job execution...")
    
    jm = JobManager(db_path='/tmp/datafactory_test/test_jobs.db')
    
    # Get the job we created
    job = jm.get_job_by_name('test_csv_to_parquet')
    assert job is not None, "Job should exist"
    
    # Execute the job
    result = jm.execute_job(job['job_id'])
    
    assert result['status'] == 'success', f"Job execution failed: {result.get('error', 'Unknown error')}"
    assert result['records_processed'] == 3, f"Expected 3 records, got {result['records_processed']}"
    
    # Verify output file was created
    output_file = '/tmp/datafactory_test/output/test_csv_to_parquet.parquet'
    assert os.path.exists(output_file), "Output file should exist"
    
    print(f"✓ Job executed successfully, processed {result['records_processed']} records")
    return True


def test_job_history():
    """Test job history"""
    print("Testing job history...")
    
    jm = JobManager(db_path='/tmp/datafactory_test/test_jobs.db')
    
    # Get the job
    job = jm.get_job_by_name('test_csv_to_parquet')
    
    # Get history
    history = jm.get_job_history(job['job_id'])
    assert len(history) > 0, "Should have at least one history entry"
    
    # Verify history details
    last_run = history[0]
    assert last_run['status'] == 'success', "Last run should be successful"
    assert last_run['records_processed'] == 3, "Should have processed 3 records"
    
    print(f"✓ Found {len(history)} history entry(ies)")
    return True


def test_job_logs():
    """Test job logs"""
    print("Testing job logs...")
    
    jm = JobManager(db_path='/tmp/datafactory_test/test_jobs.db')
    
    # Get the job
    job = jm.get_job_by_name('test_csv_to_parquet')
    
    # Get history
    history = jm.get_job_history(job['job_id'])
    history_id = history[0]['history_id']
    
    # Get logs
    logs = jm.get_job_logs(history_id)
    assert len(logs) > 0, "Should have log entries"
    
    # Verify log structure
    log = logs[0]
    assert 'level' in log, "Log should have level"
    assert 'message' in log, "Log should have message"
    assert 'timestamp' in log, "Log should have timestamp"
    
    print(f"✓ Found {len(logs)} log entry(ies)")
    for log in logs:
        print(f"  [{log['level']}] {log['message']}")
    return True


def test_job_update():
    """Test updating a job"""
    print("Testing job update...")
    
    jm = JobManager(db_path='/tmp/datafactory_test/test_jobs.db')
    
    # Get the job
    job = jm.get_job_by_name('test_csv_to_parquet')
    
    # Update the job
    success = jm.update_job(job['job_id'], enabled=False)
    assert success, "Job update should succeed"
    
    # Verify update
    updated_job = jm.get_job(job['job_id'])
    assert updated_job['enabled'] == False, "Job should be disabled"
    
    # Re-enable for other tests
    jm.update_job(job['job_id'], enabled=True)
    
    print("✓ Job updated successfully")
    return True


def test_job_with_last_run():
    """Test getting job with last run info"""
    print("Testing job with last run info...")
    
    jm = JobManager(db_path='/tmp/datafactory_test/test_jobs.db')
    
    # Get the job
    job = jm.get_job_by_name('test_csv_to_parquet')
    
    # Get job with last run
    job_with_run = jm.get_job_with_last_run(job['job_id'])
    
    assert job_with_run is not None, "Job should exist"
    assert 'last_run' in job_with_run, "Should have last_run info"
    assert job_with_run['last_run'] is not None, "Should have last run data"
    assert job_with_run['last_run']['status'] == 'success', "Last run should be successful"
    
    print("✓ Job with last run retrieved successfully")
    print(f"  Last run: {job_with_run['last_run']['started_at']}")
    print(f"  Status: {job_with_run['last_run']['status']}")
    print(f"  Records: {job_with_run['last_run']['records_processed']}")
    return True


def main():
    """Run all tests"""
    print("=" * 60)
    print("DataFactory 2.0 - Job Manager Tests")
    print("=" * 60)
    print()
    
    tests = [
        test_job_creation,
        test_job_listing,
        test_job_execution,
        test_job_history,
        test_job_logs,
        test_job_update,
        test_job_with_last_run
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            if test():
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print(f"✗ Test {test.__name__} failed with exception: {e}")
            import traceback
            traceback.print_exc()
            failed += 1
        print()
    
    print("=" * 60)
    print(f"Tests passed: {passed}/{len(tests)}")
    print(f"Tests failed: {failed}/{len(tests)}")
    print("=" * 60)
    
    return failed == 0


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
