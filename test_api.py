"""
Test script for the API
"""
import requests
import time
import os
import pandas as pd

API_URL = "http://localhost:8000"

def wait_for_api(max_attempts=10):
    """Wait for API to be ready"""
    for i in range(max_attempts):
        try:
            response = requests.get(f"{API_URL}/health")
            if response.status_code == 200:
                print("✓ API is ready")
                return True
        except:
            pass
        time.sleep(1)
    return False


def test_health_check():
    """Test health check endpoint"""
    print("Testing health check...")
    response = requests.get(f"{API_URL}/health")
    assert response.status_code == 200, f"Expected 200, got {response.status_code}"
    data = response.json()
    assert data['status'] == 'healthy', "API should be healthy"
    print("✓ Health check passed")
    return True


def test_list_connectors():
    """Test listing available connectors"""
    print("Testing list connectors...")
    response = requests.get(f"{API_URL}/connectors")
    assert response.status_code == 200, f"Expected 200, got {response.status_code}"
    data = response.json()
    assert 'source_connectors' in data, "Should have source_connectors"
    assert 'sink_connectors' in data, "Should have sink_connectors"
    
    # Check for Excel and Parquet
    source_types = [c['type'] for c in data['source_connectors']]
    sink_types = [c['type'] for c in data['sink_connectors']]
    
    assert 'excel' in source_types, "Excel should be in source connectors"
    assert 'parquet' in sink_types, "Parquet should be in sink connectors"
    
    print(f"✓ Found {len(source_types)} source and {len(sink_types)} sink connectors")
    return True


def test_create_job():
    """Test creating a job"""
    print("Testing job creation via API...")
    
    # Create test data
    os.makedirs('/tmp/api_test', exist_ok=True)
    test_data = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Test1', 'Test2', 'Test3', 'Test4', 'Test5']
    })
    csv_path = '/tmp/api_test/test.csv'
    test_data.to_csv(csv_path, index=False)
    
    job_data = {
        "job_name": "api_test_job",
        "source_type": "csv",
        "source_config": {"file_path": csv_path},
        "sink_type": "parquet",
        "sink_config": {"directory": "/tmp/api_test/output"}
    }
    
    response = requests.post(f"{API_URL}/jobs", json=job_data)
    assert response.status_code == 201, f"Expected 201, got {response.status_code}"
    
    result = response.json()
    assert 'job_id' in result, "Should return job_id"
    job_id = result['job_id']
    
    print(f"✓ Job created with ID: {job_id}")
    return job_id


def test_list_jobs(job_id):
    """Test listing jobs"""
    print("Testing list jobs...")
    response = requests.get(f"{API_URL}/jobs")
    assert response.status_code == 200, f"Expected 200, got {response.status_code}"
    
    data = response.json()
    assert 'jobs' in data, "Should have jobs"
    assert data['count'] > 0, "Should have at least one job"
    
    # Find our job
    job_found = any(job['job_id'] == job_id for job in data['jobs'])
    assert job_found, "Should find our created job"
    
    print(f"✓ Found {data['count']} job(s)")
    return True


def test_get_job(job_id):
    """Test getting a specific job"""
    print(f"Testing get job {job_id}...")
    response = requests.get(f"{API_URL}/jobs/{job_id}")
    assert response.status_code == 200, f"Expected 200, got {response.status_code}"
    
    job = response.json()
    assert job['job_id'] == job_id, "Job ID should match"
    assert job['job_name'] == 'api_test_job', "Job name should match"
    
    print(f"✓ Retrieved job: {job['job_name']}")
    return True


def test_execute_job(job_id):
    """Test executing a job"""
    print(f"Testing job execution for job {job_id}...")
    response = requests.post(f"{API_URL}/jobs/{job_id}/execute")
    assert response.status_code == 200, f"Expected 200, got {response.status_code}"
    
    result = response.json()
    assert result['status'] == 'success', f"Job should succeed: {result.get('error', '')}"
    assert result['records_processed'] == 5, f"Expected 5 records, got {result['records_processed']}"
    
    print(f"✓ Job executed successfully, processed {result['records_processed']} records")
    return result['history_id']


def test_get_history(job_id):
    """Test getting job history"""
    print(f"Testing get history for job {job_id}...")
    response = requests.get(f"{API_URL}/jobs/{job_id}/history")
    assert response.status_code == 200, f"Expected 200, got {response.status_code}"
    
    data = response.json()
    assert 'history' in data, "Should have history"
    assert data['count'] > 0, "Should have at least one history entry"
    
    print(f"✓ Found {data['count']} history entry(ies)")
    return True


def test_get_logs(history_id):
    """Test getting logs for a job execution"""
    print(f"Testing get logs for history {history_id}...")
    response = requests.get(f"{API_URL}/logs/{history_id}")
    assert response.status_code == 200, f"Expected 200, got {response.status_code}"
    
    data = response.json()
    assert 'logs' in data, "Should have logs"
    assert data['count'] > 0, "Should have log entries"
    
    print(f"✓ Found {data['count']} log entry(ies)")
    return True


def test_update_job(job_id):
    """Test updating a job"""
    print(f"Testing update job {job_id}...")
    update_data = {
        "enabled": False
    }
    
    response = requests.put(f"{API_URL}/jobs/{job_id}", json=update_data)
    assert response.status_code == 200, f"Expected 200, got {response.status_code}"
    
    # Verify update
    response = requests.get(f"{API_URL}/jobs/{job_id}")
    job = response.json()
    assert job['enabled'] == False, "Job should be disabled"
    
    # Re-enable
    update_data = {"enabled": True}
    requests.put(f"{API_URL}/jobs/{job_id}", json=update_data)
    
    print("✓ Job updated successfully")
    return True


def test_scheduled_jobs():
    """Test getting scheduled jobs"""
    print("Testing get scheduled jobs...")
    response = requests.get(f"{API_URL}/scheduler/jobs")
    assert response.status_code == 200, f"Expected 200, got {response.status_code}"
    
    data = response.json()
    assert 'scheduled_jobs' in data, "Should have scheduled_jobs"
    
    print(f"✓ Found {data['count']} scheduled job(s)")
    return True


def main():
    """Run all tests"""
    print("=" * 60)
    print("DataFactory 2.0 - API Tests")
    print("=" * 60)
    print()
    
    # Wait for API to be ready
    if not wait_for_api():
        print("✗ API is not ready")
        return False
    
    print()
    
    job_id = None
    history_id = None
    
    tests = [
        (test_health_check, []),
        (test_list_connectors, []),
        (test_create_job, []),
        (test_list_jobs, lambda: [job_id]),
        (test_get_job, lambda: [job_id]),
        (test_execute_job, lambda: [job_id]),
        (test_get_history, lambda: [job_id]),
        (test_get_logs, lambda: [history_id]),
        (test_update_job, lambda: [job_id]),
        (test_scheduled_jobs, [])
    ]
    
    passed = 0
    failed = 0
    
    for test_info in tests:
        if len(test_info) == 2:
            test, args = test_info
        else:
            test = test_info
            args = []
        
        try:
            # Get dynamic args if needed
            if callable(args):
                args = args()
            
            result = test(*args) if args else test()
            
            # Store results for later tests
            if test == test_create_job:
                job_id = result
            elif test == test_execute_job:
                history_id = result
            
            passed += 1
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
    import sys
    success = main()
    sys.exit(0 if success else 1)
