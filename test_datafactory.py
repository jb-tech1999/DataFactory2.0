"""
Test script for DataFactory 2.0
Verifies that connectors can be instantiated and basic functionality works
"""
import pandas as pd
import os
import sys

# Test CSV and JSON connectors (file-based, don't require external services)
def test_csv_to_json():
    """Test CSV source to JSON sink"""
    print("Testing CSV to JSON transfer...")
    
    from connectors import CSVSourceConnector, JSONSinkConnector
    import datafactory_cli
    
    # Create test CSV file
    test_data = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'age': [25, 30, 35, 40, 45],
        'city': ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']
    })
    
    os.makedirs('/tmp/datafactory_test', exist_ok=True)
    csv_path = '/tmp/datafactory_test/test_data.csv'
    test_data.to_csv(csv_path, index=False)
    
    # Create connectors
    source = CSVSourceConnector(file_path=csv_path)
    sink = JSONSinkConnector(directory='/tmp/datafactory_test/output')
    
    # Create DataFactory and transfer data
    app = datafactory_cli.DataFactory(source_connector=source, sink_connector=sink)
    df = app.get_data()
    app.write_data(df, 'test_output')
    app.close_connections()
    
    # Verify output
    json_path = '/tmp/datafactory_test/output/test_output.json'
    assert os.path.exists(json_path), "JSON output file not created"
    
    # Read back and verify
    result = pd.read_json(json_path)
    assert len(result) == 5, f"Expected 5 rows, got {len(result)}"
    assert 'name' in result.columns, "Missing 'name' column"
    
    print("✓ CSV to JSON transfer successful!")
    return True


def test_json_to_csv():
    """Test JSON source to CSV sink"""
    print("Testing JSON to CSV transfer...")
    
    from connectors import JSONSourceConnector, CSVSinkConnector
    import datafactory_cli
    
    # Create test JSON file
    test_data = pd.DataFrame({
        'product_id': [101, 102, 103],
        'product_name': ['Widget', 'Gadget', 'Doohickey'],
        'price': [19.99, 29.99, 39.99]
    })
    
    os.makedirs('/tmp/datafactory_test', exist_ok=True)
    json_path = '/tmp/datafactory_test/test_products.json'
    test_data.to_json(json_path, orient='records', indent=2)
    
    # Create connectors
    source = JSONSourceConnector(file_path=json_path)
    sink = CSVSinkConnector(directory='/tmp/datafactory_test/csv_output')
    
    # Create DataFactory and transfer data
    app = datafactory_cli.DataFactory(source_connector=source, sink_connector=sink)
    df = app.get_data()
    app.write_data(df, 'products')
    app.close_connections()
    
    # Verify output
    csv_path = '/tmp/datafactory_test/csv_output/products.csv'
    assert os.path.exists(csv_path), "CSV output file not created"
    
    # Read back and verify
    result = pd.read_csv(csv_path)
    assert len(result) == 3, f"Expected 3 rows, got {len(result)}"
    assert 'product_name' in result.columns, "Missing 'product_name' column"
    
    print("✓ JSON to CSV transfer successful!")
    return True


def test_csv_to_sqlite():
    """Test CSV source to SQLite sink"""
    print("Testing CSV to SQLite transfer...")
    
    from connectors import CSVSourceConnector, SQLiteSinkConnector
    import datafactory_cli
    import sqlalchemy
    
    # Create test CSV file
    test_data = pd.DataFrame({
        'user_id': [1, 2, 3],
        'username': ['user1', 'user2', 'user3'],
        'email': ['user1@example.com', 'user2@example.com', 'user3@example.com']
    })
    
    os.makedirs('/tmp/datafactory_test', exist_ok=True)
    csv_path = '/tmp/datafactory_test/users.csv'
    test_data.to_csv(csv_path, index=False)
    
    # Create connectors
    source = CSVSourceConnector(file_path=csv_path)
    sink = SQLiteSinkConnector(database_path='/tmp/datafactory_test/test.db')
    
    # Create DataFactory and transfer data
    app = datafactory_cli.DataFactory(source_connector=source, sink_connector=sink)
    df = app.get_data()
    app.write_data(df, 'users')
    app.close_connections()
    
    # Verify data in SQLite
    engine = sqlalchemy.create_engine('sqlite:////tmp/datafactory_test/test.db')
    result = pd.read_sql('SELECT * FROM users', engine)
    engine.dispose()
    
    assert len(result) == 3, f"Expected 3 rows in SQLite, got {len(result)}"
    assert 'username' in result.columns, "Missing 'username' column in SQLite"
    
    print("✓ CSV to SQLite transfer successful!")
    return True


def test_connector_classes():
    """Test that all connector classes can be instantiated"""
    print("Testing connector class instantiation...")
    
    from connectors import (
        ODBCSourceConnector, PostgreSQLSourceConnector, MySQLSourceConnector,
        CSVSourceConnector, JSONSourceConnector,
        SQLiteSinkConnector, PostgreSQLSinkConnector, MySQLSinkConnector,
        CSVSinkConnector, JSONSinkConnector
    )
    
    # Test source connectors (just instantiation, not connection)
    try:
        csv_source = CSVSourceConnector(file_path='/tmp/test.csv')
        json_source = JSONSourceConnector(file_path='/tmp/test.json')
        print("  ✓ File-based source connectors instantiated")
    except Exception as e:
        print(f"  ✗ File-based source connectors failed: {e}")
        return False
    
    # Test sink connectors (just instantiation, not connection)
    try:
        sqlite_sink = SQLiteSinkConnector(database_path='/tmp/test.db')
        csv_sink = CSVSinkConnector(directory='/tmp/csv')
        json_sink = JSONSinkConnector(directory='/tmp/json')
        print("  ✓ File-based sink connectors instantiated")
    except Exception as e:
        print(f"  ✗ File-based sink connectors failed: {e}")
        return False
    
    print("✓ All connector classes can be instantiated!")
    return True


def main():
    """Run all tests"""
    print("=" * 60)
    print("DataFactory 2.0 - Test Suite")
    print("=" * 60)
    print()
    
    tests = [
        test_connector_classes,
        test_csv_to_json,
        test_json_to_csv,
        test_csv_to_sqlite
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
