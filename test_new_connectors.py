"""
Test script for new Excel and Parquet connectors
"""
import pandas as pd
import os
import sys

def test_excel_to_parquet():
    """Test Excel source to Parquet sink"""
    print("Testing Excel to Parquet transfer...")
    
    from connectors import ExcelSourceConnector, ParquetSinkConnector
    import datafactory_cli
    
    # Create test Excel file
    test_data = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'age': [25, 30, 35, 40, 45],
        'city': ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']
    })
    
    os.makedirs('/tmp/datafactory_test', exist_ok=True)
    excel_path = '/tmp/datafactory_test/test_data.xlsx'
    test_data.to_excel(excel_path, index=False, sheet_name='TestSheet')
    
    # Create connectors
    source = ExcelSourceConnector(file_path=excel_path, sheet_name='TestSheet')
    sink = ParquetSinkConnector(directory='/tmp/datafactory_test/parquet_output')
    
    # Create DataFactory and transfer data
    app = datafactory_cli.DataFactory(source_connector=source, sink_connector=sink)
    df = app.get_data()
    app.write_data(df, 'test_output')
    app.close_connections()
    
    # Verify output
    parquet_path = '/tmp/datafactory_test/parquet_output/test_output.parquet'
    assert os.path.exists(parquet_path), "Parquet output file not created"
    
    # Read back and verify
    result = pd.read_parquet(parquet_path)
    assert len(result) == 5, f"Expected 5 rows, got {len(result)}"
    assert 'name' in result.columns, "Missing 'name' column"
    
    print("✓ Excel to Parquet transfer successful!")
    return True


def test_excel_sheets():
    """Test Excel connector with multiple sheets"""
    print("Testing Excel connector with multiple sheets...")
    
    from connectors import ExcelSourceConnector
    
    # Create test Excel file with multiple sheets
    test_data1 = pd.DataFrame({
        'col1': [1, 2, 3],
        'col2': ['A', 'B', 'C']
    })
    
    test_data2 = pd.DataFrame({
        'col3': [4, 5, 6],
        'col4': ['D', 'E', 'F']
    })
    
    os.makedirs('/tmp/datafactory_test', exist_ok=True)
    excel_path = '/tmp/datafactory_test/multi_sheet.xlsx'
    
    with pd.ExcelWriter(excel_path) as writer:
        test_data1.to_excel(writer, sheet_name='Sheet1', index=False)
        test_data2.to_excel(writer, sheet_name='Sheet2', index=False)
    
    # Test reading sheet names
    source = ExcelSourceConnector(file_path=excel_path)
    source.connect()
    tables = source.get_tables()
    
    assert 'Sheet1' in tables['TABLE_NAME'].values, "Sheet1 not found"
    assert 'Sheet2' in tables['TABLE_NAME'].values, "Sheet2 not found"
    
    source.close()
    
    print("✓ Excel multi-sheet test successful!")
    return True


def test_csv_to_parquet():
    """Test CSV to Parquet conversion"""
    print("Testing CSV to Parquet transfer...")
    
    from connectors import CSVSourceConnector, ParquetSinkConnector
    import datafactory_cli
    
    # Create test CSV file
    test_data = pd.DataFrame({
        'product_id': [101, 102, 103],
        'product_name': ['Widget', 'Gadget', 'Doohickey'],
        'price': [19.99, 29.99, 39.99]
    })
    
    os.makedirs('/tmp/datafactory_test', exist_ok=True)
    csv_path = '/tmp/datafactory_test/test_products.csv'
    test_data.to_csv(csv_path, index=False)
    
    # Create connectors
    source = CSVSourceConnector(file_path=csv_path)
    sink = ParquetSinkConnector(directory='/tmp/datafactory_test/parquet_from_csv')
    
    # Create DataFactory and transfer data
    app = datafactory_cli.DataFactory(source_connector=source, sink_connector=sink)
    df = app.get_data()
    app.write_data(df, 'products')
    app.close_connections()
    
    # Verify output
    parquet_path = '/tmp/datafactory_test/parquet_from_csv/products.parquet'
    assert os.path.exists(parquet_path), "Parquet output file not created"
    
    # Read back and verify
    result = pd.read_parquet(parquet_path)
    assert len(result) == 3, f"Expected 3 rows, got {len(result)}"
    assert 'product_name' in result.columns, "Missing 'product_name' column"
    
    print("✓ CSV to Parquet transfer successful!")
    return True


def main():
    """Run all tests"""
    print("=" * 60)
    print("DataFactory 2.0 - Excel & Parquet Connector Tests")
    print("=" * 60)
    print()
    
    tests = [
        test_excel_to_parquet,
        test_excel_sheets,
        test_csv_to_parquet
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
