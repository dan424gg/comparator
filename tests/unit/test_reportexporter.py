import pytest
import pandas as pd
import os
from ...comparator.src.exporters import ReportExporter

@pytest.fixture
def sample_comparison_data():
    return {
        'matching': [1, 3],
        'mismatched': [2],
        'only_in_source': [4],
        'only_in_target': [5],
        'comparison_details': pd.DataFrame({
            'id': [1, 2, 3],
            'source_value': [10, 20, 30],
            'target_value': [10, 25, 30],
            'difference': [0, 5, 0]
        })
    }

def test_report_initialization(sample_comparison_data):
    """Test ReportExporter initialization"""
    exporter = ReportExporter(sample_comparison_data)
    assert exporter.data == sample_comparison_data

def test_to_excel(sample_comparison_data, tmp_path):
    """Test Excel report generation"""
    output_file = tmp_path / "report.xlsx"
    
    exporter = ReportExporter(sample_comparison_data)
    exporter.to_excel(str(output_file))
    
    assert os.path.exists(output_file)
    
    # Read back the Excel file to verify contents
    df = pd.read_excel(output_file, sheet_name=None)
    
    # Check that all expected sheets exist
    expected_sheets = ['Summary', 'Matching', 'Mismatched', 'Source Only', 'Target Only']
    assert all(sheet in df.keys() for sheet in expected_sheets)

def test_to_csv(sample_comparison_data, tmp_path):
    """Test CSV report generation"""
    output_dir = tmp_path / "report_csv"
    os.makedirs(output_dir)
    
    exporter = ReportExporter(sample_comparison_data)
    exporter.to_csv(str(output_dir))
    
    # Check that all CSV files were created
    expected_files = ['summary.csv', 'matching.csv', 'mismatched.csv', 
                     'source_only.csv', 'target_only.csv']
    
    for file in expected_files:
        assert os.path.exists(output_dir / file)

def test_generate_summary(sample_comparison_data):
    """Test summary generation"""
    exporter = ReportExporter(sample_comparison_data)
    summary = exporter.generate_summary()
    
    assert isinstance(summary, pd.DataFrame)
    assert 'Category' in summary.columns
    assert 'Count' in summary.columns
    
    # Verify summary counts
    summary_dict = dict(zip(summary['Category'], summary['Count']))
    assert summary_dict['Matching Records'] == 2
    assert summary_dict['Mismatched Records'] == 1
    assert summary_dict['Source Only Records'] == 1
    assert summary_dict['Target Only Records'] == 1

def test_empty_comparison_data():
    """Test handling of empty comparison data"""
    empty_data = {
        'matching': [],
        'mismatched': [],
        'only_in_source': [],
        'only_in_target': [],
        'comparison_details': pd.DataFrame(columns=['id', 'source_value', 'target_value', 'difference'])
    }
    
    exporter = ReportExporter(empty_data)
    summary = exporter.generate_summary()
    
    assert len(summary) > 0  # Should still generate a summary with zero counts
    assert all(count == 0 for count in summary['Count'])

def test_invalid_export_path(sample_comparison_data):
    """Test error handling for invalid export paths"""
    exporter = ReportExporter(sample_comparison_data)
    
    with pytest.raises(Exception):
        exporter.to_excel('/invalid/path/report.xlsx')
    
    with pytest.raises(Exception):
        exporter.to_csv('/invalid/path/report')