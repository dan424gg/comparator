"""
Basic comparison example using CSV files
"""
from comparator import Comparator, DataSource
from comparator import ReportExporter

def main():
    # Create data sources
    source = DataSource.from_csv("data/source.csv")
    target = DataSource.from_csv("data/target.csv")

    # Configure comparison
    comparator = Comparator(
        pk=["id"],  # Primary key for matching records
        ignore_cols=["updated_at"]  # Columns to ignore in comparison
    )

    # Run comparison
    result = comparator.compare(source, target)

    # Generate report
    report = ReportExporter(result)
    report.to_excel("output/comparison_report.xlsx")

    # Print summary
    print(f"Total records compared: {len(result['all_records'])}")
    print(f"Matching records: {len(result['matching'])}")
    print(f"Mismatched records: {len(result['mismatched'])}")
    print(f"Records only in source: {len(result['only_in_source'])}")
    print(f"Records only in target: {len(result['only_in_target'])}")

if __name__ == "__main__":
    main()