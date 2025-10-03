# Usage Guide

## Quick Start

```python
from comparator import Comparator, DataSource

# Create data sources
source_data = DataSource.from_csv("source.csv")
target_data = DataSource.from_csv("target.csv")

# Initialize comparator
comparator = Comparator(pk=["id"], ignore_cols=["updated_at"])

# Run comparison
result = comparator.compare(source_data, target_data)

# Export results
report = ReportExporter(result)
report.to_excel("comparison_report.xlsx")
```

## Advanced Usage

[Additional usage examples and configurations will be added here]
