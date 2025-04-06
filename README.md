# Avro_Data_Exploration_Tool

A comprehensive Python module for exploring, inspecting, and converting Avro files to JSON and CSV formats.

## Features

- **Inspect Avro Files**: Get detailed metadata and schema information
- **Convert to JSON**: Transform Avro files to JSON format
- **Convert to CSV**: Transform Avro files to CSV format with flattened structure
- **Integrity Checks**: Validate Avro files for integrity and readability

## Installation

```bash
# Install from PyPI (once published)
pip install Avro_Data_Exploration_Tool

# Install from source
git clone https://github.com/priyeshr7/Avro_Data_Exploration_Tool.git
cd Avro_Data_Exploration_Tool
pip install -e .
```

## Usage in Jupyter Notebooks

### Quick Functions

For simple, one-off operations:

```python
from avro_explorer import inspect_avro, avro_to_json, avro_to_csv, check_avro_integrity

# Inspect an Avro file
metadata = inspect_avro("data.avro")

# Convert to JSON
json_data = avro_to_json("data.avro", "output.json")

# Convert to CSV
csv_data = avro_to_csv("data.avro", "output.csv")

# Run integrity checks
integrity = check_avro_integrity("data.avro")
```

### Class-based Usage

For more control or when performing multiple operations:

```python
from Avro_Data_Exploration_Tool import AvroDataExplorer

# Create an explorer instance
explorer = AvroDataExplorer()

# Inspect an Avro file
metadata = explorer.inspect_avro_file("data.avro")

# Convert to JSON without saving to file
json_data = explorer.convert_avro_to_json("data.avro")

# Convert to CSV with maximum 500 records
csv_data = explorer.convert_avro_to_csv("data.avro", "output.csv", max_records=500)

# Run integrity checks
integrity = explorer.run_integrity_checks("data.avro")
```

### Working with Results in Jupyter

```python
import pandas as pd
from IPython.display import display

# Convert JSON data to pandas DataFrame
df = pd.DataFrame(json_data)
display(df)

# Display specific metadata
print(f"Schema: {metadata['schema']}")
print(f"Record count: {metadata['record_count']}")
```

## Requirements

- Python 3.6+
- fastavro>=1.4.0

