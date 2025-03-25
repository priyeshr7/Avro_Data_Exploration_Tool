# Overview

The Avro Data Explorer is a comprehensive Python-based tool for working with Apache Avro files. It provides powerful capabilities to inspect, convert, and validate Avro data files.

# Features

- File Inspection: Retrieve detailed metadata about Avro files
- Data Conversion: Convert Avro files to JSON and CSV formats
- Integrity Checks: Validate Avro file structure and contents
# Prerequisites

fastavro
Python 3.7+

## Dependencies

``` bash
pip install -r requirements.txt

```
# Options

```--inspect```: Display file metadata and schema
```--to-json OUTPUT_JSON```: Convert Avro to JSON
```--to-csv OUTPUT_CSV```: Convert Avro to CSV
```--integrity```: Run comprehensive integrity checks
```--max-records N```: Limit number of records processed (default: 10000)

