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
- To display file metadata and schema

```--inspect```  

- To convert Avro to JSON format

```--to-json OUTPUT_JSON``` 

- To convert Avro to CSV

```--to-csv OUTPUT_CSV``` 

- To run comprehensive integrity checks

```--integrity``` 

- Limit number of records processed (default: 10000)

```--max-records N```  

# Examples

1. Inspect Avro File

``` bash
python avro_data_tool.py userdata5.avro --inspect
```
2. Convert to JSON
```bash
python avro_data_tool.py userdata5.avro --to-json output_userdata5.json
```
3. Convert to CSV
```bash
python avro_data_tool.py userdata5.avro --to-csv output_userdata5.csv
```
4. Run Integrity Checks
```bash
python avro_data_tool.py userdata5.avro --integrity
```
