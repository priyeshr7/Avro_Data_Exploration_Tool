import os
import sys
import json
import csv
import fastavro
import argparse
from typing import List, Dict, Any, Optional

class AvroDataExplorer:
    """
    
    A comprehensive tool for exploring, inspecting, and converting Avro files to JSON , CSV,.
    """
    
    @staticmethod
    def inspect_avro_file(file_path: str) -> Dict[str, Any]:
        """
        Inspect an Avro file and return its metadata and schema.
        
        Args:
            file_path (str): Path to the Avro file
        
        Returns:
            Dict containing file metadata and schema information
        """
        try:
            with open(file_path, 'rb') as avro_file:
                reader = fastavro.reader(avro_file)
                
                # Extract schema
                schema = reader.schema
                
                # Count records and collect metadata
                record_count = 0
                first_record = None
                
                for record in reader:
                    record_count += 1
                    if first_record is None:
                        first_record = record
                    
                    # Limit sample record to prevent memory issues
                    if record_count >= 1:
                        break
                
                return {
                    'file_path': file_path,
                    'file_size': os.path.getsize(file_path),
                    'record_count': record_count,
                    'schema': str(schema),
                    'sample_record': first_record
                }
        
        except FileNotFoundError as e:
            print(f"Error: File not found - {file_path}")
            sys.exit(1)
        except PermissionError as e:
            print(f"Error: Permission denied to read the file - {file_path}")
            sys.exit(1)
        except Exception as e:
            print(f"Error inspecting Avro file: {e}")
            sys.exit(1)
    
    @staticmethod
    def convert_avro_to_json(
        input_file: str, 
        output_file: Optional[str] = None, 
        max_records: int = 10000
    ) -> List[Dict[str, Any]]:
        """
        Convert Avro file to JSON format.
        
        Args:
            input_file (str): Path to input Avro file
            output_file (Optional[str]): Path to output JSON file
            max_records (int): Maximum number of records to convert
        
        Returns:
            List of records in JSON format
        """
        json_records = []
        
        try:
            with open(input_file, 'rb') as avro_file:
                reader = fastavro.reader(avro_file)
                
                for i, record in enumerate(reader):
                    if i >= max_records:
                        break
                    json_records.append(record)
            
            # Write to file if output specified
            if output_file:
                with open(output_file, 'w') as f:
                    json.dump(json_records, f, indent=2)
                print(f"JSON file saved to {output_file}")
            
            return json_records
        
        except FileNotFoundError:
            print(f"Error: File not found - {input_file}")
            sys.exit(1)
        except Exception as e:
            print(f"Error converting Avro to JSON: {e}")
            sys.exit(1)
    
    @staticmethod
    def convert_avro_to_csv(
        input_file: str, 
        output_file: Optional[str] = None, 
        max_records: int = 10000
    ) -> List[Dict[str, Any]]:
        """
        Convert Avro file to CSV format.
        
        Args:
            input_file (str): Path to input Avro file
            output_file (Optional[str]): Path to output CSV file
            max_records (int): Maximum number of records to convert
        
        Returns:
            List of records in native Python format
        """
        try:
            with open(input_file, 'rb') as avro_file:
                reader = fastavro.reader(avro_file)
                records = []
                
                # Read records
                for i, record in enumerate(reader):
                    if i >= max_records:
                        break
                    records.append(record)
                
                # Flatten nested structures for CSV
                flattened_records = []
                for record in records:
                    flattened_record = AvroDataExplorer._flatten_record(record)
                    flattened_records.append(flattened_record)
                
                # Write to CSV if output specified
                if output_file:
                    if flattened_records:
                        keys = flattened_records[0].keys()
                        with open(output_file, 'w', newline='') as csvfile:
                            writer = csv.DictWriter(csvfile, fieldnames=keys)
                            writer.writeheader()
                            writer.writerows(flattened_records)
                        print(f"CSV file saved to {output_file}")
                
                return flattened_records
        
        except FileNotFoundError:
            print(f"Error: File not found - {input_file}")
            sys.exit(1)
        except Exception as e:
            print(f"Error converting Avro to CSV: {e}")
            sys.exit(1)
    
    @staticmethod
    def _flatten_record(record: Dict[str, Any], parent_key: str = '', sep: str = '_') -> Dict[str, Any]:
        """
        Recursively flatten nested dictionary.
        
        Args:
            record (Dict): Record to flatten
            parent_key (str): Parent key for nested structures
            sep (str): Separator for nested keys
        
        Returns:
            Flattened dictionary
        """
        items = {}
        for k, v in record.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            
            if isinstance(v, dict):
                items.update(AvroDataExplorer._flatten_record(v, new_key, sep=sep))
            elif isinstance(v, list):
                # Handle list of dictionaries
                if v and isinstance(v[0], dict):
                    for i, item in enumerate(v):
                        items.update(
                            AvroDataExplorer._flatten_record(
                                item, 
                                f"{new_key}_{i}", 
                                sep=sep
                            )
                        )
                else:
                    # For simple lists, join as comma-separated string
                    items[new_key] = ', '.join(map(str, v))
            else:
                items[new_key] = v
        
        return items
    
    @staticmethod
    def run_integrity_checks(file_path: str) -> Dict[str, Any]:
        """
        Perform integrity checks on an Avro file.
        
        Args:
            file_path (str): Path to Avro file
        
        Returns:
            Dictionary with integrity check results
        """
        checks = {
            'file_exists': False,
            'file_readable': False,
            'schema_valid': False,
            'records_readable': False,
            'record_count': 0,
            'error_details': None
        }
        
        try:
            # Check file existence
            if not os.path.exists(file_path):
                checks['error_details'] = 'File does not exist'
                return checks
            checks['file_exists'] = True
            
            # Check file readability
            try:
                with open(file_path, 'rb') as avro_file:
                    checks['file_readable'] = True
                    
                    # Try reading the file with fastavro
                    reader = fastavro.reader(avro_file)
                    
                    # Validate schema
                    try:
                        schema = reader.schema
                        checks['schema_valid'] = True
                    except Exception as schema_err:
                        checks['error_details'] = f'Schema validation error: {schema_err}'
                    
                    # Count records and check readability
                    record_count = 0
                    try:
                        for record in reader:
                            record_count += 1
                            # Stop after 1000 records to prevent large file processing
                            if record_count >= 1000:
                                break
                    except Exception as record_err:
                        checks['error_details'] = f'Record reading error: {record_err}'
                    else:
                        checks['records_readable'] = True
                        checks['record_count'] = record_count
            
            except PermissionError:
                checks['error_details'] = 'Permission denied to read the file'
        
        except Exception as e:
            checks['error_details'] = str(e)
        
        return checks

def main():
    """
    Command-line interface for Avro Data Explorer
    """
    parser = argparse.ArgumentParser(
        description='Avro Data Exploration Tool',
        epilog='Specify at least one operation (--inspect, --to-json, --to-csv, or --integrity)'
    )
    parser.add_argument('file', nargs='?', help='Path to Avro file')
    parser.add_argument('--inspect', action='store_true', help='Inspect Avro file metadata')
    parser.add_argument('--to-json', help='Convert Avro to JSON', metavar='OUTPUT_JSON')
    parser.add_argument('--to-csv', help='Convert Avro to CSV', metavar='OUTPUT_CSV')
    parser.add_argument('--integrity', action='store_true', help='Run integrity checks')
    parser.add_argument('--max-records', type=int, default=10000, help='Maximum records to process')
    
    # If no arguments are provided, print help and exit
    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)
    
    args = parser.parse_args()
    
    # Check if file is provided
    if not args.file:
        print("Error: Avro file path is required.")
        parser.print_help(sys.stderr)
        sys.exit(1)
    
    # Validate that at least one operation is selected
    if not (args.inspect or args.to_json or args.to_csv or args.integrity):
        print("Error: Please specify at least one operation (--inspect, --to-json, --to-csv, or --integrity)")
        parser.print_help(sys.stderr)
        sys.exit(1)
    
    # Inspect file
    if args.inspect:
        print(json.dumps(
            AvroDataExplorer.inspect_avro_file(args.file), 
            indent=2
        ))
    
    # Convert to JSON
    if args.to_json:
        records = AvroDataExplorer.convert_avro_to_json(
            args.file, 
            args.to_json, 
            max_records=args.max_records
        )
        print(f"Converted {len(records)} records to JSON")
    
    # Convert to CSV
    if args.to_csv:
        records = AvroDataExplorer.convert_avro_to_csv(
            args.file, 
            args.to_csv, 
            max_records=args.max_records
        )
        print(f"Converted {len(records)} records to CSV")
    
    # Run integrity checks
    if args.integrity:
        checks = AvroDataExplorer.run_integrity_checks(args.file)
        print(json.dumps(checks, indent=2))

if __name__ == '__main__':
    main()

