import os
import sys
import json
import csv
import fastavro
from typing import List, Dict, Any, Optional


class AvroDataExplorer:
   
    def inspect_avro_file(self, file_path: str) -> Dict[str, Any]:
        """
        Inspect an Avro file and return its metadata and schema.
        
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
                    'schema': schema,
                    'sample_record': first_record
                }
        
        except FileNotFoundError:
            raise FileNotFoundError(f"File not found: {file_path}")
        except PermissionError:
            raise PermissionError(f"Permission denied to read the file: {file_path}")
        except Exception as e:
            raise Exception(f"Error inspecting Avro file: {e}")
    
    def convert_avro_to_json(
        self,
        input_file: str, 
        output_file: Optional[str] = None, 
        max_records: int = 10000
    ) -> List[Dict[str, Any]]:
        """
        Convert Avro file to JSON format.
        
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
            raise FileNotFoundError(f"File not found: {input_file}")
        except Exception as e:
            raise Exception(f"Error converting Avro to JSON: {e}")
    
    def convert_avro_to_csv(
        self,
        input_file: str, 
        output_file: Optional[str] = None, 
        max_records: int = 10000
    ) -> List[Dict[str, Any]]:
        """
        Convert Avro file to CSV format.
        
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
                    flattened_record = self._flatten_record(record)
                    flattened_records.append(flattened_record)
                
                # Write to CSV if output specified
                if output_file and flattened_records:
                    keys = flattened_records[0].keys()
                    with open(output_file, 'w', newline='') as csvfile:
                        writer = csv.DictWriter(csvfile, fieldnames=keys)
                        writer.writeheader()
                        writer.writerows(flattened_records)
                    print(f"CSV file saved to {output_file}")
                
                return flattened_records
        
        except FileNotFoundError:
            raise FileNotFoundError(f"File not found: {input_file}")
        except Exception as e:
            raise Exception(f"Error converting Avro to CSV: {e}")
    
    def _flatten_record(self, record: Dict[str, Any], parent_key: str = '', sep: str = '_') -> Dict[str, Any]:
        """
        Recursively flatten nested dictionary.
        
        """
        items = {}
        for k, v in record.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            
            if isinstance(v, dict):
                items.update(self._flatten_record(v, new_key, sep=sep))
            elif isinstance(v, list):
                # Handle list of dictionaries
                if v and isinstance(v[0], dict):
                    for i, item in enumerate(v):
                        items.update(
                            self._flatten_record(
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
    
    def run_integrity_checks(self, file_path: str) -> Dict[str, Any]:
        """
        Perform integrity checks on an Avro file.
        
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

# Convenience functions to use without instantiating the class
def inspect_avro(file_path: str) -> Dict[str, Any]:
    """
    Inspect an Avro file and return its metadata and schema.
    
    """
    explorer = AvroDataExplorer()
    return explorer.inspect_avro_file(file_path)

def avro_to_json(input_file: str, output_file: Optional[str] = None, max_records: int = 10000) -> List[Dict[str, Any]]:
    """
    Convert Avro file to JSON format.
    
    """
    explorer = AvroDataExplorer()
    return explorer.convert_avro_to_json(input_file, output_file, max_records)

def avro_to_csv(input_file: str, output_file: Optional[str] = None, max_records: int = 10000) -> List[Dict[str, Any]]:
    """
    Convert Avro file to CSV format.
    
    """
    explorer = AvroDataExplorer()
    return explorer.convert_avro_to_csv(input_file, output_file, max_records)

def check_avro_integrity(file_path: str) -> Dict[str, Any]:
    """
    Perform integrity checks on an Avro file.
    
    """
    explorer = AvroDataExplorer()
    return explorer.run_integrity_checks(file_path)