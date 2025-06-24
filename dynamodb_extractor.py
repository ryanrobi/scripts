#!/usr/bin/env python3
import json
import csv
import boto3
from decimal import Decimal
from collections import defaultdict

def decimal_default(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

def list_dynamodb_tables(profile_name=None):
    """
    List all DynamoDB tables in the current region
    """
    try:
        # Initialize DynamoDB client with optional profile
        if profile_name:
            session = boto3.Session(profile_name=profile_name)
            dynamodb = session.client('dynamodb')
            print(f"Using AWS profile: {profile_name}")
        else:
            dynamodb = boto3.client('dynamodb')
            print("Using default AWS credentials")
        
        print("Fetching available DynamoDB tables...")
        
        # List all tables
        tables = []
        response = dynamodb.list_tables()
        tables.extend(response['TableNames'])
        
        # Handle pagination
        while 'LastEvaluatedTableName' in response:
            response = dynamodb.list_tables(
                ExclusiveStartTableName=response['LastEvaluatedTableName']
            )
            tables.extend(response['TableNames'])
        
        if not tables:
            print("No DynamoDB tables found in this region.")
            return None
        
        # Display tables with numbers
        print(f"\nFound {len(tables)} DynamoDB tables:")
        print("-" * 50)
        for i, table in enumerate(tables, 1):
            print(f"{i:2d}. {table}")
        print("-" * 50)
        
        return tables
        
    except Exception as e:
        print(f"Error listing tables: {e}")
        return None

def select_table_interactively(profile_name=None):
    """
    List tables and prompt user to select one
    """
    tables = list_dynamodb_tables(profile_name)
    
    if not tables:
        return None
    
    while True:
        try:
            choice = input(f"\nSelect a table (1-{len(tables)}) or 'q' to quit: ").strip()
            
            if choice.lower() == 'q':
                return None
            
            table_index = int(choice) - 1
            
            if 0 <= table_index < len(tables):
                selected_table = tables[table_index]
                print(f"Selected table: {selected_table}")
                return selected_table
            else:
                print(f"Please enter a number between 1 and {len(tables)}")
                
        except ValueError:
            print("Please enter a valid number or 'q' to quit")
        except KeyboardInterrupt:
            print("\nOperation cancelled.")
            return None

def convert_dynamodb_item_to_dict(item):
    """
    Convert a DynamoDB item to a regular Python dictionary
    """
    result = {}
    
    for key, value in item.items():
        if 'S' in value:  # String
            result[key] = value['S']
        elif 'N' in value:  # Number
            try:
                # Try to convert to int first, then float
                if '.' in value['N']:
                    result[key] = float(value['N'])
                else:
                    result[key] = int(value['N'])
            except ValueError:
                result[key] = value['N']  # Keep as string if conversion fails
        elif 'B' in value:  # Binary
            result[key] = str(value['B'])  # Convert binary to string representation
        elif 'BOOL' in value:  # Boolean
            result[key] = value['BOOL']
        elif 'NULL' in value:  # Null
            result[key] = None
        elif 'SS' in value:  # String Set
            result[key] = ', '.join(value['SS'])
        elif 'NS' in value:  # Number Set
            result[key] = ', '.join(value['NS'])
        elif 'BS' in value:  # Binary Set
            result[key] = ', '.join([str(b) for b in value['BS']])
        elif 'L' in value:  # List
            result[key] = json.dumps(value['L'], default=decimal_default)
        elif 'M' in value:  # Map (nested object)
            result[key] = json.dumps(value['M'], default=decimal_default)
        else:
            # Fallback - convert to string
            result[key] = str(value)
    
    return result

def analyze_table_schema(items, sample_size=100):
    """
    Analyze the first sample_size items to understand the schema
    """
    if not items:
        return {}
    
    # Take sample for analysis
    sample_items = items[:sample_size] if len(items) > sample_size else items
    
    # Count attribute frequency and collect unique values for potential filtering
    attribute_stats = defaultdict(lambda: {'count': 0, 'sample_values': set(), 'data_types': set()})
    
    for item in sample_items:
        for key, value in item.items():
            attribute_stats[key]['count'] += 1
            
            # Determine data type
            if 'S' in value:
                attribute_stats[key]['data_types'].add('String')
                # Keep sample values for analysis (limit to prevent memory issues)
                if len(attribute_stats[key]['sample_values']) < 10:
                    attribute_stats[key]['sample_values'].add(value['S'])
            elif 'N' in value:
                attribute_stats[key]['data_types'].add('Number')
            elif 'BOOL' in value:
                attribute_stats[key]['data_types'].add('Boolean')
            elif 'NULL' in value:
                attribute_stats[key]['data_types'].add('Null')
            elif 'SS' in value:
                attribute_stats[key]['data_types'].add('String Set')
            elif 'NS' in value:
                attribute_stats[key]['data_types'].add('Number Set')
            elif 'L' in value:
                attribute_stats[key]['data_types'].add('List')
            elif 'M' in value:
                attribute_stats[key]['data_types'].add('Map')
    
    return attribute_stats

def detect_entity_types(items, sample_size=100):
    """
    Try to detect different entity types in a single-table design
    """
    if not items:
        return {}
    
    sample_items = items[:sample_size] if len(items) > sample_size else items
    entity_patterns = defaultdict(int)
    
    for item in sample_items:
        # Common patterns for entity type detection
        patterns = []
        
        # Check for explicit entity type fields
        for key in ['EntityType', 'entity_type', 'type', 'Type']:
            if key in item and 'S' in item[key]:
                patterns.append(f"EntityType: {item[key]['S']}")
        
        # Check for sort key patterns (common in single-table design)
        for key in ['sortKey', 'SK', 'sk', 'sort_key']:
            if key in item and 'S' in item[key]:
                sk_value = item[key]['S']
                # Extract prefix before # or other delimiters
                if '#' in sk_value:
                    prefix = sk_value.split('#')[0]
                    patterns.append(f"SK prefix: {prefix}")
                elif sk_value:
                    patterns.append(f"SK: {sk_value}")
        
        # Check for partition key patterns
        for key in ['partitionKey', 'PK', 'pk', 'partition_key']:
            if key in item and 'S' in item[key]:
                pk_value = item[key]['S']
                if '#' in pk_value:
                    # Look for entity indicators in PK
                    parts = pk_value.split('#')
                    for part in parts:
                        if part and not part.isdigit():
                            patterns.append(f"PK contains: {part}")
        
        # If no patterns found, use "unknown"
        if not patterns:
            patterns.append("unknown")
        
        # Count each pattern
        for pattern in patterns:
            entity_patterns[pattern] += 1
    
    return dict(entity_patterns)

def prompt_for_filters(entity_patterns, schema_stats):
    """
    Prompt user to select filters based on detected patterns
    """
    print("\n" + "="*60)
    print("TABLE ANALYSIS COMPLETE")
    print("="*60)
    
    print(f"\nDetected {len(schema_stats)} unique attributes in the table.")
    
    if entity_patterns:
        print(f"\nDetected entity patterns (from sample data):")
        print("-" * 40)
        for pattern, count in sorted(entity_patterns.items(), key=lambda x: x[1], reverse=True):
            print(f"  {pattern}: {count} items")
    
    print(f"\nMost common attributes:")
    print("-" * 40)
    sorted_attrs = sorted(schema_stats.items(), key=lambda x: x[1]['count'], reverse=True)
    for attr, stats in sorted_attrs[:10]:  # Show top 10
        types_str = ', '.join(stats['data_types'])
        print(f"  {attr}: {stats['count']} items ({types_str})")
    
    # Prompt for filtering
    print("\n" + "="*60)
    print("FILTERING OPTIONS")
    print("="*60)
    
    filters = {}
    
    print("\nWould you like to filter the data? (y/n): ", end="")
    if input().lower().startswith('y'):
        print("\nEnter filters (press Enter with no input to finish):")
        print("Format: attribute_name=value (e.g., EntityType=USER or sortKey=user#)")
        print("Use * for wildcard matching (e.g., sortKey=user* or *Key=*user*)")
        print("For empty/null fields: fieldName= (e.g., deletedAt=)")
        print("For NOT empty fields: fieldName!= (e.g., deletedAt!=)")
        print("For NOT equal: fieldName!=value (e.g., status!=ACTIVE)")
        
        while True:
            filter_input = input("Filter: ").strip()
            if not filter_input:
                break
            
            if '=' in filter_input:
                key, value = filter_input.split('=', 1)
                filters[key.strip()] = value.strip()
                print(f"Added filter: {key.strip()} = {value.strip()}")
            else:
                print("Invalid format. Use: attribute_name=value")
    
    return filters

def apply_filters(items, filters):
    """
    Apply user-specified filters to the items
    """
    if not filters:
        return items
    
    filtered_items = []
    
    for item in items:
        match = True
        
        for filter_key, filter_value in filters.items():
            # Handle empty/null field filtering
            if filter_value == "" or filter_value == '""':
                # Check if field is missing, null, or empty string
                if filter_key not in item:
                    # Field is missing - matches empty filter
                    continue
                elif 'NULL' in item[filter_key] and item[filter_key]['NULL']:
                    # Field is explicitly null - matches empty filter
                    continue
                elif 'S' in item[filter_key] and item[filter_key]['S'] == "":
                    # Field is empty string - matches empty filter
                    continue
                else:
                    # Field has a value - doesn't match empty filter
                    match = False
                    break
            
            # Handle NOT empty filtering (field must exist and have value)
            elif filter_value.startswith("!="):
                target_value = filter_value[2:]  # Remove !=
                if filter_key not in item:
                    if target_value == "":
                        # Field is missing but we want != empty, so no match
                        match = False
                        break
                    # Field is missing and we want != some_value, so it matches
                    continue
                
                # Get the actual value from DynamoDB format
                actual_value = ""
                if 'S' in item[filter_key]:
                    actual_value = item[filter_key]['S']
                elif 'N' in item[filter_key]:
                    actual_value = item[filter_key]['N']
                elif 'BOOL' in item[filter_key]:
                    actual_value = str(item[filter_key]['BOOL'])
                elif 'NULL' in item[filter_key]:
                    actual_value = ""
                else:
                    actual_value = str(item[filter_key])
                
                if actual_value == target_value:
                    match = False
                    break
            
            # Handle regular value filtering
            else:
                if filter_key not in item:
                    match = False
                    break
                
                # Get the actual value from DynamoDB format
                actual_value = ""
                if 'S' in item[filter_key]:
                    actual_value = item[filter_key]['S']
                elif 'N' in item[filter_key]:
                    actual_value = item[filter_key]['N']
                elif 'BOOL' in item[filter_key]:
                    actual_value = str(item[filter_key]['BOOL'])
                elif 'NULL' in item[filter_key]:
                    actual_value = ""
                else:
                    actual_value = str(item[filter_key])
                
                # Apply wildcard matching
                if '*' in filter_value:
                    import fnmatch
                    if not fnmatch.fnmatch(actual_value, filter_value):
                        match = False
                        break
                else:
                    if actual_value != filter_value:
                        match = False
                        break
        
        if match:
            filtered_items.append(item)
    
    return filtered_items

def extract_dynamodb_to_csv(table_name, output_file='dynamodb_export.csv', profile_name=None):
    """
    Extract data from DynamoDB table and save to CSV with automatic schema detection
    """
    # Initialize DynamoDB client with optional profile
    if profile_name:
        session = boto3.Session(profile_name=profile_name)
        dynamodb = session.client('dynamodb')
        print(f"Using AWS profile: {profile_name}")
    else:
        dynamodb = boto3.client('dynamodb')
        print("Using default AWS credentials")
    
    try:
        # Scan the table
        print(f"Scanning table: {table_name}")
        response = dynamodb.scan(TableName=table_name)
        
        all_items = response['Items']
        
        # Handle pagination
        while 'LastEvaluatedKey' in response:
            print("Fetching next page...")
            response = dynamodb.scan(
                TableName=table_name,
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            all_items.extend(response['Items'])
        
        print(f"Total items retrieved: {len(all_items)}")
        
        if not all_items:
            print("No items found in the table.")
            return
        
        # Analyze schema and detect entity types
        print("Analyzing table schema...")
        schema_stats = analyze_table_schema(all_items)
        entity_patterns = detect_entity_types(all_items)
        
        # Prompt for filters
        filters = prompt_for_filters(entity_patterns, schema_stats)
        
        # Apply filters
        filtered_items = apply_filters(all_items, filters)
        
        if not filtered_items:
            print("No items match the specified filters.")
            return
        
        print(f"Items after filtering: {len(filtered_items)}")
        
        # Convert DynamoDB items to regular dictionaries
        simplified_items = []
        for item in filtered_items:
            simplified_items.append(convert_dynamodb_item_to_dict(item))
        
        # Get all unique keys for CSV headers
        all_keys = set()
        for item in simplified_items:
            all_keys.update(item.keys())
        
        # Sort keys for consistent output
        sorted_keys = sorted(all_keys)
        
        # Write to CSV
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=sorted_keys)
            writer.writeheader()
            writer.writerows(simplified_items)
        
        print(f"Successfully exported {len(simplified_items)} records to {output_file}")
        print(f"CSV contains {len(sorted_keys)} columns: {', '.join(sorted_keys[:5])}{'...' if len(sorted_keys) > 5 else ''}")
        
    except Exception as e:
        print(f"Error: {e}")

def extract_from_json_file(json_file, output_file='dynamodb_export.csv'):
    """
    Extract data from JSON file and convert to CSV
    """
    try:
        with open(json_file, 'r') as f:
            data = json.load(f)
        
        items = data.get('Items', [])
        print(f"Total items in JSON: {len(items)}")
        
        if not items:
            print("No items found in the JSON file.")
            return
        
        # Analyze and filter (same as direct extraction)
        schema_stats = analyze_table_schema(items)
        entity_patterns = detect_entity_types(items)
        filters = prompt_for_filters(entity_patterns, schema_stats)
        filtered_items = apply_filters(items, filters)
        
        if not filtered_items:
            print("No items match the specified filters.")
            return
        
        print(f"Items after filtering: {len(filtered_items)}")
        
        # Convert and save to CSV
        simplified_items = []
        for item in filtered_items:
            simplified_items.append(convert_dynamodb_item_to_dict(item))
        
        all_keys = set()
        for item in simplified_items:
            all_keys.update(item.keys())
        
        sorted_keys = sorted(all_keys)
        
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=sorted_keys)
            writer.writeheader()
            writer.writerows(simplified_items)
        
        print(f"Successfully exported {len(simplified_items)} records to {output_file}")
        print(f"CSV contains {len(sorted_keys)} columns: {', '.join(sorted_keys[:5])}{'...' if len(sorted_keys) > 5 else ''}")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    import sys
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Extract data from DynamoDB table to CSV with automatic schema detection',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Interactive mode - shows table list and prompts for selection
  python3 dynamodb_extractor.py --profile prod-profile

  # Direct table specification
  python3 dynamodb_extractor.py my-table --profile prod-profile
  
  # Just list available tables
  python3 dynamodb_extractor.py --list-tables --profile prod-profile
  
  # With custom output file
  python3 dynamodb_extractor.py --profile dev-profile --output data_export.csv
  
  # From JSON file
  python3 dynamodb_extractor.py --from-json data.json --output results.csv
        '''
    )
    
    parser.add_argument('table_name', nargs='?', help='DynamoDB table name (optional - will prompt if not provided)')
    parser.add_argument('--profile', '-p', help='AWS profile name')
    parser.add_argument('--output', '-o', default='dynamodb_export.csv', help='Output CSV filename (default: dynamodb_export.csv)')
    parser.add_argument('--from-json', help='Extract from JSON file instead of DynamoDB')
    parser.add_argument('--list-tables', action='store_true', help='List all available DynamoDB tables')
    
    args = parser.parse_args()
    
    if args.list_tables:
        # Just list tables and exit
        list_dynamodb_tables(args.profile)
        sys.exit(0)
    elif args.from_json:
        # Method 2: Extract from JSON file
        extract_from_json_file(args.from_json, args.output)
    elif args.table_name:
        # Method 1: Direct extraction from DynamoDB with specified table
        extract_dynamodb_to_csv(args.table_name, args.output, args.profile)
    else:
        # Method 3: Interactive table selection
        selected_table = select_table_interactively(args.profile)
        if selected_table:
            extract_dynamodb_to_csv(selected_table, args.output, args.profile)
        else:
            print("No table selected. Exiting.")
            sys.exit(0)
