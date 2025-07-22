#!/usr/bin/env python3
"""
CLI script to parse JSON data from uploads_data.csv into tabular format.

This script extracts structured data from the json_data column in the CSV file
and converts it into separate CSV files for each data type (AfkData, AppUsage, etc.).
"""

import csv
import json
import sys
import argparse
import os
from pathlib import Path
from datetime import datetime
from dateutil import parser as date_parser
from typing import Dict, List, Any, Optional


def parse_json_data(json_str: str) -> Dict[str, List[Dict[str, Any]]]:
    """Parse JSON string and extract different data types into separate lists."""
    try:
        data = json.loads(json_str)
        result = {}
        
        for item in data:
            for key, value in item.items():
                if isinstance(value, list) and len(value) > 0:
                    # Handle different data types
                    if key == 'id':
                        # Handle ID separately - it's a special case
                        result[key] = value
                    elif key in ['AfkData', 'AppUsage', 'ScreenUnlocks', 'BucketInfo', 'log_messages', 'metadata']:
                        result[key] = value
                    elif key == 'user_omissions':
                        # Handle user_omissions which might be a string
                        if isinstance(value, str):
                            try:
                                result[key] = json.loads(value)
                            except:
                                result[key] = [{'raw_value': value}]
                        else:
                            result[key] = value
                    else:
                        result[key] = value
        
        return result
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}", file=sys.stderr)
        return {}


def extract_base_record(row: List[str]) -> Dict[str, str]:
    """Extract base record information from CSV row."""
    return {
        'id': row[0],
        'created_at': row[1],
        'submission_id': row[3],
        'platform': row[4]
    }


def create_session_datetime_string(date_str: str, time_str: str) -> str:
    """Combine date and time strings into a proper session datetime string."""
    try:
        if date_str and time_str:
            # Add seconds if missing (format HH:MM -> HH:MM:00)
            if time_str.count(':') == 1:
                time_str += ':00'
            
            # Combine date and time
            datetime_str = f"{date_str} {time_str}"
            # Parse and reformat to ensure consistent format with seconds
            dt = date_parser.parse(datetime_str)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        return ""
    except Exception:
        return ""


def create_created_at_datetime_string(created_at_str: str) -> str:
    """Parse created_at timestamp into proper datetime string with HH:MM:SS format."""
    try:
        if created_at_str:
            # Parse the created_at timestamp
            dt = date_parser.parse(created_at_str)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        return ""
    except Exception:
        return ""


def create_screen_unlocks_record(base_record: Dict[str, str], unlock_record: Dict[str, Any]) -> Dict[str, str]:
    """Create a screen unlocks record with the specified columns."""
    date_str = unlock_record.get('Date', '')
    time_str = unlock_record.get('Time', '')
    
    return {
        'session_datetime': create_session_datetime_string(date_str, time_str),
        'submission_id': base_record['submission_id'],
        'created_at_datetime': create_created_at_datetime_string(base_record['created_at'])
    }


def create_app_usage_record(base_record: Dict[str, str], app_record: Dict[str, Any]) -> Dict[str, str]:
    """Create an app usage record with the specified columns."""
    date_str = app_record.get('Date', '')
    time_str = app_record.get('Time', '')
    
    return {
        'session_datetime': create_session_datetime_string(date_str, time_str),
        'App': app_record.get('App', ''),
        'Duration (min)': app_record.get('Duration (min)', ''),
        'submission_id': base_record['submission_id'],
        'created_at_datetime': create_created_at_datetime_string(base_record['created_at'])
    }


def write_csv_file(filename: str, records: List[Dict[str, Any]]) -> None:
    """Write records to CSV file."""
    if not records:
        print(f"No records to write for {filename}")
        return
    
    # Get all unique fieldnames across all records
    fieldnames = set()
    for record in records:
        fieldnames.update(record.keys())
    
    fieldnames = sorted(fieldnames)
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)
    
    print(f"Written {len(records)} records to {filename}")


def main():
    parser = argparse.ArgumentParser(description='Parse JSON data from uploads_data.csv into tabular format')
    parser.add_argument('input_file', nargs='?', default='.tmp/uploads_data.csv', 
                        help='Input CSV file path (default: .tmp/uploads_data.csv)')
    parser.add_argument('--output-dir', '-o', default='monitoring/parsed_data',
                        help='Output directory for parsed CSV files (default: monitoring/parsed_data)')
    parser.add_argument('--limit', '-l', type=int, default=None,
                        help='Limit number of rows to process (for testing)')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Enable verbose output')
    
    args = parser.parse_args()
    
    # Check if input file exists
    if not os.path.exists(args.input_file):
        print(f"Error: Input file '{args.input_file}' not found", file=sys.stderr)
        sys.exit(1)
    
    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Set CSV field size limit to maximum
    csv.field_size_limit(sys.maxsize)
    
    # Collections for the two target tables
    screen_unlocks_records = []
    app_usage_records = []
    
    # Process CSV file
    with open(args.input_file, 'r', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        header = next(reader)
        
        if args.verbose:
            print(f"Processing file: {args.input_file}")
            print(f"Headers: {header}")
        
        processed_count = 0
        error_count = 0
        
        for row_num, row in enumerate(reader, start=2):
            if args.limit and processed_count >= args.limit:
                break
            
            try:
                base_record = extract_base_record(row)
                json_data = parse_json_data(row[2])
                
                # Process ScreenUnlocks data
                if 'ScreenUnlocks' in json_data and json_data['ScreenUnlocks']:
                    for unlock_record in json_data['ScreenUnlocks']:
                        screen_unlock = create_screen_unlocks_record(base_record, unlock_record)
                        screen_unlocks_records.append(screen_unlock)
                
                # Process AppUsage data
                if 'AppUsage' in json_data and json_data['AppUsage']:
                    for app_record in json_data['AppUsage']:
                        app_usage = create_app_usage_record(base_record, app_record)
                        app_usage_records.append(app_usage)
                
                processed_count += 1
                
                if args.verbose and processed_count % 100 == 0:
                    print(f"Processed {processed_count} rows...")
                    
            except Exception as e:
                error_count += 1
                if args.verbose:
                    print(f"Error processing row {row_num}: {e}", file=sys.stderr)
                continue
    
    # Write output files
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Write screen unlocks table
    if screen_unlocks_records:
        screen_unlocks_file = output_dir / f"screen_unlocks_{timestamp}.csv"
        write_csv_file(screen_unlocks_file, screen_unlocks_records)
    
    # Write app usage table
    if app_usage_records:
        app_usage_file = output_dir / f"app_usage_{timestamp}.csv"
        write_csv_file(app_usage_file, app_usage_records)
    
    # Summary
    print(f"\nProcessing complete!")
    print(f"Processed: {processed_count} rows")
    print(f"Errors: {error_count} rows")
    print(f"Output directory: {output_dir}")
    
    # Show record counts
    print(f"\nRecord counts:")
    print(f"  Screen unlocks: {len(screen_unlocks_records)} records")
    print(f"  App usage: {len(app_usage_records)} records")


if __name__ == "__main__":
    main()