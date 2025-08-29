#!/usr/bin/env python3
"""
CLI script to convert participant summary JSON to CSV format.

Converts participant summary JSON files to CSV with columns:
PID, DeviceType, App, Date, Duration

Date is based on screenshot_date_based_on_upload field.
Duration is time_spent_minutes from the JSON.
"""

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Dict, List, Any


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Convert participant summary JSON to CSV format",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        "json_file",
        help="Path to participant summary JSON file"
    )
    
    parser.add_argument(
        "-o", "--output",
        help="Output CSV file path (default: same name as input with .csv extension)"
    )
    
    return parser.parse_args()


def load_summary_json(json_path: Path) -> Dict[str, Any]:
    """Load and parse the participant summary JSON file."""
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: JSON file not found: {json_path}", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in file {json_path}: {e}", file=sys.stderr)
        sys.exit(1)


def extract_csv_rows(summary_data: Dict[str, Any]) -> List[Dict[str, str]]:
    """Extract CSV rows from summary JSON data."""
    rows = []
    participant_id = summary_data.get("participant_id", "")
    
    # Process each device type
    device_types = summary_data.get("device_types", {})
    
    for device_type, device_data in device_types.items():
        daily_entries = device_data.get("daily_entries", [])
        
        for entry in daily_entries:
            date = entry.get("screenshot_date_based_on_upload", "")
            apps = entry.get("apps", [])
            
            for app in apps:
                app_name = app.get("app_name", "")
                duration_minutes = app.get("time_spent_minutes", 0)
                
                # Skip apps with null or 0 duration
                if duration_minutes is None or duration_minutes == 0:
                    continue
                
                rows.append({
                    "PID": participant_id,
                    "DeviceType": device_type,
                    "App": app_name,
                    "Date": date,
                    "Duration": str(duration_minutes)
                })
    
    return rows


def write_csv(rows: List[Dict[str, str]], output_path: Path) -> None:
    """Write CSV data to file."""
    if not rows:
        print("Warning: No data rows to write to CSV", file=sys.stderr)
        return
    
    fieldnames = ["PID", "DeviceType", "App", "Date", "Duration"]
    
    try:
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        
        print(f"CSV file written: {output_path}")
        print(f"Total rows: {len(rows)}")
        
    except IOError as e:
        print(f"Error writing CSV file {output_path}: {e}", file=sys.stderr)
        sys.exit(1)


def main():
    """Main function."""
    args = parse_arguments()
    
    # Resolve input file path
    json_path = Path(args.json_file).resolve()
    
    # Determine output file path
    if args.output:
        output_path = Path(args.output).resolve()
    else:
        output_path = json_path.with_suffix('.csv')
    
    # Load JSON data
    summary_data = load_summary_json(json_path)
    
    # Extract CSV rows
    rows = extract_csv_rows(summary_data)
    
    # Write CSV file
    write_csv(rows, output_path)


if __name__ == "__main__":
    main()