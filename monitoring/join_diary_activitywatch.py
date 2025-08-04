#!/usr/bin/env python3
"""
Script to join diary responses with ActivityWatch data.

This script finds unique tuples of (androidSubmissionID1, androidSubmissionID2, androidSubmissionID3) 
and RANDOM_ID from diary_responses_lifetime.csv, then performs left joins with the most recent
ActivityWatch app usage and screen unlock data based on submission_id matching.
"""

import csv
import sys
import os
import glob
import subprocess
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Set, Tuple, Optional
import argparse
from dotenv import load_dotenv
import hashlib
import json

# Import parsing functions from parse_json_uploads
from parse_json_uploads import (
    parse_json_data,
    extract_base_record,
    create_screen_unlocks_record,
    create_app_usage_record,
    write_csv_file as parse_write_csv
)


def detect_platform_from_bucket_info(bucket_info):
    """Simple platform detection from bucket info."""
    if not bucket_info:
        return 'Other'
    
    # Convert to string and check for Android indicators
    bucket_str = str(bucket_info).lower()
    if 'android' in bucket_str or 'com.' in bucket_str:
        return 'Android'
    return 'Other'


def is_file_recent(file_path: Path, max_age_minutes: int = 10) -> bool:
    """Check if a file exists and was modified within the last N minutes."""
    if not file_path.exists():
        return False
    
    file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
    current_time = datetime.now()
    age_minutes = (current_time - file_mtime).total_seconds() / 60
    
    return age_minutes <= max_age_minutes


def load_credentials():
    """Load database credentials from .env file."""
    env_path = Path(__file__).parent.parent / "credentials" / ".env"
    load_dotenv(env_path)
    
    db_password = os.getenv("SUPABASE_DB_PW")
    db_url = os.getenv("SUPABASE_DB_URL")
    
    if not db_password or not db_url:
        raise ValueError("Missing SUPABASE_DB_PW or SUPABASE_DB_URL in credentials/.env")
    
    return db_password, db_url


def pull_supabase_data(output_file: Path) -> bool:
    """Pull fresh ActivityWatch data from Supabase uploads table."""
    try:
        db_password, db_url = load_credentials()
        
        # Build psql command
        host = f"aws-0-eu-west-2.pooler.supabase.com"
        port = "5432"
        database = "postgres"
        user = f"postgres.{db_url}"
        
        # Set PGPASSWORD environment variable
        env = os.environ.copy()
        env["PGPASSWORD"] = db_password
        
        # Build psql command to export uploads table as CSV (ActivityWatch only)
        cmd = [
            "psql",
            "-h", host,
            "-p", port,
            "-d", database,
            "-U", user,
            "-c", "\\copy (SELECT * FROM uploads WHERE platform = 'ActivityWatch') TO STDOUT WITH CSV HEADER",
            "-o", str(output_file)
        ]
        
        print(f"Connecting to Supabase database...")
        print(f"Host: {host}")
        print(f"Database: {database}")
        print(f"User: {user}")
        print(f"Output file: {output_file}")
        
        # Execute command
        result = subprocess.run(cmd, env=env, capture_output=True, text=True)
        
        if result.returncode == 0:
            print(f"✓ ActivityWatch data successfully exported to {output_file}")
            if output_file.exists():
                file_size = output_file.stat().st_size
                file_size_mb = file_size / (1024 * 1024)
                print(f"File size: {file_size_mb:.2f} MB ({file_size:,} bytes)")
                return True
        else:
            print(f"✗ Error executing psql command:")
            print(f"STDERR: {result.stderr}")
            print(f"STDOUT: {result.stdout}")
            return False
            
    except Exception as e:
        print(f"✗ Error pulling ActivityWatch data: {e}")
        return False



def parse_supabase_data(input_file: Path, output_dir: Path) -> Tuple[Optional[Path], Optional[Path]]:
    """Parse the raw Supabase CSV data into app usage and screen unlocks tables."""
    try:
        print(f"Parsing JSON data from {input_file}...")
        
        # Collections for the two target tables
        screen_unlocks_records = []
        app_usage_records = []
        
        # Platform counting for summary
        platform_counts = {'Android': 0, 'Other': 0}
        
        # Set CSV field size limit to maximum
        csv.field_size_limit(sys.maxsize)
        
        # Process CSV file
        with open(input_file, 'r', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            header = next(reader)
            
            processed_count = 0
            error_count = 0
            
            for row_num, row in enumerate(reader, start=2):
                try:
                    base_record = extract_base_record(row)
                    json_data = parse_json_data(row[2])
                    
                    # Detect platform from BucketInfo
                    platform = 'Other'  # default
                    if 'BucketInfo' in json_data and json_data['BucketInfo']:
                        platform = detect_platform_from_bucket_info(json_data['BucketInfo'])
                    
                    # Count platforms
                    platform_counts[platform] += 1
                    
                    # Process ScreenUnlocks data
                    if 'ScreenUnlocks' in json_data and json_data['ScreenUnlocks']:
                        for unlock_record in json_data['ScreenUnlocks']:
                            screen_unlock = create_screen_unlocks_record(base_record, unlock_record)
                            screen_unlock['platform'] = platform
                            screen_unlocks_records.append(screen_unlock)
                    
                    # Process AppUsage data
                    if 'AppUsage' in json_data and json_data['AppUsage']:
                        for app_record in json_data['AppUsage']:
                            app_usage = create_app_usage_record(base_record, app_record)
                            app_usage['platform'] = platform
                            app_usage_records.append(app_usage)
                    
                    processed_count += 1
                    
                    if processed_count % 1000 == 0:
                        print(f"Processed {processed_count} rows...")
                        
                except Exception as e:
                    error_count += 1
                    continue
        
        # Write output files
        app_usage_file = None
        screen_unlocks_file = None
        
        # Write screen unlocks table
        if screen_unlocks_records:
            screen_unlocks_file = output_dir / "aw_screen_unlocks.csv"
            parse_write_csv(str(screen_unlocks_file), screen_unlocks_records)
        
        # Write app usage table
        if app_usage_records:
            app_usage_file = output_dir / "aw_app_usage.csv"
            parse_write_csv(str(app_usage_file), app_usage_records)
        
        print(f"✓ Parsing complete! Processed {processed_count} rows, {error_count} errors")
        print(f"  Platform distribution - Android: {platform_counts['Android']}, Other: {platform_counts['Other']}")
        print(f"  Screen unlocks: {len(screen_unlocks_records)} records")
        print(f"  App usage: {len(app_usage_records)} records")
        
        return app_usage_file, screen_unlocks_file
        
    except Exception as e:
        print(f"✗ Error parsing Supabase data: {e}")
        return None, None


def find_activitywatch_files(directory: str, filename: str) -> Optional[str]:
    """Find ActivityWatch file with the given filename."""
    file_path = os.path.join(directory, filename)
    if os.path.exists(file_path):
        return file_path
    return None


def load_diary_unique_tuples(diary_file: str) -> Dict[str, str]:
    """
    Load unique tuples of androidSubmissionIDs and map them to RANDOM_ID.
    
    Returns:
        Dict mapping submission_id -> RANDOM_ID
    """
    submission_to_random_id = {}
    seen_tuples = set()
    
    with open(diary_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            android_id1 = row.get('androidSubmissionID1', '').strip()
            android_id2 = row.get('androidSubmissionID2', '').strip()
            android_id3 = row.get('androidSubmissionID3', '').strip()
            random_id = row.get('RANDOM_ID', '').strip()
            
            # Create tuple (excluding empty values)
            android_ids = [aid for aid in [android_id1, android_id2, android_id3] if aid]
            
            if not android_ids or not random_id:
                continue
            
            # Create a sorted tuple for uniqueness check
            tuple_key = tuple(sorted(android_ids))
            
            if tuple_key in seen_tuples:
                continue
            
            seen_tuples.add(tuple_key)
            
            # Map each individual submission ID to the random ID
            for aid in android_ids:
                submission_to_random_id[aid] = random_id
    
    return submission_to_random_id


def pull_exit_survey_data(output_dir: Path) -> bool:
    """Pull fresh exit survey data from Qualtrics using exit_export.py."""
    try:
        exit_file = output_dir / "exit_responses_lifetime.csv"
        
        print("Pulling exit survey data from Qualtrics...")
        exit_cmd = [
            "python", "monitoring/exit_export.py",
            "--lifetime",
            "--filename", "exit_responses_lifetime"
        ]
        
        result = subprocess.run(exit_cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✓ Exit survey data successfully exported from Qualtrics")
            if exit_file.exists():
                file_size = exit_file.stat().st_size
                file_size_mb = file_size / (1024 * 1024)
                print(f"File size: {file_size_mb:.2f} MB ({file_size:,} bytes)")
                return True
        else:
            print(f"✗ Error pulling exit survey data:")
            print(f"STDERR: {result.stderr}")
            print(f"STDOUT: {result.stdout}")
            return False
            
    except Exception as e:
        print(f"✗ Error pulling exit survey data: {e}")
        return False


def pull_contact_list_data(output_dir: Path) -> bool:
    """Pull fresh contact list data from Qualtrics using pull_contact_list.py."""
    try:
        contact_list_file = output_dir / "contact_list_with_embedded.csv"
        
        print("Pulling contact list data from Qualtrics...")
        contact_list_cmd = [
            "python", "monitoring/pull_contact_list.py",
            "--output", "contact_list_with_embedded"
        ]
        
        result = subprocess.run(contact_list_cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✓ Contact list data successfully exported from Qualtrics")
            if contact_list_file.exists():
                file_size = contact_list_file.stat().st_size
                file_size_mb = file_size / (1024 * 1024)
                print(f"File size: {file_size_mb:.2f} MB ({file_size:,} bytes)")
                return True
        else:
            print(f"✗ Error pulling contact list data:")
            print(f"STDERR: {result.stderr}")
            print(f"STDOUT: {result.stdout}")
            return False
            
    except Exception as e:
        print(f"✗ Error pulling contact list data: {e}")
        return False


def load_exit_survey_data(exit_file: str) -> Dict[str, str]:
    """
    Load exit survey data and create submission_id -> RANDOM_ID mapping.
    Exit data has androidSubmissionID1, androidSubmissionID2, androidSubmissionID3, and RANDOM_ID.
    
    Returns:
        Dict mapping submission_id -> RANDOM_ID
    """
    submission_to_random_id = {}
    
    if not os.path.exists(exit_file):
        print(f"Warning: Exit survey file {exit_file} not found")
        return submission_to_random_id
    
    with open(exit_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            android_id1 = row.get('androidSubmissionID1', '').strip()
            android_id2 = row.get('androidSubmissionID2', '').strip()
            android_id3 = row.get('androidSubmissionID3', '').strip()
            random_id = row.get('RANDOM_ID', '').strip()
            
            if not random_id:
                continue
            
            # Map each non-empty submission ID to RANDOM_ID
            for aid in [android_id1, android_id2, android_id3]:
                if aid:
                    submission_to_random_id[aid] = random_id
    
    return submission_to_random_id


def load_contact_list_data(contact_file: str) -> Dict[str, Dict[str, str]]:
    """
    Load contact list data and map RANDOM_ID to contact variables.
    
    Returns:
        Dict mapping RANDOM_ID -> {Condition, Platforms, phoneType, EnrollmentDate}
    """
    contact_data = {}
    
    if not os.path.exists(contact_file):
        print(f"Warning: Contact list file {contact_file} not found")
        return contact_data
    
    with open(contact_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            random_id = row.get('RANDOM_ID', '').strip()
            if not random_id:
                continue
            
            contact_data[random_id] = {
                'Condition': row.get('Condition', ''),
                'Platforms': row.get('Platforms', ''),
                'phoneType': row.get('phoneType', ''),
                'EnrollmentDate': row.get('EnrollmentDate', '')
            }
    
    return contact_data


def load_activitywatch_data(file_path: str) -> List[Dict[str, str]]:
    """Load ActivityWatch data from CSV file with datetime conversion."""
    import pandas as pd
    
    # Load as pandas DataFrame to handle datetime conversion
    df = pd.read_csv(file_path)
    
    # Convert datetime columns to proper datetime types
    datetime_columns = [col for col in df.columns if col.endswith('_datetime')]
    for col in datetime_columns:
        df[col] = pd.to_datetime(df[col])
    
    # Convert back to list of dictionaries for compatibility
    data = df.to_dict('records')
    
    return data


def perform_left_join(aw_data: List[Dict[str, str]], submission_mapping: Dict[str, str], contact_data: Dict[str, Dict[str, str]]) -> List[Dict[str, str]]:
    """
    Perform left join: add RANDOM_ID and contact data to ActivityWatch data based on submission_id.
    Only returns rows where RANDOM_ID is found (matched records).
    """
    joined_data = []
    
    for row in aw_data:
        submission_id_raw = row.get('submission_id', '')
        # Handle both string and integer submission_ids
        if isinstance(submission_id_raw, (int, float)):
            submission_id = str(submission_id_raw)
        else:
            submission_id = str(submission_id_raw).strip()
        
        random_id = submission_mapping.get(submission_id, '')
        
        # Only include rows where we have a RANDOM_ID (matched records)
        if random_id:
            new_row = row.copy()
            new_row['RANDOM_ID'] = random_id
            
            # Add contact list variables if available
            if random_id in contact_data:
                contact_vars = contact_data[random_id]
                new_row['Condition'] = contact_vars.get('Condition', '')
                new_row['Platforms'] = contact_vars.get('Platforms', '')
                new_row['phoneType'] = contact_vars.get('phoneType', '')
                new_row['EnrollmentDate'] = contact_vars.get('EnrollmentDate', '')
            else:
                # Add empty columns if contact data not found
                new_row['Condition'] = ''
                new_row['Platforms'] = ''
                new_row['phoneType'] = ''
                new_row['EnrollmentDate'] = ''
            
            joined_data.append(new_row)
    
    return joined_data


def deduplicate_app_usage(data: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """
    Remove duplicate app usage sessions based on RANDOM_ID + session_datetime + App + Duration + platform.
    Keeps the first occurrence of each unique combination.
    All input data is assumed to have valid RANDOM_ID values.
    """
    seen_combinations = set()
    deduplicated_data = []
    
    for row in data:
        random_id = str(row.get('RANDOM_ID', '')).strip()
        
        # Handle datetime objects properly
        session_datetime_raw = row.get('session_datetime', '')
        if hasattr(session_datetime_raw, 'strftime'):
            # It's a datetime/Timestamp object
            session_datetime = str(session_datetime_raw)
        else:
            session_datetime = str(session_datetime_raw).strip()
        
        app = str(row.get('App', '')).strip()
        duration = str(row.get('Duration (min)', '')).strip()
        platform = str(row.get('platform', '')).strip()
        
        combination_key = (random_id, session_datetime, app, duration, platform)
        
        if combination_key in seen_combinations:
            continue
        
        seen_combinations.add(combination_key)
        deduplicated_data.append(row)
    
    return deduplicated_data


def deduplicate_screen_unlocks(data: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """
    Remove duplicate screen unlock sessions based on RANDOM_ID + session_datetime + platform.
    Keeps the first occurrence of each unique combination.
    All input data is assumed to have valid RANDOM_ID values.
    """
    seen_combinations = set()
    deduplicated_data = []
    
    for row in data:
        random_id = str(row.get('RANDOM_ID', '')).strip()
        
        # Handle datetime objects properly
        session_datetime_raw = row.get('session_datetime', '')
        if hasattr(session_datetime_raw, 'strftime'):
            # It's a datetime/Timestamp object
            session_datetime = str(session_datetime_raw)
        else:
            session_datetime = str(session_datetime_raw).strip()
        
        platform = str(row.get('platform', '')).strip()
        
        combination_key = (random_id, session_datetime, platform)
        
        if combination_key in seen_combinations:
            continue
        
        seen_combinations.add(combination_key)
        deduplicated_data.append(row)
    
    return deduplicated_data


def write_joined_data(output_file: str, data: List[Dict[str, str]]) -> None:
    """Write joined data to CSV file."""
    if not data:
        print(f"No data to write to {output_file}")
        return
    
    # Get all fieldnames, ensuring RANDOM_ID is included
    fieldnames = set()
    for row in data:
        fieldnames.update(row.keys())
    
    fieldnames = sorted(fieldnames)
    
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    
    # Report file size
    if os.path.exists(output_file):
        file_size = os.path.getsize(output_file)
        file_size_mb = file_size / (1024 * 1024)
        print(f"✓ Written {len(data)} records to {output_file}")
        print(f"File size: {file_size_mb:.2f} MB ({file_size:,} bytes)")
    else:
        print(f"✓ Written {len(data)} records to {output_file}")


def hash_data_content(data_dict: Dict[str, str]) -> str:
    """Create a hash of relevant data content for uniqueness comparison."""
    # Create a consistent representation of the data for hashing
    # Use selected fields that represent the actual data content
    relevant_fields = ['session_datetime', 'App', 'Duration (min)', 'platform']
    content_parts = []
    
    for field in relevant_fields:
        if field in data_dict:
            content_parts.append(f"{field}:{data_dict[field]}")
    
    content_string = "|".join(sorted(content_parts))
    return hashlib.md5(content_string.encode()).hexdigest()


def parse_enrollment_date(date_str: str) -> Optional[datetime]:
    """Parse enrollment date string to datetime object."""
    if not date_str:
        return None
    
    # Try different date formats that might be in the data
    formats_to_try = [
        '%Y-%m-%d',
        '%m/%d/%Y',
        '%d/%m/%Y',
        '%Y-%m-%d %H:%M:%S',
        '%m/%d/%Y %H:%M:%S'
    ]
    
    for fmt in formats_to_try:
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
    
    print(f"Warning: Could not parse enrollment date: {date_str}")
    return None


def calculate_study_day(session_datetime, enrollment_date: datetime) -> Optional[int]:
    """Calculate which study day a session falls on (1-28) relative to enrollment."""
    if not enrollment_date:
        return None
    
    # Handle different datetime formats
    if isinstance(session_datetime, str):
        try:
            session_dt = datetime.fromisoformat(session_datetime.replace('Z', '+00:00'))
        except:
            try:
                session_dt = datetime.strptime(session_datetime, '%Y-%m-%d %H:%M:%S')
            except:
                return None
    elif hasattr(session_datetime, 'to_pydatetime'):
        # Handle pandas Timestamp
        session_dt = session_datetime.to_pydatetime()
    else:
        session_dt = session_datetime
    
    # Calculate days since enrollment (0-based)
    days_since_enrollment = (session_dt.date() - enrollment_date.date()).days
    
    # Convert to 1-based study day (1-28)
    study_day = days_since_enrollment + 1
    
    # Return only if within valid study period
    if 1 <= study_day <= 28:
        return study_day
    else:
        return None


def generate_participant_report(joined_app_usage: List[Dict[str, str]], 
                              joined_screen_unlocks: List[Dict[str, str]], 
                              contact_data: Dict[str, Dict[str, str]]) -> List[Dict[str, str]]:
    """Generate participant-level report with submission counts, unique donations, and study timeline mapping."""
    
    participant_stats = {}
    
    # Process app usage data
    for record in joined_app_usage:
        random_id = record.get('RANDOM_ID', '')
        if not random_id:
            continue
            
        if random_id not in participant_stats:
            participant_stats[random_id] = {
                'RANDOM_ID': random_id,
                'submission_ids': set(),
                'unique_content_hashes': set(),
                'study_days': set(),
                'total_donations': 0,
                'data_type': 'app_usage'
            }
        
        # Count submission IDs
        submission_id = record.get('submission_id', '')
        if submission_id:
            participant_stats[random_id]['submission_ids'].add(str(submission_id))
        
        # Track unique content
        content_hash = hash_data_content(record)
        participant_stats[random_id]['unique_content_hashes'].add(content_hash)
        participant_stats[random_id]['total_donations'] += 1
        
        # Calculate study day if enrollment date available
        enrollment_date_str = record.get('EnrollmentDate', '')
        if enrollment_date_str:
            enrollment_date = parse_enrollment_date(enrollment_date_str)
            if enrollment_date:
                study_day = calculate_study_day(record.get('session_datetime'), enrollment_date)
                if study_day:
                    participant_stats[random_id]['study_days'].add(study_day)
    
    # Process screen unlocks data
    for record in joined_screen_unlocks:
        random_id = record.get('RANDOM_ID', '')
        if not random_id:
            continue
            
        if random_id not in participant_stats:
            participant_stats[random_id] = {
                'RANDOM_ID': random_id,
                'submission_ids': set(),
                'unique_content_hashes': set(),
                'study_days': set(),
                'total_donations': 0,
                'data_type': 'screen_unlocks'
            }
        elif participant_stats[random_id]['data_type'] == 'app_usage':
            # Mixed data type
            participant_stats[random_id]['data_type'] = 'mixed'
        
        # Count submission IDs
        submission_id = record.get('submission_id', '')
        if submission_id:
            participant_stats[random_id]['submission_ids'].add(str(submission_id))
        
        # Track unique content (for screen unlocks, use different fields)
        relevant_fields = ['session_datetime', 'platform']
        content_parts = []
        for field in relevant_fields:
            if field in record:
                content_parts.append(f"{field}:{record[field]}")
        content_string = "|".join(sorted(content_parts))
        content_hash = hashlib.md5(content_string.encode()).hexdigest()
        
        participant_stats[random_id]['unique_content_hashes'].add(content_hash)
        participant_stats[random_id]['total_donations'] += 1
        
        # Calculate study day if enrollment date available
        enrollment_date_str = record.get('EnrollmentDate', '')
        if enrollment_date_str:
            enrollment_date = parse_enrollment_date(enrollment_date_str)
            if enrollment_date:
                study_day = calculate_study_day(record.get('session_datetime'), enrollment_date)
                if study_day:
                    participant_stats[random_id]['study_days'].add(study_day)
    
    # Convert to report format
    report_data = []
    for random_id, stats in participant_stats.items():
        # Get contact data
        contact_info = contact_data.get(random_id, {})
        
        report_record = {
            'RANDOM_ID': random_id,
            'Condition': contact_info.get('Condition', ''),
            'Platforms': contact_info.get('Platforms', ''),
            'phoneType': contact_info.get('phoneType', ''),
            'EnrollmentDate': contact_info.get('EnrollmentDate', ''),
            'data_type': stats['data_type'],
            'num_submission_ids': len(stats['submission_ids']),
            'num_unique_donations': len(stats['unique_content_hashes']),
            'total_donation_records': stats['total_donations'],
            'uniqueness_ratio': len(stats['unique_content_hashes']) / stats['total_donations'] if stats['total_donations'] > 0 else 0,
            'study_days_with_data': sorted(list(stats['study_days'])),
            'num_study_days_with_data': len(stats['study_days']),
            'submission_ids_list': sorted(list(stats['submission_ids']))
        }
        
        report_data.append(report_record)
    
    # Sort by RANDOM_ID for consistent output
    report_data.sort(key=lambda x: x['RANDOM_ID'])
    
    return report_data


def write_participant_report(output_file: str, report_data: List[Dict[str, str]]) -> None:
    """Write participant report to CSV file."""
    if not report_data:
        print(f"No participant data to write to {output_file}")
        return
    
    fieldnames = [
        'RANDOM_ID', 'Condition', 'Platforms', 'phoneType', 'EnrollmentDate', 'data_type',
        'num_submission_ids', 'num_unique_donations', 'total_donation_records', 'uniqueness_ratio',
        'study_days_with_data', 'num_study_days_with_data', 'submission_ids_list'
    ]
    
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(report_data)
    
    # Report file size
    if os.path.exists(output_file):
        file_size = os.path.getsize(output_file)
        file_size_mb = file_size / (1024 * 1024)
        print(f"✓ Written participant report with {len(report_data)} participants to {output_file}")
        print(f"File size: {file_size_mb:.2f} MB ({file_size:,} bytes)")
    else:
        print(f"✓ Written participant report with {len(report_data)} participants to {output_file}")


def main():
    parser = argparse.ArgumentParser(description='Full pipeline: Pull ActivityWatch & diary data from Supabase, parse it, and join with enrichment')
    parser.add_argument('--diary-file', default='.tmp/diary_responses_lifetime.csv',
                        help='Path to diary responses CSV file')
    parser.add_argument('--output-dir', default='.tmp',
                        help='Output directory for all files')
    parser.add_argument('--skip-pull', action='store_true',
                        help='Skip pulling fresh ActivityWatch & diary data from Supabase (use existing files)')
    parser.add_argument('--skip-parse', action='store_true',
                        help='Skip parsing step (use existing parsed files)')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Enable verbose output')
    parser.add_argument('--debug', action='store_true',
                        help='Skip downloading fresh data if recent files (< 10 minutes) exist')
    
    args = parser.parse_args()
    
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    diary_file_path = Path(args.diary_file)
    app_usage_file = None
    screen_unlocks_file = None
    
    # Step 1: Pull fresh data from Supabase (unless skipped)
    if not args.skip_pull:
        # Check if debug mode and recent files exist
        raw_data_file = output_dir / "uploads_data.csv"
        exit_file = output_dir / "exit_responses_lifetime.csv"
        contact_list_file = output_dir / "contact_list_with_embedded.csv"
        
        if args.debug:
            # Check each file individually and determine what needs to be pulled
            files_to_check = [
                (raw_data_file, "ActivityWatch data"),
                (diary_file_path, "Diary responses"),
                (exit_file, "Exit survey data"),
                (contact_list_file, "Contact list data")
            ]
            
            files_status = {}
            any_old = False
            
            print("🔍 DEBUG MODE: Checking file ages...")
            for file_path, description in files_to_check:
                if is_file_recent(file_path):
                    age_minutes = (datetime.now() - datetime.fromtimestamp(file_path.stat().st_mtime)).total_seconds() / 60
                    print(f"✓ {description} file is recent ({age_minutes:.1f} minutes old): {file_path}")
                    files_status[description] = 'recent'
                else:
                    print(f"✗ {description} file missing or too old: {file_path}")
                    files_status[description] = 'old'
                    any_old = True
            
            if not any_old:
                print(f"\n🚀 DEBUG MODE: All files are recent (< 10 minutes), skipping entire data pull!")
                print("=" * 60)
                print("STEP 1: SKIPPED - USING ALL RECENT FILES")
                print("=" * 60)
            else:
                print(f"\n🔄 DEBUG MODE: Pulling only old/missing data, reusing recent files...")
        else:
            files_status = {}
            any_old = True
        
        if any_old or not args.debug:
            print("=" * 60)
            print("STEP 1: PULLING FRESH DATA FROM EXTERNAL SOURCES")
            print("=" * 60)
            
            # Pull ActivityWatch data (only if needed)
            if not args.debug or files_status.get("ActivityWatch data") == 'old':
                print("\n1.1: Pulling ActivityWatch data...")
                aw_success = pull_supabase_data(raw_data_file)
                
                if not aw_success:
                    print("Failed to pull ActivityWatch data from Supabase")
                    sys.exit(1)
            else:
                print("\n1.1: Skipping ActivityWatch data (recent file exists)")
                
            # Pull diary data from Qualtrics (only if needed)
            if not args.debug or files_status.get("Diary responses") == 'old':
                print("\n1.2: Pulling diary responses data from Qualtrics...")
                diary_export_cmd = [
                    "python", "monitoring/diary_export.py", 
                    "--lifetime",
                    "--filename", "diary_responses_lifetime"
                ]
                
                diary_result = subprocess.run(diary_export_cmd, capture_output=True, text=True)
                
                if diary_result.returncode == 0:
                    print("✓ Diary data successfully exported from Qualtrics")
                    if diary_file_path.exists():
                        file_size = diary_file_path.stat().st_size
                        file_size_mb = file_size / (1024 * 1024)
                        print(f"File size: {file_size_mb:.2f} MB ({file_size:,} bytes)")
                else:
                    print(f"⚠️  Warning: Failed to pull diary data from Qualtrics")
                    print(f"STDERR: {diary_result.stderr}")
                    print("The script will continue with ActivityWatch data only")
            else:
                print("\n1.2: Skipping diary responses data (recent file exists)")

            # Pull exit survey data from Qualtrics (only if needed)
            if not args.debug or files_status.get("Exit survey data") == 'old':
                print("\n1.3: Pulling exit survey data from Qualtrics...")
                exit_success = pull_exit_survey_data(output_dir)
                
                if not exit_success:
                    print("⚠️  Warning: Failed to pull exit survey data from Qualtrics")
                    print("The script will continue with diary data only")
            else:
                print("\n1.3: Skipping exit survey data (recent file exists)")

            # Pull contact list data from Qualtrics (only if needed)
            if not args.debug or files_status.get("Contact list data") == 'old':
                print("\n1.4: Pulling contact list data from Qualtrics...")
                contact_success = pull_contact_list_data(output_dir)
                
                if not contact_success:
                    print("⚠️  Warning: Failed to pull contact list data from Qualtrics")
                    print("The script will continue without contact list enrichment")
            else:
                print("\n1.4: Skipping contact list data (recent file exists)")
    else:
        # Check if diary file exists when skipping pull
        if not diary_file_path.exists():
            print(f"Error: Diary file '{args.diary_file}' not found", file=sys.stderr)
            print("Either run without --skip-pull to download fresh data, or provide an existing diary file")
            sys.exit(1)
    
    # Step 2: Parse the raw data (unless skipped)
    if not args.skip_parse:
        if not args.skip_pull:
            # We just pulled fresh data, so parse it
            print("\n" + "=" * 60)
            print("STEP 2: PARSING JSON DATA")
            print("=" * 60)
            
            raw_data_file = output_dir / "uploads_data.csv"
            app_usage_file, screen_unlocks_file = parse_supabase_data(raw_data_file, output_dir)
            
            if not app_usage_file or not screen_unlocks_file:
                print("Failed to parse Supabase data")
                sys.exit(1)
        else:
            # We skipped pull but want to parse existing raw data
            raw_data_file = output_dir / "uploads_data.csv"
            if raw_data_file.exists():
                print("\n" + "=" * 60)
                print("STEP 2: PARSING EXISTING JSON DATA")
                print("=" * 60)
                
                app_usage_file, screen_unlocks_file = parse_supabase_data(raw_data_file, output_dir)
                
                if not app_usage_file or not screen_unlocks_file:
                    print("Failed to parse existing Supabase data")
                    sys.exit(1)
            else:
                print(f"Error: Raw data file {raw_data_file} not found for parsing")
                sys.exit(1)
    
    # If we skipped parsing step, find existing files
    if args.skip_parse:
        print("\n" + "=" * 60)
        print("FINDING EXISTING ACTIVITYWATCH FILES")
        print("=" * 60)
        
        app_usage_file = find_activitywatch_files(str(output_dir), 'aw_app_usage.csv')
        screen_unlocks_file = find_activitywatch_files(str(output_dir), 'aw_screen_unlocks.csv')
        
        if not app_usage_file:
            print(f"Error: No app usage file (aw_app_usage.csv) found in {output_dir}", file=sys.stderr)
            sys.exit(1)
        
        if not screen_unlocks_file:
            print(f"Error: No screen unlocks file (aw_screen_unlocks.csv) found in {output_dir}", file=sys.stderr)
            sys.exit(1)
        
        # Convert strings to Path objects
        app_usage_file = Path(app_usage_file)
        screen_unlocks_file = Path(screen_unlocks_file)
    
    print(f"\nUsing files:")
    print(f"  Diary: {args.diary_file}")
    print(f"  App usage: {app_usage_file}")
    print(f"  Screen unlocks: {screen_unlocks_file}")
    
    # Step 3: Join with diary and exit survey responses
    print("\n" + "=" * 60)
    print("STEP 3: JOINING WITH DIARY AND EXIT SURVEY RESPONSES")
    print("=" * 60)
    
    # Load diary data and create submission_id -> RANDOM_ID mapping
    print(f"\nLoading diary responses...")
    diary_submission_mapping = load_diary_unique_tuples(args.diary_file)
    print(f"✓ Found {len(diary_submission_mapping)} unique submission_id -> RANDOM_ID mappings from diary data")
    
    # Load exit survey data and create additional submission_id -> RANDOM_ID mapping
    exit_file = output_dir / "exit_responses_lifetime.csv"
    print(f"\nLoading exit survey responses...")
    exit_submission_mapping = load_exit_survey_data(str(exit_file))
    print(f"✓ Found {len(exit_submission_mapping)} unique submission_id -> RANDOM_ID mappings from exit survey data")
    
    # Combine diary and exit mappings (exit mappings take precedence if there are conflicts)
    submission_mapping = diary_submission_mapping.copy()
    submission_mapping.update(exit_submission_mapping)
    print(f"✓ Combined total: {len(submission_mapping)} unique submission_id -> RANDOM_ID mappings")
    
    # Load contact list data for enrichment
    contact_file = output_dir / "contact_list_with_embedded.csv"
    print(f"\nLoading contact list data...")
    contact_data = load_contact_list_data(str(contact_file))
    print(f"✓ Found {len(contact_data)} contact records with Condition/Platforms/phoneType/EnrollmentDate data")
    
    if args.verbose:
        print(f"Sample submission mappings:")
        for i, (sub_id, rand_id) in enumerate(list(submission_mapping.items())[:5]):
            print(f"  {sub_id} -> {rand_id}")
        
        print(f"Sample contact data:")
        for i, (rand_id, contact_vars) in enumerate(list(contact_data.items())[:5]):
            print(f"  {rand_id} -> {contact_vars}")
    
    # Process app usage data
    print(f"\nProcessing app usage data...")
    app_usage_data = load_activitywatch_data(app_usage_file)
    print(f"✓ Loaded {len(app_usage_data)} app usage records")
    
    joined_app_usage = perform_left_join(app_usage_data, submission_mapping, contact_data)
    print(f"✓ Matched {len(joined_app_usage)}/{len(app_usage_data)} app usage records with RANDOM_ID and contact data")
    
    # Deduplicate app usage data
    print(f"Deduplicating app usage records...")
    deduplicated_app_usage = deduplicate_app_usage(joined_app_usage)
    duplicates_removed = len(joined_app_usage) - len(deduplicated_app_usage)
    print(f"✓ Removed {duplicates_removed} duplicate app usage records ({len(deduplicated_app_usage)} remaining)")
    joined_app_usage = deduplicated_app_usage
    
    # Process screen unlocks data
    print(f"\nProcessing screen unlocks data...")
    screen_unlocks_data = load_activitywatch_data(screen_unlocks_file)
    print(f"✓ Loaded {len(screen_unlocks_data)} screen unlock records")
    
    joined_screen_unlocks = perform_left_join(screen_unlocks_data, submission_mapping, contact_data)
    print(f"✓ Matched {len(joined_screen_unlocks)}/{len(screen_unlocks_data)} screen unlock records with RANDOM_ID and contact data")
    
    # Deduplicate screen unlock data
    print(f"Deduplicating screen unlock records...")
    deduplicated_screen_unlocks = deduplicate_screen_unlocks(joined_screen_unlocks)
    duplicates_removed = len(joined_screen_unlocks) - len(deduplicated_screen_unlocks)
    print(f"✓ Removed {duplicates_removed} duplicate screen unlock records ({len(deduplicated_screen_unlocks)} remaining)")
    joined_screen_unlocks = deduplicated_screen_unlocks
    
    # Write output files
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    app_usage_output = output_dir / "joined_app_usage.csv"
    screen_unlocks_output = output_dir / "joined_screen_unlocks.csv"
    
    write_joined_data(str(app_usage_output), joined_app_usage)
    write_joined_data(str(screen_unlocks_output), joined_screen_unlocks)
    
    # Generate participant-level report
    print(f"\n" + "=" * 60)
    print("STEP 4: GENERATING PARTICIPANT-LEVEL REPORT")
    print("=" * 60)
    
    print(f"Generating participant report...")
    participant_report = generate_participant_report(joined_app_usage, joined_screen_unlocks, contact_data)
    
    # Write participant report
    participant_report_output = output_dir / "participant_report.csv"
    write_participant_report(str(participant_report_output), participant_report)
    
    # Print summary statistics
    print(f"\nParticipant Report Summary:")
    print(f"  Total participants: {len(participant_report)}")
    
    if participant_report:
        # Count by data type
        data_type_counts = {}
        total_submissions = 0
        total_unique_donations = 0
        participants_with_enrollment = 0
        
        for p in participant_report:
            data_type = p['data_type']
            data_type_counts[data_type] = data_type_counts.get(data_type, 0) + 1
            total_submissions += p['num_submission_ids']
            total_unique_donations += p['num_unique_donations']
            if p['EnrollmentDate']:
                participants_with_enrollment += 1
        
        print(f"  Data type distribution: {data_type_counts}")
        print(f"  Total submission IDs across all participants: {total_submissions}")
        print(f"  Total unique donations across all participants: {total_unique_donations}")
        print(f"  Participants with enrollment date: {participants_with_enrollment}/{len(participant_report)}")
        
        # Show top 5 participants by submission count
        sorted_by_submissions = sorted(participant_report, key=lambda x: x['num_submission_ids'], reverse=True)[:5]
        print(f"  Top 5 participants by submission count:")
        for p in sorted_by_submissions:
            print(f"    {p['RANDOM_ID']}: {p['num_submission_ids']} submissions, {p['num_unique_donations']} unique donations")
    
    # Generate mapping statistics
    print(f"\n" + "=" * 60)
    print("STEP 5: MAPPING STATISTICS SUMMARY")
    print("=" * 60)
    
    # Analyze RANDOM_ID to submission_id distribution
    random_id_to_submissions = {}
    for submission_id, random_id in submission_mapping.items():
        if random_id not in random_id_to_submissions:
            random_id_to_submissions[random_id] = set()
        random_id_to_submissions[random_id].add(submission_id)
    
    # Count distribution of submission IDs per RANDOM_ID
    submission_count_distribution = {}
    for random_id, submission_ids in random_id_to_submissions.items():
        count = len(submission_ids)
        submission_count_distribution[count] = submission_count_distribution.get(count, 0) + 1
    
    print(f"\nRANDOM_ID to submission_id distribution:")
    print(f"Total RANDOM_IDs with mappings: {len(random_id_to_submissions)}")
    
    for count in sorted(submission_count_distribution.keys()):
        num_random_ids = submission_count_distribution[count]
        print(f"  RANDOM_IDs with {count} submission_id(s): {num_random_ids}")
    
    # Count submission IDs without RANDOM_ID mapping
    all_submission_ids_in_data = set()
    
    # Collect all submission IDs from ActivityWatch data
    for record in app_usage_data:
        submission_id_raw = record.get('submission_id', '')
        if isinstance(submission_id_raw, (int, float)):
            submission_id = str(submission_id_raw)
        else:
            submission_id = str(submission_id_raw).strip()
        if submission_id:
            all_submission_ids_in_data.add(submission_id)
    
    for record in screen_unlocks_data:
        submission_id_raw = record.get('submission_id', '')
        if isinstance(submission_id_raw, (int, float)):
            submission_id = str(submission_id_raw)
        else:
            submission_id = str(submission_id_raw).strip()
        if submission_id:
            all_submission_ids_in_data.add(submission_id)
    
    # Find submission IDs without RANDOM_ID mapping
    mapped_submission_ids = set(submission_mapping.keys())
    unmapped_submission_ids = all_submission_ids_in_data - mapped_submission_ids
    
    print(f"\nSubmission ID mapping coverage:")
    print(f"  Total unique submission_ids in ActivityWatch data: {len(all_submission_ids_in_data)}")
    print(f"  Submission_ids with RANDOM_ID mapping: {len(mapped_submission_ids & all_submission_ids_in_data)}")
    print(f"  Submission_ids without RANDOM_ID mapping: {len(unmapped_submission_ids)}")
    
    if len(unmapped_submission_ids) > 0:
        coverage_percent = (len(mapped_submission_ids & all_submission_ids_in_data) / len(all_submission_ids_in_data)) * 100
        print(f"  Mapping coverage: {coverage_percent:.1f}%")
        
        # Show sample of unmapped submission IDs (first 10)
        if len(unmapped_submission_ids) <= 10:
            print(f"  Unmapped submission_ids: {sorted(list(unmapped_submission_ids))}")
        else:
            sample_unmapped = sorted(list(unmapped_submission_ids))[:10]
            print(f"  Sample unmapped submission_ids (first 10): {sample_unmapped}")
            print(f"  ... and {len(unmapped_submission_ids) - 10} more")

    print(f"\n✓ Join operation completed successfully!")
    print(f"Output files:")
    print(f"  {app_usage_output}")
    print(f"  {screen_unlocks_output}")
    print(f"  {participant_report_output}")


if __name__ == "__main__":
    main()