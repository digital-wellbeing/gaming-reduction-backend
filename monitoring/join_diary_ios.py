#!/usr/bin/env python3
"""
iOS Pipeline Monitoring Script for Gaming Reduction Study

This script ensures fresh data and preprocessing for the iOS pipeline, specifically:
1. Pulls fresh screenshot data for iOS from Qualtrics via qualtrics_image_downloader.py
2. Ensures fresh Qualtrics diary data (if not recent)
3. Ensures fresh Qualtrics contact list data (if not recent)
4. Preprocesses screenshot data via OCR scripts in monitoring/ocr

Similar to join_diary_activitywatch.py but focused on iOS screenshot analysis pipeline.
"""

import csv
import sys
import os
import subprocess
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Set, Tuple, Optional
import argparse
from dotenv import load_dotenv

# Import participant report functions from the ActivityWatch script
try:
    from join_diary_activitywatch import (
        load_contact_list_data, 
        load_diary_unique_tuples,
        write_participant_report
    )
except ImportError:
    print("Warning: Could not import participant report functions from join_diary_activitywatch.py")
    print("iOS participant report generation will be skipped")


def is_file_recent(file_path: Path, max_age_minutes: int = 60) -> bool:
    """Check if a file exists and was modified within the last N minutes."""
    if not file_path.exists():
        return False
    
    file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
    current_time = datetime.now()
    age_minutes = (current_time - file_mtime).total_seconds() / 60
    
    return age_minutes <= max_age_minutes


def pull_qualtrics_screenshots(output_dir: Path) -> bool:
    """Pull fresh screenshot data from Qualtrics using qualtrics_image_downloader.py."""
    try:
        print("Pulling fresh iOS screenshot data from Qualtrics...")
        cmd = [
            "python3", "monitoring/qualtrics_image_downloader.py",
            "--output-dir", str(output_dir / "diary_images")
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)  # 30 min timeout
        
        if result.returncode == 0:
            print("✓ iOS screenshot data successfully downloaded from Qualtrics")
            # Check if iOS directory was created with files
            ios_dir = output_dir / "diary_images" / "ios"
            if ios_dir.exists():
                participant_dirs = [d for d in ios_dir.iterdir() if d.is_dir()]
                print(f"  Found {len(participant_dirs)} iOS participant directories")
                return True
            else:
                print("⚠️ Warning: No iOS directory found after download")
                return False
        else:
            print(f"✗ Error pulling screenshot data:")
            print(f"STDOUT: {result.stdout}")
            print(f"STDERR: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print("✗ Error: Qualtrics screenshot download timed out after 30 minutes")
        return False
    except Exception as e:
        print(f"✗ Error pulling screenshot data: {e}")
        return False


def pull_diary_data(output_dir: Path) -> bool:
    """Pull fresh diary data from Qualtrics using diary_export.py."""
    try:
        diary_file = output_dir / "diary_responses_lifetime.csv"
        
        print("Pulling diary survey data from Qualtrics...")
        cmd = [
            "python3", "monitoring/diary_export.py",
            "--lifetime",
            "--filename", "diary_responses_lifetime"
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)  # 10 min timeout
        
        if result.returncode == 0:
            print("✓ Diary survey data successfully exported from Qualtrics")
            if diary_file.exists():
                file_size = diary_file.stat().st_size
                file_size_mb = file_size / (1024 * 1024)
                print(f"  File size: {file_size_mb:.2f} MB ({file_size:,} bytes)")
                return True
        else:
            print(f"✗ Error pulling diary survey data:")
            print(f"STDERR: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print("✗ Error: Diary export timed out after 10 minutes")
        return False
    except Exception as e:
        print(f"✗ Error pulling diary survey data: {e}")
        return False


def pull_contact_list_data(output_dir: Path) -> bool:
    """Pull fresh contact list data from Qualtrics using pull_contact_list.py."""
    try:
        contact_list_file = output_dir / "contact_list_with_embedded.csv"
        
        print("Pulling contact list data from Qualtrics...")
        cmd = [
            "python3", "monitoring/pull_contact_list.py",
            "--output", "contact_list_with_embedded"
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)  # 5 min timeout
        
        if result.returncode == 0:
            print("✓ Contact list data successfully exported from Qualtrics")
            if contact_list_file.exists():
                file_size = contact_list_file.stat().st_size
                file_size_mb = file_size / (1024 * 1024)
                print(f"  File size: {file_size_mb:.2f} MB ({file_size:,} bytes)")
                return True
        else:
            print(f"✗ Error pulling contact list data:")
            print(f"STDERR: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print("✗ Error: Contact list export timed out after 5 minutes")
        return False
    except Exception as e:
        print(f"✗ Error pulling contact list data: {e}")
        return False


def run_ocr_analysis() -> bool:
    """Run OCR analysis pipeline on downloaded screenshots."""
    try:
        print("\n" + "=" * 60)
        print("STEP: OCR ANALYSIS PIPELINE")
        print("=" * 60)
        
        # Step 1: Gemini screenshot analysis
        print("\nStep 1/4: Analyzing screenshots with Gemini OCR...")
        cmd = ["python3", "monitoring/ocr/gemini_screenshot_analyzer.py"]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)  # 1 hour timeout
        
        if result.returncode != 0:
            print(f"⚠️ Warning: Gemini analysis exited with non-zero status")
            print(f"STDERR: {result.stderr}")
            print(f"STDOUT: {result.stdout}")
            return False
        else:
            print("✓ Gemini screenshot analysis completed")
            
        # Step 2: Participant aggregation
        print("\nStep 2/4: Aggregating participant data...")
        cmd = ["python3", "monitoring/ocr/participant_aggregator.py"]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)  # 10 min timeout
        
        if result.returncode != 0:
            print(f"⚠️ Warning: Participant aggregation exited with non-zero status")
            print(f"STDERR: {result.stderr}")
            return False
        else:
            print("✓ Participant aggregation completed")
            
        # Step 3: Convert to CSV
        print("\nStep 3/4: Converting summary to CSV...")
        cmd = ["python3", "monitoring/ocr/summary_to_csv.py"]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)  # 5 min timeout
        
        if result.returncode != 0:
            print(f"⚠️ Warning: CSV conversion exited with non-zero status")
            print(f"STDERR: {result.stderr}")
            return False
        else:
            print("✓ CSV conversion completed")
            
        # Step 4: Gaming app classification
        print("\nStep 4/4: Classifying gaming applications...")
        cmd = ["python3", "monitoring/ocr/app_game_classifier.py"]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)  # 10 min timeout
        
        if result.returncode != 0:
            print(f"⚠️ Warning: Gaming app classification exited with non-zero status")
            print(f"STDERR: {result.stderr}")
            return False
        else:
            print("✓ Gaming app classification completed")
            
        print("\n✓ OCR analysis pipeline completed successfully!")
        return True
        
    except subprocess.TimeoutExpired as e:
        print(f"✗ Error: OCR pipeline step timed out: {e}")
        return False
    except Exception as e:
        print(f"✗ Error in OCR analysis pipeline: {e}")
        return False


def check_output_files(output_dir: Path) -> Dict[str, bool]:
    """Check if expected output files exist and report their status."""
    expected_files = {
        "diary_images": output_dir / "diary_images" / "ios",
        "diary_responses": output_dir / "diary_responses_lifetime.csv", 
        "contact_list": output_dir / "contact_list_with_embedded.csv",
        "ios_aggregated_data": output_dir.parent / "downloads" / "diary_images" / "ios" / "aggregated_participant_data_enriched.csv",
        "app_game_cache": Path("monitoring/ocr/app_game_cache.json")
    }
    
    file_status = {}
    print("\n" + "=" * 60)
    print("OUTPUT FILE STATUS CHECK")
    print("=" * 60)
    
    for file_type, file_path in expected_files.items():
        if file_path.exists():
            if file_path.is_dir():
                # Count subdirectories for iOS participant data
                subdirs = [d for d in file_path.iterdir() if d.is_dir()]
                print(f"✓ {file_type}: {len(subdirs)} participant directories found")
                file_status[file_type] = True
            else:
                file_info = file_path.stat()
                file_size_mb = file_info.st_size / (1024 * 1024)
                mod_time = datetime.fromtimestamp(file_info.st_mtime)
                print(f"✓ {file_type}: {file_size_mb:.2f} MB (modified: {mod_time.strftime('%Y-%m-%d %H:%M:%S')})")
                file_status[file_type] = True
        else:
            print(f"✗ {file_type}: Not found at {file_path}")
            file_status[file_type] = False
    
    return file_status


def generate_ios_participant_report(output_dir: Path) -> bool:
    """Generate iOS-specific participant report based on available iOS data."""
    try:
        print("\nGenerating iOS participant report...")
        
        # File paths
        diary_file = output_dir / "diary_responses_lifetime.csv"
        contact_file = output_dir / "contact_list_with_embedded.csv"
        ios_summary_file = output_dir / "ios" / "gaming_apps_summary.csv"
        
        # Load data
        if not diary_file.exists():
            print(f"⚠️ Diary file not found: {diary_file}")
            return False
            
        if not contact_file.exists():
            print(f"⚠️ Contact list file not found: {contact_file}")
            return False
        
        diary_data = load_diary_unique_tuples(str(diary_file))
        contact_data = load_contact_list_data(str(contact_file))
        
        # Create iOS participant report based on diary data and contact list
        ios_participant_report = []
        
        for random_id, submission_ids in diary_data.items():
            if random_id in contact_data:
                contact_info = contact_data[random_id]
                
                # Check if this participant has iOS data
                platforms = contact_info.get('Platforms', '')
                phone_type = contact_info.get('phoneType', '')
                has_ios_data = ('iOS' in platforms or 'iPhone' in phone_type)
                
                participant_entry = {
                    'RANDOM_ID': random_id,
                    'EnrollmentDate': contact_info.get('EnrollmentDate', ''),
                    'Condition': contact_info.get('Condition', ''),
                    'Platforms': platforms,
                    'phoneType': phone_type,
                    'submission_ids_list': str(submission_ids),
                    'num_submission_ids': len(submission_ids),
                    'has_ios_data': has_ios_data,
                    'ios_screenshots_available': 'Unknown'  # Could be enhanced to check actual iOS data
                }
                
                ios_participant_report.append(participant_entry)
        
        # Write iOS participant report
        ios_report_output = output_dir / "participant_report_ios.csv"
        write_participant_report(str(ios_report_output), ios_participant_report)
        
        print(f"✅ iOS participant report generated: {ios_report_output}")
        print(f"   Total iOS participants: {len(ios_participant_report)}")
        ios_participants = sum(1 for p in ios_participant_report if p['has_ios_data'])
        print(f"   Participants with iOS platforms: {ios_participants}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error generating iOS participant report: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description='iOS Pipeline: Pull fresh screenshot, diary, and contact data, then run OCR preprocessing'
    )
    parser.add_argument('--output-dir', default='.tmp',
                        help='Output directory for all files (default: .tmp)')
    parser.add_argument('--skip-pull', action='store_true',
                        help='Skip pulling fresh data from Qualtrics (use existing files)')
    parser.add_argument('--skip-ocr', action='store_true', 
                        help='Skip OCR preprocessing step (only pull data)')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Enable verbose output')
    parser.add_argument('--debug', action='store_true',
                        help='Skip downloading fresh data if recent files (< 60 minutes) exist')
    parser.add_argument('--cache-duration', type=int, default=60,
                        help='Cache duration in minutes for debug mode (default: 60)')
    
    args = parser.parse_args()
    
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print("=" * 60)
    print("iOS PIPELINE MONITORING SCRIPT")
    print("=" * 60)
    print(f"Output directory: {output_dir}")
    print(f"Skip pull: {args.skip_pull}")
    print(f"Skip OCR: {args.skip_ocr}")
    print(f"Debug mode: {args.debug}")
    if args.debug:
        print(f"Cache duration: {args.cache_duration} minutes")
    print("")
    
    # Step 1: Pull fresh data from external sources (unless skipped)
    if not args.skip_pull:
        # Define files to check for freshness
        screenshot_indicator = output_dir / "diary_images" / "ios"
        diary_file = output_dir / "diary_responses_lifetime.csv"
        contact_file = output_dir / "contact_list_with_embedded.csv"
        
        files_to_check = [
            (screenshot_indicator, "iOS screenshots", pull_qualtrics_screenshots),
            (diary_file, "Diary responses", pull_diary_data),
            (contact_file, "Contact list", pull_contact_list_data)
        ]
        
        if args.debug:
            print("🔍 DEBUG MODE: Checking file ages...")
            needs_refresh = []
            
            for file_path, description, pull_func in files_to_check:
                if is_file_recent(file_path, args.cache_duration):
                    if file_path.is_dir():
                        age_minutes = None  # For directories, just check existence
                        print(f"✓ {description} directory exists and is recent: {file_path}")
                    else:
                        age_minutes = (datetime.now() - datetime.fromtimestamp(file_path.stat().st_mtime)).total_seconds() / 60
                        print(f"✓ {description} file is recent ({age_minutes:.1f} min old): {file_path}")
                else:
                    print(f"✗ {description} missing or too old: {file_path}")
                    needs_refresh.append((file_path, description, pull_func))
            
            if not needs_refresh:
                print(f"\n🚀 DEBUG MODE: All files are recent (< {args.cache_duration} min), skipping data pull!")
            else:
                print(f"\n🔄 DEBUG MODE: Refreshing {len(needs_refresh)} data sources...")
                for file_path, description, pull_func in needs_refresh:
                    success = pull_func(output_dir)
                    if not success:
                        print(f"⚠️ Warning: Failed to refresh {description}")
        else:
            # Normal mode - refresh all data
            print("=" * 60)
            print("STEP 1: PULLING FRESH DATA FROM QUALTRICS")
            print("=" * 60)
            
            # Pull screenshot data
            print("\n1.1: Pulling iOS screenshot data...")
            screenshot_success = pull_qualtrics_screenshots(output_dir)
            
            # Pull diary data
            print("\n1.2: Pulling diary survey data...")
            diary_success = pull_diary_data(output_dir)
            
            # Pull contact list data  
            print("\n1.3: Pulling contact list data...")
            contact_success = pull_contact_list_data(output_dir)
            
            if not screenshot_success:
                print("⚠️ Warning: Screenshot data pull failed - OCR analysis may be incomplete")
            if not diary_success:
                print("⚠️ Warning: Diary data pull failed")
            if not contact_success:
                print("⚠️ Warning: Contact list data pull failed")
    else:
        print("=" * 60)
        print("STEP 1: SKIPPED - USING EXISTING DATA FILES")
        print("=" * 60)
    
    # Step 2: Run OCR preprocessing pipeline (unless skipped)
    if not args.skip_ocr:
        ocr_success = run_ocr_analysis()
        
        if not ocr_success:
            print("⚠️ Warning: OCR analysis pipeline had errors")
            if not args.verbose:
                print("Run with --verbose for detailed error information")
    else:
        print("\n" + "=" * 60)
        print("STEP 2: SKIPPED - OCR PREPROCESSING")
        print("=" * 60)
    
    # Step 3: Generate iOS participant report
    print("\n" + "=" * 60)
    print("STEP 3: GENERATING iOS PARTICIPANT REPORT")
    print("=" * 60)
    
    try:
        ios_report_success = generate_ios_participant_report(output_dir)
        if ios_report_success:
            print("✅ iOS participant report generated successfully")
        else:
            print("⚠️ iOS participant report generation failed or skipped")
    except Exception as e:
        print(f"❌ Error in iOS participant report generation: {e}")
    
    # Step 4: Check output files and report status
    file_status = check_output_files(output_dir)
    
    # Summary
    print("\n" + "=" * 60)
    print("iOS PIPELINE COMPLETION SUMMARY")
    print("=" * 60)
    
    success_count = sum(1 for status in file_status.values() if status)
    total_files = len(file_status)
    
    print(f"Output files status: {success_count}/{total_files} files/directories found")
    
    if success_count == total_files:
        print("✅ iOS pipeline completed successfully!")
        print("\nNext steps:")
        print("- Run monitoring/notebooks/ios_app_usage_exploration.qmd to analyze the data")
        print("- Check .tmp/ios/ directory for exported CSV files")
        return 0
    else:
        print("⚠️ iOS pipeline completed with some missing outputs")
        print("This may be normal if no iOS data was available")
        return 1


if __name__ == "__main__":
    sys.exit(main())