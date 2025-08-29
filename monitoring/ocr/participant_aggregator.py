#!/usr/bin/env python3
"""
Participant Aggregator CLI for OCR Processing

This script recursively processes all participants' screenshots in the iOS diary data
using the Gemini screenshot analyzer. It provides progress monitoring and detailed
reporting for each participant.
"""

import os
import sys
import json
import argparse
import logging
import subprocess
import csv
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass
import time

# Add the current directory to Python path to import our analyzer
sys.path.insert(0, str(Path(__file__).parent))
from gemini_screenshot_analyzer import GeminiScreenshotAnalyzer, ScreenshotAnalysisError


@dataclass
class ParticipantStats:
    """Statistics for a single participant"""
    participant_id: str
    total_images: int
    processed_images: int
    successful_images: int
    failed_images: int
    images_with_warnings: int
    processing_time_seconds: float
    response_folders: List[str]
    error_details: List[Dict[str, str]]


@dataclass
class AggregatedStats:
    """Overall statistics for all participants"""
    total_participants: int
    total_images: int
    successful_participants: int
    failed_participants: int
    total_successful_images: int
    total_failed_images: int
    total_images_with_warnings: int
    total_processing_time_seconds: float
    participant_stats: List[ParticipantStats]


class ParticipantAggregator:
    """Aggregates OCR processing for all participants"""
    
    def __init__(self, base_dir: Path, analyzer: Optional[GeminiScreenshotAnalyzer] = None, 
                 qualtrics_csv_path: Optional[Path] = None):
        """Initialize the aggregator with base directory and optional Qualtrics CSV"""
        self.base_dir = base_dir
        self.analyzer = analyzer
        self.image_extensions = {'.png', '.jpg', '.jpeg', '.bmp', '.gif', '.tiff'}
        self.qualtrics_csv_path = qualtrics_csv_path
        self._response_start_dates = {}  # Cache for response ID -> StartDate mapping
        
        # Load Qualtrics CSV data if provided
        if self.qualtrics_csv_path and self.qualtrics_csv_path.exists():
            self._load_qualtrics_start_dates()
    
    def _load_qualtrics_start_dates(self):
        """Load StartDate information from Qualtrics CSV"""
        logging.info(f"Loading StartDate data from: {self.qualtrics_csv_path}")
        
        try:
            with open(self.qualtrics_csv_path, 'r', encoding='utf-8') as f:
                # Read first row to get headers
                headers = f.readline().strip().split(',')
                
                # Skip the next 2 metadata rows  
                f.readline()  # Skip row 2 (question text)
                f.readline()  # Skip row 3 (import metadata)
                
                # Now create reader with the actual data
                reader = csv.DictReader(f, fieldnames=headers)
                
                for row in reader:
                    response_id = row.get('ResponseID', '').strip()
                    start_date = row.get('StartDate', '').strip()
                    
                    if response_id and start_date and response_id.startswith('R_'):
                        # Parse the StartDate and store it
                        try:
                            # StartDate format is typically: "2023-07-18 00:44:29"
                            start_datetime = datetime.strptime(start_date.split()[0], '%Y-%m-%d')
                            self._response_start_dates[response_id] = start_datetime
                        except (ValueError, IndexError) as e:
                            logging.warning(f"Could not parse StartDate '{start_date}' for response {response_id}: {e}")
                            
            logging.info(f"Loaded {len(self._response_start_dates)} StartDate entries")
            
        except Exception as e:
            logging.error(f"Error loading Qualtrics CSV: {e}")
            
    def _get_screenshot_date_from_upload(self, response_id: str) -> Optional[str]:
        """Get screenshot date based on upload date (StartDate - 1 day)"""
        if response_id in self._response_start_dates:
            start_date = self._response_start_dates[response_id]
            screenshot_date = start_date - timedelta(days=1)
            return screenshot_date.strftime('%Y-%m-%d')
        return None
        
    def discover_participants(self, specific_participant: Optional[str] = None) -> List[Tuple[str, Path]]:
        """Discover participant directories, optionally filtered to a specific participant"""
        participants = []
        
        if not self.base_dir.exists():
            logging.error(f"Base directory does not exist: {self.base_dir}")
            return participants
        
        if specific_participant:
            # Look for specific participant
            participant_dir = self.base_dir / specific_participant
            if participant_dir.exists() and participant_dir.is_dir():
                participants.append((specific_participant, participant_dir))
                logging.info(f"Found specific participant: {specific_participant}")
            else:
                logging.error(f"Participant {specific_participant} not found in {self.base_dir}")
        else:
            # Look for all participant directories (should be numeric IDs)
            for item in self.base_dir.iterdir():
                if item.is_dir() and item.name.isdigit():
                    participants.append((item.name, item))
                    
            # Sort by participant ID for consistent processing order
            participants.sort(key=lambda x: int(x[0]))
            
            logging.info(f"Discovered {len(participants)} participants")
            
        return participants
    
    def discover_participant_images(self, participant_dir: Path) -> List[Tuple[Path, str]]:
        """Discover all images for a specific participant"""
        images = []
        
        # Look through all response folders for this participant
        for response_dir in participant_dir.iterdir():
            if response_dir.is_dir():
                # Find image files in this response directory
                for file_path in response_dir.iterdir():
                    if file_path.suffix.lower() in self.image_extensions:
                        # Skip if JSON analysis already exists
                        json_path = file_path.parent / f"{file_path.stem}_analysis.json"
                        if not json_path.exists():
                            images.append((file_path, response_dir.name))
                            
        return images
    
    def process_participant_images(self, participant_id: str, participant_dir: Path, 
                                 skip_existing: bool = True) -> ParticipantStats:
        """Process all images for a single participant"""
        start_time = time.time()
        
        logging.info(f"Processing participant {participant_id}...")
        
        # Discover images
        images_to_process = self.discover_participant_images(participant_dir)
        response_folders = list(set([resp_folder for _, resp_folder in images_to_process]))
        
        stats = ParticipantStats(
            participant_id=participant_id,
            total_images=len(images_to_process),
            processed_images=0,
            successful_images=0,
            failed_images=0,
            images_with_warnings=0,
            processing_time_seconds=0.0,
            response_folders=response_folders,
            error_details=[]
        )
        
        if stats.total_images == 0:
            logging.info(f"  No unprocessed images found for participant {participant_id}")
            stats.processing_time_seconds = time.time() - start_time
            return stats
            
        logging.info(f"  Found {stats.total_images} unprocessed images across {len(response_folders)} response folders")
        
        # Process each image
        for i, (image_path, response_folder) in enumerate(images_to_process, 1):
            logging.info(f"  Processing image {i}/{stats.total_images}: {image_path.name}")
            
            try:
                if self.analyzer:
                    # Use direct analyzer
                    result = self.analyzer.analyze_screenshot(str(image_path))
                    if result:
                        stats.successful_images += 1
                        
                        # Check for warnings
                        warnings_info = result.get('_metadata', {}).get('analysis_warnings', [])
                        if warnings_info:
                            stats.images_with_warnings += 1
                            
                        # Save JSON next to image
                        json_path = image_path.parent / f"{image_path.stem}_analysis.json"
                        with open(json_path, 'w', encoding='utf-8') as f:
                            json.dump(result, f, indent=2, ensure_ascii=False)
                    else:
                        stats.failed_images += 1
                        stats.error_details.append({
                            'image': str(image_path),
                            'response_folder': response_folder,
                            'error': 'Analysis returned None'
                        })
                else:
                    # Use subprocess to call the CLI tool
                    cmd = [
                        sys.executable, 
                        str(Path(__file__).parent / 'gemini_screenshot_analyzer.py'),
                        str(image_path),
                        '--model', 'gemini-2.0-flash-exp'
                    ]
                    
                    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
                    
                    if result.returncode == 0:
                        stats.successful_images += 1
                        
                        # Check if warnings were logged (simple heuristic)
                        if 'WARNING' in result.stderr:
                            stats.images_with_warnings += 1
                    else:
                        stats.failed_images += 1
                        error_msg = result.stderr.strip() if result.stderr else result.stdout.strip()
                        stats.error_details.append({
                            'image': str(image_path),
                            'response_folder': response_folder,
                            'error': error_msg[:200]  # Truncate long errors
                        })
                        
                stats.processed_images += 1
                
            except subprocess.TimeoutExpired:
                stats.failed_images += 1
                stats.processed_images += 1
                stats.error_details.append({
                    'image': str(image_path),
                    'response_folder': response_folder,
                    'error': 'Analysis timed out (120s)'
                })
                logging.warning(f"    Timeout processing {image_path.name}")
                
            except Exception as e:
                stats.failed_images += 1
                stats.processed_images += 1
                stats.error_details.append({
                    'image': str(image_path),
                    'response_folder': response_folder,
                    'error': str(e)[:200]
                })
                logging.error(f"    Error processing {image_path.name}: {e}")
        
        stats.processing_time_seconds = time.time() - start_time
        
        # Report participant summary
        logging.info(f"  Participant {participant_id} completed:")
        logging.info(f"    Total images: {stats.total_images}")
        logging.info(f"    Successful: {stats.successful_images}")
        logging.info(f"    Failed: {stats.failed_images}")
        logging.info(f"    With warnings: {stats.images_with_warnings}")
        logging.info(f"    Processing time: {stats.processing_time_seconds:.1f}s")
        
        return stats
    
    def create_participant_summary_report(self, participant_id: str, participant_dir: Path) -> Dict:
        """Create a concatenated report of all app usage data for a participant"""
        logging.info(f"  Creating summary report for participant {participant_id}...")
        
        report = {
            'participant_id': participant_id,
            'report_timestamp': datetime.now().isoformat(),
            'device_types': {},
            'total_daily_entries': 0,
            'date_range': {'earliest': None, 'latest': None},
            'processing_summary': {
                'total_json_files_found': 0,
                'successfully_parsed': 0,
                'parsing_errors': []
            }
        }
        
        # Find all JSON analysis files for this participant
        json_files = []
        for response_dir in participant_dir.iterdir():
            if response_dir.is_dir():
                for file_path in response_dir.iterdir():
                    if file_path.suffix == '.json' and '_analysis' in file_path.name:
                        json_files.append((file_path, response_dir.name))
        
        report['processing_summary']['total_json_files_found'] = len(json_files)
        
        # Process each JSON file
        for json_file, response_folder in json_files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # Extract key information
                device_type = data.get('device_type', 'unknown')
                device_confidence = data.get('device_type_confidence', 0.0)
                screenshot_date = data.get('date_of_screenshot', 'unknown')
                screenshot_timestamp = data.get('screenshot_timestamp', 'unknown')
                apps = data.get('apps', [])
                analysis_warnings = data.get('_metadata', {}).get('analysis_warnings', [])
                source_image = data.get('_metadata', {}).get('source_image', 'unknown')
                
                # Initialize device type if not seen before
                if device_type not in report['device_types']:
                    report['device_types'][device_type] = {
                        'daily_entries': [],
                        'total_entries': 0,
                        'total_apps_detected': 0,
                        'unique_apps': set(),
                        'confidence_scores': []
                    }
                
                device_data = report['device_types'][device_type]
                
                # Get screenshot date based on upload (StartDate - 1 day)
                screenshot_date_from_upload = self._get_screenshot_date_from_upload(response_folder)
                
                # Create daily entry
                daily_entry = {
                    'response_id': response_folder,  # The response folder name IS the response ID
                    'response_folder': response_folder,  # Keep for backwards compatibility
                    'source_image': source_image,
                    'screenshot_date': screenshot_date,
                    'screenshot_date_based_on_upload': screenshot_date_from_upload,
                    'screenshot_timestamp': screenshot_timestamp,
                    'device_type_confidence': device_confidence,
                    'apps': apps,
                    'total_apps': len(apps),
                    'total_time_minutes': sum(app.get('time_spent_minutes', 0) for app in apps),
                    'analysis_warnings': len(analysis_warnings),
                    'warning_details': analysis_warnings,
                    'date_normalized_to_2025': data.get('_metadata', {}).get('date_normalized_to_2025', False)
                }
                
                device_data['daily_entries'].append(daily_entry)
                device_data['total_entries'] += 1
                device_data['total_apps_detected'] += len(apps)
                device_data['confidence_scores'].append(device_confidence)
                
                # Track unique apps
                for app in apps:
                    device_data['unique_apps'].add(app.get('app_name', 'unknown'))
                
                # Update date range using both date sources
                dates_to_check = [screenshot_date, screenshot_date_from_upload]
                for date_str in dates_to_check:
                    if date_str and date_str != 'unknown':
                        if report['date_range']['earliest'] is None or date_str < report['date_range']['earliest']:
                            report['date_range']['earliest'] = date_str
                        if report['date_range']['latest'] is None or date_str > report['date_range']['latest']:
                            report['date_range']['latest'] = date_str
                
                report['total_daily_entries'] += 1
                report['processing_summary']['successfully_parsed'] += 1
                
            except Exception as e:
                error_detail = {
                    'file': str(json_file),
                    'response_folder': response_folder,
                    'error': str(e)
                }
                report['processing_summary']['parsing_errors'].append(error_detail)
                logging.warning(f"    Error parsing {json_file.name}: {e}")
        
        # Calculate summary statistics for each device type
        for device_type, device_data in report['device_types'].items():
            # Convert unique apps set to list for JSON serialization
            device_data['unique_apps'] = sorted(list(device_data['unique_apps']))
            device_data['unique_apps_count'] = len(device_data['unique_apps'])
            
            # Calculate average confidence
            if device_data['confidence_scores']:
                device_data['average_confidence'] = sum(device_data['confidence_scores']) / len(device_data['confidence_scores'])
            else:
                device_data['average_confidence'] = 0.0
            
            # Sort daily entries by date
            device_data['daily_entries'].sort(key=lambda x: (x['screenshot_date'], x['screenshot_timestamp']))
            
            # Calculate total time across all entries
            device_data['total_time_minutes'] = sum(entry['total_time_minutes'] for entry in device_data['daily_entries'])
        
        return report
    
    def save_participant_summary_report(self, participant_id: str, participant_dir: Path, report: Dict) -> Path:
        """Save the participant summary report to their directory"""
        report_file = participant_dir / f"participant_{participant_id}_summary.json"
        
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        logging.info(f"  Summary report saved to: {report_file}")
        
        # Log summary statistics
        total_entries = report['total_daily_entries']
        device_types = list(report['device_types'].keys())
        total_unique_apps = sum(data['unique_apps_count'] for data in report['device_types'].values())
        
        logging.info(f"  Summary: {total_entries} daily entries, {len(device_types)} device types, {total_unique_apps} unique apps")
        
        for device_type, data in report['device_types'].items():
            logging.info(f"    {device_type}: {data['total_entries']} entries, {data['unique_apps_count']} unique apps, avg confidence {data['average_confidence']:.2f}")
        
        return report_file
    
    def convert_summary_to_csv(self, summary_json_path: Path):
        """Convert summary JSON to CSV using the summary_to_csv.py script"""
        try:
            # Get the path to the summary_to_csv.py script
            script_path = Path(__file__).parent / "summary_to_csv.py"
            
            # Run the conversion script
            result = subprocess.run(
                [sys.executable, str(script_path), str(summary_json_path)],
                capture_output=True,
                text=True,
                check=True
            )
            
            if result.stdout:
                for line in result.stdout.strip().split('\n'):
                    logging.info(f"    CSV: {line}")
                    
        except subprocess.CalledProcessError as e:
            logging.warning(f"  Failed to convert summary to CSV: {e.stderr}")
        except Exception as e:
            logging.warning(f"  Failed to convert summary to CSV: {e}")
    
    def aggregate_participant_csvs(self, participants: List[Tuple[str, Path]]) -> Path:
        """Aggregate all participant CSV files into a single CSV file"""
        aggregated_csv_path = self.base_dir / "aggregated_participant_data.csv"
        
        all_rows = []
        csv_files_found = 0
        
        for participant_id, participant_dir in participants:
            csv_file = participant_dir / f"participant_{participant_id}_summary.csv"
            
            if csv_file.exists():
                try:
                    with open(csv_file, 'r', encoding='utf-8') as f:
                        reader = csv.DictReader(f)
                        for row in reader:
                            all_rows.append(row)
                    csv_files_found += 1
                except Exception as e:
                    logging.warning(f"Failed to read CSV for participant {participant_id}: {e}")
        
        if all_rows:
            # Write aggregated CSV
            fieldnames = ["PID", "DeviceType", "App", "Date", "Duration"]
            
            with open(aggregated_csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(all_rows)
            
            logging.info(f"\nAggregated CSV created: {aggregated_csv_path}")
            logging.info(f"  Participants included: {csv_files_found}")
            logging.info(f"  Total rows: {len(all_rows)}")
            
            return aggregated_csv_path
        else:
            logging.warning("No participant CSV files found to aggregate")
            return None
    
    def process_all_participants(self, skip_existing: bool = True, 
                               specific_participant: Optional[str] = None,
                               generate_summary_reports: bool = True) -> AggregatedStats:
        """Process participants and return aggregated statistics"""
        start_time = time.time()
        
        participants = self.discover_participants(specific_participant)
        
        if not participants:
            if specific_participant:
                logging.warning(f"Participant {specific_participant} not found")
            else:
                logging.warning("No participants found")
            return AggregatedStats(
                total_participants=0,
                total_images=0,
                successful_participants=0,
                failed_participants=0,
                total_successful_images=0,
                total_failed_images=0,
                total_images_with_warnings=0,
                total_processing_time_seconds=0.0,
                participant_stats=[]
            )
        
        participant_stats = []
        successful_participants = 0
        failed_participants = 0
        
        for participant_id, participant_dir in participants:
            try:
                stats = self.process_participant_images(participant_id, participant_dir, skip_existing)
                participant_stats.append(stats)
                
                # Generate participant summary report after processing images
                if generate_summary_reports:
                    try:
                        summary_report = self.create_participant_summary_report(participant_id, participant_dir)
                        summary_json_path = self.save_participant_summary_report(participant_id, participant_dir, summary_report)
                        
                        # Convert summary to CSV
                        self.convert_summary_to_csv(summary_json_path)
                    except Exception as e:
                        logging.warning(f"Failed to create summary report for participant {participant_id}: {e}")
                
                if stats.failed_images == 0:
                    successful_participants += 1
                else:
                    failed_participants += 1
                    
            except Exception as e:
                logging.error(f"Failed to process participant {participant_id}: {e}")
                failed_participants += 1
        
        # Calculate aggregated statistics
        total_processing_time = time.time() - start_time
        
        aggregated = AggregatedStats(
            total_participants=len(participants),
            total_images=sum(s.total_images for s in participant_stats),
            successful_participants=successful_participants,
            failed_participants=failed_participants,
            total_successful_images=sum(s.successful_images for s in participant_stats),
            total_failed_images=sum(s.failed_images for s in participant_stats),
            total_images_with_warnings=sum(s.images_with_warnings for s in participant_stats),
            total_processing_time_seconds=total_processing_time,
            participant_stats=participant_stats
        )
        
        # Create aggregated CSV if in group mode and summary reports were generated
        if specific_participant is None and generate_summary_reports and len(participants) > 1:
            self.aggregate_participant_csvs(participants)
        
        return aggregated


def setup_logging(verbose: bool = False):
    """Setup logging configuration"""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )


def save_report(stats: AggregatedStats, output_file: Path):
    """Save detailed report to JSON file"""
    report = {
        'summary': {
            'total_participants': stats.total_participants,
            'total_images': stats.total_images,
            'successful_participants': stats.successful_participants,
            'failed_participants': stats.failed_participants,
            'total_successful_images': stats.total_successful_images,
            'total_failed_images': stats.total_failed_images,
            'total_images_with_warnings': stats.total_images_with_warnings,
            'total_processing_time_seconds': stats.total_processing_time_seconds,
            'success_rate': (stats.total_successful_images / stats.total_images * 100) if stats.total_images > 0 else 0,
            'report_timestamp': datetime.now().isoformat()
        },
        'participants': []
    }
    
    for participant_stat in stats.participant_stats:
        participant_report = {
            'participant_id': participant_stat.participant_id,
            'total_images': participant_stat.total_images,
            'processed_images': participant_stat.processed_images,
            'successful_images': participant_stat.successful_images,
            'failed_images': participant_stat.failed_images,
            'images_with_warnings': participant_stat.images_with_warnings,
            'processing_time_seconds': participant_stat.processing_time_seconds,
            'success_rate': (participant_stat.successful_images / participant_stat.total_images * 100) if participant_stat.total_images > 0 else 0,
            'response_folders': participant_stat.response_folders,
            'error_details': participant_stat.error_details
        }
        report['participants'].append(participant_report)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)


def main():
    parser = argparse.ArgumentParser(
        description="Process participant screenshots with OCR analysis"
    )
    
    # Processing mode - mutually exclusive group
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument(
        '--participant', '-p',
        help='Process a specific participant ID (e.g., --participant 5381621023)'
    )
    mode_group.add_argument(
        '--group', '-g',
        action='store_true',
        help='Process all participants in the directory'
    )
    
    parser.add_argument(
        '--base-dir', '-d',
        default='downloads/diary_images/ios',
        help='Base directory containing participant folders (default: downloads/diary_images/ios)'
    )
    
    parser.add_argument(
        '--qualtrics-csv',
        default='downloads/diary_images/HFF Gaming Reduction 3 - Daily Survey.csv',
        help='Path to Qualtrics CSV file for StartDate lookup (default: downloads/diary_images/HFF Gaming Reduction 3 - Daily Survey.csv)'
    )
    
    parser.add_argument(
        '--output-report', '-o',
        help='Output file for detailed JSON report'
    )
    
    parser.add_argument(
        '--reprocess-existing',
        action='store_true',
        help='Reprocess images that already have analysis JSON files'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    parser.add_argument(
        '--model',
        default='gemini-2.0-flash-exp',
        choices=['gemini-1.5-flash', 'gemini-2.0-flash-exp'],
        help='Gemini model to use for analysis (default: gemini-2.0-flash-exp)'
    )
    
    parser.add_argument(
        '--participant-limit',
        type=int,
        help='Limit processing to first N participants (for testing, only works with --group)'
    )
    
    parser.add_argument(
        '--no-summary-reports',
        action='store_true',
        help='Skip generating participant summary reports'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.verbose)
    
    try:
        # Resolve base directory
        base_dir = Path(args.base_dir)
        if not base_dir.is_absolute():
            base_dir = Path.cwd() / base_dir
            
        logging.info(f"Processing participants in: {base_dir}")
        
        # Resolve Qualtrics CSV path
        qualtrics_csv_path = Path(args.qualtrics_csv)
        if not qualtrics_csv_path.is_absolute():
            qualtrics_csv_path = Path.cwd() / qualtrics_csv_path
        
        # Initialize aggregator with Qualtrics CSV
        aggregator = ParticipantAggregator(base_dir, qualtrics_csv_path=qualtrics_csv_path)
        
        # Determine processing mode and target
        if args.participant:
            logging.info(f"Processing single participant: {args.participant}")
            specific_participant = args.participant
        else:
            logging.info("Processing all participants (group mode)")
            specific_participant = None
            
            # Apply participant limit if specified
            if args.participant_limit:
                logging.info(f"Limiting to first {args.participant_limit} participants")
        
        # Process participants
        skip_existing = not args.reprocess_existing
        generate_reports = not args.no_summary_reports
        stats = aggregator.process_all_participants(skip_existing, specific_participant, generate_reports)
        
        # Apply participant limit for group mode if specified
        if args.group and args.participant_limit and len(stats.participant_stats) > args.participant_limit:
            limited_stats = stats.participant_stats[:args.participant_limit]
            stats.participant_stats = limited_stats
            stats.total_participants = len(limited_stats)
            stats.successful_participants = sum(1 for s in limited_stats if s.failed_images == 0)
            stats.failed_participants = len(limited_stats) - stats.successful_participants
            stats.total_images = sum(s.total_images for s in limited_stats)
            stats.total_successful_images = sum(s.successful_images for s in limited_stats)
            stats.total_failed_images = sum(s.failed_images for s in limited_stats)
            stats.total_images_with_warnings = sum(s.images_with_warnings for s in limited_stats)
        
        # Print summary
        logging.info("\n" + "="*60)
        if args.participant:
            logging.info(f"PARTICIPANT {args.participant} SUMMARY")
        else:
            logging.info("GROUP SUMMARY")
        logging.info("="*60)
        logging.info(f"Total participants: {stats.total_participants}")
        if stats.total_participants > 1:
            logging.info(f"Successful participants: {stats.successful_participants}")
            logging.info(f"Failed participants: {stats.failed_participants}")
        logging.info(f"Total images: {stats.total_images}")
        logging.info(f"Successful images: {stats.total_successful_images}")
        logging.info(f"Failed images: {stats.total_failed_images}")
        logging.info(f"Images with warnings: {stats.total_images_with_warnings}")
        
        if stats.total_images > 0:
            success_rate = stats.total_successful_images / stats.total_images * 100
            logging.info(f"Overall success rate: {success_rate:.1f}%")
            
        logging.info(f"Total processing time: {stats.total_processing_time_seconds:.1f}s")
        
        # Save detailed report if requested
        if args.output_report:
            output_file = Path(args.output_report)
            save_report(stats, output_file)
            logging.info(f"Detailed report saved to: {output_file}")
        
        # Print participant-by-participant breakdown
        if args.verbose and stats.participant_stats:
            logging.info("\nPER-PARTICIPANT BREAKDOWN:")
            for p_stat in stats.participant_stats:
                if p_stat.total_images > 0:
                    success_rate = p_stat.successful_images / p_stat.total_images * 100
                    logging.info(f"  {p_stat.participant_id}: {p_stat.successful_images}/{p_stat.total_images} ({success_rate:.1f}%) - {p_stat.processing_time_seconds:.1f}s")
        
        return 0 if stats.total_failed_images == 0 else 1
        
    except Exception as e:
        logging.error(f"Error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())