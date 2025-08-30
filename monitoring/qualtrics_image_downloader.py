#!/usr/bin/env python3
"""
Qualtrics Survey Response and Image Downloader CLI

This utility downloads survey responses and participant-uploaded images 
from Qualtrics surveys using the Qualtrics REST API v3.
"""

import os
import sys
import time
import json
import zipfile
import argparse
import logging
import urllib.parse
import csv
import random
from typing import Optional, Dict, List
from pathlib import Path

import requests
from dotenv import load_dotenv


class QualtricsClient:
    """Client for interacting with Qualtrics API v3"""
    
    def __init__(self, api_key: str, datacenter_id: str, organization_id: str):
        self.api_key = api_key
        self.datacenter_id = datacenter_id
        self.organization_id = organization_id
        self.base_url = f"https://{datacenter_id}.qualtrics.com/API/v3"
        self.headers = {
            'X-API-TOKEN': api_key,
            'Content-Type': 'application/json'
        }
        self.retry_count = 0
        self.max_retries = 5
    
    def exponential_backoff_delay(self, attempt: int, base_delay: float = 1.0) -> float:
        """Calculate exponential backoff delay with jitter"""
        if attempt == 0:
            return 0
        
        # Exponential backoff: base_delay * (2 ^ attempt)
        delay = base_delay * (2 ** (attempt - 1))
        
        # Add jitter to avoid thundering herd problem
        jitter = random.uniform(0.1, 0.3) * delay
        total_delay = delay + jitter
        
        # Cap maximum delay at 60 seconds
        return min(total_delay, 60.0)
    
    def handle_rate_limit(self, response: requests.Response, attempt: int = 0) -> bool:
        """Handle rate limiting with exponential backoff"""
        if response.status_code == 429:
            delay = self.exponential_backoff_delay(attempt)
            logging.warning(f"Rate limited (429). Waiting {delay:.1f} seconds before retry {attempt + 1}/{self.max_retries}")
            time.sleep(delay)
            return True
        return False
        
    def create_response_export(self, survey_id: str, export_format: str = "csv") -> str:
        """Create a response export and return the export progress ID"""
        url = f"{self.base_url}/responseexports"
        
        payload = {
            "surveyId": survey_id,
            "format": export_format
        }
        
        # Only add useLabels for formats that support it
        if export_format in ["csv", "tsv", "spss"]:
            payload["useLabels"] = True
        
        for attempt in range(self.max_retries + 1):
            response = requests.post(url, json=payload, headers=self.headers)
            
            # Handle rate limiting
            if self.handle_rate_limit(response, attempt):
                if attempt < self.max_retries:
                    continue
                else:
                    raise requests.exceptions.HTTPError(f"Max retries exceeded for create export")
            
            if not response.ok:
                error_detail = response.text
                logging.error(f"API Error: {response.status_code} - {error_detail}")
            response.raise_for_status()
            
            result = response.json()
            logging.debug(f"Create export response: {result}")
            # Check if we get progressId (new format) or id (old format)
            if 'progressId' in result['result']:
                return result['result']['progressId']
            else:
                return result['result']['id']
    
    def check_export_progress(self, survey_id: str, progress_id: str) -> Dict:
        """Check the progress of a response export"""
        url = f"{self.base_url}/responseexports/{progress_id}"
        
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        
        return response.json()['result']
    
    def download_export_file(self, survey_id: str, progress_id: str, output_path: str) -> str:
        """Download the exported response file"""
        url = f"{self.base_url}/responseexports/{progress_id}/file"
        
        response = requests.get(url, headers=self.headers)
        if not response.ok:
            error_detail = response.text
            logging.error(f"File download API Error: {response.status_code} - {error_detail}")
            logging.error(f"URL: {url}")
        response.raise_for_status()
        
        with open(output_path, 'wb') as f:
            f.write(response.content)
        
        return output_path
    
    def get_survey_responses(self, survey_id: str, output_dir: str, export_format: str = "csv") -> str:
        """Download all survey responses using the 3-step export process"""
        logging.info(f"Starting export for survey {survey_id}")
        
        # Step 1: Create export
        progress_id = self.create_response_export(survey_id, export_format)
        logging.info(f"Export created with progress ID: {progress_id}")
        
        # Step 2: Wait for completion
        while True:
            progress = self.check_export_progress(survey_id, progress_id)
            percent_complete = progress.get('percentComplete', 0)
            status = progress.get('status', 'unknown')
            
            logging.info(f"Export progress: {percent_complete}% - Status: {status}")
            
            if percent_complete == 100 and status == 'complete':
                # Add a small delay to ensure file is ready for download
                time.sleep(2)
                break
            elif status == 'failed':
                raise Exception("Export failed")
            
            time.sleep(5)
        
        # Step 3: Download file
        file_extension = "csv" if export_format == "csv" else "json"
        output_path = os.path.join(output_dir, f"survey_{survey_id}_responses.{file_extension}")
        self.download_export_file(survey_id, progress_id, output_path)
        
        # Check if the file is compressed and extract it
        try:
            if zipfile.is_zipfile(output_path):
                logging.info("Extracting compressed response file...")
                with zipfile.ZipFile(output_path, 'r') as zip_ref:
                    zip_ref.extractall(output_dir)
                    # Find the extracted file (CSV or JSON)
                    extracted_files = zip_ref.namelist()
                    for fname in extracted_files:
                        if fname.endswith(('.json', '.csv')):
                            extracted_path = os.path.join(output_dir, fname)
                            logging.info(f"Responses extracted to: {extracted_path}")
                            return extracted_path
        except Exception as e:
            logging.warning(f"Could not extract compressed file, using original: {e}")
        
        logging.info(f"Responses downloaded to: {output_path}")
        return output_path
    
    def extract_image_urls_from_responses(self, responses_file: str, 
                                        target_questions: List[str]) -> Dict[str, Dict[str, List]]:
        """Extract image URLs from survey responses for specific questions, grouped by participant"""
        participant_data = {}
        
        if responses_file.endswith('.csv'):
            # Handle CSV format
            with open(responses_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                for response in reader:
                    response_id = response.get('ResponseID', response.get('ResponseId', 'unknown'))
                    random_id = response.get('RANDOM_ID', '').strip()
                    
                    # Skip header rows and invalid responses
                    if not response_id or response_id == 'unknown' or response_id.startswith('{'):
                        continue
                    
                    # Skip responses without RANDOM_ID
                    if not random_id:
                        continue
                    
                    # Look for file ID fields for our target questions
                    for target_question in target_questions:
                        file_id_key = f"{target_question}_FILE_ID"
                        file_name_key = f"{target_question}_FILE_NAME"
                        
                        file_url = response.get(file_id_key, "")
                        file_name = response.get(file_name_key, "")
                        
                        # Skip invalid URLs and header content
                        if (file_url and file_url.strip() and 
                            file_url.startswith('http') and 
                            not file_url.startswith('Please upload')):
                            
                            if random_id not in participant_data:
                                participant_data[random_id] = {}
                            
                            if response_id not in participant_data[random_id]:
                                participant_data[random_id][response_id] = []
                            
                            participant_data[random_id][response_id].append({
                                'question_id': target_question,
                                'file_url': file_url,
                                'file_name': file_name
                            })
        else:
            # Handle JSON format (legacy support)
            with open(responses_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            responses = data.get('responses', [])
            
            for response in responses:
                response_id = response.get('ResponseID', 'unknown')
                random_id = response.get('RANDOM_ID', '').strip()
                
                # Skip responses without RANDOM_ID
                if not random_id:
                    continue
                
                # Look for file ID fields for our target questions
                for target_question in target_questions:
                    file_id_key = f"{target_question}_FILE_ID"
                    file_name_key = f"{target_question}_FILE_NAME"
                    
                    file_url = response.get(file_id_key, "")
                    file_name = response.get(file_name_key, "")
                    
                    if file_url and file_url.strip():
                        if random_id not in participant_data:
                            participant_data[random_id] = {}
                        
                        if response_id not in participant_data[random_id]:
                            participant_data[random_id][response_id] = []
                        
                        participant_data[random_id][response_id].append({
                            'question_id': target_question,
                            'file_url': file_url,
                            'file_name': file_name
                        })
        
        return participant_data
    
    def extract_file_id_from_url(self, file_url: str) -> str:
        """Extract the file ID from a Qualtrics file URL"""
        # Example URL: http://s.qualtrics.com/WRQualtricsControlPanel/File.php?F=F_3EhxPhLCJ8zVl8T
        parsed_url = urllib.parse.urlparse(file_url)
        params = urllib.parse.parse_qs(parsed_url.query)
        
        # The file ID is typically in the 'F' parameter
        file_id = params.get('F', [''])[0]
        return file_id
    
    def get_file_extension_from_name(self, file_name: str) -> str:
        """Extract file extension from filename, default to common image extensions"""
        if not file_name:
            return '.png'  # Default for screenshots
        
        # Extract extension from filename
        if '.' in file_name:
            return '.' + file_name.split('.')[-1].lower()
        
        # Common image extensions for screenshots
        return '.png'
    
    def download_uploaded_file(self, survey_id: str, response_id: str, file_id: str, output_path: str) -> str:
        """Download an uploaded file using the proper API v3 endpoint with exponential backoff"""
        # Use the API v3 uploaded files endpoint
        url = f"{self.base_url}/surveys/{survey_id}/responses/{response_id}/uploaded-files/{file_id}"
        
        for attempt in range(self.max_retries + 1):
            response = requests.get(url, headers=self.headers)
            
            # Handle rate limiting with exponential backoff
            if self.handle_rate_limit(response, attempt):
                if attempt < self.max_retries:
                    continue
                else:
                    raise requests.exceptions.HTTPError(f"Max retries ({self.max_retries}) exceeded for rate limiting")
            
            # Check for other errors
            if not response.ok:
                if attempt == 0:  # Only log error details on first attempt
                    error_detail = response.text
                    logging.error(f"File download API Error: {response.status_code} - {error_detail}")
                    logging.error(f"URL: {url}")
                response.raise_for_status()
            
            # Success - write file and return
            with open(output_path, 'wb') as f:
                f.write(response.content)
            
            return output_path
        
        # This should not be reached, but just in case
        raise requests.exceptions.HTTPError(f"Failed to download file after {self.max_retries} retries")
    
    def download_file_from_url(self, file_url: str, output_path: str) -> str:
        """Download a file from Qualtrics using the direct file URL (fallback method)"""
        # These are direct URLs from Qualtrics file service
        response = requests.get(file_url, headers={'X-API-TOKEN': self.api_key})
        response.raise_for_status()
        
        with open(output_path, 'wb') as f:
            f.write(response.content)
        
        return output_path


def setup_logging(verbose: bool = False):
    """Setup logging configuration"""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )


def load_environment_variables():
    """Load environment variables from .env file"""
    # Load from credentials/.env file
    env_file = Path(__file__).parent.parent / 'credentials' / '.env'
    load_dotenv(env_file)
    
    required_vars = [
        'SURVEY_DIARY_ID',
        'QUALTRICS_API_KEY', 
        'QUALTRICS_DATACENTER_ID',
        'QUALTRICS_ORG_ID'
    ]
    
    env_vars = {}
    missing_vars = []
    
    for var in required_vars:
        value = os.getenv(var)
        if value:
            env_vars[var] = value
        else:
            missing_vars.append(var)
    
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {missing_vars}")
    
    return env_vars


def main():
    parser = argparse.ArgumentParser(
        description="Download Qualtrics survey responses and participant images"
    )
    parser.add_argument(
        '--output-dir', '-o',
        default='./downloads',
        help='Output directory for downloaded files (default: ./downloads)'
    )
    parser.add_argument(
        '--survey-id', '-s',
        help='Survey ID to download (overrides environment variable)'
    )
    parser.add_argument(
        '--responses-only',
        action='store_true',
        help='Only download survey responses, skip images'
    )
    parser.add_argument(
        '--images-only',
        action='store_true', 
        help='Only download images, skip survey responses'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.verbose)
    
    try:
        # Load environment variables
        env_vars = load_environment_variables()
        
        # Use survey ID from args or environment
        survey_id = args.survey_id or env_vars['SURVEY_DIARY_ID']
        
        # Create output directory
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize Qualtrics client
        client = QualtricsClient(
            api_key=env_vars['QUALTRICS_API_KEY'],
            datacenter_id=env_vars['QUALTRICS_DATACENTER_ID'],
            organization_id=env_vars['QUALTRICS_ORG_ID']
        )
        
        responses_file = None
        
        # Download survey responses
        if not args.images_only:
            logging.info("Downloading survey responses...")
            responses_file = client.get_survey_responses(survey_id, str(output_dir))
            logging.info("Survey responses downloaded successfully")
        
        # Download images
        if not args.responses_only:
            if not responses_file:
                # Look for existing responses file (extracted CSV or JSON)
                potential_files = [
                    output_dir / f"survey_{survey_id}_responses.csv",
                    output_dir / f"survey_{survey_id}_responses.json",
                    output_dir / "HFF Gaming Reduction 3 - Daily Survey.csv",  # Actual extracted filename
                    output_dir / "Platform 3 - Panel (Prolific) [V2 2025].json",  # Common extracted filename
                    output_dir / "Platform 3 - Panel (Prolific) [V2 2025].csv"
                ]
                
                # Check all files in output_dir for CSV and JSON files
                for file_path in output_dir.glob("*"):
                    if file_path.suffix in ['.json', '.csv']:
                        potential_files.append(file_path)
                
                responses_file = None
                for potential_file in potential_files:
                    if potential_file.exists() and potential_file.stat().st_size > 1000:  # Not empty
                        try:
                            # Test if it's a valid file
                            with open(potential_file, 'r', encoding='utf-8') as f:
                                first_line = f.readline()
                                if (first_line.strip().startswith('{') or  # JSON
                                    'ResponseID' in first_line or 'ResponseId' in first_line):  # CSV with headers
                                    responses_file = str(potential_file)
                                    logging.info(f"Using responses file: {responses_file}")
                                    break
                        except:
                            continue
                
                if not responses_file:
                    logging.error("No valid CSV or JSON responses file found. Run with --responses-only first or without --images-only")
                    return 1
            
            logging.info("Extracting image URLs from responses...")
            
            # Target questions based on the actual survey structure
            android_questions = [
                'androidBiweeklyUpload'  # Android upload field
            ]
            
            ios_questions = [
                'iosScreenshot1',  # iOS screenshot 1
                'iosScreenshot2',  # iOS screenshot 2
                'iosScreenshot3'   # iOS screenshot 3
            ]
            
            all_target_questions = android_questions + ios_questions
            
            participant_data = client.extract_image_urls_from_responses(responses_file, all_target_questions)
            
            if participant_data:
                # Create separate directories for Android and iOS
                android_dir = output_dir / "android"
                ios_dir = output_dir / "ios"
                android_dir.mkdir(exist_ok=True)
                ios_dir.mkdir(exist_ok=True)
                
                for random_id, responses in participant_data.items():
                    # Create participant directories
                    participant_android_dir = android_dir / random_id
                    participant_ios_dir = ios_dir / random_id
                    participant_android_dir.mkdir(exist_ok=True)
                    participant_ios_dir.mkdir(exist_ok=True)
                    
                    for response_id, files in responses.items():
                        # Separate files by platform
                        android_files = [f for f in files if f['question_id'] in android_questions]
                        ios_files = [f for f in files if f['question_id'] in ios_questions]
                        
                        # Process Android files
                        if android_files:
                            android_response_dir = participant_android_dir / response_id
                            android_response_dir.mkdir(exist_ok=True)
                            
                            for file_info in android_files:
                                question_id = file_info['question_id']
                                file_url = file_info['file_url']
                                file_name = file_info['file_name']
                                
                                # Extract file ID from the Qualtrics URL
                                file_id = client.extract_file_id_from_url(file_url)
                                if not file_id:
                                    logging.error(f"Could not extract file ID from URL: {file_url}")
                                    continue
                                
                                # Determine file extension
                                file_ext = client.get_file_extension_from_name(file_name)
                                
                                # Use original filename if available, otherwise generate one
                                if file_name and file_name.strip():
                                    # Ensure the filename has an extension
                                    if '.' not in file_name:
                                        filename = f"{question_id}_{file_name}{file_ext}"
                                    else:
                                        filename = f"{question_id}_{file_name}"
                                else:
                                    filename = f"{question_id}_{file_id}{file_ext}"
                                
                                output_path = android_response_dir / filename
                                
                                try:
                                    # Try the proper API v3 endpoint first (with built-in exponential backoff)
                                    client.download_uploaded_file(survey_id, response_id, file_id, str(output_path))
                                    logging.info(f"Downloaded (Android): {random_id}/{response_id}/{filename}")
                                except Exception as e:
                                    logging.warning(f"API v3 download failed for {filename}, trying fallback: {e}")
                                    
                                    try:
                                        # Fallback to direct URL method
                                        client.download_file_from_url(file_url, str(output_path))
                                        logging.info(f"Downloaded (Android fallback): {random_id}/{response_id}/{filename}")
                                    except Exception as e2:
                                        logging.error(f"Failed to download {filename}: {e2}")
                        
                        # Process iOS files
                        if ios_files:
                            ios_response_dir = participant_ios_dir / response_id
                            ios_response_dir.mkdir(exist_ok=True)
                            
                            for file_info in ios_files:
                                question_id = file_info['question_id']
                                file_url = file_info['file_url']
                                file_name = file_info['file_name']
                                
                                # Extract file ID from the Qualtrics URL
                                file_id = client.extract_file_id_from_url(file_url)
                                if not file_id:
                                    logging.error(f"Could not extract file ID from URL: {file_url}")
                                    continue
                                
                                # Determine file extension
                                file_ext = client.get_file_extension_from_name(file_name)
                                
                                # Use original filename if available, otherwise generate one
                                if file_name and file_name.strip():
                                    # Ensure the filename has an extension
                                    if '.' not in file_name:
                                        filename = f"{question_id}_{file_name}{file_ext}"
                                    else:
                                        filename = f"{question_id}_{file_name}"
                                else:
                                    filename = f"{question_id}_{file_id}{file_ext}"
                                
                                output_path = ios_response_dir / filename
                                
                                try:
                                    # Try the proper API v3 endpoint first (with built-in exponential backoff)
                                    client.download_uploaded_file(survey_id, response_id, file_id, str(output_path))
                                    logging.info(f"Downloaded (iOS): {random_id}/{response_id}/{filename}")
                                except Exception as e:
                                    logging.warning(f"API v3 download failed for {filename}, trying fallback: {e}")
                                    
                                    try:
                                        # Fallback to direct URL method
                                        client.download_file_from_url(file_url, str(output_path))
                                        logging.info(f"Downloaded (iOS fallback): {random_id}/{response_id}/{filename}")
                                    except Exception as e2:
                                        logging.error(f"Failed to download {filename}: {e2}")
                
                logging.info(f"Image downloads completed. Check {android_dir} and {ios_dir}")
            else:
                logging.info("No images found in survey responses")
        
        logging.info("All downloads completed successfully")
        return 0
        
    except Exception as e:
        logging.error(f"Error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())