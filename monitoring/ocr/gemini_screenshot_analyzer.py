#!/usr/bin/env python3
"""
Gemini Flash OCR CLI for Screenshot Analysis

This script uses Google's Gemini Flash model to perform OCR on screenshots
and extract structured data about app usage times and device information.
"""

import os
import sys
import json
import argparse
import logging
import warnings
import re
import time
from pathlib import Path
from typing import Dict, Any, Optional
import base64
from datetime import datetime

import google.generativeai as genai
from dotenv import load_dotenv


class ScreenshotAnalysisError(Exception):
    """Raised when critical analysis errors occur"""
    pass


class ScreenshotAnalysisWarning(UserWarning):
    """Warning for potential analysis issues"""
    pass


class GeminiScreenshotAnalyzer:
    """Analyzes screenshots using Gemini Flash OCR to extract app usage data"""
    
    def __init__(self, api_key: str, model_name: str = 'gemini-2.0-flash'):
        """Initialize the Gemini client with API key"""
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel(model_name)
        self.model_name = model_name
        
    def encode_image(self, image_path: str) -> str:
        """Encode image to base64 for API transmission"""
        with open(image_path, 'rb') as image_file:
            return base64.b64encode(image_file.read()).decode('utf-8')
    
    def create_analysis_prompt(self) -> str:
        """Create the prompt for Gemini to analyze screenshots"""
        return """
Analyze this screenshot and extract the following information in JSON format:

{
    "device_type": "iphone" or "desktop" (determine from UI elements and design),
    "device_type_confidence": 0.0 to 1.0 (how confident you are about device type),
    "date_of_screenshot": "YYYY-MM-DD" or "unknown" (extract from visible date/time if available),
    "screenshot_timestamp": "HH:MM" or "unknown" (time shown in status bar if visible),
    "apps": [
        {
            "app_name": "exact app name as shown",
            "time_spent": "time string as shown (e.g., '1h 12m', '40m', '5m')",
            "time_spent_minutes": integer (convert to total minutes, use 0 if unclear)
        }
    ],
    "analysis_notes": "brief description of what type of screen this is (e.g., 'iOS Screen Time report', 'Desktop task manager', etc.)"
}

Rules:
1. For device_type: Look for iOS status bar, Android navigation, Windows/Mac desktop elements
2. Extract app names exactly as they appear (e.g., "YouTube", "Instagram", "leagueoflegends.com")
3. Convert time formats to minutes: "1h 12m" = 72, "40m" = 40, "5m" = 5
4. If time is unclear or missing, use 0 for time_spent_minutes
5. Include websites/domains as app names if shown (e.g., "leagueoflegends.com", "apply.santander.co.uk")
6. Be conservative with device_type_confidence - only use >0.9 if very certain
7. IMPORTANT: For date_of_screenshot, always assume the year is 2025 unless explicitly shown otherwise. Convert dates like "July 15" to "2025-07-15", "15 July" to "2025-07-15", etc.
8. Return valid JSON only, no additional text

Analyze the image and provide the JSON response:
"""

    def analyze_screenshot(self, image_path: str) -> Optional[Dict[str, Any]]:
        """Analyze a screenshot using Gemini Flash OCR"""
        try:
            # Load and prepare the image
            image_file = genai.upload_file(path=image_path)
            
            # Create the prompt
            prompt = self.create_analysis_prompt()
            
            # Generate response
            response = self.model.generate_content([prompt, image_file])
            
            # Clean up the uploaded file
            genai.delete_file(image_file.name)
            
            # Parse JSON response
            response_text = response.text.strip()
            
            # Remove any markdown code blocks if present
            if response_text.startswith('```json'):
                response_text = response_text.replace('```json', '').replace('```', '')
            elif response_text.startswith('```'):
                response_text = response_text.replace('```', '')
            
            response_text = response_text.strip()
            
            # Parse JSON
            result = json.loads(response_text)
            
            # Add metadata
            result['_metadata'] = {
                'source_image': str(image_path),
                'analysis_timestamp': datetime.now().isoformat(),
                'model_used': self.model_name
            }
            
            # Normalize date to 2025 if needed
            self._normalize_date_to_2025(result)
            
            # Validate results and add warnings/errors
            self._validate_analysis_result(result, str(image_path))
            
            return result
            
        except json.JSONDecodeError as e:
            logging.error(f"Failed to parse JSON response: {e}")
            logging.error(f"Raw response: {response.text}")
            return None
        except Exception as e:
            logging.error(f"Error analyzing screenshot {image_path}: {e}")
            return None
    
    def _normalize_date_to_2025(self, result: Dict[str, Any]) -> None:
        """Normalize date_of_screenshot to assume 2025 year"""
        date_str = result.get('date_of_screenshot', '')
        
        if not date_str or date_str == 'unknown':
            return
        
        # If date already has 2025, leave it alone
        if '2025' in date_str:
            return
        
        # If date has a different year (like 2023, 2024), replace with 2025
        year_pattern = r'\b(20\d{2})\b'
        if re.search(year_pattern, date_str):
            normalized_date = re.sub(year_pattern, '2025', date_str)
            result['date_of_screenshot'] = normalized_date
            result['_metadata']['date_normalized_to_2025'] = True
            return
        
        # Try to parse dates without years and add 2025
        # Handle formats like "July 15", "15 July", "07-15", "07/15"
        month_patterns = [
            (r'\b(January|Jan)\s+(\d{1,2})\b', r'2025-01-\2'),
            (r'\b(February|Feb)\s+(\d{1,2})\b', r'2025-02-\2'),
            (r'\b(March|Mar)\s+(\d{1,2})\b', r'2025-03-\2'),
            (r'\b(April|Apr)\s+(\d{1,2})\b', r'2025-04-\2'),
            (r'\b(May)\s+(\d{1,2})\b', r'2025-05-\2'),
            (r'\b(June|Jun)\s+(\d{1,2})\b', r'2025-06-\2'),
            (r'\b(July|Jul)\s+(\d{1,2})\b', r'2025-07-\2'),
            (r'\b(August|Aug)\s+(\d{1,2})\b', r'2025-08-\2'),
            (r'\b(September|Sep|Sept)\s+(\d{1,2})\b', r'2025-09-\2'),
            (r'\b(October|Oct)\s+(\d{1,2})\b', r'2025-10-\2'),
            (r'\b(November|Nov)\s+(\d{1,2})\b', r'2025-11-\2'),
            (r'\b(December|Dec)\s+(\d{1,2})\b', r'2025-12-\2'),
            # Reverse order: "15 July"
            (r'\b(\d{1,2})\s+(January|Jan)\b', r'2025-01-\1'),
            (r'\b(\d{1,2})\s+(February|Feb)\b', r'2025-02-\1'),
            (r'\b(\d{1,2})\s+(March|Mar)\b', r'2025-03-\1'),
            (r'\b(\d{1,2})\s+(April|Apr)\b', r'2025-04-\1'),
            (r'\b(\d{1,2})\s+(May)\b', r'2025-05-\1'),
            (r'\b(\d{1,2})\s+(June|Jun)\b', r'2025-06-\1'),
            (r'\b(\d{1,2})\s+(July|Jul)\b', r'2025-07-\1'),
            (r'\b(\d{1,2})\s+(August|Aug)\b', r'2025-08-\1'),
            (r'\b(\d{1,2})\s+(September|Sep|Sept)\b', r'2025-09-\1'),
            (r'\b(\d{1,2})\s+(October|Oct)\b', r'2025-10-\1'),
            (r'\b(\d{1,2})\s+(November|Nov)\b', r'2025-11-\1'),
            (r'\b(\d{1,2})\s+(December|Dec)\b', r'2025-12-\1'),
        ]
        
        for pattern, replacement in month_patterns:
            if re.search(pattern, date_str, re.IGNORECASE):
                normalized_date = re.sub(pattern, replacement, date_str, flags=re.IGNORECASE)
                # Ensure day is zero-padded
                normalized_date = re.sub(r'2025-(\d{2})-(\d)$', r'2025-\1-0\2', normalized_date)
                result['date_of_screenshot'] = normalized_date
                result['_metadata']['date_normalized_to_2025'] = True
                return
        
        # Handle MM-DD or MM/DD formats
        md_pattern = r'\b(\d{1,2})[/-](\d{1,2})\b'
        if re.search(md_pattern, date_str):
            match = re.search(md_pattern, date_str)
            month, day = match.groups()
            normalized_date = f"2025-{month.zfill(2)}-{day.zfill(2)}"
            result['date_of_screenshot'] = normalized_date
            result['_metadata']['date_normalized_to_2025'] = True
            return
    
    def _validate_analysis_result(self, result: Dict[str, Any], image_path: str) -> None:
        """Validate analysis result and issue warnings/errors as needed"""
        image_name = os.path.basename(image_path)
        warnings_list = []
        
        # Check device type confidence
        device_confidence = result.get('device_type_confidence', 0.0)
        if device_confidence < 0.3:
            warning_msg = f"Low device type confidence ({device_confidence:.2f}) for {image_name}"
            warnings.warn(warning_msg, ScreenshotAnalysisWarning, stacklevel=2)
            logging.warning(warning_msg)
            warnings_list.append({
                'type': 'low_device_confidence',
                'message': warning_msg,
                'value': device_confidence
            })
        
        # Check if date is discernible
        screenshot_date = result.get('date_of_screenshot', 'unknown')
        if screenshot_date == 'unknown' or not screenshot_date:
            warning_msg = f"Date of screenshot not discernible for {image_name}"
            warnings.warn(warning_msg, ScreenshotAnalysisWarning, stacklevel=2)
            logging.warning(warning_msg)
            warnings_list.append({
                'type': 'unknown_date',
                'message': warning_msg,
                'value': screenshot_date
            })
        
        # Check number of apps detected
        apps = result.get('apps', [])
        num_apps = len(apps)
        
        if num_apps == 0:
            error_msg = f"No apps detected in {image_name} - this may indicate a failed analysis"
            logging.error(error_msg)
            raise ScreenshotAnalysisError(error_msg)
        
        elif num_apps < 2:
            warning_msg = f"Only {num_apps} app detected in {image_name} - expected multiple apps"
            warnings.warn(warning_msg, ScreenshotAnalysisWarning, stacklevel=2)
            logging.warning(warning_msg)
            warnings_list.append({
                'type': 'few_apps_detected',
                'message': warning_msg,
                'value': num_apps
            })
        
        # Add warnings to metadata if any were generated
        if warnings_list:
            result['_metadata']['analysis_warnings'] = warnings_list
        else:
            result['_metadata']['analysis_warnings'] = []


def setup_logging(verbose: bool = False):
    """Setup logging configuration"""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Configure warnings to be visible
    warnings.filterwarnings('default', category=ScreenshotAnalysisWarning)


def load_environment_variables():
    """Load environment variables from .env file"""
    # Load from credentials/.env file
    env_file = Path(__file__).parent.parent.parent / 'credentials' / '.env'
    load_dotenv(env_file)
    
    api_key = os.getenv('GOOGLE_API_KEY')
    if not api_key:
        raise ValueError("Missing GOOGLE_API_KEY in environment variables")
    
    return api_key


def process_single_image(analyzer: GeminiScreenshotAnalyzer, image_path: Path, 
                        output_dir: Optional[Path] = None, save_json: bool = True, 
                        reprocess_existing: bool = False) -> Optional[Dict[str, Any]]:
    """Process a single image and save results"""
    
    # Check if analysis already exists (unless reprocessing is requested)
    if save_json and not reprocess_existing:
        if output_dir:
            expected_output_file = output_dir / f"{image_path.stem}_analysis.json"
        else:
            expected_output_file = image_path.parent / f"{image_path.stem}_analysis.json"
            
        if expected_output_file.exists():
            logging.info(f"Skipping (already analyzed): {image_path}")
            # Load and return existing analysis
            try:
                with open(expected_output_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logging.warning(f"Failed to load existing analysis for {image_path}: {e}. Reprocessing...")
    
    logging.info(f"Analyzing: {image_path}")
    
    try:
        result = analyzer.analyze_screenshot(str(image_path))
        
        if result:
            logging.info(f"Successfully analyzed {image_path}")
            logging.info(f"Device: {result['device_type']} (confidence: {result['device_type_confidence']:.2f})")
            logging.info(f"Apps found: {len(result['apps'])}")
            
            # Check for warnings
            warnings_info = result.get('_metadata', {}).get('analysis_warnings', [])
            if warnings_info:
                logging.info(f"Analysis completed with {len(warnings_info)} warnings")
            
            # Save to file - by default next to the source image
            if save_json:
                if output_dir:
                    # Use specified output directory
                    output_dir.mkdir(parents=True, exist_ok=True)
                    output_file = output_dir / f"{image_path.stem}_analysis.json"
                else:
                    # Save next to the source image by default
                    output_file = image_path.parent / f"{image_path.stem}_analysis.json"
                
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(result, f, indent=2, ensure_ascii=False)
                
                logging.info(f"Results saved to: {output_file}")
            
            return result
        else:
            logging.error(f"Failed to analyze {image_path}")
            return None
            
    except ScreenshotAnalysisError as e:
        logging.error(f"Analysis error for {image_path}: {e}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error processing {image_path}: {e}")
        return None


def process_directory(analyzer: GeminiScreenshotAnalyzer, input_dir: Path, 
                     output_dir: Optional[Path] = None, save_json: bool = True, 
                     reprocess_existing: bool = False) -> Dict[str, Any]:
    """Process all images in a directory"""
    # Find all image files
    image_extensions = {'.png', '.jpg', '.jpeg', '.bmp', '.gif', '.tiff'}
    image_files = []
    
    for ext in image_extensions:
        image_files.extend(input_dir.rglob(f"*{ext}"))
        image_files.extend(input_dir.rglob(f"*{ext.upper()}"))
    
    if not image_files:
        logging.warning(f"No image files found in {input_dir}")
        return {"processed": 0, "successful": 0, "failed": 0, "results": []}
    
    logging.info(f"Found {len(image_files)} image files to process")
    
    results = {
        "total_found": len(image_files),
        "processed": 0,
        "skipped": 0,
        "successful": 0, 
        "failed": 0,
        "warnings": 0,
        "results": []
    }
    
    for image_path in image_files:
        # Check if file already exists before processing
        was_skipped = False
        if save_json and not reprocess_existing:
            if output_dir:
                expected_output_file = output_dir / f"{image_path.stem}_analysis.json"
            else:
                expected_output_file = image_path.parent / f"{image_path.stem}_analysis.json"
            was_skipped = expected_output_file.exists()
        
        result = process_single_image(analyzer, image_path, output_dir, save_json, reprocess_existing)
        
        if result:
            results["successful"] += 1
            results["results"].append(result)
            
            if was_skipped:
                results["skipped"] += 1
            else:
                results["processed"] += 1
            
            # Count warnings
            warnings_info = result.get('_metadata', {}).get('analysis_warnings', [])
            if warnings_info:
                results["warnings"] += 1
        else:
            results["failed"] += 1
    
    return results


def main():
    parser = argparse.ArgumentParser(
        description="Analyze screenshots using Gemini Flash OCR to extract app usage data"
    )
    
    parser.add_argument(
        'input_path',
        help='Path to image file or directory of images to analyze'
    )
    
    parser.add_argument(
        '--output-dir', '-o',
        help='Output directory for JSON analysis results'
    )
    
    parser.add_argument(
        '--pretty-print', '-p',
        action='store_true',
        help='Pretty print JSON results to stdout'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    parser.add_argument(
        '--summary-only',
        action='store_true',
        help='Only show summary statistics for directory processing'
    )
    
    parser.add_argument(
        '--no-save',
        action='store_true',
        help='Do not save JSON files (only print to stdout)'
    )
    
    parser.add_argument(
        '--model',
        default='gemini-2.0-flash',
        choices=['gemini-1.5-flash', 'gemini-2.0-flash', 'gemini-2.0-flash-exp'],
        help='Gemini model to use for analysis (default: gemini-2.0-flash)'
    )
    
    parser.add_argument(
        '--reprocess-existing',
        action='store_true',
        help='Reprocess images that already have analysis JSON files (default: skip existing)'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.verbose)
    
    try:
        # Load API key
        api_key = load_environment_variables()
        
        # Initialize analyzer with specified model
        analyzer = GeminiScreenshotAnalyzer(api_key, args.model)
        
        # Process input
        input_path = Path(args.input_path)
        output_dir = Path(args.output_dir) if args.output_dir else None
        
        if not input_path.exists():
            logging.error(f"Input path does not exist: {input_path}")
            return 1
        
        if input_path.is_file():
            # Process single file
            save_json = not args.no_save
            result = process_single_image(analyzer, input_path, output_dir, save_json, args.reprocess_existing)
            
            if result and args.pretty_print:
                print(json.dumps(result, indent=2, ensure_ascii=False))
                
            return 0 if result else 1
            
        elif input_path.is_dir():
            # Process directory
            save_json = not args.no_save
            results = process_directory(analyzer, input_path, output_dir, save_json, args.reprocess_existing)
            
            # Print summary
            logging.info(f"\nProcessing Summary:")
            logging.info(f"Total found: {results['total_found']}")
            logging.info(f"Skipped (already analyzed): {results['skipped']}")
            logging.info(f"Newly processed: {results['processed']}")
            logging.info(f"Successful: {results['successful']}")
            logging.info(f"With warnings: {results['warnings']}")
            logging.info(f"Failed: {results['failed']}")
            
            if args.pretty_print and not args.summary_only:
                print(json.dumps(results, indent=2, ensure_ascii=False))
            
            # Save summary if output directory specified
            if output_dir:
                summary_file = output_dir / "processing_summary.json"
                with open(summary_file, 'w', encoding='utf-8') as f:
                    json.dump(results, f, indent=2, ensure_ascii=False)
                logging.info(f"Summary saved to: {summary_file}")
            
            return 0 if results['failed'] == 0 else 1
        
        else:
            logging.error(f"Input path is neither file nor directory: {input_path}")
            return 1
            
    except Exception as e:
        logging.error(f"Error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())