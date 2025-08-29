#!/usr/bin/env python3
"""
App Game Classification with Gemini Flash 2.0

This script enriches CSV files containing app usage data with game classification
using Google's Gemini Flash 2.0 model. It includes caching to minimize API costs.
"""

import os
import sys
import json
import pandas as pd
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
import hashlib
import re

import google.generativeai as genai
from dotenv import load_dotenv
from pydantic import BaseModel, Field, field_validator


class GameClassification(BaseModel):
    """Pydantic model for individual app game classification"""
    app_name: str = Field(..., description="Exact app name from the list")
    is_game: bool = Field(..., description="Whether the app is a game")
    confidence: int = Field(..., ge=1, le=10, description="Confidence score from 1-10")
    reasoning: str = Field(..., description="Brief explanation of the classification")
    
    @field_validator('confidence')
    @classmethod
    def validate_confidence(cls, v):
        if not 1 <= v <= 10:
            raise ValueError('Confidence must be between 1 and 10')
        return v


class GameClassificationResponse(BaseModel):
    """Pydantic model for the complete classification response"""
    classifications: List[GameClassification] = Field(..., description="List of app classifications")


class AppGameClassifier:
    """Classifies apps as games using Gemini Flash 2.0 with API cost-saving caching"""
    
    def __init__(self, api_key: str, model_name: str = 'gemini-2.0-flash-exp', 
                 cache_file: str = 'app_game_cache.json'):
        """Initialize the classifier with API key and cache file"""
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel(model_name)
        self.model_name = model_name
        self.cache_file = Path(cache_file)
        self.cache = self._load_cache()
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, 
                          format='%(asctime)s - %(levelname)s - %(message)s')
    
    def _load_cache(self) -> Dict[str, Dict[str, Any]]:
        """Load existing cache from JSON file"""
        if self.cache_file.exists():
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
                logging.info(f"Loaded {len(cache_data)} cached classifications from {self.cache_file}")
                return cache_data
            except Exception as e:
                logging.warning(f"Could not load cache file {self.cache_file}: {e}")
                return {}
        return {}
    
    def _save_cache(self) -> None:
        """Save cache to JSON file"""
        try:
            # Ensure directory exists
            self.cache_file.parent.mkdir(parents=True, exist_ok=True)
            
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.cache, f, indent=2, ensure_ascii=False)
            logging.info(f"Saved cache with {len(self.cache)} entries to {self.cache_file}")
        except Exception as e:
            logging.error(f"Could not save cache file {self.cache_file}: {e}")
    
    def _normalize_app_name(self, app_name: str) -> str:
        """Normalize app name for consistent caching (lowercase, no extra spaces)"""
        return app_name.strip().lower()
    
    def _create_classification_prompt(self, app_names: List[str]) -> str:
        """Create the prompt for Gemini to classify apps as games"""
        app_list = "\n".join([f"- {app}" for app in app_names])
        
        # Create Pydantic schema for structured output
        schema = GameClassificationResponse.model_json_schema()
        
        return f"""
Analyze the following app names and determine if each one is a game or not.

App names to classify:
{app_list}

You MUST respond with valid JSON that follows this exact schema:
{json.dumps(schema, indent=2)}

Classification guidelines:
1. Games include: video games, mobile games, puzzle games, casino games, card games, board games, arcade games, action games, strategy games, etc.
2. Not games include: productivity apps, social media, browsers, messaging, utilities, news, shopping, banking, education, health, etc.
3. Be conservative - if unsure, classify as not a game with lower confidence
4. Consider abbreviated names (e.g., "LoL" likely refers to "League of Legends")
5. Consider partial names (e.g., "The Office: Somehow We Mana..." is likely a game)
6. Web domains ending in game-related terms are likely games (e.g., "leagueoflegends.com")

CRITICAL: 
- You MUST include ALL {len(app_names)} apps in your response
- Use the EXACT app names from the list above
- Respond with ONLY valid JSON, no markdown, no additional text
- Confidence must be an integer from 1 to 10
"""

    def classify_apps_batch(self, app_names: List[str]) -> Dict[str, Dict[str, Any]]:
        """Classify a batch of apps, using cache when possible"""
        # Check cache first
        uncached_apps = []
        cached_results = {}
        
        for app_name in app_names:
            normalized_name = self._normalize_app_name(app_name)
            if normalized_name in self.cache:
                cached_results[app_name] = self.cache[normalized_name]
                logging.debug(f"Using cached result for: {app_name}")
            else:
                uncached_apps.append(app_name)
        
        if cached_results:
            logging.info(f"Found {len(cached_results)} apps in cache, need to classify {len(uncached_apps)}")
        
        # Classify uncached apps
        new_results = {}
        if uncached_apps:
            logging.info(f"Calling Gemini API to classify {len(uncached_apps)} apps")
            
            try:
                # Create prompt for batch classification
                prompt = self._create_classification_prompt(uncached_apps)
                
                # Generate response
                response = self.model.generate_content(prompt)
                response_text = response.text.strip()
                
                # Clean up response (remove markdown if present)
                if response_text.startswith('```json'):
                    response_text = response_text.replace('```json', '').replace('```', '')
                elif response_text.startswith('```'):
                    response_text = response_text.replace('```', '')
                
                response_text = response_text.strip()
                
                # Parse JSON response using Pydantic for validation
                try:
                    result_data = json.loads(response_text)
                    # Validate with Pydantic
                    validated_response = GameClassificationResponse.model_validate(result_data)
                    classifications = validated_response.classifications
                except Exception as parse_error:
                    logging.error(f"Failed to parse/validate response: {parse_error}")
                    logging.error(f"Raw response: {response_text[:500]}...")
                    
                    # Try to extract partial results
                    classifications = self._extract_partial_classifications(response_text, uncached_apps)
                
                # Process results and update cache
                for classification in classifications:
                    # Handle both Pydantic objects and dict objects
                    if hasattr(classification, 'app_name'):
                        app_name = classification.app_name
                        is_game = classification.is_game
                        confidence = classification.confidence
                        reasoning = classification.reasoning
                    else:
                        app_name = classification['app_name']
                        is_game = classification['is_game'] 
                        confidence = classification['confidence']
                        reasoning = classification['reasoning']
                    
                    normalized_name = self._normalize_app_name(app_name)
                    
                    # Create result dictionary
                    result_data = {
                        'is_game': is_game,
                        'confidence': confidence,
                        'reasoning': reasoning,
                        'classification_timestamp': datetime.now().isoformat(),
                        'model_used': self.model_name
                    }
                    
                    # Add to results and cache
                    new_results[app_name] = result_data
                    self.cache[normalized_name] = result_data
                
                # Save updated cache
                self._save_cache()
                
            except Exception as e:
                logging.error(f"Error classifying apps: {e}")
                # For failed classifications, create default entries
                for app_name in uncached_apps:
                    new_results[app_name] = {
                        'is_game': False,
                        'confidence': 1,
                        'reasoning': f'Classification failed: {str(e)}',
                        'classification_timestamp': datetime.now().isoformat(),
                        'model_used': self.model_name
                    }
        
        # Combine cached and new results
        all_results = {**cached_results, **new_results}
        return all_results
    
    def _extract_partial_classifications(self, response_text: str, app_names: List[str]) -> List[Dict[str, Any]]:
        """Try to extract partial classifications from malformed response"""
        classifications = []
        
        # Try to find JSON-like objects in the text
        json_pattern = r'\{[^}]*"app_name"[^}]*\}'
        matches = re.findall(json_pattern, response_text, re.DOTALL)
        
        for match in matches:
            try:
                classification = json.loads(match)
                if all(key in classification for key in ['app_name', 'is_game', 'confidence', 'reasoning']):
                    classifications.append(classification)
            except:
                continue
        
        # If we couldn't extract anything useful, create default classifications
        if not classifications:
            logging.warning("Could not extract any classifications, creating defaults")
            for app_name in app_names:
                classifications.append({
                    'app_name': app_name,
                    'is_game': False,
                    'confidence': 1,
                    'reasoning': 'Classification failed - defaulted to not a game'
                })
        
        return classifications
    
    def detect_csv_format(self, csv_path: str) -> str:
        """
        Detect CSV format type based on column names
        
        Args:
            csv_path: Path to input CSV file
            
        Returns:
            Format type: 'ios' or 'activitywatch'
        """
        df = pd.read_csv(csv_path, nrows=1)  # Just read the header
        columns = set(df.columns.str.lower())
        
        # Check for iOS format indicators
        ios_indicators = {'pid', 'devicetype', 'probgame'}
        if ios_indicators.intersection(columns):
            return 'ios'
        
        # Check for ActivityWatch format indicators  
        aw_indicators = {'duration (min)', 'platform', 'session_datetime', 'submission_id'}
        if aw_indicators.intersection(columns):
            return 'activitywatch'
            
        # Default fallback - look for specific combinations
        if 'app' in columns:
            if 'duration (min)' in columns or 'platform' in columns:
                return 'activitywatch'
            elif 'duration' in columns:
                return 'ios'
        
        # Final fallback
        logging.warning("Could not definitively detect CSV format, assuming iOS format")
        return 'ios'

    def enrich_csv_with_game_classification(self, csv_path: str, output_path: Optional[str] = None, 
                                          force_format: Optional[str] = None) -> str:
        """
        Enrich CSV file with ProbGame and LLM_conf columns
        Supports both iOS and ActivityWatch CSV formats
        
        Args:
            csv_path: Path to input CSV file
            output_path: Path for output CSV (if None, adds '_enriched' to input filename)
            force_format: Force specific format ('ios' or 'activitywatch'), or None for auto-detection
            
        Returns:
            Path to the enriched CSV file
        """
        # Load CSV
        df = pd.read_csv(csv_path)
        logging.info(f"Loaded CSV with {len(df)} rows from {csv_path}")
        
        # Detect or use forced format
        if force_format:
            csv_format = force_format
            logging.info(f"Using forced format: {csv_format}")
        else:
            csv_format = self.detect_csv_format(csv_path)
            logging.info(f"Detected CSV format: {csv_format}")
        
        # Check if App column exists
        if 'App' not in df.columns:
            raise ValueError("CSV must contain an 'App' column")
        
        # Check if already enriched
        if 'ProbGame' in df.columns and 'LLM_conf' in df.columns:
            logging.info("CSV already contains ProbGame and LLM_conf columns")
            
            # Check if we need to process any new apps
            existing_classifications = df[['App', 'ProbGame', 'LLM_conf']].drop_duplicates()
            apps_with_missing_classification = df[
                (df['ProbGame'].isna()) | (df['LLM_conf'].isna()) | 
                (df['ProbGame'] == '') | (df['LLM_conf'] == '')
            ]['App'].unique().tolist()
            
            if not apps_with_missing_classification:
                logging.info("All apps already classified, skipping API calls")
                if output_path:
                    df.to_csv(output_path, index=False)
                    return str(output_path)
                else:
                    return csv_path
        
        # Get unique apps
        unique_apps = df['App'].unique().tolist()
        # Remove any NaN values
        unique_apps = [app for app in unique_apps if pd.notna(app)]
        
        logging.info(f"Found {len(unique_apps)} unique apps to classify")
        
        # Classify apps in batches (Gemini can handle multiple apps at once)
        batch_size = 20  # Process in batches to avoid token limits
        all_classifications = {}
        
        for i in range(0, len(unique_apps), batch_size):
            batch = unique_apps[i:i + batch_size]
            batch_results = self.classify_apps_batch(batch)
            all_classifications.update(batch_results)
            logging.info(f"Processed batch {i//batch_size + 1}/{(len(unique_apps) + batch_size - 1)//batch_size}")
        
        # Create mapping for ProbGame and LLM_conf
        game_mapping = {}
        conf_mapping = {}
        
        for app, classification in all_classifications.items():
            game_mapping[app] = "Yes" if classification['is_game'] else "No"
            conf_mapping[app] = classification['confidence']
        
        # Add columns to dataframe (or update existing ones)
        df['ProbGame'] = df['App'].map(game_mapping).fillna("No")
        df['LLM_conf'] = df['App'].map(conf_mapping).fillna(1)
        
        # Determine output path
        if output_path is None:
            input_path = Path(csv_path)
            if csv_format == 'activitywatch':
                # For ActivityWatch, save in same location but with enriched suffix
                output_path = input_path.parent / f"{input_path.stem}_enriched{input_path.suffix}"
            else:
                # For iOS, maintain existing behavior
                output_path = input_path.parent / f"{input_path.stem}_enriched{input_path.suffix}"
        
        # Save enriched CSV
        df.to_csv(output_path, index=False)
        logging.info(f"Saved enriched CSV with {len(df)} rows to {output_path}")
        
        # Print summary statistics
        game_counts = df['ProbGame'].value_counts()
        logging.info(f"Classification summary: {game_counts.to_dict()}")
        
        avg_confidence = df['LLM_conf'].mean()
        logging.info(f"Average confidence: {avg_confidence:.2f}")
        
        # Format-specific logging
        if csv_format == 'activitywatch':
            platform_stats = df.groupby(['platform', 'ProbGame']).size().unstack(fill_value=0)
            logging.info(f"ActivityWatch platform breakdown:\n{platform_stats}")
        
        return str(output_path)


def load_environment_variables():
    """Load environment variables from .env file"""
    # Load from credentials/.env file
    env_file = Path(__file__).parent.parent.parent / 'credentials' / '.env'
    load_dotenv(env_file)
    
    api_key = os.getenv('GOOGLE_API_KEY')
    if not api_key:
        raise ValueError("Missing GOOGLE_API_KEY in environment variables")
    
    return api_key


def main():
    """Main function for command line usage"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Enrich CSV files with app game classification using Gemini Flash 2.0"
    )
    
    parser.add_argument(
        'csv_path',
        help='Path to CSV file with App column to classify'
    )
    
    parser.add_argument(
        '--output', '-o',
        help='Output path for enriched CSV (default: adds _enriched to input filename)'
    )
    
    parser.add_argument(
        '--cache-file',
        default='monitoring/ocr/app_game_cache.json',
        help='Path to cache file for storing previous classifications'
    )
    
    parser.add_argument(
        '--model',
        default='gemini-2.0-flash-exp',
        help='Gemini model to use (default: gemini-2.0-flash-exp)'
    )
    
    parser.add_argument(
        '--format', '-f',
        choices=['ios', 'activitywatch', 'auto'],
        default='auto',
        help='CSV format type: ios, activitywatch, or auto-detect (default: auto)'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        # Load API key
        api_key = load_environment_variables()
        
        # Initialize classifier
        classifier = AppGameClassifier(
            api_key=api_key,
            model_name=args.model,
            cache_file=args.cache_file
        )
        
        # Process CSV with format specification
        force_format = None if args.format == 'auto' else args.format
        output_path = classifier.enrich_csv_with_game_classification(
            csv_path=args.csv_path,
            output_path=args.output,
            force_format=force_format
        )
        
        print(f"Successfully enriched CSV. Output saved to: {output_path}")
        return 0
        
    except Exception as e:
        logging.error(f"Error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())