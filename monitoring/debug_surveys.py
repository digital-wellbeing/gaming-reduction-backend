#!/usr/bin/env python3
"""
Debug script to list available surveys and check survey IDs.
"""

import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from qualtrics_utils import get_qualtrics_client

# Load environment variables from credentials/.env
load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'credentials', '.env'))

def main():
    """Debug survey access."""
    
    try:
        # Initialize client
        print("Initializing Qualtrics client...")
        client = get_qualtrics_client()
        
        # List all available surveys
        print("\nAvailable surveys:")
        surveys = client.get_all_surveys()
        
        print(f"Found {len(surveys)} surveys:")
        for survey in surveys:
            print(f"  ID: {survey['id']}")
            print(f"  Name: {survey['name']}")
            print(f"  Status: {survey.get('isActive', 'Unknown')}")
            print(f"  Created: {survey.get('creationDate', 'Unknown')}")
            print(f"  Owner: {survey.get('ownerId', 'Unknown')}")
            print("  ---")
        
        # Check configured survey IDs
        print("\nConfigured survey IDs:")
        survey_ids = {
            'Intake': os.getenv('SURVEY_INTAKE_ID'),
            'Diary': os.getenv('SURVEY_DIARY_ID'),
            'Onboarding': os.getenv('SURVEY_ONBOARDING_ID'),
            'Exit': os.getenv('SURVEY_EXIT_ID')
        }
        
        for survey_name, survey_id in survey_ids.items():
            if survey_id:
                # Check if this survey ID exists in available surveys
                found = any(s['id'] == survey_id for s in surveys)
                print(f"  {survey_name} ({survey_id}): {'✓ Found' if found else '✗ Not found'}")
                
                if found:
                    # Try to get metadata
                    try:
                        metadata = client.get_survey_metadata(survey_id)
                        print(f"    Name: {metadata.get('name', 'Unknown')}")
                        print(f"    Active: {metadata.get('isActive', 'Unknown')}")
                    except Exception as e:
                        print(f"    Error getting metadata: {e}")
            else:
                print(f"  {survey_name}: No ID configured")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()