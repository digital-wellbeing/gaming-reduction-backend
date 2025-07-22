#!/usr/bin/env python3
"""
Simple test to check basic survey data access.
"""

import os
from dotenv import load_dotenv
from qualtrics_utils import get_qualtrics_client

# Load environment variables from credentials/.env
load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'credentials', '.env'))

def main():
    """Simple test of survey access."""
    
    try:
        client = get_qualtrics_client()
        
        # Try to get basic survey information
        intake_id = os.getenv('SURVEY_INTAKE_ID')
        print(f"Testing intake survey: {intake_id}")
        
        # Get metadata first
        metadata = client.get_survey_metadata(intake_id)
        print(f"Survey name: {metadata.get('name')}")
        print(f"Survey active: {metadata.get('isActive')}")
        
        # Try to get responses without date filters
        print("\nTrying to get responses...")
        try:
            responses = client.get_survey_responses(intake_id, format='json')
            print(f"✓ Successfully retrieved {len(responses.get('responses', []))} responses")
            
            # If we have responses, show basic info
            if responses.get('responses'):
                first_response = responses['responses'][0]
                print(f"First response ID: {first_response.get('responseId')}")
                print(f"First response date: {first_response.get('recordedDate')}")
                print(f"First response progress: {first_response.get('progress')}")
        except Exception as e:
            print(f"✗ Error getting responses: {e}")
            # Try with minimal export settings
            print("Trying with minimal export settings...")
            try:
                import requests
                export_data = {
                    'format': 'json',
                    'surveyId': intake_id,
                    'compress': False
                }
                
                response = requests.post(
                    f"{client.base_url}/surveys/{intake_id}/export-responses",
                    headers=client.headers,
                    json=export_data
                )
                print(f"Response status: {response.status_code}")
                print(f"Response content: {response.text}")
            except Exception as e2:
                print(f"✗ Also failed with minimal settings: {e2}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()