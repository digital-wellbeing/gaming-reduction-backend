#!/usr/bin/env python3
"""
Debug script to check the raw structure of survey responses
"""

import sys
import os
sys.path.insert(0, 'monitoring')

from qualtrics_utils import get_qualtrics_client

def debug_raw_response():
    """Debug the raw structure of survey responses."""
    try:
        client = get_qualtrics_client()
        
        print("=== DIARY SURVEY RAW DATA ===")
        try:
            # Get raw JSON data directly
            raw_data = client.get_survey_responses(client.survey_diary_id, format='json')
            print(f"Raw data keys: {list(raw_data.keys())}")
            
            if 'responses' in raw_data and len(raw_data['responses']) > 0:
                first_response = raw_data['responses'][0]
                print(f"First response keys: {list(first_response.keys())}")
                print(f"First response sample: {first_response}")
            else:
                print("No responses found")
                
        except Exception as e:
            print(f"Error getting diary raw data: {e}")
            import traceback
            traceback.print_exc()
        
        print("\n=== EXIT SURVEY RAW DATA ===")
        try:
            # Get raw JSON data directly
            raw_data = client.get_survey_responses(client.survey_exit_id, format='json')
            print(f"Raw data keys: {list(raw_data.keys())}")
            
            if 'responses' in raw_data and len(raw_data['responses']) > 0:
                first_response = raw_data['responses'][0]
                print(f"First response keys: {list(first_response.keys())}")
                print(f"First response sample: {first_response}")
            else:
                print("No responses found")
                
        except Exception as e:
            print(f"Error getting exit raw data: {e}")
            import traceback
            traceback.print_exc()
        
    except Exception as e:
        print(f"Overall error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    debug_raw_response()