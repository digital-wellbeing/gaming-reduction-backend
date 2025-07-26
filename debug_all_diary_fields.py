#!/usr/bin/env python3
"""
Debug script to show all available fields in diary survey
"""

import sys
import os
sys.path.insert(0, 'monitoring')

from qualtrics_utils import get_qualtrics_client

def debug_all_diary_fields():
    """Show all available fields in diary survey responses."""
    try:
        client = get_qualtrics_client()
        
        print("=== ALL DIARY SURVEY FIELDS ===")
        # Get raw JSON data to see both values and labels
        raw_data = client.get_survey_responses(client.survey_diary_id, format='json')
        
        if 'responses' in raw_data and len(raw_data['responses']) > 0:
            first_response = raw_data['responses'][0]
            values = first_response.get('values', {})
            labels = first_response.get('labels', {})
            
            print(f"Total fields: {len(values)}")
            print("\nAll QID -> Label mappings:")
            
            for qid in sorted(values.keys()):
                value = values[qid]
                label = labels.get(qid, '')
                # Show value for first few chars if it's not too long
                value_preview = str(value)[:50] + ('...' if len(str(value)) > 50 else '')
                print(f"  {qid:20} -> Label: '{label}' -> Value: {value_preview}")
                
        else:
            print("No responses found")
                
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    debug_all_diary_fields()