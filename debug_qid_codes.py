#!/usr/bin/env python3
"""
Debug script to check the QID codes vs labels in survey responses
"""

import sys
import os
sys.path.insert(0, 'monitoring')

from qualtrics_utils import get_qualtrics_client

def debug_qid_codes():
    """Debug the QID codes vs labels in survey responses."""
    try:
        client = get_qualtrics_client()
        
        print("=== DIARY SURVEY QID CODES ===")
        try:
            # Get raw JSON data to see both values and labels
            raw_data = client.get_survey_responses(client.survey_diary_id, format='json')
            
            if 'responses' in raw_data and len(raw_data['responses']) > 0:
                first_response = raw_data['responses'][0]
                values = first_response.get('values', {})
                labels = first_response.get('labels', {})
                
                print("Looking for Android submission ID fields...")
                android_qids = []
                for qid, value in values.items():
                    label = labels.get(qid, '')
                    if 'android' in str(label).lower() or 'submission' in str(label).lower():
                        android_qids.append((qid, label, value))
                        print(f"  QID: {qid} -> Label: '{label}' -> Value: {value}")
                
                if not android_qids:
                    print("No obvious android/submission fields found. Showing all fields:")
                    for qid, value in list(values.items())[:20]:  # First 20 fields
                        label = labels.get(qid, '')
                        print(f"  QID: {qid} -> Label: '{label}' -> Value: {value}")
                
        except Exception as e:
            print(f"Error getting diary QID codes: {e}")
            import traceback
            traceback.print_exc()
        
        print("\n=== EXIT SURVEY QID CODES ===")
        try:
            # Get raw JSON data to see both values and labels
            raw_data = client.get_survey_responses(client.survey_exit_id, format='json')
            
            if 'responses' in raw_data and len(raw_data['responses']) > 0:
                first_response = raw_data['responses'][0]
                values = first_response.get('values', {})
                labels = first_response.get('labels', {})
                
                print("Looking for Android submission ID fields...")
                android_qids = []
                for qid, value in values.items():
                    label = labels.get(qid, '')
                    if 'android' in str(label).lower() or 'submission' in str(label).lower():
                        android_qids.append((qid, label, value))
                        print(f"  QID: {qid} -> Label: '{label}' -> Value: {value}")
                
                if not android_qids:
                    print("No obvious android/submission fields found. Showing all fields:")
                    for qid, value in list(values.items())[:20]:  # First 20 fields
                        label = labels.get(qid, '')
                        print(f"  QID: {qid} -> Label: '{label}' -> Value: {value}")
                
        except Exception as e:
            print(f"Error getting exit QID codes: {e}")
            import traceback
            traceback.print_exc()
        
    except Exception as e:
        print(f"Overall error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    debug_qid_codes()