#!/usr/bin/env python3
"""
Debug script to check the structure of survey responses
"""

import sys
import os
sys.path.insert(0, 'monitoring')

from qualtrics_utils import get_qualtrics_client

def debug_survey_structure():
    """Debug the structure of survey responses."""
    try:
        client = get_qualtrics_client()
        
        print("=== DIARY SURVEY DEBUG ===")
        try:
            diary_df = client.get_diary_responses()
            print(f"Diary survey - Shape: {diary_df.shape}")
            print(f"Diary survey - Columns: {list(diary_df.columns)}")
            if len(diary_df) > 0:
                print(f"Diary survey - First row keys: {list(diary_df.iloc[0].keys())}")
                print(f"Diary survey - Sample data:")
                print(diary_df.head(2))
        except Exception as e:
            print(f"Error getting diary responses: {e}")
            import traceback
            traceback.print_exc()
        
        print("\n=== EXIT SURVEY DEBUG ===")
        try:
            exit_df = client.get_exit_responses()
            print(f"Exit survey - Shape: {exit_df.shape}")
            print(f"Exit survey - Columns: {list(exit_df.columns)}")
            if len(exit_df) > 0:
                print(f"Exit survey - First row keys: {list(exit_df.iloc[0].keys())}")
                print(f"Exit survey - Sample data:")
                print(exit_df.head(2))
        except Exception as e:
            print(f"Error getting exit responses: {e}")
            import traceback
            traceback.print_exc()
        
    except Exception as e:
        print(f"Overall error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    debug_survey_structure()