#!/usr/bin/env python3
"""
Example usage of Qualtrics utility functions.
Run this script to test the Qualtrics API connection and data retrieval.
"""

import os
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv
from qualtrics_utils import get_qualtrics_client, get_all_study_data, get_participant_progress

# Load environment variables from credentials/.env
load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'credentials', '.env'))

def main():
    """Example usage of the Qualtrics utilities."""
    
    # Check if environment variables are set
    required_vars = ['QUALTRICS_API_KEY', 'QUALTRICS_DATACENTER_ID']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"Missing required environment variables: {', '.join(missing_vars)}")
        print("Please set these in your credentials/.env file")
        return
    
    try:
        # Initialize client
        print("Initializing Qualtrics client...")
        client = get_qualtrics_client()
        
        # Test basic connectivity
        print("Testing API connection...")
        surveys = client.get_all_surveys()
        print(f"Successfully connected! Found {len(surveys)} surveys.")
        
        # Get response counts for each survey
        print("\nSurvey Response Counts:")
        survey_ids = {
            'Intake': client.survey_intake_id,
            'Diary': client.survey_diary_id,
            'Onboarding': client.survey_onboarding_id,
            'Exit': client.survey_exit_id
        }
        
        for survey_name, survey_id in survey_ids.items():
            if survey_id:
                try:
                    counts = client.get_response_counts(survey_id)
                    print(f"  {survey_name}: {counts.get('auditable', 0)} responses")
                except Exception as e:
                    print(f"  {survey_name}: Error getting count - {e}")
            else:
                print(f"  {survey_name}: No survey ID configured")
        
        # Get recent responses (last 7 days)
        print("\nGetting recent responses (last 7 days)...")
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        
        recent_data = get_all_study_data(
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d')
        )
        
        print("Recent response counts:")
        for survey_type, df in recent_data.items():
            print(f"  {survey_type}: {len(df)} responses")
            if len(df) > 0:
                print(f"    Latest response: {df['recorded_date'].max()}")
        
        # Get participant progress
        print("\nGetting participant progress...")
        progress_df = get_participant_progress()
        print(f"Total participants: {len(progress_df)}")
        
        if len(progress_df) > 0:
            completed_exit = progress_df['exit_completed'].sum()
            avg_diary_responses = progress_df['diary_responses'].mean()
            print(f"Completed exit survey: {completed_exit}")
            print(f"Average diary responses per participant: {avg_diary_responses:.1f}")
            
            # Show first few participants
            print("\nFirst 5 participants:")
            print(progress_df.head().to_string(index=False))
        
        print("\n✅ All tests completed successfully!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()