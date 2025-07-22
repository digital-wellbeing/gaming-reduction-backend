#!/usr/bin/env python3
"""
Example showing how to use Qualtrics and Google Sheets utilities together.
This demonstrates cross-platform data analysis for the gaming reduction study.
"""

import os
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables from credentials/.env
load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'credentials', '.env'))

from qualtrics_utils import get_qualtrics_client, get_all_study_data

try:
    from googlesheets_utils import get_googlesheets_client, backup_qualtrics_to_sheets
    GOOGLESHEETS_AVAILABLE = True
except ImportError:
    GOOGLESHEETS_AVAILABLE = False

def main():
    """Example of combined Qualtrics and Google Sheets analysis."""
    
    print("🔍 Gaming Reduction Study - Data Analysis Example")
    print("=" * 60)
    
    # Check environment variables
    required_vars = ['QUALTRICS_API_KEY', 'QUALTRICS_DATACENTER_ID']
    if GOOGLESHEETS_AVAILABLE:
        required_vars.extend(['GOOGLE_SHEETS_CREDENTIALS_FILE', 'GOOGLE_SHEETS_SPREADSHEET_ID'])
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
        print("Please set these in your credentials/.env file")
        return
    
    try:
        # Initialize clients
        print("🔗 Initializing data connections...")
        qualtrics_client = get_qualtrics_client()
        
        if GOOGLESHEETS_AVAILABLE:
            sheets_client = get_googlesheets_client()
            print("✅ Connected to both Qualtrics and Google Sheets")
        else:
            print("⚠️  Google Sheets not available - install dependencies with:")
            print("   pip install gspread gspread-dataframe google-auth")
            sheets_client = None
        
        # Get participant data from Google Sheets
        if sheets_client:
            print("\n📊 Getting participant data from Google Sheets...")
            all_participants = sheets_client.get_all_study_participants()
            participants_df = all_participants['participants']
            waitlist_df = all_participants['waitlist']
            
            print(f"   Active participants: {len(participants_df)}")
            print(f"   Waitlist participants: {len(waitlist_df)}")
            
            # Show some statistics
            if not participants_df.empty:
                print(f"   Participants sheet columns: {', '.join(participants_df.columns)}")
        
        # Get survey data from Qualtrics
        print("\n📋 Getting survey data from Qualtrics...")
        
        # Get recent responses (last 30 days)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        
        try:
            qualtrics_data = get_all_study_data(
                start_date.strftime('%Y-%m-%d'),
                end_date.strftime('%Y-%m-%d')
            )
            
            print("   Survey response counts (last 30 days):")
            for survey_type, df in qualtrics_data.items():
                print(f"     {survey_type}: {len(df)} responses")
                if len(df) > 0:
                    print(f"       Latest response: {df['recorded_date'].max()}")
        except Exception as e:
            print(f"   ⚠️  Error getting survey data: {e}")
            qualtrics_data = {}
        
        # Cross-platform analysis
        if sheets_client and qualtrics_data:
            print("\n🔄 Cross-platform analysis...")
            
            # Compare participant counts
            sheets_participant_count = len(participants_df) if not participants_df.empty else 0
            qualtrics_intake_count = len(qualtrics_data.get('intake', []))
            
            print(f"   Participants in Google Sheets: {sheets_participant_count}")
            print(f"   Intake surveys in Qualtrics: {qualtrics_intake_count}")
            
            if sheets_participant_count > 0 and qualtrics_intake_count > 0:
                completion_rate = (qualtrics_intake_count / sheets_participant_count) * 100
                print(f"   Intake completion rate: {completion_rate:.1f}%")
            
            # Backup option
            print("\n💾 Backup options:")
            print("   - Backup Qualtrics data to Google Sheets")
            print("   - Sync participant progress")
            
            # Uncomment to actually perform backup
            # backup_success = backup_qualtrics_to_sheets(qualtrics_data)
            # print(f"   Backup status: {'✅ Success' if backup_success else '❌ Failed'}")
        
        # Data quality checks
        print("\n🔍 Data quality checks...")
        
        if sheets_client and not participants_df.empty:
            # Check for missing data in participants
            missing_data = participants_df.isnull().sum()
            if missing_data.any():
                print("   Missing data in Participants sheet:")
                for col, count in missing_data.items():
                    if count > 0:
                        print(f"     {col}: {count} missing values")
            else:
                print("   ✅ No missing data in Participants sheet")
        
        if qualtrics_data:
            # Check response completion rates
            for survey_type, df in qualtrics_data.items():
                if not df.empty and 'finished' in df.columns:
                    finished_count = df['finished'].sum()
                    total_count = len(df)
                    completion_rate = (finished_count / total_count) * 100
                    print(f"   {survey_type} completion rate: {completion_rate:.1f}%")
        
        print("\n✅ Combined analysis completed successfully!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()