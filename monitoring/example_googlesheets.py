#!/usr/bin/env python3
"""
Example usage of Google Sheets utility functions.
Run this script to test the Google Sheets API connection and data access.
"""

import os
import sys
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from credentials/.env
load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'credentials', '.env'))

try:
    from googlesheets_utils import get_googlesheets_client
    GOOGLESHEETS_AVAILABLE = True
except ImportError:
    GOOGLESHEETS_AVAILABLE = False

def main():
    """Example usage of the Google Sheets utilities."""
    
    if not GOOGLESHEETS_AVAILABLE:
        print("❌ Google Sheets dependencies not installed.")
        print("Run: pip install gspread gspread-dataframe google-auth")
        return
    
    # Check if environment variables are set
    required_vars = ['GOOGLE_SHEETS_CREDENTIALS_FILE', 'GOOGLE_SHEETS_SPREADSHEET_ID']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"Missing required environment variables: {', '.join(missing_vars)}")
        print("Please set these in your credentials/.env file")
        return
    
    try:
        # Initialize client
        print("Initializing Google Sheets client...")
        client = get_googlesheets_client()
        
        # Get spreadsheet info
        print("Getting spreadsheet information...")
        info = client.get_spreadsheet_info()
        print(f"Spreadsheet: {info['title']}")
        print(f"URL: {info['url']}")
        print(f"Worksheets: {', '.join(info['worksheets'])}")
        
        # Get data from both sheets
        print("\nGetting data from Participants sheet...")
        participants_df = client.get_participants_data()
        print(f"Participants data: {len(participants_df)} rows")
        if not participants_df.empty:
            print(f"Columns: {', '.join(participants_df.columns)}")
            print(f"First few rows:")
            print(participants_df.head().to_string(index=False))
        
        print("\nGetting data from Waitlist sheet...")
        waitlist_df = client.get_waitlist_data()
        print(f"Waitlist data: {len(waitlist_df)} rows")
        if not waitlist_df.empty:
            print(f"Columns: {', '.join(waitlist_df.columns)}")
            print(f"First few rows:")
            print(waitlist_df.head().to_string(index=False))
        
        # Get all study participants
        print("\nGetting all study participants...")
        all_participants = client.get_all_study_participants()
        total_participants = len(all_participants['participants'])
        total_waitlist = len(all_participants['waitlist'])
        
        print(f"Total participants: {total_participants}")
        print(f"Total waitlist: {total_waitlist}")
        print(f"Total study contacts: {total_participants + total_waitlist}")
        
        # Test search functionality
        print("\nTesting search functionality...")
        if not participants_df.empty:
            # Search for any data
            search_results = client.search_data("@", "Participants")  # Search for email addresses
            print(f"Found {len(search_results)} email addresses in Participants sheet")
            
            if search_results:
                print("Sample search results:")
                for result in search_results[:3]:  # Show first 3
                    print(f"  {result['cell']}: {result['value']}")
        
        print("\n✅ Google Sheets connection test completed successfully!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()