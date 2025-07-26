#!/usr/bin/env python3
"""
Script to list available Qualtrics directories to find the Directory ID for contacts
"""

import sys
import os
sys.path.insert(0, 'monitoring')

from qualtrics_utils import list_directories

if __name__ == '__main__':
    try:
        directories = list_directories()
        print(f"\nFound {len(directories)} directories.")
        print("\nTo use contacts, add the Directory ID to your credentials/.env file:")
        print("QUALTRICS_DIRECTORY_ID=POOL_xxxxxxxxxx")
        
    except Exception as e:
        print(f"Error listing directories: {e}")
        import traceback
        traceback.print_exc()