#!/usr/bin/env python3
"""
CLI tool to export contact list data from Qualtrics to CSV in .tmp directory
"""

import argparse
import sys
import os

# Add current directory to path
sys.path.insert(0, os.path.dirname(__file__))

try:
    from qualtrics_utils import save_contact_list_to_csv
except ImportError as e:
    print(f"Error importing qualtrics_utils: {e}")
    sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description='Export contact list data to CSV in .tmp directory')
    parser.add_argument('--output', type=str, default='contact_list_with_embedded',
                       help='Output filename (without path or extension)')
    
    args = parser.parse_args()
    
    try:
        print("Exporting contact list data from Qualtrics...")
        file_path = save_contact_list_to_csv(
            filename=args.output
        )
        
        print(f"✓ Export complete: {file_path}")
        
    except Exception as e:
        print(f"✗ Export failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()