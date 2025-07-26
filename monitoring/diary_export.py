#!/usr/bin/env python3
"""
CLI tool to export diary survey responses to CSV in .tmp directory
"""

import argparse
import sys
import os

# Add monitoring directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'monitoring'))

try:
    from qualtrics_utils import save_diary_responses_to_csv, save_recent_diary_responses
except ImportError as e:
    print(f"Error importing qualtrics_utils: {e}")
    sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description='Export diary survey responses to CSV in .tmp directory')
    parser.add_argument('--lifetime', action='store_true',
                       help='Export ALL diary survey responses (entire survey history)')
    parser.add_argument('--hours', type=int, default=24,
                       help='Export responses from last N hours (default: 24)')
    parser.add_argument('--start-date', type=str,
                       help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str,
                       help='End date (YYYY-MM-DD)')
    parser.add_argument('--filename', type=str,
                       help='Custom filename (without path or extension)')
    parser.add_argument('--no-labels', action='store_true',
                       help='Use QID codes instead of question labels as headers')
    
    args = parser.parse_args()
    
    # Determine whether to use labels
    use_labels = not args.no_labels
    
    try:
        if args.lifetime:
            # Export all responses ever
            print("Exporting ALL diary survey responses (lifetime)...")
            file_path = save_diary_responses_to_csv(
                filename=args.filename or 'diary_responses_lifetime',
                use_labels=use_labels
            )
        elif args.start_date or args.end_date:
            # Export with date range
            print(f"Exporting diary survey responses from {args.start_date or 'beginning'} to {args.end_date or 'now'}...")
            file_path = save_diary_responses_to_csv(
                start_date=args.start_date,
                end_date=args.end_date,
                filename=args.filename,
                use_labels=use_labels
            )
        else:
            # Export recent responses
            print(f"Exporting diary survey responses from last {args.hours} hours...")
            file_path = save_recent_diary_responses(
                hours=args.hours,
                filename=args.filename,
                use_labels=use_labels
            )
        
        print(f"✓ Export complete: {file_path}")
        
    except Exception as e:
        print(f"✗ Export failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()