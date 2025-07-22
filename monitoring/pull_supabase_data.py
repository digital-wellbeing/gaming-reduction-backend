#!/usr/bin/env python3
"""
CLI command to pull all data from Supabase 'uploads' table.
Usage: python pull_data.py
"""

import os
import sys
import subprocess
from pathlib import Path
from dotenv import load_dotenv

def load_credentials():
    """Load database credentials from .env file."""
    env_path = Path(__file__).parent.parent / "credentials" / ".env"
    load_dotenv(env_path)
    
    db_password = os.getenv("SUPABASE_DB_PW")
    db_url = os.getenv("SUPABASE_DB_URL")
    
    if not db_password or not db_url:
        raise ValueError("Missing SUPABASE_DB_PW or SUPABASE_DB_URL in credentials/.env")
    
    return db_password, db_url

def ensure_tmp_dir():
    """Create .tmp directory if it doesn't exist."""
    tmp_dir = Path(__file__).parent.parent / ".tmp"
    tmp_dir.mkdir(exist_ok=True)
    return tmp_dir

def pull_uploads_data():
    """Pull all data from uploads table and save to .tmp directory."""
    try:
        db_password, db_url = load_credentials()
        tmp_dir = ensure_tmp_dir()
        
        # Build psql command
        host = f"aws-0-eu-west-2.pooler.supabase.com"
        port = "5432"
        database = "postgres"
        user = f"postgres.{db_url}"
        
        # Set PGPASSWORD environment variable
        env = os.environ.copy()
        env["PGPASSWORD"] = db_password
        
        # Output file
        output_file = tmp_dir / "uploads_data.csv"
        
        # Build psql command to export uploads table as CSV (ActivityWatch only)
        cmd = [
            "psql",
            "-h", host,
            "-p", port,
            "-d", database,
            "-U", user,
            "-c", "\\copy (SELECT * FROM uploads WHERE platform = 'ActivityWatch') TO STDOUT WITH CSV HEADER",
            "-o", str(output_file)
        ]
        
        print(f"Connecting to Supabase database...")
        print(f"Host: {host}")
        print(f"Database: {database}")
        print(f"User: {user}")
        print(f"Output file: {output_file}")
        
        # Execute command
        result = subprocess.run(cmd, env=env, capture_output=True, text=True)
        
        if result.returncode == 0:
            print(f"✓ Data successfully exported to {output_file}")
            if output_file.exists():
                file_size = output_file.stat().st_size
                print(f"File size: {file_size} bytes")
        else:
            print(f"✗ Error executing psql command:")
            print(f"STDERR: {result.stderr}")
            print(f"STDOUT: {result.stdout}")
            sys.exit(1)
            
    except Exception as e:
        print(f"✗ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    pull_uploads_data()