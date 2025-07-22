import os
import pandas as pd
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
import json
from dotenv import load_dotenv

try:
    from google.oauth2.service_account import Credentials
    import gspread
    from gspread_dataframe import get_as_dataframe, set_with_dataframe
    GOOGLE_SHEETS_AVAILABLE = True
except ImportError:
    GOOGLE_SHEETS_AVAILABLE = False

# Load environment variables from credentials/.env
load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'credentials', '.env'))


class GoogleSheetsAPI:
    """Utility class for accessing Google Sheets data."""
    
    def __init__(self):
        """Initialize with credentials from environment variables."""
        if not GOOGLE_SHEETS_AVAILABLE:
            raise ImportError("Google Sheets dependencies not installed. Run: pip install gspread gspread-dataframe google-auth")
        
        self.credentials_file = os.getenv('GOOGLE_SHEETS_CREDENTIALS_FILE')
        self.spreadsheet_id = os.getenv('GOOGLE_SHEETS_SPREADSHEET_ID')
        
        if not self.credentials_file:
            raise ValueError("GOOGLE_SHEETS_CREDENTIALS_FILE not set in environment variables")
        if not self.spreadsheet_id:
            raise ValueError("GOOGLE_SHEETS_SPREADSHEET_ID not set in environment variables")
        
        # Check if credentials file exists
        if not os.path.exists(self.credentials_file):
            raise FileNotFoundError(f"Google Sheets credentials file not found: {self.credentials_file}")
        
        # Initialize Google Sheets client
        self.scope = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        
        self.credentials = Credentials.from_service_account_file(
            self.credentials_file, 
            scopes=self.scope
        )
        
        self.client = gspread.authorize(self.credentials)
        self.spreadsheet = self.client.open_by_key(self.spreadsheet_id)
    
    def get_worksheet_names(self) -> List[str]:
        """Get list of all worksheet names in the spreadsheet."""
        return [worksheet.title for worksheet in self.spreadsheet.worksheets()]
    
    def get_worksheet_data(self, worksheet_name: str, 
                          include_headers: bool = True,
                          start_row: int = 1,
                          start_col: int = 1,
                          end_row: Optional[int] = None,
                          end_col: Optional[int] = None) -> pd.DataFrame:
        """
        Get data from a specific worksheet as a pandas DataFrame.
        
        Args:
            worksheet_name: Name of the worksheet
            include_headers: Whether to treat first row as headers
            start_row: Starting row (1-indexed)
            start_col: Starting column (1-indexed)
            end_row: Ending row (1-indexed), None for all rows
            end_col: Ending column (1-indexed), None for all columns
            
        Returns:
            pandas DataFrame with worksheet data
        """
        try:
            worksheet = self.spreadsheet.worksheet(worksheet_name)
            
            if end_row is None and end_col is None:
                # Get all data
                df = get_as_dataframe(worksheet, parse_dates=True, header=0 if include_headers else None)
            else:
                # Get specific range
                if end_row is None:
                    end_row = worksheet.row_count
                if end_col is None:
                    end_col = worksheet.col_count
                
                # Convert to A1 notation
                start_cell = gspread.utils.rowcol_to_a1(start_row, start_col)
                end_cell = gspread.utils.rowcol_to_a1(end_row, end_col)
                range_name = f"{start_cell}:{end_cell}"
                
                values = worksheet.get(range_name)
                
                if include_headers and values:
                    df = pd.DataFrame(values[1:], columns=values[0])
                else:
                    df = pd.DataFrame(values)
            
            # Clean up empty columns and rows
            df = df.dropna(how='all', axis=1)  # Remove empty columns
            df = df.dropna(how='all', axis=0)  # Remove empty rows
            
            return df
            
        except gspread.exceptions.WorksheetNotFound:
            print(f"Worksheet '{worksheet_name}' not found")
            return pd.DataFrame()
        except Exception as e:
            print(f"Error reading worksheet '{worksheet_name}': {e}")
            return pd.DataFrame()
    
    def write_worksheet_data(self, worksheet_name: str, data: pd.DataFrame, 
                           clear_existing: bool = True,
                           start_row: int = 1,
                           start_col: int = 1,
                           include_headers: bool = True) -> bool:
        """
        Write pandas DataFrame to a worksheet.
        
        Args:
            worksheet_name: Name of the worksheet
            data: DataFrame to write
            clear_existing: Whether to clear existing data
            start_row: Starting row (1-indexed)
            start_col: Starting column (1-indexed)
            include_headers: Whether to include column headers
            
        Returns:
            True if successful, False otherwise
        """
        try:
            try:
                worksheet = self.spreadsheet.worksheet(worksheet_name)
            except gspread.exceptions.WorksheetNotFound:
                # Create worksheet if it doesn't exist
                worksheet = self.spreadsheet.add_worksheet(title=worksheet_name, rows=1000, cols=26)
            
            if clear_existing:
                worksheet.clear()
            
            # Write data
            set_with_dataframe(
                worksheet, 
                data, 
                row=start_row, 
                col=start_col, 
                include_column_header=include_headers
            )
            
            return True
            
        except Exception as e:
            print(f"Error writing to worksheet '{worksheet_name}': {e}")
            return False
    
    def append_worksheet_data(self, worksheet_name: str, data: pd.DataFrame) -> bool:
        """
        Append data to the end of a worksheet.
        
        Args:
            worksheet_name: Name of the worksheet
            data: DataFrame to append
            
        Returns:
            True if successful, False otherwise
        """
        try:
            worksheet = self.spreadsheet.worksheet(worksheet_name)
            
            # Convert DataFrame to list of lists
            values = data.values.tolist()
            
            # Append to worksheet
            worksheet.append_rows(values)
            
            return True
            
        except gspread.exceptions.WorksheetNotFound:
            print(f"Worksheet '{worksheet_name}' not found")
            return False
        except Exception as e:
            print(f"Error appending to worksheet '{worksheet_name}': {e}")
            return False
    
    def get_spreadsheet_info(self) -> Dict[str, Any]:
        """Get basic information about the spreadsheet."""
        return {
            'id': self.spreadsheet_id,
            'title': self.spreadsheet.title,
            'url': self.spreadsheet.url,
            'worksheets': self.get_worksheet_names(),
            'created_time': getattr(self.spreadsheet, 'created_time', None),
            'updated_time': getattr(self.spreadsheet, 'updated_time', None)
        }
    
    def search_data(self, query: str, worksheet_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Search for data across worksheets.
        
        Args:
            query: Search query
            worksheet_name: Specific worksheet to search, None for all
            
        Returns:
            List of search results with location information
        """
        results = []
        
        worksheets_to_search = [worksheet_name] if worksheet_name else self.get_worksheet_names()
        
        for ws_name in worksheets_to_search:
            try:
                worksheet = self.spreadsheet.worksheet(ws_name)
                cells = worksheet.findall(query)
                
                for cell in cells:
                    results.append({
                        'worksheet': ws_name,
                        'cell': cell.address,
                        'row': cell.row,
                        'col': cell.col,
                        'value': cell.value
                    })
                    
            except Exception as e:
                print(f"Error searching worksheet '{ws_name}': {e}")
        
        return results
    
    def get_participants_data(self, participant_id: str = None) -> pd.DataFrame:
        """
        Get participant data from the 'Participants' sheet.
        
        Args:
            participant_id: Specific participant ID to filter by
            
        Returns:
            DataFrame with participant data
        """
        df = self.get_worksheet_data('Participants')
        
        if not df.empty and participant_id:
            # Try different possible column names for participant ID
            id_columns = ['participant_id', 'id', 'ParticipantID', 'ID', 'participant', 'Participant']
            
            for col in id_columns:
                if col in df.columns:
                    df = df[df[col] == participant_id]
                    break
        
        return df
    
    def get_waitlist_data(self, participant_id: str = None) -> pd.DataFrame:
        """
        Get waitlist data from the 'Waitlist' sheet.
        
        Args:
            participant_id: Specific participant ID to filter by
            
        Returns:
            DataFrame with waitlist data
        """
        df = self.get_worksheet_data('Waitlist')
        
        if not df.empty and participant_id:
            # Try different possible column names for participant ID
            id_columns = ['participant_id', 'id', 'ParticipantID', 'ID', 'participant', 'Participant']
            
            for col in id_columns:
                if col in df.columns:
                    df = df[df[col] == participant_id]
                    break
        
        return df
    
    def get_all_study_participants(self) -> Dict[str, pd.DataFrame]:
        """
        Get all participant data from both sheets.
        
        Returns:
            Dictionary with 'participants' and 'waitlist' DataFrames
        """
        return {
            'participants': self.get_participants_data(),
            'waitlist': self.get_waitlist_data()
        }
    
    def add_participant(self, participant_data: Dict[str, Any], 
                       to_waitlist: bool = False) -> bool:
        """
        Add a new participant to either the Participants or Waitlist sheet.
        
        Args:
            participant_data: Dictionary with participant information
            to_waitlist: Whether to add to waitlist (True) or participants (False)
            
        Returns:
            True if successful, False otherwise
        """
        sheet_name = 'Waitlist' if to_waitlist else 'Participants'
        
        try:
            # Convert to DataFrame
            df = pd.DataFrame([participant_data])
            
            # Append to the appropriate sheet
            return self.append_worksheet_data(sheet_name, df)
            
        except Exception as e:
            print(f"Error adding participant to {sheet_name}: {e}")
            return False
    
    def update_participant_status(self, participant_id: str, 
                                 status_updates: Dict[str, Any],
                                 in_waitlist: bool = False) -> bool:
        """
        Update participant status in the sheets.
        
        Args:
            participant_id: ID of participant to update
            status_updates: Dictionary of fields to update
            in_waitlist: Whether participant is in waitlist (True) or participants (False)
            
        Returns:
            True if successful, False otherwise
        """
        sheet_name = 'Waitlist' if in_waitlist else 'Participants'
        
        try:
            # Get current data
            df = self.get_worksheet_data(sheet_name)
            
            if df.empty:
                print(f"No data found in {sheet_name} sheet")
                return False
            
            # Find participant row
            id_columns = ['participant_id', 'id', 'ParticipantID', 'ID']
            participant_row = None
            id_col = None
            
            for col in id_columns:
                if col in df.columns:
                    participant_row = df[df[col] == participant_id]
                    id_col = col
                    break
            
            if participant_row is None or participant_row.empty:
                print(f"Participant {participant_id} not found in {sheet_name}")
                return False
            
            # Update the row
            row_index = participant_row.index[0]
            for field, value in status_updates.items():
                if field in df.columns:
                    df.at[row_index, field] = value
                else:
                    # Add new column if it doesn't exist
                    df[field] = ''
                    df.at[row_index, field] = value
            
            # Write back to sheet
            return self.write_worksheet_data(sheet_name, df, clear_existing=True)
            
        except Exception as e:
            print(f"Error updating participant {participant_id} in {sheet_name}: {e}")
            return False


def get_googlesheets_client() -> GoogleSheetsAPI:
    """Factory function to create a GoogleSheetsAPI client."""
    return GoogleSheetsAPI()


def backup_qualtrics_to_sheets(qualtrics_data: Dict[str, pd.DataFrame], 
                             backup_timestamp: bool = True) -> bool:
    """
    Backup Qualtrics data to Google Sheets.
    
    Args:
        qualtrics_data: Dictionary of DataFrames from Qualtrics
        backup_timestamp: Whether to add timestamp to worksheet names
        
    Returns:
        True if successful, False otherwise
    """
    try:
        sheets_client = get_googlesheets_client()
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S") if backup_timestamp else ""
        
        for survey_type, df in qualtrics_data.items():
            if df.empty:
                continue
                
            worksheet_name = f"{survey_type}_{timestamp}" if timestamp else survey_type
            
            success = sheets_client.write_worksheet_data(
                worksheet_name, 
                df, 
                clear_existing=True,
                include_headers=True
            )
            
            if success:
                print(f"✓ Backed up {survey_type} data to '{worksheet_name}'")
            else:
                print(f"✗ Failed to backup {survey_type} data")
                return False
        
        return True
        
    except Exception as e:
        print(f"Error backing up to Google Sheets: {e}")
        return False


def sync_participant_progress_to_sheets(progress_df: pd.DataFrame) -> bool:
    """
    Sync participant progress data to Google Sheets.
    
    Args:
        progress_df: DataFrame with participant progress
        
    Returns:
        True if successful, False otherwise
    """
    try:
        sheets_client = get_googlesheets_client()
        
        # Add timestamp column
        progress_df['last_updated'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        success = sheets_client.write_worksheet_data(
            'participant_progress', 
            progress_df, 
            clear_existing=True,
            include_headers=True
        )
        
        if success:
            print("✓ Synced participant progress to Google Sheets")
        else:
            print("✗ Failed to sync participant progress")
            
        return success
        
    except Exception as e:
        print(f"Error syncing participant progress: {e}")
        return False