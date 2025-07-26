import os
import requests
import json
import pandas as pd
from typing import Dict, List, Optional, Any
import time
import zipfile
import io
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables from credentials/.env
load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'credentials', '.env'))


class QualtricsAPI:
    """Utility class for accessing Qualtrics survey data."""
    
    def __init__(self):
        """Initialize with credentials from environment variables."""
        self.api_key = os.getenv('QUALTRICS_API_KEY')
        self.datacenter_id = os.getenv('QUALTRICS_DATACENTER_ID')
        self.org_id = os.getenv('QUALTRICS_ORG_ID')
        self.user_id = os.getenv('QUALTRICS_USER_ID')
        
        # Survey IDs
        self.survey_intake_id = os.getenv('SURVEY_INTAKE_ID')
        self.survey_diary_id = os.getenv('SURVEY_DIARY_ID')
        self.survey_onboarding_id = os.getenv('SURVEY_ONBOARDING_ID')
        self.survey_exit_id = os.getenv('SURVEY_EXIT_ID')
        
        if not all([self.api_key, self.datacenter_id]):
            raise ValueError("Missing required Qualtrics credentials in environment variables")
        
        self.base_url = f"https://{self.datacenter_id}.qualtrics.com/API/v3"
        self.headers = {
            'X-API-TOKEN': self.api_key,
            'Content-Type': 'application/json'
        }
    
    def get_survey_responses(self, survey_id: str, format: str = 'json', 
                           start_date: Optional[str] = None, 
                           end_date: Optional[str] = None,
                           use_labels: bool = True) -> Dict[str, Any]:
        """
        Get survey responses for a specific survey.
        
        Args:
            survey_id: Qualtrics survey ID
            format: Response format ('json', 'csv', 'tsv', 'spss')
            start_date: Start date filter (YYYY-MM-DD)
            end_date: End date filter (YYYY-MM-DD)
            
        Returns:
            Dict containing survey responses
        """
        export_data = {
            'format': format
        }
        
        # Add useLabels parameter for CSV exports to get informative column names
        if format == 'csv' and use_labels:
            export_data['useLabels'] = True
        
        if start_date:
            export_data['startDate'] = start_date
        if end_date:
            export_data['endDate'] = end_date
            
        # Create export
        export_response = requests.post(
            f"{self.base_url}/surveys/{survey_id}/export-responses",
            headers=self.headers,
            json=export_data
        )
        
        if export_response.status_code == 400:
            # Try without date filters if they cause issues
            if start_date or end_date:
                export_data = {'format': format}
                export_response = requests.post(
                    f"{self.base_url}/surveys/{survey_id}/export-responses",
                    headers=self.headers,
                    json=export_data
                )
        
        export_response.raise_for_status()
        
        progress_id = export_response.json()['result']['progressId']
        
        # Check export progress
        while True:
            progress_response = requests.get(
                f"{self.base_url}/surveys/{survey_id}/export-responses/{progress_id}",
                headers=self.headers
            )
            progress_response.raise_for_status()
            
            status = progress_response.json()['result']['status']
            if status == 'complete':
                file_id = progress_response.json()['result']['fileId']
                break
            elif status == 'failed':
                raise Exception("Export failed")
            
            time.sleep(1)
        
        # Download file
        file_response = requests.get(
            f"{self.base_url}/surveys/{survey_id}/export-responses/{file_id}/file",
            headers=self.headers
        )
        file_response.raise_for_status()
        
        if format == 'json':
            # Extract JSON from ZIP
            with zipfile.ZipFile(io.BytesIO(file_response.content)) as zip_file:
                json_filename = [name for name in zip_file.namelist() if name.endswith('.json')][0]
                with zip_file.open(json_filename) as json_file:
                    return json.load(json_file)
        elif format == 'csv':
            # Extract CSV from ZIP
            with zipfile.ZipFile(io.BytesIO(file_response.content)) as zip_file:
                csv_filename = [name for name in zip_file.namelist() if name.endswith('.csv')][0]
                with zip_file.open(csv_filename) as csv_file:
                    return csv_file.read()
        else:
            return file_response.content
    
    def get_survey_responses_df(self, survey_id: str, 
                               start_date: Optional[str] = None, 
                               end_date: Optional[str] = None) -> pd.DataFrame:
        """
        Get survey responses as a pandas DataFrame.
        
        Args:
            survey_id: Qualtrics survey ID
            start_date: Start date filter (YYYY-MM-DD)
            end_date: End date filter (YYYY-MM-DD)
            
        Returns:
            pandas DataFrame with survey responses
        """
        data = self.get_survey_responses(survey_id, format='json', 
                                       start_date=start_date, end_date=end_date)
        
        # Convert to DataFrame
        responses = data['responses']
        df_data = []
        
        for response in responses:
            row = {}
            row['response_id'] = response['responseId']
            
            # Get values from the 'values' dictionary
            values = response.get('values', {})
            row['recorded_date'] = values.get('recordedDate', '')
            row['progress'] = values.get('progress', '')
            row['duration'] = values.get('duration', '')
            row['finished'] = values.get('finished', '')
            row['status'] = values.get('status', '')
            
            # Add all question responses from values
            for question_id, answer in values.items():
                row[question_id] = answer
            
            df_data.append(row)
        
        return pd.DataFrame(df_data)
    
    def get_intake_responses(self, start_date: Optional[str] = None, 
                           end_date: Optional[str] = None) -> pd.DataFrame:
        """Get intake survey responses."""
        return self.get_survey_responses_df(self.survey_intake_id, start_date, end_date)
    
    def get_diary_responses(self, start_date: Optional[str] = None, 
                          end_date: Optional[str] = None) -> pd.DataFrame:
        """Get daily diary survey responses."""
        return self.get_survey_responses_df(self.survey_diary_id, start_date, end_date)
    
    def get_onboarding_responses(self, start_date: Optional[str] = None, 
                               end_date: Optional[str] = None) -> pd.DataFrame:
        """Get onboarding survey responses."""
        return self.get_survey_responses_df(self.survey_onboarding_id, start_date, end_date)
    
    def get_exit_responses(self, start_date: Optional[str] = None, 
                         end_date: Optional[str] = None) -> pd.DataFrame:
        """Get exit survey responses."""
        return self.get_survey_responses_df(self.survey_exit_id, start_date, end_date)
    
    def get_survey_metadata(self, survey_id: str) -> Dict[str, Any]:
        """
        Get survey metadata including questions and options.
        
        Args:
            survey_id: Qualtrics survey ID
            
        Returns:
            Dict containing survey metadata
        """
        response = requests.get(
            f"{self.base_url}/surveys/{survey_id}",
            headers=self.headers
        )
        response.raise_for_status()
        return response.json()['result']
    
    def get_all_surveys(self) -> List[Dict[str, Any]]:
        """Get list of all surveys in the organization."""
        response = requests.get(
            f"{self.base_url}/surveys",
            headers=self.headers
        )
        response.raise_for_status()
        return response.json()['result']['elements']
    
    def get_response_counts(self, survey_id: str) -> Dict[str, int]:
        """
        Get response counts for a survey.
        
        Args:
            survey_id: Qualtrics survey ID
            
        Returns:
            Dict with response counts
        """
        try:
            response = requests.get(
                f"{self.base_url}/surveys/{survey_id}/response-counts",
                headers=self.headers
            )
            response.raise_for_status()
            return response.json()['result']
        except requests.exceptions.RequestException as e:
            print(f"Error getting response counts for survey {survey_id}: {e}")
            return {'auditable': 0, 'generated': 0, 'deleted': 0}
    
    def get_recent_responses(self, survey_id: str, hours: int = 24) -> pd.DataFrame:
        """
        Get responses from the last N hours.
        
        Args:
            survey_id: Qualtrics survey ID
            hours: Number of hours to look back
            
        Returns:
            pandas DataFrame with recent responses
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(hours=hours)
        
        return self.get_survey_responses_df(
            survey_id, 
            start_date.strftime('%Y-%m-%d'), 
            end_date.strftime('%Y-%m-%d')
        )


def get_qualtrics_client() -> QualtricsAPI:
    """Factory function to create a QualtricsAPI client."""
    return QualtricsAPI()


def get_all_study_data(start_date: Optional[str] = None, 
                      end_date: Optional[str] = None) -> Dict[str, pd.DataFrame]:
    """
    Get all survey data for the gaming reduction study.
    
    Args:
        start_date: Start date filter (YYYY-MM-DD)
        end_date: End date filter (YYYY-MM-DD)
        
    Returns:
        Dict with DataFrames for each survey type
    """
    client = get_qualtrics_client()
    
    data = {}
    survey_methods = {
        'intake': client.get_intake_responses,
        'diary': client.get_diary_responses,
        'onboarding': client.get_onboarding_responses,
        'exit': client.get_exit_responses
    }
    
    for survey_type, method in survey_methods.items():
        try:
            data[survey_type] = method(start_date, end_date)
        except Exception as e:
            print(f"Error getting {survey_type} data: {e}")
            data[survey_type] = pd.DataFrame()
    
    return data


def get_participant_progress(participant_id: str = None) -> pd.DataFrame:
    """
    Get progress summary for all participants or a specific participant.
    
    Args:
        participant_id: Optional participant ID to filter by
        
    Returns:
        DataFrame with participant progress information
    """
    client = get_qualtrics_client()
    
    # Get all survey data
    intake_df = client.get_intake_responses()
    diary_df = client.get_diary_responses()
    exit_df = client.get_exit_responses()
    
    # Create progress summary
    progress_data = []
    
    # Get unique participants from intake
    for _, participant in intake_df.iterrows():
        if participant_id and participant.get('participant_id') != participant_id:
            continue
            
        pid = participant.get('participant_id', participant['response_id'])
        
        progress = {
            'participant_id': pid,
            'intake_completed': True,
            'intake_date': participant['recorded_date'],
            'diary_responses': len(diary_df[diary_df.get('participant_id', '') == pid]),
            'exit_completed': len(exit_df[exit_df.get('participant_id', '') == pid]) > 0,
            'last_response': diary_df[diary_df.get('participant_id', '') == pid]['recorded_date'].max() if not diary_df.empty else None
        }
        
        progress_data.append(progress)
    
    return pd.DataFrame(progress_data)


def save_diary_responses_to_csv(start_date: Optional[str] = None, 
                               end_date: Optional[str] = None,
                               filename: Optional[str] = None,
                               use_labels: bool = True,
                               include_test: bool = False) -> str:
    """
    Save diary survey responses to CSV in .tmp directory.
    
    Args:
        start_date: Start date filter (YYYY-MM-DD)
        end_date: End date filter (YYYY-MM-DD)
        filename: Optional custom filename (without path or extension)
        use_labels: Whether to use question labels as column names (default: True)
        include_test: Include first 14 rows (normally skipped as test responses)
        
    Returns:
        Full path to the saved CSV file
    """
    client = get_qualtrics_client()
    
    # Get responses as CSV directly to get proper column names
    csv_data = client.get_survey_responses(client.survey_diary_id, 
                                          format='csv', 
                                          start_date=start_date, 
                                          end_date=end_date,
                                          use_labels=use_labels)
    
    # Convert CSV data to DataFrame
    import pandas as pd
    import io
    
    # Handle bytes or string data
    if isinstance(csv_data, bytes):
        csv_string = csv_data.decode('utf-8', errors='ignore')
    else:
        csv_string = csv_data
    
    df = pd.read_csv(io.StringIO(csv_string))
    
    if not include_test:
        # Skip first 14 rows (test responses)
        if len(df) > 14:
            df = df.iloc[14:].reset_index(drop=True)
    
    # Generate timestamp for filename if not provided
    if filename is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'diary_responses_{timestamp}'
    
    # Ensure .tmp directory exists
    tmp_dir = os.path.join(os.path.dirname(__file__), '..', '.tmp')
    os.makedirs(tmp_dir, exist_ok=True)
    
    # Save to CSV
    output_path = os.path.join(tmp_dir, f'{filename}.csv')
    df.to_csv(output_path, index=False)
    
    # Report file size
    if os.path.exists(output_path):
        file_size = os.path.getsize(output_path)
        file_size_mb = file_size / (1024 * 1024)
        print(f"File size: {file_size_mb:.2f} MB ({file_size:,} bytes)")
    
    return output_path


def save_recent_diary_responses(hours: int = 24,
                               filename: Optional[str] = None,
                               use_labels: bool = True) -> str:
    """
    Save recent diary survey responses to CSV in .tmp directory.
    
    Args:
        hours: Number of hours to look back
        filename: Optional custom filename (without path or extension)  
        use_labels: Whether to use question labels as column names (default: True)
        
    Returns:
        Full path to the saved CSV file
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(hours=hours)
    
    return save_diary_responses_to_csv(
        start_date=start_date.strftime('%Y-%m-%d'),
        end_date=end_date.strftime('%Y-%m-%d'),
        filename=filename,
        use_labels=use_labels
    )


def save_exit_responses_to_csv(start_date: Optional[str] = None, 
                              end_date: Optional[str] = None,
                              filename: Optional[str] = None,
                              use_labels: bool = True,
                              include_test: bool = False) -> str:
    """
    Save exit survey responses to CSV in .tmp directory.
    
    Args:
        start_date: Start date filter (YYYY-MM-DD)
        end_date: End date filter (YYYY-MM-DD)
        filename: Optional custom filename (without path or extension)
        use_labels: Whether to use question labels as column names (default: True)
        include_test: Include first 14 rows (normally skipped as test responses)
        
    Returns:
        Full path to the saved CSV file
    """
    client = get_qualtrics_client()
    
    # Get responses as CSV directly to get proper column names
    csv_data = client.get_survey_responses(client.survey_exit_id, 
                                          format='csv', 
                                          start_date=start_date, 
                                          end_date=end_date,
                                          use_labels=use_labels)
    
    # Convert CSV data to DataFrame
    import pandas as pd
    import io
    
    # Handle bytes or string data
    if isinstance(csv_data, bytes):
        csv_string = csv_data.decode('utf-8', errors='ignore')
    else:
        csv_string = csv_data
    
    df = pd.read_csv(io.StringIO(csv_string))
    
    if not include_test:
        # Skip first 14 rows (test responses)
        if len(df) > 14:
            df = df.iloc[14:].reset_index(drop=True)
    
    # Generate timestamp for filename if not provided
    if filename is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'exit_responses_{timestamp}'
    
    # Ensure .tmp directory exists
    tmp_dir = os.path.join(os.path.dirname(__file__), '..', '.tmp')
    os.makedirs(tmp_dir, exist_ok=True)
    
    # Save to CSV
    output_path = os.path.join(tmp_dir, f'{filename}.csv')
    df.to_csv(output_path, index=False)
    
    # Report file size
    if os.path.exists(output_path):
        file_size = os.path.getsize(output_path)
        file_size_mb = file_size / (1024 * 1024)
        print(f"File size: {file_size_mb:.2f} MB ({file_size:,} bytes)")
    
    return output_path


def save_recent_exit_responses(hours: int = 24,
                              filename: Optional[str] = None,
                              use_labels: bool = True) -> str:
    """
    Save recent exit survey responses to CSV in .tmp directory.
    
    Args:
        hours: Number of hours to look back
        filename: Optional custom filename (without path or extension)  
        use_labels: Whether to use question labels as column names (default: True)
        
    Returns:
        Full path to the saved CSV file
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(hours=hours)
    
    return save_exit_responses_to_csv(
        start_date=start_date.strftime('%Y-%m-%d'),
        end_date=end_date.strftime('%Y-%m-%d'),
        filename=filename,
        use_labels=use_labels
    )


def save_contact_list_to_csv(filename: Optional[str] = None) -> str:
    """
    Save contact list data with embedded data to CSV in .tmp directory.
    Uses Qualtrics Mailing List API to fetch contacts and their embedded data.
    
    Args:
        filename: Optional custom filename (without path or extension)
        
    Returns:
        Full path to the saved CSV file
    """
    client = get_qualtrics_client()
    
    # Get mailing list ID from environment variable
    contact_whitelist_url = os.getenv('CONTACT_WHITELIST_ID')
    if not contact_whitelist_url:
        raise ValueError("CONTACT_WHITELIST_ID environment variable not set")
    
    # Extract mailing list ID from the URL
    # Expected format: https://fra1.qualtrics.com/API/v3/directories/POOL_1CevzhtAVOaprpj/contacts/CG_3Q0i0cyiZlbt2EZ
    # We need to extract the mailing list ID (CG_3Q0i0cyiZlbt2EZ)
    if '/contacts/' in contact_whitelist_url:
        mailing_list_id = contact_whitelist_url.split('/contacts/')[-1]
    else:
        raise ValueError("Invalid CONTACT_WHITELIST_ID format. Expected URL with /contacts/ path")
    
    print(f"Using mailing list ID: {mailing_list_id}")
    
    # Fetch contacts from Qualtrics Mailing List API
    contacts_data = []
    next_page = f"{client.base_url}/mailinglists/{mailing_list_id}/contacts"
    
    while next_page:
        try:
            response = requests.get(next_page, headers=client.headers)
            if response.status_code != 200:
                print(f"API Error: {response.status_code}")
                print(f"Response: {response.text}")
                break
            response.raise_for_status()
            data = response.json()
            
            # Get basic contact info
            contacts = data['result']['elements']
            
            # For each contact, get detailed info including embedded data
            for contact in contacts:
                contact_id = contact.get('contactId', contact.get('id', ''))
                
                # Get detailed contact info with embedded data
                detail_response = requests.get(
                    f"{client.base_url}/mailinglists/{mailing_list_id}/contacts/{contact_id}",
                    headers=client.headers
                )
                detail_response.raise_for_status()
                contact_detail = detail_response.json()['result']
                
                # Extract contact info and embedded data
                contact_record = {
                    'contactId': contact_id,
                    'firstName': contact_detail.get('firstName', ''),
                    'lastName': contact_detail.get('lastName', ''),
                    'email': contact_detail.get('email', ''),
                    'phone': contact_detail.get('phone', ''),
                    'extRef': contact_detail.get('extRef', ''),
                    'language': contact_detail.get('language', ''),
                    'unsubscribed': contact_detail.get('unsubscribed', False)
                }
                
                # Add embedded data fields
                embedded_data = contact_detail.get('embeddedData', {})
                for key, value in embedded_data.items():
                    contact_record[key] = value
                
                contacts_data.append(contact_record)
            
            # Get next page URL if available
            next_page = data['result'].get('nextPage')
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching contacts: {e}")
            break
    
    import pandas as pd
    df = pd.DataFrame(contacts_data)
    
    # Generate timestamp for filename if not provided
    if filename is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'contact_list_{timestamp}'
    
    # Ensure .tmp directory exists
    tmp_dir = os.path.join(os.path.dirname(__file__), '..', '.tmp')
    os.makedirs(tmp_dir, exist_ok=True)
    
    # Save to CSV
    output_path = os.path.join(tmp_dir, f'{filename}.csv')
    df.to_csv(output_path, index=False)
    
    # Report file size
    if os.path.exists(output_path):
        file_size = os.path.getsize(output_path)
        file_size_mb = file_size / (1024 * 1024)
        print(f"Exported {len(df)} contacts to {output_path}")
        print(f"File size: {file_size_mb:.2f} MB ({file_size:,} bytes)")
    return output_path


def list_directories() -> List[Dict[str, Any]]:
    """
    List all directories in the Qualtrics organization.
    Useful for finding the directory ID to use for contacts.
    
    Returns:
        List of directory information
    """
    client = get_qualtrics_client()
    
    response = requests.get(f"{client.base_url}/directories", headers=client.headers)
    response.raise_for_status()
    
    directories = response.json()['result']['elements']
    
    print("Available Directories:")
    for directory in directories:
        print(f"  Directory data: {directory}")
        directory_id = directory.get('id', directory.get('directoryId', 'N/A'))
        print(f"  Directory ID: {directory_id}")
        print(f"  Name: {directory.get('name', 'N/A')}")
        print(f"  Type: {directory.get('type', 'N/A')}")
        print(f"  Contact Count: {directory.get('contactCount', 'N/A')}")
        print("  ---")
    
    return directories


def list_mailing_lists() -> List[Dict[str, Any]]:
    """
    List all mailing lists in the Qualtrics organization.
    Useful for finding the correct mailing list ID.
    
    Returns:
        List of mailing list information
    """
    client = get_qualtrics_client()
    
    try:
        response = requests.get(f"{client.base_url}/mailinglists", headers=client.headers)
        response.raise_for_status()
        
        mailing_lists = response.json()['result']['elements']
        
        print("Available Mailing Lists:")
        for ml in mailing_lists:
            print(f"  Mailing List data: {ml}")
            ml_id = ml.get('id', ml.get('mailingListId', 'N/A'))
            print(f"  Mailing List ID: {ml_id}")
            print(f"  Name: {ml.get('name', 'N/A')}")
            print(f"  Contact Count: {ml.get('contactCount', 'N/A')}")
            print("  ---")
        
        return mailing_lists
        
    except requests.exceptions.RequestException as e:
        print(f"Error listing mailing lists: {e}")
        return []