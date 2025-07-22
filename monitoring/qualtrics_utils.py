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
                           end_date: Optional[str] = None) -> Dict[str, Any]:
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
            row['recorded_date'] = response['recordedDate']
            row['response_type'] = response['responseType']
            row['progress'] = response['progress']
            row['duration'] = response['duration']
            row['finished'] = response['finished']
            
            # Add question responses
            for question_id, answer in response['values'].items():
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