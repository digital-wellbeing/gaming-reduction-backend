"""
Monitoring utilities for the gaming reduction study.

This package provides utilities for accessing and analyzing data from the study,
including Qualtrics survey data, Google Sheets data, and participant progress tracking.
"""

from .qualtrics_utils import (
    QualtricsAPI,
    get_qualtrics_client,
    get_all_study_data,
    get_participant_progress
)

try:
    from .googlesheets_utils import (
        GoogleSheetsAPI,
        get_googlesheets_client,
        backup_qualtrics_to_sheets,
        sync_participant_progress_to_sheets
    )
    GOOGLESHEETS_AVAILABLE = True
except ImportError:
    GOOGLESHEETS_AVAILABLE = False

__all__ = [
    'QualtricsAPI',
    'get_qualtrics_client', 
    'get_all_study_data',
    'get_participant_progress'
]

if GOOGLESHEETS_AVAILABLE:
    __all__.extend([
        'GoogleSheetsAPI',
        'get_googlesheets_client',
        'backup_qualtrics_to_sheets',
        'sync_participant_progress_to_sheets'
    ])