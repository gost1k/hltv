"""
Helper utility functions for HLTV Parser
"""
import os
import re
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional, Union

logger = logging.getLogger(__name__)

def ensure_dir_exists(directory: str) -> None:
    """
    Ensure a directory exists, create it if it doesn't
    
    Args:
        directory (str): Directory path
    """
    if not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
        logger.debug(f"Created directory: {directory}")

def save_to_json(data: Any, filepath: str) -> bool:
    """
    Save data to a JSON file
    
    Args:
        data: Data to save
        filepath (str): File path
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        ensure_dir_exists(os.path.dirname(filepath))
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        logger.error(f"Error saving JSON file {filepath}: {e}")
        return False

def load_from_json(filepath: str) -> Optional[Any]:
    """
    Load data from a JSON file
    
    Args:
        filepath (str): File path
        
    Returns:
        Any: Loaded data or None if error
    """
    try:
        if not os.path.exists(filepath):
            logger.warning(f"File doesn't exist: {filepath}")
            return None
            
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading JSON file {filepath}: {e}")
        return None

def parse_date(date_str: str) -> Optional[datetime]:
    """
    Parse date string into datetime object
    
    Args:
        date_str (str): Date string
        
    Returns:
        datetime: Datetime object or None if parsing failed
    """
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%d.%m.%Y %H:%M",
        "%d.%m.%Y %H:%M:%S",
        "%d %B %Y",
        "%d %b %Y",
        "%d %B %Y %H:%M",
        "%d %b %Y %H:%M"
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    
    logger.warning(f"Could not parse date: {date_str}")
    return None

def extract_id_from_url(url: str) -> Optional[int]:
    """
    Extract ID from HLTV URL
    
    Args:
        url (str): URL string
        
    Returns:
        int: Extracted ID or None if not found
    """
    try:
        # Extract ID from various URL formats
        # /matches/2364341/team1-vs-team2
        # /match/2364341/team1-vs-team2
        # /match?id=2364341
        if match := re.search(r'/match(?:es)?/(\d+)/', url):
            return int(match.group(1))
        elif match := re.search(r'[?&]id=(\d+)', url):
            return int(match.group(1))
        else:
            logger.warning(f"Could not extract ID from URL: {url}")
            return None
    except Exception as e:
        logger.error(f"Error extracting ID from URL {url}: {e}")
        return None 