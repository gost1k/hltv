"""
Match model class
"""
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Dict, Any


@dataclass
class Team:
    """Team information"""
    id: int
    name: str
    logo_url: Optional[str] = None
    country: Optional[str] = None
    world_ranking: Optional[int] = None
    
    
@dataclass
class Player:
    """Player information"""
    id: int
    name: str
    nickname: str
    team_id: int
    country: Optional[str] = None
    stats: Optional[Dict[str, Any]] = None


@dataclass
class Map:
    """Map information"""
    name: str
    pick: Optional[str] = None
    score_team1: Optional[int] = None
    score_team2: Optional[int] = None
    stats: Optional[Dict[str, Any]] = None


@dataclass
class Match:
    """Match data model"""
    id: int
    url: str
    team1: Team
    team2: Team
    date: datetime
    event_id: Optional[int] = None
    event_name: Optional[str] = None
    match_format: Optional[str] = None  # bo1, bo3, bo5
    status: str = "upcoming"  # upcoming, live, completed
    winner_id: Optional[int] = None
    score_team1: Optional[int] = None
    score_team2: Optional[int] = None
    maps: Optional[List[Map]] = None
    players_team1: Optional[List[Player]] = None
    players_team2: Optional[List[Player]] = None
    stats: Optional[Dict[str, Any]] = None
    
    @property
    def is_past(self) -> bool:
        """Check if match is in the past"""
        return self.status == "completed"
    
    @property
    def is_live(self) -> bool:
        """Check if match is live"""
        return self.status == "live"
    
    @property
    def is_upcoming(self) -> bool:
        """Check if match is upcoming"""
        return self.status == "upcoming" 