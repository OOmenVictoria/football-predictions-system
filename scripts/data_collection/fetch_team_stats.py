#!/usr/bin/env python3
"""
Fetch Team Stats - Collects and analyzes team statistics
Processes historical data and statistics for teams involved in upcoming matches
Optimized version with local cache, improved error handling and configurable parameters
Now includes multiple free data sources and enhanced reliability
"""

import os
import sys
import requests
import json
import logging
import time
import random
import sqlite3
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional, Set
import firebase_admin
from firebase_admin import credentials, db
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from fuzzywuzzy import fuzz

# Logging configuration with rotation
log_dir = os.path.expanduser('~/football-predictions/logs')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"fetch_team_stats_{datetime.now().strftime('%Y%m%d')}.log")

# Setup logging with multiple destinations
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger('fetch_team_stats')

# Local cache configuration
DB_PATH = os.path.expanduser("~/football-predictions/cache/team_stats_cache.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# Configurable parameters
load_dotenv()
CONFIG = {
    "max_teams_per_execution": int(os.getenv('MAX_TEAMS_PER_EXECUTION', '10')),
    "stats_fresh_hours": int(os.getenv('STATS_FRESH_HOURS', '12')),
    "api_timeout": int(os.getenv('API_TIMEOUT', '30')),
    "max_retries": int(os.getenv('MAX_RETRIES', '3')),
    "min_delay": float(os.getenv('MIN_DELAY', '2.0')),
    "max_delay": float(os.getenv('MAX_DELAY', '5.0')),
    "match_days_ahead": int(os.getenv('MATCH_DAYS_AHEAD', '3')),
    "match_days_behind": int(os.getenv('MATCH_DAYS_BEHIND', '0')),
    "cache_expiry_hours": int(os.getenv('CACHE_EXPIRY_HOURS', '12')),
    "matches_to_analyze": int(os.getenv('MATCHES_TO_ANALYZE', '10')),
    "recent_form_matches": int(os.getenv('RECENT_FORM_MATCHES', '5')),
    "max_log_size_mb": int(os.getenv('MAX_LOG_SIZE_MB', '10')),
    "scrape_timeout": int(os.getenv('SCRAPE_TIMEOUT', '15')),
    "scrape_max_attempts": int(os.getenv('SCRAPE_MAX_ATTEMPTS', '2')),
}

# API keys and URLs
API_KEY = os.getenv('FOOTBALL_API_KEY')
BASE_URL = "https://api.football-data.org/v4"

# List of User Agents to avoid blocks
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPad; CPU OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36 Edg/92.0.902.55"
]

# Free data sources configuration
FREE_SOURCES = {
    "football_data": {
        "base_url": BASE_URL,
        "requires_key": True,
        "endpoints": {
            "team": "teams/{team_id}",
            "matches": "teams/{team_id}/matches"
        }
    },
    "fbref": {
        "base_url": "https://fbref.com",
        "paths": {
            "team": "/en/squads/{team_id}/",
            "stats": "/en/squads/{team_id}/stats/"
        }
    },
    "understat": {
        "base_url": "https://understat.com",
        "paths": {
            "team": "/team/{team_name}"
        }
    },
    "sofascore": {
        "base_url": "https://api.sofascore.com/api/v1",
        "endpoints": {
            "team": "/team/{team_id}",
            "players": "/team/{team_id}/players"
        }
    },
    "transfermarkt": {
        "base_url": "https://www.transfermarkt.com",
        "paths": {
            "team": "/{team_name}/startseite/verein/{team_id}"
        }
    }
}

def init_local_cache() -> bool:
    """Initialize SQLite database for local cache"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS team_stats_cache (
            team_id TEXT PRIMARY KEY,
            data TEXT,
            timestamp INTEGER,
            source TEXT
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS execution_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER,
            teams_analyzed INTEGER,
            teams_updated INTEGER,
            duration_seconds REAL,
            source_usage TEXT
        )
        ''')
        
        conn.commit()
        conn.close()
        logger.info(f"Local cache initialized: {DB_PATH}")
        return True
    except Exception as e:
        logger.error(f"Cache initialization error: {str(e)}")
        return False

def get_from_cache(team_id: str) -> Optional[Dict]:
    """Retrieve team stats from local cache"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute("SELECT data, timestamp, source FROM team_stats_cache WHERE team_id = ?", (team_id,))
        result = cursor.fetchone()
        conn.close()
        
        if not result:
            return None
            
        data, timestamp, source = result
        cache_time = datetime.fromtimestamp(timestamp)
        
        if (datetime.now() - cache_time).total_seconds() > CONFIG["cache_expiry_hours"] * 3600:
            logger.debug(f"Cache expired for team {team_id}")
            return None
            
        stats = json.loads(data)
        stats['source'] = source  # Track data source
        return stats
    except Exception as e:
        logger.warning(f"Cache read error for team {team_id}: {str(e)}")
        return None

def save_to_cache(team_id: str, data: Dict, source: str) -> bool:
    """Save team stats to local cache"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        json_data = json.dumps(data)
        timestamp = int(time.time())
        
        cursor.execute(
            "INSERT OR REPLACE INTO team_stats_cache VALUES (?, ?, ?, ?)",
            (team_id, json_data, timestamp, source)
        )
        
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        logger.warning(f"Cache save error for team {team_id}: {str(e)}")
        return False

def initialize_firebase() -> bool:
    """Initialize Firebase connection with retry"""
    for attempt in range(CONFIG["max_retries"]):
        try:
            try:
                firebase_admin.get_app()
                logger.info("Firebase already initialized")
                return True
            except ValueError:
                pass
                
            cred_json = os.getenv('FIREBASE_CREDENTIALS')
            if cred_json:
                try:
                    cred_dict = json.loads(cred_json)
                    cred = credentials.Certificate(cred_dict)
                except json.JSONDecodeError:
                    cred = credentials.Certificate(cred_json)
            else:
                cred_path = os.path.expanduser('~/football-predictions/creds/firebase-credentials.json')
                cred = credentials.Certificate(cred_path)
                
            firebase_admin.initialize_app(cred, {
                'databaseURL': os.getenv('FIREBASE_DB_URL')
            })
            
            logger.info("Firebase initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Firebase init error (attempt {attempt+1}/{CONFIG['max_retries']}): {str(e)}")
            if attempt < CONFIG["max_retries"] - 1:
                time.sleep(2 ** attempt)
            else:
                return False

def get_session_with_retries() -> requests.Session:
    """Create HTTP session with automatic retries"""
    session = requests.Session()
    retry_strategy = Retry(
        total=CONFIG["max_retries"],
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({"User-Agent": random.choice(USER_AGENTS)})
    return session

def make_api_request(endpoint: str, params: Optional[Dict] = None) -> Optional[Dict]:
    """Make API request with error handling and retry"""
    if not API_KEY:
        logger.error("API Key not configured")
        return None
        
    url = f"{BASE_URL}/{endpoint}"
    headers = {
        "X-Auth-Token": API_KEY,
        "User-Agent": random.choice(USER_AGENTS)
    }
    
    session = get_session_with_retries()
    
    for attempt in range(CONFIG["max_retries"]):
        try:
            logger.info(f"API request: {url}")
            response = session.get(
                url, 
                headers=headers, 
                params=params, 
                timeout=CONFIG["api_timeout"]
            )
            
            time.sleep(random.uniform(CONFIG["min_delay"], CONFIG["max_delay"]))
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                logger.warning("Rate limit reached. Longer wait.")
                time.sleep(60)
                continue
            elif response.status_code == 404:
                logger.warning(f"Resource not found: {url}")
                return None
            else:
                logger.error(f"API error {response.status_code}: {response.text}")
                if attempt < CONFIG["max_retries"] - 1:
                    continue
                return None
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout for request {url}")
            if attempt < CONFIG["max_retries"] - 1:
                sleep_time = 2 ** attempt
                logger.info(f"Retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)
            else:
                return None
        except Exception as e:
            logger.error(f"Request error: {str(e)}")
            if attempt < CONFIG["max_retries"] - 1:
                sleep_time = 2 ** attempt
                logger.info(f"Retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)
            else:
                return None
    
    return None

def scrape_website(url: str, selector: Optional[str] = None) -> Optional[BeautifulSoup]:
    """Scrape website with retries and random delays"""
    try:
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.google.com/"
        }
        
        for attempt in range(CONFIG["scrape_max_attempts"]):
            try:
                time.sleep(random.uniform(1.5, 3.5))  # Respectful delay
                
                response = requests.get(
                    url,
                    headers=headers,
                    timeout=CONFIG["scrape_timeout"]
                )
                
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    if selector:
                        if soup.select_one(selector):
                            return soup
                        else:
                            logger.warning(f"Selector not found: {selector}")
                            return None
                    return soup
                elif response.status_code == 403:
                    logger.warning("Access denied - trying different user agent")
                    headers["User-Agent"] = random.choice(USER_AGENTS)
                    continue
                else:
                    logger.warning(f"HTTP {response.status_code} for {url}")
                    return None
                    
            except Exception as e:
                logger.warning(f"Scrape attempt {attempt+1} failed: {str(e)}")
                if attempt < CONFIG["scrape_max_attempts"] - 1:
                    time.sleep(5 * (attempt + 1))
                else:
                    return None
                    
    except Exception as e:
        logger.error(f"Scraping error for {url}: {str(e)}")
        return None

def get_fbref_team_stats(team_name: str, team_id: str) -> Optional[Dict]:
    """Get team stats from FBref"""
    try:
        # FBref uses team IDs in URLs - we need to find them first
        search_url = f"{FREE_SOURCES['fbref']['base_url']}/en/search/search.fcgi?search={team_name}"
        soup = scrape_website(search_url)
        
        if not soup:
            return None
            
        team_link = soup.find('a', href=re.compile(r'/en/squads/'), string=re.compile(team_name, re.I))
        if not team_link:
            return None
            
        team_url = f"{FREE_SOURCES['fbref']['base_url']}{team_link['href']}"
        stats_url = team_url.replace("/squads/", "/squads/stats/")
        soup = scrape_website(stats_url, selector='.stats_table')
        
        if not soup:
            return None
            
        # Extract advanced stats from tables
        stats = {}
        
        # Standard stats table
        standard_table = soup.select_one('table.stats_table')
        if standard_table:
            for row in standard_table.select('tbody tr'):
                if row.select_one('th[data-stat="season"]').text == '2022-2023':  # Current season
                    stats['goals_scored'] = int(row.select_one('td[data-stat="goals"]').text)
                    stats['goals_conceded'] = int(row.select_one('td[data-stat="goals_against"]').text)
                    stats['shots_per_game'] = float(row.select_one('td[data-stat="shots_per90"]').text)
                    stats['shots_on_target'] = float(row.select_one('td[data-stat="shots_on_target_per90"]').text)
                    break
        
        # Possession stats
        possession_table = soup.select_one('table#stats_squads_possession_for')
        if possession_table:
            for row in possession_table.select('tbody tr'):
                if row.select_one('th[data-stat="season"]').text == '2022-2023':
                    stats['possession'] = float(row.select_one('td[data-stat="possession"]').text)
                    stats['pass_accuracy'] = float(row.select_one('td[data-stat="passes_pct"]').text)
                    stats['passes_per_game'] = float(row.select_one('td[data-stat="passes"]').text)
                    break
        
        # Defensive stats
        defensive_table = soup.select_one('table#stats_squads_defense_for')
        if defensive_table:
            for row in defensive_table.select('tbody tr'):
                if row.select_one('th[data-stat="season"]').text == '2022-2023':
                    stats['tackles_per_game'] = float(row.select_one('td[data-stat="tackles"]').text)
                    stats['interceptions_per_game'] = float(row.select_one('td[data-stat="interceptions"]').text)
                    stats['fouls_per_game'] = float(row.select_one('td[data-stat="fouls"]').text)
                    break
        
        return stats if stats else None
        
    except Exception as e:
        logger.error(f"FBref scraping error: {str(e)}")
        return None

def get_understat_team_stats(team_name: str) -> Optional[Dict]:
    """Get team stats from Understat"""
    try:
        # Understat uses team names in URLs
        team_slug = team_name.lower().replace(' ', '_')
        url = f"{FREE_SOURCES['understat']['base_url']}/team/{team_slug}"
        
        soup = scrape_website(url)
        if not soup:
            return None
            
        # Understat stores data in script tags
        scripts = soup.find_all('script')
        for script in scripts:
            if 'teamData' in script.text:
                # Extract JSON data
                json_str = re.search(r"JSON\.parse\('(.*?)'\)", script.text).group(1)
                json_str = json_str.encode('utf8').decode('unicode_escape')
                data = json.loads(json_str)
                
                # Get current season stats
                current_season = max(data.keys())
                season_stats = data[current_season]['history']
                
                if not season_stats:
                    return None
                
                # Calculate averages
                stats = {
                    'xG': round(sum(float(match['xG']) for match in season_stats) / len(season_stats), 2),
                    'xGA': round(sum(float(match['xGA']) for match in season_stats) / len(season_stats), 2),
                    'shots_per_game': round(sum(float(match['shots']) for match in season_stats) / len(season_stats), 1),
                    'shots_on_target_per_game': round(sum(float(match['shots_on_target']) for match in season_stats) / len(season_stats), 1),
                    'deep_per_game': round(sum(float(match['deep']) for match in season_stats) / len(season_stats), 1),
                    'ppda': round(sum(float(match['ppda']['att']) / float(match['ppda']['def']) for match in season_stats if match['ppda']['def'] > 0) / len(season_stats), 2)
                }
                
                return stats
                
        return None
        
    except Exception as e:
        logger.error(f"Understat scraping error: {str(e)}")
        return None

def get_sofascore_team_stats(team_id: str) -> Optional[Dict]:
    """Get team stats from SofaScore API"""
    try:
        url = f"{FREE_SOURCES['sofascore']['base_url']}/team/{team_id}"
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "application/json"
        }
        
        response = requests.get(url, headers=headers, timeout=CONFIG["api_timeout"])
        if response.status_code != 200:
            return None
            
        data = response.json()
        
        stats = {
            'rating': data['team']['rating'],
            'matches_played': data['team']['matches'],
            'wins': data['team']['wins'],
            'draws': data['team']['draws'],
            'losses': data['team']['losses']
        }
        
        # Get players
        players_url = f"{FREE_SOURCES['sofascore']['base_url']}/team/{team_id}/players"
        response = requests.get(players_url, headers=headers, timeout=CONFIG["api_timeout"])
        if response.status_code == 200:
            players_data = response.json()
            stats['key_players'] = []
            
            for player in players_data['players'][:5]:  # Top 5 players
                stats['key_players'].append({
                    'name': player['player']['name'],
                    'rating': player['player']['rating'],
                    'goals': player['player']['goals'],
                    'assists': player['player']['assists']
                })
        
        return stats
        
    except Exception as e:
        logger.error(f"SofaScore API error: {str(e)}")
        return None

def get_transfermarkt_team_stats(team_name: str, team_id: str) -> Optional[Dict]:
    """Get team stats from Transfermarkt"""
    try:
        # Transfermarkt uses team IDs and names in URLs
        team_slug = team_name.lower().replace(' ', '-')
        url = f"{FREE_SOURCES['transfermarkt']['base_url']}/{team_slug}/startseite/verein/{team_id}"
        
        soup = scrape_website(url, selector='.items')
        if not soup:
            return None
            
        # Extract squad value
        squad_value = soup.select_one('.dataMarktwert a').text.strip()
        
        # Extract key players
        key_players = []
        for row in soup.select('.items tbody tr')[:5]:  # Top 5 players
            try:
                player = {
                    'name': row.select_one('.hauptlink a').text.strip(),
                    'position': row.select_one('.inline-table tr+tr td').text.strip(),
                    'market_value': row.select_one('.rechts.hauptlink').text.strip()
                }
                key_players.append(player)
            except:
                continue
        
        return {
            'squad_value': squad_value,
            'key_players': key_players
        }
        
    except Exception as e:
        logger.error(f"Transfermarkt scraping error: {str(e)}")
        return None

def get_team_stats(team_id: str, team_name: str) -> Optional[Dict]:
    """Get team stats from multiple sources"""
    # Check cache first
    cached_stats = get_from_cache(team_id)
    if cached_stats:
        logger.info(f"Using cached stats for team {team_name} (ID: {team_id})")
        cached_stats['last_updated'] = datetime.now().isoformat()
        return cached_stats
    
    # Try multiple sources in order of reliability
    sources = [
        ('football_data', lambda: get_football_data_team_stats(team_id)),
        ('fbref', lambda: get_fbref_team_stats(team_name, team_id)),
        ('understat', lambda: get_understat_team_stats(team_name)),
        ('sofascore', lambda: get_sofascore_team_stats(team_id)),
        ('transfermarkt', lambda: get_transfermarkt_team_stats(team_name, team_id))
    ]
    
    team_stats = None
    source_used = None
    
    for source_name, source_func in sources:
        try:
            stats = source_func()
            if stats:
                team_stats = stats
                source_used = source_name
                logger.info(f"Successfully got team stats from {source_name}")
                break
        except Exception as e:
            logger.warning(f"Error getting team stats from {source_name}: {str(e)}")
            continue
    
    if not team_stats:
        logger.error("Could not get team stats from any source")
        return None
    
    # Add team metadata
    team_stats.update({
        'id': team_id,
        'name': team_name,
        'last_updated': datetime.now().isoformat()
    })
    
    # Save to cache
    save_to_cache(team_id, team_stats, source_used)
    
    return team_stats

def get_football_data_team_stats(team_id: str) -> Optional[Dict]:
    """Get team stats from Football Data API"""
    try:
        # 1. Basic team info
        team_data = make_api_request(f"teams/{team_id}")
        if not team_data:
            return None
        
        # 2. Recent matches
        matches_data = make_api_request(f"teams/{team_id}/matches", {
            "status": "FINISHED", 
            "limit": CONFIG["matches_to_analyze"]
        })
        
        # Calculate form from recent matches
        form = ""
        goals_scored = 0
        goals_conceded = 0
        wins = 0
        draws = 0
        losses = 0
        
        recent_matches = []
        
        if matches_data and 'matches' in matches_data:
            recent_matches = sorted(matches_data['matches'], key=lambda m: m['utcDate'], reverse=True)
            recent_matches = recent_matches[:CONFIG["recent_form_matches"]]
            
            for match in recent_matches:
                is_home = match['homeTeam']['id'] == int(team_id)
                team_goals = match['score']['fullTime']['home'] if is_home else match['score']['fullTime']['away']
                opponent_goals = match['score']['fullTime']['away'] if is_home else match['score']['fullTime']['home']
                
                if team_goals is not None and opponent_goals is not None:
                    goals_scored += team_goals
                    goals_conceded += opponent_goals
                    
                    if team_goals > opponent_goals:
                        form += "W"
                        wins += 1
                    elif team_goals < opponent_goals:
                        form += "L"
                        losses += 1
                    else:
                        form += "D"
                        draws += 1
        
        # 3. League position
        league_position = None
        if team_data and 'runningCompetitions' in team_data:
            for comp in team_data['runningCompetitions']:
                if 'code' in comp:
                    standings_data = make_api_request(f"competitions/{comp['code']}/standings")
                    if standings_data and 'standings' in standings_data:
                        for standing_type in standings_data['standings']:
                            for table_row in standing_type.get('table', []):
                                if table_row['team']['id'] == int(team_id):
                                    league_position = {
                                        'competition': comp['name'],
                                        'position': table_row['position'],
                                        'played': table_row['playedGames'],
                                        'points': table_row['points'],
                                        'won': table_row['won'],
                                        'draw': table_row['draw'],
                                        'lost': table_row['lost'],
                                        'goals_for': table_row['goalsFor'],
                                        'goals_against': table_row['goalsAgainst']
                                    }
                                    break
                            if league_position:
                                break
                    if league_position:
                        break
        
        # Compose final stats
        stats = {
            'id': team_id,
            'name': team_data['name'],
            'country': team_data.get('area', {}).get('name', 'Unknown'),
            'crest_url': team_data.get('crest'),
            'form': form,
            'form_stats': {
                'wins': wins,
                'draws': draws,
                'losses': losses
            },
            'goals_stats': {
                'scored': goals_scored,
                'conceded': goals_conceded,
                'per_match_scored': round(goals_scored / max(len(recent_matches), 1), 2),
                'per_match_conceded': round(goals_conceded / max(len(recent_matches), 1), 2)
            },
            'league_position': league_position,
            'last_updated': datetime.now().isoformat()
        }
        
        return stats
        
    except Exception as e:
        logger.error(f"Football Data API error: {str(e)}")
        return None

def save_team_stats(team_stats: Dict) -> bool:
    """Save team stats to Firebase"""
    if not team_stats:
        return False
    
    try:
        team_id = team_stats['id']
        ref = db.reference(f'team_stats/{team_id}')
        ref.set(team_stats)
        logger.info(f"Team stats saved for {team_stats['name']} (ID: {team_id})")
        return True
    except Exception as e:
        logger.error(f"Error saving team stats for {team_stats.get('id', 'unknown')}: {str(e)}")
        return False

def get_teams_needing_stats() -> List[Dict]:
    """Get teams that need updated statistics"""
    try:
        ref = db.reference('matches')
        teams_to_update = []
        
        # Get matches in configured range
        today = datetime.now().date()
        start_date = today - timedelta(days=CONFIG["match_days_behind"])
        end_date = today + timedelta(days=CONFIG["match_days_ahead"])
        
        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            matches = ref.child(date_str).get() or {}
            
            for match_id, match_data in matches.items():
                # Add teams with their names
                if 'home_team_id' in match_data and 'home_team' in match_data:
                    teams_to_update.append({
                        'id': str(match_data['home_team_id']),
                        'name': match_data['home_team']
                    })
                if 'away_team_id' in match_data and 'away_team' in match_data:
                    teams_to_update.append({
                        'id': str(match_data['away_team_id']),
                        'name': match_data['away_team']
                    })
            
            current_date += timedelta(days=1)
        
        # Remove duplicates
        unique_teams = {}
        for team in teams_to_update:
            if team['id'] not in unique_teams:
                unique_teams[team['id']] = team
        
        # Check which teams need updating
        stats_ref = db.reference('team_stats')
        stats_data = stats_ref.get() or {}
        
        fresh_cutoff = (datetime.now() - timedelta(hours=CONFIG["stats_fresh_hours"])).isoformat()
        teams_to_process = []
        
        for team_id, team in unique_teams.items():
            if team_id not in stats_data or 'last_updated' not in stats_data[team_id] or stats_data[team_id]['last_updated'] < fresh_cutoff:
                teams_to_process.append(team)
        
        # Prioritize teams without stats
        teams_to_process.sort(key=lambda x: 0 if x['id'] not in stats_data else 1)
        
        logger.info(f"Found {len(teams_to_process)} teams to update out of {len(unique_teams)} total")
        return teams_to_process
        
    except Exception as e:
        logger.error(f"Error getting teams needing updates: {str(e)}")
        return []

def main():
    """Main function"""
    start_time = datetime.now()
    logger.info(f"Starting Fetch Team Stats - {start_time.isoformat()}")
    
    try:
        # Initialize cache and Firebase
        init_local_cache()
        if not initialize_firebase():
            logger.error("Firebase initialization failed")
            return 1
        
        # Get teams needing updates
        teams_to_update = get_teams_needing_stats()
        if not teams_to_update:
            logger.info("No teams need stats updates")
            return 0
        
        # Process teams with limit
        updated_count = 0
        for team in teams_to_update[:CONFIG["max_teams_per_execution"]]:
            logger.info(f"Processing stats for {team['name']} (ID: {team['id']})")
            
            try:
                team_stats = get_team_stats(team['id'], team['name'])
                if team_stats and save_team_stats(team_stats):
                    updated_count += 1
                time.sleep(random.uniform(CONFIG["min_delay"], CONFIG["max_delay"]))
            except Exception as e:
                logger.error(f"Error processing team {team['id']}: {str(e)}")
                continue
        
        logger.info(f"Updated stats for {updated_count} teams")
        return 0
        
    except Exception as e:
        logger.error(f"General error: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
