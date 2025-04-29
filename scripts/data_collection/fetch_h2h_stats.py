#!/usr/bin/env python3
"""
Fetch Head-to-Head Stats - Collects direct confrontation statistics between teams
Analyzes historical matches to enrich predictions
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
from typing import Dict, List, Any, Optional, Tuple, Set
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
log_file = os.path.join(log_dir, f"fetch_h2h_stats_{datetime.now().strftime('%Y%m%d')}.log")

# Setup logging with multiple destinations
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger('fetch_h2h_stats')

# Local cache configuration
DB_PATH = os.path.expanduser("~/football-predictions/cache/h2h_stats_cache.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# Configurable parameters
load_dotenv()
CONFIG = {
    "max_matches_per_execution": int(os.getenv('MAX_H2H_MATCHES_PER_EXECUTION', '5')),
    "api_timeout": int(os.getenv('API_TIMEOUT', '30')),
    "max_retries": int(os.getenv('MAX_RETRIES', '3')),
    "min_delay": float(os.getenv('MIN_DELAY', '2.0')),
    "max_delay": float(os.getenv('MAX_DELAY', '5.0')),
    "match_days_ahead": int(os.getenv('MATCH_DAYS_AHEAD', '3')),
    "match_days_behind": int(os.getenv('MATCH_DAYS_BEHIND', '0')),
    "cache_expiry_hours": int(os.getenv('CACHE_EXPIRY_HOURS', '24')),
    "h2h_matches_limit": int(os.getenv('H2H_MATCHES_LIMIT', '100')),
    "h2h_recent_matches": int(os.getenv('H2H_RECENT_MATCHES', '10')),
    "trend_analysis_matches": int(os.getenv('TREND_ANALYSIS_MATCHES', '3')),
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
            "h2h": "teams/{team_id}/matches"
        }
    },
    "fbref": {
        "base_url": "https://fbref.com",
        "paths": {
            "team": "/en/squads/{team_id}/",
            "h2h": "/en/matches/{match_id}"
        }
    },
    "understat": {
        "base_url": "https://understat.com",
        "paths": {
            "team": "/team/{team_name}",
            "match": "/match/{match_id}"
        }
    },
    "sofascore": {
        "base_url": "https://api.sofascore.com/api/v1",
        "endpoints": {
            "h2h": "/team/{team_id}/events/last/0"
        }
    },
    "flashscore": {
        "base_url": "https://www.flashscore.com",
        "paths": {
            "h2h": "/match/{home_team}-vs-{away_team}/h2h/"
        }
    }
}

def init_local_cache() -> bool:
    """Initialize SQLite database for local cache"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS h2h_cache (
            match_key TEXT PRIMARY KEY,
            data TEXT,
            timestamp INTEGER,
            source TEXT
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS execution_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER,
            matches_processed INTEGER,
            matches_updated INTEGER,
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

def get_from_cache(match_key: str) -> Optional[Dict]:
    """Retrieve h2h stats from local cache"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute("SELECT data, timestamp, source FROM h2h_cache WHERE match_key = ?", (match_key,))
        result = cursor.fetchone()
        conn.close()
        
        if not result:
            return None
            
        data, timestamp, source = result
        cache_time = datetime.fromtimestamp(timestamp)
        
        if (datetime.now() - cache_time).total_seconds() > CONFIG["cache_expiry_hours"] * 3600:
            logger.debug(f"Cache expired for match {match_key}")
            return None
            
        stats = json.loads(data)
        stats['source'] = source  # Track data source
        return stats
    except Exception as e:
        logger.warning(f"Cache read error for match {match_key}: {str(e)}")
        return None

def save_to_cache(match_key: str, data: Dict, source: str) -> bool:
    """Save h2h stats to local cache"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        json_data = json.dumps(data)
        timestamp = int(time.time())
        
        cursor.execute(
            "INSERT OR REPLACE INTO h2h_cache VALUES (?, ?, ?, ?)",
            (match_key, json_data, timestamp, source)
        )
        
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        logger.warning(f"Cache save error for match {match_key}: {str(e)}")
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

def get_fbref_h2h_stats(home_team: str, away_team: str) -> Optional[Dict]:
    """Get H2H stats from FBref"""
    try:
        # FBref uses team IDs in URLs - we need to find them first
        search_url = f"{FREE_SOURCES['fbref']['base_url']}/en/search/search.fcgi?search={home_team}"
        soup = scrape_website(search_url)
        
        if not soup:
            return None
            
        team_link = soup.find('a', href=re.compile(r'/en/squads/'), string=re.compile(home_team, re.I))
        if not team_link:
            return None
            
        team_url = f"{FREE_SOURCES['fbref']['base_url']}{team_link['href']}"
        team_soup = scrape_website(team_url)
        
        if not team_soup:
            return None
            
        # Find matches against opponent
        matches = []
        for row in team_soup.select('table.matches tbody tr'):
            opponent = row.select_one('td[data-stat="opponent"] a')
            if opponent and fuzz.partial_ratio(away_team.lower(), opponent.text.lower()) > 80:
                match_data = {
                    'date': row.select_one('th[data-stat="date"]').text,
                    'result': row.select_one('td[data-stat="result"]').text,
                    'score': row.select_one('td[data-stat="score"]').text,
                    'competition': row.select_one('td[data-stat="competition"]').text
                }
                matches.append(match_data)
        
        if not matches:
            return None
            
        # Calculate basic stats
        home_wins = 0
        away_wins = 0
        draws = 0
        home_goals = 0
        away_goals = 0
        
        for match in matches:
            if match['result'] == 'W':
                home_wins += 1
            elif match['result'] == 'L':
                away_wins += 1
            else:
                draws += 1
                
            # Parse score
            if 'score' in match and match['score']:
                try:
                    home, away = map(int, match['score'].split('–'))
                    home_goals += home
                    away_goals += away
                except:
                    pass
        
        return {
            'source': 'fbref',
            'total_matches': len(matches),
            'home_wins': home_wins,
            'away_wins': away_wins,
            'draws': draws,
            'home_goals': home_goals,
            'away_goals': away_goals,
            'matches': matches[:CONFIG["h2h_recent_matches"]]
        }
        
    except Exception as e:
        logger.error(f"FBref scraping error: {str(e)}")
        return None

def get_sofascore_h2h_stats(home_team_id: str, away_team_id: str) -> Optional[Dict]:
    """Get H2H stats from SofaScore API"""
    try:
        url = f"{FREE_SOURCES['sofascore']['base_url']}/teams/{home_team_id}/events/last/0"
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "application/json"
        }
        
        response = requests.get(url, headers=headers, timeout=CONFIG["api_timeout"])
        if response.status_code != 200:
            return None
            
        data = response.json()
        h2h_matches = []
        
        for event in data['events']:
            if (event['homeTeam']['id'] == int(home_team_id) and event['awayTeam']['id'] == int(away_team_id)) or \
               (event['homeTeam']['id'] == int(away_team_id) and event['awayTeam']['id'] == int(home_team_id)):
                
                if event['status']['type'] == 'finished' and 'homeScore' in event:
                    match_data = {
                        'date': event['startTimestamp'],
                        'home_team': event['homeTeam']['name'],
                        'away_team': event['awayTeam']['name'],
                        'score': f"{event['homeScore']['current']}-{event['awayScore']['current']}",
                        'competition': event['tournament']['name']
                    }
                    
                    # Add additional stats if available
                    if 'statistics' in event:
                        match_data['stats'] = event['statistics']
                    
                    h2h_matches.append(match_data)
        
        if not h2h_matches:
            return None
            
        # Calculate basic stats
        home_wins = 0
        away_wins = 0
        draws = 0
        home_goals = 0
        away_goals = 0
        
        for match in h2h_matches:
            home, away = map(int, match['score'].split('-'))
            if home > away:
                home_wins += 1
            elif away > home:
                away_wins += 1
            else:
                draws += 1
                
            home_goals += home
            away_goals += away
        
        return {
            'source': 'sofascore',
            'total_matches': len(h2h_matches),
            'home_wins': home_wins,
            'away_wins': away_wins,
            'draws': draws,
            'home_goals': home_goals,
            'away_goals': away_goals,
            'matches': h2h_matches[:CONFIG["h2h_recent_matches"]]
        }
        
    except Exception as e:
        logger.error(f"SofaScore API error: {str(e)}")
        return None

def get_flashscore_h2h_stats(home_team: str, away_team: str) -> Optional[Dict]:
    """Get H2H stats from FlashScore"""
    try:
        # Create URL-friendly team names
        home_slug = home_team.lower().replace(' ', '-')
        away_slug = away_team.lower().replace(' ', '-')
        url = f"{FREE_SOURCES['flashscore']['base_url']}/match/{home_slug}-vs-{away_slug}/h2h/"
        
        soup = scrape_website(url, selector='.h2h__section')
        if not soup:
            return None
            
        # Extract H2H matches
        matches = []
        for row in soup.select('.h2h__row'):
            try:
                date = row.select_one('.h2h__date').text.strip()
                home = row.select_one('.h2h__homeParticipant').text.strip()
                away = row.select_one('.h2h__awayParticipant').text.strip()
                score = row.select_one('.h2h__score').text.strip()
                competition = row.select_one('.h2h__competition').text.strip()
                
                matches.append({
                    'date': date,
                    'home_team': home,
                    'away_team': away,
                    'score': score,
                    'competition': competition
                })
            except:
                continue
        
        if not matches:
            return None
            
        # Calculate basic stats
        home_wins = 0
        away_wins = 0
        draws = 0
        home_goals = 0
        away_goals = 0
        
        for match in matches:
            try:
                home, away = map(int, match['score'].split(':'))
                if home > away:
                    home_wins += 1
                elif away > home:
                    away_wins += 1
                else:
                    draws += 1
                    
                home_goals += home
                away_goals += away
            except:
                continue
        
        return {
            'source': 'flashscore',
            'total_matches': len(matches),
            'home_wins': home_wins,
            'away_wins': away_wins,
            'draws': draws,
            'home_goals': home_goals,
            'away_goals': away_goals,
            'matches': matches[:CONFIG["h2h_recent_matches"]]
        }
        
    except Exception as e:
        logger.error(f"FlashScore scraping error: {str(e)}")
        return None

def get_h2h_stats(match: Dict) -> Optional[Dict]:
    """Get head-to-head stats for a match from multiple sources"""
    cache_key = f"h2h_{match['home_team_id']}_{match['away_team_id']}"
    
    # Check cache first
    cached_stats = get_from_cache(cache_key)
    if cached_stats:
        logger.info(f"Using cached h2h stats for {match['home_team']} vs {match['away_team']}")
        cached_stats['match_id'] = match['id']
        return cached_stats
    
    # Try multiple sources in order of reliability
    sources = [
        ('football_data', lambda: get_football_data_h2h(match)),
        ('sofascore', lambda: get_sofascore_h2h_stats(match['home_team_id'], match['away_team_id'])),
        ('fbref', lambda: get_fbref_h2h_stats(match['home_team'], match['away_team'])),
        ('flashscore', lambda: get_flashscore_h2h_stats(match['home_team'], match['away_team']))
    ]
    
    h2h_stats = None
    source_used = None
    
    for source_name, source_func in sources:
        try:
            stats = source_func()
            if stats:
                h2h_stats = stats
                source_used = source_name
                logger.info(f"Successfully got H2H stats from {source_name}")
                break
        except Exception as e:
            logger.warning(f"Error getting H2H from {source_name}: {str(e)}")
            continue
    
    if not h2h_stats:
        logger.error("Could not get H2H stats from any source")
        return None
    
    # Add match metadata
    h2h_stats.update({
        'match_id': match['id'],
        'home_team': match['home_team'],
        'home_team_id': match['home_team_id'],
        'away_team': match['away_team'],
        'away_team_id': match['away_team_id'],
        'last_updated': datetime.now().isoformat()
    })
    
    # Calculate additional metrics
    matches_analyzed = len(h2h_stats.get('matches', []))
    if matches_analyzed > 0:
        # Win percentages
        h2h_stats['win_percentage'] = {
            'home': round(h2h_stats['home_wins'] / matches_analyzed * 100, 2),
            'away': round(h2h_stats['away_wins'] / matches_analyzed * 100, 2),
            'draw': round(h2h_stats['draws'] / matches_analyzed * 100, 2)
        }
        
        # Average goals
        h2h_stats['avg_goals_per_match'] = round(
            (h2h_stats['home_goals'] + h2h_stats['away_goals']) / matches_analyzed, 2)
        
        # Recent trends
        recent_matches = min(CONFIG["trend_analysis_matches"], matches_analyzed)
        if recent_matches > 0:
            recent_home_wins = 0
            recent_away_wins = 0
            recent_goals = 0
            
            for m in h2h_stats['matches'][:recent_matches]:
                home, away = map(int, m['score'].split('-') if '-' in m['score'] else (0, 0))
                if home > away:
                    recent_home_wins += 1
                elif away > home:
                    recent_away_wins += 1
                recent_goals += home + away
            
            # Determine winner trend
            if recent_home_wins >= recent_matches * 0.6:
                h2h_stats['recent_trend'] = "HOME_ADVANTAGE"
            elif recent_away_wins >= recent_matches * 0.6:
                h2h_stats['recent_trend'] = "AWAY_ADVANTAGE"
            elif (recent_home_wins + recent_away_wins) == 0:
                h2h_stats['recent_trend'] = "DRAW_TREND"
            else:
                h2h_stats['recent_trend'] = "NEUTRAL"
            
            # Determine scoring trend
            avg_goals = recent_goals / recent_matches
            if avg_goals <= 1.5:
                h2h_stats['scoring_trend'] = "LOW_SCORING"
            elif avg_goals >= 3.0:
                h2h_stats['scoring_trend'] = "HIGH_SCORING"
            else:
                h2h_stats['scoring_trend'] = "MEDIUM_SCORING"
    
    # Save to cache
    save_to_cache(cache_key, h2h_stats, source_used)
    
    return h2h_stats

def save_h2h_stats(h2h_stats: Dict) -> bool:
    """Save h2h stats to Firebase"""
    if not h2h_stats:
        return False
    
    try:
        match_id = h2h_stats['match_id']
        ref = db.reference(f'h2h/{match_id}')
        ref.set(h2h_stats)
        logger.info(f"H2H saved for {h2h_stats['home_team']} vs {h2h_stats['away_team']} (Match ID: {match_id})")
        return True
    except Exception as e:
        logger.error(f"Error saving H2H stats for match ID {h2h_stats.get('match_id', 'unknown')}: {str(e)}")
        return False

def main():
    """Main function"""
    start_time = datetime.now()
    logger.info(f"Starting Fetch H2H Stats - {start_time.isoformat()}")
    
    try:
        # Initialize cache and Firebase
        init_local_cache()
        if not initialize_firebase():
            logger.error("Firebase initialization failed")
            return 1
        
        # Get matches to process
        matches = get_upcoming_matches_without_h2h()
        if not matches:
            logger.info("No matches need H2H analysis")
            return 0
        
        # Process matches with limit
        processed_count = 0
        for match in matches[:CONFIG["max_matches_per_execution"]]:
            logger.info(f"Processing H2H for: {match['home_team']} vs {match['away_team']}")
            
            try:
                h2h_stats = get_h2h_stats(match)
                if h2h_stats and save_h2h_stats(h2h_stats):
                    processed_count += 1
                time.sleep(random.uniform(CONFIG["min_delay"], CONFIG["max_delay"]))
            except Exception as e:
                logger.error(f"Error processing H2H for match {match['id']}: {str(e)}")
                continue
        
        logger.info(f"Processed {processed_count} matches")
        return 0
        
    except Exception as e:
        logger.error(f"General error: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
