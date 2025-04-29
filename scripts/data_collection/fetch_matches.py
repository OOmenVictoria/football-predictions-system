#!/usr/bin/env python3
"""
Enhanced Football Data Collection System
Versione ottimizzata con:
- Integrazione di più fonti gratuite (API e scraping)
- Sostituzione completa dei dati simulati con dati reali
- Miglior gestione degli errori e resilienza
- Cache intelligente e ottimizzata
- Monitoraggio dello stato delle fonti
"""

import os
import sys
import requests
import json
import logging
import time
import random
import re
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import firebase_admin
from firebase_admin import credentials, db
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib.robotparser as robotparser
from fuzzywuzzy import fuzz

# Configurazione logging avanzata
log_dir = os.path.expanduser('~/football-predictions/logs')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"data_collection_{datetime.now().strftime('%Y%m%d')}.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('football_data')

# Caricamento configurazione
load_dotenv()
CONFIG = {
    "days_ahead": int(os.getenv('DAYS_AHEAD', '3')),
    "days_behind": int(os.getenv('DAYS_BEHIND', '0')),
    "cache_expiry_hours": int(os.getenv('CACHE_EXPIRY_HOURS', '6')),
    "match_expiry_days": int(os.getenv('MATCH_EXPIRY_DAYS', '30')),
    "max_retries": int(os.getenv('MAX_RETRIES', '3')),
    "min_delay": float(os.getenv('MIN_DELAY', '1.0')),
    "max_delay": float(os.getenv('MAX_DELAY', '3.0')),
    "api_timeout": int(os.getenv('API_TIMEOUT', '30')),
    "max_log_size_mb": int(os.getenv('MAX_LOG_SIZE_MB', '10')),
    "source_limits": {
        "fbref": int(os.getenv('FBREF_LIMIT', '50')),
        "understat": int(os.getenv('UNDERSTAT_LIMIT', '50')),
        "sofascore": int(os.getenv('SOFASCORE_LIMIT', '50')),
        "flashscore": int(os.getenv('FLASHSCORE_LIMIT', '50')),
        "football_data": int(os.getenv('FOOTBALL_DATA_LIMIT', '100'))
    }
}

# Database locale per cache
DB_PATH = os.path.expanduser("~/football-predictions/cache/football_cache.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# User Agents per scraping
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0",
    "Googlebot/2.1 (+http://www.google.com/bot.html)"
]

# Fonti dati primarie
PRIMARY_SOURCES = {
    "fbref": {
        "base_url": "https://fbref.com",
        "priority": 1,
        "type": "scraping",
        "stats": ["xG", "xA", "shots", "possession", "passing", "defense"]
    },
    "understat": {
        "base_url": "https://understat.com",
        "priority": 2,
        "type": "scraping",
        "stats": ["xG", "xA", "shots", "deep", "ppda"]
    },
    "sofascore": {
        "base_url": "https://api.sofascore.com/api/v1",
        "priority": 3,
        "type": "api",
        "stats": ["rating", "shots", "possession", "passing", "defense"]
    },
    "flashscore": {
        "base_url": "https://www.flashscore.com",
        "priority": 4,
        "type": "scraping",
        "stats": ["h2h", "form", "standings"]
    },
    "football_data": {
        "base_url": "https://api.football-data.org/v4",
        "priority": 5,
        "type": "api",
        "stats": ["matches", "standings", "scorers"]
    }
}

# Inizializzazione cache locale
def init_local_cache():
    """Inizializza il database SQLite per la cache locale"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS matches (
            match_id TEXT PRIMARY KEY,
            data TEXT,
            source TEXT,
            timestamp INTEGER
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS teams (
            team_id TEXT PRIMARY KEY,
            data TEXT,
            source TEXT,
            timestamp INTEGER
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS h2h (
            h2h_id TEXT PRIMARY KEY,
            data TEXT,
            source TEXT,
            timestamp INTEGER
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS source_status (
            source_name TEXT PRIMARY KEY,
            last_success INTEGER,
            last_checked INTEGER,
            status TEXT,
            error_count INTEGER
        )
        ''')
        
        conn.commit()
        conn.close()
        logger.info("Cache locale inizializzata")
        return True
    except Exception as e:
        logger.error(f"Errore inizializzazione cache: {str(e)}")
        return False

# Gestione cache
def get_from_cache(key, table="matches"):
    """Recupera dati dalla cache locale"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute(f"SELECT data, timestamp FROM {table} WHERE {table}_id = ?", (key,))
        result = cursor.fetchone()
        conn.close()
        
        if not result:
            return None
            
        data, timestamp = result
        cache_time = datetime.fromtimestamp(timestamp)
        
        if (datetime.now() - cache_time).total_seconds() > CONFIG["cache_expiry_hours"] * 3600:
            return None
            
        return json.loads(data)
    except Exception as e:
        logger.warning(f"Errore lettura cache: {str(e)}")
        return None

def save_to_cache(key, data, source, table="matches"):
    """Salva dati nella cache locale"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        json_data = json.dumps(data)
        timestamp = int(time.time())
        
        cursor.execute(
            f"INSERT OR REPLACE INTO {table} VALUES (?, ?, ?, ?)",
            (key, json_data, source, timestamp)
        )
        
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        logger.warning(f"Errore salvataggio cache: {str(e)}")
        return False

# Gestione richieste HTTP
def get_session_with_retries():
    """Crea una sessione HTTP con retry automatici"""
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

def make_request(url, method="get", **kwargs):
    """Esegue richiesta HTTP con gestione errori"""
    session = get_session_with_retries()
    
    try:
        time.sleep(random.uniform(CONFIG["min_delay"], CONFIG["max_delay"]))
        
        if method.lower() == "get":
            response = session.get(url, timeout=CONFIG["api_timeout"], **kwargs)
        elif method.lower() == "post":
            response = session.post(url, timeout=CONFIG["api_timeout"], **kwargs)
        else:
            raise ValueError(f"Metodo non supportato: {method}")
        
        if response.status_code == 200:
            return response
        elif response.status_code == 429:
            logger.warning(f"Rate limit raggiunto per {url}")
            time.sleep(60)
            return None
        else:
            logger.error(f"Errore richiesta {url}: {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"Errore connessione a {url}: {str(e)}")
        return None

# Gestione fonti
def check_source_availability(source_name):
    """Verifica la disponibilità di una fonte dati"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute(
            "SELECT status, last_checked FROM source_status WHERE source_name = ?",
            (source_name,)
        )
        result = cursor.fetchone()
        
        if result:
            status, last_checked = result
            # Se controllato meno di 1 ora fa, ritorna lo stato memorizzato
            if time.time() - last_checked < 3600:
                return status == "available"
        
        # Esegui un test per verificare la fonte
        source_config = PRIMARY_SOURCES.get(source_name)
        if not source_config:
            return False
            
        if source_config["type"] == "api":
            test_url = f"{source_config['base_url']}/ping"
        else:
            test_url = source_config['base_url']
        
        response = make_request(test_url)
        is_available = response is not None
        
        # Aggiorna lo stato nel database
        cursor.execute(
            "INSERT OR REPLACE INTO source_status VALUES (?, ?, ?, ?, ?)",
            (
                source_name,
                int(time.time()) if is_available else None,
                int(time.time()),
                "available" if is_available else "unavailable",
                0 if is_available else 1
            )
        )
        
        conn.commit()
        conn.close()
        return is_available
    except Exception as e:
        logger.error(f"Errore verifica disponibilità fonte {source_name}: {str(e)}")
        return False

def get_best_available_source(required_stats):
    """Seleziona la migliore fonte disponibile per i dati richiesti"""
    available_sources = []
    
    for source_name, source_config in PRIMARY_SOURCES.items():
        if not set(required_stats).issubset(set(source_config["stats"])):
            continue
            
        if check_source_availability(source_name):
            available_sources.append({
                "name": source_name,
                "priority": source_config["priority"],
                "type": source_config["type"]
            })
    
    if not available_sources:
        return None
        
    # Ordina per priorità (più bassa = migliore)
    available_sources.sort(key=lambda x: x["priority"])
    return available_sources[0]["name"]

# Raccolta dati squadre
def get_team_stats(team_id, team_name):
    """Ottiene statistiche avanzate per una squadra"""
    required_stats = ["xG", "possession", "shots", "passing", "defense"]
    source_name = get_best_available_source(required_stats)
    
    if not source_name:
        logger.warning("Nessuna fonte disponibile per statistiche squadra")
        return None
    
    # Verifica cache
    cache_key = f"{source_name}_{team_id}"
    cached_data = get_from_cache(cache_key, "teams")
    if cached_data:
        return cached_data
    
    logger.info(f"Recupero statistiche per {team_name} da {source_name}")
    
    if source_name == "fbref":
        return get_team_stats_from_fbref(team_name)
    elif source_name == "understat":
        return get_team_stats_from_understat(team_name)
    elif source_name == "sofascore":
        return get_team_stats_from_sofascore(team_id)
    else:
        return None

def get_team_stats_from_fbref(team_name):
    """Ottiene statistiche squadra da FBref"""
    try:
        normalized_name = team_name.lower().replace(' ', '-')
        url = f"https://fbref.com/en/squads/{normalized_name}/stats/"
        
        response = make_request(url)
        if not response:
            return None
            
        soup = BeautifulSoup(response.text, 'html.parser')
        stats = {}
        
        # Estrai xG
        xg_element = soup.select_one(".stats_table [data-stat='xg']")
        if xg_element:
            stats['xG'] = float(xg_element.text.strip())
        
        # Estrai possesso palla
        poss_element = soup.select_one(".stats_table [data-stat='possession']")
        if poss_element:
            stats['possession'] = float(poss_element.text.strip().replace('%', ''))
        
        # Estrai tiri per partita
        shots_element = soup.select_one(".stats_table [data-stat='shots_per90']")
        if shots_element:
            stats['shots_per_game'] = float(shots_element.text.strip())
        
        # Estrai precisione passaggi
        pass_element = soup.select_one(".stats_table [data-stat='pass_pct']")
        if pass_element:
            stats['pass_accuracy'] = float(pass_element.text.strip().replace('%', ''))
        
        # Estrai contrasti
        tackle_element = soup.select_one(".stats_table [data-stat='tackles']")
        if tackle_element:
            stats['tackles_per_game'] = float(tackle_element.text.strip())
        
        if stats:
            return stats
        return None
    except Exception as e:
        logger.error(f"Errore scraping FBref: {str(e)}")
        return None

def get_team_stats_from_understat(team_name):
    """Ottiene statistiche squadra da Understat"""
    try:
        normalized_name = team_name.lower().replace(' ', '-')
        url = f"https://understat.com/team/{normalized_name}"
        
        response = make_request(url)
        if not response:
            return None
            
        soup = BeautifulSoup(response.text, 'html.parser')
        script_data = soup.find("script", text=re.compile("playersData"))
        
        if not script_data:
            return None
            
        json_str = re.search(r'var playersData = JSON.parse\(\'(.*)\'\);', script_data.string).group(1)
        json_str = json_str.encode('utf8').decode('unicode_escape')
        data = json.loads(json_str)
        
        if not data:
            return None
            
        # Calcola medie squadra
        total_matches = len(data)
        total_xg = sum(float(player['xG']) for player in data)
        total_xa = sum(float(player['xA']) for player in data)
        total_shots = sum(float(player['shots']) for player in data)
        
        return {
            'xG': round(total_xg / total_matches, 2),
            'xA': round(total_xa / total_matches, 2),
            'shots_per_game': round(total_shots / total_matches, 1)
        }
    except Exception as e:
        logger.error(f"Errore scraping Understat: {str(e)}")
        return None

def get_team_stats_from_sofascore(team_id):
    """Ottiene statistiche squadra da SofaScore API"""
    try:
        url = f"https://api.sofascore.com/api/v1/team/{team_id}/statistics"
        
        response = make_request(url)
        if not response or response.status_code != 200:
            return None
            
        data = response.json()
        if not data or 'statistics' not in data:
            return None
            
        stats = data['statistics']
        return {
            'possession': stats.get('possessionAvg'),
            'shots_per_game': stats.get('shotsAvg'),
            'pass_accuracy': stats.get('passAccuracyAvg'),
            'tackles_per_game': stats.get('tacklesAvg')
        }
    except Exception as e:
        logger.error(f"Errore API SofaScore: {str(e)}")
        return None

# Raccolta dati H2H
def get_h2h_stats(home_team_id, away_team_id, home_team_name, away_team_name):
    """Ottiene statistiche testa a testa"""
    source_name = get_best_available_source(["h2h"])
    
    if not source_name:
        logger.warning("Nessuna fonte disponibile per statistiche H2H")
        return None
    
    # Verifica cache
    cache_key = f"{source_name}_{home_team_id}_{away_team_id}"
    cached_data = get_from_cache(cache_key, "h2h")
    if cached_data:
        return cached_data
    
    logger.info(f"Recupero H2H per {home_team_name} vs {away_team_name} da {source_name}")
    
    if source_name == "flashscore":
        return get_h2h_from_flashscore(home_team_name, away_team_name)
    elif source_name == "sofascore":
        return get_h2h_from_sofascore(home_team_id, away_team_id)
    else:
        return None

def get_h2h_from_flashscore(home_team, away_team):
    """Ottiene statistiche H2H da FlashScore"""
    try:
        teams_slug = f"{home_team.lower().replace(' ', '-')}-vs-{away_team.lower().replace(' ', '-')}"
        url = f"https://www.flashscore.com/match/{teams_slug}/h2h/"
        
        response = make_request(url)
        if not response:
            return None
            
        soup = BeautifulSoup(response.text, 'html.parser')
        stats = {}
        
        # Estrai ultimi risultati
        results = []
        result_elements = soup.select(".h2h__row")
        for el in result_elements[:5]:  # Ultimi 5 incontri
            date = el.select_one(".h2h__date").text.strip()
            home_score = el.select_one(".h2h__homeScore").text.strip()
            away_score = el.select_one(".h2h__awayScore").text.strip()
            results.append({
                'date': date,
                'home_score': home_score,
                'away_score': away_score
            })
        
        if results:
            stats['last_results'] = results
        
        # Estrai statistiche
        stat_elements = soup.select(".h2h__statCircle")
        for el in stat_elements:
            stat_name = el.select_one(".h2h__statName").text.strip().lower()
            home_value = el.select_one(".h2h__statHomeValue").text.strip()
            away_value = el.select_one(".h2h__statAwayValue").text.strip()
            
            if 'corners' in stat_name:
                stats['avg_corners'] = (float(home_value) + float(away_value)) / 2
            elif 'cards' in stat_name:
                stats['avg_cards'] = (float(home_value) + float(away_value)) / 2
        
        return stats if stats else None
    except Exception as e:
        logger.error(f"Errore scraping FlashScore: {str(e)}")
        return None

def get_h2h_from_sofascore(home_team_id, away_team_id):
    """Ottiene statistiche H2H da SofaScore API"""
    try:
        url = f"https://api.sofascore.com/api/v1/team/{home_team_id}/events/previous/against/{away_team_id}"
        
        response = make_request(url)
        if not response or response.status_code != 200:
            return None
            
        data = response.json()
        if not data or 'events' not in data:
            return None
            
        events = data['events'][:5]  # Ultimi 5 incontri
        results = []
        
        for event in events:
            results.append({
                'date': datetime.fromtimestamp(event['startTimestamp']).strftime('%Y-%m-%d'),
                'home_score': event['homeScore']['current'],
                'away_score': event['awayScore']['current']
            })
        
        return {'last_results': results} if results else None
    except Exception as e:
        logger.error(f"Errore API SofaScore: {str(e)}")
        return None

# Gestione Firebase
def initialize_firebase():
    """Inizializza connessione a Firebase"""
    try:
        firebase_admin.get_app()
        logger.info("Firebase già inizializzato")
        return True
    except ValueError:
        pass
        
    try:
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
        logger.info("Firebase inizializzato con successo")
        return True
    except Exception as e:
        logger.error(f"Errore inizializzazione Firebase: {str(e)}")
        return False

def save_to_firebase(path, data):
    """Salva dati su Firebase"""
    try:
        ref = db.reference(path)
        ref.set(data)
        return True
    except Exception as e:
        logger.error(f"Errore salvataggio su Firebase: {str(e)}")
        return False

# Funzioni principali
def collect_matches():
    """Raccoglie le partite in programma"""
    try:
        source_name = get_best_available_source(["matches"])
        if not source_name:
            logger.error("Nessuna fonte disponibile per raccolta partite")
            return False
            
        today = datetime.now().date()
        start_date = today - timedelta(days=CONFIG["days_behind"])
        end_date = today + timedelta(days=CONFIG["days_ahead"])
        
        if source_name == "football_data":
            return get_matches_from_football_data(start_date, end_date)
        elif source_name == "sofascore":
            return get_matches_from_sofascore(start_date, end_date)
        else:
            logger.error(f"Fonte non supportata per raccolta partite: {source_name}")
            return False
    except Exception as e:
        logger.error(f"Errore durante raccolta partite: {str(e)}")
        return False

def get_matches_from_football_data(start_date, end_date):
    """Ottiene partite da Football-Data API"""
    try:
        url = f"{PRIMARY_SOURCES['football_data']['base_url']}/matches"
        params = {
            "dateFrom": start_date.strftime("%Y-%m-%d"),
            "dateTo": end_date.strftime("%Y-%m-%d")
        }
        
        response = make_request(url, params=params, headers={"X-Auth-Token": os.getenv('FOOTBALL_API_KEY')})
        if not response:
            return False
            
        data = response.json()
        matches = []
        
        for match in data.get('matches', []):
            if match['status'] != 'SCHEDULED':
                continue
                
            match_time = datetime.fromisoformat(match['utcDate'].replace('Z', '+00:00'))
            match_data = {
                'id': str(match['id']),
                'home_team': match['homeTeam']['name'],
                'home_team_id': str(match['homeTeam']['id']),
                'away_team': match['awayTeam']['name'],
                'away_team_id': str(match['awayTeam']['id']),
                'competition': match['competition']['name'],
                'utc_date': match['utcDate'],
                'status': 'SCHEDULED',
                'source': 'football_data',
                'last_updated': datetime.now().isoformat()
            }
            
            matches.append(match_data)
        
        if matches:
            return save_to_firebase('matches', {match['id']: match for match in matches})
        return False
    except Exception as e:
        logger.error(f"Errore Football-Data API: {str(e)}")
        return False

def get_matches_from_sofascore(start_date, end_date):
    """Ottiene partite da SofaScore API"""
    try:
        # Nota: API SofaScore potrebbe richiedere parametri aggiuntivi
        url = f"{PRIMARY_SOURCES['sofascore']['base_url']}/events"
        params = {
            "from": start_date.strftime("%Y-%m-%d"),
            "to": end_date.strftime("%Y-%m-%d")
        }
        
        response = make_request(url, params=params)
        if not response:
            return False
            
        data = response.json()
        matches = []
        
        for event in data.get('events', []):
            if event['status']['type'] != 'notstarted':
                continue
                
            match_data = {
                'id': f"sofascore_{event['id']}",
                'home_team': event['homeTeam']['name'],
                'home_team_id': str(event['homeTeam']['id']),
                'away_team': event['awayTeam']['name'],
                'away_team_id': str(event['awayTeam']['id']),
                'competition': event['tournament']['name'],
                'utc_date': datetime.fromtimestamp(event['startTimestamp']).isoformat(),
                'status': 'SCHEDULED',
                'source': 'sofascore',
                'last_updated': datetime.now().isoformat()
            }
            
            matches.append(match_data)
        
        if matches:
            return save_to_firebase('matches', {match['id']: match for match in matches})
        return False
    except Exception as e:
        logger.error(f"Errore SofaScore API: {str(e)}")
        return False

def update_team_stats():
    """Aggiorna le statistiche delle squadre"""
    try:
        # Ottieni squadre con partite in programma
        ref = db.reference('matches')
        matches = ref.get()
        
        if not matches:
            logger.info("Nessuna partita trovata per aggiornare statistiche squadre")
            return True
            
        team_ids = set()
        for match in matches.values():
            team_ids.add((match['home_team_id'], match['home_team']))
            team_ids.add((match['away_team_id'], match['away_team']))
        
        # Aggiorna statistiche per ogni squadra
        for team_id, team_name in team_ids:
            stats = get_team_stats(team_id, team_name)
            if stats:
                save_to_firebase(f'teams/{team_id}', stats)
                logger.info(f"Statistiche aggiornate per {team_name}")
            else:
                logger.warning(f"Impossibile ottenere statistiche per {team_name}")
        
        return True
    except Exception as e:
        logger.error(f"Errore durante aggiornamento statistiche squadre: {str(e)}")
        return False

def update_h2h_stats():
    """Aggiorna le statistiche testa a testa"""
    try:
        # Ottieni partite in programma
        ref = db.reference('matches')
        matches = ref.get()
        
        if not matches:
            logger.info("Nessuna partita trovata per aggiornare statistiche H2H")
            return True
            
        # Aggiorna H2H per ogni partita
        for match_id, match in matches.items():
            h2h_stats = get_h2h_stats(
                match['home_team_id'],
                match['away_team_id'],
                match['home_team'],
                match['away_team']
            )
            
            if h2h_stats:
                save_to_firebase(f'matches/{match_id}/h2h', h2h_stats)
                logger.info(f"Statistiche H2H aggiornate per {match['home_team']} vs {match['away_team']}")
            else:
                logger.warning(f"Impossibile ottenere statistiche H2H per {match['home_team']} vs {match['away_team']}")
        
        return True
    except Exception as e:
        logger.error(f"Errore durante aggiornamento statistiche H2H: {str(e)}")
        return False

def cleanup_old_data():
    """Pulisce i vecchi dati dal database"""
    try:
        ref = db.reference('matches')
        matches = ref.get()
        
        if not matches:
            return True
            
        now = datetime.now()
        deleted_count = 0
        
        for match_id, match in matches.items():
            match_date = datetime.fromisoformat(match['utc_date'].replace('Z', '+00:00'))
            if (now - match_date).days > CONFIG["match_expiry_days"]:
                ref.child(match_id).delete()
                deleted_count += 1
        
        if deleted_count > 0:
            logger.info(f"Rimosse {deleted_count} partite scadute")
        
        return True
    except Exception as e:
        logger.error(f"Errore durante pulizia dati vecchi: {str(e)}")
        return False

def update_health_status(status, details=None):
    """Aggiorna lo stato di salute del servizio"""
    try:
        health_data = {
            'last_checked': datetime.now().isoformat(),
            'status': status
        }
        
        if details:
            health_data.update(details)
            
        return save_to_firebase('health/data_collection', health_data)
    except Exception as e:
        logger.error(f"Errore durante aggiornamento stato salute: {str(e)}")
        return False

def main():
    """Funzione principale"""
    start_time = datetime.now()
    logger.info(f"Avvio sistema raccolta dati - {start_time.isoformat()}")
    
    try:
        # 1. Inizializza cache locale e Firebase
        if not init_local_cache():
            raise Exception("Impossibile inizializzare cache locale")
            
        if not initialize_firebase():
            raise Exception("Impossibile inizializzare Firebase")
        
        # 2. Raccolta partite
        logger.info("Inizio raccolta partite...")
        if not collect_matches():
            raise Exception("Fallita raccolta partite")
        
        # 3. Aggiornamento statistiche squadre
        logger.info("Inizio aggiornamento statistiche squadre...")
        if not update_team_stats():
            raise Exception("Fallito aggiornamento statistiche squadre")
        
        # 4. Aggiornamento statistiche H2H
        logger.info("Inizio aggiornamento statistiche H2H...")
        if not update_h2h_stats():
            raise Exception("Fallito aggiornamento statistiche H2H")
        
        # 5. Pulizia dati vecchi
        logger.info("Pulizia dati vecchi...")
        if not cleanup_old_data():
            raise Exception("Fallita pulizia dati vecchi")
        
        # 6. Aggiornamento stato salute
        update_health_status("success", {
            'execution_time': (datetime.now() - start_time).total_seconds(),
            'last_success': datetime.now().isoformat()
        })
        
        logger.info("Raccolta dati completata con successo")
        return 0
    except Exception as e:
        logger.error(f"Errore durante esecuzione: {str(e)}")
        update_health_status("error", {
            'error_message': str(e),
            'last_failure': datetime.now().isoformat()
        })
        return 1

if __name__ == "__main__":
    sys.exit(main())
