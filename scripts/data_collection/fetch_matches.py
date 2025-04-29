#!/usr/bin/env python3
"""
Fetch Matches - Script per raccogliere dati sulle partite di calcio
Raccoglie dati da API Football-Data e altre fonti, salvandoli su Firebase
Versione ottimizzata con cache locale, migliore gestione errori e parametri configurabili
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
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import firebase_admin
from firebase_admin import credentials, db
from dotenv import load_dotenv
import urllib.robotparser as robotparser
from urllib.parse import urlparse, quote
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pathlib import Path
from fuzzywuzzy import fuzz  # Aggiunto per il matching fuzzy dei nomi delle squadre

# Configurazione logging con rotazione file
log_dir = os.path.expanduser('~/football-predictions/logs')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"fetch_matches_{datetime.now().strftime('%Y%m%d')}.log")

# Setup logging con multiple destinazioni e livello configurabile
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger('fetch_matches')

# Configurazione del database locale per cache
DB_PATH = os.path.expanduser("~/football-predictions/cache/matches_cache.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# Parametri configurabili (leggibili da .env)
load_dotenv()
CONFIG = {
    "days_ahead": int(os.getenv('DAYS_AHEAD', '3')),
    "days_behind": int(os.getenv('DAYS_BEHIND', '0')),
    "publish_window_start": int(os.getenv('PUBLISH_WINDOW_START', '12')),
    "expire_time": int(os.getenv('EXPIRE_TIME', '8')),
    "api_timeout": int(os.getenv('API_TIMEOUT', '30')),
    "max_retries": int(os.getenv('MAX_RETRIES', '3')),
    "min_delay": float(os.getenv('MIN_DELAY', '1.0')),
    "max_delay": float(os.getenv('MAX_DELAY', '3.0')),
    "cache_expiry_hours": int(os.getenv('CACHE_EXPIRY_HOURS', '6')),
    "match_expiry_days": int(os.getenv('MATCH_EXPIRY_DAYS', '30')),
    "max_log_size_mb": int(os.getenv('MAX_LOG_SIZE_MB', '10')),
    # Limiti per scraping di ciascuna fonte
    "source_limits": {
        "sportinglife": int(os.getenv('SPORTINGLIFE_LIMIT', '100')),
        "bbc_sport": int(os.getenv('BBC_LIMIT', '100')),
        "goal_com": int(os.getenv('GOAL_LIMIT', '100')),
        "soccerway": int(os.getenv('SOCCERWAY_LIMIT', '100')),
        "openfootball": int(os.getenv('OPENFOOTBALL_LIMIT', '100')),
        "api_football": int(os.getenv('API_FOOTBALL_LIMIT', '100')),
        "flashscore": int(os.getenv('FLASHSCORE_LIMIT', '100')),
        "sofascore": int(os.getenv('SOFASCORE_LIMIT', '100')),
        "understat": int(os.getenv('UNDERSTAT_LIMIT', '50')),
        "fbref": int(os.getenv('FBREF_LIMIT', '100')),
        "worldfootball": int(os.getenv('WORLDFOOTBALL_LIMIT', '100')),
        "football_data_uk": int(os.getenv('FOOTBALL_DATA_UK_LIMIT', '100'))
    }
}

# API keys
API_KEY = os.getenv('FOOTBALL_API_KEY')
BASE_URL = "https://api.football-data.org/v4"

# Chiavi API aggiuntive (opzionali)
RAPID_API_KEY = os.getenv('RAPID_API_KEY', '')  # Per API-Football

# Lista di User Agents
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPad; CPU OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36 Edg/92.0.902.55",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36 OPR/78.0.4093.112",
    "Mozilla/5.0 (Windows NT 10.0; rv:91.0) Gecko/20100101 Firefox/91.0",
    "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "Mozilla/5.0 (Linux; Android 10; SM-G981B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.162 Mobile Safari/537.36"
]

# Campionati da monitorare per API Football-Data
LEAGUES = {
    # Top 5 europei
    'PL': {'name': 'Premier League', 'country': 'England'},
    'SA': {'name': 'Serie A', 'country': 'Italy'},
    'PD': {'name': 'LaLiga', 'country': 'Spain'},
    'BL1': {'name': 'Bundesliga', 'country': 'Germany'},
    'FL1': {'name': 'Ligue 1', 'country': 'France'},
    
    # Seconde divisioni
    'SB': {'name': 'Serie B', 'country': 'Italy'},
    'SD': {'name': 'LaLiga 2', 'country': 'Spain'},
    'ELC': {'name': 'Championship', 'country': 'England'},
    'BL2': {'name': 'Bundesliga 2', 'country': 'Germany'},
    'FL2': {'name': 'Ligue 2', 'country': 'France'},
    
    # Altri campionati europei
    'DED': {'name': 'Eredivisie', 'country': 'Netherlands'},
    'PPL': {'name': 'Primeira Liga', 'country': 'Portugal'},
    'ELN': {'name': 'Eliteserien', 'country': 'Norway'},
    'EPL': {'name': 'Ekstraklasa', 'country': 'Poland'},
    'SE': {'name': 'Superettan', 'country': 'Sweden'},
    'TL': {'name': 'Super Lig', 'country': 'Turkey'},
    
    # Competizioni UEFA
    'CL': {'name': 'UEFA Champions League', 'country': 'Europe'},
    'EL': {'name': 'UEFA Europa League', 'country': 'Europe'},
    'ECL': {'name': 'UEFA Conference League', 'country': 'Europe'},
    
    # Qualificazioni mondiali
    'WCQ_EU': {'name': 'World Cup Qualifiers UEFA', 'country': 'Europe'},
    'WCQ_AF': {'name': 'World Cup Qualifiers CAF', 'country': 'Africa'},
    'WCQ_AS': {'name': 'World Cup Qualifiers AFC', 'country': 'Asia'},
    'WCQ_NA': {'name': 'World Cup Qualifiers CONCACAF', 'country': 'North America'},
    'WCQ_SA': {'name': 'World Cup Qualifiers CONMEBOL', 'country': 'South America'},
    
    # Competizioni globali
    'WC': {'name': 'FIFA World Cup', 'country': 'World'},
    
    # Altri campionati internazionali
    'SPL': {'name': 'Saudi Pro League', 'country': 'Saudi Arabia'},
    'BSA': {'name': 'Brasileirão', 'country': 'Brazil'},
    'APL': {'name': 'Liga Profesional Argentina', 'country': 'Argentina'},
    'JL': {'name': 'J1 League', 'country': 'Japan'},
    'MLS': {'name': 'MLS', 'country': 'United States'}
}

# Fonti di dati supplementari per scraping
ADDITIONAL_SOURCES = {
    "sportinglife": {
        "name": "Sporting Life",
        "base_url": "https://www.sportinglife.com/football/fixtures-results",
        "fixtures_url": "https://www.sportinglife.com/football/fixtures-results/",
        "selector": ".fixtures-page__item",
        "date_format": "%A %d %B %Y",
        "country": "UK"
    },
    "bbc_sport": {
        "name": "BBC Sport",
        "base_url": "https://www.bbc.com/sport/football/scores-fixtures",
        "fixtures_url": "https://www.bbc.com/sport/football/scores-fixtures/",
        "selector": ".gs-o-list-ui__item--full.sp-c-fixture",
        "date_format": "%Y-%m-%d",
        "country": "UK",
        "lineup_selector": ".sp-c-fixture__lineups"
    },
    "goal_com": {
        "name": "Goal.com",
        "base_url": "https://www.goal.com/en/fixtures",
        "fixtures_url": "https://www.goal.com/en/fixtures/",
        "selector": ".competition-matches",
        "date_format": "%Y-%m-%d",
        "country": "International"
    },
    "sofascore": {
        "name": "SofaScore",
        "base_url": "https://www.sofascore.com/football",
        "fixtures_url": "https://www.sofascore.com/football//",
        "selector": ".event-list.js-event-list-tournament",
        "date_format": "%Y-%m-%d",
        "country": "International",
        "api_url": "https://api.sofascore.com/api/v1"
    },
    "flashscore": {
        "name": "FlashScore",
        "base_url": "https://www.flashscore.com",
        "fixtures_url": "https://www.flashscore.com/football/",
        "selector": ".sportName.soccer",
        "date_format": "%Y-%m-%d",
        "country": "International",
        "score_selector": ".matchHeader__score",
        "stat_home_selector": ".stat__homeValue",
        "stat_away_selector": ".stat__awayValue"
    },
    "soccerway": {
        "name": "Soccerway",
        "base_url": "https://us.soccerway.com",
        "fixtures_url": "https://us.soccerway.com/matches/",
        "selector": ".matches",
        "date_format": "%d %B %Y",
        "country": "International"
    },
    "fbref": {
        "name": "FBref",
        "base_url": "https://fbref.com",
        "fixtures_url": "https://fbref.com/en/comps/",
        "selector": "#div_sched",
        "date_format": "%Y-%m-%d",
        "country": "International",
        "team_stats_selector": "#team_stats tbody tr"
    },
    "understat": {
        "name": "Understat",
        "base_url": "https://understat.com",
        "fixtures_url": "https://understat.com/league/",
        "selector": ".calendar-date",
        "date_format": "%Y-%m-%d",
        "country": "International",
        "match_url_pattern": "https://understat.com/match/{match_id}"
    },
    "worldfootball": {
        "name": "WorldFootball",
        "base_url": "https://www.worldfootball.net",
        "fixtures_url": "https://www.worldfootball.net/all_matches/",
        "selector": ".standard_tabelle",
        "date_format": "%d/%m/%Y",
        "country": "International",
        "h2h_url_pattern": "https://www.worldfootball.net/teams/{team1}-vs-{team2}/9/"
    },
    "football_data_uk": {
        "name": "Football-Data UK",
        "base_url": "https://www.football-data.co.uk",
        "fixtures_url": "https://www.football-data.co.uk/mmz4281/",
        "year_pattern": "{year1}{year2}",
        "league_patterns": {
            "England": "E0",
            "Italy": "I1",
            "Spain": "SP1",
            "Germany": "D1",
            "France": "F1"
        },
        "date_format": "%d/%m/%y"
    }
}

# API aggiuntive gratuite
ADDITIONAL_APIS = {
    "api_football": {
        "name": "API-Football",
        "base_url": "https://api-football-v1.p.rapidapi.com/v3",
        "endpoints": {
            "fixtures": "/fixtures",
            "leagues": "/leagues"
        },
        "headers": {
            "x-rapidapi-key": RAPID_API_KEY,
            "x-rapidapi-host": "api-football-v1.p.rapidapi.com"
        },
        "requires_key": True
    },
    "api_football_demo": {
        "name": "API-Football Demo",
        "base_url": "https://www.api-football.com/demo",
        "requires_key": False
    },
    "football_api_org_demo": {
        "name": "Football-API.org Demo",
        "base_url": "https://football-api.com/demo", 
        "requires_key": False
    },
    "sofascore_api": {
        "name": "SofaScore API",
        "base_url": "https://api.sofascore.com/api/v1",
        "endpoints": {
            "events": "/sport/football/scheduled-events/{date}",
            "event": "/event/{id}",
            "team": "/team/{id}",
            "top_players": "/team/{id}/top-players"
        },
        "requires_key": False
    },
    "openfootball": {
        "name": "Open Football Data",
        "base_url": "https://raw.githubusercontent.com/openfootball/football.json/master",
        "endpoints": {
            "epl": "/2023-24/en.1.json",
            "bundesliga": "/2023-24/de.1.json",
            "seriea": "/2023-24/it.1.json",
            "laliga": "/2023-24/es.1.json",
            "ligue1": "/2023-24/fr.1.json"
        },
        "requires_key": False
    }
}

# Proxy gratuiti (rotazione per evitare blocchi)
FREE_PROXIES = [
    # Lista configurabile, può essere aggiornata nel tempo
    {"http": "http://185.199.229.156:7492", "https": "http://185.199.229.156:7492"},
    {"http": "http://185.199.228.220:7300", "https": "http://185.199.228.220:7300"}
]

def init_local_cache():
    """Inizializza il database SQLite per la cache locale"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Tabella per cache partite
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS matches_cache (
            match_id TEXT PRIMARY KEY,
            data TEXT,
            source TEXT,
            timestamp INTEGER
        )
        ''')
        
        # Tabella per statistiche di esecuzione
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS execution_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER,
            total_matches INTEGER,
            unique_matches INTEGER,
            saved_matches INTEGER,
            updated_matches INTEGER,
            sources TEXT,
            duration_seconds REAL
        )
        ''')
        
        # Tabella per team mappings - NUOVA
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS team_mappings (
            team_id TEXT PRIMARY KEY,
            team_name TEXT,
            normalized_name TEXT,
            aliases TEXT,
            last_updated INTEGER
        )
        ''')
        
        # Tabella per source status - NUOVA
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS source_status (
            source_name TEXT PRIMARY KEY,
            status TEXT,
            last_checked INTEGER,
            last_success INTEGER,
            avg_response_time REAL,
            success_count INTEGER,
            failure_count INTEGER
        )
        ''')
        
        conn.commit()
        conn.close()
        logger.info(f"Cache locale inizializzata: {DB_PATH}")
        return True
    except Exception as e:
        logger.error(f"Errore inizializzazione cache: {str(e)}")
        return False

def get_from_cache(match_id):
    """Recupera una partita dalla cache locale"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Cerca la partita per ID
        cursor.execute("SELECT data, timestamp FROM matches_cache WHERE match_id = ?", (match_id,))
        result = cursor.fetchone()
        conn.close()
        
        if not result:
            return None
            
        # Verifica se il dato è ancora valido
        data, timestamp = result
        cache_time = datetime.fromtimestamp(timestamp)
        now = datetime.now()
        
        # Se più vecchio di CONFIG["cache_expiry_hours"], considera non valido
        if (now - cache_time).total_seconds() > CONFIG["cache_expiry_hours"] * 3600:
            logger.debug(f"Cache scaduta per {match_id}")
            return None
            
        return json.loads(data)
    except Exception as e:
        logger.warning(f"Errore lettura cache: {str(e)}")
        return None

def save_to_cache(match_id, data, source):
    """Salva una partita nella cache locale"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Codifica in JSON e salva
        json_data = json.dumps(data)
        timestamp = int(time.time())
        
        cursor.execute(
            "INSERT OR REPLACE INTO matches_cache VALUES (?, ?, ?, ?)",
            (match_id, json_data, source, timestamp)
        )
        
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        logger.warning(f"Errore salvataggio cache: {str(e)}")
        return False

def save_execution_stats(stats):
    """Salva statistiche dell'esecuzione corrente"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute(
            "INSERT INTO execution_stats (timestamp, total_matches, unique_matches, saved_matches, updated_matches, sources, duration_seconds) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (int(time.time()), stats['total_matches'], stats['unique_matches'], stats['saved_matches'], 
             stats['updated_matches'], json.dumps(stats['sources']), stats['duration_seconds'])
        )
        
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        logger.warning(f"Errore salvataggio statistiche: {str(e)}")
        return False

def clean_old_cache_entries():
    """Rimuove vecchie voci dalla cache locale"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Calcola timestamp di scadenza (X giorni fa)
        expiry_timestamp = int(time.time()) - (CONFIG["match_expiry_days"] * 24 * 3600)
        
        # Rimuovi voci vecchie
        cursor.execute("DELETE FROM matches_cache WHERE timestamp < ?", (expiry_timestamp,))
        deleted_count = cursor.rowcount
        
        # Mantieni solo ultime 50 statistiche di esecuzione
        cursor.execute("DELETE FROM execution_stats WHERE id NOT IN (SELECT id FROM execution_stats ORDER BY timestamp DESC LIMIT 50)")
        
        conn.commit()
        conn.close()
        
        if deleted_count > 0:
            logger.info(f"Rimosse {deleted_count} voci scadute dalla cache")
        
        return deleted_count
    except Exception as e:
        logger.warning(f"Errore pulizia cache: {str(e)}")
        return 0

def update_source_status(source_name, status, request_time=None):
    """Aggiorna lo stato di una fonte di dati"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        current_time = int(time.time())
        
        # Leggi i dati esistenti per la fonte
        cursor.execute("SELECT success_count, failure_count, avg_response_time FROM source_status WHERE source_name = ?", (source_name,))
        result = cursor.fetchone()
        
        if result:
            success_count, failure_count, avg_response_time = result
            
            if status == 'success':
                success_count += 1
                # Aggiorna tempo medio di risposta
                if request_time and avg_response_time:
                    avg_response_time = (avg_response_time * (success_count - 1) + request_time) / success_count
                elif request_time:
                    avg_response_time = request_time
            else:
                failure_count += 1
        else:
            # Prima volta che incontriamo questa fonte
            if status == 'success':
                success_count, failure_count = 1, 0
                avg_response_time = request_time if request_time else None
            else:
                success_count, failure_count = 0, 1
                avg_response_time = None
        
        # Aggiorna i dati
        last_success = current_time if status == 'success' else None
        
        cursor.execute(
            "INSERT OR REPLACE INTO source_status VALUES (?, ?, ?, ?, ?, ?, ?)",
            (source_name, status, current_time, last_success, avg_response_time, success_count, failure_count)
        )
        
        conn.commit()
        conn.close()
        
        # Aggiorna anche su Firebase
        try:
            health_ref = db.reference(f'health/sources/{source_name}')
            health_data = {
                'last_checked': datetime.fromtimestamp(current_time).isoformat(),
                'status': status,
                'success_count': success_count,
                'failure_count': failure_count
            }
            
            if status == 'success':
                health_data['last_success'] = datetime.fromtimestamp(current_time).isoformat()
                if request_time:
                    health_data['avg_response_time'] = avg_response_time
                    
            health_ref.update(health_data)
        except Exception as e:
            logger.warning(f"Errore aggiornamento status su Firebase: {str(e)}")
        
        return True
    except Exception as e:
        logger.warning(f"Errore aggiornamento status fonte: {str(e)}")
        return False

def check_source_status(source_name):
    """Verifica lo stato di una fonte di dati"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute("SELECT status, last_checked, last_success FROM source_status WHERE source_name = ?", (source_name,))
        result = cursor.fetchone()
        conn.close()
        
        if not result:
            return "unknown"
            
        status, last_checked, last_success = result
        current_time = int(time.time())
        
        # Se verificato nelle ultime 6 ore, usa lo stato memorizzato
        if last_checked and current_time - last_checked < 6 * 3600:
            return status
            
        # Se ultimo successo nelle ultime 12 ore, considera disponibile
        if last_success and current_time - last_success < 12 * 3600:
            return "available"
            
        # Altrimenti, stato sconosciuto
        return "unknown"
    except Exception as e:
        logger.warning(f"Errore verifica status fonte: {str(e)}")
        return "unknown"

def choose_best_data_source(data_type, team_id=None, date_range=None):
    """Seleziona la fonte migliore per un certo tipo di dati"""
    sources = []
    
    if data_type == 'matches':
        sources = [
            {'name': 'football-data-api', 'priority': 1, 'status': check_source_status('football-data-api')},
            {'name': 'api-football', 'priority': 2, 'status': check_source_status('api-football')},
            {'name': 'openfootball', 'priority': 3, 'status': check_source_status('openfootball')},
            {'name': 'sofascore_api', 'priority': 4, 'status': check_source_status('sofascore_api')},
            {'name': 'sportinglife', 'priority': 5, 'status': check_source_status('sportinglife')},
            {'name': 'bbc_sport', 'priority': 6, 'status': check_source_status('bbc_sport')},
            {'name': 'flashscore', 'priority': 7, 'status': check_source_status('flashscore')},
            {'name': 'fbref', 'priority': 8, 'status': check_source_status('fbref')}
        ]
    elif data_type == 'team_stats':
        sources = [
            {'name': 'football-data-api', 'priority': 1, 'status': check_source_status('football-data-api')},
            {'name': 'fbref', 'priority': 2, 'status': check_source_status('fbref')},
            {'name': 'understat', 'priority': 3, 'status': check_source_status('understat')},
            {'name': 'sofascore_api', 'priority': 4, 'status': check_source_status('sofascore_api')}
        ]
    elif data_type == 'h2h_stats':
        sources = [
            {'name': 'football-data-api', 'priority': 1, 'status': check_source_status('football-data-api')},
            {'name': 'sofascore_api', 'priority': 2, 'status': check_source_status('sofascore_api')},
            {'name': 'flashscore', 'priority': 3, 'status': check_source_status('flashscore')},
            {'name': 'worldfootball', 'priority': 4, 'status': check_source_status('worldfootball')}
        ]
    
    # Ordina per priorità e disponibilità
    sources.sort(key=lambda x: (0 if x['status'] == 'available' else 1, x['priority']))
    
    # Restituisce la migliore fonte disponibile
    for source in sources:
        if source['status'] == 'available':
            return source['name']
    
    # Se nessuna fonte è disponibile, ritorna quella con priorità più alta
    return sources[0]['name'] if sources else None

def initialize_firebase():
    """Inizializza connessione Firebase con retry"""
    for attempt in range(CONFIG["max_retries"]):
        try:
            # Verifica se l'app è già inizializzata
            try:
                firebase_admin.get_app()
                logger.info("Firebase già inizializzato")
                return True
            except ValueError:
                # App non inizializzata, procedo
                pass
                
            # Usa credenziali da variabile d'ambiente o file
            cred_json = os.getenv('FIREBASE_CREDENTIALS')
            if cred_json:
                try:
                    cred_dict = json.loads(cred_json)
                    cred = credentials.Certificate(cred_dict)
                except json.JSONDecodeError:
                    # Prova a interpretarlo come path
                    cred = credentials.Certificate(cred_json)
            else:
                # Fallback al percorso file predefinito
                cred_path = os.path.expanduser('~/football-predictions/creds/firebase-credentials.json')
                cred = credentials.Certificate(cred_path)
                
            firebase_admin.initialize_app(cred, {
                'databaseURL': os.getenv('FIREBASE_DB_URL')
            })
            
            logger.info("Firebase inizializzato con successo")
            return True
            
        except Exception as e:
            logger.error(f"Errore nell'inizializzazione di Firebase (tentativo {attempt+1}/{CONFIG['max_retries']}): {str(e)}")
            if attempt < CONFIG["max_retries"] - 1:
                time.sleep(2 ** attempt)  # Backoff esponenziale
            else:
                return False

def get_random_user_agent():
    """Restituisce uno User-Agent casuale per evitare blocchi"""
    return random.choice(USER_AGENTS)

def get_random_proxy():
    """Restituisce un proxy casuale dalla lista di proxy gratuiti"""
    if not FREE_PROXIES:
        return None
    return random.choice(FREE_PROXIES)

def is_scraping_allowed(url):
    """Verifica se lo scraping è permesso per l'URL dato"""
    try:
        parsed_uri = urlparse(url)
        base = f"{parsed_uri.scheme}://{parsed_uri.netloc}"
        robots_url = f"{base}/robots.txt"
        
        rp = robotparser.RobotFileParser()
        rp.set_url(robots_url)
        rp.read()
        
        return rp.can_fetch("*", url)
    except Exception as e:
        logger.warning(f"Errore nella verifica robots.txt per {url}: {str(e)}")
        # In caso di errore, assume che sia permesso ma con cautela
        return True

def get_session_with_retries(use_proxy=False):
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
    
    # Imposta User-Agent casuale
    session.headers.update({"User-Agent": get_random_user_agent()})
    
    # Aggiungi proxy se richiesto
    if use_proxy:
        proxy = get_random_proxy()
        if proxy:
            session.proxies.update(proxy)
    
    return session

def make_api_request(endpoint, params=None):
    """Effettua richiesta all'API con gestione errori e retry"""
    url = f"{BASE_URL}/{endpoint}"
    headers = {
        "X-Auth-Token": API_KEY,
        "User-Agent": get_random_user_agent()
    }
    
    session = get_session_with_retries()
    start_time = time.time()
    
    for attempt in range(CONFIG["max_retries"]):
        try:
            logger.info(f"Richiesta API: {url}")
            response = session.get(url, headers=headers, params=params, timeout=CONFIG["api_timeout"])
            
            # Rispetta i limiti dell'API
            time.sleep(1.5)
            
            if response.status_code == 200:
                end_time = time.time()
                duration = end_time - start_time
                update_source_status('football-data-api', 'success', duration)
                return response.json()
            elif response.status_code == 429:  # Too Many Requests
                logger.warning(f"Rate limit raggiunto. Attesa più lunga.")
                time.sleep(60)  # Attendi 60 secondi
                continue
            elif response.status_code == 404:
                logger.warning(f"Risorsa non trovata: {url}")
                update_source_status('football-data-api', 'error')
                return None
            else:
                logger.error(f"Errore API {response.status_code}: {response.text}")
                if attempt < CONFIG["max_retries"] - 1:
                    continue
                update_source_status('football-data-api', 'error')
                return None
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout per richiesta {url}")
            if attempt < CONFIG["max_retries"] - 1:
                sleep_time = 2 ** attempt  # Backoff esponenziale
                logger.info(f"Ritento fra {sleep_time} secondi...")
                time.sleep(sleep_time)
            else:
                update_source_status('football-data-api', 'error')
                return None
        except Exception as e:
            logger.error(f"Errore richiesta: {str(e)}")
            if attempt < CONFIG["max_retries"] - 1:
                sleep_time = 2 ** attempt  # Backoff esponenziale
                logger.info(f"Ritento fra {sleep_time} secondi...")
                time.sleep(sleep_time)
            else:
                update_source_status('football-data-api', 'error')
                return None
    
    update_source_status('football-data-api', 'error')
    return None

def make_request_with_retry(url, max_retries=None, method="get", source_name=None, use_proxy=False, **kwargs):
    """Esegue richiesta HTTP con gestione errori e retry"""
    if max_retries is None:
        max_retries = CONFIG["max_retries"]
        
    if 'headers' not in kwargs:
        kwargs['headers'] = {'User-Agent': get_random_user_agent()}
    
    # Timeout di default se non specificato
    if 'timeout' not in kwargs:
        kwargs['timeout'] = CONFIG["api_timeout"]
    
    # Verifica se lo scraping è permesso
    if not is_scraping_allowed(url):
        logger.warning(f"Scraping non permesso per {url}")
        if source_name:
            update_source_status(source_name, 'error')
        return None
    
    session = get_session_with_retries(use_proxy)
    start_time = time.time()
    
    for attempt in range(max_retries):
        try:
            # Aggiunge un ritardo casuale per scraping etico
            delay = random.uniform(CONFIG["min_delay"], CONFIG["max_delay"])
            time.sleep(delay)
            
            if method.lower() == "get":
                response = session.get(url, **kwargs)
            elif method.lower() == "post":
                response = session.post(url, **kwargs)
            else:
                raise ValueError(f"Metodo non supportato: {method}")
            
            if response.status_code == 200:
                end_time = time.time()
                duration = end_time - start_time
                if source_name:
                    update_source_status(source_name, 'success', duration)
                return response
            elif response.status_code == 429:  # Too Many Requests
                logger.warning(f"Rate limit raggiunto per {url}. Attesa lunga.")
                time.sleep(60)  # Attesa più lunga
                continue
            else:
                logger.error(f"Errore richiesta {url}: {response.status_code}")
                if attempt < max_retries - 1:
                    sleep_time = 2 ** attempt  # Backoff esponenziale
                    time.sleep(sleep_time)
                    continue
                if source_name:
                    update_source_status(source_name, 'error')
                return None
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout per richiesta {url}")
            if attempt < max_retries - 1:
                sleep_time = 2 ** attempt  # Backoff esponenziale
                time.sleep(sleep_time)
                continue
            if source_name:
                update_source_status(source_name, 'error')
            return None
        except Exception as e:
            logger.error(f"Errore connessione a {url}: {str(e)}")
            if attempt < max_retries - 1:
                sleep_time = 2 ** attempt  # Backoff esponenziale
                time.sleep(sleep_time)
                continue
            if source_name:
                update_source_status(source_name, 'error')
            return None
    
    if source_name:
        update_source_status(source_name, 'error')
    return None

def normalize_team_name(name):
    """Normalizza il nome della squadra per il confronto"""
    if not name:
        return ""
    # Converti in minuscolo, rimuovi caratteri speciali
    normalized = re.sub(r'[^a-z0-9]', '', name.lower())
    # Gestisci abbreviazioni comuni
    normalized = normalized.replace('united', 'utd')
    normalized = normalized.replace('manchester', 'man')
    normalized = normalized.replace('tottenham', 'spurs')
    return normalized

def get_team_aliases(name):
    """Ottiene possibili alias per un nome di squadra"""
    aliases = []
    
    # Gestisci variazioni comuni
    if "United" in name:
        aliases.append(name.replace("United", "Utd"))
    if "Manchester United" in name:
        aliases.append("Man Utd")
        aliases.append("Man United")
        aliases.append("Manchester Utd")
        aliases.append("MUFC")
    if "Manchester City" in name:
        aliases.append("Man City")
        aliases.append("MCFC")
    if "Tottenham Hotspur" in name:
        aliases.append("Tottenham")
        aliases.append("Spurs")
    if "Arsenal" in name:
        aliases.append("AFC")
    if "Liverpool" in name:
        aliases.append("LFC")
    if "Chelsea" in name:
        aliases.append("CFC")
    if "Juventus" in name:
        aliases.append("Juve")
    if "Milan" == name:
        aliases.append("AC Milan")
    if "Inter" in name:
        aliases.append("Inter Milan")
        aliases.append("FC Internazionale")
    if "Barcelona" in name:
        aliases.append("Barca")
        aliases.append("FC Barcelona")
    if "Real Madrid" in name:
        aliases.append("Real")
    if "Bayern" in name:
        aliases.append("Bayern München")
        aliases.append("Bayern Munich")
        aliases.append("FC Bayern")
    if "Paris Saint" in name or "PSG" in name:
        aliases.append("Paris Saint-Germain")
        aliases.append("Paris SG")
        aliases.append("PSG")
    
    return aliases

def create_team_mapping_database():
    """Crea un database di mappatura per nomi squadre tra diverse fonti"""
    team_mappings = {}
    
    # Football-Data API teams
    teams_response = make_api_request("teams")
    if teams_response and 'teams' in teams_response:
        for team in teams_response['teams']:
            normalized_name = normalize_team_name(team['name'])
            team_mappings[normalized_name] = {
                'football_data_id': team['id'],
                'name': team['name'],
                'aliases': get_team_aliases(team['name'])
            }
    
    # Salva nel database locale
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        timestamp = int(time.time())
        
        for norm_name, team_data in team_mappings.items():
            team_id = str(team_data['football_data_id'])
            team_name = team_data['name']
            aliases = json.dumps(team_data['aliases'])
            
            cursor.execute(
                "INSERT OR REPLACE INTO team_mappings VALUES (?, ?, ?, ?, ?)",
                (team_id, team_name, norm_name, aliases, timestamp)
            )
        
        conn.commit()
        conn.close()
        logger.info(f"Database mappatura squadre creato con {len(team_mappings)} squadre")
    except Exception as e:
        logger.error(f"Errore nella creazione database mappatura: {str(e)}")
    
    # Salva anche su Firebase per riuso
    try:
        db.reference('team_mappings').set(team_mappings)
        logger.info("Mappatura squadre salvata su Firebase")
    except Exception as e:
        logger.error(f"Errore salvataggio mappatura su Firebase: {str(e)}")
    
    return team_mappings

def match_team_name(name, mappings=None):
    """Trova l'ID della squadra da qualsiasi variante del nome"""
    if not name:
        return None
        
    # Carica mappings se non fornito
    if not mappings:
        try:
            # Prima prova dal database locale
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            cursor.execute("SELECT team_id, team_name, normalized_name, aliases FROM team_mappings")
            results = cursor.fetchall()
            conn.close()
            
            if results:
                mappings = {}
                for team_id, team_name, normalized_name, aliases in results:
                    mappings[normalized_name] = {
                        'football_data_id': team_id,
                        'name': team_name,
                        'aliases': json.loads(aliases)
                    }
            else:
                # Se non ci sono dati locali, prova da Firebase
                mappings_fb = db.reference('team_mappings').get()
                if mappings_fb:
                    mappings = mappings_fb
                else:
                    # Se ancora niente, crea il database
                    mappings = create_team_mapping_database()
        except Exception as e:
            logger.error(f"Errore caricamento mappatura: {str(e)}")
            return None
    
    # Normalizza il nome da cercare
    normalized = normalize_team_name(name)
    
    # Match diretto
    if normalized in mappings:
        return mappings[normalized]
    
    # Controlla gli alias
    for team_key, team_data in mappings.items():
        if 'aliases' in team_data:
            normalized_aliases = [normalize_team_name(a) for a in team_data['aliases']]
            if normalized in normalized_aliases:
                return team_data
    
    # Match fuzzy se ancora non trovato
    best_match = None
    best_score = 0
    
    for team_key, team_data in mappings.items():
        score = fuzz.ratio(normalized, team_key)
        if score > 85 and score > best_score:  # Richiede alta confidenza
            best_match = team_data
            best_score = score
    
    return best_match

def get_matches_for_date_range(start_date, end_date):
    """Ottiene le partite in un intervallo di date dall'API Football-Data"""
    matches = []
    
    # Verifica se API key è configurata
    if not API_KEY:
        logger.warning("FOOTBALL_API_KEY non trovata nelle variabili d'ambiente")
        return matches
    
    # Converti date in stringhe formato ISO
    start_str = start_date.isoformat()
    end_str = end_date.isoformat()
    
    params = {
        "dateFrom": start_str,
        "dateTo": end_str
    }
    
    leagues_processed = 0
    
    # Raccoglie partite per ogni campionato
    for league_code, league_info in LEAGUES.items():
        # Verifica se abbiamo già dati in cache
        cache_key = f"football_data_api_{league_code}_{start_str}_{end_str}"
        cached_data = get_from_cache(cache_key)
        
        if cached_data:
            logger.info(f"Usando dati in cache per {league_info['name']}")
            matches.extend(cached_data)
            continue
        
        logger.info(f"Raccolta partite per {league_info['name']} ({start_str} - {end_str})")
        
        # Alcuni codici campionato potrebbero richiedere endpoint diversi
        if league_code.startswith('WCQ_'):
            # Le qualificazioni mondiali potrebbero richiedere approcci diversi
            # Per esempio, potremmo usare un endpoint generico e filtrare
            continue  # Saltato per ora - implementare logica specifica in futuro
        
        endpoint = f"competitions/{league_code}/matches"
        data = make_api_request(endpoint, params)
        
        if not data:
            logger.warning(f"Nessun dato per {league_info['name']}")
            continue
        
        league_matches = data.get('matches', [])
        logger.info(f"Trovate {len(league_matches)} partite in {league_info['name']}")
        
        league_processed_matches = []
        
        for match in league_matches:
            # Filtra solo le partite programmate
            if match['status'] == 'SCHEDULED':
                try:
                    match_time = datetime.fromisoformat(match['utcDate'].replace('Z', '+00:00'))
                    
                    # Calcola window di pubblicazione
                    publish_time = match_time - timedelta(hours=CONFIG["publish_window_start"])
                    expire_time = match_time + timedelta(hours=CONFIG["expire_time"])
                    
                    # Crea oggetto partita
                    match_data = {
                        'id': str(match['id']),
                        'home_team': match['homeTeam']['name'],
                        'home_team_id': match['homeTeam']['id'],
                        'away_team': match['awayTeam']['name'],
                        'away_team_id': match['awayTeam']['id'],
                        'competition': league_info['name'],
                        'competition_code': league_code,
                        'country': league_info['country'],
                        'utc_date': match['utcDate'],
                        'status': match['status'],
                        'matchday': match.get('matchday'),
                        'stage': match.get('stage'),
                        'group': match.get('group'),
                        'publish_time': publish_time.isoformat(),
                        'expire_time': expire_time.isoformat(),
                        'processed': False,
                        'article_generated': False,
                        'source': 'football-data-api',
                        'last_updated': datetime.now().isoformat()
                    }
                    
                    matches.append(match_data)
                    league_processed_matches.append(match_data)
                except Exception as e:
                    logger.error(f"Errore elaborazione partita: {str(e)}")
        
        # Salva nella cache locale
        if league_processed_matches:
            save_to_cache(cache_key, league_processed_matches, 'football-data-api')
        
        # Rispetta i limiti dell'API tra campionati
        leagues_processed += 1
        if leagues_processed % 5 == 0:  # Pausa ogni 5 campionati
            logger.info("Pausa per rispettare limiti API")
            time.sleep(5)
        else:
            time.sleep(1)
    
    return matches

def get_matches_from_api_football(start_date, end_date):
    """Ottiene partite da API-Football (RapidAPI)"""
    if not RAPID_API_KEY:
        logger.warning("API-Football: chiave API non configurata, saltando fonte")
        return []
    
    matches = []
    api_config = ADDITIONAL_APIS["api_football"]
    
    # Converti date nel formato richiesto (YYYY-MM-DD)
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    
    # Verifica cache
    cache_key = f"api_football_{start_str}_{end_str}"
    cached_data = get_from_cache(cache_key)
    
    if cached_data:
        logger.info(f"API-Football: Usando dati in cache per periodo {start_str}-{end_str}")
        return cached_data
    
    url = f"{api_config['base_url']}{api_config['endpoints']['fixtures']}"
    headers = api_config['headers']
    
    date_range = []
    current_date = start_date
    while current_date <= end_date:
        date_range.append(current_date.strftime("%Y-%m-%d"))
        current_date += timedelta(days=1)
    
    match_count = 0
    limit = CONFIG["source_limits"]["api_football"]
    
    for date_str in date_range:
        try:
            querystring = {"date": date_str}
            response = make_request_with_retry(url, headers=headers, params=querystring, source_name="api-football")
            
            if not response:
                logger.warning(f"API-Football: Nessuna risposta per {date_str}")
                continue
            
            data = response.json()
            
            if data.get('response'):
                fixtures = data['response']
                logger.info(f"API-Football: Trovate {len(fixtures)} partite per {date_str}")
                
                for fixture in fixtures:
                    if match_count >= limit:
                        logger.info(f"API-Football: Raggiunto limite di {limit} partite")
                        break
                        
                    match_time = datetime.fromisoformat(fixture['fixture']['date'])
                    
                    # Calcola finestra di pubblicazione
                    publish_time = match_time - timedelta(hours=CONFIG["publish_window_start"])
                    expire_time = match_time + timedelta(hours=CONFIG["expire_time"])
                    
                    match_data = {
                        'id': f"apif_{fixture['fixture']['id']}",
                        'home_team': fixture['teams']['home']['name'],
                        'home_team_id': f"apif_{fixture['teams']['home']['id']}",
                        'away_team': fixture['teams']['away']['name'],
                        'away_team_id': f"apif_{fixture['teams']['away']['id']}",
                        'competition': fixture['league']['name'],
                        'competition_code': f"apif_{fixture['league']['id']}",
                        'country': fixture['league']['country'],
                        'utc_date': fixture['fixture']['date'],
                        'status': 'SCHEDULED',
                        'publish_time': publish_time.isoformat(),
                        'expire_time': expire_time.isoformat(),
                        'processed': False,
                        'article_generated': False,
                        'source': 'api-football',
                        'last_updated': datetime.now().isoformat()
                    }
                    
                    matches.append(match_data)
                    match_count += 1
            
            # Rispetta i limiti dell'API
            time.sleep(2)
            
            if match_count >= limit:
                break
        
        except Exception as e:
            logger.error(f"Errore API-Football per {date_str}: {str(e)}")
    
    # Salva in cache
    if matches:
        save_to_cache(cache_key, matches, 'api-football')
    
    return matches

def get_matches_from_sofascore_api(start_date, end_date):
    """Ottiene partite dall'API SofaScore (gratuita)"""
    matches = []
    api_config = ADDITIONAL_APIS["sofascore_api"]
    
    # Verifica cache
    cache_key = f"sofascore_api_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
    cached_data = get_from_cache(cache_key)
    
    if cached_data:
        logger.info(f"SofaScore API: Usando dati in cache")
        return cached_data
    
    date_range = []
    current_date = start_date
    
    while current_date <= end_date:
        date_range.append(current_date.strftime("%Y-%m-%d"))
        current_date += timedelta(days=1)
    
    match_count = 0
    limit = CONFIG["source_limits"]["sofascore"]
    
    for date_str in date_range:
        # Crea URL con pattern corretto
        url = f"{api_config['base_url']}{api_config['endpoints']['events'].format(date=date_str)}"
        
        try:
            # L'API di SofaScore richiede un user-agent standard
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            response = make_request_with_retry(url, headers=headers, source_name="sofascore_api")
            
            if not response:
                logger.warning(f"SofaScore API: Nessuna risposta per {date_str}")
                continue
            
            data = response.json()
            
            # Estrai eventi di calcio
            if 'events' in data:
                events = data['events']
                logger.info(f"SofaScore API: Trovati {len(events)} eventi per {date_str}")
                
                for event in events:
                    if match_count >= limit:
                        break
                    
                    # Verifica che sia un evento di calcio
                    if event.get('tournament', {}).get('category', {}).get('name') == 'Football':
                        try:
                            event_id = event['id']
                            home_team = event['homeTeam']['name']
                            away_team = event['awayTeam']['name']
                            tournament = event['tournament']['name']
                            country = event['tournament'].get('category', {}).get('name', 'Unknown')
                            start_timestamp = event['startTimestamp']
                            
                            # Converti timestamp in datetime
                            match_time = datetime.fromtimestamp(start_timestamp)
                            
                            # Calcola finestra di pubblicazione
                            publish_time = match_time - timedelta(hours=CONFIG["publish_window_start"])
                            expire_time = match_time + timedelta(hours=CONFIG["expire_time"])
                            
                            match_data = {
                                'id': f"sofa_{event_id}",
                                'home_team': home_team,
                                'home_team_id': f"sofa_{event['homeTeam']['id']}",
                                'away_team': away_team,
                                'away_team_id': f"sofa_{event['awayTeam']['id']}",
                                'competition': tournament,
                                'competition_code': f"sofa_{event['tournament']['id']}",
                                'country': country,
                                'utc_date': match_time.isoformat(),
                                'status': 'SCHEDULED',
                                'publish_time': publish_time.isoformat(),
                                'expire_time': expire_time.isoformat(),
                                'processed': False,
                                'article_generated': False,
                                'source': 'sofascore_api',
                                'last_updated': datetime.now().isoformat()
                            }
                            
                            matches.append(match_data)
                            match_count += 1
                        except Exception as e:
                            logger.error(f"Errore elaborazione evento SofaScore: {str(e)}")
            
            # Rispetta i limiti dell'API
            time.sleep(2)
            
            if match_count >= limit:
                break
        
        except Exception as e:
            logger.error(f"Errore SofaScore API per {date_str}: {str(e)}")
    
    # Salva in cache
    if matches:
        save_to_cache(cache_key, matches, 'sofascore_api')
    
    return matches

def get_matches_from_openfootball():
    """Ottiene partite dai dataset OpenFootball (GitHub)"""
    matches = []
    api_config = ADDITIONAL_APIS["openfootball"]
    
    # Verifica cache
    cache_key = f"openfootball_data"
    cached_data = get_from_cache(cache_key)
    
    if cached_data:
        logger.info(f"OpenFootball: Usando dati in cache")
        return cached_data
    
    match_count = 0
    limit = CONFIG["source_limits"]["openfootball"]
    
    for league_code, endpoint in api_config['endpoints'].items():
        if match_count >= limit:
            logger.info(f"OpenFootball: Raggiunto limite di {limit} partite")
            break
            
        url = f"{api_config['base_url']}{endpoint}"
        
        try:
            response = make_request_with_retry(url, source_name="openfootball")
            if not response:
                logger.warning(f"OpenFootball: Nessuna risposta per {league_code}")
                continue
            
            data = response.json()
            
            # Mappa dei nomi dei campionati
            league_names = {
                'epl': 'Premier League',
                'bundesliga': 'Bundesliga',
                'seriea': 'Serie A',
                'laliga': 'La Liga',
                'ligue1': 'Ligue 1'
            }
            
            # Mappa dei paesi
            country_map = {
                'epl': 'England',
                'bundesliga': 'Germany',
                'seriea': 'Italy',
                'laliga': 'Spain',
                'ligue1': 'France'
            }
            
            league_name = league_names.get(league_code, league_code)
            country = country_map.get(league_code, 'Unknown')
            
            if 'matches' in data:
                match_fixtures = data['matches']
                for match in match_fixtures:
                    if match_count >= limit:
                        break
                        
                    # Estrai data della partita
                    match_date_str = match.get('date')
                    if not match_date_str:
                        continue
                    
                    try:
                        # Tenta di convertire la data
                        match_date = datetime.strptime(match_date_str, "%Y-%m-%d")
                        
                        # Verifica se è nel futuro o entro il range configurato
                        now = datetime.now()
                        if match_date < now.date() - timedelta(days=CONFIG["days_behind"]) or \
                           match_date > now.date() + timedelta(days=CONFIG["days_ahead"]):
                            continue
                        
                        # Aggiungi un'ora predefinita (ad es. 15:00)
                        match_time = datetime.combine(match_date, datetime.min.time().replace(hour=15))
                        
                        # Calcola finestra di pubblicazione
                        publish_time = match_time - timedelta(hours=CONFIG["publish_window_start"])
                        expire_time = match_time + timedelta(hours=CONFIG["expire_time"])
                        
                        # Genera un ID univoco per la partita
                        match_id = f"of_{league_code}_{match['round']}_{match['team1'].replace(' ', '')}_{match['team2'].replace(' ', '')}"
                        
                        match_data = {
                            'id': match_id,
                            'home_team': match['team1'],
                            'home_team_id': f"of_{match['team1'].replace(' ', '')}",
                            'away_team': match['team2'],
                            'away_team_id': f"of_{match['team2'].replace(' ', '')}",
                            'competition': league_name,
                            'competition_code': f"of_{league_code}",
                            'country': country,
                            'utc_date': match_time.isoformat(),
                            'status': 'SCHEDULED',
                            'matchday': match.get('round', '').replace('Matchday ', ''),
                            'publish_time': publish_time.isoformat(),
                            'expire_time': expire_time.isoformat(),
                            'processed': False,
                            'article_generated': False,
                            'source': 'openfootball',
                            'last_updated': datetime.now().isoformat()
                        }
                        
                        matches.append(match_data)
                        match_count += 1
                    except ValueError:
                        logger.error(f"Formato data non valido: {match_date_str}")
                        continue
        
        except Exception as e:
            logger.error(f"Errore OpenFootball per {league_code}: {str(e)}")
        
        # Rispetta i limiti di richieste
        time.sleep(1)
    
    # Salva in cache
    if matches:
        save_to_cache(cache_key, matches, 'openfootball')
    
    return matches

def get_matches_from_fbref(start_date, end_date):
    """Ottiene partite da FBref"""
    matches = []
    source_config = ADDITIONAL_SOURCES["fbref"]
    
    # Verifica cache
    cache_key = f"fbref_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
    cached_data = get_from_cache(cache_key)
    
    if cached_data:
        logger.info(f"FBref: Usando dati in cache")
        return cached_data
    
    # FBref ha un'organizzazione per competizioni
    competitions = {
        "9": {"name": "Premier League", "country": "England"},
        "11": {"name": "Serie A", "country": "Italy"},
        "12": {"name": "La Liga", "country": "Spain"},
        "13": {"name": "Ligue 1", "country": "France"},
        "20": {"name": "Bundesliga", "country": "Germany"},
        "8": {"name": "Champions League", "country": "Europe"}
    }
    
    match_count = 0
    limit = CONFIG["source_limits"]["fbref"]
    
    # Verifica esistenza directory cache
    fbref_cache_dir = os.path.expanduser('~/football-predictions/cache/fbref')
    os.makedirs(fbref_cache_dir, exist_ok=True)
    
    for comp_id, comp_info in competitions.items():
        if match_count >= limit:
            break
            
        url = f"{source_config['base_url']}/en/comps/{comp_id}/schedule"
        
        try:
            response = make_request_with_retry(url, source_name="fbref")
            if not response:
                logger.warning(f"FBref: Nessuna risposta per {comp_info['name']}")
                continue
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Trova la tabella con calendario partite
            schedule_table = soup.select_one("#div_sched")
            
            if not schedule_table:
                logger.warning(f"FBref: Nessuna tabella calendario trovata per {comp_info['name']}")
                continue
            
            # Estrai righe della tabella
            rows = schedule_table.select("tbody tr")
            logger.info(f"FBref: Trovate {len(rows)} righe per {comp_info['name']}")
            
            for row in rows:
                if match_count >= limit:
                    break
                
                # Verifica se è un'intestazione o divisore
                if 'spacer' in row.get('class', []) or 'thead' in row.get('class', []):
                    continue
                
                try:
                    # Estrai data
                    date_cell = row.select_one("[data-stat='date']")
                    if not date_cell:
                        continue
                    
                    date_str = date_cell.get_text(strip=True)
                    if not date_str or date_str == "Date":
                        continue
                    
                    # Converti data
                    try:
                        match_date = datetime.strptime(date_str, "%Y-%m-%d")
                    except ValueError:
                        continue
                    
                    # Verifica se è nel range richiesto
                    if match_date.date() < start_date or match_date.date() > end_date:
                        continue
                    
                    # Estrai squadre
                    home_cell = row.select_one("[data-stat='home_team']")
                    away_cell = row.select_one("[data-stat='away_team']")
                    
                    if not home_cell or not away_cell:
                        continue
                    
                    home_team = home_cell.get_text(strip=True)
                    away_team = away_cell.get_text(strip=True)
                    
                    # Estrai ora (se disponibile)
                    time_cell = row.select_one("[data-stat='start_time']")
                    match_time = match_date.replace(hour=15, minute=0)  # Default 15:00
                    
                    if time_cell and time_cell.get_text(strip=True):
                        time_str = time_cell.get_text(strip=True)
                        try:
                            time_parts = time_str.split(":")
                            match_time = match_date.replace(
                                hour=int(time_parts[0]),
                                minute=int(time_parts[1]) if len(time_parts) > 1 else 0
                            )
                        except (ValueError, IndexError):
                            pass
                    
                    # Calcola finestra di pubblicazione
                    publish_time = match_time - timedelta(hours=CONFIG["publish_window_start"])
                    expire_time = match_time + timedelta(hours=CONFIG["expire_time"])
                    
                    # Genera ID
                    match_id = f"fbref_{comp_id}_{match_date.strftime('%Y%m%d')}_{home_team.replace(' ', '')}_{away_team.replace(' ', '')}"
                    
                    # Ottieni round/matchday se disponibile
                    matchday = ""
                    round_cell = row.select_one("[data-stat='round']")
                    if round_cell:
                        matchday = round_cell.get_text(strip=True)
                    
                    match_data = {
                        'id': match_id,
                        'home_team': home_team,
                        'home_team_id': f"fbref_{normalize_team_name(home_team)}",
                        'away_team': away_team,
                        'away_team_id': f"fbref_{normalize_team_name(away_team)}",
                        'competition': comp_info['name'],
                        'competition_code': f"fbref_{comp_id}",
                        'country': comp_info['country'],
                        'utc_date': match_time.isoformat(),
                        'status': 'SCHEDULED',
                        'matchday': matchday,
                        'publish_time': publish_time.isoformat(),
                        'expire_time': expire_time.isoformat(),
                        'processed': False,
                        'article_generated': False,
                        'source': 'fbref',
                        'last_updated': datetime.now().isoformat()
                    }
                    
                    matches.append(match_data)
                    match_count += 1
                
                except Exception as e:
                    logger.error(f"Errore elaborazione partita FBref: {str(e)}")
        
        except Exception as e:
            logger.error(f"Errore FBref per {comp_info['name']}: {str(e)}")
        
        # Rispetta i limiti per scraping etico
        time.sleep(random.uniform(2, 4))
    
    # Salva in cache
    if matches:
        save_to_cache(cache_key, matches, 'fbref')
    
    return matches

def scrape_flashscore(start_date, end_date):
    """Scraping da FlashScore per ottenere partite"""
    matches = []
    source_config = ADDITIONAL_SOURCES["flashscore"]
    base_url = source_config["base_url"]
    
    # Verifica cache
    cache_key = f"flashscore_{start_date.isoformat()}_{end_date.isoformat()}"
    cached_data = get_from_cache(cache_key)
    
    if cached_data:
        logger.info(f"FlashScore: Usando dati in cache")
        return cached_data
    
    # FlashScore usa un formato specifico per date nell'URL
    date_range = []
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%Y%m%d")
        date_range.append(date_str)
        current_date += timedelta(days=1)
    
    match_count = 0
    limit = CONFIG["source_limits"]["flashscore"]
    
    for date_str in date_range:
        if match_count >= limit:
            logger.info(f"FlashScore: Raggiunto limite di {limit} partite")
            break
            
        url = f"{base_url}/football/matches/{date_str}/"
        
        try:
            # FlashScore richiede un user-agent specifico e potrebbe bloccare richieste robotiche
            # Usiamo proxy rotanti per evitare blocchi
            response = make_request_with_retry(url, max_retries=3, use_proxy=True, source_name="flashscore")
            if not response:
                logger.warning(f"Nessuna risposta da FlashScore per {date_str}")
                continue
            
            soup = BeautifulSoup(response.text, "html.parser")
            
            # FlashScore ha una struttura con tornei e partite all'interno
            tournament_blocks = soup.select(".sportName.soccer .leagues--static .event__group")
            
            for tournament in tournament_blocks:
                if match_count >= limit:
                    break
                
                try:
                    # Ottieni nome del torneo e paese
                    tournament_header = tournament.select_one(".event__title")
                    if not tournament_header:
                        continue
                    
                    tournament_text = tournament_header.get_text(strip=True)
                    # FlashScore usa formato "Paese: Competizione"
                    tournament_parts = tournament_text.split(":", 1)
                    
                    country = tournament_parts[0].strip() if len(tournament_parts) > 1 else "Unknown"
                    competition = tournament_parts[1].strip() if len(tournament_parts) > 1 else tournament_parts[0].strip()
                    
                    # Ottieni le partite in questo torneo
                    match_elements = tournament.select(".event__match")
                    
                    for element in match_elements:
                        if match_count >= limit:
                            break
                        
                        try:
                            # Verifica se è una partita non ancora giocata (programmata)
                            status_el = element.select_one(".event__stage")
                            if not status_el or "sched" not in status_el.get("class", []):
                                continue
                            
                            # Ottieni ID partita
                            match_id = element.get("id", "").replace("g_1_", "")
                            if not match_id:
                                continue
                            
                            # Ottieni squadre
                            home_el = element.select_one(".event__participant--home")
                            away_el = element.select_one(".event__participant--away")
                            
                            if not home_el or not away_el:
                                continue
                            
                            home_team = home_el.get_text(strip=True)
                            away_team = away_el.get_text(strip=True)
                            
                            # Ottieni orario
                            time_el = element.select_one(".event__time")
                            if not time_el:
                                continue
                                
                            time_str = time_el.get_text(strip=True)
                            
                            # FlashScore usa formato "HH:MM"
                            try:
                                hour, minute = map(int, time_str.split(":"))
                                match_date = datetime.strptime(date_str, "%Y%m%d")
                                match_time = match_date.replace(hour=hour, minute=minute)
                            except (ValueError, IndexError):
                                # Se formato non valido, usa ora predefinita
                                match_date = datetime.strptime(date_str, "%Y%m%d")
                                match_time = match_date.replace(hour=15, minute=0)
                            
                            # Calcola finestra di pubblicazione
                            publish_time = match_time - timedelta(hours=CONFIG["publish_window_start"])
                            expire_time = match_time + timedelta(hours=CONFIG["expire_time"])
                            
                            # Crea oggetto partita
                            match_data = {
                                'id': f"flash_{match_id}",
                                'home_team': home_team,
                                'home_team_id': f"flash_{normalize_team_name(home_team)}",
                                'away_team': away_team,
                                'away_team_id': f"flash_{normalize_team_name(away_team)}",
                                'competition': competition,
                                'competition_code': f"flash_{normalize_team_name(competition)}",
                                'country': country,
                                'utc_date': match_time.isoformat(),
                                'status': 'SCHEDULED',
                                'publish_time': publish_time.isoformat(),
                                'expire_time': expire_time.isoformat(),
                                'processed': False,
                                'article_generated': False,
                                'source': 'flashscore',
                                'last_updated': datetime.now().isoformat()
                            }
                            
                            matches.append(match_data)
                            match_count += 1
                            
                        except Exception as e:
                            logger.error(f"Errore parsing partita FlashScore: {str(e)}")
                    
                except Exception as e:
                    logger.error(f"Errore parsing torneo FlashScore: {str(e)}")
            
        except Exception as e:
            logger.error(f"Errore scraping FlashScore per {date_str}: {str(e)}")
        
        # Rispetta i limiti per scraping etico
        time.sleep(random.uniform(3, 5))
    
    # Salva in cache
    if matches:
        save_to_cache(cache_key, matches, 'flashscore')
    
    return matches

def scrape_sportinglife(start_date, end_date):
    """Scraping da Sporting Life"""
    matches = []
    source_config = ADDITIONAL_SOURCES["sportinglife"]
    base_url = source_config["base_url"]
    
    # Verifica cache
    cache_key = f"sportinglife_{start_date.isoformat()}_{end_date.isoformat()}"
    cached_data = get_from_cache(cache_key)
    
    if cached_data:
        logger.info(f"Sporting Life: Usando dati in cache")
        return cached_data
    
    # Crea URL per le date richieste
    date_range = []
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        date_range.append(date_str)
        current_date += timedelta(days=1)
    
    match_count = 0
    limit = CONFIG["source_limits"]["sportinglife"]
    
    for date_str in date_range:
        if match_count >= limit:
            logger.info(f"Sporting Life: Raggiunto limite di {limit} partite")
            break
            
        url = f"{base_url}/{date_str}"
        
        response = make_request_with_retry(url, source_name="sportinglife")
        if not response:
            logger.warning(f"Nessuna risposta da Sporting Life per {date_str}")
            continue
        
        try:
            soup = BeautifulSoup(response.text, "html.parser")
            match_elements = soup.select(source_config["selector"])
            
            logger.info(f"Sporting Life: Trovati {len(match_elements)} elementi per {date_str}")
            
            for element in match_elements:
                if match_count >= limit:
                    break
                    
                try:
                    # Estrai dati partita
                    competition_el = element.select_one(".fixtures-page__competition-name")
                    competition = competition_el.text.strip() if competition_el else "Unknown"
                    
                    home_el = element.select_one(".fixtures-page__team-name--home")
                    away_el = element.select_one(".fixtures-page__team-name--away")
                    
                    if not home_el or not away_el:
                        continue
                    
                    home_team = home_el.text.strip()
                    away_team = away_el.text.strip()
                    
                    time_el = element.select_one(".fixtures-page__team-time")
                    time_text = time_el.text.strip() if time_el else "15:00"
                    
                    # Costruisci datetime della partita
                    match_date = datetime.strptime(date_str, "%Y-%m-%d")
                    match_time_str = f"{time_text}"
                    try:
                        match_hour, match_minute = map(int, match_time_str.split(':'))
                        match_time = match_date.replace(hour=match_hour, minute=match_minute)
                    except:
                        match_time = match_date.replace(hour=15, minute=0)
                    
                    # Calcola finestra di pubblicazione
                    publish_time = match_time - timedelta(hours=CONFIG["publish_window_start"])
                    expire_time = match_time + timedelta(hours=CONFIG["expire_time"])
                    
                    # Genera ID per la partita
                    match_id = f"sl_{date_str}_{home_team.replace(' ', '')}_{away_team.replace(' ', '')}"
                    
                    match_data = {
                        'id': match_id,
                        'home_team': home_team,
                        'home_team_id': f"sl_{normalize_team_name(home_team)}",
                        'away_team': away_team,
                        'away_team_id': f"sl_{normalize_team_name(away_team)}",
                        'competition': competition,
                        'competition_code': f"sl_{normalize_team_name(competition)}",
                        'country': source_config["country"],
                        'utc_date': match_time.isoformat(),
                        'status': 'SCHEDULED',
                        'publish_time': publish_time.isoformat(),
                        'expire_time': expire_time.isoformat(),
                        'processed': False,
                        'article_generated': False,
                        'source': 'sportinglife',
                        'last_updated': datetime.now().isoformat()
                    }
                    
                    matches.append(match_data)
                    match_count += 1
                
                except Exception as e:
                    logger.error(f"Errore parsing Sporting Life: {str(e)}")
            
        except Exception as e:
            logger.error(f"Errore scraping Sporting Life per {date_str}: {str(e)}")
        
        # Rispetta i limiti per scraping etico
        time.sleep(random.uniform(2, 5))
    
    # Salva in cache
    if matches:
        save_to_cache(cache_key, matches, 'sportinglife')
    
    return matches

def scrape_bbc_sport(start_date, end_date):
    """Scraping da BBC Sport"""
    matches = []
    source_config = ADDITIONAL_SOURCES["bbc_sport"]
    base_url = source_config["base_url"]
    
    # Verifica cache
    cache_key = f"bbc_sport_{start_date.isoformat()}_{end_date.isoformat()}"
    cached_data = get_from_cache(cache_key)
    
    if cached_data:
        logger.info(f"BBC Sport: Usando dati in cache")
        return cached_data
    
    # BBC Sport usa un formato specifico per date nell'URL
    date_range = []
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        date_range.append(date_str)
        current_date += timedelta(days=1)
    
    match_count = 0
    limit = CONFIG["source_limits"]["bbc_sport"]
    
    for date_str in date_range:
        if match_count >= limit:
            logger.info(f"BBC Sport: Raggiunto limite di {limit} partite")
            break
            
        url = f"{base_url}/{date_str}"
        
        response = make_request_with_retry(url, source_name="bbc_sport")
        if not response:
            logger.warning(f"Nessuna risposta da BBC Sport per {date_str}")
            continue
        
        try:
            soup = BeautifulSoup(response.text, "html.parser")
            match_elements = soup.select(source_config["selector"])
            
            logger.info(f"BBC Sport: Trovati {len(match_elements)} elementi per {date_str}")
            
            for element in match_elements:
                if match_count >= limit:
                    break
                    
                try:
                    # Estrai dati partita
                    competition_el = element.select_one(".sp-c-fixture__competition-name")
                    competition = competition_el.text.strip() if competition_el else "Unknown"
                    
                    home_el = element.select_one(".sp-c-fixture__team-name--home .sp-c-fixture__team-name-trunc")
                    away_el = element.select_one(".sp-c-fixture__team-name--away .sp-c-fixture__team-name-trunc")
                    
                    if not home_el or not away_el:
                        continue
                    
                    home_team = home_el.text.strip()
                    away_team = away_el.text.strip()
                    
                    time_el = element.select_one(".sp-c-fixture__number--time")
                    time_text = time_el.text.strip() if time_el else "15:00"
                    
                    # Costruisci datetime della partita
                    match_date = datetime.strptime(date_str, "%Y-%m-%d")
                    try:
                        # BBC usa formato 24 ore (HH:MM)
                        match_hour, match_minute = map(int, time_text.split(':'))
                        match_time = match_date.replace(hour=match_hour, minute=match_minute)
                    except:
                        match_time = match_date.replace(hour=15, minute=0)
                    
                    # Calcola finestra di pubblicazione
                    publish_time = match_time - timedelta(hours=CONFIG["publish_window_start"])
                    expire_time = match_time + timedelta(hours=CONFIG["expire_time"])
                    
                    # Genera ID per la partita
                    match_id = f"bbc_{date_str}_{normalize_team_name(home_team)}_{normalize_team_name(away_team)}"
                    
                    match_data = {
                        'id': match_id,
                        'home_team': home_team,
                        'home_team_id': f"bbc_{normalize_team_name(home_team)}",
                        'away_team': away_team,
                        'away_team_id': f"bbc_{normalize_team_name(away_team)}",
                        'competition': competition,
                        'competition_code': f"bbc_{normalize_team_name(competition)}",
                        'country': source_config["country"],
                        'utc_date': match_time.isoformat(),
                        'status': 'SCHEDULED',
                        'publish_time': publish_time.isoformat(),
                        'expire_time': expire_time.isoformat(),
                        'processed': False,
                        'article_generated': False,
                        'source': 'bbc_sport',
                        'last_updated': datetime.now().isoformat()
                    }
                    
                    # Controlla se ci sono info lineups
                    lineup_el = element.select_one(source_config["lineup_selector"])
                    if lineup_el:
                        match_data['has_lineup'] = True
                    
                    matches.append(match_data)
                    match_count += 1
                
                except Exception as e:
                    logger.error(f"Errore parsing BBC Sport: {str(e)}")
            
        except Exception as e:
            logger.error(f"Errore scraping BBC Sport per {date_str}: {str(e)}")
        
        # Rispetta i limiti per scraping etico
        time.sleep(random.uniform(2, 5))
    
    # Salva in cache
    if matches:
        save_to_cache(cache_key, matches, 'bbc_sport')
    
    return matches

def scrape_additional_sources(start_date, end_date):
    """Raccoglie partite da tutte le fonti di scraping configurate"""
    all_matches = []
    
    # Sporting Life
    try:
        logger.info("Avvio scraping da Sporting Life...")
        sl_matches = scrape_sportinglife(start_date, end_date)
        logger.info(f"Raccolte {len(sl_matches)} partite da Sporting Life")
        all_matches.extend(sl_matches)
    except Exception as e:
        logger.error(f"Errore durante scraping Sporting Life: {str(e)}")
    
    # BBC Sport
    try:
        logger.info("Avvio scraping da BBC Sport...")
        bbc_matches = scrape_bbc_sport(start_date, end_date)
        logger.info(f"Raccolte {len(bbc_matches)} partite da BBC Sport")
        all_matches.extend(bbc_matches)
    except Exception as e:
        logger.error(f"Errore durante scraping BBC Sport: {str(e)}")
    
    # FlashScore (nuova fonte)
    try:
        logger.info("Avvio scraping da FlashScore...")
        flash_matches = scrape_flashscore(start_date, end_date)
        logger.info(f"Raccolte {len(flash_matches)} partite da FlashScore")
        all_matches.extend(flash_matches)
    except Exception as e:
        logger.error(f"Errore durante scraping FlashScore: {str(e)}")
        
    # FBref (nuova fonte)
    try:
        logger.info("Avvio scraping da FBref...")
        fbref_matches = get_matches_from_fbref(start_date, end_date)
        logger.info(f"Raccolte {len(fbref_matches)} partite da FBref")
        all_matches.extend(fbref_matches)
    except Exception as e:
        logger.error(f"Errore durante scraping FBref: {str(e)}")
    
    return all_matches

def get_matches_from_additional_apis(start_date, end_date):
    """Raccoglie partite da API aggiuntive"""
    all_matches = []
    
    # API-Football (se configurata)
    if RAPID_API_KEY:
        try:
            logger.info("Raccolta partite da API-Football...")
            apif_matches = get_matches_from_api_football(start_date, end_date)
            logger.info(f"Raccolte {len(apif_matches)} partite da API-Football")
            all_matches.extend(apif_matches)
        except Exception as e:
            logger.error(f"Errore durante raccolta da API-Football: {str(e)}")
    
    # OpenFootball Data
    try:
        logger.info("Raccolta partite da OpenFootball Data...")
        of_matches = get_matches_from_openfootball()
        logger.info(f"Raccolte {len(of_matches)} partite da OpenFootball Data")
        all_matches.extend(of_matches)
    except Exception as e:
        logger.error(f"Errore durante raccolta da OpenFootball Data: {str(e)}")
    
    # SofaScore API (nuova fonte)
    try:
        logger.info("Raccolta partite da SofaScore API...")
        sofa_matches = get_matches_from_sofascore_api(start_date, end_date)
        logger.info(f"Raccolte {len(sofa_matches)} partite da SofaScore API")
        all_matches.extend(sofa_matches)
    except Exception as e:
        logger.error(f"Errore durante raccolta da SofaScore API: {str(e)}")
    
    return all_matches

def save_matches_to_firebase(matches):
    """Salva le partite su Firebase"""
    if not matches:
        logger.info("Nessuna partita da salvare")
        return 0, 0
    
    try:
        ref = db.reference('matches')
        saved_count = 0
        updated_count = 0
        
        # Raggruppa le partite per data
        matches_by_date = {}
        for match in matches:
            match_date = datetime.fromisoformat(match['utc_date'].replace('Z', '+00:00')).strftime('%Y-%m-%d')
            if match_date not in matches_by_date:
                matches_by_date[match_date] = []
            matches_by_date[match_date].append(match)
        
        # Processa ogni data in batch per ridurre chiamate a Firebase
        for match_date, date_matches in matches_by_date.items():
            # Ottieni partite esistenti per questa data
            existing_matches = ref.child(match_date).get() or {}
            
            updates = {}
            for match in date_matches:
                match_id = match['id']
                
                if match_id in existing_matches:
                    # Aggiorna solo timestamp e mantieni dati esistenti
                    existing_match = existing_matches[match_id]
                    existing_match['last_updated'] = match['last_updated']
                    updates[match_id] = existing_match
                    updated_count += 1
                else:
                    # Aggiungi nuova partita
                    updates[match_id] = match
                    saved_count += 1
            
            # Salva in batch se ci sono aggiornamenti
            if updates:
                ref.child(match_date).update(updates)
        
        return saved_count, updated_count
    except Exception as e:
        logger.error(f"Errore durante il salvataggio su Firebase: {str(e)}")
        return 0, 0

def remove_duplicates(matches):
    """Rimuove partite duplicate basandosi su squadre, data e competizione"""
    try:
        unique_matches = {}
        duplicates_count = 0
        
        for match in matches:
            # Crea una chiave di identificazione
            # Utilizziamo data, squadre e competizione per identificare partite duplicate
            try:
                match_time = datetime.fromisoformat(match['utc_date'].replace('Z', '+00:00'))
                match_date = match_time.strftime('%Y-%m-%d')
                match_hour = match_time.hour
                
                # Normalizziamo i nomi delle squadre
                home_team = normalize_team_name(match['home_team'])
                away_team = normalize_team_name(match['away_team'])
                
                key = f"{match_date}_{match_hour}_{home_team}_{away_team}"
                
                # Se è un duplicato, diamo priorità alle fonti ufficiali
                if key in unique_matches:
                    duplicates_count += 1
                    existing_source = unique_matches[key]['source']
                    current_source = match['source']
                    
                    # Priorità alle fonti
                    source_priority = {
                        'football-data-api': 1,   # Priorità massima
                        'api-football': 2,
                        'sofascore_api': 3,
                        'openfootball': 4,
                        'fbref': 5,
                        'flashscore': 6,
                        'bbc_sport': 7,
                        'sportinglife': 8,
                        'goal_com': 9,
                        'soccerway': 10
                    }
                    
                    # Se la nuova fonte ha priorità maggiore (numero più basso), sostituisci
                    if source_priority.get(current_source, 999) < source_priority.get(existing_source, 999):
                        unique_matches[key] = match
                else:
                    unique_matches[key] = match
            except Exception as e:
                logger.warning(f"Errore durante verifica duplicato: {str(e)}")
                # Se c'è un errore nella gestione del duplicato, mantieni la partita
                unique_id = f"error_{random.randint(1000, 9999)}_{match['id']}"
                unique_matches[unique_id] = match
        
        logger.info(f"Rimosse {duplicates_count} partite duplicate")
        return list(unique_matches.values())
    except Exception as e:
        logger.error(f"Errore durante rimozione duplicati: {str(e)}")
        return matches

def cleanup_logs():
    """Pulisce vecchi file di log per risparmiare spazio"""
    try:
        log_max_size_mb = CONFIG["max_log_size_mb"]
        log_max_size_bytes = log_max_size_mb * 1024 * 1024
        
        current_log_size = os.path.getsize(log_file)
        if current_log_size > log_max_size_bytes:
            # Rotazione log: rinomina corrente e crea nuovo
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
            backup_log = f"{log_file}.{timestamp}"
            os.rename(log_file, backup_log)
            
            # Rimuovi log più vecchi di 7 giorni
            log_files = [f for f in os.listdir(log_dir) if f.startswith("fetch_matches_") and f.endswith(".log")]
            for log_f in log_files:
                file_path = os.path.join(log_dir, log_f)
                file_time = os.path.getmtime(file_path)
                if time.time() - file_time > 7 * 24 * 3600:  # 7 giorni in secondi
                    os.remove(file_path)
                    logger.info(f"Rimosso vecchio log: {log_f}")
    except Exception as e:
        logger.warning(f"Errore durante pulizia log: {str(e)}")

def update_health_status(status, stats=None):
    """Aggiorna lo stato di salute del componente"""
    try:
        health_ref = db.reference('health/fetch_matches')
        health_data = {
            'last_run': datetime.now().isoformat(),
            'status': status
        }
        
        if stats:
            health_data.update(stats)
            
        health_ref.set(health_data)
        logger.info(f"Stato di salute aggiornato: {status}")
        return True
    except Exception as e:
        logger.error(f"Errore aggiornamento stato salute: {str(e)}")
        return False

def main():
    """Funzione principale"""
    start_time = datetime.now()
    logger.info(f"Avvio Fetch Matches - {start_time.isoformat()}")
    
    try:
        # 0. Inizializza cache locale
        init_local_cache()
        
        # 1. Pulisci vecchi dati cache
        clean_old_cache_entries()
        
        # 2. Inizializza Firebase
        if not initialize_firebase():
            logger.error("Errore inizializzazione Firebase, arresto esecuzione")
            return 1
        
        # 3. Definisci periodo da raccogliere
        today = datetime.now().date()
        start_date = today - timedelta(days=CONFIG["days_behind"])
        end_date = today + timedelta(days=CONFIG["days_ahead"])
        
        # 4. Ottieni partite dalle diverse fonti
        # 4.1 API ufficiale Football-Data
        api_matches = get_matches_for_date_range(start_date, end_date)
        logger.info(f"Raccolte {len(api_matches)} partite da Football-Data API")
        
        # 4.2 API aggiuntive
        api_extra_matches = get_matches_from_additional_apis(start_date, end_date)
        logger.info(f"Raccolte {len(api_extra_matches)} partite da API aggiuntive")
        
        # 4.3 Fonti scraping
        scraped_matches = scrape_additional_sources(start_date, end_date)
        logger.info(f"Raccolte {len(scraped_matches)} partite da fonti web")
        
        # 4.4 Combina tutte le partite
        all_matches = api_matches + api_extra_matches + scraped_matches
        logger.info(f"Totale partite raccolte: {len(all_matches)}")
        
        # 5. Rimuovi duplicati
        unique_matches = remove_duplicates(all_matches)
        logger.info(f"Partite uniche dopo rimozione duplicati: {len(unique_matches)}")
        
        # 6. Salva su Firebase
        saved, updated = save_matches_to_firebase(unique_matches)
        logger.info(f"Partite salvate: {saved}, aggiornate: {updated}")
        
        # 7. Statistiche esecuzione
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        execution_stats = {
            'total_matches': len(all_matches),
            'unique_matches': len(unique_matches),
            'saved_matches': saved,
            'updated_matches': updated,
            'sources': {
                'football_data_api': len(api_matches),
                'additional_apis': len(api_extra_matches),
                'web_scraping': len(scraped_matches)
            },
            'duration_seconds': duration
        }
        
        # 8. Salva statistiche
        save_execution_stats(execution_stats)
        
        # 9. Aggiorna stato health
        update_health_status('success', {
            'matches_collected': len(all_matches),
            'matches_unique': len(unique_matches),
            'matches_saved': saved,
            'matches_updated': updated,
            'duration_seconds': duration,
            'sources': {
                'football_data_api': len(api_matches),
                'additional_apis': len(api_extra_matches),
                'web_scraping': len(scraped_matches)
            }
        })
        
        # 10. Pulizia log
        cleanup_logs()
        
    except Exception as e:
        logger.error(f"Errore generale: {str(e)}")
        
        # Aggiorna stato health con errore
        update_health_status('error', {
            'error_message': str(e)
        })
            
        return 1
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    logger.info(f"Fetch Matches completato con successo in {duration} secondi")
    return 0

if __name__ == "__main__":
    sys.exit(main())
