#!/usr/bin/env python3
"""
Fetch Matches - Script per raccogliere dati sulle partite di calcio
Raccoglie dati da API Football-Data e altre fonti, salvandoli su Firebase
"""
import os
import sys
import requests
import json
import logging
import time
import random
import re
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import firebase_admin
from firebase_admin import credentials, db
from dotenv import load_dotenv
import urllib.robotparser as robotparser
from urllib.parse import urlparse, quote

# Configurazione logging
log_dir = os.path.expanduser('~/football-predictions/logs')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"fetch_matches_{datetime.now().strftime('%Y%m%d')}.log")

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Variabili globali
load_dotenv()
API_KEY = os.getenv('FOOTBALL_API_KEY')
BASE_URL = "https://api.football-data.org/v4"

# Chiavi API aggiuntive (opzionali)
RAPID_API_KEY = os.getenv('RAPID_API_KEY', '')  # Per API-Football

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
        "country": "UK"
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
        "country": "International"
    },
    "flashscore": {
        "name": "Flashscore",
        "base_url": "https://www.flashscore.com",
        "fixtures_url": "https://www.flashscore.com/football/",
        "selector": ".sportName.soccer",
        "date_format": "%Y-%m-%d",
        "country": "International"
    },
    "soccerway": {
        "name": "Soccerway",
        "base_url": "https://us.soccerway.com",
        "fixtures_url": "https://us.soccerway.com/matches/",
        "selector": ".matches",
        "date_format": "%d %B %Y",
        "country": "International"
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

def initialize_firebase():
    """Inizializza connessione Firebase"""
    try:
        firebase_admin.get_app()
    except ValueError:
        cred_path = os.path.expanduser('~/football-predictions/creds/firebase-credentials.json')
        cred = credentials.Certificate(cred_path)
        firebase_admin.initialize_app(cred, {
            'databaseURL': os.getenv('FIREBASE_DB_URL')
        })
    return True

def get_random_user_agent():
    """Restituisce uno User-Agent casuale per evitare blocchi"""
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (iPad; CPU OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36 Edg/92.0.902.55",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36 OPR/78.0.4093.112"
    ]
    return random.choice(user_agents)

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
        logging.warning(f"Errore nella verifica robots.txt per {url}: {str(e)}")
        # In caso di errore, assume che sia permesso ma con cautela
        return True

def make_api_request(endpoint, params=None):
    """Effettua richiesta all'API con gestione errori e retry"""
    url = f"{BASE_URL}/{endpoint}"
    headers = {
        "X-Auth-Token": API_KEY,
        "User-Agent": get_random_user_agent()
    }
    
    max_retries = 3
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            logging.info(f"Richiesta API: {url}")
            response = requests.get(url, headers=headers, params=params)
            
            # Rispetta i limiti dell'API
            time.sleep(1.5)
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:  # Too Many Requests
                logging.warning(f"Rate limit raggiunto. Attesa più lunga.")
                time.sleep(60)  # Attendi 60 secondi
                continue
            elif response.status_code == 404:
                logging.warning(f"Risorsa non trovata: {url}")
                return None
            else:
                logging.error(f"Errore API {response.status_code}: {response.text}")
                return None
        except Exception as e:
            logging.error(f"Errore richiesta: {str(e)}")
            if attempt < max_retries - 1:
                sleep_time = retry_delay * (2 ** attempt)  # Backoff esponenziale
                logging.info(f"Ritento fra {sleep_time} secondi...")
                time.sleep(sleep_time)
            else:
                return None
    
    return None

def make_request_with_retry(url, max_retries=3, method="get", **kwargs):
    """Esegue richiesta HTTP con gestione errori e retry"""
    if 'headers' not in kwargs:
        kwargs['headers'] = {'User-Agent': get_random_user_agent()}
    
    # Verifica se lo scraping è permesso
    if not is_scraping_allowed(url):
        logging.warning(f"Scraping non permesso per {url}")
        return None
    
    for attempt in range(max_retries):
        try:
            # Aggiunge un ritardo casuale per scraping etico
            delay = random.uniform(1, 3)
            time.sleep(delay)
            
            if method.lower() == "get":
                response = requests.get(url, **kwargs)
            elif method.lower() == "post":
                response = requests.post(url, **kwargs)
            else:
                raise ValueError(f"Metodo non supportato: {method}")
            
            if response.status_code == 200:
                return response
            elif response.status_code == 429:  # Too Many Requests
                logging.warning(f"Rate limit raggiunto per {url}. Attesa lunga.")
                time.sleep(60)  # Attesa più lunga
                continue
            else:
                logging.error(f"Errore richiesta {url}: {response.status_code}")
                if attempt < max_retries - 1:
                    sleep_time = 2 ** attempt  # Backoff esponenziale
                    time.sleep(sleep_time)
                    continue
                return None
        except Exception as e:
            logging.error(f"Errore connessione a {url}: {str(e)}")
            if attempt < max_retries - 1:
                sleep_time = 2 ** attempt  # Backoff esponenziale
                time.sleep(sleep_time)
                continue
            return None
    
    return None

def get_matches_for_date_range(start_date, end_date):
    """Ottiene le partite in un intervallo di date dall'API Football-Data"""
    matches = []
    
    # Converti date in stringhe formato ISO
    start_str = start_date.isoformat()
    end_str = end_date.isoformat()
    
    params = {
        "dateFrom": start_str,
        "dateTo": end_str
    }
    
    # Raccoglie partite per ogni campionato
    for league_code, league_info in LEAGUES.items():
        logging.info(f"Raccolta partite per {league_info['name']} ({start_str} - {end_str})")
        
        # Alcuni codici campionato potrebbero richiedere endpoint diversi
        if league_code.startswith('WCQ_'):
            # Le qualificazioni mondiali potrebbero richiedere approcci diversi
            # Per esempio, potremmo usare un endpoint generico e filtrare
            continue  # Saltato per ora - implementare logica specifica in futuro
        
        endpoint = f"competitions/{league_code}/matches"
        data = make_api_request(endpoint, params)
        
        if not data:
            logging.warning(f"Nessun dato per {league_info['name']}")
            continue
        
        league_matches = data.get('matches', [])
        logging.info(f"Trovate {len(league_matches)} partite in {league_info['name']}")
        
        for match in league_matches:
            # Filtra solo le partite programmate
            if match['status'] == 'SCHEDULED':
                try:
                    match_time = datetime.fromisoformat(match['utcDate'].replace('Z', '+00:00'))
                    
                    # Calcola window di pubblicazione (8-6 ore prima)
                    publish_time = match_time - timedelta(hours=int(os.getenv('PUBLISH_WINDOW_START', 8)))
                    expire_time = match_time + timedelta(hours=int(os.getenv('EXPIRE_TIME', 8)))
                    
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
                except Exception as e:
                    logging.error(f"Errore elaborazione partita: {str(e)}")
        
        # Rispetta i limiti dell'API tra campionati
        time.sleep(3)
    
    return matches

def get_matches_from_api_football(start_date, end_date):
    """Ottiene partite da API-Football (RapidAPI)"""
    if not RAPID_API_KEY:
        logging.warning("API-Football: chiave API non configurata, saltando fonte")
        return []
    
    matches = []
    api_config = ADDITIONAL_APIS["api_football"]
    
    # Converti date nel formato richiesto (YYYY-MM-DD)
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    
    url = f"{api_config['base_url']}{api_config['endpoints']['fixtures']}"
    headers = api_config['headers']
    
    for date_str in [start_str, end_str]:
        try:
            querystring = {"date": date_str}
            response = make_request_with_retry(url, headers=headers, params=querystring)
            
            if not response:
                logging.warning(f"API-Football: Nessuna risposta per {date_str}")
                continue
            
            data = response.json()
            
            if data.get('response'):
                fixtures = data['response']
                logging.info(f"API-Football: Trovate {len(fixtures)} partite per {date_str}")
                
                for fixture in fixtures:
                    match_time = datetime.fromisoformat(fixture['fixture']['date'])
                    
                    # Calcola finestra di pubblicazione
                    publish_time = match_time - timedelta(hours=int(os.getenv('PUBLISH_WINDOW_START', 8)))
                    expire_time = match_time + timedelta(hours=int(os.getenv('EXPIRE_TIME', 8)))
                    
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
            
            # Rispetta i limiti dell'API
            time.sleep(2)
        
        except Exception as e:
            logging.error(f"Errore API-Football per {date_str}: {str(e)}")
    
    return matches

def get_matches_from_openfootball():
    """Ottiene partite dai dataset OpenFootball (GitHub)"""
    matches = []
    api_config = ADDITIONAL_APIS["openfootball"]
    
    for league_code, endpoint in api_config['endpoints'].items():
        url = f"{api_config['base_url']}{endpoint}"
        
        try:
            response = make_request_with_retry(url)
            if not response:
                logging.warning(f"OpenFootball: Nessuna risposta per {league_code}")
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
                    # Estrai data della partita
                    match_date_str = match.get('date')
                    if not match_date_str:
                        continue
                    
                    try:
                        # Tenta di convertire la data
                        match_date = datetime.strptime(match_date_str, "%Y-%m-%d")
                        
                        # Verifica se è nel futuro
                        now = datetime.now()
                        if match_date < now.date():
                            continue
                        
                        # Aggiungi un'ora predefinita (ad es. 15:00)
                        match_time = datetime.combine(match_date, datetime.min.time().replace(hour=15))
                        
                        # Calcola finestra di pubblicazione
                        publish_time = match_time - timedelta(hours=int(os.getenv('PUBLISH_WINDOW_START', 8)))
                        expire_time = match_time + timedelta(hours=int(os.getenv('EXPIRE_TIME', 8)))
                        
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
                    except ValueError:
                        logging.error(f"Formato data non valido: {match_date_str}")
                        continue
        
        except Exception as e:
            logging.error(f"Errore OpenFootball per {league_code}: {str(e)}")
        
        # Rispetta i limiti di richieste
        time.sleep(1)
    
    return matches

def scrape_sportinglife(start_date, end_date):
    """Scraping da Sporting Life"""
    matches = []
    source_config = ADDITIONAL_SOURCES["sportinglife"]
    base_url = source_config["base_url"]
    
    # Crea URL per le date richieste
    date_range = []
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        date_range.append(date_str)
        current_date += timedelta(days=1)
    
    for date_str in date_range:
        url = f"{base_url}/{date_str}"
        
        response = make_request_with_retry(url)
        if not response:
            logging.warning(f"Nessuna risposta da Sporting Life per {date_str}")
            continue
        
        try:
            soup = BeautifulSoup(response.text, "html.parser")
            match_elements = soup.select(source_config["selector"])
            
            logging.info(f"Sporting Life: Trovati {len(match_elements)} elementi per {date_str}")
            
            for element in match_elements:
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
                    publish_time = match_time - timedelta(hours=int(os.getenv('PUBLISH_WINDOW_START', 8)))
                    expire_time = match_time + timedelta(hours=int(os.getenv('EXPIRE_TIME', 8)))
                    
                    # Genera ID per la partita
                    match_id = f"sl_{date_str}_{home_team.replace(' ', '')}_{away_team.replace(' ', '')}"
                    
                    match_data = {
                        'id': match_id,
                        'home_team': home_team,
                        'home_team_id': f"sl_{home_team.replace(' ', '')}",
                        'away_team': away_team,
                        'away_team_id': f"sl_{away_team.replace(' ', '')}",
                        'competition': competition,
                        'competition_code': f"sl_{competition.replace(' ', '')}",
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
                
                except Exception as e:
                    logging.error(f"Errore parsing Sporting Life: {str(e)}")
            
        except Exception as e:
            logging.error(f"Errore scraping Sporting Life per {date_str}: {str(e)}")
        
        # Rispetta i limiti per scraping etico
        time.sleep(random.uniform(2, 5))
    
    return matches

def scrape_bbc_sport(start_date, end_date):
    """Scraping da BBC Sport"""
    matches = []
    source_config = ADDITIONAL_SOURCES["bbc_sport"]
    base_url = source_config["base_url"]
    
    # BBC Sport usa un formato specifico per date nell'URL
    date_range = []
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        date_range.append(date_str)
        current_date += timedelta(days=1)
    
    for date_str in date_range:
        url = f"{base_url}/{date_str}"
        
        response = make_request_with_retry(url)
        if not response:
            logging.warning(f"Nessuna risposta da BBC Sport per {date_str}")
            continue
        
        try:
            soup = BeautifulSoup(response.text, "html.parser")
            match_elements = soup.select(source_config["selector"])
            
            logging.info(f"BBC Sport: Trovati {len(match_elements)} elementi per {date_str}")
            
            for element in match_elements:
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
                    publish_time = match_time - timedelta(hours=int(os.getenv('PUBLISH_WINDOW_START', 8)))
                    expire_time = match_time + timedelta(hours=int(os.getenv('EXPIRE_TIME', 8)))
                    
                    # Genera ID per la partita
                    match_id = f"bbc_{date_str}_{home_team.replace(' ', '')}_{away_team.replace(' ', '')}"
                    
                    match_data = {
                        'id': match_id,
                        'home_team': home_team,
                        'home_team_id': f"bbc_{home_team.replace(' ', '')}",
                        'away_team': away_team,
                        'away_team_id': f"bbc_{away_team.replace(' ', '')}",
                        'competition': competition,
                        'competition_code': f"bbc_{competition.replace(' ', '')}",
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
                    
                    matches.append(match_data)
                
                except Exception as e:
                    logging.error(f"Errore parsing BBC Sport: {str(e)}")
            
        except Exception as e:
            logging.error(f"Errore scraping BBC Sport per {date_str}: {str(e)}")
        
        # Rispetta i limiti per scraping etico
        time.sleep(random.uniform(2, 5))
    
    return matches

def scrape_goal_com(start_date, end_date):
    """Scraping da Goal.com"""
    matches = []
    source_config = ADDITIONAL_SOURCES["goal_com"]
    base_url = source_config["base_url"]
    
    # Date range
    date_range = []
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        date_range.append(date_str)
        current_date += timedelta(days=1)
    
    for date_str in date_range:
        url = f"{base_url}/{date_str}"
        
        response = make_request_with_retry(url)
        if not response:
            logging.warning(f"Nessuna risposta da Goal.com per {date_str}")
            continue
        
        try:
            soup = BeautifulSoup(response.text, "html.parser")
            match_containers = soup.select(source_config["selector"])
            
            logging.info(f"Goal.com: Trovati {len(match_containers)} container di partite per {date_str}")
            
            for container in match_containers:
                try:
                    # Ottieni competizione
                    comp_el = container.select_one(".competition-name")
                    competition = comp_el.text.strip() if comp_el else "Unknown"
                    
                    # Ottieni tutti i match nel container
                    match_elements = container.select(".match-row")
                    
                    for element in match_elements:
                        # Estrai dati partita
                        home_el = element.select_one(".team-home .team-name")
                        away_el = element.select_one(".team-away .team-name")
                        
                        if not home_el or not away_el:
                            continue
                        
                        home_team = home_el.text.strip()
                        away_team = away_el.text.strip()
                        
                        time_el = element.select_one(".match-time")
                        time_text = time_el.text.strip() if time_el else "15:00"
                        
                        # Costruisci datetime della partita
                        match_date = datetime.strptime(date_str, "%Y-%m-%d")
                        try:
                            # Goal.com può usare vari formati orari
                            if ":" in time_text:
                                match_hour, match_minute = map(int, time_text.split(':'))
                                match_time = match_date.replace(hour=match_hour, minute=match_minute)
                            else:
                                match_time = match_date.replace(hour=15, minute=0)
                        except:
                            match_time = match_date.replace(hour=15, minute=0)
                        
                        # Calcola finestra di pubblicazione
                        publish_time = match_time - timedelta(hours=int(os.getenv('PUBLISH_WINDOW_START', 8)))
                        expire_time = match_time + timedelta(hours=int(os.getenv('EXPIRE_TIME', 8)))
                        
                        # Genera ID per la partita
                        match_id = f"goal_{date_str}_{home_team.replace(' ', '')}_{away_team.replace(' ', '')}"
                        
                        match_data = {
                            'id': match_id,
                            'home_team': home_team,
                            'home_team_id': f"goal_{home_team.replace(' ', '')}",
                            'away_team': away_team,
                            'away_team_id': f"goal_{away_team.replace(' ', '')}",
                            'competition': competition,
                            'competition_code': f"goal_{competition.replace(' ', '')}",
                            'country': source_config["country"],
                            'utc_date': match_time.isoformat(),
                            'status': 'SCHEDULED',
                            'publish_time': publish_time.isoformat(),
                            'expire_time': expire_time.isoformat(),
                            'processed': False,
                            'article_generated': False,
                            'source': 'goal_com',
                            'last_updated': datetime.now().isoformat()
                        }
                        
                        matches.append(match_data)
                
                except Exception as e:
                    logging.error(f"Errore parsing Goal.com: {str(e)}")
            
        except Exception as e:
            logging.error(f"Errore scraping Goal.com per {date_str}: {str(e)}")
        
        # Rispetta i limiti per scraping etico
        time.sleep(random.uniform(3, 6))
    
    return matches

def scrape_soccerway(start_date, end_date):
    """Scraping da Soccerway"""
    matches = []
    source_config = ADDITIONAL_SOURCES["soccerway"]
    base_url = source_config["base_url"]
    
    # Soccerway usa un formato YYYY/MM/DD
    date_range = []
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%Y/%m/%d")
        date_range.append(date_str)
        current_date += timedelta(days=1)
    
    for date_str in date_range:
        url = f"{base_url}/matches/{date_str}/"
        
        response = make_request_with_retry(url)
        if not response:
            logging.warning(f"Nessuna risposta da Soccerway per {date_str}")
            continue
        
        try:
            soup = BeautifulSoup(response.text, "html.parser")
            match_tables = soup.select(".matches")
            
            logging.info(f"Soccerway: Trovate {len(match_tables)} tabelle di partite per {date_str}")
            
            for table in match_tables:
                try:
                    # Estrai competizione
                    comp_el = table.select_one(".table-header")
                    competition = comp_el.text.strip() if comp_el else "Unknown"
                    
                    # Estrai paese/regione dalla competizione
                    country = "International"
                    if ":" in competition:
                        parts = competition.split(":")
                        country = parts[0].strip()
                        competition = parts[1].strip()
                    
                    # Ottieni tutte le righe della tabella
                    match_rows = table.select("tbody tr")
                    
                    for row in match_rows:
                        # Verifica se è una partita programmata (non ancora giocata)
                        if "postponed" in row.get("class", []) or not row.select_one(".score-time"):
                            continue
                        
                        home_el = row.select_one(".team-a")
                        away_el = row.select_one(".team-b")
                        
                        if not home_el or not away_el:
                            continue
                        
                        home_team = home_el.text.strip()
                        away_team = away_el.text.strip()
                        
                        time_el = row.select_one(".score-time")
                        time_text = time_el.text.strip() if time_el else "15:00"
                        
                        # Scocerway può mostrare "-" per partite con orario non definito
                        if time_text == "-":
                            time_text = "15:00"
                        
                        # Costruisci datetime della partita
                        match_date = datetime.strptime(date_str.replace("/", "-"), "%Y-%m-%d")
                        try:
                            match_hour, match_minute = map(int, time_text.split(':'))
                            match_time = match_date.replace(hour=match_hour, minute=match_minute)
                        except:
                            match_time = match_date.replace(hour=15, minute=0)
                        
                        # Calcola finestra di pubblicazione
                        publish_time = match_time - timedelta(hours=int(os.getenv('PUBLISH_WINDOW_START', 8)))
                        expire_time = match_time + timedelta(hours=int(os.getenv('EXPIRE_TIME', 8)))
                        
                        # Genera ID per la partita
                        match_id = f"scw_{date_str.replace('/', '')}_{home_team.replace(' ', '')}_{away_team.replace(' ', '')}"
                        
                        match_data = {
                            'id': match_id,
                            'home_team': home_team,
                            'home_team_id': f"scw_{home_team.replace(' ', '')}",
                            'away_team': away_team,
                            'away_team_id': f"scw_{away_team.replace(' ', '')}",
                            'competition': competition,
                            'competition_code': f"scw_{competition.replace(' ', '')}",
                            'country': country,
                            'utc_date': match_time.isoformat(),
                            'status': 'SCHEDULED',
                            'publish_time': publish_time.isoformat(),
                            'expire_time': expire_time.isoformat(),
                            'processed': False,
                            'article_generated': False,
                            'source': 'soccerway',
                            'last_updated': datetime.now().isoformat()
                        }
                        
                        matches.append(match_data)
                
                except Exception as e:
                    logging.error(f"Errore parsing Soccerway: {str(e)}")
            
        except Exception as e:
            logging.error(f"Errore scraping Soccerway per {date_str}: {str(e)}")
        
        # Rispetta i limiti per scraping etico
        time.sleep(random.uniform(3, 6))
    
    return matches

def scrape_additional_sources(start_date, end_date):
    """Raccoglie partite da tutte le fonti di scraping configurate"""
    all_matches = []
    
    # Sporting Life
    try:
        logging.info("Avvio scraping da Sporting Life...")
        sl_matches = scrape_sportinglife(start_date, end_date)
        logging.info(f"Raccolte {len(sl_matches)} partite da Sporting Life")
        all_matches.extend(sl_matches)
    except Exception as e:
        logging.error(f"Errore durante scraping Sporting Life: {str(e)}")
    
    # BBC Sport
    try:
        logging.info("Avvio scraping da BBC Sport...")
        bbc_matches = scrape_bbc_sport(start_date, end_date)
        logging.info(f"Raccolte {len(bbc_matches)} partite da BBC Sport")
        all_matches.extend(bbc_matches)
    except Exception as e:
        logging.error(f"Errore durante scraping BBC Sport: {str(e)}")
    
    # Goal.com
    try:
        logging.info("Avvio scraping da Goal.com...")
        goal_matches = scrape_goal_com(start_date, end_date)
        logging.info(f"Raccolte {len(goal_matches)} partite da Goal.com")
        all_matches.extend(goal_matches)
    except Exception as e:
        logging.error(f"Errore durante scraping Goal.com: {str(e)}")
    
    # Soccerway
    try:
        logging.info("Avvio scraping da Soccerway...")
        scw_matches = scrape_soccerway(start_date, end_date)
        logging.info(f"Raccolte {len(scw_matches)} partite da Soccerway")
        all_matches.extend(scw_matches)
    except Exception as e:
        logging.error(f"Errore durante scraping Soccerway: {str(e)}")
    
    return all_matches

def get_matches_from_additional_apis(start_date, end_date):
    """Raccoglie partite da API aggiuntive"""
    all_matches = []
    
    # API-Football (se configurata)
    if RAPID_API_KEY:
        try:
            logging.info("Raccolta partite da API-Football...")
            apif_matches = get_matches_from_api_football(start_date, end_date)
            logging.info(f"Raccolte {len(apif_matches)} partite da API-Football")
            all_matches.extend(apif_matches)
        except Exception as e:
            logging.error(f"Errore durante raccolta da API-Football: {str(e)}")
    
    # OpenFootball Data
    try:
        logging.info("Raccolta partite da OpenFootball Data...")
        of_matches = get_matches_from_openfootball()
        logging.info(f"Raccolte {len(of_matches)} partite da OpenFootball Data")
        all_matches.extend(of_matches)
    except Exception as e:
        logging.error(f"Errore durante raccolta da OpenFootball Data: {str(e)}")
    
    return all_matches

def save_matches_to_firebase(matches):
    """Salva le partite su Firebase"""
    if not matches:
        logging.info("Nessuna partita da salvare")
        return 0, 0
        
    ref = db.reference('matches')
    saved_count = 0
    updated_count = 0
    
    for match in matches:
        match_id = match['id']
        match_date = datetime.fromisoformat(match['utc_date'].replace('Z', '+00:00')).strftime('%Y-%m-%d')
        
        try:
            # Verifica se esiste già
            existing_match = ref.child(match_date).child(match_id).get()
            
            if existing_match:
                # Aggiorna solo timestamp
                ref.child(match_date).child(match_id).update({
                    'last_updated': match['last_updated']
                })
                updated_count += 1
            else:
                # Salva nuova partita
                ref.child(match_date).child(match_id).set(match)
                saved_count += 1
        except Exception as e:
            logging.error(f"Errore salvataggio partita {match_id}: {str(e)}")
    
    return saved_count, updated_count

def remove_duplicates(matches):
    """Rimuove partite duplicate basandosi su squadre, data e competizione"""
    unique_matches = {}
    duplicates_count = 0
    
    for match in matches:
        # Crea una chiave di identificazione
        # Utilizziamo data, squadre e competizione per identificare partite duplicate
        match_time = datetime.fromisoformat(match['utc_date'].replace('Z', '+00:00'))
        match_date = match_time.strftime('%Y-%m-%d')
        match_hour = match_time.hour
        
        key = f"{match_date}_{match_hour}_{match['home_team']}_{match['away_team']}_{match['competition']}"
        
        # Se è un duplicato, diamo priorità alle fonti ufficiali
        if key in unique_matches:
            duplicates_count += 1
            existing_source = unique_matches[key]['source']
            current_source = match['source']
            
            # Priorità alle fonti
            source_priority = {
                'football-data-api': 1,   # Priorità massima
                'api-football': 2,
                'openfootball': 3,
                'bbc_sport': 4,
                'sportinglife': 5,
                'goal_com': 6,
                'soccerway': 7
            }
            
            # Se la nuova fonte ha priorità maggiore (numero più basso), sostituisci
            if source_priority.get(current_source, 999) < source_priority.get(existing_source, 999):
                unique_matches[key] = match
        else:
            unique_matches[key] = match
    
    logging.info(f"Rimosse {duplicates_count} partite duplicate")
    return list(unique_matches.values())

def main():
    """Funzione principale"""
    start_time = datetime.now()
    logging.info(f"Avvio Fetch Matches - {start_time.isoformat()}")
    
    try:
        # 1. Inizializza Firebase
        initialize_firebase()
        
        # 2. Definisci periodo da raccogliere (oggi + 3 giorni)
        today = datetime.now().date()
        end_date = today + timedelta(days=3)
        
        # 3. Ottieni partite dalle diverse fonti
        # 3.1 API ufficiale Football-Data
        api_matches = get_matches_for_date_range(today, end_date)
        logging.info(f"Raccolte {len(api_matches)} partite da Football-Data API")
        
        # 3.2 API aggiuntive
        api_extra_matches = get_matches_from_additional_apis(today, end_date)
        logging.info(f"Raccolte {len(api_extra_matches)} partite da API aggiuntive")
        
        # 3.3 Fonti scraping
        scraped_matches = scrape_additional_sources(today, end_date)
        logging.info(f"Raccolte {len(scraped_matches)} partite da fonti web")
        
        # 3.4 Combina tutte le partite
        all_matches = api_matches + api_extra_matches + scraped_matches
        logging.info(f"Totale partite raccolte: {len(all_matches)}")
        
        # 4. Rimuovi duplicati
        unique_matches = remove_duplicates(all_matches)
        logging.info(f"Partite uniche dopo rimozione duplicati: {len(unique_matches)}")
        
        # 5. Salva su Firebase
        saved, updated = save_matches_to_firebase(unique_matches)
        logging.info(f"Partite salvate: {saved}, aggiornate: {updated}")
        
        # 6. Aggiorna stato health
        health_ref = db.reference('health/fetch_matches')
        health_ref.set({
            'last_run': datetime.now().isoformat(),
            'matches_collected': len(all_matches),
            'matches_unique': len(unique_matches),
            'matches_saved': saved,
            'matches_updated': updated,
            'status': 'success',
            'sources': {
                'football_data_api': len(api_matches),
                'additional_apis': len(api_extra_matches),
                'web_scraping': len(scraped_matches)
            }
        })
        
    except Exception as e:
        logging.error(f"Errore generale: {str(e)}")
        
        # Aggiorna stato health con errore
        try:
            health_ref = db.reference('health/fetch_matches')
            health_ref.set({
                'last_run': datetime.now().isoformat(),
                'status': 'error',
                'error_message': str(e)
            })
        except:
            pass
            
        return 1
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    logging.info(f"Fetch Matches completato in {duration} secondi")
    return 0

if __name__ == "__main__":
    sys.exit(main())
