#!/usr/bin/env python3
"""
Fetch Head-to-Head Stats - Raccoglie statistiche sugli scontri diretti tra squadre
Analizza lo storico degli incontri per arricchire i pronostici
"""
import os
import sys
import requests
import json
import logging
import time
import random
from datetime import datetime, timedelta
import firebase_admin
from firebase_admin import credentials, db
from dotenv import load_dotenv

# Configurazione logging
log_dir = os.path.expanduser('~/football-predictions/logs')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"fetch_h2h_stats_{datetime.now().strftime('%Y%m%d')}.log")

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Variabili globali
load_dotenv()
API_KEY = os.getenv('FOOTBALL_API_KEY')
BASE_URL = "https://api.football-data.org/v4"

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
        "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1"
    ]
    return random.choice(user_agents)

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
            time.sleep(2)
            
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

def get_upcoming_matches_without_h2h():
    """Ottiene le prossime partite che necessitano di statistiche h2h"""
    matches_to_process = []
    
    # Riferimento al database
    matches_ref = db.reference('matches')
    h2h_ref = db.reference('h2h')
    
    # Ottieni partite nei prossimi 3 giorni
    today = datetime.now().date()
    
    for i in range(3):
        date_str = (today + timedelta(days=i)).strftime('%Y-%m-%d')
        daily_matches = matches_ref.child(date_str).get() or {}
        
        for match_id, match_data in daily_matches.items():
            # Verifica se già elaborata
            if h2h_ref.child(match_id).get():
                continue
                
            # Verifica se ha gli ID necessari
            if 'home_team_id' in match_data and 'away_team_id' in match_data:
                matches_to_process.append({
                    'id': match_id,
                    'home_team_id': match_data['home_team_id'],
                    'away_team_id': match_data['away_team_id'],
                    'home_team': match_data['home_team'],
                    'away_team': match_data['away_team'],
                    'utc_date': match_data['utc_date']
                })
    
    # Ordina per data (prima le più vicine)
    matches_to_process.sort(key=lambda x: x['utc_date'])
    
    logging.info(f"Trovate {len(matches_to_process)} partite da processare per H2H")
    return matches_to_process

def get_h2h_stats(match):
    """Ottiene statistiche head-to-head per una partita"""
    home_id = match['home_team_id']
    away_id = match['away_team_id']
    
    # Utilizzo dell'endpoint dedicato h2h
    endpoint = f"teams/{home_id}/matches"
    params = {
        "limit": 100,  # Massimo numero di partite
        "status": "FINISHED"
    }
    
    # Ottieni tutte le partite del team di casa
    home_matches = make_api_request(endpoint, params)
    if not home_matches:
        logging.error(f"Impossibile ottenere partite per team {home_id}")
        return None
    
    # Filtra per partite contro il team ospite
    h2h_matches = []
    
    for m in home_matches.get('matches', []):
        if (m['homeTeam']['id'] == int(home_id) and m['awayTeam']['id'] == int(away_id)) or \
           (m['homeTeam']['id'] == int(away_id) and m['awayTeam']['id'] == int(home_id)):
            
            # Aggiungi solo se il punteggio è disponibile
            if m['score']['fullTime']['home'] is not None and m['score']['fullTime']['away'] is not None:
                match_detail = {
                    'id': m['id'],
                    'competition': m['competition']['name'],
                    'utc_date': m['utcDate'],
                    'home_team': m['homeTeam']['name'],
                    'away_team': m['awayTeam']['name'],
                    'score': {
                        'home': m['score']['fullTime']['home'],
                        'away': m['score']['fullTime']['away']
                    },
                    'winner': m['score']['winner']
                }
                h2h_matches.append(match_detail)
    
    # Ordina per data (più recenti prima)
    h2h_matches.sort(key=lambda x: x['utc_date'], reverse=True)
    
    # Limita a max 10 partite
    h2h_matches = h2h_matches[:10]
    
    # Calcola statistiche aggregate
    home_wins = 0
    away_wins = 0
    draws = 0
    home_goals = 0
    away_goals = 0
    
    for m in h2h_matches:
        if m['winner'] == 'HOME_TEAM' and m['home_team'] == match['home_team']:
            home_wins += 1
        elif m['winner'] == 'AWAY_TEAM' and m['away_team'] == match['home_team']:
            home_wins += 1
        elif m['winner'] == 'HOME_TEAM' and m['home_team'] == match['away_team']:
            away_wins += 1
        elif m['winner'] == 'AWAY_TEAM' and m['away_team'] == match['away_team']:
            away_wins += 1
        else:
            draws += 1
        
        # Conta i gol
        if m['home_team'] == match['home_team']:
            home_goals += m['score']['home']
            away_goals += m['score']['away']
        else:
            home_goals += m['score']['away']
            away_goals += m['score']['home']
    
    # Calcola tendenze recenti (ultime 3 partite se disponibili)
    recent_trend = "NEUTRAL"
    if len(h2h_matches) >= 3:
        recent_home_wins = 0
        recent_away_wins = 0
        
        for m in h2h_matches[:3]:
            if m['winner'] == 'HOME_TEAM' and m['home_team'] == match['home_team']:
                recent_home_wins += 1
            elif m['winner'] == 'AWAY_TEAM' and m['away_team'] == match['home_team']:
                recent_home_wins += 1
            elif m['winner'] == 'HOME_TEAM' and m['home_team'] == match['away_team']:
                recent_away_wins += 1
            elif m['winner'] == 'AWAY_TEAM' and m['away_team'] == match['away_team']:
                recent_away_wins += 1
        
        if recent_home_wins >= 2:
            recent_trend = "HOME_ADVANTAGE"
        elif recent_away_wins >= 2:
            recent_trend = "AWAY_ADVANTAGE"
    
    # Componi risultato
    h2h_stats = {
        'match_id': match['id'],
        'home_team': match['home_team'],
        'home_team_id': home_id,
        'away_team': match['away_team'],
        'away_team_id': away_id,
        'total_matches': len(h2h_matches),
        'home_wins': home_wins,
        'away_wins': away_wins,
        'draws': draws,
        'home_goals': home_goals,
        'away_goals': away_goals,
        'recent_trend': recent_trend,
        'matches': h2h_matches,
        'last_updated': datetime.now().isoformat()
    }
    
    return h2h_stats

def save_h2h_stats(h2h_stats):
    """Salva statistiche h2h su Firebase"""
    if not h2h_stats:
        return False
    
    match_id = h2h_stats['match_id']
    ref = db.reference(f'h2h/{match_id}')
    ref.set(h2h_stats)
    
    return True

def main():
    """Funzione principale"""
    start_time = datetime.now()
    logging.info(f"Avvio Fetch H2H Stats - {start_time.isoformat()}")
    
    try:
        # 1. Inizializza Firebase
        initialize_firebase()
        
        # 2. Ottieni partite da elaborare
        matches = get_upcoming_matches_without_h2h()
        
        # 3. Limite numero partite per esecuzione (rispetta limiti API)
        matches_to_process = matches[:5]  # Max 5 partite per esecuzione
        
        # 4. Raccogli e salva statistiche H2H
        processed_count = 0
        for match in matches_to_process:
            logging.info(f"Elaborazione H2H per: {match['home_team']} vs {match['away_team']}")
            
            # Ottieni e salva statistiche
            h2h_stats = get_h2h_stats(match)
            
            if h2h_stats:
                save_h2h_stats(h2h_stats)
                processed_count += 1
                logging.info(f"H2H salvato per match ID {match['id']}")
            else:
                logging.error(f"Impossibile ottenere H2H per match ID {match['id']}")
            
            # Pausa tra richieste
            time.sleep(3)
        
        # 5. Aggiorna stato health
        health_ref = db.reference('health/fetch_h2h_stats')
        health_ref.set({
            'last_run': datetime.now().isoformat(),
            'matches_processed': processed_count,
            'pending_matches': len(matches) - processed_count,
            'status': 'success'
        })
        
    except Exception as e:
        logging.error(f"Errore generale: {str(e)}")
        
        # Aggiorna stato health con errore
        try:
            health_ref = db.reference('health/fetch_h2h_stats')
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
    logging.info(f"Fetch H2H Stats completato in {duration} secondi")
    return 0

if __name__ == "__main__":
    sys.exit(main())
