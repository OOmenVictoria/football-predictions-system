#!/usr/bin/env python3
"""
Fetch Team Stats - Raccoglie e analizza statistiche delle squadre
Elabora dati storici e statistiche per le squadre coinvolte nelle partite programmate
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
log_file = os.path.join(log_dir, f"fetch_team_stats_{datetime.now().strftime('%Y%m%d')}.log")

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

def get_teams_needing_stats():
    """Ottiene le squadre che necessitano di statistiche aggiornate"""
    ref = db.reference('matches')
    teams_to_update = set()
    
    # Ottieni partite nei prossimi 3 giorni
    today = datetime.now().date()
    date_keys = []
    
    for i in range(3):
        date_str = (today + timedelta(days=i)).strftime('%Y-%m-%d')
        date_keys.append(date_str)
    
    # Raccogli ID squadre da queste partite
    for date_key in date_keys:
        matches = ref.child(date_key).get() or {}
        
        for match_id, match_data in matches.items():
            # Aggiungi squadre al set
            if 'home_team_id' in match_data:
                teams_to_update.add(match_data['home_team_id'])
            if 'away_team_id' in match_data:
                teams_to_update.add(match_data['away_team_id'])
    
    # Verifica quali squadre sono già aggiornate
    stats_ref = db.reference('team_stats')
    stats_data = stats_ref.get() or {}
    
    fresh_cutoff = (datetime.now() - timedelta(hours=12)).isoformat()
    teams_to_process = []
    
    for team_id in teams_to_update:
        team_id_str = str(team_id)
        # Aggiungi se non esiste o non è aggiornata recentemente
        if team_id_str not in stats_data or 'last_updated' not in stats_data[team_id_str] or stats_data[team_id_str]['last_updated'] < fresh_cutoff:
            teams_to_process.append(team_id)
    
    logging.info(f"Trovate {len(teams_to_process)} squadre da aggiornare su {len(teams_to_update)} totali")
    return teams_to_process

def get_team_stats(team_id):
    """Raccoglie statistiche per una singola squadra"""
    # 1. Informazioni base squadra
    team_data = make_api_request(f"teams/{team_id}")
    if not team_data:
        logging.error(f"Impossibile ottenere dati per squadra ID {team_id}")
        return None
    
    # 2. Ultime partite
    matches_data = make_api_request(f"teams/{team_id}/matches", {"status": "FINISHED", "limit": 10})
    
    # Calcola statistiche dalle ultime partite
    form = ""
    goals_scored = 0
    goals_conceded = 0
    wins = 0
    draws = 0
    losses = 0
    
    recent_matches = []
    
    if matches_data and 'matches' in matches_data:
        # Ultime 5 partite, dalla più recente
        all_matches = sorted(matches_data['matches'], key=lambda m: m['utcDate'], reverse=True)
        recent_matches = all_matches[:5]
        
        for match in recent_matches:
            is_home = match['homeTeam']['id'] == int(team_id)
            team_goals = match['score']['fullTime']['home'] if is_home else match['score']['fullTime']['away']
            opponent_goals = match['score']['fullTime']['away'] if is_home else match['score']['fullTime']['home']
            
            # Registra goals solo se il punteggio è disponibile
            if team_goals is not None and opponent_goals is not None:
                goals_scored += team_goals
                goals_conceded += opponent_goals
                
                # Aggiorna form
                if team_goals > opponent_goals:
                    form += "W"
                    wins += 1
                elif team_goals < opponent_goals:
                    form += "L"
                    losses += 1
                else:
                    form += "D"
                    draws += 1
    
    # 3. Posizione in classifica (se disponibile)
    league_position = None
    if team_data and 'runningCompetitions' in team_data:
        for comp in team_data['runningCompetitions']:
            if 'code' in comp:
                # Prova a ottenere la classifica
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
    
    # 4. Componi statistiche
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
        # Dati simulati per statistiche avanzate (in una versione reale verrebbero da API)
        'advanced_stats': {
            'possession': random.randint(40, 60),
            'shots_per_game': round(random.uniform(8, 16), 1),
            'shots_on_target': round(random.uniform(3, 7), 1),
            'xG': round(random.uniform(1.0, 2.2), 2),
            'corners_per_game': round(random.uniform(4, 8), 1),
            'fouls_per_game': round(random.uniform(8, 14), 1)
        },
        'last_updated': datetime.now().isoformat()
    }
    
    return stats

def save_team_stats(team_stats):
    """Salva statistiche squadra su Firebase"""
    if not team_stats:
        return False
    
    team_id = team_stats['id']
    ref = db.reference(f'team_stats/{team_id}')
    ref.set(team_stats)
    
    return True

def main():
    """Funzione principale"""
    start_time = datetime.now()
    logging.info(f"Avvio Fetch Team Stats - {start_time.isoformat()}")
    
    try:
        # 1. Inizializza Firebase
        initialize_firebase()
        
        # 2. Ottieni squadre da aggiornare
        teams_to_update = get_teams_needing_stats()
        
        # 3. Limite numero squadre per esecuzione (rispetta limiti API)
        teams_to_update = teams_to_update[:10]  # Max 10 squadre per esecuzione
        
        # 4. Raccogli e salva statistiche
        updated_count = 0
        for team_id in teams_to_update:
            logging.info(f"Elaborazione statistiche per team ID {team_id}")
            
            # Ottieni e salva statistiche
            team_stats = get_team_stats(team_id)
            
            if team_stats:
                save_team_stats(team_stats)
                updated_count += 1
                logging.info(f"Statistiche salvate per {team_stats['name']}")
            else:
                logging.error(f"Impossibile ottenere statistiche per team ID {team_id}")
            
            # Pausa tra richieste
            time.sleep(3)
        
        # 5. Aggiorna stato health
        health_ref = db.reference('health/fetch_team_stats')
        health_ref.set({
            'last_run': datetime.now().isoformat(),
            'teams_processed': len(teams_to_update),
            'teams_updated': updated_count,
            'status': 'success'
        })
        
    except Exception as e:
        logging.error(f"Errore generale: {str(e)}")
        
        # Aggiorna stato health con errore
        try:
            health_ref = db.reference('health/fetch_team_stats')
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
    logging.info(f"Fetch Team Stats completato in {duration} secondi")
    return 0

if __name__ == "__main__":
    sys.exit(main())
