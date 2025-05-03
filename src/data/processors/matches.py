"""
Processore per i dati delle partite.

Questo modulo standardizza e arricchisce i dati delle partite provenienti da diverse fonti,
applicando pulizia, normalizzazione e aggregazione per garantire dati coerenti.
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union, Tuple

# API ufficiali
from src.data.api.football_data import get_api as get_football_data_api
from src.data.api.api_football import get_api as get_api_football

# Scraper per siti con statistiche
from src.data.scrapers.flashscore import get_scraper as get_flashscore
from src.data.stats.fbref import get_scraper as get_fbref
from src.data.stats.understat import get_scraper as get_understat
from src.data.stats.sofascore import get_scraper as get_sofascore
from src.data.stats.footystats import get_scraper as get_footystats
from src.data.stats.whoscored import get_scraper as get_whoscored

# Scraper per siti web generici
from src.data.scrapers.soccerway import get_scraper as get_soccerway
from src.data.scrapers.worldfootball import get_scraper as get_worldfootball
from src.data.scrapers.transfermarkt import get_scraper as get_transfermarkt
from src.data.scrapers.wikipedia import get_scraper as get_wikipedia
from src.data.scrapers.eleven_v_eleven import get_scraper as get_eleven_v_eleven

# Open data e archivi storici
from src.data.open_data.open_football import get_loader as get_open_football
from src.data.open_data.rsssf import get_loader as get_rsssf
from src.data.open_data.kaggle_loader import get_loader as get_kaggle
from src.data.open_data.statsbomb import get_loader as get_statsbomb

from src.utils.database import FirebaseManager
from src.utils.time_utils import parse_date, format_date, get_match_status
from src.config.sources import get_sources_for_data_type, get_source_priority

# Configurazione logger
logger = logging.getLogger(__name__)

class MatchProcessor:
    """
    Processore per normalizzare e arricchire i dati delle partite.
    
    Gestisce la standardizzazione dei dati da diverse fonti, la risoluzione dei conflitti,
    e l'arricchimento con dati aggiuntivi come pronostici e statistiche.
    """
    
    def __init__(self, db: Optional[FirebaseManager] = None):
        """
        Inizializza il processore di partite.
        
        Args:
            db: Istanza di FirebaseManager. Se None, ne verrà creata una nuova.
        """
        self.db = db or FirebaseManager()
        
        # API ufficiali
        self.football_data_api = get_football_data_api()
        self.api_football = get_api_football()
        
        # Scraper per statistiche avanzate
        self.flashscore = get_flashscore()
        self.fbref = get_fbref()
        self.understat = get_understat()
        self.sofascore = get_sofascore()
        self.footystats = get_footystats()
        self.whoscored = get_whoscored()
        
        # Altri scraper
        self.soccerway = get_soccerway()
        self.worldfootball = get_worldfootball()
        self.transfermarkt = get_transfermarkt()
        self.wikipedia = get_wikipedia()
        self.eleven_v_eleven = get_eleven_v_eleven()
        
        # Open data
        self.open_football = get_open_football()
        self.rsssf = get_rsssf()
        self.kaggle = get_kaggle()
        self.statsbomb = get_statsbomb()
        
        # Cache per mappare nomi squadre
        self.team_name_cache = {}
    
    
    def process_match(
        self, 
        match_data: Dict[str, Any], 
        source: str,
        league_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Processa i dati di una singola partita standardizzandoli.
        
        Args:
            match_data: Dati grezzi della partita.
            source: Nome della fonte dei dati ('football_data', 'api_football', 'flashscore', etc.).
            league_id: ID del campionato (opzionale).
        
        Returns:
            Dizionario con i dati della partita normalizzati.
        """
        try:
            # Oggetto partita standardizzato
            standardized_match = {
                'match_id': '',
                'source': source,
                'source_ids': {},
                'home_team': {
                    'id': '',
                    'name': '',
                    'short_name': ''
                },
                'away_team': {
                    'id': '',
                    'name': '',
                    'short_name': ''
                },
                'competition': {
                    'id': league_id or '',
                    'name': ''
                },
                'datetime': '',
                'status': '',
                'score': {
                    'home': None,
                    'away': None,
                    'winner': None
                },
                'venue': {
                    'name': '',
                    'city': ''
                },
                'statistics': {},
                'lineups': {},
                'events': [],
                'odds': {},
                'last_updated': datetime.now().isoformat()
            }
            
            # Normalizzazione in base alla fonte
            if source == 'football_data':
                self._normalize_football_data(match_data, standardized_match)
            elif source == 'api_football':
                self._normalize_api_football(match_data, standardized_match)
            elif source == 'flashscore':
                self._normalize_flashscore(match_data, standardized_match)
            elif source == 'fbref':
                self._normalize_fbref(match_data, standardized_match)
            elif source == 'understat':
                self._normalize_understat(match_data, standardized_match)
            elif source == 'sofascore':
                self._normalize_sofascore(match_data, standardized_match)
            elif source == 'footystats':
                self._normalize_footystats(match_data, standardized_match)
            elif source == 'whoscored':
                self._normalize_whoscored(match_data, standardized_match)
            elif source == 'soccerway':
                self._normalize_soccerway(match_data, standardized_match)
            elif source == 'worldfootball':
                self._normalize_worldfootball(match_data, standardized_match)
            elif source == 'transfermarkt':
                self._normalize_transfermarkt(match_data, standardized_match)
            elif source == 'open_football':
                self._normalize_open_football(match_data, standardized_match)
            elif source == 'statsbomb':
                self._normalize_statsbomb(match_data, standardized_match)
            else:
                logger.warning(f"Fonte non supportata: {source}")
            
            # Genera ID interno se non presente
            if not standardized_match['match_id']:
                # Crea ID basato su squadre e data
                home_team = standardized_match['home_team']['name'].replace(' ', '_')
                away_team = standardized_match['away_team']['name'].replace(' ', '_')
                date_str = standardized_match['datetime'].split('T')[0] if 'T' in standardized_match['datetime'] else ''
                standardized_match['match_id'] = f"{home_team}_vs_{away_team}_{date_str}"
            
            # Aggiunta ID della fonte nei source_ids
            if source == 'football_data' and 'id' in match_data:
                standardized_match['source_ids']['football_data'] = str(match_data['id'])
            elif source == 'api_football' and 'fixture' in match_data and 'id' in match_data['fixture']:
                standardized_match['source_ids']['api_football'] = str(match_data['fixture']['id'])
            elif source == 'flashscore' and 'id' in match_data:
                standardized_match['source_ids']['flashscore'] = str(match_data['id'])
            elif source == 'fbref' and 'id' in match_data:
                standardized_match['source_ids']['fbref'] = str(match_data['id'])
            elif source == 'understat' and 'id' in match_data:
                standardized_match['source_ids']['understat'] = str(match_data['id'])
            elif source == 'sofascore' and 'id' in match_data:
                standardized_match['source_ids']['sofascore'] = str(match_data['id'])
            elif source == 'transfermarkt' and 'id' in match_data:
                standardized_match['source_ids']['transfermarkt'] = str(match_data['id'])
            
            return standardized_match
            
        except Exception as e:
            logger.error(f"Errore nel processare la partita da {source}: {e}")
            return {
                'match_id': '',
                'source': source,
                'error': str(e),
                'raw_data': match_data
            }
    
    def _normalize_football_data(
        self, 
        match_data: Dict[str, Any], 
        output: Dict[str, Any]
    ) -> None:
        """
        Normalizza i dati da Football-Data API.
        
        Args:
            match_data: Dati grezzi della partita da Football-Data.
            output: Dizionario di output da popolare.
        """
        try:
            # ID partita
            if 'id' in match_data:
                output['match_id'] = str(match_data['id'])
                output['source_ids']['football_data'] = str(match_data['id'])
            
            # Squadre
            if 'homeTeam' in match_data and match_data['homeTeam']:
                output['home_team']['id'] = str(match_data['homeTeam'].get('id', ''))
                output['home_team']['name'] = match_data['homeTeam'].get('name', '')
                output['home_team']['short_name'] = match_data['homeTeam'].get('shortName', output['home_team']['name'])
            
            if 'awayTeam' in match_data and match_data['awayTeam']:
                output['away_team']['id'] = str(match_data['awayTeam'].get('id', ''))
                output['away_team']['name'] = match_data['awayTeam'].get('name', '')
                output['away_team']['short_name'] = match_data['awayTeam'].get('shortName', output['away_team']['name'])
            
            # Competizione
            if 'competition' in match_data and match_data['competition']:
                output['competition']['id'] = str(match_data['competition'].get('id', ''))
                output['competition']['name'] = match_data['competition'].get('name', '')
            
            # Data e ora
            if 'utcDate' in match_data:
                output['datetime'] = match_data['utcDate']
            
            # Stato partita
            if 'status' in match_data:
                output['status'] = self._map_football_data_status(match_data['status'])
            
            # Punteggio
            if 'score' in match_data and match_data['score']:
                if 'fullTime' in match_data['score'] and match_data['score']['fullTime']:
                    home_score = match_data['score']['fullTime'].get('home')
                    away_score = match_data['score']['fullTime'].get('away')
                    
                    if home_score is not None and away_score is not None:
                        output['score']['home'] = home_score
                        output['score']['away'] = away_score
                        
                        # Determina vincitore
                        if home_score > away_score:
                            output['score']['winner'] = 'home'
                        elif away_score > home_score:
                            output['score']['winner'] = 'away'
                        else:
                            output['score']['winner'] = 'draw'
            
            # Stadio
            if 'venue' in match_data:
                output['venue']['name'] = match_data.get('venue', '')
            
            # Arbitri
            if 'referees' in match_data and match_data['referees']:
                output['referees'] = [
                    {
                        'name': ref.get('name', ''),
                        'role': ref.get('role', '')
                    }
                    for ref in match_data['referees']
                ]
            
        except Exception as e:
            logger.error(f"Errore nella normalizzazione dei dati Football-Data: {e}")
    
    def _normalize_api_football(
        self, 
        match_data: Dict[str, Any], 
        output: Dict[str, Any]
    ) -> None:
        """
        Normalizza i dati da API-Football.
        
        Args:
            match_data: Dati grezzi della partita da API-Football.
            output: Dizionario di output da popolare.
        """
        try:
            # ID partita
            if 'fixture' in match_data and 'id' in match_data['fixture']:
                output['match_id'] = str(match_data['fixture']['id'])
                output['source_ids']['api_football'] = str(match_data['fixture']['id'])
            
            # Squadre
            if 'teams' in match_data:
                if 'home' in match_data['teams'] and match_data['teams']['home']:
                    output['home_team']['id'] = str(match_data['teams']['home'].get('id', ''))
                    output['home_team']['name'] = match_data['teams']['home'].get('name', '')
                
                if 'away' in match_data['teams'] and match_data['teams']['away']:
                    output['away_team']['id'] = str(match_data['teams']['away'].get('id', ''))
                    output['away_team']['name'] = match_data['teams']['away'].get('name', '')
            
            # Competizione
            if 'league' in match_data and match_data['league']:
                output['competition']['id'] = str(match_data['league'].get('id', ''))
                output['competition']['name'] = match_data['league'].get('name', '')
            
            # Data e ora
            if 'fixture' in match_data and 'date' in match_data['fixture']:
                output['datetime'] = match_data['fixture']['date']
            
            # Stato partita
            if 'fixture' in match_data and 'status' in match_data['fixture']:
                status = match_data['fixture']['status']
                if 'short' in status:
                    output['status'] = self._map_api_football_status(status['short'])
            
            # Punteggio
            if 'goals' in match_data:
                home_score = match_data['goals'].get('home')
                away_score = match_data['goals'].get('away')
                
                if home_score is not None and away_score is not None:
                    output['score']['home'] = home_score
                    output['score']['away'] = away_score
                    
                    # Determina vincitore
                    if home_score > away_score:
                        output['score']['winner'] = 'home'
                    elif away_score > home_score:
                        output['score']['winner'] = 'away'
                    else:
                        output['score']['winner'] = 'draw'
            
            # Stadio
            if 'fixture' in match_data and 'venue' in match_data['fixture']:
                venue = match_data['fixture']['venue']
                output['venue']['name'] = venue.get('name', '')
                output['venue']['city'] = venue.get('city', '')
            
            # Statistiche (se disponibili)
            if 'statistics' in match_data and match_data['statistics']:
                stats = {}
                for team_stats in match_data['statistics']:
                    team_id = team_stats.get('team', {}).get('id')
                    if team_id:
                        stats[str(team_id)] = team_stats.get('statistics', [])
                
                if stats:
                    output['statistics'] = stats
            
            # Formazioni (se disponibili)
            if 'lineups' in match_data and match_data['lineups']:
                lineups = {}
                for team_lineup in match_data['lineups']:
                    team_id = team_lineup.get('team', {}).get('id')
                    if team_id:
                        lineups[str(team_id)] = team_lineup
                
                if lineups:
                    output['lineups'] = lineups
            
            # Eventi (se disponibili)
            if 'events' in match_data and match_data['events']:
                output['events'] = match_data['events']
            
        except Exception as e:
            logger.error(f"Errore nella normalizzazione dei dati API-Football: {e}")
    
    def _normalize_flashscore(
        self, 
        match_data: Dict[str, Any], 
        output: Dict[str, Any]
    ) -> None:
        """
        Normalizza i dati da Flashscore.
        
        Args:
            match_data: Dati grezzi della partita da Flashscore.
            output: Dizionario di output da popolare.
        """
        try:
            # ID partita
            if 'id' in match_data:
                output['match_id'] = match_data['id']
                output['source_ids']['flashscore'] = match_data['id']
            
            # Squadre
            if 'home_team' in match_data:
                output['home_team']['name'] = match_data['home_team']
            
            if 'away_team' in match_data:
                output['away_team']['name'] = match_data['away_team']
            
            # Competizione
            if 'tournament' in match_data:
                output['competition']['name'] = match_data['tournament']
            
            # Data e ora
            date_str = match_data.get('date', '')
            time_str = match_data.get('time', '')
            
            if date_str:
                try:
                    if 'datetime' in match_data:
                        output['datetime'] = match_data['datetime']
                    elif time_str:
                        # Combina data e ora
                        dt = parse_date(f"{date_str} {time_str}")
                        output['datetime'] = dt.isoformat() if dt else ''
                    else:
                        dt = parse_date(date_str)
                        output['datetime'] = dt.isoformat() if dt else ''
                except:
                    output['datetime'] = date_str
            
            # Stato partita
            if 'status' in match_data:
                output['status'] = self._map_flashscore_status(match_data['status'])
            
            # Punteggio
            if 'score' in match_data:
                home_score = match_data['score'].get('home')
                away_score = match_data['score'].get('away')
                
                if home_score is not None and away_score is not None:
                    try:
                        home_score_int = int(home_score)
                        away_score_int = int(away_score)
                        
                        output['score']['home'] = home_score_int
                        output['score']['away'] = away_score_int
                        
                        # Determina vincitore
                        if home_score_int > away_score_int:
                            output['score']['winner'] = 'home'
                        elif away_score_int > home_score_int:
                            output['score']['winner'] = 'away'
                        else:
                            output['score']['winner'] = 'draw'
                    except:
                        # Se non può essere convertito in intero, usa i valori originali
                        output['score']['home'] = home_score
                        output['score']['away'] = away_score
            
            # Statistiche (se disponibili)
            if 'statistics' in match_data and match_data['statistics']:
                output['statistics'] = match_data['statistics']
            
            # Formazioni (se disponibili)
            if 'lineups' in match_data and match_data['lineups']:
                output['lineups'] = match_data['lineups']
            
            # Eventi (gol, cartellini, ecc.)
            if 'events' in match_data and match_data['events']:
                output['events'] = match_data['events']
            
            # Quote (se disponibili)
            if 'odds' in match_data and match_data['odds']:
                output['odds'] = match_data['odds']
            
        except Exception as e:
            logger.error(f"Errore nella normalizzazione dei dati Flashscore: {e}")
    
    def _map_football_data_status(self, status: str) -> str:
        """
        Mappa lo stato della partita da Football-Data a formato standard.
        
        Args:
            status: Stato della partita nel formato Football-Data.
        
        Returns:
            Stato della partita in formato standard.
        """
        status_map = {
            'SCHEDULED': 'scheduled',
            'TIMED': 'scheduled',
            'IN_PLAY': 'in_progress',
            'PAUSED': 'in_progress',
            'FINISHED': 'finished',
            'SUSPENDED': 'suspended',
            'POSTPONED': 'postponed',
            'CANCELLED': 'cancelled',
            'AWARDED': 'awarded'
        }
        
        return status_map.get(status, 'unknown')
    
    def _map_api_football_status(self, status: str) -> str:
        """
        Mappa lo stato della partita da API-Football a formato standard.
        
        Args:
            status: Stato della partita nel formato API-Football.
        
        Returns:
            Stato della partita in formato standard.
        """
        status_map = {
            'NS': 'scheduled',  # Not Started
            'TBD': 'scheduled',  # To Be Defined
            '1H': 'in_progress',  # First Half
            'HT': 'in_progress',  # Half Time
            '2H': 'in_progress',  # Second Half
            'ET': 'in_progress',  # Extra Time
            'P': 'in_progress',   # Penalty
            'FT': 'finished',     # Full Time
            'AET': 'finished',    # After Extra Time
            'PEN': 'finished',    # Penalty Shootout
            'BT': 'break',        # Break Time
            'SUSP': 'suspended',  # Suspended
            'INT': 'interrupted', # Interrupted
            'PST': 'postponed',   # Postponed
            'CANC': 'cancelled',  # Cancelled
            'ABD': 'abandoned',   # Abandoned
            'AWD': 'awarded',     # Technical Loss
            'WO': 'walkover'      # Walkover
        }
        
        return status_map.get(status, 'unknown')
    
    def _map_flashscore_status(self, status: str) -> str:
        """
        Mappa lo stato della partita da Flashscore a formato standard.
        
        Args:
            status: Stato della partita nel formato Flashscore.
        
        Returns:
            Stato della partita in formato standard.
        """
        status = status.lower()
        
        if 'finished' in status or 'ft' in status or 'aet' in status or 'pen' in status:
            return 'finished'
        elif any(x in status for x in ['half', '1h', '2h', 'ht']):
            return 'in_progress'
        elif 'postponed' in status:
            return 'postponed'
        elif 'cancelled' in status:
            return 'cancelled'
        elif 'suspended' in status:
            return 'suspended'
        elif 'abandoned' in status:
            return 'abandoned'
        elif any(x in status for x in ['scheduled', 'not started']):
            return 'scheduled'
        
        return 'unknown'
    
    def merge_match_data(
        self, 
        match_id: str, 
        sources: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Unisce i dati della partita da diverse fonti.
        
        Args:
            match_id: ID interno della partita.
            sources: Lista di dizionari con dati della partita da diverse fonti.
        
        Returns:
            Dizionario con i dati uniti della partita.
        """
        if not sources:
            logger.warning(f"Nessuna fonte dati fornita per la partita {match_id}")
            return {'match_id': match_id, 'error': 'No data sources provided'}
        
        # Usa i dati del primo dizionario come base
        merged_data = sources[0].copy()
        
        # Se c'è solo una fonte, restituisci i dati così come sono
        if len(sources) == 1:
            return merged_data
        
        # Altrimenti, unisci i dati dalle altre fonti
        for source_data in sources[1:]:
            self._merge_match_fields(merged_data, source_data)
        
        # Aggiorna timestamp
        merged_data['last_updated'] = datetime.now().isoformat()
        
        return merged_data
    
    def _merge_match_fields(
        self, 
        target: Dict[str, Any], 
        source: Dict[str, Any]
    ) -> None:
        """
        Unisce i campi di una seconda fonte nel dizionario target.
        
        Args:
            target: Dizionario target dove unire i dati.
            source: Dizionario sorgente con i dati da unire.
        """
        # IDs della fonte
        if 'source_ids' in source and source['source_ids']:
            for source_key, source_id in source['source_ids'].items():
                if source_key not in target['source_ids'] or not target['source_ids'][source_key]:
                    target['source_ids'][source_key] = source_id
        
        # Dati squadra
        for team_key in ['home_team', 'away_team']:
            if team_key in source and source[team_key]:
                for field, value in source[team_key].items():
                    if not target[team_key][field] and value:
                        target[team_key][field] = value
        
        # Competizione
        if 'competition' in source and source['competition']:
            for field, value in source['competition'].items():
                if not target['competition'][field] and value:
                    target['competition'][field] = value
        
        # Datetime (preferisci valori in formato ISO)
        if 'datetime' in source and source['datetime'] and not target['datetime']:
            target['datetime'] = source['datetime']
        
        # Stato (preferisci valori più specifici)
        if 'status' in source and source['status'] and (not target['status'] or target['status'] == 'unknown'):
            target['status'] = source['status']
        
        # Punteggio
        if 'score' in source and source['score']:
            if source['score']['home'] is not None and target['score']['home'] is None:
                target['score']['home'] = source['score']['home']
            
            if source['score']['away'] is not None and target['score']['away'] is None:
                target['score']['away'] = source['score']['away']
            
            if source['score']['winner'] and not target['score']['winner']:
                target['score']['winner'] = source['score']['winner']
        
        # Stadio
        if 'venue' in source and source['venue']:
            for field, value in source['venue'].items():
                if not target['venue'][field] and value:
                    target['venue'][field] = value
        
        # Statistiche (unisci se le chiavi sono diverse)
        if 'statistics' in source and source['statistics'] and isinstance(source['statistics'], dict):
            if not target.get('statistics'):
                target['statistics'] = {}
            
            for team_id, stats in source['statistics'].items():
                if team_id not in target['statistics']:
                    target['statistics'][team_id] = stats
                else:
                    for stat_key, stat_value in stats.items():
                        if stat_key not in target['statistics'][team_id]:
                            target['statistics'][team_id][stat_key] = stat_value
        
        # Formazioni
        if 'lineups' in source and source['lineups'] and not target.get('lineups'):
            target['lineups'] = source['lineups']
        
        # Eventi
        if 'events' in source and source['events'] and not target.get('events'):
            target['events'] = source['events']
        
        # Quote
        if 'odds' in source and source['odds'] and not target.get('odds'):
            target['odds'] = source['odds']
    
    def _get_expected_goals_data(self, match_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Ottiene dati Expected Goals (xG) per la partita.
        
        Args:
            match_data: Dati della partita.
        
        Returns:
            Dizionario con dati xG della partita.
        """
        xg_data = {'home': 0.0, 'away': 0.0, 'home_shots': [], 'away_shots': []}
        
        # Prova prima con Understat
        if 'understat' in match_data['source_ids']:
            try:
                source_id = match_data['source_ids']['understat']
                understat_xg = self.understat.get_match_xg(source_id)
                
                if understat_xg:
                    xg_data['home'] = understat_xg.get('home_xg', 0.0)
                    xg_data['away'] = understat_xg.get('away_xg', 0.0)
                    xg_data['home_shots'] = understat_xg.get('home_shots', [])
                    xg_data['away_shots'] = understat_xg.get('away_shots', [])
                    xg_data['source'] = 'understat'
                    return xg_data
            except Exception as e:
                logger.debug(f"Errore nell'ottenere dati xG da Understat: {e}")
        
        # Poi prova con FBref
        if 'fbref' in match_data['source_ids']:
            try:
                source_id = match_data['source_ids']['fbref']
                fbref_xg = self.fbref.get_match_xg(source_id)
                
                if fbref_xg:
                    xg_data['home'] = fbref_xg.get('home_xg', 0.0)
                    xg_data['away'] = fbref_xg.get('away_xg', 0.0)
                    xg_data['source'] = 'fbref'
                    return xg_data
            except Exception as e:
                logger.debug(f"Errore nell'ottenere dati xG da FBref: {e}")
        
        # Infine prova con SofaScore
        if 'sofascore' in match_data['source_ids']:
            try:
                source_id = match_data['source_ids']['sofascore']
                sofascore_xg = self.sofascore.get_match_xg(source_id)
                
                if sofascore_xg:
                    xg_data['home'] = sofascore_xg.get('home_xg', 0.0)
                    xg_data['away'] = sofascore_xg.get('away_xg', 0.0)
                    xg_data['source'] = 'sofascore'
                    return xg_data
            except Exception as e:
                logger.debug(f"Errore nell'ottenere dati xG da SofaScore: {e}")
        
        # Se non abbiamo dati xG da nessuna fonte, proviamo a calcolarli dai tiri in porta
        if 'statistics' in match_data and match_data['statistics']:
            try:
                stats = match_data['statistics']
                if isinstance(stats, dict):
                    # Calcolo xG semplificato basato sui tiri in porta (molto approssimativo)
                    home_team_id = match_data['home_team']['id']
                    away_team_id = match_data['away_team']['id']
                    
                    # Cerca statistiche per ID squadra
                    home_stats = stats.get(home_team_id, stats.get(str(home_team_id), {}))
                    away_stats = stats.get(away_team_id, stats.get(str(away_team_id), {}))
                    
                    if home_stats and away_stats:
                        # SofaScore format
                        if 'xG' in home_stats and 'xG' in away_stats:
                            xg_data['home'] = float(home_stats['xG'])
                            xg_data['away'] = float(away_stats['xG'])
                            xg_data['source'] = 'statistics'
                            return xg_data
                        
                        # API-Football format
                        for home_stat in home_stats:
                            if isinstance(home_stat, dict) and home_stat.get('type') == 'Expected goals (xG)':
                                try:
                                    xg_data['home'] = float(home_stat.get('value', 0))
                                except:
                                    pass
                        
                        for away_stat in away_stats:
                            if isinstance(away_stat, dict) and away_stat.get('type') == 'Expected goals (xG)':
                                try:
                                    xg_data['away'] = float(away_stat.get('value', 0))
                                except:
                                    pass
                        
                        if xg_data['home'] > 0 or xg_data['away'] > 0:
                            xg_data['source'] = 'statistics'
                            return xg_data
            except Exception as e:
                logger.debug(f"Errore nel calcolo xG dalle statistiche: {e}")
        
        return None
    
    def _normalize_fbref(self, match_data: Dict[str, Any], output: Dict[str, Any]) -> None:
        """
        Normalizza i dati da FBref.
        
        Args:
            match_data: Dati grezzi della partita da FBref.
            output: Dizionario di output da popolare.
        """
        try:
            # ID partita
            if 'match_id' in match_data:
                output['match_id'] = str(match_data['match_id'])
                output['source_ids']['fbref'] = str(match_data['match_id'])
            
            # Squadre
            if 'home_team' in match_data:
                output['home_team']['name'] = match_data['home_team'].get('name', '')
                output['home_team']['id'] = match_data['home_team'].get('id', '')
            
            if 'away_team' in match_data:
                output['away_team']['name'] = match_data['away_team'].get('name', '')
                output['away_team']['id'] = match_data['away_team'].get('id', '')
            
            # Competizione
            if 'competition' in match_data:
                output['competition']['name'] = match_data['competition'].get('name', '')
                output['competition']['id'] = match_data['competition'].get('id', '')
            
            # Data e ora
            if 'date' in match_data:
                try:
                    # FBref spesso usa formato "2023-10-21"
                    date_str = match_data['date']
                    time_str = match_data.get('time', '15:00')  # Orario predefinito
                    dt = parse_date(f"{date_str} {time_str}")
                    output['datetime'] = dt.isoformat() if dt else date_str
                except:
                    output['datetime'] = match_data['date']
            
            # Stato partita
            if 'status' in match_data:
                output['status'] = self._map_fbref_status(match_data['status'])
            
            # Punteggio
            if 'score' in match_data:
                try:
                    home_score = match_data['score'].get('home', 0)
                    away_score = match_data['score'].get('away', 0)
                    
                    output['score']['home'] = int(home_score)
                    output['score']['away'] = int(away_score)
                    
                    # Determina vincitore
                    if output['score']['home'] > output['score']['away']:
                        output['score']['winner'] = 'home'
                    elif output['score']['home'] < output['score']['away']:
                        output['score']['winner'] = 'away'
                    else:
                        output['score']['winner'] = 'draw'
                except:
                    pass
            
            # Stadio
            if 'venue' in match_data:
                output['venue']['name'] = match_data['venue']
            
            # Statistiche (se disponibili)
            if 'statistics' in match_data and match_data['statistics']:
                output['statistics'] = match_data['statistics']
            
            # xG (se disponibile)
            if 'xg' in match_data:
                output['xg_stats'] = {
                    'home': match_data['xg'].get('home', 0.0),
                    'away': match_data['xg'].get('away', 0.0),
                    'source': 'fbref'
                }
            
        except Exception as e:
            logger.error(f"Errore nella normalizzazione dei dati FBref: {e}")
    
    def _normalize_understat(self, match_data: Dict[str, Any], output: Dict[str, Any]) -> None:
        """
        Normalizza i dati da Understat.
        
        Args:
            match_data: Dati grezzi della partita da Understat.
            output: Dizionario di output da popolare.
        """
        try:
            # ID partita
            if 'id' in match_data:
                output['match_id'] = str(match_data['id'])
                output['source_ids']['understat'] = str(match_data['id'])
            
            # Squadre
            if 'h' in match_data:  # home
                output['home_team']['name'] = match_data['h'].get('title', '')
                output['home_team']['id'] = str(match_data['h'].get('id', ''))
            
            if 'a' in match_data:  # away
                output['away_team']['name'] = match_data['a'].get('title', '')
                output['away_team']['id'] = str(match_data['a'].get('id', ''))
            
            # Data e ora
            if 'datetime' in match_data:
                output['datetime'] = match_data['datetime']
            
            # Stato partita (Understat usa valore numerico)
            if 'isResult' in match_data:
                output['status'] = 'finished' if match_data['isResult'] else 'scheduled'
            
            # Punteggio
            try:
                if 'goals' in match_data:
                    home_score = match_data['goals'].get('h', 0)
                    away_score = match_data['goals'].get('a', 0)
                    
                    output['score']['home'] = int(home_score)
                    output['score']['away'] = int(away_score)
                    
                    # Determina vincitore
                    if output['score']['home'] > output['score']['away']:
                        output['score']['winner'] = 'home'
                    elif output['score']['home'] < output['score']['away']:
                        output['score']['winner'] = 'away'
                    else:
                        output['score']['winner'] = 'draw'
            except:
                pass
            
            # League
            if 'league' in match_data:
                output['competition']['name'] = match_data['league'].get('name', '')
                output['competition']['id'] = str(match_data['league'].get('id', ''))
            
            # xG statistiche
            if 'xG' in match_data:
                output['xg_stats'] = {
                    'home': match_data['xG'].get('h', 0.0),
                    'away': match_data['xG'].get('a', 0.0),
                    'source': 'understat'
                }
            
            # Tiri se disponibili
            if 'shots' in match_data:
                home_shots = []
                away_shots = []
                
                for shot in match_data['shots']:
                    shot_data = {
                        'minute': shot.get('minute', 0),
                        'player': shot.get('player', ''),
                        'xG': shot.get('xG', 0.0),
                        'position': {'x': shot.get('X', 0.0), 'y': shot.get('Y', 0.0)},
                        'result': shot.get('result', '')
                    }
                    
                    if shot.get('h_a') == 'h':
                        home_shots.append(shot_data)
                    else:
                        away_shots.append(shot_data)
                
                if 'xg_stats' not in output:
                    output['xg_stats'] = {'source': 'understat'}
                    
                output['xg_stats']['home_shots'] = home_shots
                output['xg_stats']['away_shots'] = away_shots
            
        except Exception as e:
            logger.error(f"Errore nella normalizzazione dei dati Understat: {e}")
    
    def _normalize_sofascore(self, match_data: Dict[str, Any], output: Dict[str, Any]) -> None:
        """
        Normalizza i dati da SofaScore.
        
        Args:
            match_data: Dati grezzi della partita da SofaScore.
            output: Dizionario di output da popolare.
        """
        try:
            # ID partita
            if 'id' in match_data:
                output['match_id'] = str(match_data['id'])
                output['source_ids']['sofascore'] = str(match_data['id'])
            
            # Squadre
            if 'homeTeam' in match_data:
                output['home_team']['name'] = match_data['homeTeam'].get('name', '')
                output['home_team']['id'] = str(match_data['homeTeam'].get('id', ''))
                output['home_team']['short_name'] = match_data['homeTeam'].get('shortName', output['home_team']['name'])
            
            if 'awayTeam' in match_data:
                output['away_team']['name'] = match_data['awayTeam'].get('name', '')
                output['away_team']['id'] = str(match_data['awayTeam'].get('id', ''))
                output['away_team']['short_name'] = match_data['awayTeam'].get('shortName', output['away_team']['name'])
            
            # Competizione
            if 'tournament' in match_data:
                output['competition']['name'] = match_data['tournament'].get('name', '')
                output['competition']['id'] = str(match_data['tournament'].get('id', ''))
            
            # Data e ora
            if 'startTimestamp' in match_data:
                try:
                    dt = datetime.fromtimestamp(match_data['startTimestamp'])
                    output['datetime'] = dt.isoformat()
                except:
                    pass
            
            # Stato partita
            if 'status' in match_data:
                output['status'] = self._map_sofascore_status(match_data['status'].get('type', ''))
            
            # Punteggio
            if 'homeScore' in match_data and 'awayScore' in match_data:
                try:
                    output['score']['home'] = int(match_data['homeScore'].get('current', 0))
                    output['score']['away'] = int(match_data['awayScore'].get('current', 0))
                    
                    # Determina vincitore
                    if output['score']['home'] > output['score']['away']:
                        output['score']['winner'] = 'home'
                    elif output['score']['home'] < output['score']['away']:
                        output['score']['winner'] = 'away'
                    else:
                        output['score']['winner'] = 'draw'
                except:
                    pass
            
            # Stadio
            if 'venue' in match_data:
                output['venue']['name'] = match_data['venue'].get('stadium', {}).get('name', '')
                output['venue']['city'] = match_data['venue'].get('city', {}).get('name', '')
            
            # Statistiche (se disponibili)
            if 'statistics' in match_data and match_data['statistics']:
                output['statistics'] = match_data['statistics']
            
            # Eventi (se disponibili)
            if 'events' in match_data and match_data['events']:
                output['events'] = match_data['events']
            
            # xG (se disponibile)
            if 'xg' in match_data:
                output['xg_stats'] = {
                    'home': match_data['xg'].get('home', 0.0),
                    'away': match_data['xg'].get('away', 0.0),
                    'source': 'sofascore'
                }
            
        except Exception as e:
            logger.error(f"Errore nella normalizzazione dei dati SofaScore: {e}")
    
    def _normalize_footystats(self, match_data: Dict[str, Any], output: Dict[str, Any]) -> None:
        """
        Normalizza i dati da FootyStats.
        
        Args:
            match_data: Dati grezzi della partita da FootyStats.
            output: Dizionario di output da popolare.
        """
        try:
            # ID partita
            if 'id' in match_data:
                output['match_id'] = str(match_data['id'])
                output['source_ids']['footystats'] = str(match_data['id'])
            
            # Squadre
            if 'home_team' in match_data:
                output['home_team']['name'] = match_data['home_team'].get('name', '')
                output['home_team']['id'] = str(match_data['home_team'].get('id', ''))
            
            if 'away_team' in match_data:
                output['away_team']['name'] = match_data['away_team'].get('name', '')
                output['away_team']['id'] = str(match_data['away_team'].get('id', ''))
            
            # Competizione
            if 'league' in match_data:
                output['competition']['name'] = match_data['league'].get('name', '')
                output['competition']['id'] = str(match_data['league'].get('id', ''))
            
            # Data e ora
            if 'date' in match_data:
                output['datetime'] = match_data['date']
            
            # Stato partita
            if 'status' in match_data:
                output['status'] = self._map_footystats_status(match_data['status'])
            
            # Punteggio
            if 'home_score' in match_data and 'away_score' in match_data:
                try:
                    output['score']['home'] = int(match_data['home_score'])
                    output['score']['away'] = int(match_data['away_score'])
                    
                    # Determina vincitore
                    if output['score']['home'] > output['score']['away']:
                        output['score']['winner'] = 'home'
                    elif output['score']['home'] < output['score']['away']:
                        output['score']['winner'] = 'away'
                    else:
                        output['score']['winner'] = 'draw'
                except:
                    pass
            
            # Statistiche (se disponibili)
            if 'stats' in match_data and match_data['stats']:
                output['statistics'] = match_data['stats']
            
        except Exception as e:
            logger.error(f"Errore nella normalizzazione dei dati FootyStats: {e}")
    
    def _normalize_whoscored(self, match_data: Dict[str, Any], output: Dict[str, Any]) -> None:
        """
        Normalizza i dati da WhoScored.
        
        Args:
            match_data: Dati grezzi della partita da WhoScored.
            output: Dizionario di output da popolare.
        """
        try:
            # ID partita
            if 'matchId' in match_data:
                output['match_id'] = str(match_data['matchId'])
                output['source_ids']['whoscored'] = str(match_data['matchId'])
            
            # Squadre
            if 'home' in match_data:
                output['home_team']['name'] = match_data['home'].get('name', '')
                output['home_team']['id'] = str(match_data['home'].get('teamId', ''))
                output['home_team']['short_name'] = match_data['home'].get('shortName', output['home_team']['name'])
            
            if 'away' in match_data:
                output['away_team']['name'] = match_data['away'].get('name', '')
                output['away_team']['id'] = str(match_data['away'].get('teamId', ''))
                output['away_team']['short_name'] = match_data['away'].get('shortName', output['away_team']['name'])
            
            # Competizione
            if 'tournament' in match_data:
                output['competition']['name'] = match_data['tournament'].get('name', '')
                output['competition']['id'] = str(match_data['tournament'].get('tournamentId', ''))
            
            # Data e ora
            if 'matchDate' in match_data:
                output['datetime'] = match_data['matchDate']
            
            # Stato partita
            if 'status' in match_data:
                output['status'] = self._map_whoscored_status(match_data['status'])
            
            # Punteggio
            if 'score' in match_data:
                try:
                    output['score']['home'] = int(match_data['score'].get('homeScore', 0))
                    output['score']['away'] = int(match_data['score'].get('awayScore', 0))
                    
                    # Determina vincitore
                    if output['score']['home'] > output['score']['away']:
                        output['score']['winner'] = 'home'
                    elif output['score']['home'] < output['score']['away']:
                        output['score']['winner'] = 'away'
                    else:
                        output['score']['winner'] = 'draw'
                except:
                    pass
            
            # Stadio
            if 'venue' in match_data:
                output['venue']['name'] = match_data['venue'].get('name', '')
                output['venue']['city'] = match_data['venue'].get('city', '')
            
            # Statistiche (se disponibili)
            if 'stats' in match_data and match_data['stats']:
                output['statistics'] = match_data['stats']
            
            # Eventi (se disponibili)
            if 'events' in match_data and match_data['events']:
                output['events'] = match_data['events']
            
        except Exception as e:
            logger.error(f"Errore nella normalizzazione dei dati WhoScored: {e}")
    
    def _normalize_worldfootball(self, match_data: Dict[str, Any], output: Dict[str, Any]) -> None:
        """
        Normalizza i dati da WorldFootball.
        
        Args:
            match_data: Dati grezzi della partita da WorldFootball.
            output: Dizionario di output da popolare.
        """
        try:
            # ID partita
            if 'id' in match_data:
                output['match_id'] = str(match_data['id'])
                output['source_ids']['worldfootball'] = str(match_data['id'])
            
            # Squadre
            if 'home_team' in match_data:
                output['home_team']['name'] = match_data['home_team']
            
            if 'away_team' in match_data:
                output['away_team']['name'] = match_data['away_team']
            
            # Competizione
            if 'competition' in match_data:
                output['competition']['name'] = match_data['competition']
            
            # Data e ora
            if 'date' in match_data:
                try:
                    date_str = match_data['date']
                    time_str = match_data.get('time', '15:00')  # Orario predefinito
                    dt = parse_date(f"{date_str} {time_str}")
                    output['datetime'] = dt.isoformat() if dt else date_str
                except:
                    output['datetime'] = match_data['date']
            
            # Stato partita
            if 'status' in match_data:
                output['status'] = self._map_generic_status(match_data['status'])
            
            # Punteggio
            if 'score' in match_data:
                score_text = match_data['score']
                try:
                    # Formato tipico: "2:1" o "2-1"
                    parts = re.split(r'[-:]', score_text)
                    if len(parts) == 2:
                        output['score']['home'] = int(parts[0].strip())
                        output['score']['away'] = int(parts[1].strip())
                        
                        # Determina vincitore
                        if output['score']['home'] > output['score']['away']:
                            output['score']['winner'] = 'home'
                        elif output['score']['home'] < output['score']['away']:
                            output['score']['winner'] = 'away'
                        else:
                            output['score']['winner'] = 'draw'
                except:
                    pass
            
        except Exception as e:
            logger.error(f"Errore nella normalizzazione dei dati WorldFootball: {e}")
    
    def _normalize_soccerway(self, match_data: Dict[str, Any], output: Dict[str, Any]) -> None:
        """
        Normalizza i dati da Soccerway.
        
        Args:
            match_data: Dati grezzi della partita da Soccerway.
            output: Dizionario di output da popolare.
        """
        try:
            # ID partita
            if 'id' in match_data:
                output['match_id'] = str(match_data['id'])
                output['source_ids']['soccerway'] = str(match_data['id'])
            
            # Squadre
            if 'home_team' in match_data:
                output['home_team']['name'] = match_data['home_team']
            
            if 'away_team' in match_data:
                output['away_team']['name'] = match_data['away_team']
            
            # Competizione
            if 'competition' in match_data:
                output['competition']['name'] = match_data['competition']
            
            # Data e ora
            if 'date' in match_data and 'time' in match_data:
                try:
                    dt = parse_date(f"{match_data['date']} {match_data['time']}")
                    output['datetime'] = dt.isoformat() if dt else match_data['date']
                except:
                    output['datetime'] = match_data['date']
            
            # Stato partita
            if 'status' in match_data:
                output['status'] = self._map_generic_status(match_data['status'])
            
            # Punteggio
            if 'score' in match_data:
                try:
                    # Formato comune: "2 - 1"
                    parts = match_data['score'].split('-')
                    if len(parts) == 2:
                        output['score']['home'] = int(parts[0].strip())
                        output['score']['away'] = int(parts[1].strip())
                        
                        # Determina vincitore
                        if output['score']['home'] > output['score']['away']:
                            output['score']['winner'] = 'home'
                        elif output['score']['home'] < output['score']['away']:
                            output['score']['winner'] = 'away'
                        else:
                            output['score']['winner'] = 'draw'
                except:
                    pass
            
            # Stadio
            if 'venue' in match_data:
                output['venue']['name'] = match_data['venue']
            
        except Exception as e:
            logger.error(f"Errore nella normalizzazione dei dati Soccerway: {e}")
    
    def _get_expected_goals_data(self, match_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Ottiene dati Expected Goals (xG) per la partita.
        
        Args:
            match_data: Dati della partita.
        
        Returns:
            Dizionario con dati xG della partita.
        """
        xg_data = {'home': 0.0, 'away': 0.0, 'home_shots': [], 'away_shots': []}
        
        # Prova prima con Understat
        if 'understat' in match_data['source_ids']:
            try:
                source_id = match_data['source_ids']['understat']
                understat_xg = self.understat.get_match_xg(source_id)
                
                if understat_xg:
                    xg_data['home'] = understat_xg.get('home_xg', 0.0)
                    xg_data['away'] = understat_xg.get('away_xg', 0.0)
                    xg_data['home_shots'] = understat_xg.get('home_shots', [])
                    xg_data['away_shots'] = understat_xg.get('away_shots', [])
                    xg_data['source'] = 'understat'
                    return xg_data
            except Exception as e:
                logger.debug(f"Errore nell'ottenere dati xG da Understat: {e}")
        
        # Poi prova con FBref
        if 'fbref' in match_data['source_ids']:
            try:
                source_id = match_data['source_ids']['fbref']
                fbref_xg = self.fbref.get_match_xg(source_id)
                
                if fbref_xg:
                    xg_data['home'] = fbref_xg.get('home_xg', 0.0)
                    xg_data['away'] = fbref_xg.get('away_xg', 0.0)
                    xg_data['source'] = 'fbref'
                    return xg_data
            except Exception as e:
                logger.debug(f"Errore nell'ottenere dati xG da FBref: {e}")
        
        # Infine prova con SofaScore
        if 'sofascore' in match_data['source_ids']:
            try:
                source_id = match_data['source_ids']['sofascore']
                sofascore_xg = self.sofascore.get_match_xg(source_id)
                
                if sofascore_xg:
                    xg_data['home'] = sofascore_xg.get('home_xg', 0.0)
                    xg_data['away'] = sofascore_xg.get('away_xg', 0.0)
                    xg_data['source'] = 'sofascore'
                    return xg_data
            except Exception as e:
                logger.debug(f"Errore nell'ottenere dati xG da SofaScore: {e}")
        
        # Se non abbiamo dati xG da nessuna fonte, proviamo a calcolarli dai tiri in porta
        if 'statistics' in match_data and match_data['statistics']:
            try:
                stats = match_data['statistics']
                if isinstance(stats, dict):
                    # Calcolo xG semplificato basato sui tiri in porta (molto approssimativo)
                    home_team_id = match_data['home_team']['id']
                    away_team_id = match_data['away_team']['id']
                    
                    # Cerca statistiche per ID squadra
                    home_stats = stats.get(home_team_id, stats.get(str(home_team_id), {}))
                    away_stats = stats.get(away_team_id, stats.get(str(away_team_id), {}))
                    
                    if home_stats and away_stats:
                        # SofaScore format
                        if 'xG' in home_stats and 'xG' in away_stats:
                            xg_data['home'] = float(home_stats['xG'])
                            xg_data['away'] = float(away_stats['xG'])
                            xg_data['source'] = 'statistics'
                            return xg_data
                        
                        # API-Football format
                        for home_stat in home_stats:
                            if isinstance(home_stat, dict) and home_stat.get('type') == 'Expected goals (xG)':
                                try:
                                    xg_data['home'] = float(home_stat.get('value', 0))
                                except:
                                    pass
                        
                        for away_stat in away_stats:
                            if isinstance(away_stat, dict) and away_stat.get('type') == 'Expected goals (xG)':
                                try:
                                    xg_data['away'] = float(away_stat.get('value', 0))
                                except:
                                    pass
                        
                        if xg_data['home'] > 0 or xg_data['away'] > 0:
                            xg_data['source'] = 'statistics'
                            return xg_data
            except Exception as e:
                logger.debug(f"Errore nel calcolo xG dalle statistiche: {e}")
        
        return None
    
    def _normalize_fbref(self, match_data: Dict[str, Any], output: Dict[str, Any]) -> None:
        """
        Normalizza i dati da FBref.
        
        Args:
            match_data: Dati grezzi della partita da FBref.
            output: Dizionario di output da popolare.
        """
        try:
            # ID partita
            if 'match_id' in match_data:
                output['match_id'] = str(match_data['match_id'])
                output['source_ids']['fbref'] = str(match_data['match_id'])
            
            # Squadre
            if 'home_team' in match_data:
                output['home_team']['name'] = match_data['home_team'].get('name', '')
                output['home_team']['id'] = match_data['home_team'].get('id', '')
            
            if 'away_team' in match_data:
                output['away_team']['name'] = match_data['away_team'].get('name', '')
                output['away_team']['id'] = match_data['away_team'].get('id', '')
            
            # Competizione
            if 'competition' in match_data:
                output['competition']['name'] = match_data['competition'].get('name', '')
                output['competition']['id'] = match_data['competition'].get('id', '')
            
            # Data e ora
            if 'date' in match_data:
                try:
                    # FBref spesso usa formato "2023-10-21"
                    date_str = match_data['date']
                    time_str = match_data.get('time', '15:00')  # Orario predefinito
                    dt = parse_date(f"{date_str} {time_str}")
                    output['datetime'] = dt.isoformat() if dt else date_str
                except:
                    output['datetime'] = match_data['date']
            
            # Stato partita
            if 'status' in match_data:
                output['status'] = self._map_fbref_status(match_data['status'])
            
            # Punteggio
            if 'score' in match_data:
                try:
                    home_score = match_data['score'].get('home', 0)
                    away_score = match_data['score'].get('away', 0)
                    
                    output['score']['home'] = int(home_score)
                    output['score']['away'] = int(away_score)
                    
                    # Determina vincitore
                    if output['score']['home'] > output['score']['away']:
                        output['score']['winner'] = 'home'
                    elif output['score']['home'] < output['score']['away']:
                        output['score']['winner'] = 'away'
                    else:
                        output['score']['winner'] = 'draw'
                except:
                    pass
            
            # Stadio
            if 'venue' in match_data:
                output['venue']['name'] = match_data['venue']
            
            # Statistiche (se disponibili)
            if 'statistics' in match_data and match_data['statistics']:
                output['statistics'] = match_data['statistics']
            
            # xG (se disponibile)
            if 'xg' in match_data:
                output['xg_stats'] = {
                    'home': match_data['xg'].get('home', 0.0),
                    'away': match_data['xg'].get('away', 0.0),
                    'source': 'fbref'
                }
            
        except Exception as e:
            logger.error(f"Errore nella normalizzazione dei dati FBref: {e}")
    
    def _normalize_understat(self, match_data: Dict[str, Any], output: Dict[str, Any]) -> None:
        """
        Normalizza i dati da Understat.
        
        Args:
            match_data: Dati grezzi della partita da Understat.
            output: Dizionario di output da popolare.
        """
        try:
            # ID partita
            if 'id' in match_data:
                output['match_id'] = str(match_data['id'])
                output['source_ids']['understat'] = str(match_data['id'])
            
            # Squadre
            if 'h' in match_data:  # home
                output['home_team']['name'] = match_data['h'].get('title', '')
                output['home_team']['id'] = str(match_data['h'].get('id', ''))
            
            if 'a' in match_data:  # away
                output['away_team']['name'] = match_data['a'].get('title', '')
                output['away_team']['id'] = str(match_data['a'].get('id', ''))
            
            # Data e ora
            if 'datetime' in match_data:
                output['datetime'] = match_data['datetime']
            
            # Stato partita (Understat usa valore numerico)
            if 'isResult' in match_data:
                output['status'] = 'finished' if match_data['isResult'] else 'scheduled'
            
            # Punteggio
            try:
                if 'goals' in match_data:
                    home_score = match_data['goals'].get('h', 0)
                    away_score = match_data['goals'].get('a', 0)
                    
                    output['score']['home'] = int(home_score)
                    output['score']['away'] = int(away_score)
                    
                    # Determina vincitore
                    if output['score']['home'] > output['score']['away']:
                        output['score']['winner'] = 'home'
                    elif output['score']['home'] < output['score']['away']:
                        output['score']['winner'] = 'away'
                    else:
                        output['score']['winner'] = 'draw'
            except:
                pass
            
            # League
            if 'league' in match_data:
                output['competition']['name'] = match_data['league'].get('name', '')
                output['competition']['id'] = str(match_data['league'].get('id', ''))
            
            # xG statistiche
            if 'xG' in match_data:
                output['xg_stats'] = {
                    'home': match_data['xG'].get('h', 0.0),
                    'away': match_data['xG'].get('a', 0.0),
                    'source': 'understat'
                }
            
            # Tiri se disponibili
            if 'shots' in match_data:
                home_shots = []
                away_shots = []
                
                for shot in match_data['shots']:
                    shot_data = {
                        'minute': shot.get('minute', 0),
                        'player': shot.get('player', ''),
                        'xG': shot.get('xG', 0.0),
                        'position': {'x': shot.get('X', 0.0), 'y': shot.get('Y', 0.0)},
                        'result': shot.get('result', '')
                    }
                    
                    if shot.get('h_a') == 'h':
                        home_shots.append(shot_data)
                    else:
                        away_shots.append(shot_data)
                
                if 'xg_stats' not in output:
                    output['xg_stats'] = {'source': 'understat'}
                    
                output['xg_stats']['home_shots'] = home_shots
                output['xg_stats']['away_shots'] = away_shots
            
        except Exception as e:
            logger.error(f"Errore nella normalizzazione dei dati Understat: {e}")
    
    def _normalize_sofascore(self, match_data: Dict[str, Any], output: Dict[str, Any]) -> None:
        """
        Normalizza i dati da SofaScore.
        
        Args:
            match_data: Dati grezzi della partita da SofaScore.
            output: Dizionario di output da popolare.
        """
        try:
            # ID partita
            if 'id' in match_data:
                output['match_id'] = str(match_data['id'])
                output['source_ids']['sofascore'] = str(match_data['id'])
            
            # Squadre
            if 'homeTeam' in match_data:
                output['home_team']['name'] = match_data['homeTeam'].get('name', '')
                output['home_team']['id'] = str(match_data['homeTeam'].get('id', ''))
                output['home_team']['short_name'] = match_data['homeTeam'].get('shortName', output['home_team']['name'])
            
            if 'awayTeam' in match_data:
                output['away_team']['name'] = match_data['awayTeam'].get('name', '')
                output['away_team']['id'] = str(match_data['awayTeam'].get('id', ''))
                output['away_team']['short_name'] = match_data['awayTeam'].get('shortName', output['away_team']['name'])
            
            # Competizione
            if 'tournament' in match_data:
                output['competition']['name'] = match_data['tournament'].get('name', '')
                output['competition']['id'] = str(match_data['tournament'].get('id', ''))
            
            # Data e ora
            if 'startTimestamp' in match_data:
                try:
                    dt = datetime.fromtimestamp(match_data['startTimestamp'])
                    output['datetime'] = dt.isoformat()
                except:
                    pass
            
            # Stato partita
            if 'status' in match_data:
                output['status'] = self._map_sofascore_status(match_data['status'].get('type', ''))
            
            # Punteggio
            if 'homeScore' in match_data and 'awayScore' in match_data:
                try:
                    output['score']['home'] = int(match_data['homeScore'].get('current', 0))
                    output['score']['away'] = int(match_data['awayScore'].get('current', 0))
                    
                    # Determina vincitore
                    if output['score']['home'] > output['score']['away']:
                        output['score']['winner'] = 'home'
                    elif output['score']['home'] < output['score']['away']:
                        output['score']['winner'] = 'away'
                    else:
                        output['score']['winner'] = 'draw'
                except:
                    pass
            
            # Stadio
            if 'venue' in match_data:
                output['venue']['name'] = match_data['venue'].get('stadium', {}).get('name', '')
                output['venue']['city'] = match_data['venue'].get('city', {}).get('name', '')
            
            # Statistiche (se disponibili)
            if 'statistics' in match_data and match_data['statistics']:
                output['statistics'] = match_data['statistics']
            
            # Eventi (se disponibili)
            if 'events' in match_data and match_data['events']:
                output['events'] = match_data['events']
            
            # xG (se disponibile)
            if 'xg' in match_data:
                output['xg_stats'] = {
                    'home': match_data['xg'].get('home', 0.0),
                    'away': match_data['xg'].get('away', 0.0),
                    'source': 'sofascore'
                }
            
        except Exception as e:
            logger.error(f"Errore nella normalizzazione dei dati SofaScore: {e}")
    
    def _normalize_footystats(self, match_data: Dict[str, Any], output: Dict[str, Any]) -> None:
        """
        Normalizza i dati da FootyStats.
        
        Args:
            match_data: Dati grezzi della partita da FootyStats.
            output: Dizionario di output da popolare.
        """
        try:
            # ID partita
            if 'id' in match_data:
                output['match_id'] = str(match_data['id'])
                output['source_ids']['footystats'] = str(match_data['id'])
            
            # Squadre
            if 'home_team' in match_data:
                output['home_team']['name'] = match_data['home_team'].get('name', '')
                output['home_team']['id'] = str(match_data['home_team'].get('id', ''))
            
            if 'away_team' in match_data:
                output['away_team']['name'] = match_data['away_team'].get('name', '')
                output['away_team']['id'] = str(match_data['away_team'].get('id', ''))
            
            # Competizione
            if 'league' in match_data:
                output['competition']['name'] = match_data['league'].get('name', '')
                output['competition']['id'] = str(match_data['league'].get('id', ''))
            
            # Data e ora
            if 'date' in match_data:
                output['datetime'] = match_data['date']
            
            # Stato partita
            if 'status' in match_data:
                output['status'] = self._map_footystats_status(match_data['status'])
            
            # Punteggio
            if 'home_score' in match_data and 'away_score' in match_data:
                try:
                    output['score']['home'] = int(match_data['home_score'])
                    output['score']['away'] = int(match_data['away_score'])
                    
                    # Determina vincitore
                    if output['score']['home'] > output['score']['away']:
                        output['score']['winner'] = 'home'
                    elif output['score']['home'] < output['score']['away']:
                        output['score']['winner'] = 'away'
                    else:
                        output['score']['winner'] = 'draw'
                except:
                    pass
            
            # Statistiche (se disponibili)
            if 'stats' in match_data and match_data['stats']:
                output['statistics'] = match_data['stats']
            
        except Exception as e:
            logger.error(f"Errore nella normalizzazione dei dati FootyStats: {e}")
    
    def _normalize_whoscored(self, match_data: Dict[str, Any], output: Dict[str, Any]) -> None:
        """
        Normalizza i dati da WhoScored.
        
        Args:
            match_data: Dati grezzi della partita da WhoScored.
            output: Dizionario di output da popolare.
        """
        try:
            # ID partita
            if 'matchId' in match_data:
                output['match_id'] = str(match_data['matchId'])
                output['source_ids']['whoscored'] = str(match_data['matchId'])
            
            # Squadre
            if 'home' in match_data:
                output['home_team']['name'] = match_data['home'].get('name', '')
                output['home_team']['id'] = str(match_data['home'].get('teamId', ''))
                output['home_team']['short_name'] = match_data['home'].get('shortName', output['home_team']['name'])
            
            if 'away' in match_data:
                output['away_team']['name'] = match_data['away'].get('name', '')
                output['away_team']['id'] = str(match_data['away'].get('teamId', ''))
                output['away_team']['short_name'] = match_data['away'].get('shortName', output['away_team']['name'])
            
            # Competizione
            if 'tournament' in match_data:
                output['competition']['name'] = match_data['tournament'].get('name', '')
                output['competition']['id'] = str(match_data['tournament'].get('tournamentId', ''))
            
            # Data e ora
            if 'matchDate' in match_data:
                output['datetime'] = match_data['matchDate']
            
            # Stato partita
            if 'status' in match_data:
                output['status'] = self._map_whoscored_status(match_data['status'])
            
            # Punteggio
            if 'score' in match_data:
                try:
                    output['score']['home'] = int(match_data['score'].get('homeScore', 0))
                    output['score']['away'] = int(match_data['score'].get('awayScore', 0))
                    
                    # Determina vincitore
                    if output['score']['home'] > output['score']['away']:
                        output['score']['winner'] = 'home'
                    elif output['score']['home'] < output['score']['away']:
                        output['score']['winner'] = 'away'
                    else:
                        output['score']['winner'] = 'draw'
                except:
                    pass
            
            # Stadio
            # Formazioni (se disponibili)
            if 'lineups' in match_data and match_data['lineups']:
                output['lineups'] = match_data['lineups']
            
            # Valore di mercato
            if 'market_value' in match_data:
                if 'home_team' in output and 'market_value' in match_data['market_value']:
                    output['home_team']['market_value'] = match_data['market_value'].get('home', '')
                
                if 'away_team' in output and 'market_value' in match_data['market_value']:
                    output['away_team']['market_value'] = match_data['market_value'].get('away', '')
            
        except Exception as e:
            logger.error(f"Errore nella normalizzazione dei dati Transfermarkt: {e}")
    
    def _normalize_open_football(self, match_data: Dict[str, Any], output: Dict[str, Any]) -> None:
        """
        Normalizza i dati da OpenFootball.
        
        Args:
            match_data: Dati grezzi della partita da OpenFootball.
            output: Dizionario di output da popolare.
        """
        try:
            # ID partita (in OpenFootball potrebbe non esserci un ID specifico)
            if 'id' in match_data:
                output['match_id'] = str(match_data['id'])
                output['source_ids']['open_football'] = str(match_data['id'])
            
            # Squadre
            if 'team1' in match_data:
                output['home_team']['name'] = match_data['team1'].get('name', match_data['team1'])
            
            if 'team2' in match_data:
                output['away_team']['name'] = match_data['team2'].get('name', match_data['team2'])
            
            # Competizione
            if 'competition' in match_data:
                output['competition']['name'] = match_data['competition']
            
            # Data e ora
            if 'date' in match_data:
                output['datetime'] = match_data['date']
            
            # Stato (OpenFootball non ha sempre uno stato, potrebbe essere dedotto dalla data)
            current_time = datetime.now()
            match_time = parse_date(output['datetime']) if output['datetime'] else None
            
            if match_time:
                if match_time > current_time:
                    output['status'] = 'scheduled'
                else:
                    output['status'] = 'finished'
            
            # Punteggio
            if 'score1' in match_data and 'score2' in match_data:
                try:
                    output['score']['home'] = int(match_data['score1'])
                    output['score']['away'] = int(match_data['score2'])
                    
                    # Determina vincitore
                    if output['score']['home'] > output['score']['away']:
                        output['score']['winner'] = 'home'
                    elif output['score']['home'] < output['score']['away']:
                        output['score']['winner'] = 'away'
                    else:
                        output['score']['winner'] = 'draw'
                except:
                    pass
            
        except Exception as e:
            logger.error(f"Errore nella normalizzazione dei dati OpenFootball: {e}")
    
    def _normalize_statsbomb(self, match_data: Dict[str, Any], output: Dict[str, Any]) -> None:
        """
        Normalizza i dati da StatsBomb.
        
        Args:
            match_data: Dati grezzi della partita da StatsBomb.
            output: Dizionario di output da popolare.
        """
        try:
            # ID partita
            if 'match_id' in match_data:
                output['match_id'] = str(match_data['match_id'])
                output['source_ids']['statsbomb'] = str(match_data['match_id'])
            
            # Squadre
            if 'home_team' in match_data:
                output['home_team']['name'] = match_data['home_team'].get('home_team_name', '')
                output['home_team']['id'] = str(match_data['home_team'].get('home_team_id', ''))
            
            if 'away_team' in match_data:
                output['away_team']['name'] = match_data['away_team'].get('away_team_name', '')
                output['away_team']['id'] = str(match_data['away_team'].get('away_team_id', ''))
            
            # Competizione
            if 'competition' in match_data:
                output['competition']['name'] = match_data['competition'].get('competition_name', '')
                output['competition']['id'] = str(match_data['competition'].get('competition_id', ''))
            
            # Data e ora
            if 'match_date' in match_data:
                output['datetime'] = match_data['match_date']
            
            # Stato (StatsBomb ha solo dati storici, quindi tutti i match sono finiti)
            output['status'] = 'finished'
            
            # Punteggio
            if 'home_score' in match_data and 'away_score' in match_data:
                try:
                    output['score']['home'] = int(match_data['home_score'])
                    output['score']['away'] = int(match_data['away_score'])
                    
                    # Determina vincitore
                    if output['score']['home'] > output['score']['away']:
                        output['score']['winner'] = 'home'
                    elif output['score']['home'] < output['score']['away']:
                        output['score']['winner'] = 'away'
                    else:
                        output['score']['winner'] = 'draw'
                except:
                    pass
            
            # Statistiche avanzate (se disponibili)
            if 'stats' in match_data and match_data['stats']:
                output['statistics'] = match_data['stats']
            
            # Eventi (Statsbomb è ricco di dati sugli eventi)
            if 'events' in match_data and match_data['events']:
                output['events'] = match_data['events']
            
        except Exception as e:
            logger.error(f"Errore nella normalizzazione dei dati StatsBomb: {e}")
    
    def _map_fbref_status(self, status: str) -> str:
        """
        Mappa lo stato della partita da FBref a formato standard.
        
        Args:
            status: Stato della partita nel formato FBref.
        
        Returns:
            Stato della partita in formato standard.
        """
        status = status.lower()
        
        if 'complete' in status or 'ft' in status:
            return 'finished'
        elif any(x in status for x in ['postponed', 'susp', 'cancel', 'interrupt']):
            return 'postponed'
        elif any(x in status for x in ['ongoing', 'live', '1h', '2h', 'halftime']):
            return 'in_progress'
        elif 'scheduled' in status:
            return 'scheduled'
        
        # FBref può usare formati di data (es. "Oct 28, 2023") per partite programmate
        try:
            parse_date(status)  # Se riesce a interpretarlo come data
            return 'scheduled'
        except:
            pass
        
        return 'unknown'
    
    def enrich_match_data(self, match_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Arricchisce i dati della partita con informazioni aggiuntive.
        
        Args:
            match_data: Dati della partita da arricchire.
        
        Returns:
            Dizionario con i dati della partita arricchiti.
        """
        # Copia i dati originali
        enriched_data = match_data.copy()
        
        try:
            # Calcola stato corrente della partita in base alla data
            if enriched_data['datetime'] and not enriched_data['status'] in ['finished', 'cancelled', 'postponed', 'awarded']:
                enriched_data['calculated_status'] = get_match_status(enriched_data['datetime'])
            
            # Aggiungi dati delle squadre se non presenti
            for team_key, team_data in [('home_team', enriched_data['home_team']), ('away_team', enriched_data['away_team'])]:
                if team_data.get('id') and not team_data.get('short_name'):
                    try:
                        # Prova a ottenere dati aggiuntivi dalle API
                        team_id = team_data['id']
                        
                        # Prova con diverse fonti in ordine di priorità
                        sources = ['football_data', 'api_football', 'transfermarkt', 'fbref', 'sofascore']
                        for source in sources:
                            if source in enriched_data['source_ids'] and not team_data.get('short_name'):
                                try:
                                    self._enrich_team_data_from_source(team_data, team_id, source)
                                except Exception as e:
                                    logger.debug(f"Errore nell'ottenere dati squadra da {source}: {e}")
                                    continue
                    except Exception as e:
                        logger.warning(f"Errore nell'arricchimento dei dati della squadra {team_id}: {e}")
            
            # Aggiungi quote se non presenti e la partita non è finita
            if not enriched_data.get('odds') and enriched_data['status'] in ['scheduled', 'unknown']:
                try:
                    # Prova a ottenere quote da diverse fonti
                    sources = ['api_football', 'flashscore', 'sofascore']
                    for source in sources:
                        if source in enriched_data['source_ids'] and not enriched_data.get('odds'):
                            source_id = enriched_data['source_ids'][source]
                            try:
                                odds_data = self._get_odds_from_source(source_id, source)
                                if odds_data:
                                    enriched_data['odds'] = odds_data
                                    break
                            except Exception as e:
                                logger.debug(f"Errore nell'ottenere quote da {source}: {e}")
                                continue
                except Exception as e:
                    logger.warning(f"Errore nell'ottenimento delle quote per la partita {enriched_data['match_id']}: {e}")
            
            # Aggiungi formazioni se non presenti e la partita è recente
            if not enriched_data.get('lineups') and enriched_data['status'] in ['in_progress', 'finished']:
                try:
                    # Prova a ottenere formazioni da diverse fonti
                    sources = ['api_football', 'flashscore', 'sofascore', 'fbref', 'whoscored']
                    for source in sources:
                        if source in enriched_data['source_ids'] and not enriched_data.get('lineups'):
                            source_id = enriched_data['source_ids'][source]
                            try:
                                lineups_data = self._get_lineups_from_source(source_id, source)
                                if lineups_data:
                                    enriched_data['lineups'] = lineups_data
                                    break
                            except Exception as e:
                                logger.debug(f"Errore nell'ottenere formazioni da {source}: {e}")
                                continue
                except Exception as e:
                    logger.warning(f"Errore nell'ottenimento delle formazioni per la partita {enriched_data['match_id']}: {e}")
            
            # Aggiungi statistiche se non presenti e la partita è iniziata o finita
            if not enriched_data.get('statistics') and enriched_data['status'] in ['in_progress', 'finished']:
                try:
                    # Prova a ottenere statistiche da diverse fonti
                    sources = ['api_football', 'flashscore', 'sofascore', 'fbref', 'understat', 'whoscored', 'footystats']
                    for source in sources:
                        if source in enriched_data['source_ids'] and not enriched_data.get('statistics'):
                            source_id = enriched_data['source_ids'][source]
                            try:
                                stats_data = self._get_stats_from_source(source_id, source)
                                if stats_data:
                                    enriched_data['statistics'] = stats_data
                                    break
                            except Exception as e:
                                logger.debug(f"Errore nell'ottenere statistiche da {source}: {e}")
                                continue
                except Exception as e:
                    logger.warning(f"Errore nell'ottenimento delle statistiche per la partita {enriched_data['match_id']}: {e}")
            
            # Aggiungi statistiche Expected Goals (xG) se non presenti
            if (not enriched_data.get('xg_stats') and 
                enriched_data['status'] in ['in_progress', 'finished'] and 
                ('understat' in enriched_data['source_ids'] or 'fbref' in enriched_data['source_ids'])):
                try:
                    xg_data = self._get_expected_goals_data(enriched_data)
                    if xg_data:
                        enriched_data['xg_stats'] = xg_data
                except Exception as e:
                    logger.warning(f"Errore nell'ottenimento dei dati xG per la partita {enriched_data['match_id']}: {e}")
            
            # Aggiorna timestamp
            enriched_data['last_updated'] = datetime.now().isoformat()
            
            return enriched_data
            
        except Exception as e:
            logger.error(f"Errore nell'arricchimento dei dati della partita {match_data.get('match_id', 'unknown')}: {e}")
            return match_data  # Restituisci i dati originali in caso di errore
    
    def _enrich_team_data_from_source(self, team_data: Dict[str, Any], team_id: str, source: str) -> None:
        """
        Arricchisce i dati di una squadra da una fonte specifica.
        
        Args:
            team_data: Dizionario dei dati della squadra da arricchire.
            team_id: ID della squadra nella fonte.
            source: Nome della fonte dei dati.
        """
        if source == 'football_data':
            team_details = self.football_data_api.get_team(int(team_id))
            if team_details:
                team_data['name'] = team_details.get('name', team_data['name'])
                team_data['short_name'] = team_details.get('shortName', team_data['name'])
                team_data['tla'] = team_details.get('tla', '')
                team_data['logo'] = team_details.get('crest', '')
        
        elif source == 'api_football':
            team_details = self.api_football.get_team(int(team_id))
            if team_details:
                team_data['name'] = team_details.get('name', team_data['name'])
                team_data['short_name'] = team_details.get('code', team_data['name'])
                team_data['logo'] = team_details.get('logo', '')
        
        elif source == 'transfermarkt':
            team_details = self.transfermarkt.get_team_details(team_id)
            if team_details:
                team_data['name'] = team_details.get('name', team_data['name'])
                team_data['short_name'] = team_details.get('short_name', team_data['name'])
                team_data['logo'] = team_details.get('logo', '')
                team_data['market_value'] = team_details.get('market_value', '')
        
        elif source == 'fbref':
            team_details = self.fbref.get_team_basic_info(team_id)
            if team_details:
                team_data['name'] = team_details.get('name', team_data['name'])
                team_data['short_name'] = team_details.get('short_name', team_data['name'])
        
        elif source == 'sofascore':
            team_details = self.sofascore.get_team_details(team_id)
            if team_details:
                team_data['name'] = team_details.get('name', team_data['name'])
                team_data['short_name'] = team_details.get('short_name', team_data['name'])
                team_data['logo'] = team_details.get('logo', '')
    
    def _get_odds_from_source(self, source_id: str, source: str) -> Dict[str, Any]:
        """
        Ottiene quote da una fonte specifica.
        
        Args:
            source_id: ID della partita nella fonte.
            source: Nome della fonte dei dati.
        
        Returns:
            Dizionario con le quote della partita.
        """
        if source == 'api_football':
            return self.api_football.get_odds(int(source_id))
        
        elif source == 'flashscore':
            match_details = self.flashscore.get_match_details(source_id)
            return match_details.get('odds', {}) if match_details else {}
        
        elif source == 'sofascore':
            return self.sofascore.get_match_odds(source_id)
        
        return {}
    
    def filter_upcoming_matches(
        self, 
        matches: List[Dict[str, Any]], 
        hours_from_now: float = 12.0
    ) -> List[Dict[str, Any]]:
        """
        Filtra le partite in arrivo entro un certo numero di ore.
        
        Args:
            matches: Lista di partite da filtrare.
            hours_from_now: Numero di ore da adesso per considerare una partita "in arrivo".
        
        Returns:
            Lista di partite filtrate.
        """
        now = datetime.now()
        upcoming = []
        
        for match in matches:
            try:
                # Converte datetime della partita
                match_dt = parse_date(match['datetime'])
                
                if not match_dt:
                    continue
                
                # Calcola differenza in ore
                diff = match_dt - now
                hours_diff = diff.total_seconds() / 3600
                
                # Se la partita è entro le ore specificate e non è già iniziata/finita
                if 0 <= hours_diff <= hours_from_now and match['status'] in ['scheduled', 'unknown']:
                    upcoming.append(match)
            except:
                continue
        
        # Ordina per data (dalla più vicina)
        upcoming.sort(key=lambda m: parse_date(m['datetime']) or now)
        
        return upcoming
    
    def store_match_data(self, match_data: Dict[str, Any]) -> bool:
        """
        Salva i dati della partita nel database.
        
        Args:
            match_data: Dati della partita da salvare.
        
        Returns:
            True se salvato con successo, False altrimenti.
        """
        try:
            if not match_data.get('match_id'):
                logger.error("Impossibile salvare partita senza match_id")
                return False
            
            # Costruisci il percorso nel database
            match_id = match_data['match_id']
            
            # Aggiungi timestamp se non presente
            if 'last_updated' not in match_data:
                match_data['last_updated'] = datetime.now().isoformat()
            
            # Salva i dati
            self.db.set_reference(f"matches/{match_id}", match_data)
            
            logger.info(f"Dati della partita {match_id} salvati con successo")
            return True
            
        except Exception as e:
            logger.error(f"Errore nel salvare i dati della partita: {e}")
            return False
    
    def get_stored_match_data(self, match_id: str) -> Optional[Dict[str, Any]]:
        """
        Recupera i dati della partita dal database.
        
        Args:
            match_id: ID della partita.
        
        Returns:
            Dizionario con i dati della partita, o None se non trovato.
        """
        try:
            # Recupera i dati
            match_data = self.db.get_reference(f"matches/{match_id}")
            return match_data
            
        except Exception as e:
            logger.error(f"Errore nel recuperare i dati della partita {match_id}: {e}")
            return None
    
    def update_match_status(self, match_id: str, new_status: str) -> bool:
        """
        Aggiorna lo stato della partita nel database.
        
        Args:
            match_id: ID della partita.
            new_status: Nuovo stato della partita.
        
        Returns:
            True se aggiornato con successo, False altrimenti.
        """
        try:
            # Ottieni i dati attuali
            match_data = self.get_stored_match_data(match_id)
            
            if not match_data:
                logger.error(f"Partita {match_id} non trovata")
                return False
            
            # Aggiorna lo stato
            match_data['status'] = new_status
            match_data['last_updated'] = datetime.now().isoformat()
            
            # Salva i dati aggiornati
            return self.store_match_data(match_data)
            
        except Exception as e:
            logger.error(f"Errore nell'aggiornare lo stato della partita {match_id}: {e}")
            return False


# Istanza globale per un utilizzo più semplice
match_processor = MatchProcessor()

def get_processor():
    """
    Ottiene l'istanza globale del processore.
    
    Returns:
        Istanza di MatchProcessor.
    """
    return match_processor
    
