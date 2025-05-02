"""
Coordinatore centrale per la raccolta dati.
Gestisce l'acquisizione di dati da diverse fonti (API, scraper, open data),
coordinando le richieste, gestendo le priorità e implementando strategie
di fallback quando necessario.
"""

import os
import time
import json
import logging
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union, Callable, Tuple

# Importa le configurazioni
from src.config.settings import get_setting
from src.config.leagues import get_active_leagues, get_league, get_api_code
from src.config.sources import get_active_sources, get_sources_for_data_type

# Importa le utility
from src.utils.database import FirebaseManager
from src.utils.http import make_request
from src.utils.cache import cached
from src.utils.time_utils import get_current_datetime, format_datetime

# Importa i logger
from src.monitoring.logger import get_logger

# Importa i moduli API
from src.data.api.football_data import FootballDataAPI
from src.data.api.api_football import APIFootball

# Importa i moduli di statistiche
from src.data.stats.fbref import FBrefScraper
from src.data.stats.understat import UnderstatScraper
from src.data.stats.sofascore import SofaScoreScraper
from src.data.stats.footystats import FootyStatsScraper

# Importa i moduli open data
from src.data.open_data.open_football import OpenFootballLoader
from src.data.open_data.rsssf import RSSFFLoader
from src.data.open_data.kaggle_loader import KaggleLoader
from src.data.open_data.statsbomb import StatsBombLoader

# Importa i processori
from src.data.processors.matches import MatchProcessor
from src.data.processors.teams import TeamProcessor
from src.data.processors.head_to_head import HeadToHeadProcessor
from src.data.processors.xg_processor import XGProcessor
from src.data.processors.standings import StandingsProcessor

# Configura il logger
logger = get_logger('data_collector')

class DataCollector:
    """
    Coordinatore centrale per la raccolta dati da multiple fonti.
    Implementa strategie di priorità e fallback per garantire
    l'acquisizione affidabile dei dati.
    """
    
    def __init__(self):
        """Inizializza il coordinatore dati."""
        self.db = FirebaseManager()
        
        # Inizializza le API
        self.football_data_api = FootballDataAPI()
        self.api_football = APIFootball()
        
        # Inizializza gli scraper di statistiche
        self.fbref_scraper = FBrefScraper()
        self.understat_scraper = UnderstatScraper()
        self.sofascore_scraper = SofaScoreScraper()
        self.footystats_scraper = FootyStatsScraper()
        
        # Inizializza i loader di dati aperti
        self.open_football_loader = OpenFootballLoader()
        self.rsssf_loader = RSSFFLoader()
        self.kaggle_loader = KaggleLoader()
        self.statsbomb_loader = StatsBombLoader()
        
        # Inizializza i processori
        self.match_processor = MatchProcessor()
        self.team_processor = TeamProcessor()
        self.head_to_head_processor = HeadToHeadProcessor()
        self.xg_processor = XGProcessor()
        self.standings_processor = StandingsProcessor()
        
        # Ottieni le fonti dati disponibili
        self.sources = get_active_sources()
        
        # Semaforo per limitare le richieste concorrenti
        self.request_semaphore = threading.Semaphore(5)
        
        # Lista per tenere traccia degli errori
        self.errors = []
        
        logger.info("DataCollector inizializzato con successo")
    
    def collect_matches(self, league_id: str, days_ahead: int = 7, 
                       days_behind: int = 3) -> List[Dict[str, Any]]:
        """
        Raccoglie le partite programmate e recenti per un campionato.
        
        Args:
            league_id: ID del campionato
            days_ahead: Giorni futuri da considerare
            days_behind: Giorni passati da considerare
            
        Returns:
            Lista di partite con dati completi
        """
        logger.info(f"Raccolta partite per {league_id} ({days_behind} giorni passati, {days_ahead} giorni futuri)")
        
        # Ottieni fonti dati per le partite
        match_sources = get_sources_for_data_type('matches')
        
        # Ottieni informazioni campionato
        league = get_league(league_id)
        if not league:
            logger.error(f"Campionato non trovato: {league_id}")
            return []
        
        # Calcola intervallo date
        now = get_current_datetime()
        date_from = (now - timedelta(days=days_behind)).date()
        date_to = (now + timedelta(days=days_ahead)).date()
        
        raw_matches = []
        errors = []
        
        # Strategia 1: Prova API ufficiali
        for source_id in match_sources.get('primary', []):
            if source_id == 'football_data_api':
                try:
                    # Ottieni codice API per il campionato
                    api_code = get_api_code(league_id, 'football_data')
                    if api_code:
                        matches = self.football_data_api.get_matches(
                            competition_code=api_code,
                            date_from=date_from.isoformat(),
                            date_to=date_to.isoformat()
                        )
                        if matches:
                            raw_matches.extend(matches)
                            logger.info(f"Ottenute {len(matches)} partite da football_data_api")
                            break  # Usciamo se abbiamo ottenuto dati validi
                except Exception as e:
                    error_msg = f"Errore nel recupero partite da football_data_api: {str(e)}"
                    logger.warning(error_msg)
                    errors.append(error_msg)
            
            elif source_id == 'rapidapi_football':
                try:
                    # Ottieni codice API per il campionato
                    api_code = get_api_code(league_id, 'rapidapi_football')
                    if api_code:
                        matches = self.api_football.get_matches(
                            league_id=api_code,
                            from_date=date_from.isoformat(),
                            to_date=date_to.isoformat()
                        )
                        if matches:
                            raw_matches.extend(matches)
                            logger.info(f"Ottenute {len(matches)} partite da rapidapi_football")
                            break  # Usciamo se abbiamo ottenuto dati validi
                except Exception as e:
                    error_msg = f"Errore nel recupero partite da rapidapi_football: {str(e)}"
                    logger.warning(error_msg)
                    errors.append(error_msg)
        
        # Strategia 2: Se le API non hanno funzionato, prova gli scraper
        if not raw_matches:
            logger.info("Nessun dato dalle API principali, utilizzo scraper come fallback")
            for source_id in match_sources.get('fallback', []):
                if source_id == 'fbref':
                    try:
                        league_url = league.get('urls', {}).get('fbref')
                        if league_url:
                            matches = self.fbref_scraper.get_matches(
                                url=league_url,
                                days_ahead=days_ahead,
                                days_behind=days_behind
                            )
                            if matches:
                                raw_matches.extend(matches)
                                logger.info(f"Ottenute {len(matches)} partite da fbref")
                                break  # Usciamo se abbiamo ottenuto dati validi
                    except Exception as e:
                        error_msg = f"Errore nel recupero partite da fbref: {str(e)}"
                        logger.warning(error_msg)
                        errors.append(error_msg)
                
                elif source_id == 'sofascore':
                    try:
                        league_code = get_api_code(league_id, 'sofascore')
                        if league_code:
                            matches = self.sofascore_scraper.get_matches_by_league(
                                league_id=league_code,
                                from_date=date_from.isoformat(),
                                to_date=date_to.isoformat()
                            )
                            if matches:
                                raw_matches.extend(matches)
                                logger.info(f"Ottenute {len(matches)} partite da sofascore")
                                break  # Usciamo se abbiamo ottenuto dati validi
                    except Exception as e:
                        error_msg = f"Errore nel recupero partite da sofascore: {str(e)}"
                        logger.warning(error_msg)
                        errors.append(error_msg)
        
        # Se ancora non abbiamo dati, prova fonti open data
        if not raw_matches:
            logger.info("Nessun dato dalle fonti principali, tentativo con open data")
            try:
                open_football_matches = self.open_football_loader.get_matches(
                    league=league_id,
                    season=league.get('current_season', ''),
                    from_date=date_from.isoformat(),
                    to_date=date_to.isoformat()
                )
                if open_football_matches:
                    raw_matches.extend(open_football_matches)
                    logger.info(f"Ottenute {len(open_football_matches)} partite da open_football")
            except Exception as e:
                error_msg = f"Errore nel recupero partite da open_football: {str(e)}"
                logger.warning(error_msg)
                errors.append(error_msg)
        
        # Se ancora non abbiamo dati, registra un errore critico
        if not raw_matches:
            error_msg = f"Impossibile ottenere partite per il campionato {league_id} da qualsiasi fonte"
            logger.error(error_msg)
            self.errors.append({
                'timestamp': datetime.now().isoformat(),
                'type': 'matches_collection',
                'league_id': league_id,
                'message': error_msg,
                'details': errors
            })
            return []
        
        # Standardizza e arricchisci i dati
        processed_matches = self.match_processor.process(raw_matches)
        
        # Aggiorna timestamp di ultimo aggiornamento
        self.db.get_reference(f"data/matches/{league_id}/last_updated").set(datetime.now().isoformat())
        
        return processed_matches
    
    def collect_team_stats(self, team_id: str, detailed: bool = True) -> Dict[str, Any]:
        """
        Raccoglie statistiche dettagliate per una squadra.
        
        Args:
            team_id: ID della squadra
            detailed: Se recuperare statistiche dettagliate
            
        Returns:
            Dizionario con statistiche della squadra
        """
        logger.info(f"Raccolta statistiche per squadra {team_id}")
        
        # Ottieni fonti dati per le statistiche delle squadre
        team_sources = get_sources_for_data_type('teams')
        
        team_data = {}
        errors = []
        
        # Strategia 1: Prova API ufficiali per i dati base
        for source_id in team_sources.get('primary', []):
            if source_id == 'football_data_api':
                try:
                    team_info = self.football_data_api.get_team(team_id)
                    if team_info:
                        team_data.update(team_info)
                        logger.info(f"Ottenuti dati base per squadra {team_id} da football_data_api")
                except Exception as e:
                    error_msg = f"Errore nel recupero dati squadra da football_data_api: {str(e)}"
                    logger.warning(error_msg)
                    errors.append(error_msg)
            
            elif source_id == 'rapidapi_football':
                try:
                    team_info = self.api_football.get_team(team_id)
                    if team_info:
                        team_data.update(team_info)
                        logger.info(f"Ottenuti dati base per squadra {team_id} da rapidapi_football")
                except Exception as e:
                    error_msg = f"Errore nel recupero dati squadra da rapidapi_football: {str(e)}"
                    logger.warning(error_msg)
                    errors.append(error_msg)
        
        # Se richieste statistiche dettagliate
        if detailed:
            # Strategia 2: Usa FBref per statistiche avanzate
            try:
                fbref_team_id = team_data.get('fbref_id') or team_id
                team_stats = self.fbref_scraper.get_team_stats(fbref_team_id)
                if team_stats:
                    team_data['stats'] = team_data.get('stats', {})
                    team_data['stats'].update(team_stats)
                    logger.info(f"Ottenute statistiche avanzate per squadra {team_id} da fbref")
            except Exception as e:
                error_msg = f"Errore nel recupero statistiche da fbref: {str(e)}"
                logger.warning(error_msg)
                errors.append(error_msg)
            
            # Strategia 3: Usa Understat per dati xG
            try:
                understat_team_id = team_data.get('understat_id') or team_id
                xg_stats = self.understat_scraper.get_team_stats(understat_team_id)
                if xg_stats:
                    team_data['xg_stats'] = xg_stats
                    logger.info(f"Ottenuti dati xG per squadra {team_id} da understat")
            except Exception as e:
                error_msg = f"Errore nel recupero dati xG da understat: {str(e)}"
                logger.warning(error_msg)
                errors.append(error_msg)
            
            # Strategia 4: Fallback a SofaScore
            if not team_data.get('stats'):
                try:
                    sofascore_team_id = team_data.get('sofascore_id') or team_id
                    sofascore_stats = self.sofascore_scraper.get_team_stats(sofascore_team_id)
                    if sofascore_stats:
                        team_data['stats'] = sofascore_stats
                        logger.info(f"Ottenute statistiche per squadra {team_id} da sofascore")
                except Exception as e:
                    error_msg = f"Errore nel recupero statistiche da sofascore: {str(e)}"
                    logger.warning(error_msg)
                    errors.append(error_msg)
        
        # Se non abbiamo raccolto nulla, registra un errore
        if not team_data:
            error_msg = f"Impossibile ottenere dati per la squadra {team_id} da qualsiasi fonte"
            logger.error(error_msg)
            self.errors.append({
                'timestamp': datetime.now().isoformat(),
                'type': 'team_stats_collection',
                'team_id': team_id,
                'message': error_msg,
                'details': errors
            })
            return {}
        
        # Standardizza e arricchisci i dati
        processed_team = self.team_processor.process(team_data)
        
        # Aggiorna timestamp di ultimo aggiornamento
        self.db.get_reference(f"data/teams/{team_id}/last_updated").set(datetime.now().isoformat())
        
        return processed_team
    
    def collect_head_to_head(self, team1_id: str, team2_id: str, 
                           limit: int = 10) -> List[Dict[str, Any]]:
        """
        Raccoglie lo storico degli scontri diretti tra due squadre.
        
        Args:
            team1_id: ID della prima squadra
            team2_id: ID della seconda squadra
            limit: Numero massimo di partite da recuperare
            
        Returns:
            Lista di partite tra le due squadre
        """
        logger.info(f"Raccolta scontri diretti tra {team1_id} e {team2_id}")
        
        h2h_data = []
        errors = []
        
        # Strategia 1: Prova API Football
        try:
            h2h_matches = self.api_football.get_head_to_head(team1_id, team2_id, limit)
            if h2h_matches:
                h2h_data.extend(h2h_matches)
                logger.info(f"Ottenuti {len(h2h_matches)} scontri diretti da api_football")
        except Exception as e:
            error_msg = f"Errore nel recupero scontri diretti da api_football: {str(e)}"
            logger.warning(error_msg)
            errors.append(error_msg)
        
        # Strategia 2: Fallback a SofaScore
        if not h2h_data:
            try:
                sofascore_h2h = self.sofascore_scraper.get_head_to_head(team1_id, team2_id, limit)
                if sofascore_h2h:
                    h2h_data.extend(sofascore_h2h)
                    logger.info(f"Ottenuti {len(sofascore_h2h)} scontri diretti da sofascore")
            except Exception as e:
                error_msg = f"Errore nel recupero scontri diretti da sofascore: {str(e)}"
                logger.warning(error_msg)
                errors.append(error_msg)
        
        # Strategia 3: Fallback a fonti storiche
        if not h2h_data:
            try:
                historical_h2h = self.head_to_head_processor.get_from_historical_data(team1_id, team2_id, limit)
                if historical_h2h:
                    h2h_data.extend(historical_h2h)
                    logger.info(f"Ottenuti {len(historical_h2h)} scontri diretti da dati storici")
            except Exception as e:
                error_msg = f"Errore nel recupero scontri diretti da dati storici: {str(e)}"
                logger.warning(error_msg)
                errors.append(error_msg)
        
        # Se non abbiamo raccolto nulla, registra un errore
        if not h2h_data:
            error_msg = f"Impossibile ottenere scontri diretti tra {team1_id} e {team2_id} da qualsiasi fonte"
            logger.error(error_msg)
            self.errors.append({
                'timestamp': datetime.now().isoformat(),
                'type': 'h2h_collection',
                'teams': [team1_id, team2_id],
                'message': error_msg,
                'details': errors
            })
            return []
        
        # Standardizza e arricchisci i dati
        processed_h2h = self.head_to_head_processor.process(h2h_data)
        
        # Aggiorna timestamp di ultimo aggiornamento
        h2h_id = f"{team1_id}_{team2_id}"
        self.db.get_reference(f"data/head_to_head/{h2h_id}/last_updated").set(datetime.now().isoformat())
        
        return processed_h2h
    
    def collect_league_standings(self, league_id: str) -> Dict[str, Any]:
        """
        Raccoglie la classifica corrente per un campionato.
        
        Args:
            league_id: ID del campionato
            
        Returns:
            Dizionario con la classifica
        """
        logger.info(f"Raccolta classifica per campionato {league_id}")
        
        # Ottieni fonti dati per le classifiche
        standings_sources = get_sources_for_data_type('standings')
        
        # Ottieni informazioni campionato
        league = get_league(league_id)
        if not league:
            logger.error(f"Campionato non trovato: {league_id}")
            return {}
        
        standings_data = {}
        errors = []
        
        # Strategia 1: Prova API ufficiali
        for source_id in standings_sources.get('primary', []):
            if source_id == 'football_data_api':
                try:
                    # Ottieni codice API per il campionato
                    api_code = get_api_code(league_id, 'football_data')
                    if api_code:
                        standings = self.football_data_api.get_standings(api_code)
                        if standings:
                            standings_data = standings
                            logger.info(f"Ottenuta classifica per {league_id} da football_data_api")
                            break  # Usciamo se abbiamo ottenuto dati validi
                except Exception as e:
                    error_msg = f"Errore nel recupero classifica da football_data_api: {str(e)}"
                    logger.warning(error_msg)
                    errors.append(error_msg)
            
            elif source_id == 'rapidapi_football':
                try:
                    # Ottieni codice API per il campionato
                    api_code = get_api_code(league_id, 'rapidapi_football')
                    if api_code:
                        standings = self.api_football.get_standings(api_code)
                        if standings:
                            standings_data = standings
                            logger.info(f"Ottenuta classifica per {league_id} da rapidapi_football")
                            break  # Usciamo se abbiamo ottenuto dati validi
                except Exception as e:
                    error_msg = f"Errore nel recupero classifica da rapidapi_football: {str(e)}"
                    logger.warning(error_msg)
                    errors.append(error_msg)
        
        # Strategia 2: Prova scraper
        if not standings_data:
            for source_id in standings_sources.get('fallback', []):
                if source_id == 'fbref':
                    try:
                        league_url = league.get('urls', {}).get('fbref')
                        if league_url:
                            standings = self.fbref_scraper.get_standings(league_url)
                            if standings:
                                standings_data = standings
                                logger.info(f"Ottenuta classifica per {league_id} da fbref")
                                break  # Usciamo se abbiamo ottenuto dati validi
                    except Exception as e:
                        error_msg = f"Errore nel recupero classifica da fbref: {str(e)}"
                        logger.warning(error_msg)
                        errors.append(error_msg)
                
                elif source_id == 'sofascore':
                    try:
                        league_code = get_api_code(league_id, 'sofascore')
                        if league_code:
                            standings = self.sofascore_scraper.get_standings(league_code)
                            if standings:
                                standings_data = standings
                                logger.info(f"Ottenuta classifica per {league_id} da sofascore")
                                break  # Usciamo se abbiamo ottenuto dati validi
                    except Exception as e:
                        error_msg = f"Errore nel recupero classifica da sofascore: {str(e)}"
                        logger.warning(error_msg)
                        errors.append(error_msg)
        
        # Se non abbiamo raccolto nulla, registra un errore
        if not standings_data:
            error_msg = f"Impossibile ottenere classifica per il campionato {league_id} da qualsiasi fonte"
            logger.error(error_msg)
            self.errors.append({
                'timestamp': datetime.now().isoformat(),
                'type': 'standings_collection',
                'league_id': league_id,
                'message': error_msg,
                'details': errors
            })
            return {}
        
        # Standardizza e arricchisci i dati
        processed_standings = self.standings_processor.process(standings_data)
        
        # Aggiorna timestamp di ultimo aggiornamento
        self.db.get_reference(f"data/standings/{league_id}/last_updated").set(datetime.now().isoformat())
        
        return processed_standings
    
    def collect_match_predictions(self, match_id: str) -> Dict[str, Any]:
        """
        Raccoglie pronostici per una partita specifica.
        
        Args:
            match_id: ID della partita
            
        Returns:
            Dizionario con pronostici
        """
        logger.info(f"Raccolta pronostici per partita {match_id}")
        
        # Ottieni dati partita
        match_data = self.db.get_reference(f"data/matches/{match_id}").get()
        if not match_data:
            logger.error(f"Dati partita non trovati: {match_id}")
            return {}
        
        # Ottieni statistiche squadre
        home_team_id = match_data.get('home_team', {}).get('id')
        away_team_id = match_data.get('away_team', {}).get('id')
        
        if not home_team_id or not away_team_id:
            logger.error(f"ID squadre mancanti per partita {match_id}")
            return {}
        
        # Raccogli statistiche squadre se non già disponibili
        home_team_stats = self.db.get_reference(f"data/teams/{home_team_id}").get()
        if not home_team_stats:
            home_team_stats = self.collect_team_stats(home_team_id)
        
        away_team_stats = self.db.get_reference(f"data/teams/{away_team_id}").get()
        if not away_team_stats:
            away_team_stats = self.collect_team_stats(away_team_id)
        
        # Raccogli scontri diretti
        h2h_data = self.collect_head_to_head(home_team_id, away_team_id)
        
        # Raccogli quote se disponibili
        odds_data = {}
        try:
            odds_data = self.api_football.get_odds(match_id)
        except Exception as e:
            logger.warning(f"Impossibile ottenere quote per partita {match_id}: {e}")
        
        # Prepara dati per l'analisi
        prediction_data = {
            'match': match_data,
            'home_team': home_team_stats,
            'away_team': away_team_stats,
            'head_to_head': h2h_data,
            'odds': odds_data
        }
        
        # Aggiorna timestamp di ultimo aggiornamento
        self.db.get_reference(f"data/predictions/{match_id}/last_updated").set(datetime.now().isoformat())
        
        return prediction_data
    
    def refresh_league_data(self, league_id: str) -> Dict[str, Any]:
        """
        Aggiorna tutti i dati relativi a un campionato.
        
        Args:
            league_id: ID del campionato
            
        Returns:
            Dizionario con informazioni sulle operazioni svolte
        """
        logger.info(f"Aggiornamento completo dati per campionato {league_id}")
        
        results = {
            'timestamp': datetime.now().isoformat(),
            'league_id': league_id,
            'matches': {'count': 0, 'success': False},
            'standings': {'success': False},
            'teams': {'count': 0, 'success': False},
            'predictions': {'count': 0, 'success': False},
            'errors': []
        }
        
        # Ottieni partite
        try:
            matches = self.collect_matches(league_id)
            results['matches']['count'] = len(matches)
            results['matches']['success'] = len(matches) > 0
            
            # Salva partite in Firebase
            if matches:
                matches_ref = self.db.get_reference(f"data/matches/{league_id}/items")
                matches_ref.set({match['id']: match for match in matches})
        except Exception as e:
            error_msg = f"Errore nell'aggiornamento partite per {league_id}: {str(e)}"
            logger.error(error_msg)
            results['errors'].append(error_msg)
        
        # Ottieni classifica
        try:
            standings = self.collect_league_standings(league_id)
            results['standings']['success'] = bool(standings)
            
            # Salva classifica in Firebase
            if standings:
                standings_ref = self.db.get_reference(f"data/standings/{league_id}")
                standings_ref.set(standings)
        except Exception as e:
            error_msg = f"Errore nell'aggiornamento classifica per {league_id}: {str(e)}"
            logger.error(error_msg)
            results['errors'].append(error_msg)
        
        # Raccogli dati squadre
        if results['matches']['success']:
            # Estrai ID squadre dalle partite
            team_ids = set()
            for match in matches:
                if 'home_team' in match and 'id' in match['home_team']:
                    team_ids.add(match['home_team']['id'])
                if 'away_team' in match and 'id' in match['away_team']:
                    team_ids.add(match['away_team']['id'])
            
            # Ottieni statistiche per ogni squadra
            success_count = 0
            for team_id in team_ids:
                try:
                    team_stats = self.collect_team_stats(team_id)
                    if team_stats:
                        # Salva statistiche squadra in Firebase
                        team_ref = self.db.get_reference(f"data/teams/{team_id}")
                        team_ref.set(team_stats)
                        success_count += 1
                except Exception as e:
                    error_msg = f"Errore nell'aggiornamento statistiche per squadra {team_id}: {str(e)}"
                    logger.error(error_msg)
                    results['errors'].append(error_msg)
            
            results['teams']['count'] = success_count
            results['teams']['success'] = success_count > 0
        
        # Raccogli pronostici per partite future
        if results['matches']['success']:
            # Filtra solo partite future
            now = get_current_datetime()
            future_matches = [m for m in matches if m.get('datetime') and datetime.fromisoformat(m['datetime']) > now]
            
            # Ottieni pronostici per ogni partita
            success_count = 0
            for match in future_matches:
                try:
                    match_id = match['id']
                    prediction_data = self.collect_match_predictions(match_id)
                    if prediction_data:
                        # Salva dati pronostici in Firebase
                        prediction_ref = self.db.get_reference(f"data/predictions/{match_id}")
                        prediction_ref.set(prediction_data)
                        success_count += 1
                except Exception as e:
                    error_msg = f"Errore nella generazione pronostici per partita {match.get('id')}: {str(e)}"
                    logger.error(error_msg)
                    results['errors'].append(error_msg)
            
            results['predictions']['count'] = success_count
            results['predictions']['success'] = success_count > 0
        
        # Aggiorna timestamp di ultimo aggiornamento completo
        self.db.get_reference(f"data/leagues/{league_id}/last_full_update").set(datetime.now().isoformat())
        
        # Registra risultati
        self.db.get_reference(f"logs/data_collection/{league_id}").push(results)
        
        logger.info(f"Aggiornamento completato per {league_id}: {results['matches']['count']} partite, " + 
                    f"{results['teams']['count']} squadre, {results['predictions']['count']} pronostici, " +
                    f"{len(results['errors'])} errori")
        
        return results
    
    def refresh_all_leagues(self, active_only: bool = True) -> Dict[str, Any]:
        """
        Aggiorna i dati per tutti i campionati.
        
        Args:
            active_only: Se aggiornare solo i campionati attivi
            
        Returns:
            Dizionario con risultati per ogni campionato
        """
        logger.info(f"Avvio aggiornamento completo per {'campionati attivi' if active_only else 'tutti i campionati'}")
        
        # Ottieni campionati
        leagues = get_active_leagues() if active_only else get_league(None)
        
        results = {
            'timestamp': datetime.now().isoformat(),
            'leagues_count': len(leagues),
            'leagues_processed': 0,
            'leagues_success': 0,
            'total_matches': 0,
            'total_teams': 0,
            'total_predictions': 0,
            'errors': []
        }
        
        # Aggiorna ogni campionato
        for league_id, league_data in leagues.items():
            try:
                league_result = self.refresh_league_data(league_id)
                
                # Aggiorna contatori
                results['leagues_processed'] += 1
                if not league_result.get('errors'):
                    results['leagues_success'] += 1
                
                results['total_matches'] += league_result.get('matches', {}).get('count', 0)
                results['total_teams'] += league_result.get('teams', {}).get('count', 0)
                results['total_predictions'] += league_result.get('predictions', {}).get('count', 0)
                
                # Aggiungi eventuali errori
                for error in league_result.get('errors', []):
                    results['errors'].append(f"[{league_id}] {error}")
            except Exception as e:
                error_msg = f"Errore nell'aggiornamento campionato {league_id}: {str(e)}"
                logger.error(error_msg)
                results['errors'].append(error_msg)
        
        # Aggiorna timestamp di ultimo aggiornamento completo
        self.db.get_reference("data/last_full_update").set(datetime.now().isoformat())
        
        # Registra risultati
        self.db.get_reference("logs/data_collection/full_updates").push(results)
        
        logger.info(f"Aggiornamento completo: {results['leagues_success']}/{results['leagues_count']} campionati, " +
                   f"{results['total_matches']} partite, {results['total_teams']} squadre, " +
                   f"{results['total_predictions']} pronostici, {len(results['errors'])} errori")
        
        return results
    
    def collect_data_for_match(self, match_id: str) -> Dict[str, Any]:
        """
        Raccoglie tutti i dati necessari per una specifica partita.
        Utile per aggiornare i dati prima di generare un articolo.
        
        Args:
            match_id: ID della partita
            
        Returns:
            Dizionario con tutti i dati relativi alla partita
        """
        logger.info(f"Raccolta dati completi per partita {match_id}")
        
        # Ottieni dati base partita
        match_data = self.db.get_reference(f"data/matches/{match_id}").get()
        if not match_data:
            logger.error(f"Dati partita non trovati: {match_id}")
            return {}
        
        # Ottieni ID squadre
        home_team_id = match_data.get('home_team', {}).get('id')
        away_team_id = match_data.get('away_team', {}).get('id')
        
        if not home_team_id or not away_team_id:
            logger.error(f"ID squadre mancanti per partita {match_id}")
            return {}
        
        # Raccogli statistiche squadre aggiornate
        home_team_stats = self.collect_team_stats(home_team_id)
        away_team_stats = self.collect_team_stats(away_team_id)
        
        # Raccogli scontri diretti
        h2h_data = self.collect_head_to_head(home_team_id, away_team_id)
        
        # Raccogli classifica campionato
        league_id = match_data.get('competition', {}).get('id')
        standings = {}
        if league_id:
            standings = self.collect_league_standings(league_id)
        
        # Raccogli quote se disponibili
        odds_data = {}
        try:
            odds_data = self.api_football.get_odds(match_id)
        except Exception as e:
            logger.warning(f"Impossibile ottenere quote per partita {match_id}: {e}")
        
        # Combina tutti i dati
        complete_data = {
            'match': match_data,
            'home_team': home_team_stats,
            'away_team': away_team_stats,
            'head_to_head': h2h_data,
            'standings': standings,
            'odds': odds_data,
            'last_updated': datetime.now().isoformat()
        }
        
        # Salva dati completi in Firebase
        self.db.get_reference(f"data/match_data/{match_id}").set(complete_data)
        
        logger.info(f"Dati completi raccolti per partita {match_id}")
        
        return complete_data
    
    def get_error_report(self) -> List[Dict[str, Any]]:
        """
        Ottiene un report degli errori recenti nella raccolta dati.
        
        Returns:
            Lista di errori con timestamp e dettagli
        """
        return self.errors


# Funzioni di utility globali

def collect_league_data(league_id: str) -> Dict[str, Any]:
    """
    Funzione di utility per raccogliere dati di un campionato.
    
    Args:
        league_id: ID del campionato
        
    Returns:
        Risultato dell'operazione
    """
    collector = DataCollector()
    return collector.refresh_league_data(league_id)

def collect_match_data(match_id: str) -> Dict[str, Any]:
    """
    Funzione di utility per raccogliere dati di una partita.
    
    Args:
        match_id: ID della partita
        
    Returns:
        Dati completi della partita
    """
    collector = DataCollector()
    return collector.collect_data_for_match(match_id)

def collect_all_leagues_data(active_only: bool = True) -> Dict[str, Any]:
    """
    Funzione di utility per raccogliere dati di tutti i campionati.
    
    Args:
        active_only: Se raccogliere dati solo per campionati attivi
        
    Returns:
        Risultato dell'operazione
    """
    collector = DataCollector()
    return collector.refresh_all_leagues(active_only)
