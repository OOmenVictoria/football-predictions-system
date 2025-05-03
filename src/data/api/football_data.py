"""
Client API per football-data.org.

Questo modulo fornisce un client per l'API football-data.org, che offre
dati su partite, classifiche, squadre e giocatori per i principali campionati di calcio.
"""

import logging
import time
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union, Tuple

import requests

from src.utils.http import get_json, APIError
from src.utils.cache import cached
from src.config.settings import FOOTBALL_API_KEY
from src.config.leagues import get_league, get_api_code

# Configurazione logger
logger = logging.getLogger(__name__)

class FootballDataAPI:
    """
    Client per l'API di football-data.org.
    
    Fornisce metodi per ottenere dati su partite, classifiche, squadre e competizioni
    dall'API gratuita football-data.org.
    """
    
    BASE_URL = "https://api.football-data.org/v4"
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Inizializza il client API.
        
        Args:
            api_key: Chiave API per football-data.org. Se non fornita, usa quella
                    nelle impostazioni globali.
        """
        self.api_key = api_key or FOOTBALL_API_KEY
        self.headers = {"X-Auth-Token": self.api_key}
        
        if not self.api_key:
            logger.warning("Nessuna API key configurata per football-data.org")
    
    @cached(ttl=3600)  # Cache per 1 ora
    def get_competitions(self) -> List[Dict[str, Any]]:
        """
        Ottiene tutte le competizioni disponibili.
        
        Returns:
            Lista di competizioni, ciascuna come dizionario.
        
        Raises:
            APIError: Se la richiesta all'API fallisce.
        """
        try:
            url = f"{self.BASE_URL}/competitions"
            response = get_json(url, headers=self.headers)
            
            if 'competitions' in response:
                return response['competitions']
            return []
            
        except Exception as e:
            logger.error(f"Errore nell'ottenere le competizioni: {e}")
            raise APIError(f"Errore API football-data: {e}")
    
    @cached(ttl=86400)  # Cache per 1 giorno
    def get_competition(self, competition_code: str) -> Dict[str, Any]:
        """
        Ottiene informazioni dettagliate su una competizione.
        
        Args:
            competition_code: Codice della competizione (es. 'PL' per Premier League).
        
        Returns:
            Dizionario con dettagli della competizione.
        
        Raises:
            APIError: Se la richiesta all'API fallisce.
        """
        try:
            url = f"{self.BASE_URL}/competitions/{competition_code}"
            return get_json(url, headers=self.headers)
        except Exception as e:
            logger.error(f"Errore nell'ottenere la competizione {competition_code}: {e}")
            raise APIError(f"Errore API football-data: {e}")
    
    @cached(ttl=3600)  # Cache per 1 ora
    def get_matches(
        self, 
        competition_code: str, 
        date_from: Optional[str] = None, 
        date_to: Optional[str] = None,
        status: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Ottiene le partite per una competizione.
        
        Args:
            competition_code: Codice della competizione (es. 'PL' per Premier League).
            date_from: Data inizio nel formato ISO (YYYY-MM-DD).
            date_to: Data fine nel formato ISO (YYYY-MM-DD).
            status: Stato delle partite (SCHEDULED, LIVE, IN_PLAY, PAUSED, FINISHED, etc.)
        
        Returns:
            Lista di partite, ciascuna come dizionario.
        
        Raises:
            APIError: Se la richiesta all'API fallisce.
        """
        try:
            url = f"{self.BASE_URL}/competitions/{competition_code}/matches"
            params = {}
            
            if date_from:
                params['dateFrom'] = date_from
            if date_to:
                params['dateTo'] = date_to
            if status:
                params['status'] = status
            
            response = get_json(url, params=params, headers=self.headers)
            
            if 'matches' in response:
                return response['matches']
            return []
            
        except Exception as e:
            logger.error(f"Errore nell'ottenere le partite per {competition_code}: {e}")
            raise APIError(f"Errore API football-data: {e}")
    
    @cached(ttl=3600)  # Cache per 1 ora
    def get_match(self, match_id: int) -> Dict[str, Any]:
        """
        Ottiene informazioni dettagliate su una partita.
        
        Args:
            match_id: ID della partita.
        
        Returns:
            Dizionario con dettagli della partita.
        
        Raises:
            APIError: Se la richiesta all'API fallisce.
        """
        try:
            url = f"{self.BASE_URL}/matches/{match_id}"
            return get_json(url, headers=self.headers)
        except Exception as e:
            logger.error(f"Errore nell'ottenere la partita {match_id}: {e}")
            raise APIError(f"Errore API football-data: {e}")
    
    @cached(ttl=3600*12)  # Cache per 12 ore
    def get_team(self, team_id: int) -> Dict[str, Any]:
        """
        Ottiene informazioni dettagliate su una squadra.
        
        Args:
            team_id: ID della squadra.
        
        Returns:
            Dizionario con dettagli della squadra.
        
        Raises:
            APIError: Se la richiesta all'API fallisce.
        """
        try:
            url = f"{self.BASE_URL}/teams/{team_id}"
            return get_json(url, headers=self.headers)
        except Exception as e:
            logger.error(f"Errore nell'ottenere la squadra {team_id}: {e}")
            raise APIError(f"Errore API football-data: {e}")
    
    @cached(ttl=3600*3)  # Cache per 3 ore
    def get_team_matches(
        self, 
        team_id: int, 
        status: Optional[str] = None,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Ottiene le partite recenti o programmate di una squadra.
        
        Args:
            team_id: ID della squadra.
            status: Stato delle partite (SCHEDULED, FINISHED, etc.)
            limit: Numero massimo di partite da restituire.
        
        Returns:
            Lista di partite, ciascuna come dizionario.
        
        Raises:
            APIError: Se la richiesta all'API fallisce.
        """
        try:
            url = f"{self.BASE_URL}/teams/{team_id}/matches"
            params = {"limit": limit}
            
            if status:
                params['status'] = status
            
            response = get_json(url, params=params, headers=self.headers)
            
            if 'matches' in response:
                return response['matches'][:limit]
            return []
            
        except Exception as e:
            logger.error(f"Errore nell'ottenere le partite per la squadra {team_id}: {e}")
            raise APIError(f"Errore API football-data: {e}")
    
    @cached(ttl=3600*6)  # Cache per 6 ore
    def get_standings(self, competition_code: str) -> List[Dict[str, Any]]:
        """
        Ottiene la classifica per una competizione.
        
        Args:
            competition_code: Codice della competizione (es. 'PL' per Premier League).
        
        Returns:
            Lista di gruppi di classifica, ciascuno con una lista di posizioni (standings).
        
        Raises:
            APIError: Se la richiesta all'API fallisce.
        """
        try:
            url = f"{self.BASE_URL}/competitions/{competition_code}/standings"
            response = get_json(url, headers=self.headers)
            
            if 'standings' in response:
                return response['standings']
            return []
            
        except Exception as e:
            logger.error(f"Errore nell'ottenere la classifica per {competition_code}: {e}")
            raise APIError(f"Errore API football-data: {e}")
    
    @cached(ttl=86400)  # Cache per 1 giorno
    def get_scorers(self, competition_code: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Ottiene i marcatori per una competizione.
        
        Args:
            competition_code: Codice della competizione (es. 'PL' per Premier League).
            limit: Numero massimo di marcatori da restituire.
        
        Returns:
            Lista di marcatori con statistiche.
        
        Raises:
            APIError: Se la richiesta all'API fallisce.
        """
        try:
            url = f"{self.BASE_URL}/competitions/{competition_code}/scorers"
            params = {"limit": limit}
            
            response = get_json(url, params=params, headers=self.headers)
            
            if 'scorers' in response:
                return response['scorers']
            return []
            
        except Exception as e:
            logger.error(f"Errore nell'ottenere i marcatori per {competition_code}: {e}")
            raise APIError(f"Errore API football-data: {e}")
    
    def get_upcoming_matches(self, days: int = 7) -> List[Dict[str, Any]]:
        """
        Ottiene tutte le partite in programma per i prossimi giorni.
        
        Args:
            days: Numero di giorni per cui cercare partite future.
        
        Returns:
            Lista di partite in programma.
        """
        from src.config.leagues import get_active_leagues
        
        today = datetime.now().strftime("%Y-%m-%d")
        future = (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d")
        
        all_matches = []
        
        for league in get_active_leagues():
            league_code = get_api_code(league['id'], 'football_data')
            if not league_code:
                continue
                
            try:
                matches = self.get_matches(
                    league_code, 
                    date_from=today,
                    date_to=future,
                    status="SCHEDULED"
                )
                
                # Aggiungi nome campionato per reference
                for match in matches:
                    match['league_id'] = league['id']
                    match['league_name'] = league['name']
                
                all_matches.extend(matches)
                
                # Rispetta il rate limiting
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Errore nell'ottenere partite future per {league['name']}: {e}")
        
        return sorted(all_matches, key=lambda m: m.get('utcDate', ''))
    
    def get_match_head_to_head(self, team1_id: int, team2_id: int, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Ottiene lo storico degli scontri diretti tra due squadre.
        
        Args:
            team1_id: ID della prima squadra.
            team2_id: ID della seconda squadra.
            limit: Numero massimo di partite da restituire.
        
        Returns:
            Lista di partite tra le due squadre.
        """
        try:
            # Ottieni partite recenti per entrambe le squadre
            team1_matches = self.get_team_matches(team1_id, status="FINISHED", limit=50)
            
            # Filtra solo le partite contro team2
            h2h_matches = []
            
            for match in team1_matches:
                home_id = match.get('homeTeam', {}).get('id')
                away_id = match.get('awayTeam', {}).get('id')
                
                if (home_id == team1_id and away_id == team2_id) or \
                   (home_id == team2_id and away_id == team1_id):
                    h2h_matches.append(match)
            
            # Ordina per data (dalla più recente)
            h2h_matches.sort(key=lambda m: m.get('utcDate', ''), reverse=True)
            
            return h2h_matches[:limit]
            
        except Exception as e:
            logger.error(f"Errore nell'ottenere head-to-head per {team1_id} vs {team2_id}: {e}")
            return []

    def get_team_form(self, team_id: int, matches: int = 5) -> Dict[str, Any]:
        """
        Calcola la forma recente di una squadra.
        
        Args:
            team_id: ID della squadra.
            matches: Numero di partite da considerare.
        
        Returns:
            Dizionario con statistiche sulla forma recente.
        """
        try:
            # Ottieni partite recenti
            recent_matches = self.get_team_matches(team_id, status="FINISHED", limit=matches)
            
            form = []
            goals_for = 0
            goals_against = 0
            wins = 0
            draws = 0
            losses = 0
            
            for match in recent_matches:
                home_id = match.get('homeTeam', {}).get('id')
                away_id = match.get('awayTeam', {}).get('id')
                
                home_score = match.get('score', {}).get('fullTime', {}).get('home', 0) or 0
                away_score = match.get('score', {}).get('fullTime', {}).get('away', 0) or 0
                
                # Determina il risultato dal punto di vista della squadra
                if home_id == team_id:
                    team_goals = home_score
                    opponent_goals = away_score
                else:
                    team_goals = away_score
                    opponent_goals = home_score
                
                # Aggiorna statistiche
                goals_for += team_goals
                goals_against += opponent_goals
                
                if team_goals > opponent_goals:
                    result = 'W'
                    wins += 1
                elif team_goals < opponent_goals:
                    result = 'L'
                    losses += 1
                else:
                    result = 'D'
                    draws += 1
                
                form.append(result)
            
            # Calcola punti
            points = wins * 3 + draws
            max_points = len(recent_matches) * 3
            
            # Forma in percentuale
            form_percentage = (points / max_points * 100) if max_points > 0 else 0
            
            return {
                'form': form,
                'form_string': ''.join(form),
                'form_percentage': round(form_percentage, 1),
                'matches_played': len(recent_matches),
                'wins': wins,
                'draws': draws,
                'losses': losses,
                'goals_for': goals_for,
                'goals_against': goals_against,
                'goal_difference': goals_for - goals_against,
                'points': points,
                'last_updated': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Errore nel calcolare la forma per la squadra {team_id}: {e}")
            return {
                'form': [],
                'form_string': '',
                'form_percentage': 0,
                'matches_played': 0,
                'error': str(e)
            }


# Istanza globale per un utilizzo più semplice
football_data_api = FootballDataAPI()

def get_api():
    """
    Ottiene l'istanza globale dell'API.
    
    Returns:
        Istanza di FootballDataAPI.
    """
    return football_data_api

# Funzioni wrapper per permettere import diretto delle funzioni

def get_competitions() -> List[Dict[str, Any]]:
    """
    Wrapper per football_data_api.get_competitions().
    Ottiene l'elenco delle competizioni disponibili.
    
    Returns:
        Lista delle competizioni disponibili.
    """
    try:
        result = football_data_api.get_competitions()
        if isinstance(result, list):
            return result
        return []
    except Exception as e:
        logger.error(f"Errore in get_competitions(): {e}")
        return []

def get_matches(competition_id: str, **kwargs) -> List[Dict[str, Any]]:
    """
    Wrapper per football_data_api.get_matches().
    Ottiene le partite per una competizione.
    
    Args:
        competition_id: ID o codice della competizione.
        **kwargs: Parametri aggiuntivi (date_from, date_to, status, ecc.).
    
    Returns:
        Lista delle partite.
    """
    try:
        result = football_data_api.get_matches(competition_id, **kwargs)
        if isinstance(result, list):
            return result
        return []
    except Exception as e:
        logger.error(f"Errore in get_matches({competition_id}): {e}")
        return []

def get_team(team_id: int) -> Dict[str, Any]:
    """
    Wrapper per football_data_api.get_team().
    Ottiene informazioni su una squadra.
    
    Args:
        team_id: ID della squadra.
    
    Returns:
        Informazioni sulla squadra.
    """
    try:
        return football_data_api.get_team(team_id)
    except Exception as e:
        logger.error(f"Errore in get_team({team_id}): {e}")
        return {}

def get_team_matches(team_id: int, **kwargs) -> List[Dict[str, Any]]:
    """
    Wrapper per football_data_api.get_team_matches().
    Ottiene le partite di una squadra.
    
    Args:
        team_id: ID della squadra.
        **kwargs: Parametri aggiuntivi (status, limit, ecc.).
    
    Returns:
        Lista delle partite della squadra.
    """
    try:
        result = football_data_api.get_team_matches(team_id, **kwargs)
        if isinstance(result, list):
            return result
        return []
    except Exception as e:
        logger.error(f"Errore in get_team_matches({team_id}): {e}")
        return []

def get_match(match_id: int) -> Dict[str, Any]:
    """
    Wrapper per football_data_api.get_match().
    Ottiene informazioni su una partita specifica.
    
    Args:
        match_id: ID della partita.
    
    Returns:
        Informazioni sulla partita.
    """
    try:
        return football_data_api.get_match(match_id)
    except Exception as e:
        logger.error(f"Errore in get_match({match_id}): {e}")
        return {}

def get_standings(competition_id: str) -> List[Dict[str, Any]]:
    """
    Wrapper per football_data_api.get_standings().
    Ottiene la classifica di una competizione.
    
    Args:
        competition_id: ID o codice della competizione.
    
    Returns:
        Classifica della competizione.
    """
    try:
        result = football_data_api.get_standings(competition_id)
        if isinstance(result, list):
            return result
        return []
    except Exception as e:
        logger.error(f"Errore in get_standings({competition_id}): {e}")
        return []
