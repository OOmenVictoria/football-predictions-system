"""
Client API per API-Football (RapidAPI).

Questo modulo fornisce un client per l'API API-Football disponibile su RapidAPI,
che offre dati calcistici dettagliati come partite, statistiche, giocatori, etc.
"""

import logging
import time
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union, Tuple

import requests

from src.utils.http import get_json, APIError
from src.utils.cache import cached
from src.config.settings import RAPIDAPI_KEY
from src.config.leagues import get_league, get_api_code

# Configurazione logger
logger = logging.getLogger(__name__)

class APIFootball:
    """
    Client per l'API di API-Football (RapidAPI).
    
    Fornisce metodi per ottenere dati dettagliati su partite, squadre, giocatori,
    statistiche e pronostici attraverso l'API API-Football.
    """
    
    BASE_URL = "https://api-football-v1.p.rapidapi.com/v3"
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Inizializza il client API.
        
        Args:
            api_key: Chiave API per RapidAPI. Se non fornita, usa quella
                    nelle impostazioni globali.
        """
        self.api_key = api_key or RAPIDAPI_KEY
        self.headers = {
            "X-RapidAPI-Key": self.api_key,
            "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"
        }
        
        if not self.api_key:
            logger.warning("Nessuna API key configurata per API-Football (RapidAPI)")
    
    @cached(ttl=86400)  # Cache per 1 giorno
    def get_leagues(self, season: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Ottiene tutte le competizioni disponibili.
        
        Args:
            season: Anno della stagione (es. 2023).
        
        Returns:
            Lista di competizioni, ciascuna come dizionario.
        
        Raises:
            APIError: Se la richiesta all'API fallisce.
        """
        try:
            url = f"{self.BASE_URL}/leagues"
            params = {}
            
            if season:
                params['season'] = season
            
            response = get_json(url, params=params, headers=self.headers)
            
            if 'response' in response:
                return response['response']
            return []
            
        except Exception as e:
            logger.error(f"Errore nell'ottenere le competizioni: {e}")
            raise APIError(f"Errore API-Football: {e}")
    
    @cached(ttl=3600)  # Cache per 1 ora
    def get_fixtures(
        self, 
        league_id: Optional[int] = None,
        team_id: Optional[int] = None,
        date: Optional[str] = None,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        status: Optional[str] = None,
        season: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Ottiene le partite in base ai parametri forniti.
        
        Args:
            league_id: ID della competizione.
            team_id: ID della squadra.
            date: Data specifica (YYYY-MM-DD).
            from_date: Data inizio (YYYY-MM-DD).
            to_date: Data fine (YYYY-MM-DD).
            status: Stato delle partite ('NS', 'FT', 'LIVE', etc.)
            season: Anno della stagione.
        
        Returns:
            Lista di partite, ciascuna come dizionario.
        
        Raises:
            APIError: Se la richiesta all'API fallisce.
        """
        try:
            url = f"{self.BASE_URL}/fixtures"
            params = {}
            
            # Imposta parametri che sono stati forniti
            if league_id:
                params['league'] = league_id
            if team_id:
                params['team'] = team_id
            if date:
                params['date'] = date
            if from_date and to_date:
                params['from'] = from_date
                params['to'] = to_date
            if status:
                params['status'] = status
            if season:
                params['season'] = season
            
            # Almeno un parametro deve essere fornito
            if not params:
                logger.error("Nessun parametro fornito per get_fixtures")
                return []
            
            response = get_json(url, params=params, headers=self.headers)
            
            if 'response' in response:
                return response['response']
            return []
            
        except Exception as e:
            logger.error(f"Errore nell'ottenere le partite: {e}")
            raise APIError(f"Errore API-Football: {e}")
    
    @cached(ttl=3600)  # Cache per 1 ora
    def get_fixture(self, fixture_id: int) -> Dict[str, Any]:
        """
        Ottiene informazioni dettagliate su una partita.
        
        Args:
            fixture_id: ID della partita.
        
        Returns:
            Dizionario con dettagli della partita.
        
        Raises:
            APIError: Se la richiesta all'API fallisce.
        """
        try:
            url = f"{self.BASE_URL}/fixtures"
            params = {"id": fixture_id}
            
            response = get_json(url, params=params, headers=self.headers)
            
            if 'response' in response and response['response']:
                return response['response'][0]
            return {}
            
        except Exception as e:
            logger.error(f"Errore nell'ottenere la partita {fixture_id}: {e}")
            raise APIError(f"Errore API-Football: {e}")
    
    @cached(ttl=3600)  # Cache per 1 ora
    def get_fixture_statistics(self, fixture_id: int) -> Dict[str, Any]:
        """
        Ottiene statistiche dettagliate per una partita.
        
        Args:
            fixture_id: ID della partita.
        
        Returns:
            Dizionario con statistiche della partita per entrambe le squadre.
        
        Raises:
            APIError: Se la richiesta all'API fallisce.
        """
        try:
            url = f"{self.BASE_URL}/fixtures/statistics"
            params = {"fixture": fixture_id}
            
            response = get_json(url, params=params, headers=self.headers)
            
            if 'response' in response:
                # Organizza le statistiche per squadra
                stats = {}
                for team_stats in response['response']:
                    team_id = team_stats.get('team', {}).get('id')
                    if team_id:
                        stats[str(team_id)] = team_stats.get('statistics', [])
                return stats
            return {}
            
        except Exception as e:
            logger.error(f"Errore nell'ottenere le statistiche per la partita {fixture_id}: {e}")
            raise APIError(f"Errore API-Football: {e}")
    
    @cached(ttl=3600)  # Cache per 1 ora
    def get_fixture_events(self, fixture_id: int) -> List[Dict[str, Any]]:
        """
        Ottiene gli eventi di una partita (gol, cartellini, sostituzioni).
        
        Args:
            fixture_id: ID della partita.
        
        Returns:
            Lista di eventi della partita.
        
        Raises:
            APIError: Se la richiesta all'API fallisce.
        """
        try:
            url = f"{self.BASE_URL}/fixtures/events"
            params = {"fixture": fixture_id}
            
            response = get_json(url, params=params, headers=self.headers)
            
            if 'response' in response:
                return response['response']
            return []
            
        except Exception as e:
            logger.error(f"Errore nell'ottenere gli eventi per la partita {fixture_id}: {e}")
            raise APIError(f"Errore API-Football: {e}")
    
    @cached(ttl=3600)  # Cache per 1 ora
    def get_fixture_lineups(self, fixture_id: int) -> Dict[str, Any]:
        """
        Ottiene le formazioni delle squadre per una partita.
        
        Args:
            fixture_id: ID della partita.
        
        Returns:
            Dizionario con formazioni per entrambe le squadre.
        
        Raises:
            APIError: Se la richiesta all'API fallisce.
        """
        try:
            url = f"{self.BASE_URL}/fixtures/lineups"
            params = {"fixture": fixture_id}
            
            response = get_json(url, params=params, headers=self.headers)
            
            if 'response' in response:
                # Organizza le formazioni per squadra
                lineups = {}
                for team_lineup in response['response']:
                    team_id = team_lineup.get('team', {}).get('id')
                    if team_id:
                        lineups[str(team_id)] = team_lineup
                return lineups
            return {}
            
        except Exception as e:
            logger.error(f"Errore nell'ottenere le formazioni per la partita {fixture_id}: {e}")
            raise APIError(f"Errore API-Football: {e}")
    
    @cached(ttl=86400)  # Cache per 1 giorno
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
            url = f"{self.BASE_URL}/teams"
            params = {"id": team_id}
            
            response = get_json(url, params=params, headers=self.headers)
            
            if 'response' in response and response['response']:
                return response['response'][0]
            return {}
            
        except Exception as e:
            logger.error(f"Errore nell'ottenere la squadra {team_id}: {e}")
            raise APIError(f"Errore API-Football: {e}")
    
    @cached(ttl=21600)  # Cache per 6 ore
    def get_team_statistics(
        self, 
        team_id: int, 
        league_id: int, 
        season: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Ottiene statistiche dettagliate per una squadra in una competizione.
        
        Args:
            team_id: ID della squadra.
            league_id: ID della competizione.
            season: Anno della stagione.
        
        Returns:
            Dizionario con statistiche della squadra.
        
        Raises:
            APIError: Se la richiesta all'API fallisce.
        """
        try:
            url = f"{self.BASE_URL}/teams/statistics"
            params = {
                "team": team_id,
                "league": league_id
            }
            
            if season:
                params["season"] = season
            
            response = get_json(url, params=params, headers=self.headers)
            
            if 'response' in response:
                return response['response']
            return {}
            
        except Exception as e:
            logger.error(f"Errore nell'ottenere le statistiche per la squadra {team_id}: {e}")
            raise APIError(f"Errore API-Football: {e}")
    
    @cached(ttl=86400)  # Cache per 1 giorno
    def get_players(
        self, 
        team_id: int, 
        league_id: Optional[int] = None, 
        season: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Ottiene i giocatori di una squadra.
        
        Args:
            team_id: ID della squadra.
            league_id: ID della competizione.
            season: Anno della stagione.
        
        Returns:
            Lista di giocatori con statistiche.
        
        Raises:
            APIError: Se la richiesta all'API fallisce.
        """
        try:
            url = f"{self.BASE_URL}/players"
            params = {"team": team_id}
            
            if league_id:
                params["league"] = league_id
            if season:
                params["season"] = season
            
            # API-Football richiede la stagione
            if not season:
                from datetime import datetime
                current_year = datetime.now().year
                params["season"] = current_year
            
            response = get_json(url, params=params, headers=self.headers)
            
            if 'response' in response:
                return response['response']
            return []
            
        except Exception as e:
            logger.error(f"Errore nell'ottenere i giocatori per la squadra {team_id}: {e}")
            raise APIError(f"Errore API-Football: {e}")
    
    @cached(ttl=21600)  # Cache per 6 ore
    def get_standings(
        self, 
        league_id: int, 
        season: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Ottiene la classifica per una competizione.
        
        Args:
            league_id: ID della competizione.
            season: Anno della stagione.
        
        Returns:
            Lista di classifiche (possono esserci più gruppi/leghe).
        
        Raises:
            APIError: Se la richiesta all'API fallisce.
        """
        try:
            url = f"{self.BASE_URL}/standings"
            params = {"league": league_id}
            
            if season:
                params["season"] = season
            else:
                from datetime import datetime
                current_year = datetime.now().year
                params["season"] = current_year
            
            response = get_json(url, params=params, headers=self.headers)
            
            if 'response' in response:
                # API restituisce una lista di leghe, ciascuna con le proprie classifiche
                # Estrae direttamente le classifiche
                all_standings = []
                for league_data in response['response']:
                    if 'league' in league_data and 'standings' in league_data['league']:
                        all_standings.extend(league_data['league']['standings'])
                return all_standings
            return []
            
        except Exception as e:
            logger.error(f"Errore nell'ottenere la classifica per {league_id}: {e}")
            raise APIError(f"Errore API football-data: {e}")
    
    @cached(ttl=3600)  # Cache per 1 ora
    def get_predictions(self, fixture_id: int) -> Dict[str, Any]:
        """
        Ottiene pronostici per una partita.
        
        Args:
            fixture_id: ID della partita.
        
        Returns:
            Dizionario con pronostici per la partita.
        
        Raises:
            APIError: Se la richiesta all'API fallisce.
        """
        try:
            url = f"{self.BASE_URL}/predictions"
            params = {"fixture": fixture_id}
            
            response = get_json(url, params=params, headers=self.headers)
            
            if 'response' in response and response['response']:
                return response['response'][0]
            return {}
            
        except Exception as e:
            logger.error(f"Errore nell'ottenere i pronostici per la partita {fixture_id}: {e}")
            raise APIError(f"Errore API-Football: {e}")
    
    @cached(ttl=86400)  # Cache per 1 giorno
    def get_odds(
        self, 
        fixture_id: int, 
        bookmaker_id: Optional[int] = None, 
        bet_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Ottiene le quote per una partita.
        
        Args:
            fixture_id: ID della partita.
            bookmaker_id: ID del bookmaker.
            bet_id: ID del tipo di scommessa.
        
        Returns:
            Dizionario con quote per la partita.
        
        Raises:
            APIError: Se la richiesta all'API fallisce.
        """
        try:
            url = f"{self.BASE_URL}/odds"
            params = {"fixture": fixture_id}
            
            if bookmaker_id:
                params["bookmaker"] = bookmaker_id
            if bet_id:
                params["bet"] = bet_id
            
            response = get_json(url, params=params, headers=self.headers)
            
            if 'response' in response and response['response']:
                return response['response'][0]
            return {}
            
        except Exception as e:
            logger.error(f"Errore nell'ottenere le quote per la partita {fixture_id}: {e}")
            raise APIError(f"Errore API-Football: {e}")
    
    def get_upcoming_matches(
        self, 
        days: int = 7, 
        leagues: Optional[List[int]] = None
    ) -> List[Dict[str, Any]]:
        """
        Ottiene tutte le partite in programma per i prossimi giorni.
        
        Args:
            days: Numero di giorni per cui cercare partite future.
            leagues: Lista di ID delle competizioni da considerare.
        
        Returns:
            Lista di partite in programma.
        """
        from src.config.leagues import get_active_leagues
        
        today = datetime.now().strftime("%Y-%m-%d")
        future = (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d")
        
        if not leagues:
            # Ottieni tutti i campionati attivi
            active_leagues = get_active_leagues()
            leagues = []
            for league in active_leagues:
                league_code = get_api_code(league['id'], 'api_football')
                if league_code:
                    leagues.append(int(league_code))
        
        all_matches = []
        
        for league_id in leagues:
            try:
                matches = self.get_fixtures(
                    league_id=league_id,
                    from_date=today,
                    to_date=future,
                    status="NS"  # Non iniziate
                )
                
                all_matches.extend(matches)
                
                # Rispetta il rate limiting
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Errore nell'ottenere partite future per lega {league_id}: {e}")
        
        # Ordina per data
        return sorted(all_matches, key=lambda m: m.get('fixture', {}).get('date', ''))
    
    def get_match_head_to_head(
        self, 
        team1_id: int, 
        team2_id: int, 
        limit: int = 10
    ) -> List[Dict[str, Any]]:
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
            url = f"{self.BASE_URL}/fixtures/headtohead"
            params = {
                "h2h": f"{team1_id}-{team2_id}",
                "last": limit
            }
            
            response = get_json(url, params=params, headers=self.headers)
            
            if 'response' in response:
                return response['response']
            return []
            
        except Exception as e:
            logger.error(f"Errore nell'ottenere head-to-head per {team1_id} vs {team2_id}: {e}")
            return []
    
    def get_team_form(self, team_id: int, league_id: int = None, matches: int = 5) -> Dict[str, Any]:
        """
        Calcola la forma recente di una squadra.
        
        Args:
            team_id: ID della squadra.
            league_id: ID della competizione (opzionale).
            matches: Numero di partite da considerare.
        
        Returns:
            Dizionario con statistiche sulla forma recente.
        """
        try:
            # Ottieni partite recenti
            params = {
                "team": team_id,
                "status": "FT",  # Partite finite
                "last": matches
            }
            
            if league_id:
                params["league"] = league_id
            
            url = f"{self.BASE_URL}/fixtures"
            response = get_json(url, params=params, headers=self.headers)
            
            if 'response' not in response:
                return {
                    'form': [],
                    'form_string': '',
                    'matches_played': 0,
                    'error': 'Nessuna risposta valida dall\'API'
                }
            
            recent_matches = response['response']
            
            form = []
            goals_for = 0
            goals_against = 0
            wins = 0
            draws = 0
            losses = 0
            
            for match in recent_matches:
                fixture = match.get('fixture', {})
                teams = match.get('teams', {})
                goals = match.get('goals', {})
                
                # Determina se la squadra era in casa o in trasferta
                is_home = teams.get('home', {}).get('id') == team_id
                
                # Ottieni i gol
                team_goals = goals.get('home' if is_home else 'away', 0) or 0
                opponent_goals = goals.get('away' if is_home else 'home', 0) or 0
                
                # Aggiorna statistiche
                goals_for += team_goals
                goals_against += opponent_goals
                
                # Determina il risultato
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
            
            # Inverti per avere la forma più recente per prima
            form.reverse()
            
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
api_football = APIFootball()

def get_api():
    """
    Ottiene l'istanza globale dell'API.
    
    Returns:
        Istanza di APIFootball.
    """
    return api_football

# Funzioni wrapper per l'importazione diretta

def get_leagues(season: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Wrapper per api_football.get_leagues().
    Ottiene l'elenco delle competizioni disponibili.
    
    Args:
        season: Anno della stagione (es. 2023).
        
    Returns:
        Lista delle competizioni disponibili.
    """
    try:
        result = api_football.get_leagues(season)
        if isinstance(result, list):
            return result
        return []
    except Exception as e:
        logger.error(f"Errore in get_leagues(): {e}")
        return []

def get_fixtures(league_id: Optional[int] = None, **kwargs) -> List[Dict[str, Any]]:
    """
    Wrapper per api_football.get_fixtures().
    Ottiene le partite in base ai parametri forniti.
    
    Args:
        league_id: ID della competizione.
        **kwargs: Parametri aggiuntivi (team_id, date, from_date, to_date, status, season).
        
    Returns:
        Lista delle partite.
    """
    try:
        result = api_football.get_fixtures(league_id=league_id, **kwargs)
        if isinstance(result, list):
            return result
        return []
    except Exception as e:
        logger.error(f"Errore in get_fixtures(): {e}")
        return []

def get_fixture_details(fixture_id: int) -> Dict[str, Any]:
    """
    Wrapper per api_football.get_fixture().
    Ottiene informazioni dettagliate su una partita.
    
    Args:
        fixture_id: ID della partita.
        
    Returns:
        Dizionario con dettagli della partita.
    """
    try:
        return api_football.get_fixture(fixture_id)
    except Exception as e:
        logger.error(f"Errore in get_fixture_details({fixture_id}): {e}")
        return {}

def get_fixture_statistics(fixture_id: int) -> Dict[str, Any]:
    """
    Wrapper per api_football.get_fixture_statistics().
    Ottiene statistiche dettagliate per una partita.
    
    Args:
        fixture_id: ID della partita.
        
    Returns:
        Dizionario con statistiche della partita per entrambe le squadre.
    """
    try:
        return api_football.get_fixture_statistics(fixture_id)
    except Exception as e:
        logger.error(f"Errore in get_fixture_statistics({fixture_id}): {e}")
        return {}

def get_team_info(team_id: int) -> Dict[str, Any]:
    """
    Wrapper per api_football.get_team().
    Ottiene informazioni dettagliate su una squadra.
    
    Args:
        team_id: ID della squadra.
        
    Returns:
        Dizionario con dettagli della squadra.
    """
    try:
        return api_football.get_team(team_id)
    except Exception as e:
        logger.error(f"Errore in get_team_info({team_id}): {e}")
        return {}

def get_team_statistics(team_id: int, league_id: int, season: Optional[int] = None) -> Dict[str, Any]:
    """
    Wrapper per api_football.get_team_statistics().
    Ottiene statistiche dettagliate per una squadra in una competizione.
    
    Args:
        team_id: ID della squadra.
        league_id: ID della competizione.
        season: Anno della stagione.
        
    Returns:
        Dizionario con statistiche della squadra.
    """
    try:
        return api_football.get_team_statistics(team_id, league_id, season)
    except Exception as e:
        logger.error(f"Errore in get_team_statistics({team_id}, {league_id}): {e}")
        return {}

def get_team_fixtures(team_id: int, **kwargs) -> List[Dict[str, Any]]:
    """
    Wrapper per api_football.get_fixtures() filtrato per una squadra.
    Ottiene le partite di una squadra.
    
    Args:
        team_id: ID della squadra.
        **kwargs: Parametri aggiuntivi (date, from_date, to_date, status, season).
        
    Returns:
        Lista delle partite della squadra.
    """
    try:
        result = api_football.get_fixtures(team_id=team_id, **kwargs)
        if isinstance(result, list):
            return result
        return []
    except Exception as e:
        logger.error(f"Errore in get_team_fixtures({team_id}): {e}")
        return []

def get_standings(league_id: int, season: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Wrapper per api_football.get_standings().
    Ottiene la classifica per una competizione.
    
    Args:
        league_id: ID della competizione.
        season: Anno della stagione.
        
    Returns:
        Lista di classifiche (possono esserci più gruppi/leghe).
    """
    try:
        result = api_football.get_standings(league_id, season)
        if isinstance(result, list):
            return result
        return []
    except Exception as e:
        logger.error(f"Errore in get_standings({league_id}): {e}")
        return []

def get_head_to_head(team1_id: int, team2_id: int, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Wrapper per api_football.get_match_head_to_head().
    Ottiene lo storico degli scontri diretti tra due squadre.
    
    Args:
        team1_id: ID della prima squadra.
        team2_id: ID della seconda squadra.
        limit: Numero massimo di partite da restituire.
        
    Returns:
        Lista di partite tra le due squadre.
    """
    try:
        result = api_football.get_match_head_to_head(team1_id, team2_id, limit)
        if isinstance(result, list):
            return result
        return []
    except Exception as e:
        logger.error(f"Errore in get_head_to_head({team1_id}, {team2_id}): {e}")
        return []
