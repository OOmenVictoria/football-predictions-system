""" 
Modulo per l'estrazione di dati dal repository OpenFootball.
Questo modulo fornisce funzionalità per ottenere dati calcistici dal progetto
OpenFootball su GitHub (https://github.com/openfootball).
"""
import os
import json
import yaml
import logging
import requests
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union, Tuple

from src.utils.cache import cached
from src.utils.http import make_request, download_file
from src.config.settings import get_setting
from src.utils.database import FirebaseManager

logger = logging.getLogger(__name__)

class OpenFootballClient:
    """
    Cliente per l'accesso ai dati dal progetto OpenFootball.
    
    OpenFootball è un progetto open source che fornisce dati calcistici in formato
    YAML e JSON su GitHub.
    """
    
    def __init__(self):
        """Inizializza il client OpenFootball."""
        self.base_url = "https://raw.githubusercontent.com/openfootball"
        self.github_api_url = "https://api.github.com/repos/openfootball"
        self.cache_ttl = get_setting('open_data.open_football.cache_ttl', 86400)  # 24 ore
        self.db = FirebaseManager()
        
        # Repository principali di OpenFootball
        self.repositories = {
            "england": "england",
            "italy": "italy",
            "spain": "spain", 
            "germany": "deutschland",
            "france": "france",
            "champions-league": "champions-league",
            "world-cup": "world-cup",
            "euro": "euro"
        }
        
        # Mapping tra il nome del repo e la stagione corrente
        self.current_season = get_setting('open_data.open_football.current_season', "2024-25")
        
        logger.info(f"OpenFootballClient inizializzato. Stagione: {self.current_season}")
    
    @cached(ttl=86400)  # Cache di 24 ore
    def get_leagues(self) -> Dict[str, Any]:
        """
        Ottiene la lista dei campionati disponibili in OpenFootball.
        
        Returns:
            Dizionario con i campionati disponibili
        """
        leagues = {}
        
        try:
            for league_id, repo_name in self.repositories.items():
                leagues[league_id] = {
                    "id": league_id,
                    "name": self._get_league_name(league_id),
                    "repository": repo_name,
                    "seasons": self._get_available_seasons(repo_name)
                }
            
            return leagues
            
        except Exception as e:
            logger.error(f"Errore nel recupero dei campionati da OpenFootball: {e}")
            return {}
    
    def _get_league_name(self, league_id: str) -> str:
        """
        Converte l'ID del campionato in un nome leggibile.
        
        Args:
            league_id: ID del campionato
            
        Returns:
            Nome leggibile del campionato
        """
        mapping = {
            "england": "Premier League",
            "italy": "Serie A",
            "spain": "La Liga",
            "germany": "Bundesliga",
            "france": "Ligue 1",
            "champions-league": "UEFA Champions League",
            "world-cup": "FIFA World Cup",
            "euro": "UEFA European Championship"
        }
        
        return mapping.get(league_id, league_id.replace("-", " ").title())
    
    @cached(ttl=604800)  # Cache di 1 settimana
    def _get_available_seasons(self, repo_name: str) -> List[str]:
        """
        Ottiene le stagioni disponibili per un repository.
        
        Args:
            repo_name: Nome del repository
            
        Returns:
            Lista delle stagioni disponibili
        """
        try:
            url = f"{self.github_api_url}/{repo_name}/contents"
            response = make_request(url)
            
            if not response:
                return []
            
            data = response.json()
            seasons = []
            
            for item in data:
                if item["type"] == "dir" and (item["name"].startswith("20") or item["name"].startswith("19")):
                    seasons.append(item["name"])
            
            # Ordina le stagioni dalla più recente
            seasons.sort(reverse=True)
            
            return seasons
            
        except Exception as e:
            logger.error(f"Errore nel recupero delle stagioni per {repo_name}: {e}")
            return []
    
    @cached(ttl=86400)  # Cache di 24 ore
    def get_teams(self, league_id: str, season: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Ottiene la lista delle squadre per un campionato e stagione.
        
        Args:
            league_id: ID del campionato
            season: Stagione (formato YYYY-YY, es. "2023-24")
            
        Returns:
            Lista delle squadre
        """
        if not season:
            season = self.current_season
            
        repo = self.repositories.get(league_id)
        if not repo:
            logger.error(f"Campionato non supportato: {league_id}")
            return []
        
        try:
            # Tenta prima di leggere dal file clubs.txt
            clubs_url = f"{self.base_url}/{repo}/master/{season}/clubs.txt"
            response = make_request(clubs_url)
            
            if not response or response.status_code != 200:
                # Prova la posizione alternativa
                clubs_url = f"{self.base_url}/{repo}/master/clubs/clubs.txt"
                response = make_request(clubs_url)
                
                if not response or response.status_code != 200:
                    logger.warning(f"File clubs.txt non trovato per {league_id} stagione {season}")
                    return []
            
            teams = []
            current_team = None
            
            # Parse del file clubs.txt
            lines = response.text.split('\n')
            for line in lines:
                line = line.strip()
                
                # Nuova squadra
                if line and not line.startswith('#') and not line.startswith('['):
                    parts = line.split(',', 1)
                    if len(parts) >= 1:
                        team_name = parts[0].strip()
                        current_team = {
                            "name": team_name,
                            "code": None,
                            "city": None,
                            "country": self._get_country_from_league(league_id)
                        }
                        
                        # Estrai abbreviazione/codice se presente
                        for part in parts[1:] if len(parts) > 1 else []:
                            part = part.strip()
                            if len(part) <= 3 and part.isupper():
                                current_team["code"] = part
                
                # Informazioni aggiuntive sulla squadra
                elif line.startswith("  ") and current_team:
                    if "city" in line.lower():
                        current_team["city"] = line.split(":", 1)[1].strip() if ":" in line else line.strip()
                
                # Fine della squadra, aggiungi alla lista
                elif not line and current_team:
                    teams.append(current_team)
                    current_team = None
            
            # Aggiungi l'ultima squadra se presente
            if current_team:
                teams.append(current_team)
                
            return teams
                
        except Exception as e:
            logger.error(f"Errore nel recupero delle squadre per {league_id} stagione {season}: {e}")
            return []
    
    def _get_country_from_league(self, league_id: str) -> str:
        """
        Determina il paese dalla lega.
        
        Args:
            league_id: ID del campionato
            
        Returns:
            Codice del paese
        """
        mapping = {
            "england": "ENG",
            "italy": "ITA",
            "spain": "ESP",
            "germany": "GER",
            "france": "FRA"
        }
        
        return mapping.get(league_id, "")
    
    @cached(ttl=3600)  # Cache di 1 ora
    def get_matches(self, league_id: str, season: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Ottiene la lista delle partite per un campionato e stagione.
        
        Args:
            league_id: ID del campionato
            season: Stagione (formato YYYY-YY, es. "2023-24")
            
        Returns:
            Lista delle partite
        """
        if not season:
            season = self.current_season
            
        repo = self.repositories.get(league_id)
        if not repo:
            logger.error(f"Campionato non supportato: {league_id}")
            return []
        
        try:
            # Ottieni prima le squadre per i mapping dei nomi
            teams = self.get_teams(league_id, season)
            team_mapping = {team["name"]: team for team in teams}
            
            # Ottieni le partite dal file del campionato
            matches_url = f"{self.base_url}/{repo}/master/{season}/{league_id}.txt"
            response = make_request(matches_url)
            
            if not response or response.status_code != 200:
                logger.warning(f"File del campionato non trovato per {league_id} stagione {season}")
                return []
            
            matches = []
            current_round = None
            current_date = None
            
            # Parse del file del campionato
            lines = response.text.split('\n')
            for line in lines:
                line = line.strip()
                
                # Ignora commenti e linee vuote
                if not line or line.startswith('#'):
                    continue
                
                # Round/Matchday
                if line.startswith('Matchday') or line.startswith('Round'):
                    current_round = line.split()[1]
                    continue
                
                # Data
                if line.startswith('[') and line.endswith(']'):
                    current_date = line[1:-1]  # Rimuovi le parentesi quadre
                    continue
                
                # Partita
                if ' - ' in line and current_date:
                    parts = line.split(' - ')
                    if len(parts) == 2:
                        home_team = parts[0].strip()
                        away_parts = parts[1].split()
                        
                        # Estrai il punteggio se presente
                        score_index = -1
                        for i, part in enumerate(away_parts):
                            if ':' in part and part[0].isdigit() and part[2].isdigit():
                                score_index = i
                                break
                        
                        if score_index >= 0:
                            # Partita già giocata
                            away_team = ' '.join(away_parts[:score_index]).strip()
                            score_parts = away_parts[score_index].split(':')
                            
                            if len(score_parts) == 2:
                                try:
                                    home_score = int(score_parts[0])
                                    away_score = int(score_parts[1])
                                    
                                    match = {
                                        "date": self._format_date(current_date),
                                        "round": current_round,
                                        "home_team": home_team,
                                        "away_team": away_team,
                                        "home_score": home_score,
                                        "away_score": away_score,
                                        "status": "FINISHED",
                                        "league_id": league_id,
                                        "season": season
                                    }
                                    
                                    # Aggiungi informazioni aggiuntive dalle squadre
                                    if home_team in team_mapping:
                                        match["home_team_info"] = team_mapping[home_team]
                                    if away_team in team_mapping:
                                        match["away_team_info"] = team_mapping[away_team]
                                    
                                    matches.append(match)
                                except ValueError:
                                    logger.warning(f"Formato punteggio non valido: {away_parts[score_index]}")
                        else:
                            # Partita futura
                            away_team = ' '.join(away_parts).strip()
                            
                            match = {
                                "date": self._format_date(current_date),
                                "round": current_round,
                                "home_team": home_team,
                                "away_team": away_team,
                                "status": "SCHEDULED",
                                "league_id": league_id,
                                "season": season
                            }
                            
                            # Aggiungi informazioni aggiuntive dalle squadre
                            if home_team in team_mapping:
                                match["home_team_info"] = team_mapping[home_team]
                            if away_team in team_mapping:
                                match["away_team_info"] = team_mapping[away_team]
                            
                            matches.append(match)
            
            return matches
                
        except Exception as e:
            logger.error(f"Errore nel recupero delle partite per {league_id} stagione {season}: {e}")
            return []
    
    def _format_date(self, date_str: str) -> str:
        """
        Formatta la data nel formato ISO.
        
        Args:
            date_str: Data in formato "GG/MM/YYYY"
            
        Returns:
            Data in formato ISO (YYYY-MM-DDTHH:MM:SSZ)
        """
        try:
            # Gestisci vari formati di data
            if '/' in date_str:
                parts = date_str.split('/')
                if len(parts) == 3:
                    day, month, year = parts
                    return f"{year}-{month.zfill(2)}-{day.zfill(2)}T12:00:00Z"  # Imposta un orario di default
            elif '-' in date_str:
                parts = date_str.split('-')
                if len(parts) == 3:
                    year, month, day = parts
                    return f"{year}-{month.zfill(2)}-{day.zfill(2)}T12:00:00Z"  # Imposta un orario di default
                
            logger.warning(f"Formato data non riconosciuto: {date_str}")
            return date_str
        except Exception as e:
            logger.error(f"Errore nella formattazione della data {date_str}: {e}")
            return date_str
    
    @cached(ttl=604800)  # Cache di 1 settimana
    def get_standings(self, league_id: str, season: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Ottiene la classifica per un campionato e stagione.
        
        Args:
            league_id: ID del campionato
            season: Stagione (formato YYYY-YY, es. "2023-24")
            
        Returns:
            Classifica del campionato
        """
        if not season:
            season = self.current_season
            
        repo = self.repositories.get(league_id)
        if not repo:
            logger.error(f"Campionato non supportato: {league_id}")
            return []
        
        # Calcola la classifica dalle partite
        matches = self.get_matches(league_id, season)
        if not matches:
            return []
        
        # Raggruppa per squadra
        standings = {}
        
        for match in matches:
            # Considera solo le partite finite
            if match.get("status") != "FINISHED":
                continue
                
            home_team = match["home_team"]
            away_team = match["away_team"]
            home_score = match["home_score"]
            away_score = match["away_score"]
            
            # Inizializza se necessario
            if home_team not in standings:
                standings[home_team] = {
                    "team": home_team,
                    "played": 0,
                    "won": 0,
                    "drawn": 0,
                    "lost": 0,
                    "goals_for": 0,
                    "goals_against": 0,
                    "points": 0
                }
                
            if away_team not in standings:
                standings[away_team] = {
                    "team": away_team,
                    "played": 0,
                    "won": 0,
                    "drawn": 0,
                    "lost": 0,
                    "goals_for": 0,
                    "goals_against": 0,
                    "points": 0
                }
                
            # Aggiorna statistiche squadra di casa
            standings[home_team]["played"] += 1
            standings[home_team]["goals_for"] += home_score
            standings[home_team]["goals_against"] += away_score
            
            # Aggiorna statistiche squadra in trasferta
            standings[away_team]["played"] += 1
            standings[away_team]["goals_for"] += away_score
            standings[away_team]["goals_against"] += home_score
            
            # Assegna punti in base al risultato
            if home_score > away_score:
                standings[home_team]["won"] += 1
                standings[home_team]["points"] += 3
                standings[away_team]["lost"] += 1
            elif home_score < away_score:
                standings[away_team]["won"] += 1
                standings[away_team]["points"] += 3
                standings[home_team]["lost"] += 1
            else:
                standings[home_team]["drawn"] += 1
                standings[home_team]["points"] += 1
                standings[away_team]["drawn"] += 1
                standings[away_team]["points"] += 1
        
        # Converti in lista e aggiungi statistiche derivate
        result = []
        for team, stats in standings.items():
            stats["goal_difference"] = stats["goals_for"] - stats["goals_against"]
            result.append(stats)
        
        # Ordina per punti, differenza reti e gol fatti
        result.sort(key=lambda x: (x["points"], x["goal_difference"], x["goals_for"]), reverse=True)
        
        # Aggiungi posizione
        for i, team in enumerate(result):
            team["position"] = i + 1
        
        return result
    
    def update_firebase_data(self, league_id: str, season: Optional[str] = None) -> Dict[str, Any]:
        """
        Aggiorna i dati su Firebase per un campionato specifico.
        
        Args:
            league_id: ID del campionato
            season: Stagione (formato YYYY-YY, es. "2023-24")
            
        Returns:
            Risultato dell'operazione
        """
        if not season:
            season = self.current_season
            
        logger.info(f"Aggiornamento dati OpenFootball su Firebase per {league_id} stagione {season}")
        
        result = {
            "teams": {"success": 0, "error": None},
            "matches": {"success": 0, "error": None},
            "standings": {"success": 0, "error": None}
        }
        
        try:
            # Aggiorna le squadre
            teams = self.get_teams(league_id, season)
            if teams:
                teams_ref = self.db.get_reference(f"open_data/open_football/{league_id}/{season}/teams")
                teams_ref.set({i: team for i, team in enumerate(teams)})
                result["teams"]["success"] = len(teams)
            
            # Aggiorna le partite
            matches = self.get_matches(league_id, season)
            if matches:
                matches_ref = self.db.get_reference(f"open_data/open_football/{league_id}/{season}/matches")
                matches_ref.set({i: match for i, match in enumerate(matches)})
                result["matches"]["success"] = len(matches)
            
            # Aggiorna la classifica
            standings = self.get_standings(league_id, season)
            if standings:
                standings_ref = self.db.get_reference(f"open_data/open_football/{league_id}/{season}/standings")
                standings_ref.set({i: standing for i, standing in enumerate(standings)})
                result["standings"]["success"] = len(standings)
            
            # Aggiorna timestamp
            meta_ref = self.db.get_reference(f"open_data/open_football/{league_id}/{season}/meta")
            meta_ref.set({
                "last_update": datetime.now().isoformat(),
                "source": "openfootball",
                "league_id": league_id,
                "season": season
            })
            
            logger.info(f"Dati OpenFootball aggiornati con successo: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Errore nell'aggiornamento dei dati OpenFootball: {e}")
            for key in result:
                if result[key]["error"] is None:
                    result[key]["error"] = str(e)
            return result
            
    def update_all_leagues(self, seasons: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Aggiorna i dati per tutti i campionati disponibili.
        
        Args:
            seasons: Lista delle stagioni da aggiornare (opzionale)
            
        Returns:
            Risultato dell'operazione per campionato
        """
        if not seasons:
            seasons = [self.current_season]
            
        results = {}
        
        for league_id in self.repositories:
            for season in seasons:
                logger.info(f"Aggiornamento dati per {league_id} stagione {season}")
                try:
                    results[f"{league_id}_{season}"] = self.update_firebase_data(league_id, season)
                    # Pausa per evitare troppe richieste
                    time.sleep(2)
                except Exception as e:
                    logger.error(f"Errore nell'aggiornamento di {league_id} stagione {season}: {e}")
                    results[f"{league_id}_{season}"] = {"error": str(e)}
        
        return results

# Funzioni di utilità globali
def get_open_football_client() -> OpenFootballClient:
    """
    Ottiene un'istanza del client OpenFootball.
    
    Returns:
        Istanza di OpenFootballClient
    """
    return OpenFootballClient()

def get_teams(league_id: str, season: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Ottiene le squadre per un campionato specifico.
    
    Args:
        league_id: ID del campionato
        season: Stagione (opzionale)
        
    Returns:
        Lista delle squadre
    """
    client = get_open_football_client()
    return client.get_teams(league_id, season)

def get_matches(league_id: str, season: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Ottiene le partite per un campionato specifico.
    
    Args:
        league_id: ID del campionato
        season: Stagione (opzionale)
        
    Returns:
        Lista delle partite
    """
    client = get_open_football_client()
    return client.get_matches(league_id, season)

def get_standings(league_id: str, season: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Ottiene la classifica per un campionato specifico.
    
    Args:
        league_id: ID del campionato
        season: Stagione (opzionale)
        
    Returns:
        Classifica del campionato
    """
    client = get_open_football_client()
    return client.get_standings(league_id, season)

def update_league_data(league_id: str, season: Optional[str] = None) -> Dict[str, Any]:
    """
    Aggiorna i dati per un campionato specifico.
    
    Args:
        league_id: ID del campionato
        season: Stagione (opzionale)
        
    Returns:
        Risultato dell'operazione
    """
    client = get_open_football_client()
    return client.update_firebase_data(league_id, season)

def update_all_leagues(seasons: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Aggiorna i dati per tutti i campionati disponibili.
    
    Args:
        seasons: Lista delle stagioni da aggiornare (opzionale)
        
    Returns:
        Risultato dell'operazione
    """
    client = get_open_football_client()
    return client.update_all_leagues(seasons)

# Alias per retrocompatibilità - per risolvere errori di importazione
OpenFootballLoader = OpenFootballClient
