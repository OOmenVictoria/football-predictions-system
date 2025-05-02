"""
Processore per le classifiche dei campionati.
Questo modulo fornisce funzionalità per elaborare, normalizzare e arricchire
le classifiche dei campionati provenienti da varie fonti.
"""
import os
import sys
import time
import logging
from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime, timedelta

from src.utils.cache import cached
from src.utils.database import FirebaseManager
from src.config.settings import get_setting
from src.config.leagues import get_league, get_league_url
from src.data.collectors import collect_league_data

logger = logging.getLogger(__name__)

class StandingsProcessor:
    """
    Processore per normalizzare e arricchire le classifiche dei campionati.
    
    Gestisce la standardizzazione delle classifiche da diverse fonti, risolve
    i conflitti, e arricchisce i dati con statistiche aggiuntive.
    """
    
    def __init__(self):
        """Inizializza il processore delle classifiche."""
        self.db = FirebaseManager()
        self.preferred_source = get_setting('processors.standings.preferred_source', 'football_data')
        self.fallback_sources = get_setting('processors.standings.fallback_sources', 
                                          ['api_football', 'flashscore', 'fbref'])
        self.min_standings_size = get_setting('processors.standings.min_size', 10)
        self.auto_update_days = get_setting('processors.standings.auto_update_days', 1)
        
        logger.info(f"StandingsProcessor inizializzato con fonte primaria: {self.preferred_source}")
    
    @cached(ttl=3600 * 6)  # 6 ore
    def process_league_standings(self, league_id: str, 
                               season: Optional[str] = None,
                               force_update: bool = False) -> Dict[str, Any]:
        """
        Processa le classifiche di un campionato, normalizzandole e arricchendole.
        
        Args:
            league_id: ID del campionato
            season: Stagione (opzionale, usa stagione corrente se non specificata)
            force_update: Forza aggiornamento anche se dati recenti
            
        Returns:
            Classifica elaborata e normalizzata
        """
        logger.info(f"Elaborazione classifica per league_id={league_id}, season={season}")
        
        # Ottieni dettagli campionato
        league_info = get_league(league_id)
        if not league_info:
            logger.warning(f"League {league_id} non trovata in configurazione")
            return {}
        
        # Determina stagione corrente se non specificata
        if not season:
            season = league_info.get('current_season', '')
            logger.info(f"Usando stagione corrente: {season}")
        
        # Verifica se abbiamo classifiche recenti nel database
        standings_ref = self.db.get_reference(f"data/standings/{league_id}/{season}")
        existing_standings = standings_ref.get()
        
        if existing_standings and not force_update:
            last_updated = existing_standings.get('last_updated', '')
            if last_updated:
                try:
                    update_time = datetime.fromisoformat(last_updated)
                    age_days = (datetime.now() - update_time).total_seconds() / 86400
                    
                    if age_days < self.auto_update_days:
                        logger.info(f"Usando classifiche esistenti (aggiornate {age_days:.1f} giorni fa)")
                        return existing_standings
                except:
                    pass
        
        # Se arriviamo qui, serve un aggiornamento o non ci sono dati esistenti
        logger.info(f"Raccolta classifiche aggiornate per {league_id}")
        
        # Raccogli dati da fonti disponibili
        standings_data = self._collect_standings_from_sources(league_id, season)
        
        if not standings_data:
            logger.warning(f"Nessuna classifica disponibile per {league_id}")
            return {}
        
        # Normalizza e arricchisci
        processed_data = self._process_standings_data(standings_data, league_info)
        
        # Aggiorna database
        processed_data['last_updated'] = datetime.now().isoformat()
        processed_data['league_id'] = league_id
        processed_data['season'] = season
        
        standings_ref.set(processed_data)
        logger.info(f"Classifica elaborata e salvata per {league_id}")
        
        return processed_data
    
    def _collect_standings_from_sources(self, league_id: str, 
                                      season: str) -> Dict[str, Any]:
        """
        Raccoglie classifiche da varie fonti dati.
        
        Args:
            league_id: ID del campionato
            season: Stagione
            
        Returns:
            Dati di classifiche da varie fonti
        """
        standings_data = {}
        sources = [self.preferred_source] + self.fallback_sources
        
        # Prova ogni fonte in ordine
        for source in sources:
            # Ottieni dati per questa fonte
            source_data = self._get_standings_from_source(source, league_id, season)
            
            if source_data and 'standings' in source_data and len(source_data['standings']) >= self.min_standings_size:
                standings_data[source] = source_data
                logger.info(f"Ottenute classifiche da {source} ({len(source_data['standings'])} squadre)")
                
                # Se è la fonte preferita, possiamo fermarci qui
                if source == self.preferred_source:
                    break
        
        return standings_data
    
    def _get_standings_from_source(self, source: str, league_id: str, 
                                 season: str) -> Dict[str, Any]:
        """
        Ottiene classifiche da una fonte specifica.
        
        Args:
            source: Nome della fonte
            league_id: ID del campionato
            season: Stagione
            
        Returns:
            Dati della classifica dalla fonte specificata
        """
        try:
            # Ottieni URL o ID specifico per la fonte
            source_id = get_league_url(league_id, source)
            if not source_id:
                logger.warning(f"Nessun ID {source} configurato per {league_id}")
                return {}
            
            # Raccogli dati dal collettore appropriato
            data = collect_league_data(source_id, source, season)
            
            # Estrai la classifica dai dati
            if not data or 'standings' not in data:
                logger.warning(f"Nessuna classifica da {source} per {league_id}")
                return {}
            
            return data
        
        except Exception as e:
            logger.error(f"Errore nel recupero classifica da {source}: {str(e)}")
            return {}
    
    def _process_standings_data(self, standings_data: Dict[str, Any], 
                              league_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Elabora e normalizza i dati delle classifiche.
        
        Args:
            standings_data: Dati classifiche da varie fonti
            league_info: Informazioni sul campionato
            
        Returns:
            Classifica normalizzata e arricchita
        """
        # Se non ci sono dati, restituisci dizionario vuoto
        if not standings_data:
            return {}
        
        # Scegli fonte primaria (quella con più squadre)
        primary_source = self._select_primary_source(standings_data)
        primary_data = standings_data.get(primary_source, {})
        
        # Inizializza struttura risultante
        result = {
            "name": primary_data.get("name", league_info.get("name", "")),
            "country": primary_data.get("country", league_info.get("country", "")),
            "source": primary_source,
            "standings": [],
            "groups": [],
            "has_relegation": False,
            "has_qualification": False,
            "stats": {
                "avg_goals_per_match": 0,
                "home_win_percentage": 0,
                "away_win_percentage": 0,
                "draw_percentage": 0
            }
        }
        
        # Verifica se è una classifica a gironi
        is_grouped = self._is_grouped_standings(primary_data)
        
        if is_grouped:
            # Elabora classifiche a gironi
            result = self._process_grouped_standings(primary_data, result)
        else:
            # Elabora classifica singola
            result = self._process_single_standings(primary_data, result)
        
        # Arricchisci con statistiche aggiunte
        result = self._enrich_standings(result, standings_data)
        
        return result
    
    def _select_primary_source(self, standings_data: Dict[str, Any]) -> str:
        """
        Seleziona la fonte primaria da usare.
        
        Args:
            standings_data: Dati classifiche da varie fonti
            
        Returns:
            Nome della fonte primaria
        """
        # Se la fonte preferita è disponibile, usa quella
        if self.preferred_source in standings_data:
            return self.preferred_source
        
        # Altrimenti, scegli la fonte con più squadre
        max_teams = 0
        selected_source = ""
        
        for source, data in standings_data.items():
            num_teams = len(data.get("standings", []))
            if num_teams > max_teams:
                max_teams = num_teams
                selected_source = source
        
        return selected_source
    
    def _is_grouped_standings(self, data: Dict[str, Any]) -> bool:
        """
        Verifica se la classifica è organizzata in gruppi.
        
        Args:
            data: Dati della classifica
            
        Returns:
            True se la classifica è a gironi, False altrimenti
        """
        return "groups" in data and len(data.get("groups", [])) > 0
    
    def _process_single_standings(self, data: Dict[str, Any], 
                                result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Elabora una classifica singola (non a gironi).
        
        Args:
            data: Dati della classifica
            result: Struttura risultato da popolare
            
        Returns:
            Risultato arricchito
        """
        # Estrai classifiche
        standings = data.get("standings", [])
        
        # Normalizza ogni riga della classifica
        processed_standings = []
        for team in standings:
            processed_team = self._normalize_team_standings(team)
            
            # Aggiungi informazioni sulle zone di classifica
            self._add_position_context(processed_team, len(standings), data)
            
            processed_standings.append(processed_team)
        
        # Ordina per posizione
        processed_standings.sort(key=lambda x: x.get("position", 99))
        
        # Aggiungi al risultato
        result["standings"] = processed_standings
        
        # Determina se ci sono zone retrocessione/qualificazione
        has_relegation = any(team.get("is_relegation", False) for team in processed_standings)
        has_qualification = any(team.get("is_qualification", False) for team in processed_standings)
        
        result["has_relegation"] = has_relegation
        result["has_qualification"] = has_qualification
        
        return result
    
    def _process_grouped_standings(self, data: Dict[str, Any], 
                                 result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Elabora una classifica a gironi.
        
        Args:
            data: Dati della classifica
            result: Struttura risultato da popolare
            
        Returns:
            Risultato arricchito
        """
        groups = data.get("groups", [])
        processed_groups = []
        all_standings = []
        
        for group in groups:
            group_name = group.get("name", "")
            group_standings = group.get("standings", [])
            
            # Normalizza ogni riga della classifica
            processed_standings = []
            for team in group_standings:
                processed_team = self._normalize_team_standings(team)
                
                # Aggiungi informazioni sulle zone di classifica
                self._add_position_context(processed_team, len(group_standings), group)
                
                # Aggiungi riferimento al gruppo
                processed_team["group"] = group_name
                
                processed_standings.append(processed_team)
            
            # Ordina per posizione
            processed_standings.sort(key=lambda x: x.get("position", 99))
            
            # Aggiungi ai gruppi elaborati
            processed_group = {
                "name": group_name,
                "standings": processed_standings
            }
            processed_groups.append(processed_group)
            
            # Aggiungi alla classifica globale
            all_standings.extend(processed_standings)
        
        # Aggiungi al risultato
        result["groups"] = processed_groups
        result["standings"] = all_standings
        
        # Determina se ci sono zone retrocessione/qualificazione
        has_relegation = any(team.get("is_relegation", False) for team in all_standings)
        has_qualification = any(team.get("is_qualification", False) for team in all_standings)
        
        result["has_relegation"] = has_relegation
        result["has_qualification"] = has_qualification
        
        return result
    
    def _normalize_team_standings(self, team: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalizza i dati di una squadra nella classifica.
        
        Args:
            team: Dati della squadra da normalizzare
            
        Returns:
            Dati della squadra normalizzati
        """
        # Struttura standard
        normalized = {
            "position": team.get("position", team.get("rank", 0)),
            "team_id": team.get("team_id", ""),
            "team_name": team.get("team_name", team.get("name", "")),
            "matches_played": team.get("matches_played", team.get("played", 0)),
            "wins": team.get("wins", team.get("won", 0)),
            "draws": team.get("draws", team.get("drawn", 0)),
            "losses": team.get("losses", team.get("lost", 0)),
            "goals_for": team.get("goals_for", team.get("goals_scored", 0)),
            "goals_against": team.get("goals_against", team.get("goals_conceded", 0)),
            "goal_difference": team.get("goal_difference", team.get("goals_for", 0) - team.get("goals_against", 0)),
            "points": team.get("points", 0)
        }
        
        # Calcola media gol
        matches = normalized["matches_played"]
        if matches > 0:
            normalized["avg_goals_for"] = normalized["goals_for"] / matches
            normalized["avg_goals_against"] = normalized["goals_against"] / matches
        else:
            normalized["avg_goals_for"] = 0
            normalized["avg_goals_against"] = 0
        
        # Aggiungi informazioni sulla forma se disponibili
        if "form" in team:
            normalized["form"] = team["form"]
        
        # Aggiungi statistiche casa/trasferta se disponibili
        if "home" in team:
            normalized["home"] = team["home"]
        
        if "away" in team:
            normalized["away"] = team["away"]
        
        return normalized
    
    def _add_position_context(self, team: Dict[str, Any], total_teams: int, 
                            data: Dict[str, Any]) -> None:
        """
        Aggiunge informazioni di contesto sulla posizione in classifica.
        
        Args:
            team: Dati della squadra da arricchire
            total_teams: Numero totale di squadre
            data: Dati completi della classifica
            
        Returns:
            None (aggiorna team in-place)
        """
        position = team.get("position", 0)
        
        # Informazioni sulle posizioni
        team["is_top"] = position <= 4  # Top 4
        team["is_bottom"] = position > total_teams - 3  # Bottom 3
        
        # Zone di qualificazione/retrocessione
        qualification_positions = data.get("qualification_positions", [1, 2, 3, 4])
        relegation_positions = data.get("relegation_positions", [total_teams - 2, total_teams - 1, total_teams])
        
        team["is_qualification"] = position in qualification_positions
        team["is_relegation"] = position in relegation_positions
        
        # Stato della posizione
        if team["is_qualification"]:
            if position == 1:
                team["position_status"] = "champion"
            else:
                team["position_status"] = "qualification"
        elif team["is_relegation"]:
            team["position_status"] = "relegation"
        else:
            team["position_status"] = "mid_table"
    
    def _enrich_standings(self, result: Dict[str, Any], 
                        standings_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Arricchisce le classifiche con statistiche aggiuntive.
        
        Args:
            result: Classifiche elaborate
            standings_data: Dati classifiche da varie fonti
            
        Returns:
            Classifiche arricchite
        """
        # Calcola statistiche generali del campionato
        standings = result.get("standings", [])
        
        if not standings:
            return result
        
        # Calcola statistiche
        total_games = sum(team.get("matches_played", 0) for team in standings) / 2
        total_goals = sum(team.get("goals_for", 0) for team in standings)
        
        home_wins = sum(team.get("home", {}).get("wins", 0) for team in standings if "home" in team)
        away_wins = sum(team.get("away", {}).get("wins", 0) for team in standings if "away" in team)
        draws = sum(team.get("draws", 0) for team in standings)
        
        if total_games > 0:
            result["stats"]["avg_goals_per_match"] = total_goals / total_games
            
            total_results = home_wins + away_wins + draws
            if total_results > 0:
                result["stats"]["home_win_percentage"] = (home_wins / total_results) * 100
                result["stats"]["away_win_percentage"] = (away_wins / total_results) * 100
                result["stats"]["draw_percentage"] = (draws / total_results) * 100
        
        # Aggiungi statistiche da altre fonti se disponibili
        for source, data in standings_data.items():
            if source != result["source"] and "stats" in data:
                # Integra statistiche aggiuntive
                source_stats = data.get("stats", {})
                for key, value in source_stats.items():
                    if key not in result["stats"]:
                        result["stats"][key] = value
        
        return result
    
    def get_recent_form(self, team_id: str, league_id: str, 
                       season: Optional[str] = None) -> Dict[str, Any]:
        """
        Ottiene la forma recente di una squadra dalla classifica.
        
        Args:
            team_id: ID della squadra
            league_id: ID del campionato
            season: Stagione (opzionale)
            
        Returns:
            Dati sulla forma recente
        """
        standings = self.process_league_standings(league_id, season)
        
        if not standings or "standings" not in standings:
            return {}
        
        # Cerca la squadra nella classifica
        for team in standings["standings"]:
            if team.get("team_id") == team_id:
                # Estrai dati forma
                form_data = {
                    "position": team.get("position", 0),
                    "points": team.get("points", 0),
                    "matches_played": team.get("matches_played", 0),
                    "wins": team.get("wins", 0),
                    "draws": team.get("draws", 0),
                    "losses": team.get("losses", 0),
                    "goals_for": team.get("goals_for", 0),
                    "goals_against": team.get("goals_against", 0)
                }
                
                # Aggiungi sequenza forma se disponibile
                if "form" in team:
                    form_data["form"] = team["form"]
                
                # Aggiungi contesto posizione
                form_data["is_top"] = team.get("is_top", False)
                form_data["is_bottom"] = team.get("is_bottom", False)
                form_data["position_status"] = team.get("position_status", "mid_table")
                
                return form_data
        
        return {}

# Funzioni di utilità globali
def process_league_standings(league_id: str, season: Optional[str] = None, 
                           force_update: bool = False) -> Dict[str, Any]:
    """
    Processa le classifiche di un campionato.
    
    Args:
        league_id: ID del campionato
        season: Stagione (opzionale)
        force_update: Forza aggiornamento dai dati origine
        
    Returns:
        Classifica elaborata
    """
    processor = StandingsProcessor()
    return processor.process_league_standings(league_id, season, force_update)

def get_team_standing(team_id: str, league_id: str, 
                    season: Optional[str] = None) -> Dict[str, Any]:
    """
    Ottiene la posizione in classifica di una squadra.
    
    Args:
        team_id: ID della squadra
        league_id: ID del campionato
        season: Stagione (opzionale)
        
    Returns:
        Dati sulla posizione in classifica
    """
    processor = StandingsProcessor()
    standings = processor.process_league_standings(league_id, season)
    
    if not standings or "standings" not in standings:
        return {}
    
    # Cerca la squadra nella classifica
    for team in standings["standings"]:
        if team.get("team_id") == team_id:
            return team
    
    return {}

def get_team_form(team_id: str, league_id: str, 
                season: Optional[str] = None) -> Dict[str, Any]:
    """
    Ottiene la forma recente di una squadra.
    
    Args:
        team_id: ID della squadra
        league_id: ID del campionato
        season: Stagione (opzionale)
        
    Returns:
        Dati sulla forma recente
    """
    processor = StandingsProcessor()
    return processor.get_recent_form(team_id, league_id, season)
