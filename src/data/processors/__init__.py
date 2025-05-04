""" 
Package processors per l'elaborazione e la normalizzazione dei dati calcistici.
Questo package fornisce funzionalità per elaborare, normalizzare e arricchire
i dati raccolti da varie fonti, producendo un formato standardizzato per l'analisi.
"""
from typing import Dict, List, Any, Optional, Union
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

# Importa le funzioni principali dai moduli
from src.data.processors.matches import (
    MatchProcessor,
    get_processor as get_match_processor,
)
from src.data.processors.teams import (
    TeamProcessor,
    get_processor as get_team_processor,
)
from src.data.processors.head_to_head import (
    HeadToHeadProcessor,
    get_processor as get_h2h_processor,
)
from src.data.processors.xg_processor import (
    XGProcessor,
    process_match_xg,
    get_team_xg_history,
    predict_match_xg
)
from src.data.processors.standings import (
    StandingsProcessor,
    process_league_standings,
    get_team_standing,
    get_team_form,
)

# =============================================================================
# FUNZIONI DI UTILITÀ UNIFICATE
# =============================================================================

class StandardizedResponse:
    """Risposta standardizzata per tutte le operazioni di processing."""
    def __init__(self, success: int = 0, error: Optional[str] = None, data: Any = None):
        self.success = success
        self.error = error
        self.data = data
        
    def to_dict(self) -> Dict[str, Any]:
        """Converte la risposta in dizionario."""
        result = {
            "success": self.success,
            "error": self.error
        }
        if self.data is not None:
            result["data"] = self.data
        return result

# -----------------------------------------------------------------------------
# Funzioni per gestire le partite
# -----------------------------------------------------------------------------

def get_matches_for_date(date: Union[str, datetime], league_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Ottiene le partite per una data specifica.
    
    Args:
        date: Data come stringa (YYYY-MM-DD) o oggetto datetime
        league_id: ID del campionato (opzionale)
    
    Returns:
        Lista delle partite per la data specificata
    """
    processor = get_match_processor()
    
    # Converte la data in stringa se necessario
    if isinstance(date, datetime):
        date_str = date.strftime("%Y-%m-%d")
    else:
        date_str = date
    
    # Ottiene le partite per la data
    try:
        matches = processor.get_matches_for_date(date_str)
        
        # Filtra per campionato se specificato
        if league_id and matches:
            matches = [match for match in matches if match.get("league_id") == league_id]
        
        return matches
    except Exception as e:
        logger.error(f"Errore nel recupero partite per data {date_str}: {e}")
        return []

def get_matches_for_team(team_id: str, limit: int = 20) -> List[Dict[str, Any]]:
    """
    Ottiene le partite di una squadra specifica.
    
    Args:
        team_id: ID della squadra
        limit: Numero massimo di partite da restituire
    
    Returns:
        Lista delle partite della squadra
    """
    processor = get_match_processor()
    try:
        return processor.get_matches_for_team(team_id)[:limit]
    except Exception as e:
        logger.error(f"Errore nel recupero partite per team {team_id}: {e}")
        return []

def get_upcoming_matches(days: int = 7, league_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Ottiene le partite imminenti in un determinato intervallo di giorni.
    
    Args:
        days: Numero di giorni futuri da considerare
        league_id: ID del campionato (opzionale)
    
    Returns:
        Lista delle partite imminenti
    """
    # Calcola l'intervallo di date
    today = datetime.now()
    end_date = today + timedelta(days=days)
    
    # Ottieni le partite per ogni giorno nell'intervallo
    matches = []
    current_date = today
    
    while current_date <= end_date:
        day_matches = get_matches_for_date(current_date, league_id)
        
        # Filtra solo partite programmate o sconosciute
        if day_matches:
            day_matches = [match for match in day_matches 
                          if match.get("status") in ["scheduled", "unknown"]]
        
        matches.extend(day_matches)
        current_date += timedelta(days=1)
    
    # Ordina per data e ora
    matches.sort(key=lambda x: x.get("datetime", ""))
    
    return matches

def get_recent_matches(days: int = 7, league_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Ottiene le partite recenti in un determinato intervallo di giorni passati.
    
    Args:
        days: Numero di giorni passati da considerare
        league_id: ID del campionato (opzionale)
    
    Returns:
        Lista delle partite recenti
    """
    # Calcola l'intervallo di date
    today = datetime.now()
    start_date = today - timedelta(days=days)
    
    # Ottieni le partite per ogni giorno nell'intervallo
    matches = []
    current_date = start_date
    
    while current_date <= today:
        day_matches = get_matches_for_date(current_date, league_id)
        
        # Filtra solo per partite completate
        if day_matches:
            day_matches = [match for match in day_matches 
                          if match.get("status") == "finished"]
        
        matches.extend(day_matches)
        current_date += timedelta(days=1)
    
    # Ordina per data e ora (più recenti prima)
    matches.sort(key=lambda x: x.get("datetime", ""), reverse=True)
    
    return matches

def get_match_by_id(match_id: str) -> Dict[str, Any]:
    """
    Ottiene le informazioni complete su una partita.
    
    Args:
        match_id: ID della partita
    
    Returns:
        Informazioni complete sulla partita
    """
    processor = get_match_processor()
    
    match = processor.get_stored_match_data(match_id)
    
    if match:
        # Arricchisci con informazioni aggiuntive
        return processor.enrich_match_data(match)
    
    return {}

# -----------------------------------------------------------------------------
# Funzioni per gestire le squadre
# -----------------------------------------------------------------------------

def get_team_info(team_identifier: str, by_id: bool = False) -> Dict[str, Any]:
    """
    Ottiene le informazioni complete su una squadra.
    
    Args:
        team_identifier: Nome o ID della squadra
        by_id: Se True, l'identificatore è un ID, altrimenti un nome
    
    Returns:
        Informazioni complete sulla squadra
    """
    processor = get_team_processor()
    
    if by_id:
        team = processor.get_stored_team_data(team_identifier)
    else:
        team = processor.find_team_by_name(team_identifier)
    
    if team:
        # Arricchisci con informazioni aggiuntive
        return processor.enrich_team_data(team)
    
    return {}

# -----------------------------------------------------------------------------
# Funzioni per gestire gli scontri diretti
# -----------------------------------------------------------------------------

def get_head_to_head(team1_id: str, team2_id: str, max_matches: int = 10) -> Dict[str, Any]:
    """
    Ottiene le statistiche testa a testa tra due squadre.
    
    Args:
        team1_id: ID della prima squadra
        team2_id: ID della seconda squadra
        max_matches: Numero massimo di partite da considerare
    
    Returns:
        Statistiche testa a testa
    """
    processor = get_h2h_processor()
    return processor.get_head_to_head(team1_id, team2_id, max_matches=max_matches)

# -----------------------------------------------------------------------------
# Funzioni per gestire le classifiche
# -----------------------------------------------------------------------------

def get_current_standings(league_id: str) -> List[Dict[str, Any]]:
    """
    Ottiene la classifica corrente di un campionato.
    
    Args:
        league_id: ID del campionato
    
    Returns:
        Classifica del campionato
    """
    standings_data = process_league_standings(league_id)
    return standings_data.get('standings', []) if standings_data else []

def get_standings_history(team_id: str, league_id: str) -> Dict[str, Any]:
    """
    Ottiene la cronologia della posizione in classifica di una squadra.
    
    Args:
        team_id: ID della squadra
        league_id: ID del campionato
    
    Returns:
        Cronologia posizioni in classifica
    """
    return get_team_standing(team_id, league_id)

def calculate_form_table(league_id: str, last_n_matches: int = 5) -> List[Dict[str, Any]]:
    """
    Calcola la classifica basata sulla forma recente.
    
    Args:
        league_id: ID del campionato
        last_n_matches: Numero di partite recenti da considerare
    
    Returns:
        Classifica basata sulla forma
    """
    # Implementazione semplificata
    all_teams = get_current_standings(league_id)
    form_standings = []
    
    for team in all_teams:
        team_id = team.get("team_id", "")
        if team_id:
            form_data = get_team_form(team_id, league_id)
            if form_data:
                form_standings.append({
                    "team_id": team_id,
                    "team_name": team.get("team_name", ""),
                    "form": form_data.get("form", ""),
                    "recent_points": sum(1 if r == 'W' else 0.5 if r == 'D' else 0 
                                       for r in form_data.get("form", "")[:last_n_matches])
                })
    
    # Ordina per punti recenti
    form_standings.sort(key=lambda x: x.get("recent_points", 0), reverse=True)
    
    return form_standings

# -----------------------------------------------------------------------------
# Funzioni per gestire gli Expected Goals (xG)
# -----------------------------------------------------------------------------

def get_xg_data(match_id: str) -> Dict[str, Any]:
    """
    Ottiene i dati Expected Goals per una partita.
    
    Args:
        match_id: ID della partita
    
    Returns:
        Dati xG della partita
    """
    return process_match_xg(match_id)

def analyze_xg_performance(team_id: str, matches_limit: int = 10) -> Dict[str, Any]:
    """
    Analizza le performance xG di una squadra.
    
    Args:
        team_id: ID della squadra
        matches_limit: Numero di partite da analizzare
    
    Returns:
        Analisi performance xG
    """
    return get_team_xg_history(team_id, matches_limit)

# -----------------------------------------------------------------------------
# Funzione di analisi completa di una partita
# -----------------------------------------------------------------------------

def analyze_match(match_id: str) -> Dict[str, Any]:
    """
    Esegue un'analisi completa di una partita.
    
    Args:
        match_id: ID della partita
    
    Returns:
        Analisi completa della partita
    """
    # Ottieni i dati di base della partita
    match = get_match_by_id(match_id)
    
    if not match:
        return {"error": "Partita non trovata"}
    
    # Ottieni squadre
    home_team_id = match.get("home_team_id", "")
    away_team_id = match.get("away_team_id", "")
    
    home_team = get_team_info(home_team_id, by_id=True)
    away_team = get_team_info(away_team_id, by_id=True)
    
    # Analisi testa a testa
    head_to_head = get_head_to_head(home_team_id, away_team_id)
    
    # Analisi xG
    xg_data = get_xg_data(match_id)
    
    # Classifica attuale
    league_id = match.get("league_id", "")
    standings = get_current_standings(league_id) if league_id else []
    
    # Componi analisi completa
    analysis = {
        "match": match,
        "home_team": home_team,
        "away_team": away_team,
        "head_to_head": head_to_head,
        "xg_data": xg_data,
        "standings": standings,
        "analysis_time": datetime.now().isoformat()
    }
    
    return analysis

# -----------------------------------------------------------------------------
# Funzioni di batch processing
# -----------------------------------------------------------------------------

def batch_process_matches(match_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Elabora un batch di partite.
    
    Args:
        match_ids: Lista di ID delle partite
    
    Returns:
        Dizionario con i risultati dell'elaborazione per ogni partita
    """
    results = {}
    
    for match_id in match_ids:
        try:
            match_data = get_match_by_id(match_id)
            if match_data:
                # Arricchisci con analisi xG se disponibile
                xg_data = get_xg_data(match_id)
                if xg_data:
                    match_data["xg_analysis"] = xg_data
                
                results[match_id] = StandardizedResponse(
                    success=1,
                    data=match_data
                ).to_dict()
            else:
                results[match_id] = StandardizedResponse(
                    error=f"Partita {match_id} non trovata"
                ).to_dict()
        except Exception as e:
            results[match_id] = StandardizedResponse(
                error=str(e)
            ).to_dict()
    
    return results

def batch_process_teams(team_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Elabora un batch di squadre.
    
    Args:
        team_ids: Lista di ID delle squadre
    
    Returns:
        Dizionario con i risultati dell'elaborazione per ogni squadra
    """
    results = {}
    
    for team_id in team_ids:
        try:
            team_data = get_team_info(team_id, by_id=True)
            if team_data:
                # Arricchisci con performance xG
                xg_performance = analyze_xg_performance(team_id)
                if xg_performance:
                    team_data["xg_performance"] = xg_performance
                
                results[team_id] = StandardizedResponse(
                    success=1,
                    data=team_data
                ).to_dict()
            else:
                results[team_id] = StandardizedResponse(
                    error=f"Squadra {team_id} non trovata"
                ).to_dict()
        except Exception as e:
            results[team_id] = StandardizedResponse(
                error=str(e)
            ).to_dict()
    
    return results

# -----------------------------------------------------------------------------
# Informazioni sul package
# -----------------------------------------------------------------------------

__version__ = "1.0.0"
__author__ = "Soccer Data Processors Package"
__license__ = "MIT"

# Esporta tutte le funzioni principali
__all__ = [
    # Match functions
    "get_matches_for_date",
    "get_matches_for_team",
    "get_upcoming_matches",
    "get_recent_matches",
    "get_match_by_id",
    
    # Team functions
    "get_team_info",
    
    # Head-to-head functions
    "get_head_to_head",
    
    # Standing functions
    "get_current_standings",
    "get_standings_history",
    "calculate_form_table",
    
    # xG functions
    "get_xg_data",
    "analyze_xg_performance",
    
    # Analysis functions
    "analyze_match",
    
    # Batch processing
    "batch_process_matches",
    "batch_process_teams",
    
    # Utility classes
    "StandardizedResponse"
]
