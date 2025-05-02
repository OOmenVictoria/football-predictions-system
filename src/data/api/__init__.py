""" 
Package api per l'integrazione con API calcistiche.
Questo package fornisce accesso unificato a varie API che offrono dati calcistici.
"""
from typing import Dict, List, Any, Optional, Union

# Importa le funzioni dai moduli
from src.data.api.football_data import (
    get_competitions as get_football_data_competitions,
    get_matches as get_football_data_matches,
    get_team as get_football_data_team,
    get_team_matches as get_football_data_team_matches,
    get_match as get_football_data_match,
    get_standings as get_football_data_standings
)

from src.data.api.api_football import (
    get_leagues as get_api_football_leagues,
    get_fixtures as get_api_football_fixtures,
    get_team_info as get_api_football_team,
    get_team_fixtures as get_api_football_team_fixtures,
    get_fixture_details as get_api_football_fixture,
    get_standings as get_api_football_standings,
    get_team_statistics as get_api_football_team_stats,
    get_fixture_statistics as get_api_football_fixture_stats,
    get_head_to_head as get_api_football_h2h
)

# Funzioni di utilità

def get_all_sources() -> List[Dict[str, Any]]:
    """
    Ottiene la lista di tutte le API disponibili.
    
    Returns:
        Lista delle API
    """
    sources = [
        {
            "id": "football_data",
            "name": "Football-Data.org",
            "description": "API per dati calcistici dei principali campionati europei",
            "website": "https://www.football-data.org",
            "type": "api",
            "requires_key": True,
            "free_tier_available": True,
            "data_types": ["competitions", "matches", "teams", "standings"]
        },
        {
            "id": "api_football",
            "name": "API-Football",
            "description": "API completa con statistiche dettagliate per numerosi campionati",
            "website": "https://www.api-football.com",
            "type": "api",
            "requires_key": True,
            "free_tier_available": True,
            "data_types": ["leagues", "fixtures", "teams", "statistics", "standings", "head_to_head"]
        }
    ]
    
    return sources

def get_api_status() -> Dict[str, Dict[str, Any]]:
    """
    Verifica lo stato delle API configurate.
    
    Returns:
        Stato di ciascuna API
    """
    status = {}
    
    # Verifica Football-Data.org
    try:
        competitions = get_football_data_competitions()
        status["football_data"] = {
            "status": "active" if competitions else "error",
            "competitions_available": len(competitions) if competitions else 0,
            "error": None
        }
    except Exception as e:
        status["football_data"] = {
            "status": "error",
            "error": str(e)
        }
    
    # Verifica API-Football
    try:
        leagues = get_api_football_leagues()
        status["api_football"] = {
            "status": "active" if leagues else "error",
            "leagues_available": len(leagues) if leagues else 0,
            "error": None
        }
    except Exception as e:
        status["api_football"] = {
            "status": "error",
            "error": str(e)
        }
    
    return status

def select_api_source(source_priority: List[str] = None) -> str:
    """
    Seleziona l'API da utilizzare in base alla priorità e disponibilità.
    
    Args:
        source_priority: Lista di API in ordine di priorità
    
    Returns:
        ID dell'API da utilizzare
    """
    if not source_priority:
        source_priority = ["football_data", "api_football"]
    
    # Verifica la disponibilità di ciascuna API
    status = get_api_status()
    
    for source in source_priority:
        if source in status and status[source]["status"] == "active":
            return source
    
    # Se nessuna API è disponibile, restituisci la prima
    return source_priority[0]

def get_competitions(source: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Ottiene le competizioni da un'API specifica.
    
    Args:
        source: ID dell'API (opzionale, altrimenti viene selezionata automaticamente)
    
    Returns:
        Lista delle competizioni
    """
    if not source:
        source = select_api_source()
    
    if source == "football_data":
        return get_football_data_competitions()
    elif source == "api_football":
        return get_api_football_leagues()
    
    return []

def get_matches(competition_id: str, source: Optional[str] = None, **kwargs) -> List[Dict[str, Any]]:
    """
    Ottiene le partite di una competizione da un'API specifica.
    
    Args:
        competition_id: ID della competizione
        source: ID dell'API (opzionale, altrimenti viene selezionata automaticamente)
        **kwargs: Parametri aggiuntivi (date, status, ecc.)
    
    Returns:
        Lista delle partite
    """
    if not source:
        source = select_api_source()
    
    if source == "football_data":
        return get_football_data_matches(competition_id, **kwargs)
    elif source == "api_football":
        return get_api_football_fixtures(league_id=competition_id, **kwargs)
    
    return []

def get_team(team_id: str, source: Optional[str] = None) -> Dict[str, Any]:
    """
    Ottiene le informazioni su una squadra da un'API specifica.
    
    Args:
        team_id: ID della squadra
        source: ID dell'API (opzionale, altrimenti viene selezionata automaticamente)
    
    Returns:
        Informazioni sulla squadra
    """
    if not source:
        source = select_api_source()
    
    if source == "football_data":
        return get_football_data_team(team_id)
    elif source == "api_football":
        return get_api_football_team(team_id)
    
    return {}

def get_team_matches(team_id: str, source: Optional[str] = None, **kwargs) -> List[Dict[str, Any]]:
    """
    Ottiene le partite di una squadra da un'API specifica.
    
    Args:
        team_id: ID della squadra
        source: ID dell'API (opzionale, altrimenti viene selezionata automaticamente)
        **kwargs: Parametri aggiuntivi (date, status, ecc.)
    
    Returns:
        Lista delle partite della squadra
    """
    if not source:
        source = select_api_source()
    
    if source == "football_data":
        return get_football_data_team_matches(team_id, **kwargs)
    elif source == "api_football":
        return get_api_football_team_fixtures(team_id, **kwargs)
    
    return []

def get_match(match_id: str, source: Optional[str] = None) -> Dict[str, Any]:
    """
    Ottiene le informazioni su una partita da un'API specifica.
    
    Args:
        match_id: ID della partita
        source: ID dell'API (opzionale, altrimenti viene selezionata automaticamente)
    
    Returns:
        Informazioni sulla partita
    """
    if not source:
        source = select_api_source()
    
    if source == "football_data":
        return get_football_data_match(match_id)
    elif source == "api_football":
        return get_api_football_fixture(match_id)
    
    return {}

def get_match_statistics(match_id: str, source: Optional[str] = None) -> Dict[str, Any]:
    """
    Ottiene le statistiche di una partita da un'API specifica.
    
    Args:
        match_id: ID della partita
        source: ID dell'API (opzionale, altrimenti viene selezionata automaticamente)
    
    Returns:
        Statistiche della partita
    """
    if not source:
        source = select_api_source(["api_football", "football_data"])
    
    if source == "api_football":
        return get_api_football_fixture_stats(match_id)
    
    # Football-Data.org non fornisce statistiche dettagliate
    return {}

def get_standings(competition_id: str, source: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Ottiene la classifica di una competizione da un'API specifica.
    
    Args:
        competition_id: ID della competizione
        source: ID dell'API (opzionale, altrimenti viene selezionata automaticamente)
    
    Returns:
        Classifica della competizione
    """
    if not source:
        source = select_api_source()
    
    if source == "football_data":
        return get_football_data_standings(competition_id)
    elif source == "api_football":
        return get_api_football_standings(competition_id)
    
    return []

def get_team_statistics(team_id: str, league_id: str, source: Optional[str] = None) -> Dict[str, Any]:
    """
    Ottiene le statistiche di una squadra in una competizione da un'API specifica.
    
    Args:
        team_id: ID della squadra
        league_id: ID della competizione
        source: ID dell'API (opzionale, altrimenti viene selezionata automaticamente)
    
    Returns:
        Statistiche della squadra
    """
    if not source:
        source = select_api_source(["api_football", "football_data"])
    
    if source == "api_football":
        return get_api_football_team_stats(team_id, league_id)
    
    # Football-Data.org non fornisce statistiche dettagliate delle squadre
    return {}

def get_head_to_head(team1_id: str, team2_id: str, source: Optional[str] = None, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Ottiene le partite tra due squadre da un'API specifica.
    
    Args:
        team1_id: ID della prima squadra
        team2_id: ID della seconda squadra
        source: ID dell'API (opzionale, altrimenti viene selezionata automaticamente)
        limit: Numero massimo di partite da restituire
    
    Returns:
        Lista delle partite tra le due squadre
    """
    if not source:
        source = select_api_source(["api_football", "football_data"])
    
    if source == "api_football":
        return get_api_football_h2h(team1_id, team2_id, limit)
    
    # Football-Data.org non ha un endpoint specifico per H2H, quindi dobbiamo simularlo
    # Otteniamo le partite di entrambe le squadre e troviamo le intersezioni
    team1_matches = get_football_data_team_matches(team1_id)
    
    if not team1_matches:
        return []
    
    h2h_matches = []
    for match in team1_matches:
        home_team_id = match.get("homeTeam", {}).get("id", "")
        away_team_id = match.get("awayTeam", {}).get("id", "")
        
        if (home_team_id == team1_id and away_team_id == team2_id) or \
           (home_team_id == team2_id and away_team_id == team1_id):
            h2h_matches.append(match)
    
    # Limita il numero di partite
    h2h_matches = h2h_matches[:limit]
    
    return h2h_matches

def search_matches(query: Dict[str, Any], source: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Cerca partite in base a criteri specifici.
    
    Args:
        query: Criteri di ricerca (date, squadre, competizioni, ecc.)
        source: ID dell'API (opzionale, altrimenti viene selezionata automaticamente)
    
    Returns:
        Lista delle partite che corrispondono ai criteri
    """
    if not source:
        source = select_api_source()
    
    # Estrai parametri comuni
    date_from = query.get("date_from")
    date_to = query.get("date_to")
    team_id = query.get("team_id")
    competition_id = query.get("competition_id")
    status = query.get("status")
    
    # Se è specificata una squadra, cerca le partite della squadra
    if team_id:
        return get_team_matches(team_id, source, dateFrom=date_from, dateTo=date_to, status=status)
    
    # Se è specificata una competizione, cerca le partite della competizione
    if competition_id:
        return get_matches(competition_id, source, dateFrom=date_from, dateTo=date_to, status=status)
    
    # Altrimenti, cerca in tutte le competizioni disponibili
    all_matches = []
    competitions = get_competitions(source)
    
    for competition in competitions[:5]:  # Limita a 5 competizioni per evitare troppe richieste
        competition_id = competition.get("id") or competition.get("competition_id")
        if competition_id:
            matches = get_matches(competition_id, source, dateFrom=date_from, dateTo=date_to, status=status)
            all_matches.extend(matches)
    
    return all_matches
