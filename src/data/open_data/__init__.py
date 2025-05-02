""" 
Package open_data per l'integrazione di fonti di dati calcistici open source.
Questo package fornisce accesso unificato a varie fonti di dati calcistici gratuite.
"""
from typing import Dict, List, Any, Optional, Union

# Importa le funzioni dai moduli
from src.data.open_data.open_football import (
    get_teams as get_open_football_teams,
    get_matches as get_open_football_matches,
    get_standings as get_open_football_standings,
    update_league_data as update_open_football_league,
    update_all_leagues as update_all_open_football
)

from src.data.open_data.rsssf import (
    get_seasons_for_country as get_rsssf_seasons,
    get_league_table as get_rsssf_league_table,
    update_country_data as update_rsssf_country,
    update_all_countries as update_all_rsssf
)

from src.data.open_data.kaggle_loader import (
    get_available_datasets as get_kaggle_datasets,
    get_international_matches,
    get_world_cup_matches,
    get_premier_league_matches,
    update_dataset as update_kaggle_dataset,
    update_all_datasets as update_all_kaggle
)

from src.data.open_data.statsbomb import (
    get_competitions as get_statsbomb_competitions,
    get_matches as get_statsbomb_matches,
    get_match_details as get_statsbomb_match_details,
    get_team_matches as get_statsbomb_team_matches,
    update_competition_data as update_statsbomb_competition,
    update_all_competitions as update_all_statsbomb
)

# Funzioni di utilità

def get_all_sources() -> List[Dict[str, Any]]:
    """
    Ottiene la lista di tutte le fonti di dati disponibili.
    
    Returns:
        Lista delle fonti di dati
    """
    sources = [
        {
            "id": "open_football",
            "name": "OpenFootball",
            "description": "Dati calcistici open source da GitHub",
            "website": "https://github.com/openfootball",
            "type": "open_data",
            "data_types": ["teams", "matches", "standings"]
        },
        {
            "id": "rsssf",
            "name": "RSSSF (Rec.Sport.Soccer Statistics Foundation)",
            "description": "Archivio storico di statistiche calcistiche",
            "website": "http://www.rsssf.com",
            "type": "open_data",
            "data_types": ["historical", "standings", "results"]
        },
        {
            "id": "kaggle",
            "name": "Kaggle Datasets",
            "description": "Dataset calcistici da Kaggle",
            "website": "https://www.kaggle.com",
            "type": "open_data",
            "data_types": ["matches", "players", "international"]
        },
        {
            "id": "statsbomb",
            "name": "StatsBomb Open Data",
            "description": "Dati dettagliati a livello di eventi per partite selezionate",
            "website": "https://github.com/statsbomb/open-data",
            "type": "open_data",
            "data_types": ["events", "lineups", "statistics"]
        }
    ]
    
    return sources

def get_teams(source: str, **kwargs) -> List[Dict[str, Any]]:
    """
    Ottiene le squadre da una fonte specifica.
    
    Args:
        source: ID della fonte di dati
        **kwargs: Parametri specifici per fonte
    
    Returns:
        Lista delle squadre
    """
    if source == "open_football":
        return get_open_football_teams(**kwargs)
    else:
        return []

def get_matches(source: str, **kwargs) -> List[Dict[str, Any]]:
    """
    Ottiene le partite da una fonte specifica.
    
    Args:
        source: ID della fonte di dati
        **kwargs: Parametri specifici per fonte
    
    Returns:
        Lista delle partite
    """
    if source == "open_football":
        return get_open_football_matches(**kwargs)
    elif source == "kaggle":
        dataset = kwargs.pop("dataset", "international_matches")
        if dataset == "international_matches":
            return get_international_matches(**kwargs)
        elif dataset == "world_cup":
            return get_world_cup_matches(**kwargs)
        elif dataset == "premier_league":
            return get_premier_league_matches(**kwargs)
    elif source == "statsbomb":
        competition_id = kwargs.get("competition_id")
        season_id = kwargs.get("season_id")
        if competition_id and season_id:
            return get_statsbomb_matches(competition_id, season_id)
    
    return []

def get_standings(source: str, **kwargs) -> List[Dict[str, Any]]:
    """
    Ottiene le classifiche da una fonte specifica.
    
    Args:
        source: ID della fonte di dati
        **kwargs: Parametri specifici per fonte
    
    Returns:
        Classifica
    """
    if source == "open_football":
        return get_open_football_standings(**kwargs)
    elif source == "rsssf":
        return get_rsssf_league_table(**kwargs)
    
    return []

def get_historical_data(source: str, **kwargs) -> Dict[str, Any]:
    """
    Ottiene dati storici da una fonte specifica.
    
    Args:
        source: ID della fonte di dati
        **kwargs: Parametri specifici per fonte
    
    Returns:
        Dati storici
    """
    if source == "rsssf":
        country = kwargs.get("country")
        if country:
            return {"seasons": get_rsssf_seasons(country)}
    elif source == "kaggle":
        dataset = kwargs.get("dataset")
        if dataset == "world_cup":
            return {"matches": get_world_cup_matches(**kwargs)}
    
    return {}

def search_matches(query: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Cerca partite in tutte le fonti di dati in base ai criteri di ricerca.
    
    Args:
        query: Criteri di ricerca
    
    Returns:
        Lista delle partite che corrispondono ai criteri
    """
    results = []
    
    # Estrai parametri di ricerca
    team = query.get("team")
    start_date = query.get("start_date")
    end_date = query.get("end_date")
    league = query.get("league")
    
    # Cerca in OpenFootball
    if league:
        open_football_matches = get_open_football_matches(league_id=league)
        for match in open_football_matches:
            if team and (match.get("home_team") != team and match.get("away_team") != team):
                continue
            # Altre verifiche di filtro...
            results.append({
                "source": "open_football",
                "data": match
            })
    
    # Cerca in Kaggle
    kaggle_matches = []
    if league == "international":
        kaggle_matches = get_international_matches(team=team)
    elif league == "world_cup":
        kaggle_matches = get_world_cup_matches(team=team)
    elif league == "premier_league":
        kaggle_matches = get_premier_league_matches(team=team)
    
    for match in kaggle_matches:
        results.append({
            "source": "kaggle",
            "data": match
        })
    
    return results

def update_source_data(source: str, **kwargs) -> Dict[str, Any]:
    """
    Aggiorna i dati per una fonte specifica.
    
    Args:
        source: ID della fonte di dati
        **kwargs: Parametri specifici per fonte
    
    Returns:
        Risultato dell'operazione
    """
    if source == "open_football":
        league_id = kwargs.get("league_id")
        if league_id:
            return update_open_football_league(league_id, kwargs.get("season"))
        else:
            return update_all_open_football(kwargs.get("seasons"))
    elif source == "rsssf":
        country_id = kwargs.get("country_id")
        if country_id:
            return update_rsssf_country(country_id)
        else:
            return update_all_rsssf()
    elif source == "kaggle":
        dataset_id = kwargs.get("dataset_id")
        if dataset_id:
            return update_kaggle_dataset(dataset_id)
        else:
            return update_all_kaggle()
    elif source == "statsbomb":
        competition_id = kwargs.get("competition_id")
        season_id = kwargs.get("season_id")
        if competition_id and season_id:
            return update_statsbomb_competition(competition_id, season_id)
        else:
            return update_all_statsbomb()
    
    return {"error": f"Fonte {source} non supportata"}

def update_all_sources() -> Dict[str, Dict[str, Any]]:
    """
    Aggiorna i dati per tutte le fonti disponibili.
    
    Returns:
        Risultato dell'operazione per fonte
    """
    results = {}
    
    # OpenFootball
    try:
        results["open_football"] = update_all_open_football()
    except Exception as e:
        results["open_football"] = {"error": str(e)}
    
    # RSSSF
    try:
        results["rsssf"] = update_all_rsssf()
    except Exception as e:
        results["rsssf"] = {"error": str(e)}
    
    # Kaggle
    try:
        results["kaggle"] = update_all_kaggle()
    except Exception as e:
        results["kaggle"] = {"error": str(e)}
    
    # StatsBomb
    try:
        results["statsbomb"] = update_all_statsbomb()
    except Exception as e:
        results["statsbomb"] = {"error": str(e)}
    
    return results
