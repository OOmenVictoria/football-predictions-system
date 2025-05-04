""" 
Package open_data per l'integrazione di fonti di dati calcistici open source.
Questo package fornisce accesso unificato a varie fonti di dati calcistici gratuite.
"""
from typing import Dict, List, Any, Optional, Union
from functools import wraps
import logging

# Importa le funzioni dai moduli (usando nomi standardizzati)
from src.data.open_data.open_football import (
    OpenFootballClient,
    get_open_football_client as get_open_football,
    get_teams as get_open_football_teams,
    get_matches as get_open_football_matches,
    get_standings as get_open_football_standings,
    update_league_data as update_open_football_league,
    update_all_leagues as update_all_open_football
)

from src.data.open_data.rsssf import (
    RSSFFScraper,
    get_rsssf_scraper as get_rsssf,
    get_seasons_for_country as get_rsssf_seasons,
    get_league_table as get_rsssf_league_table,
    update_country_data as update_rsssf_country,
    update_all_countries as update_all_rsssf
)

from src.data.open_data.kaggle_loader import (
    KaggleDataLoader,
    get_kaggle_loader as get_kaggle,
    get_available_datasets as get_kaggle_datasets,
    get_international_matches,
    get_world_cup_matches,
    get_premier_league_matches,
    update_dataset as update_kaggle_dataset,
    update_all_datasets as update_all_kaggle
)

from src.data.open_data.statsbomb import (
    StatsBombClient,
    get_statsbomb_client as get_statsbomb,
    get_competitions as get_statsbomb_competitions,
    get_matches as get_statsbomb_matches,
    get_match_details as get_statsbomb_match_details,
    get_team_matches as get_statsbomb_team_matches,
    update_competition_data as update_statsbomb_competition,
    update_all_competitions as update_all_statsbomb
)

# Logger per il package
logger = logging.getLogger(__name__)

# Tipizzazione personalizzata per standardizzare le risposte
class StandardResponse:
    """Risposta standardizzata per tutte le operazioni."""
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

# Decorator per standardizzare le risposte
def standardize_response(func):
    """Decorator per standardizzare il formato delle risposte."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
            if isinstance(result, dict) and ("success" in result or "error" in result):
                return result
            elif isinstance(result, list) or isinstance(result, dict):
                return StandardResponse(success=len(result) if hasattr(result, '__len__') else 1, data=result).to_dict()
            else:
                return StandardResponse(error="Unknown response format").to_dict()
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {e}")
            return StandardResponse(error=str(e)).to_dict()
    return wrapper

# Funzione di supporto per gestire la selezione della fonte
def _resolve_source(source: str) -> Dict[str, Any]:
    """Risolve il source e ritorna le funzioni appropriate."""
    source_map = {
        "open_football": {
            "client": OpenFootballClient,
            "get_client": get_open_football
        },
        "rsssf": {
            "client": RSSFFScraper,
            "get_client": get_rsssf
        },
        "kaggle": {
            "client": KaggleDataLoader,
            "get_client": get_kaggle
        },
        "statsbomb": {
            "client": StatsBombClient,
            "get_client": get_statsbomb
        }
    }
    return source_map.get(source.lower(), {})

# Funzioni di supporto per standardizzare i parametri
def _standardize_team_params(source: str, **kwargs) -> Dict[str, Any]:
    """Standardizza i parametri per le funzioni get_teams."""
    if source in ["rsssf", "kaggle", "statsbomb"]:
        # Rimuovi parametri non supportati
        return {k: v for k, v in kwargs.items() if k not in ["league_id", "season"]}
    return kwargs

def _standardize_matches_params(source: str, **kwargs) -> Dict[str, Any]:
    """Standardizza i parametri per le funzioni get_matches."""
    if source == "rsssf":
        # RSSSF usa solo URL
        if "season_url" in kwargs:
            return {"season_url": kwargs["season_url"]}
        return {}
    elif source == "kaggle":
        # Kaggle usa nomi specifici di dataset
        if "dataset" in kwargs:
            return kwargs
        return {}
    return kwargs

# Funzioni di utilità globali standardizzate

def get_all_sources() -> List[Dict[str, Any]]:
    """
    Ottiene la lista di tutte le fonti di dati disponibili in formato standardizzato.
    
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
            "data_types": ["teams", "matches", "standings"],
            "supported_methods": ["get_teams", "get_matches", "get_standings", "update_data"]
        },
        {
            "id": "rsssf",
            "name": "RSSSF (Rec.Sport.Soccer Statistics Foundation)",
            "description": "Archivio storico di statistiche calcistiche",
            "website": "http://www.rsssf.com",
            "type": "open_data",
            "data_types": ["historical", "standings", "results"],
            "supported_methods": ["get_seasons", "get_standing", "update_data"]
        },
        {
            "id": "kaggle",
            "name": "Kaggle Datasets",
            "description": "Dataset calcistici da Kaggle",
            "website": "https://www.kaggle.com",
            "type": "open_data",
            "data_types": ["matches", "players", "international"],
            "supported_methods": ["get_matches", "get_datasets", "update_data"]
        },
        {
            "id": "statsbomb",
            "name": "StatsBomb Open Data",
            "description": "Dati dettagliati a livello di eventi per partite selezionate",
            "website": "https://github.com/statsbomb/open-data",
            "type": "open_data",
            "data_types": ["events", "lineups", "statistics"],
            "supported_methods": ["get_matches", "get_competitions", "get_events", "update_data"]
        }
    ]
    
    return sources

@standardize_response
def get_teams(source: str, **kwargs) -> Dict[str, Any]:
    """
    Ottiene le squadre da una fonte specifica in formato standardizzato.
    
    Args:
        source: ID della fonte di dati (open_football, rsssf, kaggle, statsbomb)
        **kwargs: Parametri specifici per fonte
            - league_id: ID del campionato
            - season: Stagione (formato YYYY-YY o YYYY)
            - country: Paese per RSSSF
    
    Returns:
        Risposta standardizzata con lista delle squadre
    """
    source_config = _resolve_source(source)
    if not source_config:
        return StandardResponse(error=f"Fonte '{source}' non supportata").to_dict()
    
    params = _standardize_team_params(source, **kwargs)
    
    if source == "open_football":
        return get_open_football_teams(**params)
    elif source == "statsbomb":
        # StatsBomb non ha una funzione get_teams, ma possiamo estrarre le squadre dalle competizioni
        competitions = get_statsbomb_competitions()
        teams = []
        for comp in competitions:
            matches = get_statsbomb_matches(comp.get("competition_id"), comp.get("season_id"))
            for match in matches:
                if "home_team" in match:
                    teams.append(match["home_team"])
                if "away_team" in match:
                    teams.append(match["away_team"])
        # Rimuovi duplicati
        teams = list({team["id"]: team for team in teams if isinstance(team, dict) and "id" in team}.values())
        return StandardResponse(success=len(teams), data=teams).to_dict()
    else:
        return StandardResponse(error=f"Funzione 'get_teams' non disponibile per '{source}'").to_dict()

@standardize_response
def get_matches(source: str, **kwargs) -> Dict[str, Any]:
    """
    Ottiene le partite da una fonte specifica in formato standardizzato.
    
    Args:
        source: ID della fonte di dati
        **kwargs: Parametri specifici per fonte
            - league_id: ID del campionato
            - season: Stagione
            - dataset: Nome del dataset per Kaggle
            - competition_id, season_id: Per StatsBomb
            - team: Nome squadra per filtro
    
    Returns:
        Risposta standardizzata con lista delle partite
    """
    source_config = _resolve_source(source)
    if not source_config:
        return StandardResponse(error=f"Fonte '{source}' non supportata").to_dict()
    
    params = _standardize_matches_params(source, **kwargs)
    
    if source == "open_football":
        return get_open_football_matches(**params)
    elif source == "kaggle":
        dataset = kwargs.get("dataset", "international_matches")
        if dataset == "international_matches":
            return get_international_matches(**params)
        elif dataset == "world_cup":
            return get_world_cup_matches(**params)
        elif dataset == "premier_league":
            return get_premier_league_matches(**params)
        else:
            return StandardResponse(error=f"Dataset '{dataset}' non supportato per Kaggle").to_dict()
    elif source == "statsbomb":
        competition_id = kwargs.get("competition_id")
        season_id = kwargs.get("season_id")
        if competition_id and season_id:
            return get_statsbomb_matches(competition_id, season_id)
        else:
            return StandardResponse(error="Parametri competition_id e season_id richiesti per StatsBomb").to_dict()
    elif source == "rsssf":
        # RSSSF usa get_league_table per ottenere i risultati
        season_url = kwargs.get("season_url")
        if season_url:
            data = get_rsssf_league_table(season_url)
            return StandardResponse(success=len(data.get("results", [])), data=data.get("results", [])).to_dict()
        else:
            return StandardResponse(error="Parametro season_url richiesto per RSSSF").to_dict()
    
    return StandardResponse(error=f"Metodo per fonte '{source}' non implementato").to_dict()

@standardize_response
def get_standings(source: str, **kwargs) -> Dict[str, Any]:
    """
    Ottiene le classifiche da una fonte specifica in formato standardizzato.
    
    Args:
        source: ID della fonte di dati
        **kwargs: Parametri specifici per fonte
    
    Returns:
        Risposta standardizzata con classifica
    """
    source_config = _resolve_source(source)
    if not source_config:
        return StandardResponse(error=f"Fonte '{source}' non supportata").to_dict()
    
    if source == "open_football":
        return get_open_football_standings(**kwargs)
    elif source == "rsssf":
        season_url = kwargs.get("season_url")
        if season_url:
            data = get_rsssf_league_table(season_url)
            return StandardResponse(success=len(data.get("standings", [])), data=data.get("standings", [])).to_dict()
        else:
            return StandardResponse(error="Parametro season_url richiesto per RSSSF").to_dict()
    else:
        return StandardResponse(error=f"Funzione 'get_standings' non disponibile per '{source}'").to_dict()

@standardize_response
def get_historical_data(source: str, **kwargs) -> Dict[str, Any]:
    """
    Ottiene dati storici da una fonte specifica.
    
    Args:
        source: ID della fonte di dati
        **kwargs: Parametri specifici per fonte
    
    Returns:
        Risposta standardizzata con dati storici
    """
    source_config = _resolve_source(source)
    if not source_config:
        return StandardResponse(error=f"Fonte '{source}' non supportata").to_dict()
    
    if source == "rsssf":
        country = kwargs.get("country")
        if country:
            seasons = get_rsssf_seasons(country)
            return StandardResponse(success=len(seasons), data={"seasons": seasons}).to_dict()
        else:
            return StandardResponse(error="Parametro country richiesto per RSSSF").to_dict()
    elif source == "kaggle":
        dataset = kwargs.get("dataset")
        if dataset == "world_cup":
            matches = get_world_cup_matches(**kwargs)
            return StandardResponse(success=len(matches), data={"matches": matches}).to_dict()
        else:
            return StandardResponse(error=f"Dataset '{dataset}' non supportato per dati storici").to_dict()
    else:
        return StandardResponse(error=f"Funzione 'get_historical_data' non disponibile per '{source}'").to_dict()

@standardize_response
def search_matches(query: Dict[str, Any]) -> Dict[str, Any]:
    """
    Cerca partite in tutte le fonti di dati in base ai criteri di ricerca.
    
    Args:
        query: Criteri di ricerca
            - team: Nome squadra
            - start_date: Data inizio
            - end_date: Data fine
            - league: Campionato
            - sources: Lista di fonti specifiche (opzionale)
    
    Returns:
        Risposta standardizzata con lista delle partite
    """
    results = []
    
    # Estrai parametri di ricerca
    team = query.get("team")
    start_date = query.get("start_date")
    end_date = query.get("end_date")
    league = query.get("league")
    sources = query.get("sources", list(_resolve_source.keys()))
    
    for source in sources:
        try:
            # Cerca partite in base alla fonte
            if source == "open_football":
                matches = get_open_football_matches(league_id=league)
                for match in matches:
                    if team and (match.get("home_team") != team and match.get("away_team") != team):
                        continue
                    results.append({
                        "source": source,
                        "data": match
                    })
            
            elif source == "kaggle":
                if league == "international":
                    matches = get_international_matches(team=team)
                elif league == "world_cup":
                    matches = get_world_cup_matches(team=team)
                elif league == "premier_league":
                    matches = get_premier_league_matches(team=team)
                else:
                    continue
                
                for match in matches:
                    results.append({
                        "source": source,
                        "data": match
                    })
            
            elif source == "statsbomb":
                # Per StatsBomb, cerchiamo per team
                if team:
                    # Nota: questa è una ricerca semplificata
                    competitions = get_statsbomb_competitions()
                    for comp in competitions[:3]:  # Limita a 3 competizioni per performance
                        comp_id = comp.get("competition_id")
                        season_id = comp.get("season_id")
                        if comp_id and season_id:
                            matches = get_statsbomb_matches(comp_id, season_id)
                            for match in matches:
                                if (team in str(match.get("home_team", "")) or 
                                    team in str(match.get("away_team", ""))):
                                    results.append({
                                        "source": source,
                                        "data": match
                                    })
        
        except Exception as e:
            logger.error(f"Errore nella ricerca per fonte '{source}': {e}")
    
    return StandardResponse(success=len(results), data=results).to_dict()

@standardize_response
def update_source_data(source: str, **kwargs) -> Dict[str, Any]:
    """
    Aggiorna i dati per una fonte specifica in formato standardizzato.
    
    Args:
        source: ID della fonte di dati
        **kwargs: Parametri specifici per fonte
    
    Returns:
        Risposta standardizzata con risultato dell'operazione
    """
    source_config = _resolve_source(source)
    if not source_config:
        return StandardResponse(error=f"Fonte '{source}' non supportata").to_dict()
    
    try:
        if source == "open_football":
            league_id = kwargs.get("league_id")
            if league_id:
                result = update_open_football_league(league_id, kwargs.get("season"))
            else:
                result = update_all_open_football(kwargs.get("seasons"))
            
        elif source == "rsssf":
            country_id = kwargs.get("country_id")
            if country_id:
                result = update_rsssf_country(country_id)
            else:
                result = update_all_rsssf()
            
        elif source == "kaggle":
            dataset_id = kwargs.get("dataset_id")
            if dataset_id:
                result = update_kaggle_dataset(dataset_id)
            else:
                result = update_all_kaggle()
            
        elif source == "statsbomb":
            competition_id = kwargs.get("competition_id")
            season_id = kwargs.get("season_id")
            if competition_id and season_id:
                result = update_statsbomb_competition(competition_id, season_id)
            else:
                result = update_all_statsbomb()
        
        else:
            return StandardResponse(error=f"Metodo update non implementato per '{source}'").to_dict()
        
        # Standardizza il risultato
        if isinstance(result, dict):
            total_success = sum(v.get("success", 0) for v in result.values())
            first_error = next((v.get("error") for v in result.values() if v.get("error")), None)
            return StandardResponse(success=total_success, error=first_error, data=result).to_dict()
        
        return result
        
    except Exception as e:
        return StandardResponse(error=str(e)).to_dict()

@standardize_response
def update_all_sources() -> Dict[str, Any]:
    """
    Aggiorna i dati per tutte le fonti disponibili.
    
    Returns:
        Risposta standardizzata con risultato dell'operazione per fonte
    """
    results = {}
    total_success = 0
    
    for source in ["open_football", "rsssf", "kaggle", "statsbomb"]:
        try:
            result = update_source_data(source)
            results[source] = result
            
            # Estrai successo totale
            if result and isinstance(result, dict) and "success" in result:
                total_success += result["success"]
        
        except Exception as e:
            logger.error(f"Errore nell'aggiornamento di '{source}': {e}")
            results[source] = StandardResponse(error=str(e)).to_dict()
    
    return StandardResponse(success=total_success, data=results).to_dict()

# Funzioni aggiuntive per compatibilità con interfacce specifiche

def get_competitions(source: str = "statsbomb") -> List[Dict[str, Any]]:
    """
    Ottiene le competizioni disponibili (principalmente per StatsBomb).
    
    Args:
        source: ID della fonte di dati (default: statsbomb)
    
    Returns:
        Lista delle competizioni
    """
    if source == "statsbomb":
        response = standardize_response(get_statsbomb_competitions)()
        return response.get("data", [])
    else:
        return []

def get_events(source: str, match_id: int) -> List[Dict[str, Any]]:
    """
    Ottiene gli eventi per una partita specifica (principalmente per StatsBomb).
    
    Args:
        source: ID della fonte di dati
        match_id: ID della partita
    
    Returns:
        Lista degli eventi
    """
    if source == "statsbomb":
        response = standardize_response(get_statsbomb_match_details)(match_id)
        events = response.get("data", {}).get("events", [])
        return events
    else:
        return []

def get_datasets(source: str = "kaggle") -> Dict[str, Any]:
    """
    Ottiene i dataset disponibili (principalmente per Kaggle).
    
    Args:
        source: ID della fonte di dati (default: kaggle)
    
    Returns:
        Dizionario con i dataset disponibili
    """
    if source == "kaggle":
        response = standardize_response(get_kaggle_datasets)()
        return response.get("data", {})
    else:
        return {}

# Informazioni sulla versione e configurazione del package
__version__ = "1.0.0"
__author__ = "Soccer Data Integration Package"
__license__ = "MIT"

# Esporta tutte le funzioni principali
__all__ = [
    "get_all_sources",
    "get_teams",
    "get_matches",
    "get_standings",
    "get_historical_data",
    "search_matches",
    "update_source_data",
    "update_all_sources",
    "get_competitions",
    "get_events",
    "get_datasets",
    "StandardResponse",
    "standardize_response"
]
