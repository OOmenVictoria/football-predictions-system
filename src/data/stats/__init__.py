"""
Pacchetto per l'estrazione di statistiche calcistiche avanzate.
Questo pacchetto fornisce moduli per estrarre statistiche avanzate da vari siti specializzati,
inclusi dati di Expected Goals (xG), metriche di prestazione e valutazioni.
"""
from typing import Dict, List, Any, Optional, Union, Callable
from functools import wraps
import logging
import datetime

# Importa tutte le funzioni principali dai vari moduli
from src.data.stats.fbref import (
    get_team_stats as get_fbref_team_stats,
    get_player_stats as get_fbref_player_stats,
    get_match_stats as get_fbref_match_stats,
    get_scraper as get_fbref_scraper
)

from src.data.stats.understat import (
    get_team_xg as get_understat_team_xg,
    get_player_xg as get_understat_player_xg,
    get_match_xg as get_understat_match_xg,
    get_scraper as get_understat_scraper
)

from src.data.stats.footystats import (
    get_team_stats as get_footystats_team_stats,
    get_match_stats as get_footystats_match_stats,
    get_scraper as get_footystats_scraper
)

from src.data.stats.sofascore import (
    get_team_stats as get_sofascore_team_stats,
    get_player_ratings as get_sofascore_player_ratings,
    get_match_stats as get_sofascore_match_stats,
    get_scraper as get_sofascore_scraper
)

from src.data.stats.whoscored import (
    get_team_stats as get_whoscored_team_stats,
    get_player_stats as get_whoscored_player_stats,
    get_match_stats as get_whoscored_match_stats,
    get_scraper as get_whoscored_scraper
)

logger = logging.getLogger(__name__)

# Registry delle fonti di statistiche disponibili
STATS_SOURCES = {
    "fbref": {
        "name": "FBref",
        "description": "Statistiche avanzate da Sports Reference",
        "url": "https://fbref.com/",
        "capabilities": {
            "team_stats": True,
            "player_stats": True,
            "match_stats": True,
            "xg_data": True,
            "player_ratings": False,
            "advanced_metrics": True,
            "historical_data": True,
            "formations": True,
            "progressive_stats": True
        }
    },
    "understat": {
        "name": "Understat",
        "description": "Dati Expected Goals (xG) dettagliati",
        "url": "https://understat.com/",
        "capabilities": {
            "team_stats": True,
            "player_stats": True,
            "match_stats": True,
            "xg_data": True,
            "player_ratings": False,
            "advanced_metrics": True,
            "shot_data": True,
            "xg_timeline": True,
            "xg_by_position": True
        }
    },
    "footystats": {
        "name": "FootyStats",
        "description": "Statistiche dettagliate per analisi e scommesse",
        "url": "https://footystats.org/",
        "capabilities": {
            "team_stats": True,
            "player_stats": False,
            "match_stats": True,
            "xg_data": True,
            "betting_stats": True,
            "form_analysis": True,
            "h2h_data": True,
            "league_stats": True
        }
    },
    "sofascore": {
        "name": "SofaScore",
        "description": "Statistiche e valutazioni giocatori in tempo reale",
        "url": "https://www.sofascore.com/",
        "capabilities": {
            "team_stats": True,
            "player_stats": True,
            "match_stats": True,
            "xg_data": True,
            "player_ratings": True,
            "live_stats": True,
            "lineups": True,
            "incidents": True
        }
    },
    "whoscored": {
        "name": "WhoScored-like",
        "description": "Metriche di prestazione avanzate e valutazioni",
        "url": "https://www.whoscored.com/",
        "capabilities": {
            "team_stats": True,
            "player_stats": True,
            "match_stats": True,
            "xg_data": True,
            "player_ratings": True,
            "match_ratings": True,
            "tactical_analysis": True,
            "touch_heatmaps": False
        }
    }
}

# Cache delle istanze singleton
_scraper_instances = {}

# Tracciamento delle operazioni per priorità di fonte
SOURCE_PRIORITIES = {
    "xg_data": ["understat", "fbref", "sofascore", "whoscored", "footystats"],
    "player_ratings": ["sofascore", "whoscored"],
    "advanced_metrics": ["fbref", "understat", "whoscored"],
    "live_stats": ["sofascore"],
    "tactical_analysis": ["whoscored"],
    "historical_data": ["fbref", "understat"],
    "betting_stats": ["footystats"]
}

class StatsError(Exception):
    """Errore generico per le operazioni di statistiche."""
    pass

class StatsResponse:
    """Risposta standardizzata per le operazioni di statistiche."""
    
    def __init__(self, success: bool = True, data: Any = None, 
                 error: Optional[str] = None, source: Optional[str] = None):
        self.success = success
        self.data = data
        self.error = error
        self.source = source
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'success': self.success,
            'data': self.data,
            'error': self.error,
            'source': self.source
        }

def standardize_response(func):
    """
    Decorator per standardizzare le risposte dei metodi di statistiche.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
            if result is None:
                return StatsResponse(success=False, error="No data returned")
            return StatsResponse(success=True, data=result)
        except Exception as e:
            logger.error(f"Errore in {func.__name__}: {str(e)}")
            return StatsResponse(success=False, error=str(e))
    return wrapper

def with_source_tracking(func):
    """
    Decorator per tracciare la fonte dei dati nelle risposte.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        source = kwargs.pop('source', None) or (args[1] if len(args) > 1 else None)
        result = func(*args, **kwargs)
        
        if isinstance(result, StatsResponse):
            result.source = source
        elif isinstance(result, dict):
            result['source'] = source
        
        return result
    return wrapper

def get_scraper(source: str):
    """
    Ottiene un'istanza singleton dello scraper specificato.
    
    Args:
        source: Tipo di scraper ('fbref', 'understat', ecc.)
    
    Returns:
        Istanza dello scraper richiesto
    
    Raises:
        ValueError: Se il tipo di scraper non è supportato
    """
    if source not in STATS_SOURCES:
        available_sources = ', '.join(STATS_SOURCES.keys())
        raise ValueError(f"Fonte non supportata: {source}. Disponibili: {available_sources}")
    
    # Verifica se l'istanza è già nella cache
    if source not in _scraper_instances:
        # Usa il metodo get_scraper specifico
        if source == 'fbref':
            _scraper_instances[source] = get_fbref_scraper()
        elif source == 'understat':
            _scraper_instances[source] = get_understat_scraper()
        elif source == 'footystats':
            _scraper_instances[source] = get_footystats_scraper()
        elif source == 'sofascore':
            _scraper_instances[source] = get_sofascore_scraper()
        elif source == 'whoscored':
            _scraper_instances[source] = get_whoscored_scraper()
    
    return _scraper_instances[source]

def get_available_stats_sources() -> Dict[str, Dict[str, Union[str, bool]]]:
    """
    Ottiene la lista delle fonti di statistiche disponibili.
    
    Returns:
        Dizionario delle fonti disponibili con le loro capacità
    """
    return STATS_SOURCES

def get_sources_by_capability(capability: str) -> List[str]:
    """
    Ottiene le fonti che supportano una specifica capacità.
    
    Args:
        capability: Nome della capacità da cercare
    
    Returns:
        Lista di fonti che supportano la capacità
    """
    sources = []
    for source, info in STATS_SOURCES.items():
        if info.get("capabilities", {}).get(capability, False):
            sources.append(source)
    return sources

def get_best_source_for(capability: str) -> str:
    """
    Ottiene la migliore fonte per una specifica capacità.
    
    Args:
        capability: Nome della capacità
    
    Returns:
        Nome della migliore fonte
    """
    if capability in SOURCE_PRIORITIES:
        priorities = SOURCE_PRIORITIES[capability]
        for source in priorities:
            if source in STATS_SOURCES:
                return source
    
    # Fallback alla prima fonte disponibile
    sources = get_sources_by_capability(capability)
    return sources[0] if sources else "fbref"

# Interfaccia unificata per operazioni comuni

@standardize_response
@with_source_tracking
def get_team_stats(team_id: str, source: str = "auto", **kwargs) -> Dict[str, Any]:
    """
    Ottiene statistiche di una squadra dalla fonte specificata o automaticamente.
    
    Args:
        team_id: ID della squadra
        source: Fonte di statistiche da utilizzare ("auto" per selezione automatica)
        **kwargs: Parametri aggiuntivi specifici della fonte
    
    Returns:
        Statistiche della squadra
    """
    if source == "auto":
        source = get_best_source_for("team_stats")
    
    if source == "fbref":
        return get_fbref_team_stats(team_id, **kwargs)
    elif source == "understat":
        # Understat usa funzione diversa
        return get_understat_team_xg(int(team_id), **kwargs)
    elif source == "footystats":
        return get_footystats_team_stats(team_id, **kwargs)
    elif source == "sofascore":
        return get_sofascore_team_stats(team_id, **kwargs)
    elif source == "whoscored":
        return get_whoscored_team_stats(team_id, **kwargs)
    else:
        raise ValueError(f"Fonte non supportata: {source}")

@standardize_response
@with_source_tracking
def get_player_stats(player_id: str, source: str = "auto", **kwargs) -> Dict[str, Any]:
    """
    Ottiene statistiche di un giocatore dalla fonte specificata o automaticamente.
    
    Args:
        player_id: ID del giocatore
        source: Fonte di statistiche da utilizzare ("auto" per selezione automatica)
        **kwargs: Parametri aggiuntivi specifici della fonte
    
    Returns:
        Statistiche del giocatore
    """
    if source == "auto":
        source = get_best_source_for("player_stats")
    
    if source == "fbref":
        return get_fbref_player_stats(player_id, **kwargs)
    elif source == "understat":
        # Understat usa funzione diversa
        return get_understat_player_xg(int(player_id), **kwargs)
    elif source == "sofascore":
        return get_sofascore_player_ratings(player_id, **kwargs)
    elif source == "whoscored":
        return get_whoscored_player_stats(player_id, **kwargs)
    else:
        raise ValueError(f"Fonte non supportata per player_stats: {source}")

@standardize_response
@with_source_tracking
def get_match_stats(match_id: str, source: str = "auto", **kwargs) -> Dict[str, Any]:
    """
    Ottiene statistiche di una partita dalla fonte specificata o automaticamente.
    
    Args:
        match_id: ID della partita
        source: Fonte di statistiche da utilizzare ("auto" per selezione automatica)
        **kwargs: Parametri aggiuntivi specifici della fonte
    
    Returns:
        Statistiche della partita
    """
    if source == "auto":
        source = get_best_source_for("match_stats")
    
    if source == "fbref":
        return get_fbref_match_stats(match_id, **kwargs)
    elif source == "understat":
        # Understat usa funzione diversa
        return get_understat_match_xg(int(match_id), **kwargs)
    elif source == "footystats":
        return get_footystats_match_stats(match_id, **kwargs)
    elif source == "sofascore":
        return get_sofascore_match_stats(match_id, **kwargs)
    elif source == "whoscored":
        return get_whoscored_match_stats(match_id, **kwargs)
    else:
        raise ValueError(f"Fonte non supportata: {source}")

@standardize_response
def get_team_xg(team_id: str, source: str = "auto", **kwargs) -> Dict[str, Any]:
    """
    Ottiene dati Expected Goals (xG) per una squadra.
    
    Args:
        team_id: ID della squadra
        source: Fonte di statistiche da utilizzare ("auto" per selezione automatica)
        **kwargs: Parametri aggiuntivi specifici della fonte
    
    Returns:
        Dati xG della squadra
    """
    if source == "auto":
        source = get_best_source_for("xg_data")
    
    if source == "understat":
        return get_understat_team_xg(int(team_id), **kwargs)
    elif source == "fbref":
        stats = get_fbref_team_stats(team_id, **kwargs)
        if stats and "xg" in stats:
            return stats["xg"]
        return None
    elif source == "sofascore":
        stats = get_sofascore_team_stats(team_id, **kwargs)
        if stats and "xg" in stats:
            return stats["xg"]
        return None
    elif source == "whoscored":
        stats = get_whoscored_team_stats(team_id, **kwargs)
        if stats and "xg" in stats:
            return stats["xg"]
        return None
    else:
        raise ValueError(f"Fonte non supportata per team_xg: {source}")

@standardize_response
def get_player_xg(player_id: str, source: str = "auto", **kwargs) -> Dict[str, Any]:
    """
    Ottiene dati Expected Goals (xG) per un giocatore.
    
    Args:
        player_id: ID del giocatore
        source: Fonte di statistiche da utilizzare ("auto" per selezione automatica)
        **kwargs: Parametri aggiuntivi specifici della fonte
    
    Returns:
        Dati xG del giocatore
    """
    if source == "auto":
        source = get_best_source_for("xg_data")
    
    if source == "understat":
        return get_understat_player_xg(int(player_id), **kwargs)
    elif source == "fbref":
        stats = get_fbref_player_stats(player_id, **kwargs)
        if stats and "xg" in stats:
            return stats["xg"]
        return None
    elif source == "whoscored":
        stats = get_whoscored_player_stats(player_id, **kwargs)
        if stats and "xg" in stats:
            return stats["xg"]
        return None
    else:
        raise ValueError(f"Fonte non supportata per player_xg: {source}")

@standardize_response
def get_match_xg(match_id: str, priority_sources: Optional[List[str]] = None) -> Dict[str, float]:
    """
    Ottiene i dati Expected Goals (xG) per una partita,
    con fallback tra le varie fonti basato sulle priorità.
    
    Args:
        match_id: ID della partita
        priority_sources: Lista opzionale di fonti in ordine di priorità
    
    Returns:
        Dati xG della partita (home_xg, away_xg, source)
    """
    # Usa priorità predefinite se non specificate
    sources = priority_sources or SOURCE_PRIORITIES["xg_data"]
    
    for source in sources:
        try:
            if source == "understat":
                result = get_understat_match_xg(int(match_id))
                if result and "home_xg" in result and "away_xg" in result:
                    return {
                        "home_xg": result["home_xg"],
                        "away_xg": result["away_xg"],
                        "source": "understat"
                    }
            elif source == "fbref":
                result = get_fbref_match_stats(match_id)
                if result and "xg" in result:
                    return {
                        "home_xg": result["xg"]["home"],
                        "away_xg": result["xg"]["away"],
                        "source": "fbref"
                    }
            elif source == "sofascore":
                result = get_sofascore_match_stats(match_id)
                if result and "xg" in result.get("team_stats", {}).get("home", {}):
                    home_stats = result["team_stats"]["home"]
                    away_stats = result["team_stats"]["away"]
                    return {
                        "home_xg": home_stats["xg"],
                        "away_xg": away_stats["xg"],
                        "source": "sofascore"
                    }
            elif source == "whoscored":
                result = get_whoscored_match_stats(match_id)
                if result and "stats" in result.get("home", {}):
                    home_stats = result["home"]["stats"]
                    away_stats = result["away"]["stats"]
                    if "xg" in home_stats and "xg" in away_stats:
                        return {
                            "home_xg": home_stats["xg"],
                            "away_xg": away_stats["xg"],
                            "source": "whoscored"
                        }
            elif source == "footystats":
                result = get_footystats_match_stats(match_id)
                if result and "xg" in result:
                    return {
                        "home_xg": result["xg"]["home"],
                        "away_xg": result["xg"]["away"],
                        "source": "footystats"
                    }
        except Exception as e:
            logger.error(f"Errore con fonte {source}: {str(e)}")
            continue
    
    # Se non troviamo nulla, restituisci valori di default
    return {
        "home_xg": 0.0,
        "away_xg": 0.0,
        "source": "none"
    }

@standardize_response
def get_league_stats(league_id: str, source: str = "auto", **kwargs) -> Dict[str, Any]:
    """
    Ottiene statistiche per una lega/campionato.
    
    Args:
        league_id: ID della lega/campionato
        source: Fonte di statistiche da utilizzare ("auto" per selezione automatica)
        **kwargs: Parametri aggiuntivi specifici della fonte
    
    Returns:
        Statistiche della lega
    """
    if source == "auto":
        source = get_best_source_for("league_stats")
    
    if source == "fbref":
        # FBref potrebbe avere un metodo specifico per le leghe
        return get_fbref_team_stats(league_id, **kwargs)
    elif source == "sofascore":
        # SofaScore potrebbe avere un metodo specifico per le leghe
        return get_sofascore_team_stats(league_id, **kwargs)
    elif source == "footystats":
        # FootyStats è particolarmente forte per le statistiche di lega
        return get_footystats_team_stats(league_id, **kwargs)
    else:
        raise ValueError(f"Fonte non supportata per league_stats: {source}")

@standardize_response
def get_supported_stats(source: str = None) -> Dict[str, Any]:
    """
    Ottiene le statistiche supportate da una fonte specifica o tutte le fonti.
    
    Args:
        source: Fonte specifica (opzionale)
    
    Returns:
        Dizionario delle statistiche supportate
    """
    if source:
        if source in STATS_SOURCES:
            return STATS_SOURCES[source]["capabilities"]
        else:
            raise ValueError(f"Fonte non supportata: {source}")
    else:
        return {source: info["capabilities"] for source, info in STATS_SOURCES.items()}

@standardize_response
def get_player_ratings(player_id: str, priority_sources: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Ottiene le valutazioni di un giocatore da più fonti.
    
    Args:
        player_id: ID del giocatore
        priority_sources: Lista opzionale di fonti in ordine di priorità
    
    Returns:
        Valutazioni del giocatore da diverse fonti
    """
    sources = priority_sources or SOURCE_PRIORITIES.get("player_ratings", ["sofascore", "whoscored"])
    
    ratings = {}
    for source in sources:
        try:
            if source == "sofascore":
                result = get_sofascore_player_ratings(player_id)
                if result:
                    ratings["sofascore"] = result
            elif source == "whoscored":
                result = get_whoscored_player_stats(player_id)
                if result and "ratings" in result:
                    ratings["whoscored"] = result["ratings"]
        except Exception as e:
            logger.error(f"Errore con fonte {source}: {str(e)}")
            continue
    
    return ratings

@standardize_response
def aggregate_team_stats(team_id: str, sources: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Aggrega le statistiche di una squadra da più fonti.
    
    Args:
        team_id: ID della squadra
        sources: Lista opzionale di fonti da utilizzare
    
    Returns:
        Statistiche aggregate da tutte le fonti
    """
    if sources is None:
        sources = [s for s, info in STATS_SOURCES.items() 
                  if info["capabilities"].get("team_stats", False)]
    
    aggregated = {
        "team_id": team_id,
        "sources_used": [],
        "stats": {}
    }
    
    for source in sources:
        try:
            stats = get_team_stats(team_id, source=source)
            if stats and stats.data:
                aggregated["stats"][source] = stats.data
                aggregated["sources_used"].append(source)
        except Exception as e:
            logger.error(f"Errore nell'aggregazione da {source}: {str(e)}")
            continue
    
    return aggregated

@standardize_response
def get_comprehensive_match_analysis(match_id: str) -> Dict[str, Any]:
    """
    Ottiene un'analisi completa di una partita da tutte le fonti disponibili.
    
    Args:
        match_id: ID della partita
    
    Returns:
        Analisi completa con dati da tutte le fonti
    """
    analysis = {
        "match_id": match_id,
        "timestamp": str(datetime.datetime.now().isoformat()),
        "sources_used": [],
        "data": {
            "basic_stats": {},
            "xg_data": get_match_xg(match_id).data,
            "advanced_metrics": {},
            "lineups": {},
            "incidents": {},
            "player_ratings": {}
        }
    }
    
    # Raccogli dati da tutte le fonti possibili
    for source, info in STATS_SOURCES.items():
        if info["capabilities"].get("match_stats", False):
            try:
                stats = get_match_stats(match_id, source=source)
                if stats and stats.data:
                    analysis["sources_used"].append(source)
                    
                    # Organizza i dati per categoria
                    if "info" in stats.data:
                        analysis["data"]["basic_stats"][source] = stats.data["info"]
                    
                    if "lineups" in stats.data:
                        analysis["data"]["lineups"][source] = stats.data["lineups"]
                    
                    if "incidents" in stats.data or "events" in stats.data:
                        incidents_key = "incidents" if "incidents" in stats.data else "events"
                        analysis["data"]["incidents"][source] = stats.data[incidents_key]
                    
                    if "player_stats" in stats.data:
                        analysis["data"]["player_ratings"][source] = stats.data["player_stats"]
                    
                    # Aggiungi statistiche avanzate
                    if source == "fbref" and "advanced_stats" in stats.data:
                        analysis["data"]["advanced_metrics"]["fbref"] = stats.data["advanced_stats"]
                    elif source == "understat" and "shots" in stats.data:
                        analysis["data"]["advanced_metrics"]["understat"] = stats.data["shots"]
                    
            except Exception as e:
                logger.error(f"Errore nell'analisi completa da {source}: {str(e)}")
                continue
    
    # Aggiungi riepilogo
    analysis["summary"] = {
        "total_sources": len(analysis["sources_used"]),
        "has_xg_data": bool(analysis["data"]["xg_data"]),
        "has_lineups": bool(analysis["data"]["lineups"]),
        "has_player_ratings": bool(analysis["data"]["player_ratings"])
    }
    
    return analysis

# Esporta tutte le funzioni principali
__all__ = [
    # Funzioni di accesso generale
    'get_team_stats',
    'get_player_stats',
    'get_match_stats',
    'get_team_xg',
    'get_player_xg',
    'get_match_xg',
    'get_league_stats',
    'get_player_ratings',
    'get_supported_stats',
    
    # Funzioni di aggregazione
    'aggregate_team_stats',
    'get_comprehensive_match_analysis',
    
    # Funzioni di utilità
    'get_scraper',
    'get_best_source_for',
    'get_available_stats_sources',
    'get_sources_by_capability',
    
    # Oggetti di supporto
    'StatsResponse',
    'StatsError',
    
    # Funzioni specifiche per fonte (retrocompatibilità)
    'get_fbref_team_stats',
    'get_understat_team_xg',
    'get_footystats_team_stats',
    'get_sofascore_team_stats',
    'get_whoscored_team_stats',
    'get_fbref_player_stats',
    'get_understat_player_xg',
    'get_sofascore_player_ratings',
    'get_whoscored_player_stats',
    'get_fbref_match_stats',
    'get_understat_match_xg',
    'get_footystats_match_stats',
    'get_sofascore_match_stats',
    'get_whoscored_match_stats'
]
