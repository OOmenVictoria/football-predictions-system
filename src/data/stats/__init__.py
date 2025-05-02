"""
Pacchetto per l'estrazione di statistiche calcistiche avanzate.
Questo pacchetto fornisce moduli per estrarre statistiche avanzate da vari siti specializzati,
inclusi dati di Expected Goals (xG), metriche di prestazione e valutazioni.
"""

from typing import Dict, List, Any, Optional, Union

# Importa le funzioni principali dai vari moduli
from src.data.stats.fbref import get_team_stats as get_fbref_team_stats
from src.data.stats.fbref import get_player_stats as get_fbref_player_stats
from src.data.stats.fbref import get_match_stats as get_fbref_match_stats

from src.data.stats.understat import get_team_xg as get_understat_team_xg
from src.data.stats.understat import get_player_xg as get_understat_player_xg
from src.data.stats.understat import get_match_xg as get_understat_match_xg

from src.data.stats.footystats import get_team_stats as get_footystats_team_stats
from src.data.stats.footystats import get_match_stats as get_footystats_match_stats

from src.data.stats.sofascore import get_team_stats as get_sofascore_team_stats
from src.data.stats.sofascore import get_player_ratings as get_sofascore_player_ratings
from src.data.stats.sofascore import get_match_stats as get_sofascore_match_stats

from src.data.stats.whoscored import get_team_stats as get_whoscored_team_stats
from src.data.stats.whoscored import get_player_stats as get_whoscored_player_stats
from src.data.stats.whoscored import get_match_stats as get_whoscored_match_stats

# Lista delle fonti di statistiche disponibili
STATS_SOURCES = {
    "fbref": {
        "name": "FBref",
        "description": "Statistiche avanzate da Sports Reference",
        "url": "https://fbref.com/"
    },
    "understat": {
        "name": "Understat",
        "description": "Dati Expected Goals (xG) dettagliati",
        "url": "https://understat.com/"
    },
    "footystats": {
        "name": "FootyStats",
        "description": "Statistiche dettagliate per analisi e scommesse",
        "url": "https://footystats.org/"
    },
    "sofascore": {
        "name": "SofaScore",
        "description": "Statistiche e valutazioni giocatori in tempo reale",
        "url": "https://www.sofascore.com/"
    },
    "whoscored": {
        "name": "WhoScored-like",
        "description": "Metriche di prestazione avanzate e valutazioni",
        "url": "https://www.whoscored.com/"
    }
}

def get_available_stats_sources() -> Dict[str, Dict[str, str]]:
    """
    Ottiene la lista delle fonti di statistiche disponibili.
    
    Returns:
        Dizionario delle fonti disponibili
    """
    return STATS_SOURCES

def get_team_stats(team_id: str, source: str = "fbref") -> Dict[str, Any]:
    """
    Ottiene statistiche di una squadra dalla fonte specificata.
    
    Args:
        team_id: ID della squadra
        source: Fonte di statistiche da utilizzare
        
    Returns:
        Statistiche della squadra
    """
    if source == "fbref":
        return get_fbref_team_stats(team_id)
    elif source == "understat":
        return get_understat_team_xg(team_id)
    elif source == "footystats":
        return get_footystats_team_stats(team_id)
    elif source == "sofascore":
        return get_sofascore_team_stats(team_id)
    elif source == "whoscored":
        return get_whoscored_team_stats(team_id)
    else:
        raise ValueError(f"Fonte di statistiche non supportata: {source}")

def get_match_stats(match_id: str, source: str = "fbref") -> Dict[str, Any]:
    """
    Ottiene statistiche di una partita dalla fonte specificata.
    
    Args:
        match_id: ID della partita
        source: Fonte di statistiche da utilizzare
        
    Returns:
        Statistiche della partita
    """
    if source == "fbref":
        return get_fbref_match_stats(match_id)
    elif source == "understat":
        return get_understat_match_xg(match_id)
    elif source == "footystats":
        return get_footystats_match_stats(match_id)
    elif source == "sofascore":
        return get_sofascore_match_stats(match_id)
    elif source == "whoscored":
        return get_whoscored_match_stats(match_id)
    else:
        raise ValueError(f"Fonte di statistiche non supportata: {source}")

def get_match_xg(match_id: str) -> Dict[str, float]:
    """
    Ottiene i dati Expected Goals (xG) per una partita,
    con fallback tra le varie fonti.
    
    Args:
        match_id: ID della partita
        
    Returns:
        Dati xG della partita (home_xg, away_xg)
    """
    # Prova prima con Understat (fonte migliore per xG)
    try:
        xg_data = get_understat_match_xg(match_id)
        if xg_data and "home_xg" in xg_data and "away_xg" in xg_data:
            return {
                "home_xg": xg_data["home_xg"],
                "away_xg": xg_data["away_xg"],
                "source": "understat"
            }
    except Exception:
        pass
    
    # Fallback a FBref
    try:
        xg_data = get_fbref_match_stats(match_id)
        if xg_data and "xg" in xg_data:
            return {
                "home_xg": xg_data["xg"]["home"],
                "away_xg": xg_data["xg"]["away"],
                "source": "fbref"
            }
    except Exception:
        pass
    
    # Ultimo fallback a altre fonti
    for source in ["sofascore", "whoscored", "footystats"]:
        try:
            if source == "sofascore":
                stats = get_sofascore_match_stats(match_id)
            elif source == "whoscored":
                stats = get_whoscored_match_stats(match_id)
            elif source == "footystats":
                stats = get_footystats_match_stats(match_id)
            
            if stats and "xg" in stats:
                return {
                    "home_xg": stats["xg"]["home"],
                    "away_xg": stats["xg"]["away"],
                    "source": source
                }
        except Exception:
            continue
    
    # Se non troviamo nulla, restituisci valori di default
    return {
        "home_xg": 0.0,
        "away_xg": 0.0,
        "source": "none"
    }
