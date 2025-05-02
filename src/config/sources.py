""" Configurazione delle fonti dati per il sistema di pronostici calcistici.
Contiene informazioni sulle fonti dati utilizzate dal sistema, inclusi API,
servizi di scraping, e altre fonti di statistiche.
"""

import os
import json
import logging
from typing import Dict, List, Any, Optional, Union
import firebase_admin
from firebase_admin import db

from src.config.settings import initialize_firebase

# Logger
logger = logging.getLogger(__name__)

# Definizione delle priorità delle fonti dati (più basso = più affidabile)
SOURCE_PRIORITIES = {
    "football_data_api": 10,  # API ufficiale, più affidabile
    "rapidapi_football": 20,
    "fbref": 30,
    "understat": 40,
    "sofascore": 50,
    "open_football": 60,
    "soccerway": 70,
    "footystats": 80,
    "worldfootball": 90,
    "transfermarkt": 100
}

# Definizione delle fonti dati supportate
# Questa è la definizione di base, può essere sovrascritta da Firebase
SOURCES = {
    "football_data_api": {
        "name": "Football-Data.org API",
        "type": "api",
        "url": "https://www.football-data.org/",
        "requires_key": True,
        "rate_limit": {
            "requests_per_minute": 10,
            "requests_per_day": 100  # Tier gratuito
        },
        "available_data": [
            "matches",
            "teams",
            "standings",
            "players",
            "competitions"
        ],
        "documentation": "https://www.football-data.org/documentation/api",
        "active": True,
        "priority": 10,
        "notes": "API ufficiale con dati affidabili, ma con limiti nel tier gratuito"
    },
    "rapidapi_football": {
        "name": "API-Football (RapidAPI)",
        "type": "api",
        "url": "https://rapidapi.com/api-sports/api/api-football/",
        "requires_key": True,
        "rate_limit": {
            "requests_per_minute": 10,
            "requests_per_day": 100  # Tier gratuito
        },
        "available_data": [
            "matches",
            "teams",
            "standings",
            "players",
            "statistics",
            "odds",
            "injuries",
            "transfers"
        ],
        "documentation": "https://rapidapi.com/api-sports/api/api-football/",
        "active": True,
        "priority": 20,
        "notes": "API completa con dati estesi, ma con limiti nel tier gratuito"
    },
    "fbref": {
        "name": "FBref",
        "type": "scraper",
        "url": "https://fbref.com/",
        "requires_key": False,
        "rate_limit": {
            "requests_per_minute": 10,
            "delay_between_requests": 3  # secondi
        },
        "available_data": [
            "matches",
            "teams",
            "standings",
            "players",
            "statistics",
            "advanced_stats",
            "xg",
            "possession",
            "passing",
            "shooting"
        ],
        "documentation": "https://fbref.com/en/about/",
        "active": True,
        "priority": 30,
        "notes": "Ottima fonte per statistiche avanzate, richiede scraping rispettoso"
    },
    "understat": {
        "name": "Understat",
        "type": "scraper",
        "url": "https://understat.com/",
        "requires_key": False,
        "rate_limit": {
            "requests_per_minute": 5,
            "delay_between_requests": 5  # secondi
        },
        "available_data": [
            "matches",
            "teams",
            "players",
            "xg",
            "xg_timeline",
            "shot_map"
        ],
        "active": True,
        "priority": 40,
        "notes": "Specializzato in dati Expected Goals (xG)"
    },
    "sofascore": {
        "name": "SofaScore",
        "type": "scraper",
        "url": "https://www.sofascore.com/",
        "requires_key": False,
        "rate_limit": {
            "requests_per_minute": 5,
            "delay_between_requests": 5  # secondi
        },
        "available_data": [
            "matches",
            "teams",
            "standings",
            "players",
            "statistics",
            "lineups",
            "events",
            "live_scores"
        ],
        "active": True,
        "priority": 50,
        "notes": "Dati aggiornati in tempo reale, interfaccia dinamica che richiede tecniche avanzate di scraping"
    },
    "open_football": {
        "name": "OpenFootball",
        "type": "open_data",
        "url": "https://github.com/openfootball",
        "requires_key": False,
        "data_format": ["CSV", "TXT", "JSON"],
        "update_frequency": "variabile",
        "available_data": [
            "matches",
            "teams",
            "standings",
            "players",
            "competitions"
        ],
        "documentation": "https://github.com/openfootball/docs",
        "active": True,
        "priority": 60,
        "notes": "Repository open source con dati strutturati, aggiornamenti manuali"
    },
    "soccerway": {
        "name": "Soccerway",
        "type": "scraper",
        "url": "https://uk.soccerway.com/",
        "requires_key": False,
        "rate_limit": {
            "requests_per_minute": 5,
            "delay_between_requests": 5  # secondi
        },
        "available_data": [
            "matches",
            "teams",
            "standings",
            "players",
            "head_to_head",
            "venues"
        ],
        "active": True,
        "priority": 70,
        "notes": "Buona copertura globale, formato di dati consistente"
    },
    "footystats": {
        "name": "FootyStats",
        "type": "scraper",
        "url": "https://footystats.org/",
        "requires_key": False,
        "rate_limit": {
            "requests_per_minute": 3,
            "delay_between_requests": 10  # secondi
        },
        "available_data": [
            "matches",
            "teams",
            "standings",
            "players",
            "statistics",
            "predictions",
            "trends"
        ],
        "active": True,
        "priority": 80,
        "notes": "Buone statistiche e previsioni, ma controllo anti-scraping rigido"
    },
    "worldfootball": {
        "name": "WorldFootball.net",
        "type": "scraper",
        "url": "https://www.worldfootball.net/",
        "requires_key": False,
        "rate_limit": {
            "requests_per_minute": 5,
            "delay_between_requests": 5  # secondi
        },
        "available_data": [
            "matches",
            "teams",
            "standings",
            "players",
            "historical_data",
            "venues"
        ],
        "active": True,
        "priority": 90,
        "notes": "Ottima copertura storica, anche per campionati minori"
    },
    "transfermarkt": {
        "name": "Transfermarkt",
        "type": "scraper",
        "url": "https://www.transfermarkt.com/",
        "requires_key": False,
        "rate_limit": {
            "requests_per_minute": 2,
            "delay_between_requests": 15  # secondi
        },
        "available_data": [
            "teams",
            "players",
            "transfers",
            "market_values",
            "injuries",
            "contracts"
        ],
        "active": True,
        "priority": 100,
        "notes": "Specializzato in trasferimenti e valori di mercato, scraping molto delicato"
    },
    "kaggle_datasets": {
        "name": "Kaggle Football Datasets",
        "type": "open_data",
        "url": "https://www.kaggle.com/datasets?search=football",
        "requires_key": False,
        "data_format": ["CSV", "JSON", "SQLite"],
        "update_frequency": "variabile",
        "available_data": [
            "matches",
            "teams",
            "players",
            "statistics",
            "historical_data"
        ],
        "active": True,
        "priority": 110,
        "notes": "Dataset pronti all'uso, ma spesso non aggiornati regolarmente"
    },
    "rsssf": {
        "name": "RSSSF (Rec.Sport.Soccer Statistics Foundation)",
        "type": "open_data",
        "url": "http://www.rsssf.com/",
        "requires_key": False,
        "data_format": ["HTML", "TXT"],
        "update_frequency": "settimanale/mensile",
        "available_data": [
            "matches",
            "standings",
            "historical_data",
            "competitions",
            "national_teams"
        ],
        "active": True,
        "priority": 120,
        "notes": "Fonte storica eccellente, formato non strutturato che richiede elaborazione"
    },
    "statsbomb": {
        "name": "StatsBomb Open Data",
        "type": "open_data",
        "url": "https://github.com/statsbomb/open-data",
        "requires_key": False,
        "data_format": ["JSON"],
        "update_frequency": "irregolare",
        "available_data": [
            "matches",
            "events",
            "lineups",
            "advanced_stats"
        ],
        "active": True,
        "priority": 130,
        "notes": "Dati di eventi avanzati, disponibili solo per alcune competizioni/stagioni"
    },
    "eleven_v_eleven": {
        "name": "11v11",
        "type": "scraper",
        "url": "https://www.11v11.com/",
        "requires_key": False,
        "rate_limit": {
            "requests_per_minute": 5,
            "delay_between_requests": 5  # secondi
        },
        "available_data": [
            "matches",
            "teams",
            "players",
            "historical_data",
            "head_to_head"
        ],
        "active": True,
        "priority": 140,
        "notes": "Ottima fonte storica, specialmente per dati inglesi"
    },
    "wikipedia": {
        "name": "Wikipedia",
        "type": "scraper",
        "url": "https://en.wikipedia.org/",
        "requires_key": False,
        "rate_limit": {
            "requests_per_minute": 5,
            "delay_between_requests": 5  # secondi
        },
        "available_data": [
            "teams",
            "players",
            "competitions",
            "seasons",
            "historical_data"
        ],
        "active": True,
        "priority": 150,
        "notes": "Fonte generica di informazioni, formato variabile che richiede adattamento"
    }
}

# Mappa dei nomi alle fonti per facilitare la ricerca per nome
SOURCE_NAME_MAP = {
    # Nomi ufficiali
    "Football-Data.org API": "football_data_api",
    "API-Football": "rapidapi_football",
    "FBref": "fbref",
    "Understat": "understat",
    "SofaScore": "sofascore",
    "OpenFootball": "open_football",
    "Soccerway": "soccerway",
    "FootyStats": "footystats",
    "WorldFootball.net": "worldfootball",
    "Transfermarkt": "transfermarkt",
    "Kaggle Football Datasets": "kaggle_datasets",
    "RSSSF": "rsssf",
    "StatsBomb Open Data": "statsbomb",
    "11v11": "eleven_v_eleven",
    "Wikipedia": "wikipedia",
    
    # Nomi alternativi
    "Football-Data": "football_data_api",
    "Football Data": "football_data_api",
    "API Football": "rapidapi_football",
    "RapidAPI Football": "rapidapi_football",
    "Sofa Score": "sofascore",
    "Open Football": "open_football",
    "Footy Stats": "footystats",
    "World Football": "worldfootball",
    "Transfer Markt": "transfermarkt",
    "Kaggle": "kaggle_datasets",
    "StatsBomb": "statsbomb",
    "Eleven v Eleven": "eleven_v_eleven",
    "Wiki": "wikipedia"
}

# Configurazione per ciascun tipo di dato
DATA_TYPE_CONFIG = {
    "matches": {
        "primary_sources": ["football_data_api", "rapidapi_football"],
        "fallback_sources": ["fbref", "sofascore", "soccerway"],
        "cache_ttl": 1800,  # 30 minuti
        "update_frequency": "daily"
    },
    "teams": {
        "primary_sources": ["football_data_api", "rapidapi_football", "fbref"],
        "fallback_sources": ["sofascore", "transfermarkt", "soccerway"],
        "cache_ttl": 86400,  # 1 giorno
        "update_frequency": "weekly"
    },
    "players": {
        "primary_sources": ["rapidapi_football", "fbref", "transfermarkt"],
        "fallback_sources": ["sofascore", "worldfootball"],
        "cache_ttl": 86400 * 3,  # 3 giorni
        "update_frequency": "weekly"
    },
    "standings": {
        "primary_sources": ["football_data_api", "rapidapi_football"],
        "fallback_sources": ["fbref", "sofascore", "soccerway"],
        "cache_ttl": 3600,  # 1 ora
        "update_frequency": "daily"
    },
    "statistics": {
        "primary_sources": ["fbref", "understat", "rapidapi_football"],
        "fallback_sources": ["sofascore", "footystats"],
        "cache_ttl": 43200,  # 12 ore
        "update_frequency": "daily"
    },
    "odds": {
        "primary_sources": ["rapidapi_football"],
        "fallback_sources": ["footystats"],
        "cache_ttl": 1800,  # 30 minuti
        "update_frequency": "hourly"
    },
    "xg": {
        "primary_sources": ["understat", "fbref"],
        "fallback_sources": ["footystats", "sofascore"],
        "cache_ttl": 43200,  # 12 ore
        "update_frequency": "daily"
    },
    "historical_data": {
        "primary_sources": ["worldfootball", "rsssf", "eleven_v_eleven"],
        "fallback_sources": ["wikipedia", "kaggle_datasets"],
        "cache_ttl": 2592000,  # 30 giorni
        "update_frequency": "monthly"
    }
}

# Funzione per ottenere le fonti dati da Firebase
def get_sources_from_firebase() -> Dict[str, Any]:
    """
    Ottiene le fonti dati da Firebase.
    
    Returns:
        Dizionario con le fonti dati
    """
    try:
        initialize_firebase()
        sources_ref = db.reference('config/sources')
        sources_data = sources_ref.get()
        
        if sources_data:
            return sources_data
        return {}
    except Exception as e:
        logger.warning(f"Impossibile ottenere le fonti dati da Firebase: {e}")
        return {}

# Funzione per salvare le fonti dati su Firebase
def save_sources_to_firebase(sources_data: Dict[str, Any]) -> bool:
    """
    Salva le fonti dati su Firebase.
    
    Args:
        sources_data: Dizionario con le fonti dati
        
    Returns:
        True se l'operazione è riuscita, False altrimenti
    """
    try:
        initialize_firebase()
        sources_ref = db.reference('config/sources')
        sources_ref.set(sources_data)
        logger.info("Fonti dati salvate su Firebase con successo")
        return True
    except Exception as e:
        logger.error(f"Impossibile salvare le fonti dati su Firebase: {e}")
        return False

# Funzione per ottenere le fonti dati attive
def get_active_sources() -> Dict[str, Dict[str, Any]]:
    """
    Ottiene le fonti dati attive, combinando dati locali e Firebase.
    
    Returns:
        Dizionario con le fonti dati attive
    """
    # Ottieni dati da Firebase
    firebase_sources = get_sources_from_firebase()
    
    # Combina con dati locali
    combined_sources = SOURCES.copy()
    
    # Aggiorna con dati da Firebase
    for source_id, source_data in firebase_sources.items():
        if source_id in combined_sources:
            # Aggiorna campi esistenti
            combined_sources[source_id].update(source_data)
        else:
            # Aggiungi nuova fonte
            combined_sources[source_id] = source_data
    
    # Filtra per fonti attive
    active_sources = {
        source_id: source_data 
        for source_id, source_data in combined_sources.items() 
        if source_data.get('active', True)
    }
    
    return active_sources

# Funzione per ottenere una fonte dati specifica
def get_source(source_id: str) -> Optional[Dict[str, Any]]:
    """
    Ottiene i dati di una fonte specifica.
    
    Args:
        source_id: ID della fonte
        
    Returns:
        Dati della fonte o None se non trovata
    """
    # Ottieni tutte le fonti
    all_sources = get_active_sources()
    
    # Restituisci la fonte richiesta se esiste
    return all_sources.get(source_id)

# Funzione per ottenere una fonte dati tramite nome
def get_source_by_name(name: str) -> Optional[Dict[str, Any]]:
    """
    Ottiene informazioni su una fonte dati dal nome.
    
    Args:
        name: Nome della fonte (ufficiale o alternativo)
        
    Returns:
        Dizionario con informazioni sulla fonte o None se non trovata
    """
    # Normalizzazione del nome (rimuove spazi extra e converte in lowercase)
    name = name.strip().lower()
    
    # Verifica diretta nel dizionario dei nomi
    for source_name, source_key in SOURCE_NAME_MAP.items():
        if name == source_name.lower():
            return get_source(source_key)
    
    # Prova corrispondenze parziali
    for source_name, source_key in SOURCE_NAME_MAP.items():
        if name in source_name.lower() or source_name.lower() in name:
            return get_source(source_key)
    
    # Cerca nei dati delle fonti
    active_sources = get_active_sources()
    for source_key, source_data in active_sources.items():
        source_name = source_data.get("name", "").lower()
        
        if (name in source_name or source_name in name or 
            name in source_key.lower()):
            return source_data
    
    # Nessuna corrispondenza trovata
    logger.warning(f"Fonte non trovata con nome: {name}")
    return None

# Funzione per ottenere fonti dati ordinate per priorità
def get_sources_by_priority() -> List[Dict[str, Any]]:
    """
    Ottiene le fonti dati ordinate per priorità.
    
    Returns:
        Lista di fonti dati ordinate per priorità
    """
    active_sources = get_active_sources()
    
    # Converti in lista per ordinamento
    sources_list = [
        {"id": source_id, **source_data}
        for source_id, source_data in active_sources.items()
    ]
    
    # Ordina per priorità (più bassa prima)
    sources_list.sort(key=lambda x: x.get('priority', 999))
    
    return sources_list

# Funzione per ottenere le fonti dati per un tipo di dato
def get_sources_for_data_type(data_type: str) -> Dict[str, List[str]]:
    """
    Ottiene le fonti dati appropriate per un tipo di dato specifico.
    
    Args:
        data_type: Tipo di dato (matches, teams, players, etc.)
        
    Returns:
        Dizionario con fonti primarie e di fallback
    """
    # Verifica se il tipo di dato è supportato
    if data_type not in DATA_TYPE_CONFIG:
        logger.warning(f"Tipo di dato non supportato: {data_type}")
        return {
            "primary": [],
            "fallback": []
        }
    
    # Ottieni configurazione
    config = DATA_TYPE_CONFIG[data_type]
    
    # Ottieni fonti attive
    active_sources = get_active_sources()
    
    # Filtra per fonti attive
    primary_sources = [
        source_id for source_id in config.get('primary_sources', [])
        if source_id in active_sources
    ]
    
    fallback_sources = [
        source_id for source_id in config.get('fallback_sources', [])
        if source_id in active_sources
    ]
    
    return {
        "primary": primary_sources,
        "fallback": fallback_sources
    }

# Funzione per ottenere il TTL della cache per un tipo di dato
def get_cache_ttl_for_data_type(data_type: str) -> int:
    """
    Ottiene il TTL della cache per un tipo di dato specifico.
    
    Args:
        data_type: Tipo di dato (matches, teams, players, etc.)
        
    Returns:
        TTL in secondi
    """
    # Verifica se il tipo di dato è supportato
    if data_type not in DATA_TYPE_CONFIG:
        logger.warning(f"Tipo di dato non supportato: {data_type}")
        return 3600  # Default: 1 ora
    
    # Ottieni configurazione
    config = DATA_TYPE_CONFIG[data_type]
    
    return config.get('cache_ttl', 3600)

# Inizializza le fonti dati su Firebase se necessario
def initialize_sources():
    """Inizializza le fonti dati su Firebase se non esistono."""
    try:
        initialize_firebase()
        sources_ref = db.reference('config/sources')
        
        # Verifica se i dati esistono già
        existing_data = sources_ref.get()
        
        if not existing_data:
            # Salva i dati iniziali
            save_sources_to_firebase(SOURCES)
            logger.info("Fonti dati inizializzate su Firebase")
            
            # Salva anche la configurazione dei tipi di dato
            data_types_ref = db.reference('config/data_types')
            data_types_ref.set(DATA_TYPE_CONFIG)
            logger.info("Configurazione tipi di dato inizializzata su Firebase")
    except Exception as e:
        logger.warning(f"Impossibile inizializzare le fonti dati su Firebase: {e}")

# Inizializza il modulo quando viene importato
try:
    # Tentativo di inizializzazione delle fonti dati
    initialize_sources()
except Exception as e:
    logger.warning(f"Impossibile inizializzare le fonti dati su Firebase: {e}")
