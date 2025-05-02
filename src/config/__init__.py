"""
Package per le configurazioni del sistema.
Questo package fornisce moduli per la gestione delle impostazioni globali,
configurazione dei campionati e delle fonti dati.
"""

from src.config.settings import get_setting, set_setting, reload_settings
from src.config.leagues import get_league, get_active_leagues, get_league_by_name
from src.config.sources import get_source, get_active_sources, get_source_by_name

def initialize_config():
    """
    Inizializza le configurazioni del sistema.
    
    Returns:
        Stato dell'inizializzazione delle configurazioni
    """
    reload_settings()
    return {"status": "initialized"}
