"""
Package per le utilities condivise del sistema.
Questo package fornisce moduli di utilità generale utilizzati da vari
componenti del sistema di pronostici calcistici.
"""

# Import delle funzioni di utilità
from src.utils.time_utils import (format_date, parse_date, get_time_until, 
                                 get_datetime_now, get_current_datetime)
from src.utils.text_utils import clean_text, normalize_team_name
from src.utils.http import make_request, download_file
from src.utils.cache import cached, clear_cache

# Firebase viene importato solo quando necessario per evitare circolarità
_firebase_initialized = False

def initialize_utils():
    """
    Inizializza i componenti di utilità del sistema.
    
    Returns:
        dict: Stato dell'inizializzazione
    """
    global _firebase_initialized
    
    # Inizializzazione Firebase (solo quando necessario)
    if not _firebase_initialized:
        from src.utils.database import FirebaseManager
        db = FirebaseManager()
        db_test = db.test_connection()
        _firebase_initialized = True
    else:
        db_test = True
    
    # Inizializza il sistema di cache
    cache_status = clear_cache(expired_only=True)
    
    return {
        "database": "connected" if db_test else "error",
        "cache": cache_status
    }

# Alias per compatibilità
get_current_datetime = get_datetime_now
