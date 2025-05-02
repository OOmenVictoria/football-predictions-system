"""
Package per le utilities condivise del sistema.
Questo package fornisce moduli di utilità generale utilizzati da vari
componenti del sistema di pronostici calcistici.
"""

from src.utils.database import FirebaseManager
from src.utils.http import make_request, download_file
from src.utils.cache import cached, clear_cache
from src.utils.time_utils import format_date, parse_date, get_time_until
from src.utils.text_utils import clean_text, normalize_team_name

def initialize_utils():
    """
    Inizializza i componenti di utilità del sistema.
    
    Returns:
        Stato dell'inizializzazione
    """
    # Verifica la connessione al database
    db = FirebaseManager()
    db_test = db.test_connection()
    
    # Inizializza il sistema di cache
    cache_status = clear_cache(expired_only=True)
    
    return {
        "database": "connected" if db_test else "error",
        "cache": cache_status
    }
