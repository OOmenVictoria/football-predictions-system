"""
Package per le utilities condivise del sistema.
Questo package fornisce moduli di utilità generale utilizzati da vari
componenti del sistema di pronostici calcistici.

Contiene:
- Funzioni per la gestione di date e orari
- Utility per chiamate HTTP e download
- Gestione della cache
- Funzioni per la pulizia del testo
- Gestione del database Firebase
"""

import os
from typing import Dict, Any
from firebase_admin import db

# Import delle funzioni di utilità
from .time_utils import (
    format_date,
    parse_date,
    get_time_until,
    get_datetime_now,
    get_current_datetime,
    timestamp_to_datetime,
    datetime_to_timestamp,
    format_timeago
)
from .text_utils import (
    clean_text,
    normalize_team_name,
    slugify,
    truncate_text
)
from .http import (
    make_request,
    download_file,
    post_json,
    get_with_retry
)
from .cache import (
    cached,
    clear_cache,
    get_cache_size,
    purge_old_cache
)
from .exceptions import (
    DataCollectionError,
    DatabaseConnectionError,
    InvalidConfigurationError
)

# Inizializzazione Firebase
_firebase_initialized = False
_firebase_manager = None

def initialize_utils(config: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Inizializza i componenti di utilità del sistema.
    
    Args:
        config: Dizionario di configurazione con:
            - firebase_credentials: Path al file credentials
            - database_url: URL del database Firebase
            - cache_max_size: Dimensione massima cache in MB
    
    Returns:
        dict: Stato dell'inizializzazione con:
            - database: Stato connessione Firebase
            - cache: Stato inizializzazione cache
            - version: Versione del package
    """
    global _firebase_initialized, _firebase_manager
    
    status = {
        "database": "not_initialized",
        "cache": "not_initialized",
        "version": "1.2.0"
    }
    
    # Inizializzazione Firebase
    try:
        if not _firebase_initialized:
            from .database import FirebaseManager
            
            cred_path = (
                config.get('firebase_credentials') 
                if config 
                else os.getenv('FIREBASE_CREDENTIALS')
            )
            
            db_url = (
                config.get('database_url') 
                if config 
                else os.getenv('FIREBASE_DB_URL')
            )
            
            _firebase_manager = FirebaseManager(cred_path, db_url)
            _firebase_initialized = True
            
            # Test connessione
            ref = db.reference('/status')
            ref.set({'health': 'ok'})
            status["database"] = "connected"
    
    except Exception as e:
        status["database"] = f"error: {str(e)}"
    
    # Inizializzazione Cache
    try:
        max_size = (
            config.get('cache_max_size') 
            if config 
            else os.getenv('CACHE_MAX_SIZE', 100)
        )
        
        clear_cache(expired_only=True)
        status["cache"] = f"initialized (max {max_size}MB)"
    
    except Exception as e:
        status["cache"] = f"error: {str(e)}"
    
    return status

def get_firebase_manager():
    """
    Restituisce l'istanza del manager Firebase.
    
    Returns:
        FirebaseManager: Istanza configurata
    Raises:
        DatabaseConnectionError: Se Firebase non è inizializzato
    """
    if not _firebase_initialized:
        raise DatabaseConnectionError("Firebase non inizializzato")
    return _firebase_manager

# Esporta le funzioni principali
__all__ = [
    # time_utils
    'format_date',
    'parse_date',
    'get_time_until',
    'get_datetime_now',
    'get_current_datetime',
    'timestamp_to_datetime',
    'datetime_to_timestamp',
    'format_timeago',
    
    # text_utils
    'clean_text',
    'normalize_team_name',
    'slugify',
    'truncate_text',
    
    # http
    'make_request',
    'download_file',
    'post_json',
    'get_with_retry',
    
    # cache
    'cached',
    'clear_cache',
    'get_cache_size',
    'purge_old_cache',
    
    # database
    'initialize_utils',
    'get_firebase_manager',
    
    # exceptions
    'DataCollectionError',
    'DatabaseConnectionError',
    'InvalidConfigurationError'
]

# Alias per compatibilità
get_current_datetime = get_datetime_now
