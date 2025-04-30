""" Impostazioni globali per il sistema di pronostici calcistici.
Contiene configurazioni, costanti, e opzioni che vengono utilizzate
in tutto il sistema. Supporta caricamento da variabili d'ambiente,
file di configurazione locale, e Firebase.
"""

import os
import json
import logging
from typing import Dict, List, Any, Optional, Union
from datetime import timedelta
import firebase_admin
from firebase_admin import credentials, db
from dotenv import load_dotenv

# Carica variabili d'ambiente da file .env se presente
load_dotenv()

# Percorso root del repository
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Percorsi directory
LOGS_DIR = os.path.join(ROOT_DIR, 'logs')
CACHE_DIR = os.path.join(ROOT_DIR, 'cache')
BACKUP_DIR = os.path.join(ROOT_DIR, 'backup')

# Crea directory se non esistono
for directory in [LOGS_DIR, CACHE_DIR, BACKUP_DIR]:
    os.makedirs(directory, exist_ok=True)

# Modalità debug
DEBUG = os.getenv('DEBUG', 'False').lower() in ('true', '1', 't')

# Configurazione logging
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
LOG_FILE = os.path.join(LOGS_DIR, 'football_predictions.log')

# Configurazione Firebase
FIREBASE_CREDENTIALS = os.getenv('FIREBASE_CREDENTIALS', os.path.join(ROOT_DIR, 'firebase_credentials.json'))
FIREBASE_DATABASE_URL = os.getenv('FIREBASE_DB_URL', '')

# Per inizializzare Firebase solo se necessario
_firebase_initialized = False

def initialize_firebase():
    """Inizializza Firebase se non è già inizializzato."""
    global _firebase_initialized
    
    if _firebase_initialized:
        return
    
    try:
        # Se già inizializzato in altra parte del codice
        if firebase_admin._apps:
            _firebase_initialized = True
            return
        
        # Verifica che il file di credenziali esista
        if not os.path.exists(FIREBASE_CREDENTIALS):
            raise FileNotFoundError(f"File credenziali Firebase non trovato: {FIREBASE_CREDENTIALS}")
        
        # Inizializza app Firebase
        cred = credentials.Certificate(FIREBASE_CREDENTIALS)
        firebase_admin.initialize_app(cred, {
            'databaseURL': FIREBASE_DATABASE_URL
        })
        
        _firebase_initialized = True
        
    except Exception as e:
        logging.error(f"Errore nell'inizializzazione di Firebase: {e}")
        raise

# Configurazione WordPress
WORDPRESS_URL = os.getenv('WP_URL', '')
WORDPRESS_USERNAME = os.getenv('WP_USER', '')
WORDPRESS_APP_PASSWORD = os.getenv('WP_APP_PASSWORD', '')

# Configurazione API
FOOTBALL_DATA_API_KEY = os.getenv('FOOTBALL_API_KEY', '')
RAPIDAPI_KEY = os.getenv('RAPIDAPI_KEY', '')

# Configurazione cache
CACHE_TTL = {
    'default': int(os.getenv('DEFAULT_CACHE_TTL', 3600)),  # 1 ora
    'matches': int(os.getenv('MATCHES_CACHE_TTL', 1800)),  # 30 minuti
    'team_stats': int(os.getenv('TEAM_STATS_CACHE_TTL', 86400)),  # 1 giorno
    'player_stats': int(os.getenv('PLAYER_STATS_CACHE_TTL', 86400 * 3)),  # 3 giorni
    'historical': int(os.getenv('HISTORICAL_CACHE_TTL', 86400 * 7))  # 7 giorni
}

# Configurazione scraper
SCRAPER_CONFIG = {
    'user_agents': [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36'
    ],
    'request_timeout': int(os.getenv('REQUEST_TIMEOUT', 30)),  # secondi
    'max_retries': int(os.getenv('MAX_RETRIES', 3)),
    'retry_delay': int(os.getenv('RETRY_DELAY', 5)),  # secondi
    'respect_robots_txt': os.getenv('RESPECT_ROBOTS_TXT', 'True').lower() in ('true', '1', 't'),
    'rate_limit': {
        'default': float(os.getenv('DEFAULT_RATE_LIMIT', 1.0)),  # richieste al secondo
        'fbref': float(os.getenv('FBREF_RATE_LIMIT', 0.5)),
        'understat': float(os.getenv('UNDERSTAT_RATE_LIMIT', 0.5)),
        'sofascore': float(os.getenv('SOFASCORE_RATE_LIMIT', 0.33)),
        'wordpress': float(os.getenv('WORDPRESS_RATE_LIMIT', 0.5))
    }
}

# Configurazione pubblicazione
PUBLISHING_CONFIG = {
    'article_ttl': timedelta(hours=int(os.getenv('ARTICLE_TTL_HOURS', 8))),  # 8 ore dopo la partita
    'publish_before': timedelta(hours=int(os.getenv('PUBLISH_BEFORE_HOURS', 12))),  # 12 ore prima della partita
    'max_articles_per_day': int(os.getenv('MAX_ARTICLES_PER_DAY', 100)),
    'max_articles_per_hour': int(os.getenv('MAX_ARTICLES_PER_HOUR', 15)),
    'batch_size': int(os.getenv('PUBLISH_BATCH_SIZE', 5))
}

# Configurazione predizione
PREDICTION_CONFIG = {
    'min_matches_for_prediction': int(os.getenv('MIN_MATCHES_FOR_PREDICTION', 3)),
    'rating_weight': float(os.getenv('RATING_WEIGHT', 0.4)),
    'form_weight': float(os.getenv('FORM_WEIGHT', 0.3)),
    'h2h_weight': float(os.getenv('H2H_WEIGHT', 0.2)),
    'odds_weight': float(os.getenv('ODDS_WEIGHT', 0.1))
}

# Configurazione monitoraggio
MONITORING_CONFIG = {
    'health_check_interval': int(os.getenv('HEALTH_CHECK_INTERVAL', 30)),  # minuti
    'backup_schedule': {
        'daily': os.getenv('DAILY_BACKUP_TIME', '02:00'),
        'weekly': os.getenv('WEEKLY_BACKUP_DAY', '1'),  # Lunedì
        'monthly': os.getenv('MONTHLY_BACKUP_DAY', '1')  # Primo del mese
    },
    'error_notification_threshold': int(os.getenv('ERROR_NOTIFICATION_THRESHOLD', 5)),
    'retain_logs_days': int(os.getenv('RETAIN_LOGS_DAYS', 30))
}

# Configurazione traduzione
TRANSLATION_CONFIG = {
    'default_source_language': os.getenv('DEFAULT_SOURCE_LANGUAGE', 'en'),
    'target_languages': os.getenv('TARGET_LANGUAGES', 'it').split(','),
    'service': os.getenv('TRANSLATION_SERVICE', 'libre_translate'),
    'libre_translate_url': os.getenv('LIBRE_TRANSLATE_URL', 'https://libretranslate.de'),
    'libre_translate_api_key': os.getenv('LIBRE_TRANSLATE_API_KEY', '')
}

class FirebaseSettings:
    """Gestisce le impostazioni su Firebase, con fallback su quelle locali."""
    
    def __init__(self):
        """Inizializza il gestore delle impostazioni."""
        self._initialized = False
        self._settings_ref = None
        self._cache = {}
        self._last_update = 0
    
    def _init_if_needed(self):
        """Inizializza la connessione a Firebase se necessario."""
        if not self._initialized:
            try:
                initialize_firebase()
                self._settings_ref = db.reference('config/settings')
                self._initialized = True
            except Exception as e:
                logging.warning(f"Impossibile inizializzare FirebaseSettings: {e}")
                self._initialized = False
    
    def _refresh_cache(self, force=False):
        """Aggiorna la cache delle impostazioni da Firebase."""
        import time
        
        self._init_if_needed()
        
        # Se non possiamo usare Firebase, non fare nulla
        if not self._initialized:
            return
        
        # Aggiorna cache solo se sono passati più di 60 secondi dall'ultimo aggiornamento o force=True
        current_time = time.time()
        if force or (current_time - self._last_update) > 60:
            try:
                settings = self._settings_ref.get()
                if settings:
                    self._cache = settings
                self._last_update = current_time
            except Exception as e:
                logging.warning(f"Errore nell'aggiornamento della cache delle impostazioni: {e}")
    
    def get(self, key: str, default=None):
        """
        Ottiene un'impostazione da Firebase, con fallback sulle impostazioni locali.
        
        Args:
            key: Chiave dell'impostazione
            default: Valore di default se l'impostazione non è trovata
        
        Returns:
            Valore dell'impostazione
        """
        # Prova a ottenere da Firebase
        try:
            self._refresh_cache()
            if key in self._cache:
                return self._cache[key]
        except:
            pass
        
        # Fallback alle impostazioni locali
        if hasattr(globals(), key.upper()):
            return globals()[key.upper()]
        
        return default
    
    def set(self, key: str, value) -> bool:
        """
        Imposta un'impostazione su Firebase.
        
        Args:
            key: Chiave dell'impostazione
            value: Valore dell'impostazione
        
        Returns:
            True se l'operazione è riuscita, False altrimenti
        """
        self._init_if_needed()
        
        if not self._initialized:
            logging.warning(f"Impossibile salvare l'impostazione {key} su Firebase: connessione non inizializzata")
            return False
        
        try:
            self._settings_ref.update({key: value})
            self._cache[key] = value
            return True
        except Exception as e:
            logging.error(f"Errore nel salvataggio dell'impostazione {key} su Firebase: {e}")
            return False
    
    def get_all(self) -> Dict[str, Any]:
        """
        Ottiene tutte le impostazioni.
        
        Returns:
            Dizionario con tutte le impostazioni (Firebase + locali)
        """
        self._refresh_cache(force=True)
        
        # Combina impostazioni locali e Firebase
        all_settings = {}
        
        # Aggiungi impostazioni locali
        for key in globals():
            if key.isupper() and not key.startswith('_'):
                all_settings[key.lower()] = globals()[key]
        
        # Sovrascrivi/aggiungi impostazioni da Firebase
        for key, value in self._cache.items():
            all_settings[key.lower()] = value
        
        return all_settings


# Istanza singleton per le impostazioni
settings = FirebaseSettings()


def get_setting(key: str, default=None):
    """
    Ottiene un'impostazione.
    
    Args:
        key: Chiave dell'impostazione
        default: Valore di default se l'impostazione non è trovata
    
    Returns:
        Valore dell'impostazione
    """
    return settings.get(key, default)


def set_setting(key: str, value) -> bool:
    """
    Imposta un'impostazione.
    
    Args:
        key: Chiave dell'impostazione
        value: Valore dell'impostazione
    
    Returns:
        True se l'operazione è riuscita, False altrimenti
    """
    return settings.set(key, value)


def get_all_settings() -> Dict[str, Any]:
    """
    Ottiene tutte le impostazioni.
    
    Returns:
        Dizionario con tutte le impostazioni
    """
    return settings.get_all()
