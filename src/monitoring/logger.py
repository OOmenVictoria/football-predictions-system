""" Sistema di logging avanzato per il progetto di pronostici calcistici.
Configura il logging con formattazione personalizzata, rotazione dei file,
integrazione con Firebase e notifiche per errori critici.
"""

import os
import sys
import logging
import logging.handlers
import json
import traceback
from datetime import datetime
from typing import Dict, Any, Optional, List, Union
import firebase_admin
from firebase_admin import db
from functools import wraps

# Configurazione base del logger
LOG_DIRECTORY = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'logs')
LOG_FILENAME = os.path.join(LOG_DIRECTORY, 'football_predictions.log')
ERROR_LOG_FILENAME = os.path.join(LOG_DIRECTORY, 'errors.log')

# Livelli di logging personalizzati
VERBOSE = 5  # Più dettagliato di DEBUG
logging.addLevelName(VERBOSE, "VERBOSE")

# Creazione directory log se non esiste
os.makedirs(LOG_DIRECTORY, exist_ok=True)

class FootballLogger:
    """Gestisce il logging per l'applicazione di pronostici calcistici."""
    
    _instance = None
    
    def __new__(cls):
        """Implementa il pattern Singleton per evitare configurazioni multiple."""
        if cls._instance is None:
            cls._instance = super(FootballLogger, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """Inizializza il logger se non è già stato inizializzato."""
        if self._initialized:
            return
        
        self.logger = logging.getLogger('football_predictions')
        self.logger.setLevel(logging.DEBUG)
        self.logger.propagate = False
        
        # Evita handler duplicati
        if not self.logger.handlers:
            self._setup_handlers()
            
        self._firebase_ref = None
        self._initialized = True
    
    def _setup_handlers(self):
        """Configura gli handler per il logging."""
        # Formattatore dettagliato per i file
        file_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(name)s - %(filename)s:%(lineno)d - %(message)s'
        )
        
        # Formattatore semplice per la console
        console_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        )
        
        # Handler per console (INFO e superiori)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)
        
        # Handler per file principale con rotazione (tutti i livelli)
        file_handler = logging.handlers.RotatingFileHandler(
            LOG_FILENAME,
            maxBytes=5 * 1024 * 1024,  # 5 MB
            backupCount=10,
            encoding='utf-8'
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(file_formatter)
        self.logger.addHandler(file_handler)
        
        # Handler separato per errori
        error_file_handler = logging.handlers.RotatingFileHandler(
            ERROR_LOG_FILENAME,
            maxBytes=5 * 1024 * 1024,  # 5 MB
            backupCount=5,
            encoding='utf-8'
        )
        error_file_handler.setLevel(logging.ERROR)
        error_file_handler.setFormatter(file_formatter)
        self.logger.addHandler(error_file_handler)
    
    def set_firebase_logging(self, enabled: bool = True, ref_path: str = 'logs'):
        """
        Attiva o disattiva il logging su Firebase.
        
        Args:
            enabled: Se attivare il logging su Firebase
            ref_path: Percorso di riferimento nel database Firebase
        """
        if enabled:
            try:
                # Verifica se Firebase è già inizializzato
                if not firebase_admin._apps:
                    # Se no, gestisce la situazione (l'utente dovrebbe inizializzare Firebase prima)
                    self.logger.warning("Firebase non inizializzato. Il logging su Firebase non sarà attivo.")
                    return
                
                # Ottiene il riferimento al database
                self._firebase_ref = db.reference(ref_path)
                self.logger.info(f"Logging su Firebase attivato (percorso: {ref_path})")
                
                # Aggiunge handler Firebase
                firebase_handler = FirebaseLogHandler(self._firebase_ref)
                firebase_handler.setLevel(logging.INFO)  # Solo INFO e superiori su Firebase
                self.logger.addHandler(firebase_handler)
            except Exception as e:
                self.logger.error(f"Errore nell'inizializzazione del logging Firebase: {e}")
        else:
            # Rimuove handler Firebase se presente
            for handler in self.logger.handlers[:]:
                if isinstance(handler, FirebaseLogHandler):
                    self.logger.removeHandler(handler)
            self._firebase_ref = None
            self.logger.info("Logging su Firebase disattivato")
    
    def verbose(self, msg, *args, **kwargs):
        """Log a livello VERBOSE."""
        self.logger.log(VERBOSE, msg, *args, **kwargs)
    
    def debug(self, msg, *args, **kwargs):
        """Log a livello DEBUG."""
        self.logger.debug(msg, *args, **kwargs)
    
    def info(self, msg, *args, **kwargs):
        """Log a livello INFO."""
        self.logger.info(msg, *args, **kwargs)
    
    def warning(self, msg, *args, **kwargs):
        """Log a livello WARNING."""
        self.logger.warning(msg, *args, **kwargs)
    
    def error(self, msg, *args, **kwargs):
        """Log a livello ERROR."""
        self.logger.error(msg, *args, **kwargs)
    
    def critical(self, msg, *args, **kwargs):
        """Log a livello CRITICAL."""
        self.logger.critical(msg, *args, **kwargs)
    
    def exception(self, msg, *args, **kwargs):
        """Log di un'eccezione con traceback."""
        self.logger.exception(msg, *args, **kwargs)
    
    def log_function_call(self, func_name: str, args: tuple, kwargs: dict):
        """
        Registra una chiamata di funzione.
        
        Args:
            func_name: Nome della funzione
            args: Argomenti posizionali
            kwargs: Argomenti per nome
        """
        args_str = ', '.join([repr(arg) for arg in args])
        kwargs_str = ', '.join([f"{k}={repr(v)}" for k, v in kwargs.items()])
        all_args = ', '.join(filter(None, [args_str, kwargs_str]))
        self.debug(f"Chiamata funzione: {func_name}({all_args})")
    
    def log_api_request(self, method: str, url: str, status_code: Optional[int] = None, 
                        elapsed: Optional[float] = None, error: Optional[str] = None):
        """
        Registra una richiesta API.
        
        Args:
            method: Metodo HTTP (GET, POST, etc.)
            url: URL della richiesta
            status_code: Codice di stato della risposta
            elapsed: Tempo di esecuzione in secondi
            error: Eventuale errore
        """
        log_data = {
            'timestamp': datetime.now().isoformat(),
            'method': method,
            'url': url
        }
        
        if status_code is not None:
            log_data['status_code'] = status_code
        
        if elapsed is not None:
            log_data['elapsed_ms'] = int(elapsed * 1000)
        
        if error:
            log_data['error'] = str(error)
            self.error(f"API {method} {url}: {error}")
        elif status_code and status_code >= 400:
            self.warning(f"API {method} {url}: HTTP {status_code}")
        else:
            elapsed_str = f" ({int(elapsed * 1000)}ms)" if elapsed else ""
            self.debug(f"API {method} {url}: HTTP {status_code}{elapsed_str}")
        
        # Registra su Firebase se configurato
        if self._firebase_ref:
            try:
                self._firebase_ref.child('api_requests').push(log_data)
            except Exception as e:
                self.error(f"Errore nel logging Firebase della richiesta API: {e}")
    
    def log_scraper_operation(self, scraper_name: str, operation: str, 
                             url: Optional[str] = None, success: bool = True, 
                             items_count: Optional[int] = None, error: Optional[str] = None):
        """
        Registra un'operazione di scraping.
        
        Args:
            scraper_name: Nome dello scraper
            operation: Tipo di operazione
            url: URL di scraping
            success: Se l'operazione ha avuto successo
            items_count: Numero di elementi estratti
            error: Eventuale errore
        """
        log_data = {
            'timestamp': datetime.now().isoformat(),
            'scraper': scraper_name,
            'operation': operation,
            'success': success
        }
        
        if url:
            log_data['url'] = url
        
        if items_count is not None:
            log_data['items_count'] = items_count
        
        if error:
            log_data['error'] = str(error)
            self.error(f"Scraper {scraper_name} {operation}: {error}")
        elif success:
            items_str = f" ({items_count} items)" if items_count is not None else ""
            self.info(f"Scraper {scraper_name} {operation}: OK{items_str}")
        else:
            self.warning(f"Scraper {scraper_name} {operation}: Failed")
        
        # Registra su Firebase se configurato
        if self._firebase_ref:
            try:
                self._firebase_ref.child('scraper_operations').push(log_data)
            except Exception as e:
                self.error(f"Errore nel logging Firebase dell'operazione di scraping: {e}")


class FirebaseLogHandler(logging.Handler):
    """Handler personalizzato per inviare log a Firebase."""
    
    def __init__(self, db_ref):
        """
        Inizializza l'handler Firebase.
        
        Args:
            db_ref: Riferimento al database Firebase
        """
        super().__init__()
        self.db_ref = db_ref
    
    def emit(self, record):
        """
        Invia un record di log a Firebase.
        
        Args:
            record: Record di logging da inviare
        """
        try:
            # Preparazione del record per Firebase
            log_entry = {
                'timestamp': datetime.fromtimestamp(record.created).isoformat(),
                'level': record.levelname,
                'message': record.getMessage(),
                'source': f"{record.name}.{record.funcName}",
                'line': record.lineno
            }
            
            # Aggiunge traceback se presente
            if record.exc_info:
                log_entry['traceback'] = ''.join(traceback.format_exception(*record.exc_info))
            
            # Se è un errore, lo salva nella sezione errors
            if record.levelno >= logging.ERROR:
                self.db_ref.child('errors').push(log_entry)
            
            # Salva comunque nella sezione all_logs (con limite)
            logs_ref = self.db_ref.child('all_logs')
            logs_ref.push(log_entry)
            
            # Pulizia dei log più vecchi (mantiene ultimi 1000)
            try:
                # Ordina per chiave (che contiene timestamp) e limita a 1000
                old_logs = logs_ref.order_by_key().limit_to_first(100).get()
                # Se abbiamo più di 1000 log, rimuoviamo i più vecchi
                if old_logs and len(logs_ref.get() or {}) > 1000:
                    for key in list(old_logs.keys())[:-1000]:
                        logs_ref.child(key).delete()
            except Exception:
                # Se fallisce la pulizia, continuiamo comunque
                pass
                
        except Exception:
            # Non possiamo usare self.handleError perché potrebbe causare ricorsione
            # Stampiamo l'errore direttamente su stderr
            sys.stderr.write("Errore nell'invio del log a Firebase\n")
            traceback.print_exc(file=sys.stderr)


def log_function(logger=None):
    """
    Decoratore per il logging delle chiamate di funzione.
    
    Args:
        logger: Logger da usare (se None, usa il logger globale)
        
    Returns:
        Decoratore configurato
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal logger
            if logger is None:
                logger = FootballLogger()
            
            # Log della chiamata
            logger.log_function_call(func.__name__, args, kwargs)
            
            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                logger.exception(f"Errore in {func.__name__}: {e}")
                raise
        return wrapper
    return decorator


# Istanza globale del logger
logger = FootballLogger()


def get_logger(name=None):
    """
    Restituisce il logger configurato.
    
    Args:
        name: Nome del logger (se None, usa il logger root)
        
    Returns:
        Logger configurato
    """
    if name:
        # Crea un logger child con il nome specificato
        child_logger = logging.getLogger(f'football_predictions.{name}')
        return child_logger
    return logger.logger
