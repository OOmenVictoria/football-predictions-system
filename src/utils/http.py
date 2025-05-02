"""
Modulo con utilità per la gestione delle richieste HTTP.
Fornisce funzioni per le richieste HTTP con gestione errori, retry, cache e altre funzionalità.
"""
import os
import time
import json
import logging
import random
import hashlib
import sqlite3
import requests
from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pathlib import Path

# Configurazione logging
logger = logging.getLogger(__name__)

# Lista di User Agents per rotazione
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36 Edg/92.0.902.55",
    "Mozilla/5.0 (iPad; CPU OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36 OPR/78.0.4093.184"
]

# Aggiungiamo la funzione make_request che è richiesta da altri moduli
def make_request(
    url: str,
    method: str = "GET",
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    data: Optional[Any] = None,
    json: Optional[Dict[str, Any]] = None,
    timeout: int = 30,
    retries: int = 3,
    backoff_factor: float = 0.3,
    status_forcelist: List[int] = [500, 502, 503, 504],
) -> Optional[requests.Response]:
    """
    Make HTTP request with retry capability.
    
    Args:
        url: URL to request
        method: HTTP method (GET, POST, etc.)
        params: URL parameters
        headers: HTTP headers
        data: Request body data
        json: JSON data for request body
        timeout: Request timeout in seconds
        retries: Number of retries for failed requests
        backoff_factor: Backoff factor for retries
        status_forcelist: List of status codes to retry
        
    Returns:
        Response object or None if failed
    """
    try:
        # Create session with retry
        session = create_session(max_retries=retries, backoff_factor=backoff_factor, status_forcelist=status_forcelist)
        
        # Default headers
        default_headers = {
            "User-Agent": random.choice(USER_AGENTS)
        }
        
        # Merge headers
        if headers:
            default_headers.update(headers)
        
        # Make request
        response = session.request(
            method=method,
            url=url,
            params=params,
            headers=default_headers,
            data=data,
            json=json,
            timeout=timeout
        )
        
        # Log request info
        logger.debug(f"Request: {method} {url} - Status: {response.status_code}")
        
        return response
    
    except requests.RequestException as e:
        logger.error(f"Request error: {str(e)} - URL: {url}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error during request: {str(e)} - URL: {url}")
        return None

class HTTPCache:
    """
    Cache per le richieste HTTP per ridurre le chiamate ripetute.
    Implementa una cache in SQLite locale.
    """
    def __init__(self, name: str = "http_cache", cache_dir: Optional[str] = None):
        """
        Inizializza la cache.
        
        Args:
            name: Nome della cache
            cache_dir: Directory per la cache, se None usa '~/football-predictions/cache'
        """
        if not cache_dir:
            cache_dir = os.path.expanduser("~/football-predictions/cache")
            
        os.makedirs(cache_dir, exist_ok=True)
        self.db_path = os.path.join(cache_dir, f"{name}.db")
        self._init_db()
    
    def _init_db(self):
        """Inizializza il database SQLite."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS http_cache (
            key TEXT PRIMARY KEY,
            url TEXT,
            method TEXT,
            params TEXT,
            headers TEXT,
            response TEXT,
            status_code INTEGER,
            timestamp INTEGER,
            expires INTEGER
        )
        ''')
        conn.commit()
        
        # Pulizia cache scaduta
        self._cleanup()
        
        conn.close()
    
    def _cleanup(self):
        """Rimuove le voci di cache scadute."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        now = int(time.time())
        cursor.execute("DELETE FROM http_cache WHERE expires < ?", (now,))
        deleted_count = cursor.rowcount
        conn.commit()
        conn.close()
        
        if deleted_count > 0:
            logger.debug(f"Rimossi {deleted_count} elementi scaduti dalla cache HTTP")
    
    def _generate_key(self, url: str, method: str, params: Dict = None, headers: Dict = None) -> str:
        """
        Genera una chiave univoca per la richiesta.
        
        Args:
            url: URL della richiesta
            method: Metodo HTTP (GET, POST, etc.)
            params: Parametri della query string
            headers: Headers della richiesta (vengono filtrati solo quelli rilevanti)
            
        Returns:
            Stringa hash MD5
        """
        # Filtriamo solo gli header rilevanti per la cache
        # (escludiamo headers che non influenzano la risposta)
        relevant_headers = {}
        if headers:
            for header in ["Accept", "Accept-Language", "Content-Type"]:
                if header in headers:
                    relevant_headers[header] = headers[header]
        
        # Creiamo una stringa che rappresenta la richiesta
        key_parts = [url, method.upper()]
        
        if params:
            # Ordina le chiavi per garantire consistenza
            key_parts.append(json.dumps(params, sort_keys=True))
        
        if relevant_headers:
            key_parts.append(json.dumps(relevant_headers, sort_keys=True))
        
        # Genera hash MD5
        key = hashlib.md5("".join(key_parts).encode()).hexdigest()
        return key
    
    def get(self, url: str, method: str = "GET", params: Dict = None, headers: Dict = None) -> Optional[Dict]:
        """
        Recupera una risposta dalla cache.
        
        Args:
            url: URL della richiesta
            method: Metodo HTTP
            params: Parametri query string
            headers: Headers della richiesta
            
        Returns:
            Dizionario con i dati della risposta o None se non in cache
        """
        key = self._generate_key(url, method, params, headers)
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT response, status_code, expires 
                FROM http_cache 
                WHERE key = ?
            """, (key,))
            result = cursor.fetchone()
            conn.close()
            
            if not result:
                return None
                
            response_text, status_code, expires = result
            
            # Verifica scadenza
            now = int(time.time())
            if expires < now:
                return None
                
            return {
                "text": response_text,
                "status_code": status_code
            }
        except Exception as e:
            logger.warning(f"Errore lettura cache HTTP: {str(e)}")
            return None
    
    def set(self, url: str, method: str, params: Dict, headers: Dict, 
            response_text: str, status_code: int, ttl: int):
        """
        Salva una risposta nella cache.
        
        Args:
            url: URL della richiesta
            method: Metodo HTTP
            params: Parametri query string
            headers: Headers della richiesta
            response_text: Corpo della risposta
            status_code: Codice stato HTTP
            ttl: Tempo di vita in secondi
        """
        key = self._generate_key(url, method, params, headers)
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            now = int(time.time())
            expires = now + ttl
            
            # Salva i dati della richiesta e della risposta
            cursor.execute("""
                INSERT OR REPLACE INTO http_cache 
                (key, url, method, params, headers, response, status_code, timestamp, expires) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                key, 
                url, 
                method, 
                json.dumps(params) if params else None, 
                json.dumps(headers) if headers else None, 
                response_text, 
                status_code, 
                now, 
                expires
            ))
            
            conn.commit()
            conn.close()
        except Exception as e:
            logger.warning(f"Errore salvataggio cache HTTP: {str(e)}")

def create_session(max_retries: int = 3, backoff_factor: float = 0.3, 
                  status_forcelist: List[int] = None) -> requests.Session:
    """
    Crea una sessione HTTP con retry automatici.
    
    Args:
        max_retries: Numero massimo di tentativi
        backoff_factor: Fattore di incremento attesa tra retry
        status_forcelist: Lista di codici di stato per cui tentare il retry
        
    Returns:
        Sessione requests configurata
    """
    if status_forcelist is None:
        status_forcelist = [429, 500, 502, 503, 504]
        
    session = requests.Session()
    
    # Configura strategia di retry
    retry_strategy = Retry(
        total=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS"]
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    # Imposta User-Agent casuale
    session.headers.update({"User-Agent": random.choice(USER_AGENTS)})
    
    return session

def get(url: str, params: Dict = None, headers: Dict = None, timeout: int = 30, 
        max_retries: int = 3, use_cache: bool = True, cache_ttl: int = 3600,
        cache_name: str = "http_cache") -> Optional[requests.Response]:
    """
    Effettua una richiesta GET con retry, cache e gestione errori.
    
    Args:
        url: URL della richiesta
        params: Parametri della query string
        headers: Headers HTTP
        timeout: Timeout in secondi
        max_retries: Numero massimo di tentativi
        use_cache: Se usare la cache
        cache_ttl: Tempo di vita della cache in secondi
        cache_name: Nome della cache
        
    Returns:
        Oggetto Response o None in caso di errore
    """
    # Inizializza cache se necessario
    cache = None
    if use_cache:
        cache = HTTPCache(cache_name)
        
        # Verifica cache
        cached = cache.get(url, "GET", params, headers)
        if cached:
            # Crea una risposta fittizia con i dati dalla cache
            response = requests.Response()
            response.status_code = cached["status_code"]
            response._content = cached["text"].encode()
            response.url = url
            logger.debug(f"Cache hit per {url}")
            return response
    
    # Crea sessione con retry
    session = create_session(max_retries=max_retries)
    
    # Aggiungi headers se forniti
    if headers:
        session.headers.update(headers)
    
    try:
        logger.debug(f"GET: {url}")
        response = session.get(url, params=params, timeout=timeout)
        
        # Salva in cache se richiesto e status code accettabile
        if use_cache and cache and response.status_code == 200:
            cache.set(
                url=url,
                method="GET",
                params=params,
                headers=headers,
                response_text=response.text,
                status_code=response.status_code,
                ttl=cache_ttl
            )
            
        return response
    except Exception as e:
        logger.error(f"Errore nella richiesta GET a {url}: {str(e)}")
        return None

def post(url: str, data: Dict = None, json_data: Dict = None, headers: Dict = None, 
         timeout: int = 30, max_retries: int = 3) -> Optional[requests.Response]:
    """
    Effettua una richiesta POST con retry e gestione errori.
    
    Args:
        url: URL della richiesta
        data: Dati form
        json_data: Dati JSON
        headers: Headers HTTP
        timeout: Timeout in secondi
        max_retries: Numero massimo di tentativi
        
    Returns:
        Oggetto Response o None in caso di errore
    """
    # Crea sessione con retry
    session = create_session(max_retries=max_retries)
    
    # Aggiungi headers se forniti
    if headers:
        session.headers.update(headers)
    
    try:
        logger.debug(f"POST: {url}")
        response = session.post(url, data=data, json=json_data, timeout=timeout)
        return response
    except Exception as e:
        logger.error(f"Errore nella richiesta POST a {url}: {str(e)}")
        return None

def download_file(url: str, output_path: str, chunk_size: int = 8192, 
                 headers: Dict = None, max_retries: int = 3) -> bool:
    """
    Scarica un file con gestione errori e retry.
    
    Args:
        url: URL del file
        output_path: Percorso dove salvare il file
        chunk_size: Dimensione chunk in byte
        headers: Headers HTTP
        max_retries: Numero massimo di tentativi
        
    Returns:
        True se successo, False altrimenti
    """
    # Crea directory di output se non esiste
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    
    # Crea sessione con retry
    session = create_session(max_retries=max_retries)
    
    # Aggiungi headers se forniti
    if headers:
        session.headers.update(headers)
    
    try:
        logger.info(f"Scaricamento file: {url} -> {output_path}")
        response = session.get(url, stream=True)
        
        if response.status_code != 200:
            logger.error(f"Errore download, status code: {response.status_code}")
            return False
        
        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
        
        logger.info(f"File scaricato: {output_path}")
        return True
    except Exception as e:
        logger.error(f"Errore nel download da {url}: {str(e)}")
        # Rimuovi file parziale
        if os.path.exists(output_path):
            os.remove(output_path)
        return False

def get_json(url: str, params: Dict = None, headers: Dict = None, timeout: int = 30,
            max_retries: int = 3, use_cache: bool = True, cache_ttl: int = 3600) -> Optional[Dict]:
    """
    Effettua una richiesta GET e converte il risultato in JSON.
    
    Args:
        url: URL della richiesta
        params: Parametri della query string
        headers: Headers HTTP
        timeout: Timeout in secondi
        max_retries: Numero massimo di tentativi
        use_cache: Se usare la cache
        cache_ttl: Tempo di vita della cache in secondi
        
    Returns:
        Dizionario JSON o None in caso di errore
    """
    response = get(
        url=url, 
        params=params, 
        headers=headers, 
        timeout=timeout,
        max_retries=max_retries,
        use_cache=use_cache,
        cache_ttl=cache_ttl
    )
    
    if not response or response.status_code != 200:
        return None
    
    try:
        return response.json()
    except Exception as e:
        logger.error(f"Errore parsing JSON da {url}: {str(e)}")
        return None

def post_json(url: str, data: Dict = None, json_data: Dict = None, headers: Dict = None,
             timeout: int = 30, max_retries: int = 3) -> Optional[Dict]:
    """
    Effettua una richiesta POST e converte il risultato in JSON.
    
    Args:
        url: URL della richiesta
        data: Dati form
        json_data: Dati JSON
        headers: Headers HTTP
        timeout: Timeout in secondi
        max_retries: Numero massimo di tentativi
        
    Returns:
        Dizionario JSON o None in caso di errore
    """
    response = post(
        url=url, 
        data=data, 
        json_data=json_data, 
        headers=headers, 
        timeout=timeout,
        max_retries=max_retries
    )
    
    if not response or response.status_code != 200:
        return None
    
    try:
        return response.json()
    except Exception as e:
        logger.error(f"Errore parsing JSON da {url}: {str(e)}")
        return None

def get_random_user_agent() -> str:
    """
    Restituisce uno User-Agent casuale per evitare blocchi.
    
    Returns:
        Stringa User-Agent
    """
    return random.choice(USER_AGENTS)
