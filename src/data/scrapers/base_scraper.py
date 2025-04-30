"""
Classe base per tutti gli scraper del sistema.
Fornisce funzionalità comuni come gestione sessioni HTTP, cache,
rispetto dei robots.txt, e gestione degli errori.
"""
import os
import time
import random
import hashlib
import json
import logging
import requests
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import sqlite3

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

class ScraperCache:
    """
    Cache per i risultati dello scraping per ridurre le richieste HTTP.
    Implementa una cache in SQLite locale.
    """
    def __init__(self, cache_name):
        """
        Inizializza la cache.
        
        Args:
            cache_name (str): Nome della cache (usato per il file SQLite)
        """
        self.cache_dir = os.path.expanduser("~/football-predictions/cache")
        os.makedirs(self.cache_dir, exist_ok=True)
        self.db_path = os.path.join(self.cache_dir, f"{cache_name}.db")
        self._init_db()
    
    def _init_db(self):
        """Inizializza il database SQLite."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS cache (
            key TEXT PRIMARY KEY,
            value TEXT,
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
        cursor.execute("DELETE FROM cache WHERE expires < ?", (now,))
        conn.commit()
        conn.close()
    
    def get(self, key):
        """
        Recupera un valore dalla cache.
        
        Args:
            key (str): Chiave della cache
            
        Returns:
            str: Valore in cache o None se non presente/scaduto
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT value, expires FROM cache WHERE key = ?", (key,))
        result = cursor.fetchone()
        conn.close()
        
        if result:
            value, expires = result
            now = int(time.time())
            if expires > now:
                return value
        
        return None
    
    def set(self, key, value, ttl):
        """
        Salva un valore nella cache.
        
        Args:
            key (str): Chiave della cache
            value (str): Valore da memorizzare
            ttl (int): Tempo di vita in secondi
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        now = int(time.time())
        expires = now + ttl
        
        cursor.execute(
            "INSERT OR REPLACE INTO cache VALUES (?, ?, ?, ?)",
            (key, value, now, expires)
        )
        
        conn.commit()
        conn.close()

class BaseScraper:
    """
    Classe base per tutti gli scraper con funzionalità comuni:
    - Gestione sessione HTTP
    - Rispetto robots.txt
    - Rotazione User Agent
    - Rate limiting
    - Caching
    - Parsing HTML
    """
    
    def __init__(self, name, base_url, cache_ttl=3600, respect_robots=True, 
                 delay_range=(1.0, 3.0), max_retries=3):
        """
        Inizializza lo scraper base.
        
        Args:
            name (str): Nome dello scraper
            base_url (str): URL base del sito 
            cache_ttl (int): Tempo di vita della cache in secondi (default 1 ora)
            respect_robots (bool): Se rispettare il robots.txt
            delay_range (tuple): Range di delay tra richieste (min, max) in secondi
            max_retries (int): Numero massimo di tentativi per richiesta
        """
        self.name = name
        self.base_url = base_url
        self.cache_ttl = cache_ttl
        self.respect_robots = respect_robots
        self.delay_range = delay_range
        self.max_retries = max_retries
        self.session = self._create_session()
        self.cache = ScraperCache(f"{name.lower()}_cache")
        self.robots_parser = self._init_robots_parser() if respect_robots else None
        self.logger = logging.getLogger(f"scraper.{name.lower()}")
        
        # Timestamp ultima richiesta per gestire rate limiting
        self.last_request_time = 0
    
    def _create_session(self):
        """Crea sessione HTTP con retry automatici."""
        session = requests.Session()
        retry_strategy = Retry(
            total=self.max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update({"User-Agent": self._get_random_user_agent()})
        return session
    
    def _get_random_user_agent(self):
        """Restituisce uno User-Agent casuale."""
        return random.choice(USER_AGENTS)
    
    def _init_robots_parser(self):
        """Inizializza il parser robots.txt."""
        try:
            parser = RobotFileParser()
            parser.set_url(f"{self.base_url}/robots.txt")
            parser.read()
            return parser
        except Exception as e:
            self.logger.warning(f"Errore nel parsing robots.txt: {e}")
            return None
    
    def is_allowed(self, url):
        """
        Verifica se lo scraping dell'URL è permesso.
        
        Args:
            url (str): URL da verificare
            
        Returns:
            bool: True se consentito, False altrimenti
        """
        if not self.respect_robots or not self.robots_parser:
            return True
        return self.robots_parser.can_fetch("*", url)
    
    def get(self, url, params=None, use_cache=True, force_new_agent=False):
        """
        Effettua una richiesta GET con cache e gestione errori.
        
        Args:
            url (str): URL da richiedere
            params (dict): Parametri della query string
            use_cache (bool): Se usare la cache
            force_new_agent (bool): Se forzare un nuovo user agent
            
        Returns:
            str: Contenuto della risposta o None in caso di errore
        """
        # Genera chiave cache
        cache_key = self._generate_cache_key(url, params)
        
        # Verifica nella cache
        if use_cache:
            cached_response = self.cache.get(cache_key)
            if cached_response:
                self.logger.debug(f"Cache hit per {url}")
                return cached_response
        
        # Verifica robots.txt
        if not self.is_allowed(url):
            self.logger.warning(f"URL non permesso da robots.txt: {url}")
            return None
        
        # Attesa per rispettare rate limits
        self._wait()
        
        # Aggiorna User-Agent se richiesto
        if force_new_agent:
            self.session.headers.update({"User-Agent": self._get_random_user_agent()})
        
        # Effettua la richiesta
        try:
            self.logger.info(f"Richiesta a {url}")
            response = self.session.get(url, params=params, timeout=30)
            
            # Aggiorna timestamp ultima richiesta
            self.last_request_time = time.time()
            
            if response.status_code == 200:
                # Salva in cache e restituisci
                if use_cache:
                    self.cache.set(cache_key, response.text, self.cache_ttl)
                return response.text
            elif response.status_code == 429:  # Too Many Requests
                self.logger.warning(f"Rate limit raggiunto per {url}. Attesa più lunga.")
                time.sleep(60)  # Attesa molto più lunga
                return self.get(url, params, use_cache, True)  # Riprova con nuovo User-Agent
            else:
                self.logger.error(f"Errore {response.status_code} per {url}")
                return None
        except Exception as e:
            self.logger.error(f"Eccezione durante richiesta a {url}: {str(e)}")
            return None
    
    def _generate_cache_key(self, url, params=None):
        """
        Genera una chiave univoca per la cache.
        
        Args:
            url (str): URL 
            params (dict): Parametri query string
            
        Returns:
            str: Chiave hash MD5
        """
        key_parts = [url]
        if params:
            key_parts.append(json.dumps(params, sort_keys=True))
        return hashlib.md5("".join(key_parts).encode()).hexdigest()
    
    def _wait(self):
        """Attende un periodo casuale per rispettare i rate limits."""
        # Calcola tempo trascorso dall'ultima richiesta
        elapsed = time.time() - self.last_request_time
        
        # Determina il delay
        min_delay, max_delay = self.delay_range
        
        # Se il tempo trascorso è minore del delay minimo, attendi
        if elapsed < min_delay:
            wait_time = min_delay - elapsed + random.uniform(0, max_delay - min_delay)
            time.sleep(wait_time)
    
    def parse(self, html, selector=None):
        """
        Parse HTML con BeautifulSoup.
        
        Args:
            html (str): Contenuto HTML
            selector (str): Selettore CSS opzionale
            
        Returns:
            BeautifulSoup o list: Oggetto BeautifulSoup o lista di elementi se selector specificato
        """
        try:
            soup = BeautifulSoup(html, 'html.parser')
            if selector:
                return soup.select(selector)
            return soup
        except Exception as e:
            self.logger.error(f"Errore nel parsing HTML: {e}")
            return [] if selector else None
    
    def extract_text(self, element, selector, default=""):
        """
        Estrae testo da un elemento con gestione errori.
        
        Args:
            element: Elemento BeautifulSoup
            selector (str): Selettore CSS
            default: Valore predefinito se non trovato
            
        Returns:
            str: Testo estratto o valore predefinito
        """
        try:
            selected = element.select_one(selector)
            return selected.text.strip() if selected else default
        except Exception:
            return default
    
    def extract_attr(self, element, selector, attr, default=""):
        """
        Estrae attributo da un elemento con gestione errori.
        
        Args:
            element: Elemento BeautifulSoup
            selector (str): Selettore CSS
            attr (str): Nome attributo
            default: Valore predefinito se non trovato
            
        Returns:
            str: Valore attributo o predefinito
        """
        try:
            selected = element.select_one(selector)
            return selected.get(attr, default) if selected else default
        except Exception:
            return default
    
    def to_numeric(self, value, default=0.0):
        """
        Converte un valore in numero con gestione errori.
        
        Args:
            value (str): Valore da convertire
            default: Valore predefinito in caso di errore
            
        Returns:
            float: Valore numerico o default
        """
        try:
            # Rimuovi caratteri non numerici (eccetto punto/virgola decimale)
            cleaned = ''.join(c for c in value if c.isdigit() or c in '.-')
            return float(cleaned)
        except (ValueError, TypeError):
            return default
