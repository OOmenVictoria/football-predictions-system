"""
Sistema di cache multi-livello per il progetto di pronostici calcistici.
Fornisce cache in memoria, su disco e su Firebase per ottimizzare le prestazioni.
"""
import os
import time
import json
import pickle
import logging
import sqlite3
import hashlib
from typing import Dict, List, Any, Optional, Union, Callable
from datetime import datetime
from pathlib import Path
import threading

# Import Firebase solo se disponibile
try:
    import firebase_admin
    from firebase_admin import credentials, db
    FIREBASE_AVAILABLE = True
except ImportError:
    FIREBASE_AVAILABLE = False

# Configurazione logging
logger = logging.getLogger(__name__)

class Cache:
    """
    Classe base per tutti i tipi di cache.
    Definisce l'interfaccia comune per tutti i livelli di cache.
    """
    
    def get(self, key: str) -> Optional[Any]:
        """
        Recupera un valore dalla cache.
        
        Args:
            key: Chiave di ricerca
            
        Returns:
            Valore associato alla chiave o None se non trovato
        """
        raise NotImplementedError()
    
    def set(self, key: str, value: Any, ttl: int = 3600) -> bool:
        """
        Salva un valore nella cache.
        
        Args:
            key: Chiave di memorizzazione
            value: Valore da memorizzare
            ttl: Tempo di vita in secondi (default: 1 ora)
            
        Returns:
            True se salvato con successo, False altrimenti
        """
        raise NotImplementedError()
    
    def delete(self, key: str) -> bool:
        """
        Rimuove un valore dalla cache.
        
        Args:
            key: Chiave da rimuovere
            
        Returns:
            True se rimosso con successo, False altrimenti
        """
        raise NotImplementedError()
    
    def clear(self) -> bool:
        """
        Svuota completamente la cache.
        
        Returns:
            True se l'operazione ha avuto successo, False altrimenti
        """
        raise NotImplementedError()

class MemoryCache(Cache):
    """
    Implementazione cache in memoria (primo livello, più veloce).
    Usa un dizionario in memoria con scadenza per ogni valore.
    """
    
    def __init__(self):
        """Inizializza la cache in memoria."""
        self._cache = {}
        self._lock = threading.RLock()  # Per thread-safety
    
    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            if key not in self._cache:
                return None
            
            entry = self._cache[key]
            
            # Verifica se l'entry è scaduta
            if time.time() > entry['expires']:
                del self._cache[key]
                return None
                
            return entry['value']
    
    def set(self, key: str, value: Any, ttl: int = 3600) -> bool:
        with self._lock:
            # Calcola timestamp di scadenza
            expires = time.time() + ttl
            
            # Memorizza valore e scadenza
            self._cache[key] = {
                'value': value,
                'expires': expires
            }
            
            return True
    
    def delete(self, key: str) -> bool:
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False
    
    def clear(self) -> bool:
        with self._lock:
            self._cache.clear()
            return True

class DiskCache(Cache):
    """
    Implementazione cache su disco (secondo livello).
    Usa un database SQLite per la persistenza.
    """
    
    def __init__(self, namespace: str = "default", cache_dir: Optional[str] = None):
        """
        Inizializza la cache su disco.
        
        Args:
            namespace: Namespace per separare diverse cache
            cache_dir: Directory per la cache, se None usa '~/football-predictions/cache'
        """
        if not cache_dir:
            cache_dir = os.path.expanduser("~/football-predictions/cache")
            
        os.makedirs(cache_dir, exist_ok=True)
        self.db_path = os.path.join(cache_dir, f"{namespace}.db")
        self._init_db()
    
    def _init_db(self):
        """Inizializza il database SQLite."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS cache (
            key TEXT PRIMARY KEY,
            value BLOB,
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
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            now = int(time.time())
            cursor.execute("DELETE FROM cache WHERE expires < ?", (now,))
            deleted_count = cursor.rowcount
            conn.commit()
            conn.close()
            
            if deleted_count > 0:
                logger.debug(f"Rimossi {deleted_count} elementi scaduti dalla cache su disco")
        except Exception as e:
            logger.warning(f"Errore pulizia cache disco: {str(e)}")
    
    def get(self, key: str) -> Optional[Any]:
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT value, expires FROM cache WHERE key = ?", (key,))
            result = cursor.fetchone()
            conn.close()
            
            if not result:
                return None
                
            value_blob, expires = result
            
            # Verifica scadenza
            now = int(time.time())
            if expires < now:
                # Rimuovi l'elemento scaduto
                self.delete(key)
                return None
                
            # Deserializza il valore
            try:
                return pickle.loads(value_blob)
            except Exception as e:
                logger.warning(f"Errore deserializzazione cache: {str(e)}")
                self.delete(key)
                return None
        except Exception as e:
            logger.warning(f"Errore lettura cache disco: {str(e)}")
            return None
    
    def set(self, key: str, value: Any, ttl: int = 3600) -> bool:
        try:
            # Serializza il valore
            value_blob = pickle.dumps(value)
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            now = int(time.time())
            expires = now + ttl
            
            cursor.execute(
                "INSERT OR REPLACE INTO cache VALUES (?, ?, ?, ?)",
                (key, value_blob, now, expires)
            )
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logger.warning(f"Errore salvataggio cache disco: {str(e)}")
            return False
    
    def delete(self, key: str) -> bool:
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("DELETE FROM cache WHERE key = ?", (key,))
            result = cursor.rowcount > 0
            conn.commit()
            conn.close()
            return result
        except Exception as e:
            logger.warning(f"Errore rimozione chiave da cache disco: {str(e)}")
            return False
    
    def clear(self) -> bool:
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("DELETE FROM cache")
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logger.warning(f"Errore pulizia cache disco: {str(e)}")
            return False

class FirebaseCache(Cache):
    """
    Implementazione cache su Firebase (terzo livello).
    Consente la condivisione della cache tra diverse esecuzioni.
    """
    
    def __init__(self, namespace: str = "default"):
        """
        Inizializza la cache Firebase.
        
        Args:
            namespace: Namespace per separare diverse cache
        """
        if not FIREBASE_AVAILABLE:
            logger.warning("Firebase non disponibile. FirebaseCache disabilitata.")
            self.available = False
            return
            
        self.namespace = namespace
        self.available = self._initialize()
    
    def _initialize(self) -> bool:
        """Inizializza connessione Firebase."""
        try:
            if not FIREBASE_AVAILABLE:
                return False
                
            # Verifica se l'app è già inizializzata
            try:
                firebase_admin.get_app()
            except ValueError:
                # Usa credenziali da variabile d'ambiente o file
                cred_json = os.getenv('FIREBASE_CREDENTIALS')
                if cred_json:
                    try:
                        cred_dict = json.loads(cred_json)
                        cred = credentials.Certificate(cred_dict)
                    except json.JSONDecodeError:
                        # Prova a interpretarlo come path
                        cred = credentials.Certificate(cred_json)
                else:
                    # Fallback al percorso file predefinito
                    cred_path = os.path.expanduser('~/football-predictions/creds/firebase-credentials.json')
                    if not os.path.exists(cred_path):
                        logger.warning(f"File credenziali Firebase non trovato: {cred_path}")
                        return False
                        
                    cred = credentials.Certificate(cred_path)
                    
                firebase_admin.initialize_app(cred, {
                    'databaseURL': os.getenv('FIREBASE_DB_URL')
                })
            
            # Verifica connessione
            ref = db.reference(f'cache/{self.namespace}/.test')
            ref.set({"timestamp": time.time()})
            ref.delete()
            
            # Pulizia vecchie entries
            self._cleanup()
            
            return True
        except Exception as e:
            logger.warning(f"Errore inizializzazione Firebase: {str(e)}")
            return False
    
    def _cleanup(self):
        """Rimuove le voci di cache scadute."""
        if not self.available:
            return
            
        try:
            ref = db.reference(f'cache/{self.namespace}')
            now = time.time()
            
            # Firebase non supporta facilmente query di questo tipo,
            # dovremmo scaricare tutti i dati e filtrarli
            # Per evitare sovraccarichi, implementiamo un sistema di pulizia incrementale
            
            # Otteniamo un campione limitato di chiavi
            snapshot = ref.order_by_child('expires').limit_to_first(100).get()
            
            if not snapshot:
                return
                
            # Rimuoviamo le entries scadute
            deleted_count = 0
            for key, value in snapshot.items():
                if value.get('expires', 0) < now:
                    ref.child(key).delete()
                    deleted_count += 1
            
            if deleted_count > 0:
                logger.debug(f"Rimossi {deleted_count} elementi scaduti dalla cache Firebase")
                
        except Exception as e:
            logger.warning(f"Errore pulizia cache Firebase: {str(e)}")
    
    def get(self, key: str) -> Optional[Any]:
        if not self.available:
            return None
            
        try:
            # Genera chiave sicura per path Firebase
            safe_key = self._make_safe_key(key)
            
            ref = db.reference(f'cache/{self.namespace}/{safe_key}')
            data = ref.get()
            
            if not data:
                return None
                
            # Verifica scadenza
            expires = data.get('expires', 0)
            if time.time() > expires:
                # Rimuovi l'elemento scaduto
                ref.delete()
                return None
                
            # Restituisci il valore
            return data.get('value')
            
        except Exception as e:
            logger.warning(f"Errore lettura cache Firebase: {str(e)}")
            return None
    
    def set(self, key: str, value: Any, ttl: int = 3600) -> bool:
        if not self.available:
            return False
            
        try:
            # Genera chiave sicura per path Firebase
            safe_key = self._make_safe_key(key)
            
            # Verifica che value sia serializzabile in JSON
            try:
                json.dumps(value)
            except TypeError:
                logger.warning(f"Impossibile serializzare il valore per cache Firebase: {type(value)}")
                return False
                
            # Calcola timestamp di scadenza
            expires = time.time() + ttl
            
            # Salva in Firebase
            ref = db.reference(f'cache/{self.namespace}/{safe_key}')
            ref.set({
                'value': value,
                'timestamp': time.time(),
                'expires': expires
            })
            
            return True
        except Exception as e:
            logger.warning(f"Errore salvataggio cache Firebase: {str(e)}")
            return False
    
    def delete(self, key: str) -> bool:
        if not self.available:
            return False
            
        try:
            # Genera chiave sicura per path Firebase
            safe_key = self._make_safe_key(key)
            
            ref = db.reference(f'cache/{self.namespace}/{safe_key}')
            ref.delete()
            return True
        except Exception as e:
            logger.warning(f"Errore rimozione chiave da cache Firebase: {str(e)}")
            return False
    
    def clear(self) -> bool:
        if not self.available:
            return False
            
        try:
            ref = db.reference(f'cache/{self.namespace}')
            ref.delete()
            return True
        except Exception as e:
            logger.warning(f"Errore pulizia cache Firebase: {str(e)}")
            return False
    
    def _make_safe_key(self, key: str) -> str:
        """
        Converte una chiave in un formato sicuro per Firebase.
        Firebase non accetta: ., $, #, [, ], / nei percorsi.
        
        Args:
            key: Chiave originale
            
        Returns:
            Chiave sicura per Firebase
        """
        # Come hash MD5 è sicuro per i percorsi
        return hashlib.md5(key.encode()).hexdigest()

class MultiLevelCache(Cache):
    """
    Cache multi-livello che combina memoria, disco e opzionalmente Firebase.
    Utilizza una strategia di cache a cascata con fallback.
    """
    
    def __init__(self, namespace: str = "default", enable_firebase: bool = True, 
                 cache_dir: Optional[str] = None):
        """
        Inizializza la cache multi-livello.
        
        Args:
            namespace: Namespace per separare diverse cache
            enable_firebase: Se attivare la cache Firebase
            cache_dir: Directory per la cache su disco
        """
        # Inizializza i diversi livelli di cache
        self.memory_cache = MemoryCache()
        self.disk_cache = DiskCache(namespace, cache_dir)
        
        # Inizializza Firebase solo se richiesto
        self.firebase_cache = None
        if enable_firebase and FIREBASE_AVAILABLE:
            self.firebase_cache = FirebaseCache(namespace)
            
        self.namespace = namespace
    
    def get(self, key: str) -> Optional[Any]:
        """
        Cerca il valore in tutti i livelli di cache.
        Promuove il valore ai livelli superiori se trovato nei livelli inferiori.
        
        Args:
            key: Chiave da cercare
            
        Returns:
            Valore associato alla chiave o None se non trovato
        """
        # 1. Cerca in memoria (più veloce)
        value = self.memory_cache.get(key)
        if value is not None:
            return value
            
        # 2. Cerca su disco
        value = self.disk_cache.get(key)
        if value is not None:
            # Promuovi a memoria
            self.memory_cache.set(key, value)
            return value
            
        # 3. Cerca su Firebase (se disponibile)
        if self.firebase_cache and self.firebase_cache.available:
            value = self.firebase_cache.get(key)
            if value is not None:
                # Promuovi a memoria e disco
                self.memory_cache.set(key, value)
                self.disk_cache.set(key, value)
                return value
                
        return None
    
    def set(self, key: str, value: Any, ttl: int = 3600, levels: List[str] = None) -> bool:
        """
        Salva il valore in tutti i livelli di cache richiesti.
        
        Args:
            key: Chiave per memorizzare il valore
            value: Valore da memorizzare
            ttl: Tempo di vita in secondi
            levels: Livelli di cache da utilizzare, None per tutti
            
        Returns:
            True se salvato in almeno un livello, False altrimenti
        """
        if levels is None:
            # Default: tutti i livelli disponibili
            levels = ['memory', 'disk', 'firebase']
            
        success = False
        
        # 1. Salva in memoria
        if 'memory' in levels:
            success = self.memory_cache.set(key, value, ttl) or success
            
        # 2. Salva su disco
        if 'disk' in levels:
            success = self.disk_cache.set(key, value, ttl) or success
            
        # 3. Salva su Firebase (se disponibile)
        if 'firebase' in levels and self.firebase_cache and self.firebase_cache.available:
            # Per Firebase, assicuriamoci che il valore sia serializzabile JSON
            try:
                json.dumps(value)
                success = self.firebase_cache.set(key, value, ttl) or success
            except TypeError:
                logger.debug(f"Valore non serializzabile in JSON, saltato salvataggio in Firebase: {type(value)}")
                
        return success
    
    def delete(self, key: str) -> bool:
        """
        Rimuove il valore da tutti i livelli di cache.
        
        Args:
            key: Chiave da rimuovere
            
        Returns:
            True se rimosso da almeno un livello, False altrimenti
        """
        success = False
        
        # Rimuovi da memoria
        success = self.memory_cache.delete(key) or success
        
        # Rimuovi da disco
        success = self.disk_cache.delete(key) or success
        
        # Rimuovi da Firebase (se disponibile)
        if self.firebase_cache and self.firebase_cache.available:
            success = self.firebase_cache.delete(key) or success
            
        return success
    
    def clear(self) -> bool:
        """
        Svuota tutti i livelli di cache.
        
        Returns:
            True se l'operazione ha avuto successo su almeno un livello, False altrimenti
        """
        success = False
        
        # Svuota memoria
        success = self.memory_cache.clear() or success
        
        # Svuota disco
        success = self.disk_cache.clear() or success
        
        # Svuota Firebase (se disponibile)
        if self.firebase_cache and self.firebase_cache.available:
            success = self.firebase_cache.clear() or success
            
        return success

def cached(ttl: int = 3600, namespace: str = "default", key_fn: Optional[Callable] = None):
    """
    Decoratore per cachare i risultati di una funzione.
    
    Args:
        ttl: Tempo di vita in secondi
        namespace: Namespace per la cache
        key_fn: Funzione per generare la chiave, default: usa args e kwargs
        
    Returns:
        Funzione decorata
    """
    cache = MultiLevelCache(namespace)
    
    def decorator(func):
        def wrapper(*args, **kwargs):
            # Genera la chiave di cache
            if key_fn:
                # Usa la funzione personalizzata
                key = key_fn(*args, **kwargs)
            else:
                # Genera chiave basata su funzione, args e kwargs
                key_parts = [func.__module__, func.__name__]
                
                # Aggiungi args (se serializzabili)
                for arg in args:
                    try:
                        key_parts.append(str(arg))
                    except:
                        key_parts.append(str(hash(arg)))
                
                # Aggiungi kwargs (se serializzabili)
                if kwargs:
                    for k, v in sorted(kwargs.items()):
                        try:
                            key_parts.append(f"{k}:{v}")
                        except:
                            key_parts.append(f"{k}:{hash(v)}")
                
                # Genera hash della chiave
                key = hashlib.md5(":".join(key_parts).encode()).hexdigest()
            
            # Cerca nella cache
            result = cache.get(key)
            if result is not None:
                return result
                
            # Esegui la funzione
            result = func(*args, **kwargs)
            
            # Salva in cache
            cache.set(key, result, ttl)
            
            return result
        return wrapper
    return decorator

# Aggiungiamo la funzione clear_cache a livello di modulo
def clear_cache(namespace: Optional[str] = None, cache_dir: Optional[str] = None, expired_only: bool = False) -> bool:
    """
    Pulisce la cache.
    
    Args:
        namespace: Nome specifico della cache da pulire, se None pulisce tutte le cache
        cache_dir: Directory per la cache, se None usa la directory predefinita
        expired_only: Se True, rimuove solo le voci scadute
        
    Returns:
        True se pulito con successo, False altrimenti
    """
    try:
        # Set default cache directory
        if not cache_dir:
            cache_dir = os.path.expanduser("~/football-predictions/cache")
            
        if not os.path.exists(cache_dir):
            logger.warning(f"Directory cache non trovata: {cache_dir}")
            return True
            
        success = False
        
        if expired_only:
            # Rimuovi solo le voci scadute
            logger.info("Pulizia cache: rimozione solo voci scadute")
            
            if namespace:
                # Pulisci solo una cache specifica
                disk_cache = DiskCache(namespace, cache_dir)
                disk_cache._cleanup()  # Rimuove voci scadute
                
                # Firebase cache
                if FIREBASE_AVAILABLE:
                    firebase_cache = FirebaseCache(namespace)
                    if firebase_cache.available:
                        firebase_cache._cleanup()
                
                logger.info(f"Voci scadute della cache '{namespace}' rimosse con successo")
            else:
                # Pulisci tutte le cache su disco
                try:
                    # Trova tutti i file .db nella directory cache
                    for file in os.listdir(cache_dir):
                        if file.endswith(".db"):
                            namespace = file[:-3]  # Rimuovi estensione .db
                            disk_cache = DiskCache(namespace, cache_dir)
                            disk_cache._cleanup()
                except Exception as e:
                    logger.error(f"Errore durante la pulizia delle cache scadute su disco: {str(e)}")
                
                # Pulisci cache scadute su Firebase
                if FIREBASE_AVAILABLE:
                    try:
                        # Pulisci tutte le cache Firebase
                        firebase_cache = FirebaseCache()
                        if firebase_cache.available:
                            # Ottieni tutte le sottodirectory di cache
                            ref = db.reference('cache')
                            namespaces = ref.get()
                            
                            if namespaces:
                                for ns in namespaces:
                                    firebase_cache = FirebaseCache(ns)
                                    firebase_cache._cleanup()
                    except Exception as e:
                        logger.error(f"Errore durante la pulizia delle cache scadute su Firebase: {str(e)}")
                
                logger.info("Voci scadute di tutte le cache rimosse con successo")
            
            success = True
        else:
            # Rimuovi tutte le voci
            if namespace:
                # Pulisce solo una cache specifica
                cache = MultiLevelCache(namespace, cache_dir=cache_dir)
                success = cache.clear()
                logger.info(f"Cache '{namespace}' pulita con successo: {success}")
            else:
                # Pulisce tutte le cache
                
                # 1. Pulisci cache in memoria
                memory_cache = MemoryCache()
                memory_success = memory_cache.clear()
                
                # 2. Pulisci cache su disco
                disk_success = False
                try:
                    # Rimuovi tutti i file .db nella directory cache
                    for file in os.listdir(cache_dir):
                        if file.endswith(".db"):
                            file_path = os.path.join(cache_dir, file)
                            os.remove(file_path)
                            logger.debug(f"Rimosso file cache: {file_path}")
                    disk_success = True
                except Exception as e:
                    logger.error(f"Errore durante la pulizia della cache su disco: {str(e)}")
                
                # 3. Pulisci cache Firebase
                firebase_success = False
                if FIREBASE_AVAILABLE:
                    try:
                        # Inizializza Firebase
                        firebase_cache = FirebaseCache()
                        if firebase_cache.available:
                            # Rimuovi il nodo cache
                            ref = db.reference('cache')
                            ref.delete()
                            firebase_success = True
                    except Exception as e:
                        logger.error(f"Errore durante la pulizia della cache Firebase: {str(e)}")
                
                success = memory_success or disk_success or firebase_success
                logger.info(f"Tutte le cache pulite con successo: {success}")
        
        return success
    except Exception as e:
        logger.error(f"Errore durante la pulizia della cache: {str(e)}")
        return False
