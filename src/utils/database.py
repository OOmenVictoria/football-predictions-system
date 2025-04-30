"""
Modulo di utilità per l'interazione con Firebase Realtime Database.
Gestisce connessione, lettura, scrittura, aggiornamento e altri pattern comuni.
"""
import os
import json
import logging
import time
from typing import Dict, List, Any, Optional, Union, Callable
from datetime import datetime
import threading

# Gestione conditional import per Firebase
try:
    import firebase_admin
    from firebase_admin import credentials, db
    FIREBASE_AVAILABLE = True
except ImportError:
    FIREBASE_AVAILABLE = False

# Configurazione logging
logger = logging.getLogger(__name__)

class FirebaseManager:
    """
    Gestisce l'interazione con Firebase Realtime Database.
    Fornisce metodi per inizializzazione, lettura, scrittura, operazioni batch e altro.
    """
    
    # Singleton instance
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls, *args, **kwargs):
        """Implementazione Singleton per garantire una sola istanza."""
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(FirebaseManager, cls).__new__(cls)
                cls._instance._initialized = False
            return cls._instance
    
    def __init__(self, app_name: str = None):
        """
        Inizializza il manager Firebase.
        
        Args:
            app_name: Nome opzionale dell'app Firebase
        """
        # Se già inizializzato, evita re-inizializzazione
        if self._initialized:
            return
            
        # Imposta attributi
        self.app_name = app_name
        self.firebase_app = None
        self.available = FIREBASE_AVAILABLE
        
        # Tentativo di inizializzazione
        self._initialized = self._initialize()
    
    def _initialize(self) -> bool:
        """
        Inizializza la connessione a Firebase.
        
        Returns:
            True se l'inizializzazione ha avuto successo, False altrimenti
        """
        if not FIREBASE_AVAILABLE:
            logger.warning("Firebase non disponibile (firebase_admin non installato)")
            return False
            
        try:
            # Verifica se un'app è già inizializzata
            try:
                self.firebase_app = firebase_admin.get_app(self.app_name)
                logger.info(f"Firebase già inizializzato con app: {self.app_name}")
                return True
            except ValueError:
                # Nessuna app esistente, procedi con l'inizializzazione
                pass
                
            # Ottieni credenziali
            cred = self._get_credentials()
            if not cred:
                logger.error("Impossibile ottenere credenziali Firebase")
                return False
                
            # Database URL
            db_url = os.getenv('FIREBASE_DB_URL')
            if not db_url:
                logger.error("FIREBASE_DB_URL non impostato nelle variabili d'ambiente")
                return False
                
            # Inizializza app
            self.firebase_app = firebase_admin.initialize_app(
                credential=cred,
                options={'databaseURL': db_url},
                name=self.app_name
            )
            
            # Verifica connessione
            test_ref = db.reference('connection_test')
            test_ref.set({"timestamp": datetime.now().isoformat()})
            test_ref.delete()
            
            logger.info("Firebase inizializzato con successo")
            return True
            
        except Exception as e:
            logger.error(f"Errore nell'inizializzazione di Firebase: {str(e)}")
            self.available = False
            return False
    
    def _get_credentials(self) -> Optional[credentials.Certificate]:
        """
        Ottiene le credenziali per l'autenticazione Firebase.
        
        Returns:
            Oggetto Certificate o None se non trovato
        """
        try:
            # Verifica credenziali nelle variabili d'ambiente
            cred_json = os.getenv('FIREBASE_CREDENTIALS')
            if cred_json:
                try:
                    # Prova a interpretare come JSON
                    cred_dict = json.loads(cred_json)
                    return credentials.Certificate(cred_dict)
                except json.JSONDecodeError:
                    # Potrebbe essere un path al file
                    if os.path.exists(cred_json):
                        return credentials.Certificate(cred_json)
                    else:
                        logger.error(f"File credenziali non trovato: {cred_json}")
                        return None
            
            # Fallback: cerca nel percorso standard
            cred_path = os.path.expanduser('~/football-predictions/creds/firebase-credentials.json')
            if os.path.exists(cred_path):
                return credentials.Certificate(cred_path)
            else:
                logger.error(f"File credenziali non trovato: {cred_path}")
                return None
                
        except Exception as e:
            logger.error(f"Errore recupero credenziali Firebase: {str(e)}")
            return None
    
    def is_available(self) -> bool:
        """
        Verifica se Firebase è disponibile e inizializzato.
        
        Returns:
            True se Firebase è disponibile, False altrimenti
        """
        return self.available and self._initialized
    
    def get_reference(self, path: str) -> Optional[Any]:
        """
        Ottiene un riferimento a un percorso nel database.
        
        Args:
            path: Percorso nel database
            
        Returns:
            Oggetto Reference o None se non disponibile
        """
        if not self.is_available():
            return None
            
        try:
            return db.reference(path)
        except Exception as e:
            logger.error(f"Errore creazione reference Firebase: {str(e)}")
            return None
    
    def get(self, path: str, default: Any = None) -> Any:
        """
        Legge dati da un percorso nel database.
        
        Args:
            path: Percorso nel database
            default: Valore da restituire in caso di errore
            
        Returns:
            Dati dal database o valore default
        """
        ref = self.get_reference(path)
        if not ref:
            return default
            
        try:
            return ref.get() or default
        except Exception as e:
            logger.error(f"Errore lettura Firebase [{path}]: {str(e)}")
            return default
    
    def set(self, path: str, data: Any) -> bool:
        """
        Scrive dati in un percorso nel database, sovrascrivendo eventuali dati esistenti.
        
        Args:
            path: Percorso nel database
            data: Dati da scrivere
            
        Returns:
            True se l'operazione ha avuto successo, False altrimenti
        """
        ref = self.get_reference(path)
        if not ref:
            return False
            
        try:
            ref.set(data)
            return True
        except Exception as e:
            logger.error(f"Errore scrittura Firebase [{path}]: {str(e)}")
            return False
    
    def update(self, path: str, data: Dict) -> bool:
        """
        Aggiorna dati in un percorso nel database, preservando i dati non menzionati.
        
        Args:
            path: Percorso nel database
            data: Dizionario con i dati da aggiornare
            
        Returns:
            True se l'operazione ha avuto successo, False altrimenti
        """
        ref = self.get_reference(path)
        if not ref:
            return False
            
        try:
            ref.update(data)
            return True
        except Exception as e:
            logger.error(f"Errore aggiornamento Firebase [{path}]: {str(e)}")
            return False
    
    def push(self, path: str, data: Any) -> Optional[str]:
        """
        Aggiunge dati a una lista con chiave generata automaticamente.
        
        Args:
            path: Percorso nel database
            data: Dati da aggiungere
            
        Returns:
            Chiave generata o None in caso di errore
        """
        ref = self.get_reference(path)
        if not ref:
            return None
            
        try:
            new_ref = ref.push(data)
            return new_ref.key
        except Exception as e:
            logger.error(f"Errore push Firebase [{path}]: {str(e)}")
            return None
    
    def delete(self, path: str) -> bool:
        """
        Elimina dati da un percorso nel database.
        
        Args:
            path: Percorso nel database
            
        Returns:
            True se l'operazione ha avuto successo, False altrimenti
        """
        ref = self.get_reference(path)
        if not ref:
            return False
            
        try:
            ref.delete()
            return True
        except Exception as e:
            logger.error(f"Errore eliminazione Firebase [{path}]: {str(e)}")
            return False
    
    def exists(self, path: str) -> bool:
        """
        Verifica l'esistenza di un percorso nel database.
        
        Args:
            path: Percorso nel database
            
        Returns:
            True se il percorso esiste, False altrimenti
        """
        return self.get(path) is not None
    
    def query(self, path: str, order_by: str = None, equal_to: Any = None, 
              start_at: Any = None, end_at: Any = None, limit: int = None) -> Optional[Dict]:
        """
        Esegue una query avanzata sul database.
        
        Args:
            path: Percorso nel database
            order_by: Attributo per ordinamento
            equal_to: Valore di uguaglianza
            start_at: Valore iniziale range
            end_at: Valore finale range
            limit: Numero massimo di risultati
            
        Returns:
            Risultati query o None in caso di errore
        """
        ref = self.get_reference(path)
        if not ref:
            return None
            
        try:
            # Costruisci query
            query_ref = ref
            
            if order_by:
                query_ref = query_ref.order_by_child(order_by)
            
            if equal_to is not None:
                query_ref = query_ref.equal_to(equal_to)
                
            if start_at is not None:
                query_ref = query_ref.start_at(start_at)
                
            if end_at is not None:
                query_ref = query_ref.end_at(end_at)
                
            if limit:
                if isinstance(limit, int) and limit > 0:
                    query_ref = query_ref.limit_to_first(limit)
            
            # Esegui query
            return query_ref.get()
            
        except Exception as e:
            logger.error(f"Errore query Firebase [{path}]: {str(e)}")
            return None
    
    def transaction(self, path: str, update_fn: Callable[[Any], Any]) -> bool:
        """
        Esegue un aggiornamento atomico tramite transazione.
        
        Args:
            path: Percorso nel database
            update_fn: Funzione che riceve il valore corrente e restituisce il nuovo valore
            
        Returns:
            True se la transazione ha avuto successo, False altrimenti
        """
        ref = self.get_reference(path)
        if not ref:
            return False
            
        try:
            ref.transaction(update_fn)
            return True
        except Exception as e:
            logger.error(f"Errore transazione Firebase [{path}]: {str(e)}")
            return False
    
    def save_with_timestamp(self, path: str, data: Dict) -> bool:
        """
        Salva dati aggiungendo automaticamente timestamp.
        
        Args:
            path: Percorso nel database
            data: Dati da salvare
            
        Returns:
            True se l'operazione ha avuto successo, False altrimenti
        """
        if not isinstance(data, dict):
            data = {"value": data}
            
        # Aggiungi timestamp
        data.update({
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        })
        
        return self.set(path, data)
    
    def update_with_timestamp(self, path: str, data: Dict) -> bool:
        """
        Aggiorna dati aggiornando automaticamente il timestamp.
        
        Args:
            path: Percorso nel database
            data: Dati da aggiornare
            
        Returns:
            True se l'operazione ha avuto successo, False altrimenti
        """
        if not isinstance(data, dict):
            return False
            
        # Aggiungi timestamp di aggiornamento
        data.update({
            "updated_at": datetime.now().isoformat()
        })
        
        return self.update(path, data)
    
    def batch_update(self, updates: Dict[str, Any]) -> bool:
        """
        Esegue aggiornamenti multipli in batch.
        
        Args:
            updates: Dizionario di {percorso: valore}
            
        Returns:
            True se l'operazione ha avuto successo, False altrimenti
        """
        if not self.is_available():
            return False
            
        try:
            # Ottieni la reference root
            root_ref = db.reference('/')
            
            # Esegui batch update
            root_ref.update(updates)
            return True
        except Exception as e:
            logger.error(f"Errore batch update Firebase: {str(e)}")
            return False
    
    def listen(self, path: str, callback: Callable[[Dict], None]) -> bool:
        """
        Registra un listener per ascoltare cambiamenti in tempo reale.
        
        Args:
            path: Percorso nel database
            callback: Funzione da chiamare con i nuovi dati
            
        Returns:
            True se il listener è stato registrato, False altrimenti
        """
        ref = self.get_reference(path)
        if not ref:
            return False
            
        try:
            # Registra listener
            ref.listen(callback)
            return True
        except Exception as e:
            logger.error(f"Errore registrazione listener Firebase [{path}]: {str(e)}")
            return False

# Istanza globale
firebase = FirebaseManager()

# Espone funzioni di alto livello
def init_firebase(app_name: str = None) -> bool:
    """
    Inizializza Firebase con configurazione specificata.
    
    Args:
        app_name: Nome opzionale dell'app Firebase
        
    Returns:
        True se l'inizializzazione ha avuto successo, False altrimenti
    """
    return FirebaseManager(app_name).is_available()

def get_firebase() -> FirebaseManager:
    """
    Ottiene l'istanza globale del manager Firebase.
    
    Returns:
        Istanza FirebaseManager
    """
    return firebase

def is_firebase_available() -> bool:
    """
    Verifica se Firebase è disponibile.
    
    Returns:
        True se Firebase è disponibile, False altrimenti
    """
    return firebase.is_available()

def get_data(path: str, default: Any = None) -> Any:
    """
    Legge dati da un percorso nel database.
    
    Args:
        path: Percorso nel database
        default: Valore da restituire in caso di errore
        
    Returns:
        Dati dal database o valore default
    """
    return firebase.get(path, default)

def set_data(path: str, data: Any) -> bool:
    """
    Scrive dati in un percorso nel database.
    
    Args:
        path: Percorso nel database
        data: Dati da scrivere
        
    Returns:
        True se l'operazione ha avuto successo, False altrimenti
    """
    return firebase.set(path, data)

def update_data(path: str, data: Dict) -> bool:
    """
    Aggiorna dati in un percorso nel database.
    
    Args:
        path: Percorso nel database
        data: Dizionario con i dati da aggiornare
        
    Returns:
        True se l'operazione ha avuto successo, False altrimenti
    """
    return firebase.update(path, data)

def push_data(path: str, data: Any) -> Optional[str]:
    """
    Aggiunge dati a una lista con chiave generata automaticamente.
    
    Args:
        path: Percorso nel database
        data: Dati da aggiungere
        
    Returns:
        Chiave generata o None in caso di errore
    """
    return firebase.push(path, data)

def delete_data(path: str) -> bool:
    """
    Elimina dati da un percorso nel database.
    
    Args:
        path: Percorso nel database
        
    Returns:
        True se l'operazione ha avuto successo, False altrimenti
    """
    return firebase.delete(path)

def query_data(path: str, **kwargs) -> Optional[Dict]:
    """
    Esegue una query avanzata sul database.
    
    Args:
        path: Percorso nel database
        **kwargs: Parametri di query (order_by, equal_to, start_at, end_at, limit)
        
    Returns:
        Risultati query o None in caso di errore
    """
    return firebase.query(path, **kwargs)
