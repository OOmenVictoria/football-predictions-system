"""
Modulo per la gestione di Firebase nel sistema di pronostici calcistici.
Gestisce l'inizializzazione, la connessione e le operazioni su Firebase.
"""

import os
import json
import logging
import firebase_admin
from firebase_admin import credentials, db
from typing import Dict, List, Any, Optional, Union

# Configurazione logger
logger = logging.getLogger(__name__)

class FirebaseManager:
    """
    Gestore per l'interazione con Firebase.
    
    Fornisce un'interfaccia unificata per tutte le operazioni su Firebase,
    gestendo inizializzazione, cache, e riconnessioni.
    """
    
    _instance = None
    
    @classmethod
    def get_instance(cls):
        """
        Ottiene l'istanza singleton di FirebaseManager.
        
        Returns:
            Istanza di FirebaseManager
        """
        if cls._instance is None:
            cls._instance = FirebaseManager()
        return cls._instance
    
    def __init__(self):
        """
        Inizializza il manager Firebase.
        """
        self.app = None
        self.db_url = os.getenv('FIREBASE_DB_URL', '')
        self.service_account_path = os.getenv('FIREBASE_SERVICE_ACCOUNT', '')
        self.is_initialized = False
        self._initialize()
    
    def _initialize(self) -> bool:
        """
        Inizializza la connessione a Firebase.
        
        Returns:
            True se l'inizializzazione ha successo, False altrimenti
        """
        try:
            # Se l'app è già inizializzata, non fare nulla
            if self.is_initialized:
                return True
            
            # Verifica se l'app predefinita esiste già
            try:
                self.app = firebase_admin.get_app()
                self.is_initialized = True
                logger.info("Connessione a Firebase esistente rilevata")
                return True
            except ValueError:
                # L'app non esiste, continua con l'inizializzazione
                pass
            
            # Verifica che i parametri di configurazione siano disponibili
            if not self.db_url:
                logger.warning("URL del database Firebase non configurato")
                return False
            
            # Inizializza con credenziali se disponibili
            if self.service_account_path and os.path.exists(self.service_account_path):
                cred = credentials.Certificate(self.service_account_path)
                self.app = firebase_admin.initialize_app(
                    cred, {'databaseURL': self.db_url}
                )
            else:
                # Inizializza senza credenziali (per test o ambiente di sviluppo)
                self.app = firebase_admin.initialize_app(
                    None, {'databaseURL': self.db_url}
                )
            
            logger.info("Firebase inizializzato con successo")
            self.is_initialized = True
            return True
            
        except Exception as e:
            logger.error(f"Errore nell'inizializzazione di Firebase: {str(e)}")
            self.is_initialized = False
            return False
    
    def reinitialize(self) -> bool:
        """
        Tenta di reinizializzare Firebase in caso di errori.
        
        Returns:
            True se la reinizializzazione ha successo, False altrimenti
        """
        try:
            # Se c'è un'app esistente, eliminala
            if self.app:
                firebase_admin.delete_app(self.app)
                self.app = None
            
            # Resetta lo stato
            self.is_initialized = False
            
            # Tenta di inizializzare nuovamente
            return self._initialize()
            
        except Exception as e:
            logger.error(f"Errore nella reinizializzazione di Firebase: {str(e)}")
            return False
    
    def get_reference(self, path: str):
        """
        Ottiene un riferimento a un percorso nel database.
        
        Args:
            path: Percorso nel database
        
        Returns:
            Riferimento al percorso nel database
        """
        if not self.is_initialized and not self._initialize():
            logger.error(f"Impossibile ottenere riferimento per {path}: Firebase non inizializzato")
            return None
        
        try:
            # Sanitizza il percorso rimuovendo caratteri non consentiti
            # I percorsi Firebase non possono contenere ., $, #, [, ], / all'inizio/fine
            path = self._sanitize_path(path)
            return db.reference(path)
        except Exception as e:
            logger.error(f"Errore nell'ottenere riferimento per {path}: {str(e)}")
            return None
    
    def _sanitize_path(self, path: str) -> str:
        """
        Sanitizza un percorso Firebase rimuovendo caratteri non consentiti.
        
        Args:
            path: Percorso da sanitizzare
        
        Returns:
            Percorso sanitizzato
        """
        # Rimuovi . $ # [ ] / all'inizio e alla fine
        path = path.strip('.$#[]/')
        
        # Sostituisci caratteri non consentiti con '_'
        import re
        path = re.sub(r'[.$#\[\]]', '_', path)
        
        # Assicurati che non ci siano // nel percorso
        while '//' in path:
            path = path.replace('//', '/')
        
        # Rimuovi . all'inizio di ciascun componente del percorso
        parts = path.split('/')
        parts = [part[1:] if part.startswith('.') else part for part in parts]
        path = '/'.join(parts)
        
        # Se il percorso è vuoto, restituisci '/'
        if not path:
            return '/'
        
        return path
    
    def get(self, path: str) -> Any:
        """
        Legge dati da un percorso nel database.
        
        Args:
            path: Percorso nel database
        
        Returns:
            Dati letti dal database
        """
        ref = self.get_reference(path)
        if not ref:
            return None
        
        try:
            return ref.get()
        except Exception as e:
            logger.error(f"Errore nella lettura da {path}: {str(e)}")
            return None
    
    def set(self, path: str, data: Any) -> bool:
        """
        Scrive dati in un percorso nel database.
        
        Args:
            path: Percorso nel database
            data: Dati da scrivere
        
        Returns:
            True se l'operazione ha successo, False altrimenti
        """
        ref = self.get_reference(path)
        if not ref:
            return False
        
        try:
            ref.set(data)
            return True
        except Exception as e:
            logger.error(f"Errore nella scrittura su {path}: {str(e)}")
            return False
    
    def update(self, path: str, data: Dict[str, Any]) -> bool:
        """
        Aggiorna dati in un percorso nel database.
        
        Args:
            path: Percorso nel database
            data: Dati da aggiornare (dizionario)
        
        Returns:
            True se l'operazione ha successo, False altrimenti
        """
        ref = self.get_reference(path)
        if not ref:
            return False
        
        try:
            ref.update(data)
            return True
        except Exception as e:
            logger.error(f"Errore nell'aggiornamento di {path}: {str(e)}")
            return False
    
    def push(self, path: str, data: Any) -> Optional[str]:
        """
        Aggiunge dati a una lista in un percorso nel database.
        
        Args:
            path: Percorso nel database
            data: Dati da aggiungere
        
        Returns:
            ID dell'elemento aggiunto, None in caso di errore
        """
        ref = self.get_reference(path)
        if not ref:
            return None
        
        try:
            new_ref = ref.push(data)
            return new_ref.key
        except Exception as e:
            logger.error(f"Errore nell'aggiunta a {path}: {str(e)}")
            return None
    
    def delete(self, path: str) -> bool:
        """
        Elimina dati da un percorso nel database.
        
        Args:
            path: Percorso nel database
        
        Returns:
            True se l'operazione ha successo, False altrimenti
        """
        ref = self.get_reference(path)
        if not ref:
            return False
        
        try:
            ref.delete()
            return True
        except Exception as e:
            logger.error(f"Errore nell'eliminazione di {path}: {str(e)}")
            return False
    
    def test_connection(self) -> bool:
        """
        Testa la connessione a Firebase.
        
        Returns:
            True se il test ha successo, False altrimenti
        """
        try:
            # Prova a scrivere e leggere un valore di test
            test_path = 'cache/default/.test'
            test_value = {'timestamp': int(time.time())}
            
            # Assicurati che il percorso sia sanitizzato
            test_path = self._sanitize_path(test_path)
            
            # Scrivi il valore di test
            if not self.set(test_path, test_value):
                return False
            
            # Leggi il valore di test
            read_value = self.get(test_path)
            if not read_value or 'timestamp' not in read_value:
                return False
            
            # Pulisci
            self.delete(test_path)
            
            return True
            
        except Exception as e:
            logger.error(f"Errore nel test di connessione Firebase: {str(e)}")
            return False


# Singleton per un utilizzo più semplice
firebase_manager = FirebaseManager.get_instance()

def initialize_firebase():
    """
    Inizializza Firebase.
    
    Returns:
        True se l'inizializzazione ha successo, False altrimenti
    """
    return firebase_manager._initialize()

def get_firebase():
    """
    Ottiene l'istanza singleton di FirebaseManager.
    
    Returns:
        Istanza di FirebaseManager
    """
    return firebase_manager
