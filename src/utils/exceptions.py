"""
Modulo di definizione delle eccezioni personalizzate per il sistema di pronostici calcistici.
Fornisce classi di eccezione specifiche per gestire vari tipi di errori in modo strutturato.
"""

class FootballPredictionException(Exception):
    """Classe base per tutte le eccezioni del sistema di pronostici."""
    
    def __init__(self, message: str = "Si è verificato un errore nel sistema di pronostici."):
        self.message = message
        super().__init__(self.message)

class DataCollectionError(FootballPredictionException):
    """
    Eccezione sollevata quando si verificano errori durante la raccolta dati.
    Ad esempio errori API, problemi di rete, o dati mancanti.
    """
    
    def __init__(self, message: str = "Errore durante la raccolta dati.", source: str = None, details: dict = None):
        self.source = source  # Fonte dati che ha generato l'errore
        self.details = details or {}  # Dettagli aggiuntivi sull'errore
        
        if source:
            message = f"{message} [Fonte: {source}]"
            
        super().__init__(message)

class DatabaseConnectionError(FootballPredictionException):
    """
    Eccezione sollevata quando si verificano errori di connessione al database.
    Ad esempio credenziali errate, database non disponibile, ecc.
    """
    
    def __init__(self, message: str = "Errore di connessione al database.", db_type: str = None):
        self.db_type = db_type  # Tipo di database (Firebase, SQLite, ecc.)
        
        if db_type:
            message = f"{message} [DB: {db_type}]"
            
        super().__init__(message)

class InvalidConfigurationError(FootballPredictionException):
    """
    Eccezione sollevata quando la configurazione del sistema è invalida.
    Ad esempio API key mancanti, percorsi non validi, ecc.
    """
    
    def __init__(self, message: str = "Configurazione non valida.", config_key: str = None):
        self.config_key = config_key  # Chiave di configurazione problematica
        
        if config_key:
            message = f"{message} [Chiave: {config_key}]"
            
        super().__init__(message)

class PredictionModelError(FootballPredictionException):
    """
    Eccezione sollevata durante l'esecuzione dei modelli predittivi.
    Ad esempio dati insufficienti, errori matematici, ecc.
    """
    
    def __init__(self, message: str = "Errore nel modello predittivo.", model_name: str = None):
        self.model_name = model_name  # Nome del modello predittivo
        
        if model_name:
            message = f"{message} [Modello: {model_name}]"
            
        super().__init__(message)

class ContentGenerationError(FootballPredictionException):
    """
    Eccezione sollevata durante la generazione di contenuti testuali.
    Ad esempio template mancanti, errori di rendering, ecc.
    """
    
    def __init__(self, message: str = "Errore nella generazione contenuti.", content_type: str = None):
        self.content_type = content_type  # Tipo di contenuto (articolo, tweet, ecc.)
        
        if content_type:
            message = f"{message} [Tipo: {content_type}]"
            
        super().__init__(message)

class PublishingError(FootballPredictionException):
    """
    Eccezione sollevata durante la pubblicazione dei contenuti.
    Ad esempio errori API WordPress, problemi di autenticazione, ecc.
    """
    
    def __init__(self, message: str = "Errore nella pubblicazione contenuti.", platform: str = None):
        self.platform = platform  # Piattaforma di pubblicazione (WordPress, Twitter, ecc.)
        
        if platform:
            message = f"{message} [Piattaforma: {platform}]"
            
        super().__init__(message)

class ValidationError(FootballPredictionException):
    """
    Eccezione sollevata durante la validazione dei dati.
    Ad esempio formati non validi, tipi errati, vincoli non rispettati.
    """
    
    def __init__(self, message: str = "Errore di validazione dati.", field: str = None, value: str = None):
        self.field = field  # Campo validato
        self.value = value  # Valore non valido
        
        if field:
            if value is not None:
                message = f"{message} [Campo: {field}, Valore: {value}]"
            else:
                message = f"{message} [Campo: {field}]"
            
        super().__init__(message)

class CacheError(FootballPredictionException):
    """
    Eccezione sollevata durante l'utilizzo della cache.
    Ad esempio errori di serializzazione, cache corrotta, ecc.
    """
    
    def __init__(self, message: str = "Errore nella gestione della cache.", cache_type: str = None):
        self.cache_type = cache_type  # Tipo di cache (memoria, disco, Firebase)
        
        if cache_type:
            message = f"{message} [Cache: {cache_type}]"
            
        super().__init__(message)

class AuthenticationError(FootballPredictionException):
    """
    Eccezione sollevata durante l'autenticazione verso servizi esterni.
    Ad esempio token scaduti, credenziali errate, ecc.
    """
    
    def __init__(self, message: str = "Errore di autenticazione.", service: str = None):
        self.service = service  # Servizio verso cui ci si autentica
        
        if service:
            message = f"{message} [Servizio: {service}]"
            
        super().__init__(message)
