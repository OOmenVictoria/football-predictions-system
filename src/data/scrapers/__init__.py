"""
Package per gli scraper web per l'estrazione di dati calcistici.
Questo package fornisce implementazioni di vari scraper per estrarre
dati da siti web di statistiche calcistiche.
"""
from typing import Dict, List, Any, Optional, Union
from functools import wraps
import logging

# Import di tutte le classi scraper
from src.data.scrapers.base_scraper import BaseScraper
from src.data.scrapers.flashscore import FlashScoreScraper, get_scraper as get_flashscore_scraper
from src.data.scrapers.soccerway import SoccerwayScraper, get_scraper as get_soccerway_scraper
from src.data.scrapers.worldfootball import WorldFootballScraper, get_scraper as get_worldfootball_scraper
from src.data.scrapers.transfermarkt import TransfermarktScraper, get_scraper as get_transfermarkt_scraper
from src.data.scrapers.wikipedia import WikipediaScraper, get_scraper as get_wikipedia_scraper
from src.data.scrapers.eleven_v_eleven import ElevenVElevenScraper, get_scraper as get_elevenvelevencraper

logger = logging.getLogger(__name__)

# Registry di tutti gli scraper disponibili
SCRAPER_REGISTRY = {
    'flashscore': FlashScoreScraper,
    'soccerway': SoccerwayScraper,
    'worldfootball': WorldFootballScraper,
    'transfermarkt': TransfermarktScraper,
    'wikipedia': WikipediaScraper,
    'eleven_v_eleven': ElevenVElevenScraper
}

# Cache delle istanze singleton
_scraper_instances = {}

class ScraperError(Exception):
    """Errore generico per le operazioni degli scraper."""
    pass

class ScraperResponse:
    """Risposta standardizzata per tutti gli scraper."""
    
    def __init__(self, success: bool = True, data: Any = None, error: Optional[str] = None):
        self.success = success
        self.data = data
        self.error = error
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'success': self.success,
            'data': self.data,
            'error': self.error
        }

def standardize_response(func):
    """
    Decorator per standardizzare le risposte dei metodi degli scraper.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
            return ScraperResponse(success=True, data=result)
        except Exception as e:
            logger.error(f"Errore in {func.__name__}: {str(e)}")
            return ScraperResponse(success=False, error=str(e))
    return wrapper

def get_scraper(scraper_type: str) -> BaseScraper:
    """
    Ottiene un'istanza singleton dello scraper specificato.
    
    Args:
        scraper_type: Tipo di scraper ('flashscore', 'soccerway', ecc.)
    
    Returns:
        Istanza dello scraper richiesto
    
    Raises:
        ValueError: Se il tipo di scraper non è supportato
    """
    if scraper_type not in SCRAPER_REGISTRY:
        available_types = ', '.join(SCRAPER_REGISTRY.keys())
        raise ValueError(f"Tipo di scraper non supportato: {scraper_type}. Disponibili: {available_types}")
    
    # Verifica se l'istanza è già nella cache
    if scraper_type not in _scraper_instances:
        # Usa il metodo get_scraper specifico se disponibile
        if scraper_type == 'flashscore':
            _scraper_instances[scraper_type] = get_flashscore_scraper()
        elif scraper_type == 'soccerway':
            _scraper_instances[scraper_type] = get_soccerway_scraper()
        elif scraper_type == 'worldfootball':
            _scraper_instances[scraper_type] = get_worldfootball_scraper()
        elif scraper_type == 'transfermarkt':
            _scraper_instances[scraper_type] = get_transfermarkt_scraper()
        elif scraper_type == 'wikipedia':
            _scraper_instances[scraper_type] = get_wikipedia_scraper()
        elif scraper_type == 'eleven_v_eleven':
            _scraper_instances[scraper_type] = get_elevenvelevencraper()
        else:
            # Fallback per creare una nuova istanza
            _scraper_instances[scraper_type] = SCRAPER_REGISTRY[scraper_type]()
    
    return _scraper_instances[scraper_type]

# Metodi standardizzati per accesso rapido a funzionalità comuni
@standardize_response
def search_team(scraper_type: str, team_name: str, **kwargs) -> Dict[str, Any]:
    """
    Cerca una squadra utilizzando lo scraper specificato.
    
    Args:
        scraper_type: Tipo di scraper da utilizzare
        team_name: Nome della squadra da cercare
        **kwargs: Parametri aggiuntivi specifici dello scraper
    
    Returns:
        Risultati della ricerca
    """
    scraper = get_scraper(scraper_type)
    
    if hasattr(scraper, 'search_team'):
        return scraper.search_team(team_name, **kwargs)
    elif hasattr(scraper, 'search'):
        return scraper.search(team_name, **kwargs)
    else:
        raise ScraperError(f"Lo scraper {scraper_type} non supporta la ricerca squadre")

@standardize_response
def get_team_info(scraper_type: str, team_id: str, **kwargs) -> Dict[str, Any]:
    """
    Ottiene informazioni su una squadra.
    
    Args:
        scraper_type: Tipo di scraper da utilizzare
        team_id: ID della squadra
        **kwargs: Parametri aggiuntivi specifici dello scraper
    
    Returns:
        Informazioni sulla squadra
    """
    scraper = get_scraper(scraper_type)
    
    if hasattr(scraper, 'get_team_info'):
        return scraper.get_team_info(team_id, **kwargs)
    else:
        raise ScraperError(f"Lo scraper {scraper_type} non supporta il recupero info squadre")

@standardize_response
def get_team_squad(scraper_type: str, team_id: str, **kwargs) -> List[Dict[str, Any]]:
    """
    Ottiene la rosa di una squadra.
    
    Args:
        scraper_type: Tipo di scraper da utilizzare
        team_id: ID della squadra
        **kwargs: Parametri aggiuntivi specifici dello scraper
    
    Returns:
        Lista di giocatori
    """
    scraper = get_scraper(scraper_type)
    
    if hasattr(scraper, 'get_team_squad'):
        return scraper.get_team_squad(team_id, **kwargs)
    else:
        raise ScraperError(f"Lo scraper {scraper_type} non supporta il recupero rosa squadre")

@standardize_response
def get_league_table(scraper_type: str, league_id: str, season: Optional[str] = None, **kwargs) -> Dict[str, Any]:
    """
    Ottiene la classifica di un campionato.
    
    Args:
        scraper_type: Tipo di scraper da utilizzare
        league_id: ID del campionato
        season: Stagione (opzionale)
        **kwargs: Parametri aggiuntivi specifici dello scraper
    
    Returns:
        Classifica del campionato
    """
    scraper = get_scraper(scraper_type)
    
    if hasattr(scraper, 'get_league_table'):
        return scraper.get_league_table(league_id, season, **kwargs)
    else:
        raise ScraperError(f"Lo scraper {scraper_type} non supporta il recupero classifiche")

@standardize_response
def get_league_matches(scraper_type: str, league_id: str, season: Optional[str] = None, **kwargs) -> List[Dict[str, Any]]:
    """
    Ottiene le partite di un campionato.
    
    Args:
        scraper_type: Tipo di scraper da utilizzare
        league_id: ID del campionato
        season: Stagione (opzionale)
        **kwargs: Parametri aggiuntivi specifici dello scraper
    
    Returns:
        Lista di partite
    """
    scraper = get_scraper(scraper_type)
    
    # Vari nomi possibili per questo metodo
    method_names = ['get_league_matches', 'get_league_fixtures', 'get_matches']
    
    for method_name in method_names:
        if hasattr(scraper, method_name):
            method = getattr(scraper, method_name)
            return method(league_id, season, **kwargs)
    
    raise ScraperError(f"Lo scraper {scraper_type} non supporta il recupero partite campionato")

@standardize_response
def get_match_details(scraper_type: str, match_id: str, **kwargs) -> Dict[str, Any]:
    """
    Ottiene dettagli di una partita.
    
    Args:
        scraper_type: Tipo di scraper da utilizzare
        match_id: ID della partita
        **kwargs: Parametri aggiuntivi specifici dello scraper
    
    Returns:
        Dettagli della partita
    """
    scraper = get_scraper(scraper_type)
    
    if hasattr(scraper, 'get_match_details'):
        return scraper.get_match_details(match_id, **kwargs)
    else:
        raise ScraperError(f"Lo scraper {scraper_type} non supporta il recupero dettagli partite")

@standardize_response
def get_player_info(scraper_type: str, player_id: str, **kwargs) -> Dict[str, Any]:
    """
    Ottiene informazioni su un giocatore.
    
    Args:
        scraper_type: Tipo di scraper da utilizzare
        player_id: ID del giocatore
        **kwargs: Parametri aggiuntivi specifici dello scraper
    
    Returns:
        Informazioni sul giocatore
    """
    scraper = get_scraper(scraper_type)
    
    if hasattr(scraper, 'get_player_info'):
        return scraper.get_player_info(player_id, **kwargs)
    else:
        raise ScraperError(f"Lo scraper {scraper_type} non supporta il recupero info giocatori")

@standardize_response
def search_matches(scraper_type: str, query: str, **kwargs) -> List[Dict[str, Any]]:
    """
    Cerca partite utilizzando lo scraper specificato.
    
    Args:
        scraper_type: Tipo di scraper da utilizzare
        query: Query di ricerca
        **kwargs: Parametri aggiuntivi specifici dello scraper
    
    Returns:
        Lista di partite trovate
    """
    scraper = get_scraper(scraper_type)
    
    # Vari metodi possibili per la ricerca partite
    method_names = ['search_matches', 'search', 'get_matches_by_date']
    
    for method_name in method_names:
        if hasattr(scraper, method_name):
            method = getattr(scraper, method_name)
            return method(query, **kwargs)
    
    raise ScraperError(f"Lo scraper {scraper_type} non supporta la ricerca partite")

# Mappatura delle funzionalità supportate per ogni scraper
SCRAPER_FEATURES = {
    'flashscore': {
        'search_team': True,
        'team_info': True,
        'league_table': True,
        'league_matches': True,
        'match_details': True,
        'player_info': False,
        'transfer_history': False,
        'market_value': False,
        'head_to_head': True,
        'wikipedia_search': False
    },
    'soccerway': {
        'search_team': True,
        'team_info': True,
        'league_table': True,
        'league_matches': True,
        'match_details': True,
        'player_info': False,
        'transfer_history': False,
        'market_value': False,
        'head_to_head': False,
        'wikipedia_search': False
    },
    'worldfootball': {
        'search_team': False,
        'team_info': True,
        'league_table': True,
        'league_matches': True,
        'match_details': True,
        'player_info': True,
        'transfer_history': False,
        'market_value': False,
        'head_to_head': False,
        'wikipedia_search': False
    },
    'transfermarkt': {
        'search_team': True,
        'team_info': True,
        'league_table': False,
        'league_matches': False,
        'match_details': False,
        'player_info': True,
        'transfer_history': True,
        'market_value': True,
        'head_to_head': False,
        'wikipedia_search': False
    },
    'wikipedia': {
        'search_team': True,
        'team_info': True,
        'league_table': False,
        'league_matches': False,
        'match_details': False,
        'player_info': True,
        'transfer_history': False,
        'market_value': False,
        'head_to_head': True,
        'wikipedia_search': True
    },
    'eleven_v_eleven': {
        'search_team': False,
        'team_info': True,
        'league_table': True,
        'league_matches': True,
        'match_details': False,
        'player_info': True,
        'transfer_history': False,
        'market_value': False,
        'head_to_head': True,
        'wikipedia_search': False
    }
}

def get_supported_features(scraper_type: str) -> Dict[str, bool]:
    """
    Ottiene le funzionalità supportate da uno scraper.
    
    Args:
        scraper_type: Tipo di scraper
    
    Returns:
        Dizionario con le funzionalità supportate
    """
    return SCRAPER_FEATURES.get(scraper_type, {})

def get_scrapers_by_feature(feature: str) -> List[str]:
    """
    Ottiene la lista di scraper che supportano una funzionalità specifica.
    
    Args:
        feature: Nome della funzionalità
    
    Returns:
        Lista di tipi di scraper che supportano la funzionalità
    """
    return [
        scraper_type for scraper_type, features in SCRAPER_FEATURES.items()
        if features.get(feature, False)
    ]

@standardize_response
def get_best_scrapers_for(query_type: str, **kwargs) -> List[str]:
    """
    Raccomanda i migliori scraper per un tipo di query specifico.
    
    Args:
        query_type: Tipo di query ('team', 'league', 'player', ecc.)
        **kwargs: Parametri aggiuntivi per affinare la raccomandazione
    
    Returns:
        Lista di scraper raccomandati in ordine di preferenza
    """
    recommendations = {
        'team_info': ['soccerway', 'worldfootball', 'transfermarkt', 'wikipedia'],
        'league_table': ['worldfootball', 'soccerway', 'eleven_v_eleven'],
        'league_matches': ['worldfootball', 'soccerway', 'flashscore', 'eleven_v_eleven'],
        'player_info': ['transfermarkt', 'worldfootball', 'wikipedia'],
        'match_details': ['flashscore', 'soccerway', 'worldfootball'],
        'transfer_data': ['transfermarkt'],
        'market_values': ['transfermarkt'],
        'head_to_head': ['eleven_v_eleven', 'wikipedia', 'flashscore'],
        'search': ['soccerway', 'transfermarkt', 'wikipedia', 'flashscore']
    }
    
    # Aggiungi criteri di filtraggio se necessario
    if 'country' in kwargs:
        country = kwargs['country'].lower()
        if country == 'italy':
            # Ordina per priorità i siti con buona copertura italiana
            return recommendations.get(query_type, ['soccerway', 'worldfootball', 'flashscore'])
    
    return recommendations.get(query_type, ['soccerway', 'flashscore', 'worldfootball'])

# Alias per retrocompatibilità
FlashScoreScraper = FlashScoreScraper
SoccerwayScraper = SoccerwayScraper
WorldFootballScraper = WorldFootballScraper
TransfermarktScraper = TransfermarktScraper
WikipediaScraper = WikipediaScraper
ElevenVElevenScraper = ElevenVElevenScraper
