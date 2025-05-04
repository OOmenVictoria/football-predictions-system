"""
Pacchetto per la raccolta e l'elaborazione dei dati calcistici.
Questo pacchetto fornisce moduli per raccogliere dati da varie fonti (API, scraper, open data)
e processarli in un formato standardizzato per l'analisi.
"""
from typing import Dict, List, Any, Optional, Union
from functools import wraps
import threading
import logging

# Importa i sottopacchetti
from src.data.scrapers import (
    get_scraper,
    get_supported_features,
    get_scrapers_by_feature,
    get_best_scrapers_for,
    search_team,
    get_team_info,
    get_team_squad,
    get_league_table,
    get_league_matches,
    get_match_details,
    get_player_info,
    search_matches
)

from src.data.stats import (
    get_match_xg,
    get_team_xg,
    get_player_xg,
    get_match_stats,
    get_team_stats,
    get_player_stats,
    get_league_stats,
    get_player_ratings,
    aggregate_team_stats,
    get_comprehensive_match_analysis,
    get_best_source_for,
    get_supported_stats
)

from src.data.collector import DataCollector

# Logger per il package
logger = logging.getLogger(__name__)

# Istanza singleton del DataCollector
_collector_instance = None
_collector_lock = threading.Lock()

def get_collector() -> DataCollector:
    """Ottiene l'istanza singleton del DataCollector."""
    global _collector_instance
    
    if _collector_instance is None:
        with _collector_lock:
            if _collector_instance is None:
                _collector_instance = DataCollector()
    
    return _collector_instance

# Proxy functions per i metodi principali del DataCollector
def collect_matches(league_id: str, days_ahead: int = 7, days_behind: int = 3) -> List[Dict[str, Any]]:
    """
    Raccoglie le partite programmate e recenti per un campionato.
    
    Args:
        league_id: ID del campionato
        days_ahead: Giorni futuri da considerare
        days_behind: Giorni passati da considerare
        
    Returns:
        Lista di partite con dati completi
    """
    return get_collector().collect_matches(league_id, days_ahead, days_behind)

def collect_team_stats(team_id: str, detailed: bool = True) -> Dict[str, Any]:
    """
    Raccoglie statistiche dettagliate per una squadra.
    
    Args:
        team_id: ID della squadra
        detailed: Se recuperare statistiche dettagliate
        
    Returns:
        Dizionario con statistiche della squadra
    """
    return get_collector().collect_team_stats(team_id, detailed)

def collect_head_to_head(team1_id: str, team2_id: str, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Raccoglie lo storico degli scontri diretti tra due squadre.
    
    Args:
        team1_id: ID della prima squadra
        team2_id: ID della seconda squadra
        limit: Numero massimo di partite da recuperare
        
    Returns:
        Lista di partite tra le due squadre
    """
    return get_collector().collect_head_to_head(team1_id, team2_id, limit)

def collect_league_standings(league_id: str) -> Dict[str, Any]:
    """
    Raccoglie la classifica corrente per un campionato.
    
    Args:
        league_id: ID del campionato
        
    Returns:
        Dizionario con la classifica
    """
    return get_collector().collect_league_standings(league_id)

def collect_match_predictions(match_id: str) -> Dict[str, Any]:
    """
    Raccoglie pronostici per una partita specifica.
    
    Args:
        match_id: ID della partita
        
    Returns:
        Dizionario con pronostici
    """
    return get_collector().collect_match_predictions(match_id)

def collect_league_data(league_id: str) -> Dict[str, Any]:
    """
    Aggiorna tutti i dati relativi a un campionato.
    
    Args:
        league_id: ID del campionato
        
    Returns:
        Dizionario con informazioni sulle operazioni svolte
    """
    return get_collector().refresh_league_data(league_id)

def collect_all_leagues_data(active_only: bool = True) -> Dict[str, Any]:
    """
    Aggiorna i dati per tutti i campionati.
    
    Args:
        active_only: Se aggiornare solo i campionati attivi
        
    Returns:
        Dizionario con risultati per ogni campionato
    """
    return get_collector().refresh_all_leagues(active_only)

def collect_match_data(match_id: str) -> Dict[str, Any]:
    """
    Raccoglie tutti i dati necessari per una specifica partita.
    
    Args:
        match_id: ID della partita
        
    Returns:
        Dizionario con tutti i dati relativi alla partita
    """
    return get_collector().collect_data_for_match(match_id)

def get_error_report() -> List[Dict[str, Any]]:
    """
    Ottiene un report degli errori recenti nella raccolta dati.
    
    Returns:
        Lista di errori con timestamp e dettagli
    """
    return get_collector().get_error_report()

# Decorator per standardizzare le risposte
def standardize_data_response(func):
    """
    Decorator per standardizzare le risposte dalle funzioni di raccolta dati.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
            return {
                'success': True,
                'data': result,
                'error': None,
                'timestamp': get_current_datetime().isoformat()
            }
        except Exception as e:
            logger.error(f"Errore in {func.__name__}: {str(e)}")
            return {
                'success': False,
                'data': None,
                'error': str(e),
                'timestamp': get_current_datetime().isoformat()
            }
    return wrapper

# Funzioni standardizzate con decoratore
@standardize_data_response
def get_standardized_matches(league_id: str, days_ahead: int = 7, days_behind: int = 3):
    """Versione standardizzata di collect_matches."""
    return collect_matches(league_id, days_ahead, days_behind)

@standardize_data_response
def get_standardized_team_stats(team_id: str, detailed: bool = True):
    """Versione standardizzata di collect_team_stats."""
    return collect_team_stats(team_id, detailed)

@standardize_data_response
def get_standardized_head_to_head(team1_id: str, team2_id: str, limit: int = 10):
    """Versione standardizzata di collect_head_to_head."""
    return collect_head_to_head(team1_id, team2_id, limit)

@standardize_data_response
def get_standardized_league_standings(league_id: str):
    """Versione standardizzata di collect_league_standings."""
    return collect_league_standings(league_id)

@standardize_data_response
def get_standardized_match_predictions(match_id: str):
    """Versione standardizzata di collect_match_predictions."""
    return collect_match_predictions(match_id)

@standardize_data_response
def get_standardized_match_data(match_id: str):
    """Versione standardizzata di collect_match_data."""
    return collect_match_data(match_id)

# Utilitàper l'accesso intelligente ai dati
class DataAccessor:
    """
    Classe per l'accesso intelligente ai dati calcistici.
    Combina le funzionalità di DataCollector con quelle degli scraper e stats.
    """
    
    def __init__(self):
        self.collector = get_collector()
    
    def get_comprehensive_match_data(self, match_id: str) -> Dict[str, Any]:
        """
        Ottiene dati completi per una partita usando tutte le fonti disponibili.
        
        Args:
            match_id: ID della partita
            
        Returns:
            Dizionario con dati aggregati
        """
        # Raccogli dati base
        base_data = self.collector.collect_data_for_match(match_id)
        
        # Ottieni statistiche avanzate
        match_stats = get_match_stats(match_id, source="auto")
        match_xg = get_match_xg(match_id, source="auto")
        
        # Combina i dati
        comprehensive_data = {
            **base_data,
            'advanced_stats': match_stats,
            'xg_data': match_xg,
            'analysis': get_comprehensive_match_analysis(match_id)
        }
        
        return comprehensive_data
    
    def get_team_complete_profile(self, team_id: str) -> Dict[str, Any]:
        """
        Ottiene un profilo completo di una squadra.
        
        Args:
            team_id: ID della squadra
            
        Returns:
            Profilo completo della squadra
        """
        # Raccogli dati base
        team_data = self.collector.collect_team_stats(team_id, detailed=True)
        
        # Ottieni statistiche avanzate
        team_stats = get_team_stats(team_id, source="auto")
        team_xg = get_team_xg(team_id, source="auto")
        
        # Raccogli roster
        squad = get_team_squad(team_id)
        
        # Combina i dati
        complete_profile = {
            **team_data,
            'advanced_stats': team_stats,
            'xg_analysis': team_xg,
            'squad': squad,
        }
        
        return complete_profile
    
    def find_best_data_source(self, data_type: str, query: str) -> Dict[str, Any]:
        """
        Trova la migliore fonte di dati per un tipo specifico di query.
        
        Args:
            data_type: Tipo di dati (team, match, player)
            query: Query specifica
            
        Returns:
            Informazioni sulla migliore fonte
        """
        # Per team data
        if data_type == 'team':
            # Combina info da scrapers e stats
            scraper_info = get_best_scrapers_for('team_info')
            stats_info = get_best_source_for('team_stats')
            
            return {
                'scraper': scraper_info,
                'stats': stats_info,
                'recommendation': 'hybrid' if scraper_info and stats_info else 'single'
            }
        
        # Per match data
        elif data_type == 'match':
            scraper_info = get_best_scrapers_for('match_details')
            stats_info = get_best_source_for('match_stats')
            
            return {
                'scraper': scraper_info,
                'stats': stats_info,
                'recommendation': 'hybrid'
            }
        
        # Per player data
        elif data_type == 'player':
            scraper_info = get_best_scrapers_for('player_info')
            stats_info = get_best_source_for('player_stats')
            
            return {
                'scraper': scraper_info,
                'stats': stats_info,
                'recommendation': 'hybrid'
            }
        
        return {}

# Istanza globale per accesso rapido
data_accessor = DataAccessor()

# Funzioni di utility
def get_data_intelligently(entity_type: str, entity_id: str, **kwargs) -> Dict[str, Any]:
    """
    Ottiene dati per un'entità usando l'approccio migliore disponibile.
    
    Args:
        entity_type: Tipo di entità (match, team, player)
        entity_id: ID dell'entità
        
    Returns:
        Dati completi per l'entità
    """
    if entity_type == 'match':
        return data_accessor.get_comprehensive_match_data(entity_id)
    elif entity_type == 'team':
        return data_accessor.get_team_complete_profile(entity_id)
    elif entity_type == 'player':
        # Implementa logica per dati giocatore
        pass
    
    return {}

# Importa tutte le funzioni utili
from src.utils.time_utils import get_current_datetime

# Esporta tutte le funzioni principali
__all__ = [
    # DataCollector functions
    'collect_matches',
    'collect_team_stats',
    'collect_head_to_head',
    'collect_league_standings',
    'collect_match_predictions',
    'collect_league_data',
    'collect_all_leagues_data',
    'collect_match_data',
    'get_error_report',
    
    # Standardized functions
    'get_standardized_matches',
    'get_standardized_team_stats',
    'get_standardized_head_to_head',
    'get_standardized_league_standings',
    'get_standardized_match_predictions',
    'get_standardized_match_data',
    
    # Scraper functions (già definite nel modulo scrapers)
    'get_scraper',
    'get_supported_features',
    'get_scrapers_by_feature',
    'get_best_scrapers_for',
    'search_team',
    'get_team_info',
    'get_team_squad',
    'get_league_table',
    'get_league_matches',
    'get_match_details',
    'get_player_info',
    'search_matches',
    
    # Stats functions (già definite nel modulo stats)
    'get_match_xg',
    'get_team_xg',
    'get_player_xg',
    'get_match_stats',
    'get_team_stats',
    'get_player_stats',
    'get_league_stats',
    'get_player_ratings',
    'aggregate_team_stats',
    'get_comprehensive_match_analysis',
    'get_best_source_for',
    'get_supported_stats',
    
    # Utility functions
    'get_collector',
    'data_accessor',
    'get_data_intelligently',
    'DataAccessor'
]
