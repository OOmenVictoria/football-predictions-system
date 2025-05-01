"""
Processore per i dati delle squadre.

Questo modulo standardizza e arricchisce i dati delle squadre calcistiche
provenienti da diverse fonti, applicando pulizia, normalizzazione e aggregazione.
"""

import logging
import re
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union, Tuple

# API ufficiali
from src.data.api.football_data import get_api as get_football_data_api
from src.data.api.api_football import get_api as get_api_football

# Scraper per siti con statistiche
from src.data.stats.fbref import get_scraper as get_fbref
from src.data.stats.understat import get_scraper as get_understat
from src.data.stats.sofascore import get_scraper as get_sofascore
from src.data.stats.footystats import get_scraper as get_footystats
from src.data.stats.whoscored import get_scraper as get_whoscored

# Scraper per siti web generici
from src.data.scrapers.flashscore import get_scraper as get_flashscore
from src.data.scrapers.transfermarkt import get_scraper as get_transfermarkt
from src.data.scrapers.soccerway import get_scraper as get_soccerway

# Open data
from src.data.open_data.open_football import get_loader as get_open_football

from src.utils.database import FirebaseManager
from src.utils.cache import cached
from src.config.sources import get_sources_for_data_type, get_source_priority

# Configurazione logger
logger = logging.getLogger(__name__)

class TeamProcessor:
    """
    Processore per normalizzare e arricchire i dati delle squadre.
    
    Gestisce la standardizzazione dei dati da diverse fonti, la risoluzione
    dei conflitti, e l'arricchimento con dati aggiuntivi.
    """
    
    def __init__(self, db: Optional[FirebaseManager] = None):
        """
        Inizializza il processore di squadre.
        
        Args:
            db: Istanza di FirebaseManager. Se None, ne verrà creata una nuova.
        """
        self.db = db or FirebaseManager()
        
        # API ufficiali
        self.football_data_api = get_football_data_api()
        self.api_football = get_api_football()
        
        # Scraper per statistiche avanzate
        self.fbref = get_fbref()
        self.understat = get_understat()
        self.sofascore = get_sofascore()
        self.footystats = get_footystats()
        self.whoscored = get_whoscored()
        
        # Altri scraper
        self.flashscore = get_flashscore()
        self.transfermarkt = get_transfermarkt()
        self.soccerway = get_soccerway()
        
        # Open data
        self.open_football = get_open_football()
        
        # Cache per mappare nomi squadre
        self.team_name_cache = {}
        self.team_id_map = {}
        
        # Carica mappature ID squadre da Firebase
        self._load_team_id_mappings()
    
    def _load_team_id_mappings(self):
        """Carica le mappature degli ID delle squadre da Firebase."""
        try:
            mappings = self.db.get_reference("team_mappings")
            if mappings:
                self.team_id_map = mappings
                logger.info(f"Caricate {len(self.team_id_map)} mappature di ID squadre da Firebase")
        except Exception as e:
            logger.error(f"Errore nel caricare le mappature degli ID delle squadre: {e}")
    
    def process_team(
        self, 
        team_data: Dict[str, Any], 
        source: str,
        league_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Processa i dati di una singola squadra standardizzandoli.
        
        Args:
            team_data: Dati grezzi della squadra.
            source: Nome della fonte dei dati.
            league_id: ID del campionato (opzionale).
        
        Returns:
            Dizionario con i dati della squadra normalizzati.
        """
        try:
            # Oggetto squadra standardizzato
            standardized_team = {
                'team_id': '',
                'source': source,
                'source_ids': {},
                'name': '',
                'short_name': '',
                'code': '',
                'country': '',
                'founded': None,
                'logo_url': '',
                'primary_color': '',
                'secondary_color': '',
                'venue': {
                    'name': '',
                    'city': '',
                    'capacity': None
                },
                'coach': {
                    'name': '',
                    'nationality': ''
                },
                'current_league': {
                    'id': league_id or '',
                    'name': ''
                },
                'current_position': None,
                'url': '',
                'last_updated': datetime.now().isoformat()
            }
            
            # Normalizzazione in base alla fonte
            if source == 'football_data':
                self._normalize_football_data_team(team_data, standardized_team)
            elif source == 'api_football':
                self._normalize_api_football_team(team_data, standardized_team)
            elif source == 'fbref':
                self._normalize_fbref_team(team_data, standardized_team)
            elif source == 'understat':
                self._normalize_understat_team(team_data, standardized_team)
            elif source == 'sofascore':
                self._normalize_sofascore_team(team_data, standardized_team)
            elif source == 'flashscore':
                self._normalize_flashscore_team(team_data, standardized_team)
            elif source == 'transfermarkt':
                self._normalize_transfermarkt_team(team_data, standardized_team)
            elif source == 'footystats':
                self._normalize_footystats_team(team_data, standardized_team)
            elif source == 'whoscored':
                self._normalize_whoscored_team(team_data, standardized_team)
            elif source == 'soccerway':
                self._normalize_soccerway_team(team_data, standardized_team)
            elif source == 'open_football':
                self._normalize_open_football_team(team_data, standardized_team)
            else:
                logger.warning(f"Fonte non supportata: {source}")
            
            # Genera un team_id se non è presente
            if not standardized_team['team_id']:
                # Usa il nome normalizzato per creare un ID consistente
                normalized_name = self._normalize_team_name(standardized_team['name'])
                standardized_team['team_id'] = normalized_name.replace(' ', '_').lower()
            
            # Aggiungi ID della fonte nei source_ids
            source_id_field = self._get_source_id_field(source)
            if source_id_field in team_data:
                standardized_team['source_ids'][source] = str(team_data[source_id_field])
            
            return standardized_team
            
        except Exception as e:
            logger.error(f"Errore nel processare la squadra da {source}: {e}")
            return {
                'team_id': '',
                'source': source,
                'name': team_data.get('name', ''),
                'error': str(e),
                'raw_data': team_data
            }
    
    def _get_source_id_field(self, source: str) -> str:
        """
        Restituisce il nome del campo contenente l'ID per una data fonte.
        
        Args:
            source: Nome della fonte dei dati.
            
        Returns:
            Nome del campo ID per la fonte specificata.
        """
        id_fields = {
            'football_data': 'id',
            'api_football': 'id',
            'fbref': 'id',
            'understat': 'id',
            'sofascore': 'id',
            'flashscore': 'id',
            'transfermarkt': 'id',
            'footystats': 'id',
            'whoscored': 'teamId',
            'soccerway': 'id',
            'open_football': 'team_id'
        }
        
        return id_fields.get(source, 'id')
    
    def _normalize_football_data_team(
        self, 
        team_data: Dict[str, Any], 
        output: Dict[str, Any]
    ) -> None:
        """
        Normalizza i dati da Football-Data API.
        
        Args:
            team_data: Dati grezzi della squadra da Football-Data.
            output: Dizionario di output da popolare.
        """
        try:
            # ID squadra
            if 'id' in team_data:
                output['team_id'] = str(team_data['id'])
                output['source_ids']['football_data'] = str(team_data['id'])
            
            # Nome e short name
            if 'name' in team_data:
                output['name'] = team_data['name']
            
            if 'shortName' in team_data:
                output['short_name'] = team_data['shortName']
            elif 'name' in team_data:
                # Derivare un nome breve dal nome completo
                output['short_name'] = self._derive_short_name(team_data['name'])
            
            # Codice squadra (tipicamente 3 lettere)
            if 'tla' in team_data:
                output['code'] = team_data['tla']
            
            # Paese
            if 'area' in team_data and 'name' in team_data['area']:
                output['country'] = team_data['area']['name']
            
            # Logo URL
            if 'crest' in team_data:
                output['logo_url'] = team_data['crest']
            
            # Anno di fondazione (potrebbe non essere presente)
            if 'founded' in team_data:
                output['founded'] = team_data['founded']
            
            # Stadio
            if 'venue' in team_data:
                output['venue']['name'] = team_data['venue']
            
            # Colori (non sempre disponibili in Football-Data)
            if 'clubColors' in team_data:
                colors = team_data['clubColors'].split(' / ')
                if len(colors) > 0:
                    output['primary_color'] = colors[0]
                if len(colors) > 1:
                    output['secondary_color'] = colors[1]
            
            # Allenatore (non disponibile direttamente in Football-Data)
            
            # URL del sito web (se disponibile)
            if 'website' in team_data:
                output['url'] = team_data['website']
            
        except Exception as e:
            logger.error(f"Errore nella normalizzazione dei dati Football-Data: {e}")
    
    def _normalize_api_football_team(
        self, 
        team_data: Dict[str, Any], 
        output: Dict[str, Any]
    ) -> None:
        """
        Normalizza i dati da API-Football.
        
        Args:
            team_data: Dati grezzi della squadra da API-Football.
            output: Dizionario di output da popolare.
        """
        try:
            # ID squadra
            if 'id' in team_data:
                output['team_id'] = str(team_data['id'])
                output['source_ids']['api_football'] = str(team_data['id'])
            
            # Nome
            if 'name' in team_data:
                output['name'] = team_data['name']
            
            # Nome breve
            if 'code' in team_data and team_data['code']:
                output['short_name'] = team_data['code']
                output['code'] = team_data['code']
            elif 'name' in team_data:
                output['short_name'] = self._derive_short_name(team_data['name'])
            
            # Paese
            if 'country' in team_data:
                output['country'] = team_data['country']
            
            # Logo URL
            if 'logo' in team_data:
                output['logo_url'] = team_data['logo']
            
            # Anno di fondazione
            if 'founded' in team_data:
                output['founded'] = team_data['founded']
            
            # Stadio
            if 'venue' in team_data:
                venue_data = team_data['venue']
                if isinstance(venue_data, dict):
                    output['venue']['name'] = venue_data.get('name', '')
                    output['venue']['city'] = venue_data.get('city', '')
                    
                    if 'capacity' in venue_data and venue_data['capacity']:
                        try:
                            output['venue']['capacity'] = int(venue_data['capacity'])
                        except:
                            pass
            
            # Informazioni nazionali
            if 'national' in team_data:
                output['is_national_team'] = team_data['national']
            
        except Exception as e:
            logger.error(f"Errore nella normalizzazione dei dati API-Football: {e}")
    
    def _normalize_fbref_team(
        self, 
        team_data: Dict[str, Any], 
        output: Dict[str, Any]
    ) -> None:
        """
        Normalizza i dati da FBref.
        
        Args:
            team_data: Dati grezzi della squadra da FBref.
            output: Dizionario di output da popolare.
        """
        try:
            # ID squadra (in FBref potrebbero essere URL o ID numerici)
            if 'id' in team_data:
                output['team_id'] = str(team_data['id'])
                output['source_ids']['fbref'] = str(team_data['id'])
            elif 'url' in team_data:
                # Estrai l'ID dall'URL
                match = re.search(r'/teams/([^/]+)/', team_data['url'])
                if match:
                    output['source_ids']['fbref'] = match.group(1)
            
            # Nome
            if 'name' in team_data:
                output['name'] = team_data['name']
            
            # Nome breve (potrebbe non essere disponibile in FBref)
            if 'short_name' in team_data:
                output['short_name'] = team_data['short_name']
            elif 'name' in team_data:
                output['short_name'] = self._derive_short_name(team_data['name'])
            
            # Paese
            if 'country' in team_data:
                output['country'] = team_data['country']
            
            # URL del team
            if 'url' in team_data:
                output['url'] = team_data['url']
            
            # Lega corrente
            if 'league' in team_data:
                if isinstance(team_data['league'], dict):
                    output['current_league']['name'] = team_data['league'].get('name', '')
                    output['current_league']['id'] = str(team_data['league'].get('id', ''))
                else:
                    output['current_league']['name'] = str(team_data['league'])
            
            # Posizione attuale
            if 'position' in team_data and team_data['position']:
                try:
                    output['current_position'] = int(team_data['position'])
                except:
                    output['current_position'] = team_data['position']
            
            # Statistiche (se disponibili)
            if 'stats' in team_data and team_data['stats']:
                output['statistics'] = team_data['stats']
            
        except Exception as e:
            logger.error(f"Errore nella normalizzazione dei dati FBref: {e}")
    
    def _normalize_understat_team(
        self, 
        team_data: Dict[str, Any], 
        output: Dict[str, Any]
    ) -> None:
        """
        Normalizza i dati da Understat.
        
        Args:
            team_data: Dati grezzi della squadra da Understat.
            output: Dizionario di output da popolare.
        """
        try:
            # ID squadra
            if 'id' in team_data:
                output['team_id'] = str(team_data['id'])
                output['source_ids']['understat'] = str(team_data['id'])
            
            # Nome
            if 'title' in team_data:
                output['name'] = team_data['title']
            elif 'name' in team_data:
                output['name'] = team_data['name']
            
            # Nome breve
            if 'short_name' in team_data:
                output['short_name'] = team_data['short_name']
            elif 'title' in team_data:
                output['short_name'] = self._derive_short_name(team_data['title'])
            elif 'name' in team_data:
                output['short_name'] = self._derive_short_name(team_data['name'])
            
            # Statistiche specifiche Understat (xG)
            if 'xG' in team_data or 'xGA' in team_data:
                output['expected_goals'] = {
                    'xG': float(team_data.get('xG', 0)),
                    'xGA': float(team_data.get('xGA', 0)),
                    'xpts': float(team_data.get('xpts', 0)),
                    'npxG': float(team_data.get('npxG', 0))
                }
            
            # Lega corrente
            if 'league' in team_data:
                output['current_league']['name'] = team_data['league']
            
        except Exception as e:
            logger.error(f"Errore nella normalizzazione dei dati Understat: {e}")
    
    def _normalize_sofascore_team(
        self, 
        team_data: Dict[str, Any], 
        output: Dict[str, Any]
    ) -> None:
        """
        Normalizza i dati da SofaScore.
        
        Args:
            team_data: Dati grezzi della squadra da SofaScore.
            output: Dizionario di output da popolare.
        """
        try:
            # ID squadra
            if 'id' in team_data:
                output['team_id'] = str(team_data['id'])
                output['source_ids']['sofascore'] = str(team_data['id'])
            
            # Nome
            if 'name' in team_data:
                output['name'] = team_data['name']
            
            # Nome breve
            if 'shortName' in team_data:
                output['short_name'] = team_data['shortName']
            elif 'name' in team_data:
                output['short_name'] = self._derive_short_name(team_data['name'])
            
            # Codice (spesso disponibile in SofaScore)
            if 'slug' in team_data:
                output['code'] = team_data['slug'].upper()[:3]
            
            # Paese
            if 'country' in team_data:
                if isinstance(team_data['country'], dict):
                    output['country'] = team_data['country'].get('name', '')
                else:
                    output['country'] = team_data['country']
            
            # Logo URL
            if 'logo' in team_data:
                output['logo_url'] = team_data['logo']
            
            # Colori (SofaScore a volte li fornisce)
            if 'primaryColor' in team_data:
                output['primary_color'] = team_data['primaryColor']
            if 'secondaryColor' in team_data:
                output['secondary_color'] = team_data['secondaryColor']
            
            # Allenatore
            if 'manager' in team_data and team_data['manager']:
                mgr_data = team_data['manager']
                output['coach']['name'] = mgr_data.get('name', '')
                if 'country' in mgr_data and isinstance(mgr_data['country'], dict):
                    output['coach']['nationality'] = mgr_data['country'].get('name', '')
            
            # Posizione attuale
            if 'position' in team_data and team_data['position']:
                try:
                    output['current_position'] = int(team_data['position'])
                except:
                    pass
            
            # Lega corrente
            if 'tournament' in team_data:
                if isinstance(team_data['tournament'], dict):
                    output['current_league']['name'] = team_data['tournament'].get('name', '')
                    output['current_league']['id'] = str(team_data['tournament'].get('id', ''))
            
            # Statistiche
            if 'statistics' in team_data and team_data['statistics']:
                output['statistics'] = team_data['statistics']
            
        except Exception as e:
            logger.error(f"Errore nella normalizzazione dei dati SofaScore: {e}")
    
    def _normalize_flashscore_team(
        self, 
        team_data: Dict[str, Any], 
        output: Dict[str, Any]
    ) -> None:
        """
        Normalizza i dati da Flashscore.
        
        Args:
            team_data: Dati grezzi della squadra da Flashscore.
            output: Dizionario di output da popolare.
        """
        try:
            # ID squadra (in Flashscore potrebbe essere un valore alfanumerico o URL)
            if 'id' in team_data:
                output['team_id'] = str(team_data['id'])
                output['source_ids']['flashscore'] = str(team_data['id'])
            
            # Nome
            if 'name' in team_data:
                output['name'] = team_data['name']
            
            # Nome breve
            if 'short_name' in team_data:
                output['short_name'] = team_data['short_name']
            elif 'name' in team_data:
                output['short_name'] = self._derive_short_name(team_data['name'])
            
            # Paese
            if 'country' in team_data:
                output['country'] = team_data['country']
            
            # Logo URL
            if 'logo' in team_data:
                output['logo_url'] = team_data['logo']
            
            # Lega corrente
            if 'league' in team_data:
                output['current_league']['name'] = team_data['league']
            
            # URL
            if 'url' in team_data:
                output['url'] = team_data['url']
            
            # Posizione in classifica
            if 'position' in team_data and team_data['position']:
                try:
                    output['current_position'] = int(team_data['position'])
                except:
                    pass
            
            # Statistiche (se disponibili)
            if 'stats' in team_data and team_data['stats']:
                output['statistics'] = team_data['stats']
            
            # Forma recente
            if 'form' in team_data:
                output['form'] = team_data['form']
            
        except Exception as e:
            logger.error(f"Errore nella normalizzazione dei dati Flashscore: {e}")
    
    def _normalize_transfermarkt_team(
        self, 
        team_data: Dict[str, Any], 
        output: Dict[str, Any]
    ) -> None:
        """
        Normalizza i dati da Transfermarkt.
        
        Args:
            team_data: Dati grezzi della squadra da Transfermarkt.
            output: Dizionario di output da popolare.
        """
        try:
            # ID squadra
            if 'id' in team_data:
                output['team_id'] = str(team_data['id'])
                output['source_ids']['transfermarkt'] = str(team_data['id'])
            
            # Nome
            if 'name' in team_data:
                output['name'] = team_data['name']
            
            # Nome breve
            if 'short_name' in team_data:
                output['short_name'] = team_data['short_name']
            elif 'name' in team_data:
                output['short_name'] = self._derive_short_name(team_data['name'])
            
            # Paese
            if 'country' in team_data:
                output['country'] = team_data['country']
            
            # Logo URL
            if 'logo' in team_data:
                output['logo_url'] = team_data['logo']
            
            # Anno di fondazione
            if 'founded' in team_data:
                try:
                    output['founded'] = int(team_data['founded'])
                except:
                    pass
            
            # Stadio
            if 'stadium' in team_data:
                if isinstance(team_data['stadium'], dict):
                    output['venue']['name'] = team_data['stadium'].get('name', '')
                    output['venue']['capacity'] = team_data['stadium'].get('capacity')
                else:
                    output['venue']['name'] = team_data['stadium']
            
            # Allenatore
            if 'coach' in team_data:
                if isinstance(team_data['coach'], dict):
                    output['coach']['name'] = team_data['coach'].get('name', '')
                    output['coach']['nationality'] = team_data['coach'].get('nationality', '')
                else:
                    output['coach']['name'] = team_data['coach']
            
            # URL
            if 'url' in team_data:
                output['url'] = team_data['url']
            
            # Valore di mercato (dato specifico Transfermarkt)
            if 'market_value' in team_data:
                output['market_value'] = team_data['market_value']
            
            # Dimensione della rosa
            if 'squad_size' in team_data:
                output['squad_size'] = team_data['squad_size']
            
            # Età media
            if 'average_age' in team_data:
                output['average_age'] = team_data['average_age']
            
            # Valore totale della rosa
            if 'total_value' in team_data:
                output['total_value'] = team_data['total_value']
            
        except Exception as e:
            logger.error(f"Errore nella normalizzazione dei dati Transfermarkt: {e}")
    
    def _normalize_footystats_team(
        self, 
        team_data: Dict[str, Any], 
        output: Dict[str, Any]
    ) -> None:
        """
        Normalizza i dati da FootyStats.
        
        Args:
            team_data: Dati grezzi della squadra da FootyStats.
            output: Dizionario di output da popolare.
        """
        try:
            # ID squadra
            if 'id' in team_data:
                output['team_id'] = str(team_data['id'])
                output['source_ids']['footystats'] = str(team_data['id'])
            
            # Nome
            if 'name' in team_data:
                output['name'] = team_data['name']
            
            # Nome breve
            if 'short_name' in team_data:
                output['short_name'] = team_data['short_name']
            elif 'name' in team_data:
                output['short_name'] = self._derive_short_name(team_data['name'])
            
            # Paese
            if 'country' in team_data:
                output['country'] = team_data['country']
            
            # Posizione attuale
            if 'position' in team_data and team_data['position']:
                try:
                    output['current_position'] = int(team_data['position'])
                except:
                    pass
            
            # Lega corrente
            if 'league' in team_data:
                output['current_league']['name'] = team_data['league']
            
            # Statistiche (FootyStats è ricco di statistiche)
            if 'stats' in team_data and team_data['stats']:
                output['statistics'] = team_data['stats']
            
            # Forma
            if 'form' in team_data:
                output['form'] = team_data['form']
            
        except Exception as e:
            logger.error(f"Errore nella normalizzazione dei dati FootyStats: {e}")
    
    def _normalize_whoscored_team(
        self, 
        team_data: Dict[str, Any], 
        output: Dict[str, Any]
    ) -> None:
        """
        Normalizza i dati da WhoScored.
        
        Args:
            team_data: Dati grezzi della squadra da WhoScored.
            output: Dizionario di output da popolare.
        """
        try:
            # ID squadra
            if 'teamId' in team_data:
                output['team_id'] = str(team_data['teamId'])
                output['source_ids']['whoscored'] = str(team_data['teamId'])
            
            # Nome
            if 'name' in team_data:
                output['name'] = team_data['name']
            
            # Nome breve
            if 'shortName' in team_data:
                output['short_name'] = team_data['shortName']
            elif 'name' in team_data:
                output['short_name'] = self._derive_short_name(team_data['name'])
            
            # Regione (potrebbe essere il paese)
            if 'region' in team_data:
                if isinstance(team_data['region'], dict):
                    output['country'] = team_data['region'].get('name', '')
                else:
                    output['country'] = team_data['region']
            
            # Logo URL
            if 'logoUrl' in team_data:
                output['logo_url'] = team_data['logoUrl']
            
            # Statistiche (WhoScored è ricco di statistiche)
            if 'stats' in team_data and team_data['stats']:
                output['statistics'] = team_data['stats']
            elif 'statistics' in team_data and team_data['statistics']:
                output['statistics'] = team_data['statistics']
            
        except Exception as e:
            logger.error(f"Errore nella normalizzazione dei dati WhoScored: {e}")
    
    def _normalize_soccerway_team(
        self, 
        team_data: Dict[str, Any], 
        output: Dict[str, Any]
    ) -> None:
        """
        Normalizza i dati da Soccerway.
        
        Args:
            team_data: Dati grezzi della squadra da Soccerway.
            output: Dizionario di output da popolare.
        """
        try:
            # ID squadra
            if 'id' in team_data:
                output['team_id'] = str(team_data['id'])
                output['source_ids']['soccerway'] = str(team_data['id'])
            
            # Nome
            if 'name' in team_data:
                output['name'] = team_data['name']
            
            # Nome breve
            if 'short_name' in team_data:
                output['short_name'] = team_data['short_name']
            elif 'name' in team_data:
                output['short_name'] = self._derive_short_name(team_data['name'])
            
            # Paese
            if 'country' in team_data:
                output['country'] = team_data['country']
            
            # Logo URL
            if 'logo' in team_data:
                output['logo_url'] = team_data['logo']
            
            # Stadio
            if 'stadium' in team_data:
                output['venue']['name'] = team_data['stadium']
            
            # URL
            if 'url' in team_data:
                output['url'] = team_data['url']
            
            # Posizione
            if 'position' in team_data and team_data['position']:
                try:
                    output['current_position'] = int(team_data['position'])
                except:
                    pass
            
            # Lega corrente
            if 'league' in team_data:
                output['current_league']['name'] = team_data['league']
            
        except Exception as e:
            logger.error(f"Errore nella normalizzazione dei dati Soccerway: {e}")
    
    def _normalize_open_football_team(
        self, 
        team_data: Dict[str, Any], 
        output: Dict[str, Any]
    ) -> None:
        """
        Normalizza i dati da OpenFootball.
        
        Args:
            team_data: Dati grezzi della squadra da OpenFootball.
            output: Dizionario di output da popolare.
        """
        try:
            # ID squadra (OpenFootball potrebbe non avere ID specifici)
            if 'team_id' in team_data:
                output['team_id'] = str(team_data['team_id'])
                output['source_ids']['open_football'] = str(team_data['team_id'])
            
            # Nome
            if 'name' in team_data:
                output['name'] = team_data['name']
            elif 'team' in team_data:
                output['name'] = team_data['team']
            
            # Nome breve (non sempre disponibile in OpenFootball)
            if 'code' in team_data:
                output['short_name'] = team_data['code']
                output['code'] = team_data['code']
            elif 'name' in team_data:
                output['short_name'] = self._derive_short_name(team_data['name'])
            elif 'team' in team_data:
                output['short_name'] = self._derive_short_name(team_data['team'])
            
            # Paese (potrebbe essere derivato dal percorso del file)
            if 'country' in team_data:
                output['country'] = team_data['country']
            
            # Città (OpenFootball spesso include la città)
            if 'city' in team_data:
                output['venue']['city'] = team_data['city']
            
        except Exception as e:
            logger.error(f"Errore nella normalizzazione dei dati OpenFootball: {e}")
    
    def _derive_short_name(self, full_name: str) -> str:
        """
        Deriva un nome breve dal nome completo della squadra.
        
        Args:
            full_name: Nome completo della squadra.
            
        Returns:
            Nome breve derivato.
        """
        # Lista di prefissi comuni da rimuovere
        prefixes = ['FC ', 'AC ', 'SS ', 'AS ', 'CF ', 'CD ', 'SC ', 'RC ', 'Real ', 'Sporting ', 'Club ']
        
        # Rimuovi prefissi se presenti
        short_name = full_name
        for prefix in prefixes:
            if short_name.startswith(prefix):
                short_name = short_name[len(prefix):]
                break
        
        # Se il nome è troppo lungo, prendi solo la prima parola
        if len(short_name) > 12 and ' ' in short_name:
            short_name = short_name.split(' ')[0]
        
        return short_name
    
    def _normalize_team_name(self, name: str) -> str:
        """
        Normalizza il nome della squadra per ottenere un formato standard.
        
        Args:
            name: Nome originale della squadra.
            
        Returns:
            Nome normalizzato.
        """
        if not name:
            return ''
        
        # Converti in minuscolo e rimuovi caratteri speciali
        normalized = name.lower()
        normalized = re.sub(r'[^\w\s]', '', normalized)
        
        # Rimuovi prefissi comuni
        prefixes = ['fc ', 'ac ', 'ss ', 'as ', 'cf ', 'cd ', 'sc ', 'rc ']
        for prefix in prefixes:
            if normalized.startswith(prefix):
                normalized = normalized[len(prefix):]
                break
        
        # Rimuovi spazi extra
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        
        # Converti in formato con iniziali maiuscole
        return ' '.join(word.capitalize() for word in normalized.split())
    
    def merge_team_data(
        self, 
        team_id: str, 
        sources: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Unisce i dati della squadra da diverse fonti.
        
        Args:
            team_id: ID interno della squadra.
            sources: Lista di dizionari con dati della squadra da diverse fonti.
        
        Returns:
            Dizionario con i dati uniti della squadra.
        """
        if not sources:
            logger.warning(f"Nessuna fonte dati fornita per la squadra {team_id}")
            return {'team_id': team_id, 'error': 'No data sources provided'}
        
        # Usa i dati del primo dizionario come base
        merged_data = sources[0].copy()
        
        # Se c'è solo una fonte, restituisci i dati così come sono
        if len(sources) == 1:
            return merged_data
        
        # Altrimenti, unisci i dati dalle altre fonti
        for source_data in sources[1:]:
            self._merge_team_fields(merged_data, source_data)
        
        # Aggiorna timestamp
        merged_data['last_updated'] = datetime.now().isoformat()
        
        return merged_data
    
    def _merge_team_fields(
        self, 
        target: Dict[str, Any], 
        source: Dict[str, Any]
    ) -> None:
        """
        Unisce i campi di una seconda fonte nel dizionario target.
        
        Args:
            target: Dizionario target dove unire i dati.
            source: Dizionario sorgente con i dati da unire.
        """
        # IDs della fonte
        if 'source_ids' in source and source['source_ids']:
            for source_key, source_id in source['source_ids'].items():
                if source_key not in target['source_ids'] or not target['source_ids'][source_key]:
                    target['source_ids'][source_key] = source_id
        
        # Campi semplici
        simple_fields = [
            'name', 'short_name', 'code', 'country', 'founded', 
            'logo_url', 'primary_color', 'secondary_color', 'url'
        ]
        
        for field in simple_fields:
            if field in source and source[field] and (field not in target or not target[field]):
                target[field] = source[field]
        
        # Stadio
        if 'venue' in source and source['venue']:
            for field, value in source['venue'].items():
                if not target['venue'][field] and value:
                    target['venue'][field] = value
        
        # Allenatore
        if 'coach' in source and source['coach']:
            for field, value in source['coach'].items():
                if not target['coach'][field] and value:
                    target['coach'][field] = value
        
        # Lega corrente
        if 'current_league' in source and source['current_league']:
            for field, value in source['current_league'].items():
                if not target['current_league'][field] and value:
                    target['current_league'][field] = value
        
        # Posizione
        if 'current_position' in source and source['current_position'] is not None and (
                'current_position' not in target or target['current_position'] is None):
            target['current_position'] = source['current_position']
        
        # Statistiche (unisci se le chiavi sono diverse)
        if 'statistics' in source and source['statistics'] and isinstance(source['statistics'], dict):
            if 'statistics' not in target or not target['statistics']:
                target['statistics'] = {}
            
            for stat_key, stat_value in source['statistics'].items():
                if stat_key not in target['statistics']:
                    target['statistics'][stat_key] = stat_value
        
        # Expected Goals data (specifico per Understat)
        if 'expected_goals' in source and source['expected_goals']:
            if 'expected_goals' not in target:
                target['expected_goals'] = {}
            
            for key, value in source['expected_goals'].items():
                if key not in target['expected_goals']:
                    target['expected_goals'][key] = value
        
        # Valore di mercato (specifico per Transfermarkt)
        if 'market_value' in source and source['market_value'] and (
                'market_value' not in target or not target['market_value']):
            target['market_value'] = source['market_value']
        
        # Forma recente
        if 'form' in source and source['form'] and ('form' not in target or not target['form']):
            target['form'] = source['form']
    
    def enrich_team_data(self, team_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Arricchisce i dati della squadra con informazioni aggiuntive.
        
        Args:
            team_data: Dati della squadra da arricchire.
        
        Returns:
            Dizionario con i dati della squadra arricchiti.
        """
        # Copia i dati originali
        enriched_data = team_data.copy()
        
        try:
            # Logo se non presente
            if not enriched_data.get('logo_url') and enriched_data.get('name'):
                try:
                    # Cerca su Transfermarkt
                    if 'transfermarkt' not in enriched_data['source_ids']:
                        team_name = enriched_data['name']
                        search_results = self.transfermarkt.search_team(team_name)
                        
                        if search_results and len(search_results) > 0:
                            first_result = search_results[0]
                            if 'logo' in first_result:
                                enriched_data['logo_url'] = first_result['logo']
                                enriched_data['source_ids']['transfermarkt'] = str(first_result.get('id', ''))
                except Exception as e:
                    logger.debug(f"Errore nel cercare logo su Transfermarkt: {e}")
            
            # Valore di mercato se non presente
            if not enriched_data.get('market_value') and 'source_ids' in enriched_data:
                try:
                    # Cerca su Transfermarkt
                    if 'transfermarkt' in enriched_data['source_ids']:
                        team_id = enriched_data['source_ids']['transfermarkt']
                        team_details = self.transfermarkt.get_team_details(team_id)
                        
                        if team_details and 'market_value' in team_details:
                            enriched_data['market_value'] = team_details['market_value']
                            
                            # Aggiungi anche dettagli della rosa se disponibili
                            if 'squad_size' in team_details:
                                enriched_data['squad_size'] = team_details['squad_size']
                            if 'average_age' in team_details:
                                enriched_data['average_age'] = team_details['average_age']
                except Exception as e:
                    logger.debug(f"Errore nel cercare valore di mercato: {e}")
            
            # Statistiche Expected Goals (xG) se non presenti
            if not enriched_data.get('expected_goals') and 'source_ids' in enriched_data:
                try:
                    # Cerca su Understat
                    if 'understat' in enriched_data['source_ids']:
                        team_id = enriched_data['source_ids']['understat']
                        team_stats = self.understat.get_team_stats(team_id)
                        
                        if team_stats and 'xG' in team_stats:
                            enriched_data['expected_goals'] = {
                                'xG': float(team_stats.get('xG', 0)),
                                'xGA': float(team_stats.get('xGA', 0)),
                                'xpts': float(team_stats.get('xpts', 0)),
                                'npxG': float(team_stats.get('npxG', 0))
                            }
                except Exception as e:
                    logger.debug(f"Errore nel cercare statistiche xG: {e}")
            
            # Forma recente se non presente
            if not enriched_data.get('form') and 'source_ids' in enriched_data:
                try:
                    # Cerca su Flashscore
                    if 'name' in enriched_data:
                        team_name = enriched_data['name']
                        team_matches = self.flashscore.get_team_matches(team_name, limit=5)
                        
                        if team_matches:
                            form = []
                            for match in team_matches:
                                is_home = match.get('home_team') == team_name
                                score = match.get('score', {})
                                
                                if score:
                                    home_score = int(score.get('home', 0))
                                    away_score = int(score.get('away', 0))
                                    
                                    if is_home:
                                        if home_score > away_score:
                                            form.append('W')
                                        elif home_score < away_score:
                                            form.append('L')
                                        else:
                                            form.append('D')
                                    else:
                                        if away_score > home_score:
                                            form.append('W')
                                        elif away_score < home_score:
                                            form.append('L')
                                        else:
                                            form.append('D')
                            
                            if form:
                                enriched_data['form'] = ''.join(form)
                except Exception as e:
                    logger.debug(f"Errore nel calcolare la forma recente: {e}")
            
            # Aggiorna timestamp
            enriched_data['last_updated'] = datetime.now().isoformat()
            
            return enriched_data
            
        except Exception as e:
            logger.error(f"Errore nell'arricchimento dei dati della squadra {team_data.get('team_id', 'unknown')}: {e}")
            return team_data  # Restituisci i dati originali in caso di errore
    
    def store_team_data(self, team_data: Dict[str, Any]) -> bool:
        """
        Salva i dati della squadra nel database.
        
        Args:
            team_data: Dati della squadra da salvare.
        
        Returns:
            True se salvato con successo, False altrimenti.
        """
        try:
            if not team_data.get('team_id'):
                logger.error("Impossibile salvare squadra senza team_id")
                return False
            
            # Costruisci il percorso nel database
            team_id = team_data['team_id']
            
            # Aggiungi timestamp se non presente
            if 'last_updated' not in team_data:
                team_data['last_updated'] = datetime.now().isoformat()
            
            # Salva i dati
            self.db.set_reference(f"teams/{team_id}", team_data)
            
            # Aggiorna anche le mappature degli ID
            self._update_team_id_mappings(team_data)
            
            logger.info(f"Dati della squadra {team_id} salvati con successo")
            return True
            
        except Exception as e:
            logger.error(f"Errore nel salvare i dati della squadra: {e}")
            return False
    
    def _update_team_id_mappings(self, team_data: Dict[str, Any]) -> None:
        """
        Aggiorna le mappature degli ID delle squadre.
        
        Args:
            team_data: Dati della squadra con gli ID da mappare.
        """
        try:
            if 'source_ids' not in team_data or not team_data['source_ids']:
                return
            
            team_id = team_data['team_id']
            
            # Aggiorna le mappature
            for source, source_id in team_data['source_ids'].items():
                if source and source_id:
                    mapping_key = f"{source}:{source_id}"
                    self.team_id_map[mapping_key] = team_id
            
            # Salva le mappature aggiornate
            self.db.set_reference("team_mappings", self.team_id_map)
            
        except Exception as e:
            logger.error(f"Errore nell'aggiornare le mappature degli ID delle squadre: {e}")
    
    def get_stored_team_data(self, team_id: str) -> Optional[Dict[str, Any]]:
        """
        Recupera i dati della squadra dal database.
        
        Args:
            team_id: ID della squadra.
        
        Returns:
            Dizionario con i dati della squadra, o None se non trovato.
        """
        try:
            # Recupera i dati
            team_data = self.db.get_reference(f"teams/{team_id}")
            return team_data
            
        except Exception as e:
            logger.error(f"Errore nel recuperare i dati della squadra {team_id}: {e}")
            return None
    
    def find_team_by_name(self, team_name: str) -> Optional[Dict[str, Any]]:
        """
        Cerca una squadra per nome.
        
        Args:
            team_name: Nome della squadra da cercare.
        
        Returns:
            Dizionario con i dati della squadra, o None se non trovato.
        """
        try:
            # Normalizza il nome per la ricerca
            normalized_name = self._normalize_team_name(team_name)
            
            # Controlla la cache
            if normalized_name in self.team_name_cache:
                team_id = self.team_name_cache[normalized_name]
                return self.get_stored_team_data(team_id)
            
            # Altrimenti cerca nel database
            teams = self.db.get_reference("teams")
            if not teams:
                return None
            
            # Cerca la squadra con il nome più simile
            best_match = None
            best_score = 0
            
            for team_id, team_data in teams.items():
                if 'name' in team_data:
                    current_name = self._normalize_team_name(team_data['name'])
                    
                    # Calcola la somiglianza tra i nomi
                    score = self._name_similarity(normalized_name, current_name)
                    
                    if score > best_score and score > 0.8:  # Soglia di somiglianza
                        best_score = score
                        best_match = team_data
            
            # Aggiorna la cache se trovato
            if best_match and 'team_id' in best_match:
                self.team_name_cache[normalized_name] = best_match['team_id']
            
            return best_match
            
        except Exception as e:
            logger.error(f"Errore nella ricerca della squadra per nome '{team_name}': {e}")
            return None
    
    def find_team_by_source_id(self, source: str, source_id: str) -> Optional[Dict[str, Any]]:
        """
        Cerca una squadra per ID da una fonte specifica.
        
        Args:
            source: Nome della fonte dei dati.
            source_id: ID della squadra nella fonte.
        
        Returns:
            Dizionario con i dati della squadra, o None se non trovato.
        """
        try:
            # Controlla la mappa degli ID
            mapping_key = f"{source}:{source_id}"
            if mapping_key in self.team_id_map:
                team_id = self.team_id_map[mapping_key]
                return self.get_stored_team_data(team_id)
            
            # Altrimenti cerca manualmente
            teams = self.db.get_reference("teams")
            if not teams:
                return None
            
            for team_id, team_data in teams.items():
                if 'source_ids' in team_data and source in team_data['source_ids']:
                    if team_data['source_ids'][source] == source_id:
                        # Aggiorna la mappa per future ricerche
                        self.team_id_map[mapping_key] = team_id
                        self.db.set_reference(f"team_mappings/{mapping_key}", team_id)
                        return team_data
            
            return None
            
        except Exception as e:
            logger.error(f"Errore nella ricerca della squadra con ID '{source}:{source_id}': {e}")
            return None
    
    def _name_similarity(self, name1: str, name2: str) -> float:
        """
        Calcola la somiglianza tra due nomi di squadre.
        
        Args:
            name1: Primo nome.
            name2: Secondo nome.
        
        Returns:
            Valore di somiglianza tra 0 e 1.
        """
        # Implementazione semplice basata sulla lunghezza della sottostringa comune più lunga
        if not name1 or not name2:
            return 0
        
        name1 = name1.lower()
        name2 = name2.lower()
        
        # Se i nomi sono identici
        if name1 == name2:
            return 1.0
        
        # Trova la sottostringa comune più lunga
        len1, len2 = len(name1), len(name2)
        matrix = [[0 for _ in range(len2 + 1)] for _ in range(len1 + 1)]
        max_length = 0
        
        for i in range(1, len1 + 1):
            for j in range(1, len2 + 1):
                if name1[i-1] == name2[j-1]:
                    matrix[i][j] = matrix[i-1][j-1] + 1
                    max_length = max(max_length, matrix[i][j])
        
        # Calcola la somiglianza come rapporto tra la lunghezza della sottostringa comune
        # e la lunghezza media dei due nomi
        avg_length = (len1 + len2) / 2
        return max_length / avg_length if avg_length > 0 else 0


# Istanza globale per un utilizzo più semplice
team_processor = TeamProcessor()

def get_processor():
    """
    Ottiene l'istanza globale del processore.
    
    Returns:
        Istanza di TeamProcessor.
    """
    return team_processor
