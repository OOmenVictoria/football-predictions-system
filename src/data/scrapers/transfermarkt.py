"""
Modulo per l'estrazione di dati da Transfermarkt.
Questo modulo fornisce funzionalità per estrarre dati di mercato, valori giocatori, 
e altre informazioni non strettamente statistiche dal sito Transfermarkt.
"""
import re
import json
import time
import logging
import unicodedata
from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime

import requests
from bs4 import BeautifulSoup

from src.data.scrapers.base_scraper import BaseScraper
from src.utils.cache import cached
from src.utils.time_utils import parse_date
from src.config.settings import get_setting

logger = logging.getLogger(__name__)

class TransfermarktScraper(BaseScraper):
    """
    Scraper per estrarre dati da Transfermarkt.
    
    Questo scraper permette di ottenere:
    - Informazioni squadre (valore, rosa, età media)
    - Dati di mercato giocatori (valore, storia trasferimenti)
    - Informazioni contrattuali
    - Storia infortuni
    """
    
    def __init__(self, cache_ttl: int = None):
        """
        Inizializza lo scraper Transfermarkt.
        
        Args:
            cache_ttl: Tempo di vita della cache in secondi (default da settings)
        """
        base_url = "https://www.transfermarkt.com"
        cache_ttl = cache_ttl or get_setting('scrapers.transfermarkt.cache_ttl', 86400)  # 24h default
        
        super().__init__(
            base_url=base_url,
            name="transfermarkt",
            cache_ttl=cache_ttl
        )
        
        # Impostazioni specifiche per Transfermarkt
        self.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'no-cache'
        })
        
        # Mappa delle lingue (Transfermarkt ha siti localizzati)
        self.language_domains = {
            'en': 'com',
            'it': 'it', 
            'es': 'es',
            'de': 'de',
            'fr': 'fr',
            'pt': 'pt',
            'nl': 'nl',
            'tr': 'com.tr',
            'ru': 'ru'
        }
        
        self.default_language = 'en'
        
        logger.info(f"TransfermarktScraper inizializzato con cache TTL: {cache_ttl}s")
    
    def _get_localized_url(self, path: str, language: str = None) -> str:
        """
        Ottiene l'URL localizzato per il percorso specificato.
        
        Args:
            path: Percorso relativo
            language: Codice lingua (default: default_language)
            
        Returns:
            URL completo localizzato
        """
        language = language or self.default_language
        tld = self.language_domains.get(language, self.language_domains[self.default_language])
        
        # Rimuovi barra iniziale se presente
        if path.startswith('/'):
            path = path[1:]
            
        return f"https://www.transfermarkt.{tld}/{path}"
    
    @cached(ttl=86400)  # 24h cache
    def search_team(self, team_name: str, country: Optional[str] = None, language: str = None) -> Dict[str, Any]:
        """
        Cerca una squadra su Transfermarkt.
        
        Args:
            team_name: Nome della squadra da cercare
            country: Paese della squadra (opzionale, migliora la precisione)
            language: Lingua della ricerca
            
        Returns:
            Dati di base della squadra con ID Transfermarkt
        """
        logger.info(f"Cercando squadra: {team_name} (country: {country})")
        
        try:
            # Normalizza il nome per la ricerca
            search_term = team_name.lower().replace(' ', '+')
            url = self._get_localized_url(f"schnellsuche/ergebnis/schnellsuche?query={search_term}&x=0&y=0", language)
            
            soup = self.get_soup(url)
            results = []
            
            # Cerca le squadre nei risultati
            team_boxes = soup.select("div.box")
            for box in team_boxes:
                if "Vereine" in box.text or "Clubs" in box.text or "Club" in box.text:  # diverse lingue
                    for row in box.select("table.items tbody tr"):
                        team_link = row.select_one("td.hauptlink a")
                        if not team_link:
                            continue
                            
                        team_url = team_link.get('href', '')
                        team_id = re.search(r'/verein/(\d+)/', team_url)
                        team_id = team_id.group(1) if team_id else None
                        
                        if not team_id:
                            continue
                            
                        team_country_elem = row.select_one("td.zentriert img.flaggenrahmen")
                        team_country = team_country_elem.get('title', '') if team_country_elem else None
                        
                        # Se è specificato un paese, filtra i risultati
                        if country and team_country and country.lower() != team_country.lower():
                            continue
                            
                        results.append({
                            'id': team_id,
                            'name': team_link.text.strip(),
                            'url': team_url,
                            'country': team_country
                        })
            
            # Trova la corrispondenza migliore
            if results:
                # Ordina per somiglianza nome (semplice)
                best_match = sorted(results, key=lambda x: self._similarity(x['name'].lower(), team_name.lower()), reverse=True)[0]
                
                # Aggiungi URL completo
                best_match['url'] = self._get_localized_url(best_match['url'], language)
                return best_match
                
            logger.warning(f"Nessuna squadra trovata per: {team_name}")
            return {}
            
        except Exception as e:
            logger.error(f"Errore nella ricerca della squadra {team_name}: {str(e)}")
            return {}
    
    @cached(ttl=86400)  # 24h cache
    def get_team_info(self, team_id: Union[str, int], language: str = None) -> Dict[str, Any]:
        """
        Ottiene informazioni dettagliate su una squadra.
        
        Args:
            team_id: ID Transfermarkt della squadra
            language: Lingua per i dati
            
        Returns:
            Dati completi sulla squadra
        """
        logger.info(f"Ottenendo informazioni squadra ID: {team_id}")
        
        try:
            url = self._get_localized_url(f"verein/{team_id}", language)
            soup = self.get_soup(url)
            
            # Informazioni di base
            team_info = {
                'id': team_id,
                'name': self._extract_team_name(soup),
                'country': self._extract_team_country(soup),
                'league': self._extract_team_league(soup),
                'squad_size': self._extract_squad_size(soup),
                'squad_value': self._extract_squad_value(soup),
                'average_age': self._extract_average_age(soup),
                'foreigners_percent': self._extract_foreigners_percent(soup),
                'stadium': self._extract_stadium(soup),
                'coach': self._extract_coach(soup),
                'url': url
            }
            
            return team_info
            
        except Exception as e:
            logger.error(f"Errore nell'ottenere info squadra {team_id}: {str(e)}")
            return {'id': team_id, 'error': str(e)}
    
    @cached(ttl=86400)  # 24h cache
    def get_team_squad(self, team_id: Union[str, int], season: Optional[str] = None, language: str = None) -> List[Dict[str, Any]]:
        """
        Ottiene la rosa completa di una squadra.
        
        Args:
            team_id: ID Transfermarkt della squadra
            season: Stagione (formato: 2023 per 2023/2024)
            language: Lingua per i dati
            
        Returns:
            Lista di giocatori con informazioni dettagliate
        """
        logger.info(f"Ottenendo rosa squadra ID: {team_id}, stagione: {season}")
        
        try:
            season_path = f"/plus/1?saison_id={season}" if season else ""
            url = self._get_localized_url(f"verein/{team_id}/kader{season_path}", language)
            
            soup = self.get_soup(url)
            players = []
            
            # Estrai table della rosa
            squad_table = soup.select_one("table.items")
            if not squad_table:
                logger.warning(f"Tabella rosa non trovata per squadra {team_id}")
                return []
                
            # Cicla su righe del tavola (giocatori)
            for row in squad_table.select("tbody tr"):
                if 'odd' not in row.get('class', []) and 'even' not in row.get('class', []):
                    continue  # Salta intestazioni e righe non valide
                    
                player = self._extract_player_from_row(row, team_id)
                if player:
                    players.append(player)
            
            return players
            
        except Exception as e:
            logger.error(f"Errore nell'ottenere rosa squadra {team_id}: {str(e)}")
            return []
    
    @cached(ttl=86400)  # 24h cache
    def get_player_info(self, player_id: Union[str, int], language: str = None) -> Dict[str, Any]:
        """
        Ottiene informazioni dettagliate su un giocatore.
        
        Args:
            player_id: ID Transfermarkt del giocatore
            language: Lingua per i dati
            
        Returns:
            Dati completi sul giocatore
        """
        logger.info(f"Ottenendo informazioni giocatore ID: {player_id}")
        
        try:
            url = self._get_localized_url(f"spieler/{player_id}", language)
            soup = self.get_soup(url)
            
            # Estrai dati di base
            player_info = {
                'id': player_id,
                'name': self._extract_player_name(soup),
                'full_name': self._extract_player_full_name(soup),
                'birth_date': self._extract_player_birth_date(soup),
                'age': self._extract_player_age(soup),
                'height': self._extract_player_height(soup),
                'nationality': self._extract_player_nationality(soup),
                'position': self._extract_player_position(soup),
                'foot': self._extract_player_foot(soup),
                'current_club': self._extract_player_current_club(soup),
                'market_value': self._extract_player_market_value(soup),
                'contract_until': self._extract_player_contract(soup),
                'agent': self._extract_player_agent(soup),
                'url': url
            }
            
            return player_info
            
        except Exception as e:
            logger.error(f"Errore nell'ottenere info giocatore {player_id}: {str(e)}")
            return {'id': player_id, 'error': str(e)}
    
    @cached(ttl=86400)  # 24h cache
    def get_player_transfer_history(self, player_id: Union[str, int], language: str = None) -> List[Dict[str, Any]]:
        """
        Ottiene la storia dei trasferimenti di un giocatore.
        
        Args:
            player_id: ID Transfermarkt del giocatore
            language: Lingua per i dati
            
        Returns:
            Lista di trasferimenti con dettagli
        """
        logger.info(f"Ottenendo storia trasferimenti giocatore ID: {player_id}")
        
        try:
            url = self._get_localized_url(f"spieler/{player_id}/transfers", language)
            soup = self.get_soup(url)
            
            transfers = []
            transfer_tables = soup.select("div.box table.items")
            
            for table in transfer_tables:
                for row in table.select("tbody tr.odd, tbody tr.even"):
                    transfer = self._extract_transfer_from_row(row)
                    if transfer:
                        transfers.append(transfer)
            
            return transfers
            
        except Exception as e:
            logger.error(f"Errore nell'ottenere trasferimenti giocatore {player_id}: {str(e)}")
            return []
    
    @cached(ttl=86400)  # 24h cache
    def get_player_injury_history(self, player_id: Union[str, int], language: str = None) -> List[Dict[str, Any]]:
        """
        Ottiene la storia degli infortuni di un giocatore.
        
        Args:
            player_id: ID Transfermarkt del giocatore
            language: Lingua per i dati
            
        Returns:
            Lista di infortuni con dettagli
        """
        logger.info(f"Ottenendo storia infortuni giocatore ID: {player_id}")
        
        try:
            url = self._get_localized_url(f"spieler/{player_id}/verletzungen", language)
            soup = self.get_soup(url)
            
            injuries = []
            injury_table = soup.select_one("table.items")
            
            if injury_table:
                for row in injury_table.select("tbody tr"):
                    injury = self._extract_injury_from_row(row)
                    if injury:
                        injuries.append(injury)
            
            return injuries
            
        except Exception as e:
            logger.error(f"Errore nell'ottenere infortuni giocatore {player_id}: {str(e)}")
            return []
    
    @cached(ttl=86400)  # 24h cache
    def get_team_transfers(self, team_id: Union[str, int], season: Optional[str] = None, 
                         transfer_type: str = 'all', language: str = None) -> Dict[str, List[Dict[str, Any]]]:
        """
        Ottiene i trasferimenti di una squadra.
        
        Args:
            team_id: ID Transfermarkt della squadra
            season: Stagione (formato: 2023 per 2023/2024, None per l'ultima)
            transfer_type: Tipo di trasferimento ('all', 'in', 'out')
            language: Lingua per i dati
            
        Returns:
            Dizionario con trasferimenti in entrata e uscita
        """
        logger.info(f"Ottenendo trasferimenti squadra ID: {team_id}, stagione: {season}")
        
        try:
            season_param = f"&saison_id={season}" if season else ""
            url = self._get_localized_url(f"verein/{team_id}/transfers{season_param}", language)
            
            soup = self.get_soup(url)
            
            result = {
                'in': [],
                'out': []
            }
            
            # Trasferimenti in entrata
            if transfer_type in ['all', 'in']:
                in_table = soup.select_one("div#yw1 table.items")
                if in_table:
                    for row in in_table.select("tbody tr"):
                        transfer = self._extract_team_transfer_from_row(row, 'in')
                        if transfer:
                            result['in'].append(transfer)
            
            # Trasferimenti in uscita
            if transfer_type in ['all', 'out']:
                out_table = soup.select_one("div#yw2 table.items")
                if out_table:
                    for row in out_table.select("tbody tr"):
                        transfer = self._extract_team_transfer_from_row(row, 'out')
                        if transfer:
                            result['out'].append(transfer)
            
            return result
            
        except Exception as e:
            logger.error(f"Errore nell'ottenere trasferimenti squadra {team_id}: {str(e)}")
            return {'in': [], 'out': []}
    
    @cached(ttl=86400)  # 24h cache
    def get_market_values(self, team_id: Union[str, int], language: str = None) -> List[Dict[str, Any]]:
        """
        Ottiene i valori di mercato dell'intera squadra.
        
        Args:
            team_id: ID Transfermarkt della squadra
            language: Lingua per i dati
            
        Returns:
            Lista di giocatori con valori di mercato
        """
        logger.info(f"Ottenendo valori di mercato squadra ID: {team_id}")
        
        try:
            url = self._get_localized_url(f"verein/{team_id}/marktwert", language)
            soup = self.get_soup(url)
            
            players = []
            market_table = soup.select_one("table.items")
            
            if market_table:
                for row in market_table.select("tbody tr"):
                    player = self._extract_market_value_from_row(row)
                    if player:
                        players.append(player)
            
            return players
            
        except Exception as e:
            logger.error(f"Errore nell'ottenere valori mercato squadra {team_id}: {str(e)}")
            return []
    
    # ---- Metodi di estrazione dati ---- #
    
    def _extract_team_name(self, soup: BeautifulSoup) -> str:
        """Estrae il nome della squadra."""
        try:
            h1 = soup.select_one("h1.data-header__headline-wrapper")
            if h1:
                return h1.text.strip()
            return ""
        except Exception:
            return ""
    
    def _extract_team_country(self, soup: BeautifulSoup) -> str:
        """Estrae il paese della squadra."""
        try:
            span = soup.select_one("span.data-header__club a")
            if span:
                return span.text.strip()
            return ""
        except Exception:
            return ""
    
    def _extract_team_league(self, soup: BeautifulSoup) -> str:
        """Estrae il campionato della squadra."""
        try:
            league = soup.select_one("div.data-header__league a")
            if league:
                return league.text.strip()
            return ""
        except Exception:
            return ""
    
    def _extract_squad_size(self, soup: BeautifulSoup) -> int:
        """Estrae la dimensione della rosa."""
        try:
            size_span = soup.select_one("span.data-header__label:contains('Squad size') + span.data-header__content")
            if size_span:
                return int(size_span.text.strip())
            # Try other languages
            size_span = soup.select_one("span.data-header__label:contains('Dimensione rosa') + span.data-header__content")
            if size_span:
                return int(size_span.text.strip())
            return 0
        except Exception:
            return 0
    
    def _extract_squad_value(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Estrae il valore della rosa."""
        try:
            value_span = soup.select_one("div.data-header__box a[href*='marktwert'] span.data-header__content")
            if value_span:
                value_text = value_span.text.strip()
                return self._parse_money_value(value_text)
            return {'value': 0, 'currency': 'EUR', 'text': '€0'}
        except Exception:
            return {'value': 0, 'currency': 'EUR', 'text': '€0'}
    
    def _extract_average_age(self, soup: BeautifulSoup) -> float:
        """Estrae l'età media della rosa."""
        try:
            age_span = soup.select_one("span.data-header__label:contains('Average age') + span.data-header__content")
            if age_span:
                return float(age_span.text.strip())
            # Try other languages
            age_span = soup.select_one("span.data-header__label:contains('Età media') + span.data-header__content")
            if age_span:
                return float(age_span.text.strip())
            return 0.0
        except Exception:
            return 0.0
    
    def _extract_foreigners_percent(self, soup: BeautifulSoup) -> float:
        """Estrae la percentuale di stranieri nella rosa."""
        try:
            foreigner_span = soup.select_one("span.data-header__label:contains('Foreigners') + span.data-header__content")
            if foreigner_span:
                text = foreigner_span.text.strip()
                match = re.search(r'(\d+,\d+|\d+)%', text)
                if match:
                    return float(match.group(1).replace(',', '.'))
            return 0.0
        except Exception:
            return 0.0
    
    def _extract_stadium(self, soup: BeautifulSoup) -> str:
        """Estrae lo stadio della squadra."""
        try:
            stadium_div = soup.select_one("div.data-header__details span:contains('Stadium:') + span")
            if stadium_div:
                return stadium_div.text.strip()
            # Try other languages
            stadium_div = soup.select_one("div.data-header__details span:contains('Stadio:') + span")
            if stadium_div:
                return stadium_div.text.strip()
            return ""
        except Exception:
            return ""
    
    def _extract_coach(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Estrae l'allenatore della squadra."""
        try:
            coach_div = soup.select_one("div.container-wappen a.tm-img-with-tooltip[href*='trainer']")
            if coach_div:
                coach_name = coach_div.get('title', '')
                coach_url = coach_div.get('href', '')
                coach_id = re.search(r'/trainer/(\d+)/', coach_url)
                
                return {
                    'name': coach_name,
                    'id': coach_id.group(1) if coach_id else None,
                    'url': self._get_localized_url(coach_url)
                }
            return {}
        except Exception:
            return {}
    
    def _extract_player_from_row(self, row: BeautifulSoup, team_id: str) -> Dict[str, Any]:
        """Estrae dati di un giocatore da una riga della tabella della rosa."""
        try:
            # Ottieni link player
            player_cell = row.select_one("td.hauptlink")
            if not player_cell:
                return None
                
            player_link = player_cell.select_one("a")
            if not player_link:
                return None
                
            player_url = player_link.get('href', '')
            player_id = re.search(r'/spieler/(\d+)/', player_url)
            
            if not player_id:
                return None
                
            player_id = player_id.group(1)
            
            # Dati base
            player = {
                'id': player_id,
                'name': player_link.text.strip(),
                'url': self._get_localized_url(player_url),
                'team_id': team_id
            }
            
            # Position
            position_cell = row.select_one("td.posrela table.inline-table tr:nth-child(2)")
            if position_cell:
                player['position'] = position_cell.text.strip()
            
            # Birth date
            birth_cell = row.select_one("td.zentriert:nth-of-type(4)")
            if birth_cell:
                player['birth_date'] = birth_cell.text.strip()
                
            # Nationality
            nat_cell = row.select_one("td.zentriert:nth-of-type(5)")
            if nat_cell:
                nationalities = []
                for flag in nat_cell.select("img.flaggenrahmen"):
                    if flag.get('title'):
                        nationalities.append(flag.get('title'))
                if nationalities:
                    player['nationality'] = nationalities
            
            # Market value
            value_cell = row.select_one("td.rechts")
            if value_cell:
                player['market_value'] = self._parse_money_value(value_cell.text.strip())
            
            return player
            
        except Exception as e:
            logger.error(f"Errore nell'estrarre dati giocatore da riga: {str(e)}")
            return None
    
    def _extract_player_name(self, soup: BeautifulSoup) -> str:
        """Estrae il nome del giocatore."""
        try:
            h1 = soup.select_one("h1.data-header__headline-wrapper")
            if h1:
                return h1.text.strip()
            return ""
        except Exception:
            return ""
    
    def _extract_player_full_name(self, soup: BeautifulSoup) -> str:
        """Estrae il nome completo del giocatore."""
        try:
            info_table = soup.select_one("div.info-table")
            if info_table:
                name_row = info_table.select_one("span.info-table__content--bold:contains('Full name:')")
                if name_row and name_row.parent:
                    return name_row.parent.select_one("span.info-table__content").text.strip()
            return ""
        except Exception:
            return ""
    
    def _extract_player_birth_date(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Estrae la data di nascita del giocatore."""
        try:
            info_table = soup.select_one("div.info-table")
            if info_table:
                date_row = info_table.select_one("span.info-table__content--bold:contains('Date of birth:')")
                if date_row and date_row.parent:
                    date_text = date_row.parent.select_one("span.info-table__content").text.strip()
                    date_match = re.search(r'(\w{3} \d{1,2}, \d{4})', date_text)
                    if date_match:
                        date_str = date_match.group(1)
                        try:
                            date_obj = datetime.strptime(date_str, '%b %d, %Y')
                            return {
                                'date': date_obj.strftime('%Y-%m-%d'),
                                'text': date_str
                            }
                        except Exception:
                            pass
            return {}
        except Exception:
            return {}
    
    def _extract_player_age(self, soup: BeautifulSoup) -> int:
        """Estrae l'età del giocatore."""
        try:
            info_table = soup.select_one("div.info-table")
            if info_table:
                age_row = info_table.select_one("span.info-table__content--bold:contains('Age:')")
                if age_row and age_row.parent:
                    age_text = age_row.parent.select_one("span.info-table__content").text.strip()
                    age_match = re.search(r'(\d+)', age_text)
                    if age_match:
                        return int(age_match.group(1))
            return 0
        except Exception:
            return 0
    
    def _extract_player_height(self, soup: BeautifulSoup) -> str:
        """Estrae l'altezza del giocatore."""
        try:
            info_table = soup.select_one("div.info-table")
            if info_table:
                height_row = info_table.select_one("span.info-table__content--bold:contains('Height:')")
                if height_row and height_row.parent:
                    return height_row.parent.select_one("span.info-table__content").text.strip()
            return ""
        except Exception:
            return ""
    
    def _extract_player_nationality(self, soup: BeautifulSoup) -> List[str]:
        """Estrae la nazionalità del giocatore."""
        try:
            info_table = soup.select_one("div.info-table")
            if info_table:
                nat_row = info_table.select_one("span.info-table__content--bold:contains('Citizenship:')")
                if nat_row and nat_row.parent:
                    nat_spans = nat_row.parent.select("span.info-table__content img.flaggenrahmen")
                    return [flag.get('title', '') for flag in nat_spans if flag.get('title')]
            return []
        except Exception:
            return []
    
    def _extract_player_position(self, soup: BeautifulSoup) -> str:
        """Estrae la posizione del giocatore."""
        try:
            info_table = soup.select_one("div.info-table")
            if info_table:
                pos_row = info_table.select_one("span.info-table__content--bold:contains('Position:')")
                if pos_row and pos_row.parent:
                    return pos_row.parent.select_one("span.info-table__content").text.strip()
            return ""
        except Exception:
            return ""
    
    def _extract_player_foot(self, soup: BeautifulSoup) -> str:
        """Estrae il piede preferito del giocatore."""
        try:
            info_table = soup.select_one("div.info-table")
            if info_table:
                foot_row = info_table.select_one("span.info-table__content--bold:contains('Foot:')")
                if foot_row and foot_row.parent:
                    return foot_row.parent.select_one("span.info-table__content").text.strip()
            return ""
        except Exception:
            return ""
    
    def _extract_player_current_club(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Estrae il club attuale del giocatore."""
        try:
            club_box = soup.select_one("span.data-header__club a")
            if club_box:
                club_name = club_box.text.strip()
                club_url = club_box.get('href', '')
                club_id = re.search(r'/verein/(\d+)/', club_url)
                
                return {
                    'name': club_name,
                    'id': club_id.group(1) if club_id else None,
                    'url': self._get_localized_url(club_url)
                }
            return {}
        except Exception:
            return {}
    
    def _extract_player_market_value(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Estrae il valore di mercato del giocatore."""
        try:
            value_div = soup.select_one("div.tm-player-market-value-development__current-value")
            if value_div:
                return self._parse_money_value(value_div.text.strip())
            return {'value': 0, 'currency': 'EUR', 'text': '€0'}
        except Exception:
            return {'value': 0, 'currency': 'EUR', 'text': '€0'}
    
    def _extract_player_contract(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Estrae i dati contrattuali del giocatore."""
        try:
            contract_div = soup.select_one("span.data-header__label:contains('Contract expires:') + span.data-header__content")
            if contract_div:
                date_text = contract_div.text.strip()
                if date_text:
                    try:
                        date_obj = datetime.strptime(date_text, '%b %d, %Y')
                        return {
                            'date': date_obj.strftime('%Y-%m-%d'),
                            'text': date_text
                        }
                    except Exception:
                        pass
            return {}
        except Exception:
            return {}
    
    def _extract_player_agent(self, soup: BeautifulSoup) -> str:
        """Estrae l'agente del giocatore."""
        try:
            info_table = soup.select_one("div.info-table")
            if info_table:
                agent_row = info_table.select_one("span.info-table__content--bold:contains('Player agent:')")
                if agent_row and agent_row.parent:
                    return agent_row.parent.select_one("span.info-table__content").text.strip()
            return ""
        except Exception:
            return ""
    
    def _extract_transfer_from_row(self, row: BeautifulSoup) -> Dict[str, Any]:
        """Estrae dati di un trasferimento da una riga della tabella trasferimenti."""
        try:
            # Periodo trasferimento
            season_cell = row.select_one("td.zentriert")
            if not season_cell:
                return None
                
            season = season_cell.text.strip()
            
            # Data
            date_cell = row.select_one("td.zentriert:nth-of-type(2)")
            date = date_cell.text.strip() if date_cell else ""
            
            # Squadra provenienza
            from_cell = row.select_one("td.no-border-rechts")
            if not from_cell:
                return None
                
            from_team_link = from_cell.select_one("a")
            from_team = {
                'name': from_team_link.text.strip() if from_team_link else from_cell.text.strip(),
                'id': None,
                'url': None
            }
            
            if from_team_link:
                from_team_url = from_team_link.get('href', '')
                from_team_id = re.search(r'/verein/(\d+)/', from_team_url)
                if from_team_id:
                    from_team['id'] = from_team_id.group(1)
                    from_team['url'] = self._get_localized_url(from_team_url)
            
            # Squadra destinazione
            to_cell = row.select_one("td.no-border-links")
            if not to_cell:
                return None
                
            to_team_link = to_cell.select_one("a")
            to_team = {
                'name': to_team_link.text.strip() if to_team_link else to_cell.text.strip(),
                'id': None,
                'url': None
            }
            
            if to_team_link:
                to_team_url = to_team_link.get('href', '')
                to_team_id = re.search(r'/verein/(\d+)/', to_team_url)
                if to_team_id:
                    to_team['id'] = to_team_id.group(1)
                    to_team['url'] = self._get_localized_url(to_team_url)
            
            # Valore trasferimento
            fee_cell = row.select_one("td.rechts")
            fee = self._parse_money_value(fee_cell.text.strip()) if fee_cell else {'value': 0, 'currency': 'EUR', 'text': ''}
            
            return {
                'season': season,
                'date': date,
                'from_team': from_team,
                'to_team': to_team,
                'fee': fee
            }
            
        except Exception as e:
            logger.error(f"Errore nell'estrarre dati trasferimento da riga: {str(e)}")
            return None
    
    def _extract_injury_from_row(self, row: BeautifulSoup) -> Dict[str, Any]:
        """Estrae dati di un infortunio da una riga della tabella infortuni."""
        try:
            # Stagione
            season_cell = row.select_one("td.zentriert")
            if not season_cell:
                return None
                
            season = season_cell.text.strip()
            
            # Tipo infortunio
            injury_cell = row.select_one("td:nth-of-type(2)")
            injury_type = injury_cell.text.strip() if injury_cell else ""
            
            # Data inizio
            from_cell = row.select_one("td.zentriert:nth-of-type(3)")
            from_date = from_cell.text.strip() if from_cell else ""
            
            # Data fine
            until_cell = row.select_one("td.zentriert:nth-of-type(4)")
            until_date = until_cell.text.strip() if until_cell else ""
            
            # Giorni
            days_cell = row.select_one("td.zentriert:nth-of-type(5)")
            days = int(days_cell.text.strip()) if days_cell and days_cell.text.strip().isdigit() else 0
            
            # Partite perse
            games_cell = row.select_one("td.zentriert:nth-of-type(6)")
            games_missed = int(games_cell.text.strip()) if games_cell and games_cell.text.strip().isdigit() else 0
            
            return {
                'season': season,
                'injury_type': injury_type,
                'from_date': from_date,
                'until_date': until_date,
                'days': days,
                'games_missed': games_missed
            }
            
        except Exception as e:
            logger.error(f"Errore nell'estrarre dati infortunio da riga: {str(e)}")
            return None
    
    def _extract_team_transfer_from_row(self, row: BeautifulSoup, transfer_type: str) -> Dict[str, Any]:
        """
        Estrae dati di un trasferimento di squadra da una riga della tabella trasferimenti.
        
        Args:
            row: Riga HTML
            transfer_type: Tipo di trasferimento ('in' o 'out')
            
        Returns:
            Dati del trasferimento
        """
        try:
            # Giocatore
            player_cell = row.select_one("td.hauptlink")
            if not player_cell:
                return None
                
            player_link = player_cell.select_one("a")
            if not player_link:
                return None
                
            player_url = player_link.get('href', '')
            player_id = re.search(r'/spieler/(\d+)/', player_url)
            
            player = {
                'id': player_id.group(1) if player_id else None,
                'name': player_link.text.strip(),
                'url': self._get_localized_url(player_url) if player_id else None
            }
            
            # Età
            age_cell = row.select_one("td.zentriert:nth-of-type(2)")
            age = int(age_cell.text.strip()) if age_cell and age_cell.text.strip().isdigit() else 0
            
            # Nazionalità
            nat_cell = row.select_one("td.zentriert:nth-of-type(3)")
            nationality = []
            if nat_cell:
                for img in nat_cell.select("img.flaggenrahmen"):
                    if img.get('title'):
                        nationality.append(img.get('title'))
            
            # Posizione
            pos_cell = row.select_one("td.zentriert:nth-of-type(4)")
            position = pos_cell.text.strip() if pos_cell else ""
            
            # Altra squadra coinvolta (dipende dal tipo di trasferimento)
            other_team_cell = row.select_one("td.no-border-links" if transfer_type == 'in' else "td.no-border-rechts")
            other_team = {}
            
            if other_team_cell:
                other_team_link = other_team_cell.select_one("a")
                if other_team_link:
                    other_team_name = other_team_link.text.strip()
                    other_team_url = other_team_link.get('href', '')
                    other_team_id = re.search(r'/verein/(\d+)/', other_team_url)
                    
                    other_team = {
                        'name': other_team_name,
                        'id': other_team_id.group(1) if other_team_id else None,
                        'url': self._get_localized_url(other_team_url) if other_team_id else None
                    }
                else:
                    other_team = {'name': other_team_cell.text.strip()}
            
            # Valore del trasferimento
            fee_cell = row.select_one("td.rechts")
            fee = self._parse_money_value(fee_cell.text.strip()) if fee_cell else {'value': 0, 'currency': 'EUR', 'text': ''}
            
            return {
                'player': player,
                'age': age,
                'nationality': nationality,
                'position': position,
                'other_team': other_team,
                'fee': fee,
                'transfer_type': transfer_type
            }
            
        except Exception as e:
            logger.error(f"Errore nell'estrarre dati trasferimento squadra da riga: {str(e)}")
            return None
    
    def _extract_market_value_from_row(self, row: BeautifulSoup) -> Dict[str, Any]:
        """Estrae valore di mercato di un giocatore da una riga della tabella valori di mercato."""
        try:
            # Giocatore
            player_cell = row.select_one("td a[href*='spieler']")
            if not player_cell:
                return None
                
            player_url = player_cell.get('href', '')
            player_id = re.search(r'/spieler/(\d+)/', player_url)
            
            if not player_id:
                return None
                
            player_id = player_id.group(1)
            
            # Valore attuale
            value_cell = row.select_one("td.rechts")
            current_value = self._parse_money_value(value_cell.text.strip()) if value_cell else {'value': 0, 'currency': 'EUR', 'text': '€0'}
            
            # Ulteriori informazioni
            player_info = {
                'id': player_id,
                'name': player_cell.text.strip(),
                'url': self._get_localized_url(player_url),
                'current_value': current_value
            }
            
            # Aggiungi dati addizionali se disponibili
            pos_cell = row.select_one("td:nth-of-type(4)")
            if pos_cell:
                player_info['position'] = pos_cell.text.strip()
                
            age_cell = row.select_one("td.zentriert:nth-of-type(3)")
            if age_cell:
                player_info['age'] = int(age_cell.text.strip()) if age_cell.text.strip().isdigit() else 0
                
            # Trend del valore
            trend_cell = row.select_one("td.zentriert span[class*='market-value']")
            if trend_cell:
                trend_class = trend_cell.get('class', [])
                trend = None
                if 'market-value-positivtrend' in trend_class:
                    trend = 'up'
                elif 'market-value-negativtrend' in trend_class:
                    trend = 'down'
                elif 'market-value-neutraltrend' in trend_class:
                    trend = 'stable'
                    
                if trend:
                    player_info['value_trend'] = trend
            
            return player_info
            
        except Exception as e:
            logger.error(f"Errore nell'estrarre valore di mercato da riga: {str(e)}")
            return None
    
    # ---- Metodi di utilità ---- #
    
    def _parse_money_value(self, value_text: str) -> Dict[str, Any]:
        """
        Converte un valore monetario testuale in un dizionario strutturato.
        
        Args:
            value_text: Testo con valore (es. "€25.00m", "£350k")
            
        Returns:
            Dizionario con valore numerico, valuta e testo originale
        """
        try:
            value_text = value_text.strip()
            if not value_text or value_text == '-':
                return {'value': 0, 'currency': 'EUR', 'text': value_text}
            
            # Riconosci valuta
            if '€' in value_text:
                currency = 'EUR'
            elif '£' in value_text:
                currency = 'GBP'
            elif '$' in value_text:
                currency = 'USD'
            else:
                currency = 'EUR'  # Default
            
            # Rimuovi simboli non numerici tranne punti e virgole
            value_text = value_text.replace(currency, '').replace('€', '').replace('£', '').replace('$', '')
            
            # Estrai il valore numerico
            value = 0
            multiplier = 1
            
            if 'm' in value_text:
                multiplier = 1000000
                value_text = value_text.replace('m', '')
            elif 'k' in value_text:
                multiplier = 1000
                value_text = value_text.replace('k', '')
                
            # Normalizza separatori
            value_text = value_text.replace(',', '.').strip()
            
            # Converti in numero
            try:
                value = float(value_text) * multiplier
            except ValueError:
                value = 0
            
            return {
                'value': value,
                'currency': currency,
                'text': value_text  # Testo originale
            }
            
        except Exception:
            return {'value': 0, 'currency': 'EUR', 'text': value_text}
    
    def _similarity(self, a: str, b: str) -> float:
        """
        Calcola una semplice somiglianza tra due stringhe.
        Usato per trovare la corrispondenza migliore nelle ricerche.
        
        Args:
            a: Prima stringa
            b: Seconda stringa
            
        Returns:
            Valore di somiglianza (0-1)
        """
        # Normalizza le stringhe
        a = self._normalize_string(a)
        b = self._normalize_string(b)
        
        # Calcola semplice metrica di somiglianza
        len_a, len_b = len(a), len(b)
        if len_a == 0 or len_b == 0:
            return 0
            
        # Se una è sottostringa dell'altra, alta somiglianza
        if a in b or b in a:
            return 0.8
            
        # Altrimenti calcola somiglianza di caratteri
        common = sum(1 for c in a if c in b)
        return common / max(len_a, len_b)
    
    def _normalize_string(self, s: str) -> str:
        """
        Normalizza una stringa per confronti insensibili a case e accenti.
        
        Args:
            s: Stringa da normalizzare
            
        Returns:
            Stringa normalizzata
        """
        if not s:
            return ""
            
        # Converti a minuscolo
        s = s.lower()
        
        # Rimuovi accenti
        s = unicodedata.normalize('NFD', s).encode('ascii', 'ignore').decode('utf-8')
        
        # Rimuovi punteggiatura e caratteri speciali
        s = re.sub(r'[^\w\s]', '', s)
        
        # Rimuovi spazi multipli
        s = re.sub(r'\s+', ' ', s).strip()
        
        return s

# Funzioni di utilità globali

def search_team(team_name: str, country: Optional[str] = None, language: str = None) -> Dict[str, Any]:
    """
    Cerca una squadra su Transfermarkt.
    
    Args:
        team_name: Nome della squadra da cercare
        country: Paese della squadra (opzionale, migliora la precisione)
        language: Lingua della ricerca
        
    Returns:
        Dati di base della squadra con ID Transfermarkt
    """
    scraper = TransfermarktScraper()
    return scraper.search_team(team_name, country, language)

def get_team_info(team_id: Union[str, int], language: str = None) -> Dict[str, Any]:
    """
    Ottiene informazioni dettagliate su una squadra.
    
    Args:
        team_id: ID Transfermarkt della squadra
        language: Lingua per i dati
        
    Returns:
        Dati completi sulla squadra
    """
    scraper = TransfermarktScraper()
    return scraper.get_team_info(team_id, language)

def get_team_squad(team_id: Union[str, int], season: Optional[str] = None, language: str = None) -> List[Dict[str, Any]]:
    """
    Ottiene la rosa completa di una squadra.
    
    Args:
        team_id: ID Transfermarkt della squadra
        season: Stagione (formato: 2023 per 2023/2024)
        language: Lingua per i dati
        
    Returns:
        Lista di giocatori con informazioni dettagliate
    """
    scraper = TransfermarktScraper()
    return scraper.get_team_squad(team_id, season, language)

def get_player_info(player_id: Union[str, int], language: str = None) -> Dict[str, Any]:
    """
    Ottiene informazioni dettagliate su un giocatore.
    
    Args:
        player_id: ID Transfermarkt del giocatore
        language: Lingua per i dati
        
    Returns:
        Dati completi sul giocatore
    """
    scraper = TransfermarktScraper()
    return scraper.get_player_info(player_id, language)

def get_player_transfer_history(player_id: Union[str, int], language: str = None) -> List[Dict[str, Any]]:
    """
    Ottiene la storia dei trasferimenti di un giocatore.
    
    Args:
        player_id: ID Transfermarkt del giocatore
        language: Lingua per i dati
        
    Returns:
        Lista di trasferimenti con dettagli
    """
    scraper = TransfermarktScraper()
    return scraper.get_player_transfer_history(player_id, language)

def get_player_injury_history(player_id: Union[str, int], language: str = None) -> List[Dict[str, Any]]:
    """
    Ottiene la storia degli infortuni di un giocatore.
    
    Args:
        player_id: ID Transfermarkt del giocatore
        language: Lingua per i dati
        
    Returns:
        Lista di infortuni con dettagli
    """
    scraper = TransfermarktScraper()
    return scraper.get_player_injury_history(player_id, language)

def get_team_transfers(team_id: Union[str, int], season: Optional[str] = None, 
                      transfer_type: str = 'all', language: str = None) -> Dict[str, List[Dict[str, Any]]]:
    """
    Ottiene i trasferimenti di una squadra.
    
    Args:
        team_id: ID Transfermarkt della squadra
        season: Stagione (formato: 2023 per 2023/2024, None per l'ultima)
        transfer_type: Tipo di trasferimento ('all', 'in', 'out')
        language: Lingua per i dati
        
    Returns:
        Dizionario con trasferimenti in entrata e uscita
    """
    scraper = TransfermarktScraper()
    return scraper.get_team_transfers(team_id, season, transfer_type, language)

def get_market_values(team_id: Union[str, int], language: str = None) -> List[Dict[str, Any]]:
    """
    Ottiene i valori di mercato dell'intera squadra.
    
    Args:
        team_id: ID Transfermarkt della squadra
        language: Lingua per i dati
        
    Returns:
        Lista di giocatori con valori di mercato
    """
    scraper = TransfermarktScraper()
    return scraper.get_market_values(team_id, language)

# Alla fine del file transfermarkt.py

# Istanza globale
_scraper_instance = None

def get_scraper():
    """
    Ottiene l'istanza globale dello scraper Transfermarkt.
    
    Returns:
        Istanza di TransfermarktScraper.
    """
    global _scraper_instance
    if _scraper_instance is None:
        _scraper_instance = TransfermarktScraper()
    return _scraper_instance
