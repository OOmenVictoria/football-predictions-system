""" 
Modulo per l'estrazione di statistiche avanzate da WhoScored-like.
Questo modulo fornisce funzionalità per estrarre statistiche avanzate,
ratings giocatori, e dati tattici da fonti simili a WhoScored.
"""
import os
import re
import json
import time
import logging
import datetime
from typing import Dict, List, Any, Optional, Union, Tuple
from bs4 import BeautifulSoup

from src.data.scrapers.base_scraper import BaseScraper
from src.utils.cache import cached
from src.utils.database import FirebaseManager
from src.config.settings import get_setting

logger = logging.getLogger(__name__)

class WhoScoredScraper(BaseScraper):
    """
    Scraper per estrarre statistiche avanzate da fonti simili a WhoScored.
    
    Poiché WhoScored implementa protezioni anti-scraping avanzate,
    questo scraper ottiene dati simili da fonti alternative che forniscono 
    statistiche di livello paragonabile senza blocchi significativi.
    """
    
    def __init__(self):
        """Inizializza lo scraper WhoScored-like."""
        super().__init__()
        self.db = FirebaseManager()
        self.base_urls = {
            'footystats': 'https://footystats.org',
            'fbref': 'https://fbref.com',
            'sofascore': 'https://www.sofascore.com'
        }
        self.preferred_source = get_setting('scrapers.whoscored.preferred_source', 'sofascore')
        self.min_wait_time = get_setting('scrapers.whoscored.min_wait_time', 3)
        
        logger.info(f"WhoScoredScraper inizializzato con source primaria: {self.preferred_source}")
    
    @cached(ttl=86400)  # 24 ore
    def get_match_statistics(self, match_id: str, league_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Ottiene statistiche avanzate per una partita specifica.
        
        Args:
            match_id: ID della partita
            league_id: ID del campionato (opzionale, può migliorare la ricerca)
            
        Returns:
            Statistiche complete della partita
        """
        logger.info(f"Raccolta statistiche avanzate per match_id={match_id}")
        
        # Carica dati partita da Firebase per identificare le squadre
        match_data = self.db.get_reference(f"data/matches/{match_id}").get()
        
        if not match_data:
            logger.warning(f"Partita {match_id} non trovata nel database")
            return {}
        
        try:
            # Identifica squadre
            home_team = match_data.get('home_team', '')
            away_team = match_data.get('away_team', '')
            match_date = match_data.get('datetime', '')
            
            if not home_team or not away_team:
                logger.warning(f"Informazioni squadre mancanti per match_id={match_id}")
                return {}
            
            # Prova fonte primaria
            source = self.preferred_source
            stats = self._get_stats_from_source(source, match_id, home_team, away_team, match_date)
            
            # Se la fonte primaria fallisce, prova alternative
            if not stats:
                for alt_source in self.base_urls.keys():
                    if alt_source != self.preferred_source:
                        logger.info(f"Tentativo con fonte alternativa: {alt_source}")
                        stats = self._get_stats_from_source(alt_source, match_id, home_team, away_team, match_date)
                        if stats:
                            break
            
            if not stats:
                logger.warning(f"Nessuna statistica trovata per {home_team} vs {away_team}")
                return {}
            
            # Aggiorna info partita con le statistiche
            stats['match_id'] = match_id
            stats['home_team'] = home_team
            stats['away_team'] = away_team
            stats['source'] = self.preferred_source
            stats['collected_at'] = datetime.datetime.now().isoformat()
            
            return stats
        
        except Exception as e:
            logger.error(f"Errore nel recupero statistiche per match_id={match_id}: {str(e)}")
            return {}
    
    def _get_stats_from_source(self, source: str, match_id: str, 
                              home_team: str, away_team: str, 
                              match_date: str) -> Dict[str, Any]:
        """
        Ottiene statistiche da una fonte specifica.
        
        Args:
            source: Nome della fonte (footystats, fbref, sofascore)
            match_id: ID della partita
            home_team: Nome squadra casa
            away_team: Nome squadra trasferta
            match_date: Data della partita
            
        Returns:
            Statistiche della partita dalla fonte specificata
        """
        source_methods = {
            'footystats': self._get_footystats_data,
            'fbref': self._get_fbref_data,
            'sofascore': self._get_sofascore_data
        }
        
        method = source_methods.get(source)
        if not method:
            logger.warning(f"Fonte non supportata: {source}")
            return {}
        
        try:
            return method(match_id, home_team, away_team, match_date)
        except Exception as e:
            logger.error(f"Errore nell'estrazione da {source}: {str(e)}")
            return {}
    
    def _get_sofascore_data(self, match_id: str, home_team: str, 
                          away_team: str, match_date: str) -> Dict[str, Any]:
        """
        Estrae statistiche da SofaScore.
        
        Args:
            match_id: ID della partita
            home_team: Nome squadra casa
            away_team: Nome squadra trasferta
            match_date: Data della partita
            
        Returns:
            Statistiche della partita in formato WhoScored-like
        """
        # Normalizza nomi squadre per la ricerca
        home_team_norm = self._normalize_team_name(home_team)
        away_team_norm = self._normalize_team_name(away_team)
        
        # Crea URL ricerca
        search_url = f"{self.base_urls['sofascore']}/search/result/{home_team_norm}%20{away_team_norm}"
        
        try:
            # Cerca pagina partita
            soup = self._get_soup(search_url)
            
            # Trova link partite nei risultati di ricerca
            match_links = soup.select('a[href*="/football/"]')
            match_url = None
            
            # Filtra per trovare la partita corretta
            for link in match_links:
                if home_team_norm.lower() in link.text.lower() and away_team_norm.lower() in link.text.lower():
                    match_url = self.base_urls['sofascore'] + link['href']
                    break
            
            if not match_url:
                logger.warning(f"Partita non trovata su SofaScore: {home_team} vs {away_team}")
                return {}
            
            # Estrazione statistiche dalla pagina partita
            soup = self._get_soup(match_url)
            
            # Inizializziamo la struttura dati risultante
            stats = {
                'home': {'team': home_team, 'stats': {}},
                'away': {'team': away_team, 'stats': {}},
                'players': {'home': [], 'away': []},
                'events': [],
                'tactics': {'home': '', 'away': ''}
            }
            
            # Estrazione statistiche squadra
            self._extract_team_stats(soup, stats)
            
            # Estrazione valutazioni giocatori
            self._extract_player_ratings(soup, stats)
            
            # Estrazione eventi partita (gol, cartellini)
            self._extract_match_events(soup, stats)
            
            # Estrazione formazioni tattiche
            self._extract_formations(soup, stats)
            
            return stats
        
        except Exception as e:
            logger.error(f"Errore nell'estrazione dati SofaScore: {str(e)}")
            return {}
    
    def _get_fbref_data(self, match_id: str, home_team: str, 
                       away_team: str, match_date: str) -> Dict[str, Any]:
        """
        Estrae statistiche da FBref (Sports Reference).
        
        Args:
            match_id: ID della partita
            home_team: Nome squadra casa
            away_team: Nome squadra trasferta
            match_date: Data della partita
            
        Returns:
            Statistiche della partita in formato WhoScored-like
        """
        # Conversione formato data per la ricerca
        try:
            match_date_obj = datetime.datetime.strptime(match_date, "%Y-%m-%dT%H:%M:%SZ")
            search_date = match_date_obj.strftime("%Y-%m-%d")
        except:
            search_date = ""
        
        # Normalizza nomi squadre per la ricerca
        home_team_norm = self._normalize_team_name(home_team)
        away_team_norm = self._normalize_team_name(away_team)
        
        # URL ricerca partite del giorno
        search_url = f"{self.base_urls['fbref']}/en/matches/{search_date}"
        
        try:
            # Cerca partita
            soup = self._get_soup(search_url)
            
            # Trova tabella partite
            match_links = soup.select('div.scorebox a[href*="/squads/"]')
            match_url = None
            
            # Cerca squadre nella pagina
            teams_found = []
            for link in match_links:
                teams_found.append(link.text.strip())
            
            # Trova il div della scorebox che contiene entrambe le squadre
            scoreboxes = soup.select('div.scorebox')
            target_scorebox = None
            
            for box in scoreboxes:
                team_names = [t.text.strip() for t in box.select('a[href*="/squads/"]')]
                if (home_team_norm.lower() in [t.lower() for t in team_names] and 
                    away_team_norm.lower() in [t.lower() for t in team_names]):
                    target_scorebox = box
                    break
            
            if not target_scorebox:
                logger.warning(f"Partita non trovata su FBref: {home_team} vs {away_team}")
                return {}
            
            # Inizializziamo la struttura dati risultante
            stats = {
                'home': {'team': home_team, 'stats': {}},
                'away': {'team': away_team, 'stats': {}},
                'players': {'home': [], 'away': []},
                'events': [],
                'tactics': {'home': '', 'away': ''}
            }
            
            # Link alle statistiche complete
            stats_link = soup.select_one('a[href*="matchreport"]')
            if stats_link:
                full_stats_url = self.base_urls['fbref'] + stats_link['href']
                stats_soup = self._get_soup(full_stats_url)
                
                # Estrai statistiche squadra
                self._extract_fbref_team_stats(stats_soup, stats)
                
                # Estrai valutazioni giocatori
                self._extract_fbref_player_stats(stats_soup, stats)
            
            return stats
        
        except Exception as e:
            logger.error(f"Errore nell'estrazione dati FBref: {str(e)}")
            return {}
    
    def _get_footystats_data(self, match_id: str, home_team: str, 
                            away_team: str, match_date: str) -> Dict[str, Any]:
        """
        Estrae statistiche da FootyStats.
        
        Args:
            match_id: ID della partita
            home_team: Nome squadra casa
            away_team: Nome squadra trasferta
            match_date: Data della partita
            
        Returns:
            Statistiche della partita in formato WhoScored-like
        """
        # Normalizza nomi squadre per la ricerca
        home_team_norm = self._normalize_team_name(home_team).replace(' ', '-').lower()
        away_team_norm = self._normalize_team_name(away_team).replace(' ', '-').lower()
        
        # Crea URL ricerca
        search_url = f"{self.base_urls['footystats']}/teams/{home_team_norm}-vs-{away_team_norm}"
        
        try:
            # Cerca pagina statistiche
            soup = self._get_soup(search_url)
            
            # Inizializziamo la struttura dati risultante
            stats = {
                'home': {'team': home_team, 'stats': {}},
                'away': {'team': away_team, 'stats': {}},
                'players': {'home': [], 'away': []},
                'events': [],
                'tactics': {'home': '', 'away': ''}
            }
            
            # Estrazione statistiche squadra
            stats_tables = soup.select('table.team-comparison-stats')
            if stats_tables:
                self._extract_footystats_team_stats(stats_tables[0], stats)
            
            return stats
        
        except Exception as e:
            logger.error(f"Errore nell'estrazione dati FootyStats: {str(e)}")
            return {}
    
    def _extract_team_stats(self, soup: BeautifulSoup, stats: Dict[str, Any]) -> None:
        """
        Estrae statistiche di squadra dalla pagina SofaScore.
        
        Args:
            soup: Oggetto BeautifulSoup della pagina
            stats: Dizionario statistiche da aggiornare
        """
        # Estrai statistiche dai riquadri delle statistiche
        stat_rows = soup.select('div[class*="stat-cell"]')
        
        for row in stat_rows:
            # Ottieni tipo di statistica
            stat_type = row.select_one('div[class*="title"]')
            if not stat_type:
                continue
                
            stat_name = stat_type.text.strip().lower().replace(' ', '_')
            
            # Ottieni valori per casa e trasferta
            values = row.select('div[class*="value"]')
            if len(values) >= 2:
                home_value = self._parse_numeric(values[0].text)
                away_value = self._parse_numeric(values[1].text)
                
                stats['home']['stats'][stat_name] = home_value
                stats['away']['stats'][stat_name] = away_value
    
    def _extract_player_ratings(self, soup: BeautifulSoup, stats: Dict[str, Any]) -> None:
        """
        Estrae valutazioni giocatori dalla pagina SofaScore.
        
        Args:
            soup: Oggetto BeautifulSoup della pagina
            stats: Dizionario statistiche da aggiornare
        """
        # Trova le tabelle dei giocatori per ogni squadra
        lineup_home = soup.select_one('div[class*="home-team-lineup"]')
        lineup_away = soup.select_one('div[class*="away-team-lineup"]')
        
        if lineup_home:
            stats['players']['home'] = self._extract_players_from_lineup(lineup_home)
        
        if lineup_away:
            stats['players']['away'] = self._extract_players_from_lineup(lineup_away)
    
    def _extract_players_from_lineup(self, lineup: BeautifulSoup) -> List[Dict[str, Any]]:
        """
        Estrae dati giocatori da un riquadro formazione.
        
        Args:
            lineup: Elemento BeautifulSoup della formazione
            
        Returns:
            Lista di statistiche giocatori
        """
        players = []
        player_items = lineup.select('div[class*="player-row"]')
        
        for item in player_items:
            # Nome giocatore
            name_elem = item.select_one('span[class*="name"]')
            if not name_elem:
                continue
                
            name = name_elem.text.strip()
            
            # Valutazione
            rating_elem = item.select_one('span[class*="rating"]')
            rating = self._parse_numeric(rating_elem.text) if rating_elem else None
            
            # Ruolo
            position_elem = item.select_one('div[class*="position"]')
            position = position_elem.text.strip() if position_elem else ""
            
            # Costruisce dati giocatore
            player_data = {
                'name': name,
                'rating': rating,
                'position': position,
                'stats': {}
            }
            
            # Aggiungi statistiche aggiuntive se presenti
            stats_items = item.select('div[class*="stat-value"]')
            stats_labels = item.select('div[class*="stat-label"]')
            
            for i, stat_item in enumerate(stats_items):
                if i < len(stats_labels):
                    stat_name = stats_labels[i].text.strip().lower().replace(' ', '_')
                    stat_value = self._parse_numeric(stat_item.text)
                    player_data['stats'][stat_name] = stat_value
            
            players.append(player_data)
        
        return players
    
    def _extract_match_events(self, soup: BeautifulSoup, stats: Dict[str, Any]) -> None:
        """
        Estrae eventi della partita dalla pagina SofaScore.
        
        Args:
            soup: Oggetto BeautifulSoup della pagina
            stats: Dizionario statistiche da aggiornare
        """
        # Cerca riquadro eventi
        events_container = soup.select_one('div[class*="event-list"]')
        if not events_container:
            return
            
        event_items = events_container.select('div[class*="event-item"]')
        
        for item in event_items:
            # Tipo evento
            event_type = ""
            if 'goal' in item.get('class', []):
                event_type = 'goal'
            elif 'yellow-card' in item.get('class', []):
                event_type = 'yellow_card'
            elif 'red-card' in item.get('class', []):
                event_type = 'red_card'
            elif 'substitution' in item.get('class', []):
                event_type = 'substitution'
            else:
                continue
            
            # Minuto
            time_elem = item.select_one('span[class*="time"]')
            minute = int(time_elem.text.replace("'", "")) if time_elem else 0
            
            # Giocatore
            player_elem = item.select_one('span[class*="player-name"]')
            player_name = player_elem.text.strip() if player_elem else ""
            
            # Squadra (home/away)
            team = 'home' if 'home-team' in item.get('class', []) else 'away'
            
            # Costruisce evento
            event = {
                'type': event_type,
                'minute': minute,
                'player': player_name,
                'team': team
            }
            
            # Aggiungi dettagli specifici per tipo evento
            if event_type == 'goal':
                score_elem = item.select_one('span[class*="score"]')
                event['score'] = score_elem.text.strip() if score_elem else ""
                
                assist_elem = item.select_one('span[class*="assist"]')
                if assist_elem:
                    event['assist'] = assist_elem.text.replace('Assist:', '').strip()
            
            elif event_type == 'substitution':
                sub_in = item.select_one('span[class*="player-in"]')
                sub_out = item.select_one('span[class*="player-out"]')
                
                if sub_in and sub_out:
                    event['player_in'] = sub_in.text.strip()
                    event['player_out'] = sub_out.text.strip()
            
            stats['events'].append(event)
    
    def _extract_formations(self, soup: BeautifulSoup, stats: Dict[str, Any]) -> None:
        """
        Estrae formazioni tattiche dalla pagina SofaScore.
        
        Args:
            soup: Oggetto BeautifulSoup della pagina
            stats: Dizionario statistiche da aggiornare
        """
        # Cerca elementi formazione
        formation_home = soup.select_one('div[class*="home-team-formation"]')
        formation_away = soup.select_one('div[class*="away-team-formation"]')
        
        if formation_home:
            stats['tactics']['home'] = formation_home.text.strip()
        
        if formation_away:
            stats['tactics']['away'] = formation_away.text.strip()
    
    def _extract_fbref_team_stats(self, soup: BeautifulSoup, stats: Dict[str, Any]) -> None:
        """
        Estrae statistiche di squadra dalla pagina FBref.
        
        Args:
            soup: Oggetto BeautifulSoup della pagina
            stats: Dizionario statistiche da aggiornare
        """
        # Cerca tabella statistiche
        stats_table = soup.select_one('table#team_stats')
        if not stats_table:
            return
            
        rows = stats_table.select('tr')
        
        for row in rows:
            # Salta righe intestazione
            cells = row.select('td')
            if len(cells) < 3:
                continue
            
            # Estrai nome statistica e valori
            stat_name = row.select_one('th').text.strip().lower().replace(' ', '_')
            
            # A volte su FBref i valori possono essere in celle diverse
            # a seconda del layout della tabella
            home_value = self._parse_numeric(cells[0].text)
            away_value = self._parse_numeric(cells[-1].text)
            
            stats['home']['stats'][stat_name] = home_value
            stats['away']['stats'][stat_name] = away_value
    
    def _extract_fbref_player_stats(self, soup: BeautifulSoup, stats: Dict[str, Any]) -> None:
        """
        Estrae statistiche giocatori dalla pagina FBref.
        
        Args:
            soup: Oggetto BeautifulSoup della pagina
            stats: Dizionario statistiche da aggiornare
        """
        # Cerca tabelle statistiche giocatori
        home_table = soup.select_one('table#stats_a')
        away_table = soup.select_one('table#stats_b')
        
        if home_table:
            stats['players']['home'] = self._extract_players_from_fbref_table(home_table)
        
        if away_table:
            stats['players']['away'] = self._extract_players_from_fbref_table(away_table)
    
    def _extract_players_from_fbref_table(self, table: BeautifulSoup) -> List[Dict[str, Any]]:
        """
        Estrae dati giocatori da tabella FBref.
        
        Args:
            table: Elemento BeautifulSoup della tabella
            
        Returns:
            Lista di statistiche giocatori
        """
        players = []
        rows = table.select('tbody tr')
        
        # Ottieni nomi colonne
        headers = []
        header_cells = table.select('thead th')
        for th in header_cells:
            header = th.text.strip().lower().replace(' ', '_')
            if header:
                headers.append(header)
        
        for row in rows:
            cells = row.select('td')
            if len(cells) < 3:
                continue
            
            # Nome giocatore (prima cella)
            name_cell = row.select_one('th')
            if not name_cell:
                continue
                
            name = name_cell.text.strip()
            
            # Prepara dati giocatore
            player_data = {
                'name': name,
                'position': '',
                'rating': None,
                'stats': {}
            }
            
            # Aggiungi altre statistiche
            for i, cell in enumerate(cells):
                if i < len(headers):
                    header = headers[i + 1]  # +1 perché abbiamo già preso il nome dalla colonna th
                    value = self._parse_numeric(cell.text)
                    
                    # Tratta alcuni campi in modo speciale
                    if header == 'pos':
                        player_data['position'] = cell.text.strip()
                    elif header == 'rating':
                        player_data['rating'] = value
                    else:
                        player_data['stats'][header] = value
            
            players.append(player_data)
        
        return players
    
    def _extract_footystats_team_stats(self, table: BeautifulSoup, stats: Dict[str, Any]) -> None:
        """
        Estrae statistiche di squadra dalla pagina FootyStats.
        
        Args:
            table: Tabella statistiche
            stats: Dizionario statistiche da aggiornare
        """
        rows = table.select('tr')
        
        for row in rows:
            cells = row.select('td')
            if len(cells) < 3:
                continue
            
            # Il nome statistica è nella cella centrale
            stat_name = cells[1].text.strip().lower().replace(' ', '_')
            
            # Valori home e away
            home_val = self._parse_numeric(cells[0].text)
            away_val = self._parse_numeric(cells[2].text)
            
            stats['home']['stats'][stat_name] = home_val
            stats['away']['stats'][stat_name] = away_val
    
    def _normalize_team_name(self, name: str) -> str:
        """
        Normalizza il nome di una squadra per la ricerca.
        
        Args:
            name: Nome squadra originale
            
        Returns:
            Nome squadra normalizzato
        """
        if not name:
            return ""
            
        # Rimuovi prefissi/suffissi comuni
        name = re.sub(r'^FC\s+', '', name)
        name = re.sub(r'\s+FC$', '', name)
        
        # Rimuovi caratteri speciali
        name = re.sub(r'[^\w\s]', '', name)
        
        # Normalizza spazi
        name = ' '.join(name.split())
        
        return name
    
    def _parse_numeric(self, text: str) -> Union[float, int, str]:
        """
        Converte una stringa in valore numerico quando possibile.
        
        Args:
            text: Testo da convertire
            
        Returns:
            Valore convertito (int, float o stringa originale)
        """
        if not text or not isinstance(text, str):
            return text
            
        # Pulisci il testo
        clean_text = text.strip().replace(',', '.')
        
        # Prova a convertire in numerico
        try:
            # Se contiene punto decimale
            if '.' in clean_text:
                return float(clean_text)
            # Altrimenti intero
            else:
                return int(clean_text)
        except:
            # Se fallisce ritorna la stringa originale
            return text
    
    @cached(ttl=86400)  # 24 ore
    def get_team_statistics(self, team_id: str, league_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Ottiene statistiche avanzate per una squadra.
        
        Args:
            team_id: ID della squadra
            league_id: ID del campionato (opzionale)
            
        Returns:
            Statistiche complete della squadra
        """
        logger.info(f"Raccolta statistiche avanzate per team_id={team_id}")
        
        # Carica dati squadra da Firebase
        team_data = self.db.get_reference(f"data/teams/{team_id}").get()
        
        if not team_data:
            logger.warning(f"Squadra {team_id} non trovata nel database")
            return {}
        
        try:
            # Ottieni nome squadra
            team_name = team_data.get('name', '')
            
            if not team_name:
                logger.warning(f"Nome squadra mancante per team_id={team_id}")
                return {}
            
            # Ottieni le ultime partite della squadra
            matches_ref = self.db.get_reference("data/matches")
            query = matches_ref.order_by_child("home_team_id").equal_to(team_id)
            home_matches = query.get() or {}
            
            query = matches_ref.order_by_child("away_team_id").equal_to(team_id)
            away_matches = query.get() or {}
            
            # Unisci risultati
            all_matches = {**home_matches, **away_matches}
            
            # Se abbiamo un campionato specifico, filtra
            if league_id:
                all_matches = {k: v for k, v in all_matches.items() if v.get('league_id') == league_id}
            
            # Se non abbiamo match, prova a cercare direttamente
            if not all_matches:
                return self._scrape_team_statistics(team_name, league_id)
            
            # Analizza le partite per ottenere statistiche aggregate
            return self._process_team_matches(team_id, team_name, all_matches)
            
        except Exception as e:
            logger.error(f"Errore nel recupero statistiche per team_id={team_id}: {str(e)}")
            return {}
    
    def _scrape_team_statistics(self, team_name: str, league_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Estrae statistiche di una squadra direttamente dalle fonti.
        
        Args:
            team_name: Nome della squadra
            league_id: ID del campionato (opzionale)
            
        Returns:
            Statistiche della squadra
        """
        # Normalizza nome squadra
        team_name_norm = self._normalize_team_name(team_name)
        
        stats = {}
        
        # Prova con fonte preferita
        source = self.preferred_source
        stats = self._get_team_stats_from_source(source, team_name_norm, league_id)
        
        # Se la fonte primaria fallisce, prova alternative
        if not stats:
            for alt_source in self.base_urls.keys():
                if alt_source != self.preferred_source:
                    logger.info(f"Tentativo con fonte alternativa: {alt_source}")
                    stats = self._get_team_stats_from_source(alt_source, team_name_norm, league_id)
                    if stats:
                        break
        
        if not stats:
            logger.warning(f"Nessuna statistica trovata per {team_name}")
            return {}
        
        # Aggiungi metadati
        stats['team_name'] = team_name
        stats['source'] = source
        stats['collected_at'] = datetime.datetime.now().isoformat()
        
        return stats
    
    def _get_team_stats_from_source(self, source: str, team_name: str, 
                                  league_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Ottiene statistiche di una squadra da una fonte specifica.
        
        Args:
            source: Nome della fonte
            team_name: Nome della squadra
            league_id: ID del campionato (opzionale)
            
        Returns:
            Statistiche della squadra dalla fonte specificata
        """
        source_methods = {
            'footystats': self._get_footystats_team_stats,
            'fbref': self._get_fbref_team_stats,
            'sofascore': self._get_sofascore_team_stats
        }
        
        method = source_methods.get(source)
        if not method:
            logger.warning(f"Fonte non supportata: {source}")
            return {}
        
        try:
            return method(team_name, league_id)
        except Exception as e:
            logger.error(f"Errore nell'estrazione da {source}: {str(e)}")
            return {}
    
    def _get_sofascore_team_stats(self, team_name: str, 
                               league_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Estrae statistiche squadra da SofaScore.
        
        Args:
            team_name: Nome della squadra
            league_id: ID del campionato (opzionale)
            
        Returns:
            Statistiche della squadra
        """
        # URL ricerca
        search_url = f"{self.base_urls['sofascore']}/search/teams/{team_name}"
        
        try:
            # Cerca la squadra
            soup = self._get_soup(search_url)
            
            # Trova link squadra nei risultati
            team_links = soup.select('a[href*="/team/"]')
            team_url = None
            
            for link in team_links:
                if team_name.lower() in link.text.lower():
                    team_url = self.base_urls['sofascore'] + link['href']
                    break
            
            if not team_url:
                logger.warning(f"Squadra non trovata su SofaScore: {team_name}")
                return {}
            
            # Estrai statistiche dalla pagina squadra
            soup = self._get_soup(team_url)
            
            # Inizializza struttura dati
            stats = {
                'team_info': {
                    'name': team_name,
                    'league': '',
                    'manager': '',
                    'stadium': ''
                },
                'current_form': [],
                'season_stats': {},
                'players': []
            }
            
            # Estrai info squadra
            info_container = soup.select_one('div[class*="team-info"]')
            if info_container:
                # Nome campionato
                league_elem = info_container.select_one('a[href*="/tournament/"]')
                if league_elem:
                    stats['team_info']['league'] = league_elem.text.strip()
                
                # Manager e stadio
                details = info_container.select('div[class*="details"] p')
                for detail in details:
                    text = detail.text.strip()
                    if 'Coach' in text or 'Manager' in text:
                        stats['team_info']['manager'] = text.split(':')[1].strip()
                    elif 'Stadium' in text:
                        stats['team_info']['stadium'] = text.split(':')[1].strip()
            
            # Estrai forma recente
            form_container = soup.select_one('div[class*="recent-form"]')
            if form_container:
                form_items = form_container.select('div[class*="form-item"]')
                for item in form_items:
                    result = 'W' if 'win' in item.get('class', []) else 'L' if 'loss' in item.get('class', []) else 'D'
                    stats['current_form'].append(result)
            
            # Estrai statistiche stagionali
            stats_tables = soup.select('table[class*="stats-table"]')
            for table in stats_tables:
                table_title = table.select_one('div[class*="table-title"]')
                if not table_title:
                    continue
                    
                category = table_title.text.strip().lower().replace(' ', '_')
                stats['season_stats'][category] = {}
                
                rows = table.select('tr')
                for row in rows:
                    cells = row.select('td')
                    if len(cells) < 2:
                        continue
                        
                    stat_name = cells[0].text.strip().lower().replace(' ', '_')
                    stat_value = self._parse_numeric(cells[1].text)
                    stats['season_stats'][category][stat_name] = stat_value
            
            # Estrai valutazioni giocatori
            players_table = soup.select_one('table[class*="players-table"]')
            if players_table:
                rows = players_table.select('tbody tr')
                for row in rows:
                    cells = row.select('td')
                    if len(cells) < 3:
                        continue
                        
                    player_name = cells[0].text.strip()
                    position = cells[1].text.strip()
                    rating = self._parse_numeric(cells[2].text)
                    
                    player_data = {
                        'name': player_name,
                        'position': position,
                        'rating': rating,
                        'stats': {}
                    }
                    
                    # Statistiche aggiuntive
                    for i in range(3, len(cells)):
                        header = players_table.select('th')[i].text.strip().lower().replace(' ', '_')
                        value = self._parse_numeric(cells[i].text)
                        player_data['stats'][header] = value
                    
                    stats['players'].append(player_data)
            
            return stats
        
        except Exception as e:
            logger.error(f"Errore nell'estrazione dati squadra SofaScore: {str(e)}")
            return {}
    
    def _get_fbref_team_stats(self, team_name: str, 
                           league_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Estrae statistiche squadra da FBref.
        
        Args:
            team_name: Nome della squadra
            league_id: ID del campionato (opzionale)
            
        Returns:
            Statistiche della squadra
        """
        try:
            # Normalizza il nome per la ricerca
            team_name = team_name.lower().replace(' ', '-')
            
            # URL ricerca
            search_url = f"{self.base_urls['fbref']}/search/search.fcgi?search={team_name}"
            
            # Cerca la squadra
            soup = self._get_soup(search_url)
            
            # Trova link squadra nei risultati
            team_section = soup.select_one('div#teams')
            if not team_section:
                logger.warning(f"Nessun risultato squadra per {team_name} su FBref")
                return {}
                
            team_link = team_section.select_one('div.search-item-url')
            if not team_link:
                logger.warning(f"Link squadra non trovato per {team_name} su FBref")
                return {}
                
            team_url = team_link.text.strip()
            
            # Estrai statistiche dalla pagina squadra
            soup = self._get_soup(team_url)
            
            # Inizializza struttura dati
            stats = {
                'team_info': {
                    'name': team_name,
                    'league': '',
                    'manager': '',
                    'stadium': ''
                },
                'current_form': [],
                'season_stats': {},
                'players': []
            }
            
            # Estrai info squadra
            info_div = soup.select_one('div#meta')
            if info_div:
                # Nome del campionato
                league_p = info_div.find('p', string=re.compile('League'))
                if league_p:
                    stats['team_info']['league'] = league_p.find('a').text.strip()
                
                # Manager
                manager_p = info_div.find('p', string=re.compile('Manager'))
                if manager_p:
                    stats['team_info']['manager'] = manager_p.find('a').text.strip()
            
            # Estrai statistiche stagionali
            stats_tables = soup.select('table.stats_table')
            for table in stats_tables:
                table_id = table.get('id', '')
                
                if 'stats' in table_id:
                    category = table_id.replace('stats_', '').lower()
                    stats['season_stats'][category] = {}
                    
                    rows = table.select('tbody tr')
                    for row in rows:
                        cells = row.select('td')
                        if len(cells) < 2:
                            continue
                            
                        stat_name = row.select_one('th').text.strip().lower().replace(' ', '_')
                        for cell in cells:
                            if cell.get('data-stat'):
                                stat_type = cell.get('data-stat').lower().replace(' ', '_')
                                stat_value = self._parse_numeric(cell.text)
                                stats['season_stats'][category][f"{stat_name}_{stat_type}"] = stat_value
            
            # Estrai giocatori
            players_table = soup.select_one('table#stats_standard_squads')
            if players_table:
                rows = players_table.select('tbody tr')
                for row in rows:
                    cells = row.select('td')
                    if len(cells) < 3:
                        continue
                        
                    player_name = row.select_one('th').text.strip()
                    
                    player_data = {
                        'name': player_name,
                        'position': '',
                        'stats': {}
                    }
                    
                    for cell in cells:
                        if cell.get('data-stat'):
                            stat_name = cell.get('data-stat').lower().replace(' ', '_')
                            stat_value = self._parse_numeric(cell.text)
                            
                            if stat_name == 'pos':
                                player_data['position'] = cell.text.strip()
                            else:
                                player_data['stats'][stat_name] = stat_value
                    
                    stats['players'].append(player_data)
            
            return stats
        
        except Exception as e:
            logger.error(f"Errore nell'estrazione dati squadra FBref: {str(e)}")
            return {}
    
    def _get_footystats_team_stats(self, team_name: str, 
                                league_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Estrae statistiche squadra da FootyStats.
        
        Args:
            team_name: Nome della squadra
            league_id: ID del campionato (opzionale)
            
        Returns:
            Statistiche della squadra
        """
        try:
            # Normalizza il nome per la ricerca
            team_name = team_name.lower().replace(' ', '-')
            
            # URL squadra
            team_url = f"{self.base_urls['footystats']}/team/{team_name}"
            
            # Estrai statistiche
            soup = self._get_soup(team_url)
            
            # Inizializza struttura dati
            stats = {
                'team_info': {
                    'name': team_name,
                    'league': '',
                    'stadium': ''
                },
                'season_stats': {},
                'form': []
            }
            
            # Estrai info squadra
            info_div = soup.select_one('div.team-name-container')
            if info_div:
                # Nome squadra completo
                team_name_h1 = info_div.select_one('h1')
                if team_name_h1:
                    stats['team_info']['name'] = team_name_h1.text.strip()
                
                # Campionato
                league_a = info_div.select_one('a[href*="/league/"]')
                if league_a:
                    stats['team_info']['league'] = league_a.text.strip()
            
            # Estrai statistiche stagionali
            stats_tables = soup.select('table.team-stats-table')
            for table in stats_tables:
                caption = table.select_one('caption')
                if not caption:
                    continue
                    
                category = caption.text.strip().lower().replace(' ', '_')
                stats['season_stats'][category] = {}
                
                rows = table.select('tr')
                for row in rows:
                    cells = row.select('td')
                    if len(cells) < 2:
                        continue
                        
                    stat_name = cells[0].text.strip().lower().replace(' ', '_')
                    stat_value = self._parse_numeric(cells[1].text)
                    stats['season_stats'][category][stat_name] = stat_value
            
            # Estrai forma recente
            form_div = soup.select_one('div.team-form')
            if form_div:
                form_items = form_div.select('span.form-item')
                for item in form_items:
                    result = item.text.strip()
                    stats['form'].append(result)
            
            return stats
        
        except Exception as e:
            logger.error(f"Errore nell'estrazione dati squadra FootyStats: {str(e)}")
            return {}
    
    def _process_team_matches(self, team_id: str, team_name: str, 
                            matches: Dict[str, Any]) -> Dict[str, Any]:
        """
        Processa le partite di una squadra per ottenere statistiche aggregate.
        
        Args:
            team_id: ID della squadra
            team_name: Nome della squadra
            matches: Dizionario di partite
            
        Returns:
            Statistiche aggregate della squadra
        """
        # Inizializza struttura risultante
        team_stats = {
            'team_id': team_id,
            'team_name': team_name,
            'matches_analyzed': len(matches),
            'home_matches': 0,
            'away_matches': 0,
            'results': {
                'wins': 0,
                'draws': 0,
                'losses': 0
            },
            'goals': {
                'scored': 0,
                'conceded': 0,
                'avg_scored': 0,
                'avg_conceded': 0
            },
            'xg': {
                'total_for': 0,
                'total_against': 0,
                'avg_for': 0,
                'avg_against': 0
            },
            'stats': {
                'corners': {
                    'total': 0,
                    'avg': 0
                },
                'shots': {
                    'total': 0,
                    'on_target': 0,
                    'avg': 0
                },
                'possession': {
                    'total': 0,
                    'avg': 0
                },
                'cards': {
                    'yellow': 0,
                    'red': 0
                }
            },
            'form': [],
            'detailed_matches': []
        }
        
        # Processa ogni partita
        for match_id, match in matches.items():
            # Determina se la squadra gioca in casa o trasferta
            is_home = match.get('home_team_id') == team_id
            
            if is_home:
                team_stats['home_matches'] += 1
            else:
                team_stats['away_matches'] += 1
            
            # Estrai risultato
            home_score = match.get('home_score')
            away_score = match.get('away_score')
            
            if home_score is not None and away_score is not None:
                team_score = home_score if is_home else away_score
                opponent_score = away_score if is_home else home_score
                
                # Registra gol
                team_stats['goals']['scored'] += team_score
                team_stats['goals']['conceded'] += opponent_score
                
                # Registra risultato
                if team_score > opponent_score:
                    team_stats['results']['wins'] += 1
                    team_stats['form'].append('W')
                elif team_score < opponent_score:
                    team_stats['results']['losses'] += 1
                    team_stats['form'].append('L')
                else:
                    team_stats['results']['draws'] += 1
                    team_stats['form'].append('D')
            
            # Estrai xG se disponibile
            if 'xg' in match:
                xg_for = match['xg']['home'] if is_home else match['xg']['away']
                xg_against = match['xg']['away'] if is_home else match['xg']['home']
                
                team_stats['xg']['total_for'] += xg_for
                team_stats['xg']['total_against'] += xg_against
            
            # Estrai statistiche aggiuntive se disponibili
            if 'stats' in match:
                stats = match['stats']
                
                # Corners
                if 'corners' in stats:
                    corners = stats['corners']['home'] if is_home else stats['corners']['away']
                    team_stats['stats']['corners']['total'] += corners
                
                # Tiri
                if 'shots' in stats:
                    shots = stats['shots']['home'] if is_home else stats['shots']['away']
                    team_stats['stats']['shots']['total'] += shots
                
                if 'shots_on_target' in stats:
                    shots_ot = stats['shots_on_target']['home'] if is_home else stats['shots_on_target']['away']
                    team_stats['stats']['shots']['on_target'] += shots_ot
                
                # Possesso
                if 'possession' in stats:
                    possession = stats['possession']['home'] if is_home else stats['possession']['away']
                    team_stats['stats']['possession']['total'] += possession
                
                # Cartellini
                if 'cards' in stats:
                    yellow = stats['cards']['yellow']['home'] if is_home else stats['cards']['yellow']['away']
                    red = stats['cards']['red']['home'] if is_home else stats['cards']['red']['away']
                    
                    team_stats['stats']['cards']['yellow'] += yellow
                    team_stats['stats']['cards']['red'] += red
            
            # Aggiungi dettagli partita
            opponent_id = match.get('away_team_id') if is_home else match.get('home_team_id')
            opponent_name = match.get('away_team') if is_home else match.get('home_team')
            
            match_details = {
                'match_id': match_id,
                'date': match.get('datetime', ''),
                'home_away': 'home' if is_home else 'away',
                'opponent_id': opponent_id,
                'opponent_name': opponent_name,
                'score': f"{team_score}-{opponent_score}" if home_score is not None and away_score is not None else 'N/A',
                'result': team_stats['form'][-1] if team_stats['form'] else 'N/A'
            }
            
            team_stats['detailed_matches'].append(match_details)
        
        # Calcola medie
        matches_count = len(matches)
        if matches_count > 0:
            team_stats['goals']['avg_scored'] = team_stats['goals']['scored'] / matches_count
            team_stats['goals']['avg_conceded'] = team_stats['goals']['conceded'] / matches_count
            
            team_stats['xg']['avg_for'] = team_stats['xg']['total_for'] / matches_count if team_stats['xg']['total_for'] > 0 else 0
            team_stats['xg']['avg_against'] = team_stats['xg']['total_against'] / matches_count if team_stats['xg']['total_against'] > 0 else 0
            
            team_stats['stats']['corners']['avg'] = team_stats['stats']['corners']['total'] / matches_count
            team_stats['stats']['shots']['avg'] = team_stats['stats']['shots']['total'] / matches_count
            team_stats['stats']['possession']['avg'] = team_stats['stats']['possession']['total'] / matches_count
        
        return team_stats
    
    @cached(ttl=3600)  # 1 ora
    def get_player_statistics(self, player_name: str, team_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Ottiene statistiche avanzate per un giocatore.
        
        Args:
            player_name: Nome del giocatore
            team_id: ID della squadra (opzionale, migliora la ricerca)
            
        Returns:
            Statistiche complete del giocatore
        """
        logger.info(f"Raccolta statistiche per giocatore: {player_name}")
        
        # Se abbiamo team_id, ottieni info squadra
        team_name = ""
        if team_id:
            team_data = self.db.get_reference(f"data/teams/{team_id}").get()
            if team_data:
                team_name = team_data.get('name', '')
        
        # Prova fonte primaria
        source = self.preferred_source
        player_stats = self._get_player_stats_from_source(source, player_name, team_name)
        
        # Se la fonte primaria fallisce, prova alternative
        if not player_stats:
            for alt_source in self.base_urls.keys():
                if alt_source != self.preferred_source:
                    logger.info(f"Tentativo con fonte alternativa: {alt_source}")
                    player_stats = self._get_player_stats_from_source(alt_source, player_name, team_name)
                    if player_stats:
                        break
        
        if not player_stats:
            logger.warning(f"Nessuna statistica trovata per {player_name}")
            return {}
        
        # Aggiungi metadati
        player_stats['player_name'] = player_name
        player_stats['team_id'] = team_id
        player_stats['source'] = source
        player_stats['collected_at'] = datetime.datetime.now().isoformat()
        
        return player_stats
    
    def _get_player_stats_from_source(self, source: str, player_name: str, 
                                    team_name: str = "") -> Dict[str, Any]:
        """
        Ottiene statistiche di un giocatore da una fonte specifica.
        
        Args:
            source: Nome della fonte
            player_name: Nome del giocatore
            team_name: Nome della squadra (opzionale)
            
        Returns:
            Statistiche del giocatore dalla fonte specificata
        """
        source_methods = {
            'footystats': self._get_footystats_player_stats,
            'fbref': self._get_fbref_player_stats,
            'sofascore': self._get_sofascore_player_stats
        }
        
        method = source_methods.get(source)
        if not method:
            logger.warning(f"Fonte non supportata: {source}")
            return {}
        
        try:
            return method(player_name, team_name)
        except Exception as e:
            logger.error(f"Errore nell'estrazione da {source}: {str(e)}")
            return {}
    
    def _get_sofascore_player_stats(self, player_name: str, team_name: str = "") -> Dict[str, Any]:
        """
        Estrae statistiche giocatore da SofaScore.
        
        Args:
            player_name: Nome del giocatore
            team_name: Nome della squadra (opzionale)
            
        Returns:
            Statistiche del giocatore
        """
        search_query = f"{player_name}"
        if team_name:
            search_query += f" {team_name}"
            
        search_url = f"{self.base_urls['sofascore']}/search/players/{search_query}"
        
        try:
            # Cerca il giocatore
            soup = self._get_soup(search_url)
            
            # Trova link giocatore nei risultati
            player_links = soup.select('a[href*="/player/"]')
            player_url = None
            
            for link in player_links:
                if player_name.lower() in link.text.lower():
                    player_url = self.base_urls['sofascore'] + link['href']
                    break
            
            if not player_url:
                logger.warning(f"Giocatore non trovato su SofaScore: {player_name}")
                return {}
            
            # Estrai statistiche dalla pagina giocatore
            soup = self._get_soup(player_url)
            
            # Inizializza struttura dati
            stats = {
                'player_info': {
                    'name': player_name,
                    'team': '',
                    'position': '',
                    'nationality': '',
                    'age': None,
                    'height': None,
                    'weight': None
                },
                'season_stats': {},
                'ratings': {
                    'overall': None,
                    'by_category': {}
                },
                'last_matches': []
            }
            
            # Estrai info giocatore
            info_div = soup.select_one('div[class*="player-info"]')
            if info_div:
                # Team
                team_elem = info_div.select_one('a[href*="/team/"]')
                if team_elem:
                    stats['player_info']['team'] = team_elem.text.strip()
                
                # Position
                position_elem = info_div.select_one('div[class*="player-pos"]')
                if position_elem:
                    stats['player_info']['position'] = position_elem.text.strip()
                
                # Altri dettagli
                details = info_div.select('div[class*="details"] p')
                for detail in details:
                    text = detail.text.strip()
                    if 'Age' in text:
                        stats['player_info']['age'] = self._parse_numeric(text.split(':')[1])
                    elif 'Height' in text:
                        stats['player_info']['height'] = self._parse_numeric(text.split(':')[1])
                    elif 'Weight' in text:
                        stats['player_info']['weight'] = self._parse_numeric(text.split(':')[1])
                    elif 'Nationality' in text:
                        stats['player_info']['nationality'] = text.split(':')[1].strip()
            
            # Estrai valutazione
            rating_div = soup.select_one('div[class*="rating"]')
            if rating_div:
                stats['ratings']['overall'] = self._parse_numeric(rating_div.text)
            
            # Estrai statistiche stagionali
            stats_tables = soup.select('table[class*="stats-table"]')
            for table in stats_tables:
                table_title = table.select_one('div[class*="table-title"]')
                if not table_title:
                    continue
                    
                category = table_title.text.strip().lower().replace(' ', '_')
                stats['season_stats'][category] = {}
                
                rows = table.select('tr')
                for row in rows:
                    cells = row.select('td')
                    if len(cells) < 2:
                        continue
                        
                    stat_name = cells[0].text.strip().lower().replace(' ', '_')
                    stat_value = self._parse_numeric(cells[1].text)
                    stats['season_stats'][category][stat_name] = stat_value
            
            # Estrai ultime partite
            matches_table = soup.select_one('table[class*="matches-table"]')
            if matches_table:
                rows = matches_table.select('tbody tr')
                for row in rows:
                    cells = row.select('td')
                    if len(cells) < 4:
                        continue
                        
                    date = cells[0].text.strip()
                    opponent = cells[1].text.strip()
                    result = cells[2].text.strip()
                    rating = self._parse_numeric(cells[3].text)
                    
                    match_data = {
                        'date': date,
                        'opponent': opponent,
                        'result': result,
                        'rating': rating
                    }
                    
                    stats['last_matches'].append(match_data)
            
            return stats
        
        except Exception as e:
            logger.error(f"Errore nell'estrazione dati giocatore SofaScore: {str(e)}")
            return {}
    
    def _get_fbref_player_stats(self, player_name: str, team_name: str = "") -> Dict[str, Any]:
        """
        Estrae statistiche giocatore da FBref.
        
        Args:
            player_name: Nome del giocatore
            team_name: Nome della squadra (opzionale)
            
        Returns:
            Statistiche del giocatore
        """
        try:
            # Normalizza il nome per la ricerca
