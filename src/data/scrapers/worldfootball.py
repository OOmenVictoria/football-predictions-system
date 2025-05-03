"""
Modulo per l'estrazione di dati da WorldFootball.net.
Questo modulo fornisce funzionalità per estrarre dati storici e attuali su partite,
campionati e statistiche dal sito WorldFootball.net.
"""
import re
import json
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union, Tuple

import requests
from bs4 import BeautifulSoup

from src.data.scrapers.base_scraper import BaseScraper
from src.utils.cache import cached
from src.utils.time_utils import parse_date, date_to_str
from src.config.settings import get_setting

logger = logging.getLogger(__name__)

class WorldFootballScraper(BaseScraper):
    """
    Scraper per estrarre dati da WorldFootball.net.
    
    Questo scraper permette di ottenere:
    - Statistiche di campionati attuali e storici
    - Risultati di partite
    - Dati su coppe e competizioni internazionali
    - Statistiche di squadre e record storici
    """
    
    def __init__(self, cache_ttl: int = None):
        """
        Inizializza lo scraper WorldFootball.
        
        Args:
            cache_ttl: Tempo di vita della cache in secondi (default da settings)
        """
        base_url = "https://www.worldfootball.net"
        cache_ttl = cache_ttl or get_setting('scrapers.worldfootball.cache_ttl', 86400)  # 24h default
        
        super().__init__(
            base_url=base_url,
            name="worldfootball",
            cache_ttl=cache_ttl
        )
        
        # Impostazioni specifiche per WorldFootball
        self.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9'
        })
        
        # Mappatura dei campionati principali
        self.leagues_map = {
            'premier_league': 'eng-premier-league',
            'championship': 'eng-championship',
            'serie_a': 'ita-serie-a',
            'serie_b': 'ita-serie-b',
            'la_liga': 'esp-primera-division',
            'la_liga_2': 'esp-segunda-division',
            'bundesliga': 'bundesliga',
            'bundesliga_2': '2-bundesliga',
            'ligue_1': 'fra-ligue-1',
            'ligue_2': 'fra-ligue-2',
            'primeira_liga': 'por-primeira-liga',
            'eredivisie': 'ned-eredivisie',
            'super_lig': 'tur-sueperlig',
            'primeira_divisao': 'bra-serie-a',
            'superliga_argentina': 'arg-primera-division',
            'j1_league': 'jpn-j1-league',
            'mls': 'usa-major-league-soccer'
        }
        
        # Mappatura delle competizioni internazionali
        self.competitions_map = {
            'champions_league': 'champions-league',
            'europa_league': 'europa-league',
            'conference_league': 'uefa-conference-league',
            'world_cup': 'world-cup',
            'copa_libertadores': 'copa-libertadores',
            'copa_sudamericana': 'copa-sudamericana',
            'euro': 'european-championships',
            'copa_america': 'copa-america'
        }
        
        logger.info(f"WorldFootballScraper inizializzato con cache TTL: {cache_ttl}s")

    # ---- Metodi principali per ottenere dati ---- #
    
    @cached(ttl=86400 * 7)  # 7 giorni
    def get_league_seasons(self, league_id: str) -> List[Dict[str, Any]]:
        """
        Ottiene le stagioni disponibili per un campionato.
        
        Args:
            league_id: ID del campionato (chiave da leagues_map o URL diretto)
            
        Returns:
            Lista di stagioni disponibili
        """
        logger.info(f"Recuperando stagioni per campionato: {league_id}")
        
        try:
            # Converti l'ID del campionato se necessario
            league_path = self._get_league_path(league_id)
            url = f"{self.base_url}/competitions/{league_path}/"
            
            soup = self.get_soup(url)
            seasons = []
            
            # Cerca nella sezione delle stagioni
            season_links = soup.select("div.data table.competition-rounds a")
            for link in season_links:
                season_url = link.get('href', '')
                if not season_url:
                    continue
                    
                season_name = link.text.strip()
                season_id = season_url.strip('/').split('/')[-1]
                
                # Cerca di estrarre l'anno
                year_match = re.search(r'(\d{4})-(\d{4}|\d{2})', season_name)
                year = None
                if year_match:
                    year = year_match.group(1)
                
                seasons.append({
                    'id': season_id,
                    'name': season_name,
                    'year': year,
                    'url': self.base_url + season_url
                })
            
            return sorted(seasons, key=lambda x: x.get('year', '0'), reverse=True)
            
        except Exception as e:
            logger.error(f"Errore nel recupero stagioni per campionato {league_id}: {str(e)}")
            return []
    
    @cached(ttl=86400)  # 24 ore
    def get_league_table(self, league_id: str, season: Optional[str] = None) -> Dict[str, Any]:
        """
        Ottiene la classifica di un campionato per una stagione specifica.
        
        Args:
            league_id: ID del campionato (chiave da leagues_map o URL diretto)
            season: ID della stagione (opzionale, default: stagione corrente)
            
        Returns:
            Classifica completa
        """
        logger.info(f"Recuperando classifica per campionato: {league_id}, stagione: {season}")
        
        try:
            # Converti l'ID del campionato se necessario
            league_path = self._get_league_path(league_id)
            
            # Se la stagione non è specificata, cerca di ottenere quella corrente
            if not season:
                seasons = self.get_league_seasons(league_id)
                if seasons:
                    season = seasons[0]['id']  # Prima stagione (quella più recente)
                else:
                    logger.warning(f"Nessuna stagione trovata per campionato {league_id}")
                    return {'league_id': league_id, 'teams': []}
            
            url = f"{self.base_url}/competitions/{league_path}/{season}/table/"
            soup = self.get_soup(url)
            
            # Estrai nome campionato e stagione dal titolo
            title_elem = soup.select_one("div.content div.box h1")
            league_name = title_elem.text.strip() if title_elem else ""
            
            # Cerca la tabella con la classifica
            table = soup.select_one("table.standard_tabelle")
            if not table:
                logger.warning(f"Tabella classifica non trovata per {league_id} - {season}")
                return {'league_id': league_id, 'league_name': league_name, 'season': season, 'teams': []}
            
            teams = []
            
            # Estrai righe della tabella (ignora intestazione)
            rows = table.select("tr:not(.thead)")
            for row in rows:
                # Ignora righe che non hanno abbastanza celle
                cells = row.select("td")
                if len(cells) < 10:
                    continue
                
                # Estrai dati team
                position = cells[0].text.strip()
                
                # Il nome della squadra può essere in formato diverso, cerca quello corretto
                team_cell = cells[2]
                team_link = team_cell.select_one("a")
                
                if not team_link:
                    continue
                    
                team_name = team_link.text.strip()
                team_url = team_link.get('href', '')
                team_id = self._extract_team_id(team_url)
                
                # Estrai statistiche
                matches_played = int(cells[3].text.strip()) if cells[3].text.strip().isdigit() else 0
                wins = int(cells[4].text.strip()) if cells[4].text.strip().isdigit() else 0
                draws = int(cells[5].text.strip()) if cells[5].text.strip().isdigit() else 0
                losses = int(cells[6].text.strip()) if cells[6].text.strip().isdigit() else 0
                
                goals_for = cells[7].text.strip().split(':')[0]
                goals_against = cells[7].text.strip().split(':')[1]
                goals_for = int(goals_for) if goals_for.isdigit() else 0
                goals_against = int(goals_against) if goals_against.isdigit() else 0
                
                # Estrai punti
                points = int(cells[8].text.strip()) if cells[8].text.strip().isdigit() else 0
                
                teams.append({
                    'position': position,
                    'id': team_id,
                    'name': team_name,
                    'url': self.base_url + team_url if team_url.startswith('/') else team_url,
                    'matches_played': matches_played,
                    'wins': wins,
                    'draws': draws,
                    'losses': losses,
                    'goals_for': goals_for,
                    'goals_against': goals_against,
                    'goal_difference': goals_for - goals_against,
                    'points': points
                })
            
            return {
                'league_id': league_id,
                'league_name': league_name,
                'season': season,
                'teams': teams
            }
            
        except Exception as e:
            logger.error(f"Errore nel recupero classifica per campionato {league_id}, stagione {season}: {str(e)}")
            return {'league_id': league_id, 'season': season, 'teams': []}
    
    @cached(ttl=86400)  # 24 ore
    def get_league_fixtures(self, league_id: str, season: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Ottiene le partite di un campionato per una stagione specifica.
        
        Args:
            league_id: ID del campionato (chiave da leagues_map o URL diretto)
            season: ID della stagione (opzionale, default: stagione corrente)
            
        Returns:
            Lista di partite
        """
        logger.info(f"Recuperando partite per campionato: {league_id}, stagione: {season}")
        
        try:
            # Converti l'ID del campionato se necessario
            league_path = self._get_league_path(league_id)
            
            # Se la stagione non è specificata, cerca di ottenere quella corrente
            if not season:
                seasons = self.get_league_seasons(league_id)
                if seasons:
                    season = seasons[0]['id']  # Prima stagione (quella più recente)
                else:
                    logger.warning(f"Nessuna stagione trovata per campionato {league_id}")
                    return []
            
            url = f"{self.base_url}/competitions/{league_path}/{season}/matches/"
            soup = self.get_soup(url)
            
            matches = []
            
            # Estrai le giornate del campionato
            rounds_tables = soup.select("div.data > h2, div.data > table.standard_tabelle")
            current_round = None
            
            for element in rounds_tables:
                # Se è un'intestazione, imposta la giornata corrente
                if element.name == 'h2':
                    current_round = element.text.strip()
                # Se è una tabella, estrai le partite
                elif element.name == 'table' and current_round:
                    round_matches = self._extract_matches_from_table(element, current_round, league_id, season)
                    matches.extend(round_matches)
            
            return matches
            
        except Exception as e:
            logger.error(f"Errore nel recupero partite per campionato {league_id}, stagione {season}: {str(e)}")
            return []
    
    @cached(ttl=43200)  # 12 ore
    def get_match_details(self, match_id: str) -> Dict[str, Any]:
        """
        Ottiene dettagli completi su una partita.
        
        Args:
            match_id: ID della partita
            
        Returns:
            Dettagli completi sulla partita
        """
        logger.info(f"Recuperando dettagli partita: {match_id}")
        
        try:
            url = f"{self.base_url}/matches/{match_id}/"
            soup = self.get_soup(url)
            
            # Estrai dati di base
            match_details = self._extract_match_header(soup)
            
            # Estrai formazioni
            lineups = self._extract_match_lineups(soup)
            if lineups:
                match_details['lineups'] = lineups
            
            # Estrai eventi (gol, cartellini, ecc.)
            events = self._extract_match_events(soup)
            if events:
                match_details['events'] = events
            
            # Estrai statistiche
            stats = self._extract_match_stats(soup)
            if stats:
                match_details['stats'] = stats
            
            return match_details
            
        except Exception as e:
            logger.error(f"Errore nel recupero dettagli partita {match_id}: {str(e)}")
            return {'match_id': match_id, 'error': str(e)}
    
    @cached(ttl=86400 * 7)  # 7 giorni
    def get_team_info(self, team_id: str) -> Dict[str, Any]:
        """
        Ottiene informazioni su una squadra.
        
        Args:
            team_id: ID della squadra
            
        Returns:
            Informazioni sulla squadra
        """
        logger.info(f"Recuperando informazioni squadra: {team_id}")
        
        try:
            url = f"{self.base_url}/teams/{team_id}/"
            soup = self.get_soup(url)
            
            # Estrai nome squadra
            title_elem = soup.select_one("div.content div.box h1")
            team_name = title_elem.text.strip() if title_elem else ""
            
            # Estrai paese
            country_elem = soup.select_one("div.content div.box p.breadcrumb")
            country = country_elem.text.strip().split(' » ')[0] if country_elem else ""
            
            # Estrai info dal riquadro principale
            info_box = soup.select_one("div.data table.standard_tabelle")
            
            team_info = {
                'id': team_id,
                'name': team_name,
                'country': country,
                'url': url
            }
            
            # Estrai dettagli aggiuntivi se disponibili
            if info_box:
                for row in info_box.select("tr"):
                    cells = row.select("td")
                    if len(cells) >= 2:
                        label = cells[0].text.strip(':').strip().lower().replace(' ', '_')
                        value = cells[1].text.strip()
                        
                        if label and value:
                            team_info[label] = value
            
            # Estrai rosa attuale
            squad_link = soup.select_one("div.navi a[href*='/teams/'][href*='/roster/']")
            if squad_link:
                team_info['squad_url'] = self.base_url + squad_link.get('href', '')
            
            return team_info
            
        except Exception as e:
            logger.error(f"Errore nel recupero informazioni squadra {team_id}: {str(e)}")
            return {'id': team_id, 'error': str(e)}
    
    @cached(ttl=86400)  # 24 ore
    def get_team_squad(self, team_id: str, season: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Ottiene la rosa di una squadra per una stagione specifica.
        
        Args:
            team_id: ID della squadra
            season: ID della stagione (opzionale, default: stagione corrente)
            
        Returns:
            Lista di giocatori
        """
        logger.info(f"Recuperando rosa squadra: {team_id}, stagione: {season}")
        
        try:
            # Se la stagione è specificata, usa l'URL con la stagione
            # altrimenti usa l'URL della rosa attuale
            if season:
                url = f"{self.base_url}/teams/{team_id}/roster/season/{season}/"
            else:
                url = f"{self.base_url}/teams/{team_id}/roster/"
            
            soup = self.get_soup(url)
            
            # Estrai tabella rosa
            squad_table = soup.select_one("div.data table.standard_tabelle")
            if not squad_table:
                logger.warning(f"Tabella rosa non trovata per squadra {team_id}")
                return []
            
            players = []
            
            # Estrai giocatori dalla tabella
            for row in squad_table.select("tr:not(.thead)"):
                cells = row.select("td")
                if len(cells) < 5:
                    continue
                
                # Estrai dati giocatore
                player_link = cells[1].select_one("a")
                if not player_link:
                    continue
                    
                player_name = player_link.text.strip()
                player_url = player_link.get('href', '')
                player_id = self._extract_player_id(player_url)
                
                # Estrai numero di maglia
                jersey_number = cells[0].text.strip()
                
                # Estrai posizione
                position = cells[2].text.strip()
                
                # Estrai data di nascita
                birth_date_text = cells[3].text.strip()
                birth_date = None
                if birth_date_text:
                    try:
                        birth_date = datetime.strptime(birth_date_text, '%d.%m.%Y').strftime('%Y-%m-%d')
                    except ValueError:
                        pass
                
                # Estrai nazionalità
                nationality_elem = cells[4].select_one("img")
                nationality = nationality_elem.get('alt', '') if nationality_elem else cells[4].text.strip()
                
                players.append({
                    'id': player_id,
                    'name': player_name,
                    'url': self.base_url + player_url if player_url.startswith('/') else player_url,
                    'jersey_number': jersey_number,
                    'position': position,
                    'birth_date': birth_date,
                    'nationality': nationality
                })
            
            return players
            
        except Exception as e:
            logger.error(f"Errore nel recupero rosa squadra {team_id}: {str(e)}")
            return []
    
    @cached(ttl=86400)  # 24 ore
    def get_team_fixtures(self, team_id: str, season: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Ottiene le partite di una squadra per una stagione specifica.
        
        Args:
            team_id: ID della squadra
            season: ID della stagione (opzionale, default: stagione corrente)
            
        Returns:
            Lista di partite
        """
        logger.info(f"Recuperando partite squadra: {team_id}, stagione: {season}")
        
        try:
            # Se la stagione è specificata, usa l'URL con la stagione
            # altrimenti usa l'URL delle partite attuali
            if season:
                url = f"{self.base_url}/teams/{team_id}/fixtures/season/{season}/"
            else:
                url = f"{self.base_url}/teams/{team_id}/fixtures/"
            
            soup = self.get_soup(url)
            
            # Estrai tabelle partite (possono essere più di una per competizioni diverse)
            tables = soup.select("div.data table.standard_tabelle")
            
            matches = []
            current_competition = None
            
            # Per ogni tabella di partite
            for table_idx, table in enumerate(tables):
                # Cerca il titolo della competizione (può essere in un h2 prima della tabella)
                comp_title = soup.select(f"div.data > h2")[table_idx] if len(soup.select("div.data > h2")) > table_idx else None
                if comp_title:
                    current_competition = comp_title.text.strip()
                
                # Estrai partite dalla tabella
                for row in table.select("tr:not(.thead)"):
                    cells = row.select("td")
                    if len(cells) < 7:
                        continue
                    
                    # Estrai dati match
                    date_text = cells[0].text.strip()
                    time_text = cells[1].text.strip()
                    
                    # Converti data e ora in ISO format
                    match_datetime = None
                    if date_text:
                        try:
                            if time_text:
                                match_datetime = datetime.strptime(f"{date_text} {time_text}", '%d.%m.%Y %H:%M').isoformat()
                            else:
                                match_datetime = datetime.strptime(date_text, '%d.%m.%Y').isoformat()
                        except ValueError:
                            pass
                    
                    # Estrai competizione (se non già ottenuta dall'intestazione)
                    competition = current_competition
                    if not competition and len(cells) >= 8:
                        competition = cells[7].text.strip()
                    
                    # Estrai squadre
                    home_team_link = None
                    away_team_link = None
                    home_team_name = cells[2].text.strip()
                    away_team_name = cells[4].text.strip()
                    
                    # Se presenti i link alle squadre, estrai ID
                    home_link = cells[2].select_one("a")
                    if home_link:
                        home_team_link = home_link.get('href', '')
                        
                    away_link = cells[4].select_one("a")
                    if away_link:
                        away_team_link = away_link.get('href', '')
                    
                    # Estrai risultato
                    result = cells[5].text.strip()
                    home_score = None
                    away_score = None
                    
                    if result and ':' in result:
                        score_parts = result.split(':')
                        if len(score_parts) >= 2:
                            home_score = int(score_parts[0].strip()) if score_parts[0].strip().isdigit() else None
                            away_score = int(score_parts[1].strip()) if score_parts[1].strip().isdigit() else None
                    
                    # Estrai link dettagli partita
                    match_id = None
                    match_link_idx = 6  # Indice comune per il link alla partita
                    match_link = cells[match_link_idx].select_one("a[href*='/matches/']") if match_link_idx < len(cells) else None
                    if match_link:
                        match_url = match_link.get('href', '')
                        match_id = self._extract_match_id(match_url)
                    
                    match_data = {
                        'date': match_datetime,
                        'competition': competition,
                        'home_team': {
                            'name': home_team_name,
                            'id': self._extract_team_id(home_team_link) if home_team_link else None
                        },
                        'away_team': {
                            'name': away_team_name,
                            'id': self._extract_team_id(away_team_link) if away_team_link else None
                        },
                        'team_id': team_id  # ID della squadra richiesta
                    }
                    
                    # Aggiungi punteggio se disponibile
                    if home_score is not None and away_score is not None:
                        match_data['home_score'] = home_score
                        match_data['away_score'] = away_score
                        match_data['result'] = f"{home_score}:{away_score}"
                    
                    # Aggiungi ID partita se disponibile
                    if match_id:
                        match_data['id'] = match_id
                    
                    matches.append(match_data)
            
            return matches
            
        except Exception as e:
            logger.error(f"Errore nel recupero partite squadra {team_id}: {str(e)}")
            return []
    
    @cached(ttl=86400 * 7)  # 7 giorni
    def get_player_info(self, player_id: str) -> Dict[str, Any]:
        """
        Ottiene informazioni su un giocatore.
        
        Args:
            player_id: ID del giocatore
            
        Returns:
            Informazioni sul giocatore
        """
        logger.info(f"Recuperando informazioni giocatore: {player_id}")
        
        try:
            url = f"{self.base_url}/players/{player_id}/"
            soup = self.get_soup(url)
            
            # Estrai nome giocatore
            title_elem = soup.select_one("div.content div.box h1")
            player_name = title_elem.text.strip() if title_elem else ""
            
            # Estrai info dal riquadro principale
            info_box = soup.select_one("div.data table.standard_tabelle")
            
            player_info = {
                'id': player_id,
                'name': player_name,
                'url': url
            }
            
            # Estrai dettagli aggiuntivi se disponibili
            if info_box:
                for row in info_box.select("tr"):
                    cells = row.select("td")
                    if len(cells) >= 2:
                        label = cells[0].text.strip(':').strip().lower().replace(' ', '_')
                        value = cells[1].text.strip()
                        
                        if label and value:
                            player_info[label] = value
            
            # Estrai squadra attuale
            current_team_box = soup.select_one("div.data div.portfolio a[href*='/teams/']")
            if current_team_box:
                team_name = current_team_box.text.strip()
                team_url = current_team_box.get('href', '')
                team_id = self._extract_team_id(team_url)
                
                if team_id:
                    player_info['current_team'] = {
                        'id': team_id,
                        'name': team_name,
                        'url': self.base_url + team_url if team_url.startswith('/') else team_url
                    }
            
            return player_info
            
        except Exception as e:
            logger.error(f"Errore nel recupero informazioni giocatore {player_id}: {str(e)}")
            return {'id': player_id, 'error': str(e)}

    # ---- Metodi di supporto per l'estrazione dati ---- #

    def _extract_match_header(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Estrae le informazioni principali dall'intestazione di una pagina di partita.
        
        Args:
            soup: Oggetto BeautifulSoup della pagina
            
        Returns:
            Dati di intestazione della partita
        """
        try:
            match_info = {}
            
            # Estrai ID dalla URL
            url = soup.select_one("link[rel='canonical']")
            if url:
                match_url = url.get('href', '')
                match_id = self._extract_match_id(match_url)
                if match_id:
                    match_info['id'] = match_id
                    match_info['url'] = match_url
            
            # Estrai squadre e punteggio
            title_elem = soup.select_one("div.content div.box h1")
            if title_elem:
                title_text = title_elem.text.strip()
                
                # Cerca di estrarre squadre e risultato dal titolo
                title_match = re.search(r'(.+?)\s*(-|:)\s*(.+?)\s+(\d+):(\d+)', title_text)
                if title_match:
                    home_name = title_match.group(1).strip()
                    away_name = title_match.group(3).strip()
                    home_score = int(title_match.group(4))
                    away_score = int(title_match.group(5))
                    
                    match_info['home_team'] = {'name': home_name}
                    match_info['away_team'] = {'name': away_name}
                    match_info['home_score'] = home_score
                    match_info['away_score'] = away_score
                    match_info['result'] = f"{home_score}:{away_score}"
                    
                    # Determina il vincitore
                    if home_score > away_score:
                        match_info['winner'] = 'home'
                    elif away_score > home_score:
                        match_info['winner'] = 'away'
                    else:
                        match_info['winner'] = 'draw'
                else:
                    # Titolo senza risultato (partita futura)
                    title_match = re.search(r'(.+?)\s*(-|:)\s*(.+)', title_text)
                    if title_match:
                        home_name = title_match.group(1).strip()
                        away_name = title_match.group(3].strip()
                        
                        match_info['home_team'] = {'name': home_name}
                        match_info['away_team'] = {'name': away_name}
            
            # Estrai data, competizione e stadio
            subtitle_elem = soup.select_one("div.content div.box div.data p.standard")
            if subtitle_elem:
                subtitle_text = subtitle_elem.text.strip()
                
                # Estrai data e ora
                date_match = re.search(r'(\d{2}\.\d{2}\.\d{4})', subtitle_text)
                time_match = re.search(r'(\d{2}:\d{2})', subtitle_text)
                
                if date_match:
                    date_text = date_match.group(1)
                    time_text = time_match.group(1) if time_match else "00:00"
                    
                    try:
                        match_datetime = datetime.strptime(f"{date_text} {time_text}", '%d.%m.%Y %H:%M').isoformat()
                        match_info['date'] = match_datetime
                    except ValueError:
                        pass
                
                # Estrai competizione e stadio
                competition_match = re.search(r'([^,]+),', subtitle_text)
                stadium_match = re.search(r'Stadium: ([^,]+)', subtitle_text)
                
                if competition_match:
                    match_info['competition'] = competition_match.group(1).strip()
                
                if stadium_match:
                    match_info['stadium'] = stadium_match.group(1).strip()
            
            # Estrai link alle squadre per ottenere gli ID
            team_links = soup.select("div.content div.box div.data table.standard_tabelle td.dunkel a[href*='/teams/']")
            if len(team_links) >= 2:
                home_link = team_links[0].get('href', '')
                away_link = team_links[1].get('href', '')
                
                home_id = self._extract_team_id(home_link)
                away_id = self._extract_team_id(away_link)
                
                if home_id and 'home_team' in match_info:
                    match_info['home_team']['id'] = home_id
                    match_info['home_team']['url'] = self.base_url + home_link if home_link.startswith('/') else home_link
                
                if away_id and 'away_team' in match_info:
                    match_info['away_team']['id'] = away_id
                    match_info['away_team']['url'] = self.base_url + away_link if away_link.startswith('/') else away_link
            
            return match_info
            
        except Exception as e:
            logger.error(f"Errore nell'estrazione intestazione partita: {str(e)}")
            return {}
    
    def _extract_match_lineups(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Estrae le formazioni delle squadre da una pagina di partita.
        
        Args:
            soup: Oggetto BeautifulSoup della pagina
            
        Returns:
            Dati sulle formazioni
        """
        try:
            lineups = {
                'home': {'starting': [], 'substitutes': []},
                'away': {'starting': [], 'substitutes': []}
            }
            
            # Cerca la sezione delle formazioni
            lineup_section = soup.select_one("div.content div.box div.data div.aufstellung")
            if not lineup_section:
                return None
            
            # Estrai formazione titolare
            home_starters = lineup_section.select("div.aufstellung-team-a div.aufstellung-player-box")
            away_starters = lineup_section.select("div.aufstellung-team-b div.aufstellung-player-box")
            
            # Elabora titolari squadra in casa
            for player_box in home_starters:
                player = self._extract_lineup_player(player_box)
                if player:
                    lineups['home']['starting'].append(player)
            
            # Elabora titolari squadra in trasferta
            for player_box in away_starters:
                player = self._extract_lineup_player(player_box)
                if player:
                    lineups['away']['starting'].append(player)
            
            # Estrai panchine
            substitutes_section = soup.select("div.content div.box div.data table.standard_tabelle")
            if len(substitutes_section) >= 3:  # Spesso la terza tabella contiene le panchine
                subs_table = substitutes_section[2]
                
                # Determina quale colonna è per la squadra di casa e quale per quella in trasferta
                headers = subs_table.select("tr.dunkel th")
                if len(headers) >= 2:
                    home_subs = []
                    away_subs = []
                    
                    # Leggi le righe
                    for row in subs_table.select("tr:not(.dunkel)"):
                        cells = row.select("td")
                        if len(cells) >= 2:
                            # Estrai sostituto squadra in casa
                            home_sub = self._extract_substitute_player(cells[0])
                            if home_sub:
                                home_subs.append(home_sub)
                            
                            # Estrai sostituto squadra in trasferta
                            away_sub = self._extract_substitute_player(cells[1])
                            if away_sub:
                                away_subs.append(away_sub)
                    
                    lineups['home']['substitutes'] = home_subs
                    lineups['away']['substitutes'] = away_subs
            
            # Estrai allenatori
            coaches_section = soup.select("div.content div.box div.data table.standard_tabelle")
            if len(coaches_section) >= 2:  # Di solito la seconda tabella contiene gli allenatori
                coaches_table = coaches_section[1]
                
                # Leggi le righe
                for row in coaches_table.select("tr"):
                    cells = row.select("td")
                    if len(cells) >= 2:
                        # Verifica se è la riga dell'allenatore
                        if 'Coach' in cells[0].text or 'Trainer' in cells[0].text:
                            # Estrai allenatore squadra in casa
                            home_coach = cells[1].text.strip()
                            if home_coach:
                                lineups['home']['coach'] = home_coach
                            
                            # Estrai allenatore squadra in trasferta
                            if len(cells) >= 3:
                                away_coach = cells[2].text.strip()
                                if away_coach:
                                    lineups['away']['coach'] = away_coach
            
            return lineups
            
        except Exception as e:
            logger.error(f"Errore nell'estrazione formazioni: {str(e)}")
            return None
    
    def _extract_lineup_player(self, player_box: BeautifulSoup) -> Dict[str, Any]:
        """
        Estrae informazioni su un giocatore dalla formazione.
        
        Args:
            player_box: Tag BeautifulSoup del box giocatore
            
        Returns:
            Dati sul giocatore
        """
        try:
            player_link = player_box.select_one("a")
            if not player_link:
                return None
                
            player_url = player_link.get('href', '')
            player_id = self._extract_player_id(player_url)
            
            # Estrai numero di maglia
            number_span = player_box.select_one("span.aufstellung-rueckennummer")
            number = number_span.text.strip() if number_span else ""
            
            # Estrai nome
            name_div = player_box.select_one("div.aufstellung-spieler-name")
            name = name_div.text.strip() if name_div else player_link.text.strip()
            
            return {
                'id': player_id,
                'name': name,
                'number': number,
                'url': self.base_url + player_url if player_url.startswith('/') else player_url
            }
            
        except Exception:
            return None
    
    def _extract_substitute_player(self, cell: BeautifulSoup) -> Dict[str, Any]:
        """
        Estrae informazioni su un giocatore dalla panchina.
        
        Args:
            cell: Cella della tabella con info sul giocatore
            
        Returns:
            Dati sul giocatore
        """
        try:
            player_link = cell.select_one("a")
            if not player_link:
                return None
                
            player_url = player_link.get('href', '')
            player_id = self._extract_player_id(player_url)
            player_name = player_link.text.strip()
            
            return {
                'id': player_id,
                'name': player_name,
                'url': self.base_url + player_url if player_url.startswith('/') else player_url
            }
            
        except Exception:
            return None
    
    def _extract_match_events(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """
        Estrae gli eventi di una partita (gol, cartellini, ecc.).
        
        Args:
            soup: Oggetto BeautifulSoup della pagina
            
        Returns:
            Lista di eventi della partita
        """
        try:
            events = []
            
            # Cerca la sezione degli eventi
            events_table = soup.select_one("div.content div.box div.data table.standard_tabelle")
            if not events_table:
                return []
            
            # Estrai eventi dalla tabella
            for row in events_table.select("tr:not(.dunkel)"):
                cells = row.select("td")
                if len(cells) < 3:
                    continue
                
                # Estrai minuto
                minute_text = cells[0].text.strip()
                minute = None
                
                # Gestisci formato standard e minuti di recupero
                minute_match = re.search(r'(\d+)(?:\+(\d+))?', minute_text)
                if minute_match:
                    minute = int(minute_match.group(1))
                    added_time = minute_match.group(2)
                    if added_time:
                        minute_text = f"{minute}+{added_time}"
                
                # Verifica a quale squadra appartiene l'evento
                team = None
                event_type = None
                player_id = None
                player_name = None
                
                # Determina il tipo di evento
                if cells[1].text.strip():
                    # Evento squadra casa
                    team = 'home'
                    event_cell = cells[1]
                else:
                    # Evento squadra trasferta
                    team = 'away'
                    event_cell = cells[2]
                
                # Estrai tipo evento e giocatore
                event_text = event_cell.text.strip()
                
                # Estrai link al giocatore
                player_link = event_cell.select_one("a")
                if player_link:
                    player_url = player_link.get('href', '')
                    player_id = self._extract_player_id(player_url)
                    player_name = player_link.text.strip()
                
                # Determina tipo evento
                event_icon = event_cell.select_one("img")
                if event_icon:
                    icon_src = event_icon.get('src', '')
                    alt_text = event_icon.get('alt', '')
                    
                    if 'tor.png' in icon_src or 'goal' in alt_text.lower():
                        event_type = 'goal'
                    elif 'eigentor.png' in icon_src or 'own' in alt_text.lower():
                        event_type = 'own_goal'
                    elif 'gelb.png' in icon_src or 'yellow' in alt_text.lower():
                        event_type = 'yellow_card'
                    elif 'rot.png' in icon_src or 'red' in alt_text.lower():
                        event_type = 'red_card'
                    elif 'gelbrot.png' in icon_src or 'second yellow' in alt_text.lower():
                        event_type = 'second_yellow'
                    elif 'wechsel.png' in icon_src or 'substitution' in alt_text.lower():
                        event_type = 'substitution'
                    elif 'elfmeter.png' in icon_src or 'penalty' in alt_text.lower():
                        event_type = 'penalty'
                else:
                    # Tenta di determinare il tipo evento dal testo
                    if '⚽' in event_text or 'goal' in event_text.lower():
                        event_type = 'goal'
                    elif 'yellow' in event_text.lower() or 'gelb' in event_text.lower():
                        event_type = 'yellow_card'
                    elif 'red' in event_text.lower() or 'rot' in event_text.lower():
                        event_type = 'red_card'
                    elif 'substitution' in event_text.lower() or 'wechsel' in event_text.lower():
                        event_type = 'substitution'
                    elif 'penalty' in event_text.lower() or 'elfmeter' in event_text.lower():
                        event_type = 'penalty'
                
                # Estrai dettagli sostituzione
                substitution_in = None
                substitution_out = None
                
                if event_type == 'substitution':
                    # Cerca il giocatore che entra e quello che esce
                    substitution_match = re.search(r'(.*?)\s+(?:for|für|per)\s+(.*?)', event_text)
                    if substitution_match:
                        substitution_in = substitution_match.group(1).strip()
                        substitution_out = substitution_match.group(2).strip()
                
                # Crea oggetto evento
                event = {
                    'minute': minute,
                    'minute_text': minute_text,
                    'team': team,
                    'type': event_type,
                    'text': event_text
                }
                
                # Aggiungi dettagli giocatore se disponibili
                if player_id:
                    event['player'] = {
                        'id': player_id,
                        'name': player_name
                    }
                
                # Aggiungi dettagli sostituzione se disponibili
                if substitution_in:
                    event['substitution_in'] = substitution_in
                if substitution_out:
                    event['substitution_out'] = substitution_out
                
                events.append(event)
            
            return sorted(events, key=lambda x: x.get('minute', 0) or 0)
            
        except Exception as e:
            logger.error(f"Errore nell'estrazione eventi: {str(e)}")
            return []
    
    def _extract_match_stats(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Estrae le statistiche di una partita.
        
        Args:
            soup: Oggetto BeautifulSoup della pagina
            
        Returns:
            Dati statistici della partita
        """
        try:
            stats = {
                'home': {},
                'away': {}
            }
            
            # Cerca la sezione delle statistiche
            stats_table = None
            tables = soup.select("div.content div.box div.data table.standard_tabelle")
            
            # Cerca la tabella che contiene le statistiche (in genere quella con intestazione "Statistics")
            for table in tables:
                header = table.select_one("tr.dunkel")
                if header and ('Statistics' in header.text or 'Statistik' in header.text):
                    stats_table = table
                    break
            
            if not stats_table:
                return {}
            
            # Estrai statistiche dalla tabella
            for row in stats_table.select("tr:not(.dunkel)"):
                cells = row.select("td")
                if len(cells) != 3:
                    continue
                
                # Estrai nome statistica e valori
                stat_name = cells[1].text.strip().lower().replace(' ', '_').replace('%', 'percentage')
                home_value = cells[0].text.strip()
                away_value = cells[2].text.strip()
                
                # Converte in numeri quando possibile
                try:
                    if home_value.isdigit():
                        home_value = int(home_value)
                    elif '%' in home_value:
                        home_value = float(home_value.replace('%', ''))
                except ValueError:
                    pass
                
                try:
                    if away_value.isdigit():
                        away_value = int(away_value)
                    elif '%' in away_value:
                        away_value = float(away_value.replace('%', ''))
                except ValueError:
                    pass
                
                stats['home'][stat_name] = home_value
                stats['away'][stat_name] = away_value
            
            return stats
            
        except Exception as e:
            logger.error(f"Errore nell'estrazione statistiche: {str(e)}")
            return {}
    
    def _extract_matches_from_table(self, table: BeautifulSoup, round_name: str, league_id: str, season: str) -> List[Dict[str, Any]]:
        """
        Estrae partite da una tabella di giornata di campionato.
        
        Args:
            table: Tag BeautifulSoup della tabella
            round_name: Nome della giornata (es. 'Giornata 1')
            league_id: ID del campionato
            season: ID della stagione
            
        Returns:
            Lista di partite
        """
        matches = []
        
        for row in table.select("tr:not(.thead)"):
            cells = row.select("td")
            if len(cells) < 5:
                continue
            
            # Estrai data
            date_text = cells[0].text.strip()
            time_text = cells[1].text.strip() if len(cells) > 1 else ""
            
            # Converti data e ora in ISO format
            match_datetime = None
            if date_text:
                try:
                    if time_text:
                        match_datetime = datetime.strptime(f"{date_text} {time_text}", '%d.%m.%Y %H:%M').isoformat()
                    else:
                        match_datetime = datetime.strptime(date_text, '%d.%m.%Y').isoformat()
                except ValueError:
                    pass
            
            # Estrai squadre
            home_team_link = None
            away_team_link = None
            
            # Controlla la struttura della tabella per determinare le celle corrette
            home_idx = 2
            away_idx = 4
            result_idx = 3
            match_link_idx = 5
            
            if len(cells) >= 6:
                # Formato standard: data, ora, home, risultato, away, link
                home_team_name = cells[home_idx].text.strip()
                away_team_name = cells[away_idx].text.strip()
                
                # Se presenti i link alle squadre, estrai ID
                home_link = cells[home_idx].select_one("a")
                if home_link:
                    home_team_link = home_link.get('href', '')
                    
                away_link = cells[away_idx].select_one("a")
                if away_link:
                    away_team_link = away_link.get('href', '')
                
                # Estrai risultato
                result = cells[result_idx].text.strip()
                home_score = None
                away_score = None
                
                if result and ':' in result:
                    score_parts = result.split(':')
                    if len(score_parts) >= 2:
                        home_score = int(score_parts[0].strip()) if score_parts[0].strip().isdigit() else None
                        away_score = int(score_parts[1].strip()) if score_parts[1].strip().isdigit() else None
                
                # Estrai link dettagli partita
                match_id = None
                match_link = cells[match_link_idx].select_one("a[href*='/matches/']") if match_link_idx < len(cells) else None
                if match_link:
                    match_url = match_link.get('href', '')
                    match_id = self._extract_match_id(match_url)
                
                match_data = {
                    'id': match_id,
                    'date': match_datetime,
                    'round': round_name,
                    'league_id': league_id,
                    'season': season,
                    'home_team': {
                        'name': home_team_name,
                        'id': self._extract_team_id(home_team_link) if home_team_link else None
                    },
                    'away_team': {
                        'name': away_team_name,
                        'id': self._extract_team_id(away_team_link) if away_team_link else None
                    }
                }
                
                # Aggiungi punteggio se disponibile
                if home_score is not None and away_score is not None:
                    match_data['home_score'] = home_score
                    match_data['away_score'] = away_score
                    match_data['result'] = f"{home_score}:{away_score}"
                    
                    # Determina il vincitore
                    if home_score > away_score:
                        match_data['winner'] = 'home'
                    elif away_score > home_score:
                        match_data['winner'] = 'away'
                    else:
                        match_data['winner'] = 'draw'
                
                matches.append(match_data)
        
        return matches

    # ---- Metodi di supporto per l'estrazione ID ---- #
    
    def _get_league_path(self, league_id: str) -> str:
        """
        Converte un league_id nella corrispondente path dell'URL.
        
        Args:
            league_id: ID del campionato
            
        Returns:
            Path dell'URL corrispondente
        """
        # Se l'ID è già una path completa, restituiscila
        if '/' in league_id:
            return league_id
            
        # Altrimenti cerca nella mappatura
        return self.leagues_map.get(league_id, league_id)
    
    def _extract_team_id(self, url: Optional[str]) -> Optional[str]:
        """
        Estrae l'ID della squadra da un URL.
        
        Args:
            url: URL della squadra
            
        Returns:
            ID della squadra o None se non trovato
        """
        if not url:
            return None
            
        # Formato tipico: /teams/italia-teams/123/
        match = re.search(r'/teams/(?:.*?)/(\w+)/?', url)
        if match:
            return match.group(1)
            
        return None
    
    def _extract_player_id(self, url: Optional[str]) -> Optional[str]:
        """
        Estrae l'ID del giocatore da un URL.
        
        Args:
            url: URL del giocatore
            
        Returns:
            ID del giocatore o None se non trovato
        """
        if not url:
            return None
            
        # Formato tipico: /players/joe-bloggs/123/
        match = re.search(r'/players/(?:.*?)/(\w+)/?', url)
        if match:
            return match.group(1)
            
        return None
    
    def _extract_match_id(self, url: Optional[str]) -> Optional[str]:
        """
        Estrae l'ID della partita da un URL.
        
        Args:
            url: URL della partita
            
        Returns:
            ID della partita o None se non trovato
        """
        if not url:
            return None
            
        # Formato tipico: /matches/2023/08/25/2392123/
        match = re.search(r'/matches/(?:.*?)/(\w+)/?', url)
        if match:
            return match.group(1)
            
        return None

# ---- Funzioni di utilità globali ---- #

def get_league_seasons(league_id: str) -> List[Dict[str, Any]]:
    """
    Ottiene le stagioni disponibili per un campionato.
    
    Args:
        league_id: ID del campionato (chiave da leagues_map o URL diretto)
        
    Returns:
        Lista di stagioni disponibili
    """
    scraper = WorldFootballScraper()
    return scraper.get_league_seasons(league_id)

def get_league_table(league_id: str, season: Optional[str] = None) -> Dict[str, Any]:
    """
    Ottiene la classifica di un campionato per una stagione specifica.
    
    Args:
        league_id: ID del campionato (chiave da leagues_map o URL diretto)
        season: ID della stagione (opzionale, default: stagione corrente)
        
    Returns:
        Classifica completa
    """
    scraper = WorldFootballScraper()
    return scraper.get_league_table(league_id, season)

def get_league_fixtures(league_id: str, season: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Ottiene le partite di un campionato per una stagione specifica.
    
    Args:
        league_id: ID del campionato (chiave da leagues_map o URL diretto)
        season: ID della stagione (opzionale, default: stagione corrente)
        
    Returns:
        Lista di partite
    """
    scraper = WorldFootballScraper()
    return scraper.get_league_fixtures(league_id, season)

def get_match_details(match_id: str) -> Dict[str, Any]:
    """
    Ottiene dettagli completi su una partita.
    
    Args:
        match_id: ID della partita
        
    Returns:
        Dettagli completi sulla partita
    """
    scraper = WorldFootballScraper()
    return scraper.get_match_details(match_id)

def get_team_info(team_id: str) -> Dict[str, Any]:
    """
    Ottiene informazioni su una squadra.
    
    Args:
        team_id: ID della squadra
        
    Returns:
        Informazioni sulla squadra
    """
    scraper = WorldFootballScraper()
    return scraper.get_team_info(team_id)

def get_team_squad(team_id: str, season: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Ottiene la rosa di una squadra per una stagione specifica.
    
    Args:
        team_id: ID della squadra
        season: ID della stagione (opzionale, default: stagione corrente)
        
    Returns:
        Lista di giocatori
    """
    scraper = WorldFootballScraper()
    return scraper.get_team_squad(team_id, season)

def get_team_fixtures(team_id: str, season: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Ottiene le partite di una squadra per una stagione specifica.
    
    Args:
        team_id: ID della squadra
        season: ID della stagione (opzionale, default: stagione corrente)
        
    Returns:
        Lista di partite
    """
    scraper = WorldFootballScraper()
    return scraper.get_team_fixtures(team_id, season)

def get_player_info(player_id: str) -> Dict[str, Any]:
    """
    Ottiene informazioni su un giocatore.
    
    Args:
        player_id: ID del giocatore
        
    Returns:
        Informazioni sul giocatore
    """
    scraper = WorldFootballScraper()
    return scraper.get_player_info(player_id)
