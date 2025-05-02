"""
Modulo per l'estrazione di dati storici da 11v11.com.
Questo modulo fornisce funzionalità per estrarre dati storici su partite, squadre e giocatori
dal sito 11v11.com, specializzato in dati storici del calcio.
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
from src.utils.time_utils import parse_date
from src.config.settings import get_setting

logger = logging.getLogger(__name__)

class ElevenVElevenScraper(BaseScraper):
    """
    Scraper per estrarre dati storici da 11v11.com.
    
    Questo scraper permette di ottenere:
    - Archivi storici di partite risalenti a inizio '900
    - Statistiche head-to-head storiche tra squadre
    - Dati su competizioni storiche e tornei internazionali
    - Profili dettagliati di giocatori e allenatori storici
    """
    
    def __init__(self, cache_ttl: int = None):
        """
        Inizializza lo scraper 11v11.
        
        Args:
            cache_ttl: Tempo di vita della cache in secondi (default da settings)
        """
        base_url = "https://www.11v11.com"
        cache_ttl = cache_ttl or get_setting('scrapers.eleven_v_eleven.cache_ttl', 86400 * 7)  # 7 giorni default
        
        super().__init__(
            base_url=base_url,
            name="eleven_v_eleven",
            cache_ttl=cache_ttl
        )
        
        # Impostazioni specifiche per 11v11
        self.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9'
        })
        
        # Mappatura delle competizioni principali
        self.competitions_map = {
            'premier_league': 'league/premier-league',
            'fa_cup': 'cup/fa-cup',
            'league_cup': 'cup/league-cup',
            'championship': 'league/championship',
            'champions_league': 'european/champions-league',
            'europa_league': 'european/europa-league',
            'world_cup': 'international/fifa-world-cup',
            'euro': 'international/uefa-european-championship'
        }
        
        # Mappatura delle nazionali principali
        self.countries_map = {
            'england': 'international/england',
            'scotland': 'international/scotland',
            'wales': 'international/wales',
            'northern_ireland': 'international/northern-ireland',
            'republic_of_ireland': 'international/republic-of-ireland',
            'italy': 'international/italy',
            'spain': 'international/spain',
            'germany': 'international/germany',
            'france': 'international/france',
            'netherlands': 'international/netherlands',
            'belgium': 'international/belgium',
            'portugal': 'international/portugal',
            'brazil': 'international/brazil',
            'argentina': 'international/argentina'
        }
        
        logger.info(f"ElevenVElevenScraper inizializzato con cache TTL: {cache_ttl}s")
    
    @cached(ttl=86400 * 30)  # 30 giorni
    def get_team_info(self, team_id: str) -> Dict[str, Any]:
        """
        Ottiene informazioni dettagliate su una squadra.
        
        Args:
            team_id: ID della squadra (path relativo o nome)
            
        Returns:
            Informazioni dettagliate sulla squadra
        """
        logger.info(f"Recuperando informazioni squadra: {team_id}")
        
        try:
            # Controlla se è un ID o un nome squadra
            if '/' in team_id:
                url = f"{self.base_url}/{team_id}"
            else:
                # Cerca di costruire l'URL dalla stringa
                formatted_name = team_id.lower().replace(' ', '-')
                url = f"{self.base_url}/teams/{formatted_name}"
            
            # Ottieni la pagina
            soup = self.get_soup(url)
            
            # Estrai dati di base
            team_info = self._extract_team_basic_info(soup)
            team_info['url'] = url
            
            # Aggiungi ID originale
            team_info['id'] = team_id
            
            # Estrai fondazione e stadio se disponibili
            team_info.update(self._extract_team_details(soup))
            
            # Estrai statistiche all-time
            team_info['all_time_stats'] = self._extract_team_all_time_stats(soup)
            
            # Estrai manager (allenatore attuale)
            manager = self._extract_team_manager(soup)
            if manager:
                team_info['manager'] = manager
            
            return team_info
            
        except Exception as e:
            logger.error(f"Errore nel recupero info squadra {team_id}: {str(e)}")
            return {'id': team_id, 'error': str(e)}
    
    @cached(ttl=86400 * 7)  # 7 giorni
    def get_all_time_table(self, competition_id: str) -> List[Dict[str, Any]]:
        """
        Ottiene la classifica all-time di una competizione.
        
        Args:
            competition_id: ID della competizione (chiave da competitions_map o path)
            
        Returns:
            Classifica all-time della competizione
        """
        logger.info(f"Recuperando classifica all-time: {competition_id}")
        
        try:
            # Determina l'URL corretto
            if competition_id in self.competitions_map:
                url = f"{self.base_url}/{self.competitions_map[competition_id]}/all-time-table"
            else:
                url = f"{self.base_url}/{competition_id}/all-time-table"
            
            # Ottieni la pagina
            soup = self.get_soup(url)
            
            # Estrai titolo della competizione
            title_elem = soup.select_one("div.container h1")
            competition_name = title_elem.text.strip() if title_elem else competition_id
            
            # Cerca la tabella della classifica
            table = soup.select_one("table.content-half-width")
            if not table:
                logger.warning(f"Tabella classifica all-time non trovata per {competition_id}")
                return []
            
            teams = []
            
            # Estrai righe della tabella (ignora intestazione)
            rows = table.select("tbody tr")
            for row in rows:
                cells = row.select("td")
                if len(cells) < 9:  # Verifica numero minimo di celle
                    continue
                
                # Estrai posizione
                position = cells[0].text.strip()
                
                # Estrai squadra
                team_cell = cells[1]
                team_link = team_cell.select_one("a")
                
                team_name = team_cell.text.strip()
                team_url = team_link.get('href', '') if team_link else ''
                team_id = self._extract_team_id(team_url) if team_url else team_name
                
                # Estrai statistiche principali
                matches_played = int(cells[2].text.strip()) if cells[2].text.strip().isdigit() else 0
                wins = int(cells[3].text.strip()) if cells[3].text.strip().isdigit() else 0
                draws = int(cells[4].text.strip()) if cells[4].text.strip().isdigit() else 0
                losses = int(cells[5].text.strip()) if cells[5].text.strip().isdigit() else 0
                
                # Estrai gol
                goals_for = int(cells[6].text.strip()) if cells[6].text.strip().isdigit() else 0
                goals_against = int(cells[7].text.strip()) if cells[7].text.strip().isdigit() else 0
                
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
            
            return teams
            
        except Exception as e:
            logger.error(f"Errore nel recupero classifica all-time {competition_id}: {str(e)}")
            return []
    
    @cached(ttl=86400 * 7)  # 7 giorni
    def get_player_info(self, player_id: str) -> Dict[str, Any]:
        """
        Ottiene informazioni dettagliate su un giocatore.
        
        Args:
            player_id: ID del giocatore (path relativo o nome)
            
        Returns:
            Informazioni dettagliate sul giocatore
        """
        logger.info(f"Recuperando informazioni giocatore: {player_id}")
        
        try:
            # Controlla se è un ID o un nome giocatore
            if '/' in player_id:
                url = f"{self.base_url}/{player_id}"
            else:
                # Cerca di costruire l'URL dalla stringa
                formatted_name = player_id.lower().replace(' ', '-')
                url = f"{self.base_url}/players/{formatted_name}"
            
            # Ottieni la pagina
            soup = self.get_soup(url)
            
            # Estrai dati di base
            player_info = self._extract_player_basic_info(soup)
            player_info['url'] = url
            
            # Aggiungi ID originale
            player_info['id'] = player_id
            
            # Estrai dettagli biografici
            player_info.update(self._extract_player_details(soup))
            
            # Estrai statistiche di carriera
            career_stats = self._extract_player_career_stats(soup)
            if career_stats:
                player_info['career_stats'] = career_stats
            
            # Estrai statistiche internazionali
            international_stats = self._extract_player_international_stats(soup)
            if international_stats:
                player_info['international'] = international_stats
            
            return player_info
            
        except Exception as e:
            logger.error(f"Errore nel recupero info giocatore {player_id}: {str(e)}")
            return {'id': player_id, 'error': str(e)}
    
    @cached(ttl=86400)  # 24 ore
    def get_head_to_head(self, team1_id: str, team2_id: str) -> Dict[str, Any]:
        """
        Ottiene statistiche head-to-head tra due squadre.
        
        Args:
            team1_id: ID della prima squadra
            team2_id: ID della seconda squadra
            
        Returns:
            Statistiche head-to-head complete
        """
        logger.info(f"Recuperando statistiche head-to-head: {team1_id} vs {team2_id}")
        
        try:
            # Costruisci l'URL della pagina head-to-head
            # Controlla se sono ID o nomi squadra
            team1_path = team1_id if '/' in team1_id else f"teams/{team1_id.lower().replace(' ', '-')}"
            team2_path = team2_id if '/' in team2_id else f"teams/{team2_id.lower().replace(' ', '-')}"
            
            url = f"{self.base_url}/head2head/{team1_path}/{team2_path}"
            
            # Ottieni la pagina
            soup = self.get_soup(url)
            
            # Estrai nomi squadre
            title_elem = soup.select_one("div.container h1")
            h2h_title = title_elem.text.strip() if title_elem else f"{team1_id} vs {team2_id}"
            
            # Spesso il titolo è nel formato "Team1 v Team2"
            teams_match = re.search(r'(.+?)\s+v\s+(.+)', h2h_title)
            team1_name = teams_match.group(1).strip() if teams_match else team1_id
            team2_name = teams_match.group(2).strip() if teams_match else team2_id
            
            # Estrai sommario statistico
            summary = self._extract_h2h_summary(soup)
            
            # Estrai partite recenti
            matches = self._extract_h2h_matches(soup)
            
            return {
                'team1': {
                    'id': team1_id,
                    'name': team1_name
                },
                'team2': {
                    'id': team2_id,
                    'name': team2_name
                },
                'summary': summary,
                'matches': matches,
                'updated_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Errore nel recupero h2h {team1_id} vs {team2_id}: {str(e)}")
            return {
                'team1': {'id': team1_id},
                'team2': {'id': team2_id},
                'error': str(e)
            }
    
    @cached(ttl=86400 * 7)  # 7 giorni
    def get_competition_seasons(self, competition_id: str) -> List[Dict[str, Any]]:
        """
        Ottiene le stagioni disponibili per una competizione.
        
        Args:
            competition_id: ID della competizione (chiave da competitions_map o path)
            
        Returns:
            Lista di stagioni disponibili
        """
        logger.info(f"Recuperando stagioni per competizione: {competition_id}")
        
        try:
            # Determina l'URL corretto
            if competition_id in self.competitions_map:
                url = f"{self.base_url}/{self.competitions_map[competition_id]}"
            else:
                url = f"{self.base_url}/{competition_id}"
            
            # Ottieni la pagina
            soup = self.get_soup(url)
            
            seasons = []
            
            # Cerca le stagioni nella barra laterale
            season_links = soup.select("div.block-content ul li a[href*='season']")
            for link in season_links:
                season_url = link.get('href', '')
                if not season_url:
                    continue
                
                season_name = link.text.strip()
                
                # Estrai l'anno dalla stagione
                year_match = re.search(r'(\d{4})-(\d{4}|\d{2})', season_name)
                if year_match:
                    start_year = year_match.group(1)
                    end_year = year_match.group(2)
                    
                    # Se l'anno finale è in formato breve (es. '09'), convertilo in completo
                    if len(end_year) == 2:
                        if end_year < '50':  # Euristico semplice
                            end_year = f"20{end_year}"
                        else:
                            end_year = f"19{end_year}"
                    
                    seasons.append({
                        'id': season_url.split('/')[-1] if '/' in season_url else season_url,
                        'name': season_name,
                        'url': self.base_url + season_url if season_url.startswith('/') else season_url,
                        'start_year': start_year,
                        'end_year': end_year
                    })
            
            return sorted(seasons, key=lambda x: x.get('start_year', '0'), reverse=True)
            
        except Exception as e:
            logger.error(f"Errore nel recupero stagioni per competizione {competition_id}: {str(e)}")
            return []
    
    @cached(ttl=86400)  # 24 ore
    def get_competition_table(self, competition_id: str, season_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Ottiene la classifica di una competizione per una stagione specifica.
        
        Args:
            competition_id: ID della competizione (chiave da competitions_map o path)
            season_id: ID della stagione (opzionale, default: ultima stagione)
            
        Returns:
            Classifica della competizione
        """
        logger.info(f"Recuperando classifica per competizione: {competition_id}, stagione: {season_id}")
        
        try:
            # Determina l'URL di base
            if competition_id in self.competitions_map:
                base_url = f"{self.base_url}/{self.competitions_map[competition_id]}"
            else:
                base_url = f"{self.base_url}/{competition_id}"
            
            # Se la stagione è specificata, aggiungi all'URL
            url = f"{base_url}/season/{season_id}" if season_id else base_url
            
            # Ottieni la pagina
            soup = self.get_soup(url)
            
            # Estrai titolo
            title_elem = soup.select_one("div.container h1")
            title = title_elem.text.strip() if title_elem else ""
            
            # Estrai stagione dal titolo se non fornita
            season_name = None
            if season_id:
                season_name = season_id
            else:
                season_match = re.search(r'(\d{4}-\d{2,4})', title)
                if season_match:
                    season_name = season_match.group(1)
            
            # Cerca la tabella della classifica
            table = soup.select_one("table.table-striped")
            if not table:
                logger.warning(f"Tabella classifica non trovata per {competition_id}")
                return {'competition_id': competition_id, 'season': season_name, 'teams': []}
            
            teams = []
            
            # Estrai righe della tabella (ignora intestazione)
            rows = table.select("tbody tr")
            for row in rows:
                cells = row.select("td")
                if len(cells) < 9:  # Verifica numero minimo di celle
                    continue
                
                # Estrai posizione
                position = cells[0].text.strip()
                
                # Estrai squadra
                team_cell = cells[1]
                team_link = team_cell.select_one("a")
                
                team_name = team_cell.text.strip()
                team_url = team_link.get('href', '') if team_link else ''
                team_id = self._extract_team_id(team_url) if team_url else team_name
                
                # Estrai statistiche principali
                matches_played = int(cells[2].text.strip()) if cells[2].text.strip().isdigit() else 0
                wins = int(cells[3].text.strip()) if cells[3].text.strip().isdigit() else 0
                draws = int(cells[4].text.strip()) if cells[4].text.strip().isdigit() else 0
                losses = int(cells[5].text.strip()) if cells[5].text.strip().isdigit() else 0
                
                # Estrai gol
                goals_for = int(cells[6].text.strip()) if cells[6].text.strip().isdigit() else 0
                goals_against = int(cells[7].text.strip()) if cells[7].text.strip().isdigit() else 0
                
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
                'competition_id': competition_id,
                'competition_name': title,
                'season': season_name,
                'teams': teams
            }
            
        except Exception as e:
            logger.error(f"Errore nel recupero classifica {competition_id}: {str(e)}")
            return {'competition_id': competition_id, 'season': season_id, 'teams': []}
    
    @cached(ttl=86400)  # 24 ore
    def get_competition_matches(self, competition_id: str, season_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Ottiene le partite di una competizione per una stagione specifica.
        
        Args:
            competition_id: ID della competizione (chiave da competitions_map o path)
            season_id: ID della stagione (opzionale, default: ultima stagione)
            
        Returns:
            Lista di partite
        """
        logger.info(f"Recuperando partite per competizione: {competition_id}, stagione: {season_id}")
        
        try:
            # Determina l'URL di base
            if competition_id in self.competitions_map:
                base_url = f"{self.base_url}/{self.competitions_map[competition_id]}"
            else:
                base_url = f"{self.base_url}/{competition_id}"
            
            # Se la stagione è specificata, aggiungi all'URL
            if season_id:
                url = f"{base_url}/season/{season_id}/matches"
            else:
                url = f"{base_url}/matches"
            
            # Ottieni la pagina
            soup = self.get_soup(url)
            
            matches = []
            
            # Estrai partite dalla tabella
            matches_table = soup.select_one("table.table-striped")
            if not matches_table:
                logger.warning(f"Tabella partite non trovata per {competition_id}")
                return []
            
            # Estrai righe della tabella (ignora intestazione)
            rows = matches_table.select("tbody tr")
            for row in rows:
                cells = row.select("td")
                if len(cells) < 5:  # Verifica numero minimo di celle
                    continue
                
                # Estrai data
                date_cell = cells[0]
                date_text = date_cell.text.strip()
                match_date = None
                
                if date_text:
                    try:
                        # Formato tipico: "DD/MM/YYYY"
                        match_date = datetime.strptime(date_text, '%d/%m/%Y').strftime('%Y-%m-%d')
                    except ValueError:
                        pass
                
                # Estrai squadre e risultato
                home_team_cell = cells[1]
                score_cell = cells[2]
                away_team_cell = cells[3]
                
                # Estrai link squadre
                home_team_link = home_team_cell.select_one("a")
                away_team_link = away_team_cell.select_one("a")
                
                home_team = {
                    'name': home_team_cell.text.strip()
                }
                
                away_team = {
                    'name': away_team_cell.text.strip()
                }
                
                if home_team_link:
                    home_url = home_team_link.get('href', '')
                    home_id = self._extract_team_id(home_url)
                    if home_id:
                        home_team['id'] = home_id
                        home_team['url'] = self.base_url + home_url if home_url.startswith('/') else home_url
                
                if away_team_link:
                    away_url = away_team_link.get('href', '')
                    away_id = self._extract_team_id(away_url)
                    if away_id:
                        away_team['id'] = away_id
                        away_team['url'] = self.base_url + away_url if away_url.startswith('/') else away_url
                
                # Estrai punteggio
                score_text = score_cell.text.strip()
                home_score = None
                away_score = None
                
                score_match = re.search(r'(\d+)\s*-\s*(\d+)', score_text)
                if score_match:
                    home_score = int(score_match.group(1))
                    away_score = int(score_match.group(2))
                
                # Estrai round/fase (se disponibile)
                round_cell = cells[4] if len(cells) > 4 else None
                round_text = round_cell.text.strip() if round_cell else None
                
                match_data = {
                    'date': match_date,
                    'home_team': home_team,
                    'away_team': away_team,
                    'competition': competition_id
                }
                
                # Aggiungi punteggio se disponibile
                if home_score is not None and away_score is not None:
                    match_data['home_score'] = home_score
                    match_data['away_score'] = away_score
                    match_data['result'] = f"{home_score}-{away_score}"
                    
                    # Determina il vincitore
                    if home_score > away_score:
                        match_data['winner'] = 'home'
                    elif away_score > home_score:
                        match_data['winner'] = 'away'
                    else:
                        match_data['winner'] = 'draw'
                
                # Aggiungi round se disponibile
                if round_text:
                    match_data['round'] = round_text
                
                matches.append(match_data)
            
            return matches
            
        except Exception as e:
            logger.error(f"Errore nel recupero partite {competition_id}: {str(e)}")
            return []
    
    @cached(ttl=86400 * 7)  # 7 giorni
    def get_team_awards(self, team_id: str) -> List[Dict[str, Any]]:
        """
        Ottiene i trofei e riconoscimenti vinti da una squadra.
        
        Args:
            team_id: ID della squadra
            
        Returns:
            Lista di trofei e riconoscimenti
        """
        logger.info(f"Recuperando trofei per squadra: {team_id}")
        
        try:
            # Controlla se è un ID o un nome squadra
            if '/' in team_id:
                url = f"{self.base_url}/{team_id}/honours"
            else:
                # Cerca di costruire l'URL dalla stringa
                formatted_name = team_id.lower().replace(' ', '-')
                url = f"{self.base_url}/teams/{formatted_name}/honours"
            
            # Ottieni la pagina
            soup = self.get_soup(url)
            
            awards = []
            
            # Estrai trofei dalle tabelle
            tables = soup.select("table.table-striped")
            
            for table in tables:
                # Trova il titolo della competizione
                table_header = table.select_one("thead th")
                competition = table_header.text.strip() if table_header else "Unknown"
                
                # Estrai righe della tabella
                for row in table.select("tbody tr"):
                    cells = row.select("td")
                    if len(cells) < 2:
                        continue
                    
                    # Estrai anno
                    year_cell = cells[0]
                    year = year_cell.text.strip()
                    
                    # Estrai dettagli (può contenere l'avversario in finale, ecc.)
                    details_cell = cells[1]
                    details = details_cell.text.strip()
                    
                    awards.append({
                        'competition': competition,
                        'year': year,
                        'details': details
                    })
            
            return awards
            
        except Exception as e:
            logger.error(f"Errore nel recupero trofei squadra {team_id}: {str(e)}")
            return []
    
    # ---- Metodi di estrazione dati ---- #
    
    def _extract_team_basic_info(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Estrae informazioni di base su una squadra.
        
        Args:
            soup: Oggetto BeautifulSoup della pagina
            
        Returns:
            Informazioni di base sulla squadra
        """
        try:
            # Estrai nome squadra
            title_elem = soup.select_one("div.container h1")
            team_name = title_elem.text.strip() if title_elem else ""
            
            # Cerca logo squadra
            logo_elem = soup.select_one("div.team-badge img")
            logo_url = logo_elem.get('src', '') if logo_elem else None
            
            team_info = {
                'name': team_name,
                'logo_url': logo_url
            }
            
            return team_info
            
        except Exception as e:
            logger.error(f"Errore nell'estrazione info base squadra: {str(e)}")
            return {}
    
    def _extract_team_details(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Estrae dettagli aggiuntivi su una squadra.
        
        Args:
            soup: Oggetto BeautifulSoup della pagina
            
        Returns:
            Dettagli aggiuntivi sulla squadra
        """
        try:
            details = {}
            
            # Cerca dettagli squadra
            info_section = soup.select("div.team-info p")
            
            for paragraph in info_section:
                text = paragraph.text.strip()
                
                # Estrai anno fondazione
                founded_match = re.search(r'[Ff]ounded[:\s]+(\d{4})', text)
                if founded_match:
                    details['founded'] = int(founded_match.group(1))
                
                # Estrai stadio
                stadium_match = re.search(r'[Ss]tadium[:\s]+(.*?)(?:[\.,]|$)', text)
                if stadium_match:
                    details['stadium'] = stadium_match.group(1).strip()
                
                # Estrai capacità stadio
                capacity_match = re.search(r'[Cc]apacity[:\s]+([\d,\.]+)', text)
                if capacity_match:
                    capacity_text = capacity_match.group(1).replace(',', '')
                    try:
                        details['stadium_capacity'] = int(capacity_text)
                    except ValueError:
                        pass
            
            return details
            
        except Exception as e:
            logger.error(f"Errore nell'estrazione dettagli squadra: {str(e)}")
            return {}
    
    def _extract_team_all_time_stats(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Estrae statistiche all-time di una squadra.
        
        Args:
            soup: Oggetto BeautifulSoup della pagina
            
        Returns:
            Statistiche all-time
        """
        try:
            stats = {}
            
            # Cerca tabella statistiche
            stats_table = soup.select_one("table.table-striped")
            if not stats_table:
                return stats
            
            # Estrai righe della tabella
            for row in stats_table.select("tbody tr"):
                cells = row.select("td")
                if len(cells) < 2:
                    continue
                
                # Estrai nome statistica e valore
                stat_name = cells[0].text.strip().lower().replace(' ', '_').replace('/', '_per_')
                stat_value = cells[1].text.strip()
                
                # Converti in numeri quando possibile
                try:
                    if stat_value.isdigit():
                        stat_value = int(stat_value)
                    elif stat_value.replace('.', '').isdigit():
                        stat_value = float(stat_value)
                except ValueError:
                    pass
                
                stats[stat_name] = stat_value
            
            return stats
            
        except Exception as e:
            logger.error(f"Errore nell'estrazione statistiche all-time: {str(e)}")
            return {}
    
    def _extract_team_manager(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Estrae informazioni sull'allenatore di una squadra.
        
        Args:
            soup: Oggetto BeautifulSoup della pagina
            
        Returns:
            Informazioni sull'allenatore
        """
        try:
            # Cerca sezione manager
            manager_section = soup.select_one("div.manager")
            if not manager_section:
                return None
            
            # Estrai nome manager
            manager_name_elem = manager_section.select_one("div.manager-name a")
            if not manager_name_elem:
                return None
                
            manager_name = manager_name_elem.text.strip()
            manager_url = manager_name_elem.get('href', '')
            manager_id = self._extract_player_id(manager_url)
            
            # Estrai nazionalità
            nationality_elem = manager_section.select_one("div.manager-country")
            nationality = nationality_elem.text.strip() if nationality_elem else None
            
            # Estrai periodo
            period_elem = manager_section.select_one("div.manager-years")
            period = period_elem.text.strip() if period_elem else None
            
            return {
                'id': manager_id,
                'name': manager_name,
                'url': self.base_url + manager_url if manager_url.startswith('/') else manager_url,
                'nationality': nationality,
                'period': period
            }
            
        except Exception as e:
            logger.error(f"Errore nell'estrazione manager: {str(e)}")
            return None
    
    def _extract_player_basic_info(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Estrae informazioni di base su un giocatore.
        
        Args:
            soup: Oggetto BeautifulSoup della pagina
            
        Returns:
            Informazioni di base sul giocatore
        """
        try:
            # Estrai nome giocatore
            title_elem = soup.select_one("div.container h1")
            player_name = title_elem.text.strip() if title_elem else ""
            
            # Cerca foto giocatore
            photo_elem = soup.select_one("div.player-badge img")
            photo_url = photo_elem.get('src', '') if photo_elem else None
            
            player_info = {
                'name': player_name,
                'photo_url': photo_url
            }
            
            return player_info
            
        except Exception as e:
            logger.error(f"Errore nell'estrazione info base giocatore: {str(e)}")
            return {}
    
    def _extract_player_details(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Estrae dettagli biografici di un giocatore.
        
        Args:
            soup: Oggetto BeautifulSoup della pagina
            
        Returns:
            Dettagli biografici
        """
        try:
            details = {}
            
            # Cerca tabella dati biografici
            bio_table = soup.select_one("table.table-striped")
            if not bio_table:
                return details
            
            # Estrai righe della tabella
            for row in bio_table.select("tr"):
                cells = row.select("td")
                if len(cells) < 2:
                    continue
                
                # Estrai etichetta e valore
                label = cells[0].text.strip().lower().replace(' ', '_')
                value = cells[1].text.strip()
                
                if label and value:
                    details[label] = value
            
            # Estrai data di nascita
            if 'date_of_birth' in details:
                birth_date = details['date_of_birth']
                try:
                    # Formato tipico: "DD/MM/YYYY"
                    parsed_date = datetime.strptime(birth_date, '%d/%m/%Y').strftime('%Y-%m-%d')
                    details['birth_date'] = parsed_date
                except ValueError:
                    pass
            
            return details
            
        except Exception as e:
            logger.error(f"Errore nell'estrazione dettagli giocatore: {str(e)}")
            return {}
    
    def _extract_player_career_stats(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """
        Estrae statistiche di carriera di un giocatore.
        
        Args:
            soup: Oggetto BeautifulSoup della pagina
            
        Returns:
            Statistiche di carriera
        """
        try:
            career_stats = []
            
            # Cerca tabella statistiche carriera
            stats_table = soup.select_one("div.tab-player-stats table.table-striped")
            if not stats_table:
                return career_stats
            
            # Estrai righe della tabella
            for row in stats_table.select("tbody tr"):
                cells = row.select("td")
                if len(cells) < 7:  # Numero minimo celle
                    continue
                
                # Estrai squadra
                team_cell = cells[0]
                team_link = team_cell.select_one("a")
                
                team_name = team_cell.text.strip()
                team_url = team_link.get('href', '') if team_link else ''
                team_id = self._extract_team_id(team_url) if team_url else None
                
                # Estrai stagione
                season = cells[1].text.strip()
                
                # Estrai competizione
                competition = cells[2].text.strip()
                
                # Estrai statistiche
                appearances = int(cells[3].text.strip()) if cells[3].text.strip().isdigit() else 0
                goals = int(cells[4].text.strip()) if cells[4].text.strip().isdigit() else 0
                
                # Estrai statistiche opzionali
                cups = int(cells[5].text.strip()) if cells[5].text.strip().isdigit() else 0
                european = int(cells[6].text.strip()) if cells[6].text.strip().isdigit() else 0
                
                career_stats.append({
                    'team': {
                        'name': team_name,
                        'id': team_id,
                        'url': self.base_url + team_url if team_url.startswith('/') else team_url if team_url else None
                    },
                    'season': season,
                    'competition': competition,
                    'appearances': appearances,
                    'goals': goals,
                    'cups': cups,
                    'european': european
                })
            
            return career_stats
            
        except Exception as e:
            logger.error(f"Errore nell'estrazione statistiche carriera: {str(e)}")
            return []
    
    def _extract_player_international_stats(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Estrae statistiche internazionali di un giocatore.
        
        Args:
            soup: Oggetto BeautifulSoup della pagina
            
        Returns:
            Statistiche internazionali
        """
        try:
            international = {}
            
            # Cerca sezione internazionale
            international_section = soup.select_one("div.tab-international")
            if not international_section:
                return international
            
            # Estrai tabella statistiche
            stats_table = international_section.select_one("table.table-striped")
            if not stats_table:
                return international
            
            # Estrai righe della tabella
            rows = stats_table.select("tbody tr")
            if not rows:
                return international
            
            # Estrai nazione
            country_row = rows[0]
            country_cells = country_row.select("td")
            
            if len(country_cells) >= 2:
                country_cell = country_cells[0]
                country_link = country_cell.select_one("a")
                
                country_name = country_cell.text.strip()
                country_url = country_link.get('href', '') if country_link else ''
                country_id = self._extract_team_id(country_url) if country_url else None
                
                international['country'] = {
                    'name': country_name,
                    'id': country_id,
                    'url': self.base_url + country_url if country_url.startswith('/') else country_url if country_url else None
                }
                
                # Estrai caps e gol
                appearances = country_cells[1].text.strip()
                goals = country_cells[2].text.strip() if len(country_cells) > 2 else '0'
                
                international['caps'] = int(appearances) if appearances.isdigit() else 0
                international['goals'] = int(goals) if goals.isdigit() else 0
            
            return international
            
        except Exception as e:
            logger.error(f"Errore nell'estrazione statistiche internazionali: {str(e)}")
            return {}
    
    def _extract_h2h_summary(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Estrae il sommario statistico head-to-head.
        
        Args:
            soup: Oggetto BeautifulSoup della pagina
            
        Returns:
            Sommario statistico
        """
        try:
            summary = {}
            
            # Cerca tabella sommario
            summary_table = soup.select_one("table.versus-table")
            if not summary_table:
                return summary
            
            # Estrai celle della tabella
            cells = summary_table.select("td")
            if len(cells) < 5:
                return summary
            
            # Estrai statistiche
            team1_wins = cells[0].text.strip()
            draws = cells[2].text.strip()
            team2_wins = cells[4].text.strip()
            
            summary['team1_wins'] = int(team1_wins) if team1_wins.isdigit() else 0
            summary['draws'] = int(draws) if draws.isdigit() else 0
            summary['team2_wins'] = int(team2_wins) if team2_wins.isdigit() else 0
            summary['total_matches'] = summary['team1_wins'] + summary['draws'] + summary['team2_wins']
            
            # Estrai statistiche gol se disponibili
            goals_section = soup.select_one("div.versus-goals")
            if goals_section:
                goals_text = goals_section.text.strip()
                
                # Formato tipico: "Goals: Team1 100-80 Team2"
                goals_match = re.search(r'Goals:.*?(\d+)[^\d]+(\d+)', goals_text)
                if goals_match:
                    team1_goals = int(goals_match.group(1))
                    team2_goals = int(goals_match.group(2))
                    
                    summary['team1_goals'] = team1_goals
                    summary['team2_goals'] = team2_goals
                    summary['total_goals'] = team1_goals + team2_goals
            
            return summary
            
        except Exception as e:
            logger.error(f"Errore nell'estrazione sommario h2h: {str(e)}")
            return {}
    
    def _extract_h2h_matches(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """
        Estrae le partite head-to-head.
        
        Args:
            soup: Oggetto BeautifulSoup della pagina
            
        Returns:
            Lista di partite
        """
        try:
            matches = []
            
            # Cerca tabella partite
            matches_table = soup.select_one("table.matches-table")
            if not matches_table:
                return matches
            
            # Estrai righe della tabella
            rows = matches_table.select("tbody tr")
            for row in rows:
                cells = row.select("td")
                if len(cells) < 4:  # Numero minimo celle
                    continue
                
                # Estrai data
                date_cell = cells[0]
                date_text = date_cell.text.strip()
                match_date = None
                
                if date_text:
                    try:
                        # Formato tipico: "DD/MM/YYYY"
                        match_date = datetime.strptime(date_text, '%d/%m/%Y').strftime('%Y-%m-%d')
                    except ValueError:
                        pass
                
                # Estrai competizione
                competition_cell = cells[1]
                competition = competition_cell.text.strip()
                
                # Estrai risultato
                result_cell = cells[2]
                result_text = result_cell.text.strip()
                
                home_score = None
                away_score = None
                
                # Formato tipico: "3-1" o "3 - 1"
                result_match = re.search(r'(\d+)\s*-\s*(\d+)', result_text)
                if result_match:
                    home_score = int(result_match.group(1))
                    away_score = int(result_match.group(2))
                
                # Estrai dettagli/venue
                venue_cell = cells[3]
                venue = venue_cell.text.strip()
                
                match_data = {
                    'date': match_date,
                    'competition': competition,
                    'venue': venue
                }
                
                # Aggiungi punteggio se disponibile
                if home_score is not None and away_score is not None:
                    match_data['home_score'] = home_score
                    match_data['away_score'] = away_score
                    match_data['result'] = f"{home_score}-{away_score}"
                    
                    # Determina il vincitore (prospettiva team1)
                    if home_score > away_score:
                        match_data['winner'] = 'team1'
                    elif away_score > home_score:
                        match_data['winner'] = 'team2'
                    else:
                        match_data['winner'] = 'draw'
                
                matches.append(match_data)
            
            return matches
            
        except Exception as e:
            logger.error(f"Errore nell'estrazione partite h2h: {str(e)}")
            return []
    
    # ---- Metodi di supporto ---- #
    
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
            
        # Formato tipico: /teams/team-name/
        match = re.search(r'/teams/([^/]+)', url)
        if match:
            return match.group(1)
            
        # Formato internazionale: /international/team-name/
        match = re.search(r'/international/([^/]+)', url)
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
            
        # Formato tipico: /players/player-name/
        match = re.search(r'/players/([^/]+)', url)
        if match:
            return match.group(1)
            
        # Formato manager: /managers/manager-name/
        match = re.search(r'/managers/([^/]+)', url)
        if match:
            return match.group(1)
            
        return None

# Funzioni di utilità globali

def get_team_info(team_id: str) -> Dict[str, Any]:
    """
    Ottiene informazioni dettagliate su una squadra.
    
    Args:
        team_id: ID della squadra (path relativo o nome)
        
    Returns:
        Informazioni dettagliate sulla squadra
    """
    scraper = ElevenVElevenScraper()
    return scraper.get_team_info(team_id)

def get_all_time_table(competition_id: str) -> List[Dict[str, Any]]:
    """
    Ottiene la classifica all-time di una competizione.
    
    Args:
        competition_id: ID della competizione (chiave da competitions_map o path)
        
    Returns:
        Classifica all-time della competizione
    """
    scraper = ElevenVElevenScraper()
    return scraper.get_all_time_table(competition_id)

def get_player_info(player_id: str) -> Dict[str, Any]:
    """
    Ottiene informazioni dettagliate su un giocatore.
    
    Args:
        player_id: ID del giocatore (path relativo o nome)
        
    Returns:
        Informazioni dettagliate sul giocatore
    """
    scraper = ElevenVElevenScraper()
    return scraper.get_player_info(player_id)

def get_head_to_head(team1_id: str, team2_id: str) -> Dict[str, Any]:
    """
    Ottiene statistiche head-to-head tra due squadre.
    
    Args:
        team1_id: ID della prima squadra
        team2_id: ID della seconda squadra
        
    Returns:
        Statistiche head-to-head complete
    """
    scraper = ElevenVElevenScraper()
    return scraper.get_head_to_head(team1_id, team2_id)

def get_competition_seasons(competition_id: str) -> List[Dict[str, Any]]:
    """
    Ottiene le stagioni disponibili per una competizione.
    
    Args:
        competition_id: ID della competizione (chiave da competitions_map o path)
        
    Returns:
        Lista di stagioni disponibili
    """
    scraper = ElevenVElevenScraper()
    return scraper.get_competition_seasons(competition_id)

def get_competition_table(competition_id: str, season_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Ottiene la classifica di una competizione per una stagione specifica.
    
    Args:
        competition_id: ID della competizione (chiave da competitions_map o path)
        season_id: ID della stagione (opzionale, default: ultima stagione)
        
    Returns:
        Classifica della competizione
    """
    scraper = ElevenVElevenScraper()
    return scraper.get_competition_table(competition_id, season_id)

def get_competition_matches(competition_id: str, season_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Ottiene le partite di una competizione per una stagione specifica.
    
    Args:
        competition_id: ID della competizione (chiave da competitions_map o path)
        season_id: ID della stagione (opzionale, default: ultima stagione)
        
    Returns:
        Lista di partite
    """
    scraper = ElevenVElevenScraper()
    return scraper.get_competition_matches(competition_id, season_id)

def get_team_awards(team_id: str) -> List[Dict[str, Any]]:
    """
    Ottiene i trofei e riconoscimenti vinti da una squadra.
    
    Args:
        team_id: ID della squadra
        
    Returns:
        Lista di trofei e riconoscimenti
    """
    scraper = ElevenVElevenScraper()
    return scraper.get_team_awards(team_id)
