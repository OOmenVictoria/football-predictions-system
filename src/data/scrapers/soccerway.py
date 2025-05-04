"""
Modulo per l'estrazione di dati da Soccerway.
Questo modulo fornisce funzionalità per estrarre dati su partite, squadre e giocatori
dal sito Soccerway (https://int.soccerway.com/).
"""
import os
import re
import time
import json
import logging
import random
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union, Tuple
from urllib.parse import urljoin, urlparse, parse_qs

from bs4 import BeautifulSoup
import requests

from src.data.scrapers.base_scraper import BaseScraper
from src.config.settings import get_setting
from src.utils.cache import cached
from src.utils.time_utils import parse_date, date_to_str

logger = logging.getLogger(__name__)

class SoccerwayScraper(BaseScraper):
    """
    Scraper per estrazione dati da Soccerway.
    
    Implementa funzionalità per estrarre informazioni su partite, squadre,
    giocatori e competizioni dal sito Soccerway.
    """
    
    def __init__(self):
        """Inizializza lo scraper Soccerway."""
        super().__init__()
        self.base_url = "https://int.soccerway.com"
        self.search_url = "https://int.soccerway.com/search/?"
        self.matches_url = "https://int.soccerway.com/matches/"
        self.teams_url = "https://int.soccerway.com/teams/"
        self.competitions_url = "https://int.soccerway.com/competitions/"
        
        # Parametri specifici per Soccerway
        self.request_delay = get_setting("scraping.soccerway.delay", 3)
        self.max_retries = get_setting("scraping.soccerway.max_retries", 3)
        
        # Mappatura competizioni
        self.competitions_map = get_setting("scraping.soccerway.competitions", {
            "premier_league": "/national/england/premier-league/",
            "serie_a": "/national/italy/serie-a/",
            "la_liga": "/national/spain/primera-division/",
            "bundesliga": "/national/germany/bundesliga/",
            "ligue_1": "/national/france/ligue-1/",
            "eredivisie": "/national/netherlands/eredivisie/",
            "primeira_liga": "/national/portugal/portuguese-liga-/",
            "super_lig": "/national/turkey/super-lig/",
            "championship": "/national/england/championship/",
            "serie_b": "/national/italy/serie-b/",
            "champions_league": "/international/europe/uefa-champions-league/",
            "europa_league": "/international/europe/uefa-europa-league/"
        })
        
        logger.info(f"SoccerwayScraper inizializzato con base URL: {self.base_url}")
    
    @cached(ttl=3600 * 12)  # 12 ore
    def search(self, query: str, category: str = "all") -> List[Dict[str, Any]]:
        """
        Esegue una ricerca su Soccerway.
        
        Args:
            query: Termine di ricerca
            category: Categoria di ricerca ("all", "teams", "players", "competitions")
            
        Returns:
            Lista di risultati di ricerca
        """
        logger.info(f"Ricerca Soccerway per '{query}' (categoria: {category})")
        
        # Costruisci i parametri di ricerca
        params = {
            "q": query
        }
        
        # Mappa delle categorie
        if category == "teams":
            params["module"] = "team"
        elif category == "players":
            params["module"] = "player"
        elif category == "competitions":
            params["module"] = "competition"
        
        try:
            # Effettua la richiesta
            response = self.session.get(
                self.search_url,
                params=params,
                headers=self.headers,
                timeout=self.timeout
            )
            
            # Controlla risposta
            if response.status_code != 200:
                logger.warning(f"Errore nella ricerca: HTTP {response.status_code}")
                return []
            
            # Analizza HTML
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Estrai risultati
            results = []
            
            # Cerca nella tabella dei risultati
            result_tables = soup.select("table.search-results")
            
            if not result_tables:
                logger.warning("Nessun risultato trovato")
                return []
            
            for table in result_tables:
                # Ottieni categoria della tabella
                category_title = table.find_previous("h2")
                category_name = category_title.text.strip() if category_title else "Risultati"
                
                # Estrai righe
                rows = table.select("tbody tr")
                
                for row in rows:
                    # Estrai link e nome
                    link_element = row.select_one("a")
                    if not link_element:
                        continue
                    
                    name = link_element.text.strip()
                    link = link_element.get("href", "")
                    
                    # Estrai tipo di risultato
                    type_element = row.select_one("td:nth-child(2)")
                    result_type = type_element.text.strip() if type_element else ""
                    
                    # Costruisci URL completo
                    full_url = urljoin(self.base_url, link)
                    
                    # Estrai ID dalla URL
                    result_id = self._extract_id_from_url(full_url)
                    
                    # Aggiungi ai risultati
                    results.append({
                        "id": result_id,
                        "name": name,
                        "type": result_type,
                        "category": category_name,
                        "url": full_url
                    })
            
            logger.info(f"Trovati {len(results)} risultati")
            return results
            
        except Exception as e:
            logger.error(f"Errore nella ricerca: {e}")
            return []
    
    def _extract_id_from_url(self, url: str) -> str:
        """
        Estrae l'ID numerico da una URL di Soccerway.
        
        Args:
            url: URL da analizzare
            
        Returns:
            ID estratto o stringa vuota se non trovato
        """
        # Pattern per ID nella URL
        pattern = r"/([cp]?[0-9]+)(?:/|$)"
        match = re.search(pattern, url)
        
        if match:
            return match.group(1)
        
        # Alternativa: estrai dalla query string
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        
        if "id" in query_params:
            return query_params["id"][0]
        
        return ""
    
    @cached(ttl=3600 * 24)  # 24 ore
    def get_team_info(self, team_id: str) -> Dict[str, Any]:
        """
        Ottiene informazioni dettagliate su una squadra.
        
        Args:
            team_id: ID Soccerway della squadra
            
        Returns:
            Dati completi della squadra
        """
        logger.info(f"Estrazione info squadra con ID {team_id}")
        
        # Gestisci ID o URL completo
        if team_id.startswith("http"):
            team_url = team_id
        else:
            # Cerca la squadra per ID
            team_url = f"{self.teams_url}teams/{team_id}/"
        
        try:
            # Effettua la richiesta
            response = self.session.get(
                team_url,
                headers=self.headers,
                timeout=self.timeout
            )
            
            # Controlla risposta
            if response.status_code != 200:
                logger.warning(f"Errore nel recupero squadra: HTTP {response.status_code}")
                return {}
            
            # Analizza HTML
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Estrai informazioni base
            team_name = self._extract_team_name(soup)
            country = self._extract_team_country(soup)
            logo_url = self._extract_team_logo(soup)
            
            # Estrai informazioni stadio
            stadium = self._extract_team_stadium(soup)
            
            # Estrai informazioni sullo staff
            staff = self._extract_team_staff(soup)
            
            # Estrai statistiche di partite recenti
            recent_form = self._extract_team_form(soup)
            
            # Estrai rosa della squadra
            squad = self._extract_team_squad(soup)
            
            # Componi il risultato
            team_info = {
                "team_id": team_id,
                "name": team_name,
                "country": country,
                "logo_url": logo_url,
                "stadium": stadium,
                "staff": staff,
                "recent_form": recent_form,
                "squad": squad,
                "source": "soccerway",
                "source_url": team_url,
                "last_update": datetime.now().isoformat()
            }
            
            return team_info
            
        except Exception as e:
            logger.error(f"Errore nel recupero squadra: {e}")
            return {}
    
    def _extract_team_name(self, soup: BeautifulSoup) -> str:
        """Estrae il nome della squadra."""
        name_element = soup.select_one("div.team-header h1")
        return name_element.text.strip() if name_element else ""
    
    def _extract_team_country(self, soup: BeautifulSoup) -> str:
        """Estrae il paese della squadra."""
        country_element = soup.select_one("div.team-header span.country")
        return country_element.text.strip() if country_element else ""
    
    def _extract_team_logo(self, soup: BeautifulSoup) -> str:
        """Estrae l'URL del logo della squadra."""
        logo_element = soup.select_one("div.team-header img.logo")
        return logo_element.get("src", "") if logo_element else ""
    
    def _extract_team_stadium(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Estrae le informazioni sullo stadio della squadra."""
        stadium = {}
        
        # Cerca nella tabella delle informazioni
        info_table = soup.select_one("div.details-box table")
        if info_table:
            rows = info_table.select("tr")
            for row in rows:
                header = row.select_one("th")
                value = row.select_one("td")
                
                if header and value and "Venue" in header.text:
                    stadium["name"] = value.text.strip()
                    break
        
        return stadium
    
    def _extract_team_staff(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Estrae le informazioni sullo staff della squadra."""
        staff = []
        
        # Cerca nella sezione dello staff
        staff_table = soup.select_one("table.staffs")
        if staff_table:
            rows = staff_table.select("tbody tr")
            for row in rows:
                name_element = row.select_one("td.name")
                role_element = row.select_one("td.position")
                
                if name_element and role_element:
                    name = name_element.text.strip()
                    role = role_element.text.strip()
                    
                    staff.append({
                        "name": name,
                        "role": role
                    })
        
        return staff
    
    def _extract_team_form(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Estrae la forma recente della squadra."""
        form = {
            "sequence": [],
            "last_matches": []
        }
        
        # Cerca il blocco di forma
        form_block = soup.select_one("div.block_team_form")
        if form_block:
            # Estrai la sequenza di forma
            form_sequence = form_block.select("span.form-icon")
            for icon in form_sequence:
                result = icon.get("class", [""])[0].split("-")[-1]
                form["sequence"].append(result.upper())
            
            # Estrai le ultime partite
            match_rows = form_block.select("table.matches-table tbody tr")
            for row in match_rows:
                date_element = row.select_one("td.date")
                home_element = row.select_one("td.team-a")
                away_element = row.select_one("td.team-b")
                score_element = row.select_one("td.score-time")
                competition_element = row.select_one("td.competition")
                
                if date_element and home_element and away_element and score_element:
                    date_str = date_element.text.strip()
                    home_team = home_element.text.strip()
                    away_team = away_element.text.strip()
                    score = score_element.text.strip()
                    competition = competition_element.text.strip() if competition_element else ""
                    
                    match_info = {
                        "date": date_str,
                        "home_team": home_team,
                        "away_team": away_team,
                        "score": score,
                        "competition": competition
                    }
                    
                    form["last_matches"].append(match_info)
        
        return form
    
    def _extract_team_squad(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Estrae la rosa della squadra."""
        squad = []
        
        # Cerca nella tabella della rosa
        squad_table = soup.select_one("table.squad")
        if squad_table:
            rows = squad_table.select("tbody tr")
            for row in rows:
                # Estrai informazioni giocatore
                number_element = row.select_one("td.shirt")
                name_element = row.select_one("td.name a")
                pos_element = row.select_one("td.position")
                age_element = row.select_one("td.age")
                
                if name_element:
                    # Estrai dati
                    number = number_element.text.strip() if number_element else ""
                    name = name_element.text.strip()
                    position = pos_element.text.strip() if pos_element else ""
                    age = age_element.text.strip() if age_element else ""
                    
                    # Estrai URL e ID
                    player_url = name_element.get("href", "")
                    player_id = self._extract_id_from_url(player_url)
                    
                    # Costruisci URL completo
                    full_url = urljoin(self.base_url, player_url)
                    
                    # Aggiungi alla rosa
                    squad.append({
                        "player_id": player_id,
                        "name": name,
                        "number": number,
                        "position": position,
                        "age": age,
                        "url": full_url
                    })
        
        return squad
    
    @cached(ttl=3600 * 4)  # 4 ore
    def get_league_table(self, league_id: str) -> Dict[str, Any]:
        """
        Ottiene la classifica di un campionato.
        
        Args:
            league_id: ID Soccerway o chiave del campionato
            
        Returns:
            Classifica completa del campionato
        """
        logger.info(f"Estrazione classifica per league_id={league_id}")
        
        # Gestisci ID o URL completo o chiave di mappa
        if league_id.startswith("http"):
            league_url = league_id
        elif league_id in self.competitions_map:
            league_url = self.base_url + self.competitions_map[league_id]
        else:
            # Costruisci URL della competizione
            league_url = f"{self.competitions_url}{league_id}/"
        
        try:
            # Effettua la richiesta
            response = self.session.get(
                league_url,
                headers=self.headers,
                timeout=self.timeout
            )
            
            # Controlla risposta
            if response.status_code != 200:
                logger.warning(f"Errore nel recupero classifica: HTTP {response.status_code}")
                return {}
            
            # Analizza HTML
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Estrai nome campionato
            league_name = self._extract_league_name(soup)
            
            # Estrai stagione
            season = self._extract_league_season(soup)
            
            # Estrai classifica
            standings = self._extract_league_standings(soup)
            
            # Estrai informazioni aggiuntive
            info = self._extract_league_info(soup)
            
            # Componi il risultato
            league_data = {
                "league_id": league_id,
                "name": league_name,
                "season": season,
                "standings": standings,
                "info": info,
                "source": "soccerway",
                "source_url": league_url,
                "last_update": datetime.now().isoformat()
            }
            
            return league_data
            
        except Exception as e:
            logger.error(f"Errore nel recupero classifica: {e}")
            return {}
    
    def _extract_league_name(self, soup: BeautifulSoup) -> str:
        """Estrae il nome del campionato."""
        name_element = soup.select_one("div.tournament-header h1")
        return name_element.text.strip() if name_element else ""
    
    def _extract_league_season(self, soup: BeautifulSoup) -> str:
        """Estrae la stagione del campionato."""
        season_element = soup.select_one("div.tournament-header span.season")
        return season_element.text.strip() if season_element else ""
    
    def _extract_league_info(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Estrae informazioni aggiuntive sul campionato."""
        info = {}
        
        # Cerca nella tabella delle informazioni
        info_table = soup.select_one("div.tournament-card table")
        if info_table:
            rows = info_table.select("tr")
            for row in rows:
                header = row.select_one("th")
                value = row.select_one("td")
                
                if header and value:
                    header_text = header.text.strip().lower()
                    value_text = value.text.strip()
                    
                    # Mappa header a campi informativi
                    if "country" in header_text:
                        info["country"] = value_text
                    elif "teams" in header_text:
                        info["teams_count"] = value_text
                    elif "champion" in header_text:
                        info["champion"] = value_text
                    elif "confederation" in header_text:
                        info["confederation"] = value_text
                    else:
                        info[header_text] = value_text
        
        return info
    
    def _extract_league_standings(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Estrae la classifica del campionato."""
        standings = []
        
        # Cerca nella tabella della classifica
        standings_table = soup.select_one("table.leaguetable")
        if standings_table:
            rows = standings_table.select("tbody tr")
            for row in rows:
                # Ignora righe di intestazione o spaziatura
                if "subheader" in row.get("class", []):
                    continue
                
                # Estrai posizione
                position_element = row.select_one("td.rank")
                team_element = row.select_one("td.team a")
                
                # Salta se non ci sono elementi essenziali
                if not position_element or not team_element:
                    continue
                
                # Estrai statistiche
                position = position_element.text.strip()
                team_name = team_element.text.strip()
                team_url = team_element.get("href", "")
                team_id = self._extract_id_from_url(team_url)
                
                # Estrai colonne statistiche
                cols = row.select("td")
                
                # Mappa le colonne a statistiche
                stats = {
                    "position": position,
                    "team_id": team_id,
                    "team": team_name,
                    "played": self._get_text(cols, 3),
                    "wins": self._get_text(cols, 4),
                    "draws": self._get_text(cols, 5),
                    "losses": self._get_text(cols, 6),
                    "goals_for": self._get_text(cols, 7),
                    "goals_against": self._get_text(cols, 8),
                    "goal_difference": self._get_text(cols, 9),
                    "points": self._get_text(cols, 10)
                }
                
                # Converti valori numerici
                for key in ["played", "wins", "draws", "losses", "goals_for", 
                           "goals_against", "goal_difference", "points"]:
                    try:
                        stats[key] = int(stats[key])
                    except (ValueError, TypeError):
                        stats[key] = 0
                
                standings.append(stats)
        
        return standings
    
    def _get_text(self, elements: List, index: int) -> str:
        """Estrae il testo da un elemento a un indice specifico."""
        if index < len(elements):
            return elements[index].text.strip()
        return ""
    
    @cached(ttl=3600 * 2)  # 2 ore
    def get_matches_by_date(self, date: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        Ottiene le partite per una data specifica.
        
        Args:
            date: Data per cui recuperare le partite (default: oggi)
            
        Returns:
            Lista di partite per la data specificata
        """
        # Imposta la data predefinita a oggi se non specificata
        if date is None:
            date = datetime.now()
        
        date_str = date.strftime("%Y/%m/%d")
        logger.info(f"Estrazione partite per data {date_str}")
        
        # Costruisci URL
        matches_url = f"{self.matches_url}{date_str}/"
        
        try:
            # Effettua la richiesta
            response = self.session.get(
                matches_url,
                headers=self.headers,
                timeout=self.timeout
            )
            
            # Controlla risposta
            if response.status_code != 200:
                logger.warning(f"Errore nel recupero partite: HTTP {response.status_code}")
                return []
            
            # Analizza HTML
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Estrai partite
            matches = []
            
            # Cerca blocchi di competizioni
            competition_blocks = soup.select("div.competition-matches")
            
            for block in competition_blocks:
                # Estrai nome competizione
                competition_header = block.select_one("div.table-header")
                competition_name = competition_header.text.strip() if competition_header else "Sconosciuto"
                
                # Estrai partite della competizione
                match_rows = block.select("table.matches tbody tr")
                
                for row in match_rows:
                    # Salta intestazioni o righe non valide
                    if "subheader" in row.get("class", []):
                        continue
                    
                    # Estrai link della partita
                    match_link = row.select_one("td.score-time a")
                    if not match_link:
                        continue
                    
                    match_url = match_link.get("href", "")
                    match_id = self._extract_id_from_url(match_url)
                    
                    # Estrai squadre
                    home_element = row.select_one("td.team-a")
                    away_element = row.select_one("td.team-b")
                    
                    if not home_element or not away_element:
                        continue
                    
                    home_team = home_element.text.strip()
                    away_team = away_element.text.strip()
                    
                    # Estrai orario o risultato
                    score_element = row.select_one("td.score-time")
                    score_time = score_element.text.strip() if score_element else ""
                    
                    # Determina se partita completata, in corso o futura
                    status = "SCHEDULED"
                    home_score = None
                    away_score = None
                    
                    if "-" in score_time and ":" not in score_time:
                        # Formato risultato: "1-0"
                        status = "FINISHED"
                        scores = score_time.split("-")
                        if len(scores) == 2:
                            try:
                                home_score = int(scores[0].strip())
                                away_score = int(scores[1].strip())
                            except ValueError:
                                pass
                    elif "'" in score_time:
                        # Partita in corso (es. "45'")
                        status = "IN_PLAY"
                    
                    # Costruisci URL completo
                    full_match_url = urljoin(self.base_url, match_url)
                    
                    # Componi il risultato
                    match_info = {
                        "match_id": match_id,
                        "competition": competition_name,
                        "home_team": home_team,
                        "away_team": away_team,
                        "score_time": score_time,
                        "status": status,
                        "date": date_str,
                        "url": full_match_url,
                        "source": "soccerway"
                    }
                    
                    # Aggiungi risultato se disponibile
                    if home_score is not None and away_score is not None:
                        match_info["home_score"] = home_score
                        match_info["away_score"] = away_score
                    
                    matches.append(match_info)
            
            logger.info(f"Trovate {len(matches)} partite per {date_str}")
            return matches
            
        except Exception as e:
            logger.error(f"Errore nel recupero partite: {e}")
            return []
    
    @cached(ttl=3600 * 2)  # 2 ore
    def get_match_details(self, match_id: str) -> Dict[str, Any]:
        """
        Ottiene dettagli completi di una partita.
        
        Args:
            match_id: ID Soccerway della partita
            
        Returns:
            Dati completi della partita
        """
        logger.info(f"Estrazione dettagli partita con ID {match_id}")
        
        # Gestisci ID o URL completo
        if match_id.startswith("http"):
            match_url = match_id
        else:
            # Costruisci URL della partita
            match_url = f"{self.matches_url}match/{match_id}/"
        
        try:
            # Effettua la richiesta
            response = self.session.get(
                match_url,
                headers=self.headers,
                timeout=self.timeout
            )
            
            # Controlla risposta
            if response.status_code != 200:
                logger.warning(f"Errore nel recupero partita: HTTP {response.status_code}")
                return {}
            
            # Analizza HTML
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Estrai informazioni base
            match_info = self._extract_match_info(soup)
            
            # Estrai statistiche
            stats = self._extract_match_stats(soup)
            
            # Estrai eventi
            events = self._extract_match_events(soup)
            
            # Estrai formazioni
            lineups = self._extract_match_lineups(soup)
            
            # Componi il risultato
            match_data = {
                "match_id": match_id,
                "info": match_info,
                "stats": stats,
                "events": events,
                "lineups": lineups,
                "source": "soccerway",
                "source_url": match_url,
                "last_update": datetime.now().isoformat()
            }
            
            return match_data
            
        except Exception as e:
            logger.error(f"Errore nel recupero partita: {e}")
            return {}
    
    def _extract_match_info(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Estrae le informazioni base della partita."""
        info = {}
        
        # Estrai nome delle squadre
        home_element = soup.select_one("div.container-left div.team")
        away_element = soup.select_one("div.container-right div.team")
        
        if home_element and away_element:
            info["home_team"] = home_element.text.strip()
            info["away_team"] = away_element.text.strip()
            
            # Estrai URL delle squadre
            home_link = home_element.select_one("a")
            away_link = away_element.select_one("a")
            
            if home_link and away_link:
                home_url = home_link.get("href", "")
                away_url = away_link.get("href", "")
                
                info["home_team_id"] = self._extract_id_from_url(home_url)
                info["away_team_id"] = self._extract_id_from_url(away_url)
        
        # Estrai risultato
        score_element = soup.select_one("div.container-center div.score-container")
        if score_element:
            score_text = score_element.text.strip()
            
            # Estrai risultato dal formato "1 - 0"
            if "-" in score_text:
                scores = score_text.split("-")

# Alla fine del file soccerway.py

# Istanza globale
_scraper_instance = None

def get_scraper():
    """
    Ottiene l'istanza globale dello scraper Soccerway.
    
    Returns:
        Istanza di SoccerwayScraper.
    """
    global _scraper_instance
    if _scraper_instance is None:
        _scraper_instance = SoccerwayScraper()
    return _scraper_instance
