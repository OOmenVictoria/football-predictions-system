"""
Modulo per l'estrazione di dati da Wikipedia.
Questo modulo fornisce funzionalità per estrarre informazioni calcistiche
da pagine Wikipedia, incluse squadre, competizioni, giocatori e statistiche storiche.
"""
import re
import json
import time
import logging
import requests
from bs4 import BeautifulSoup
from typing import Dict, List, Any, Optional, Union

from src.utils.cache import cached
from src.utils.http import make_request, HTTPError
from src.config.settings import get_setting
from src.data.scrapers.base_scraper import BaseScraper

logger = logging.getLogger(__name__)

class WikipediaScraper(BaseScraper):
    """
    Scraper per estrarre informazioni calcistiche da Wikipedia.
    
    Fornisce funzionalità per l'estrazione di:
    - Informazioni squadre (storia, stadio, trofei)
    - Dati competizioni (storici, regolamento)
    - Profili giocatori (carriera, statistiche)
    - Classifiche storiche
    """
    
    def __init__(self):
        """Inizializza lo scraper Wikipedia."""
        # Imposta prima le proprietà necessarie
        self.lang = get_setting('scrapers.wikipedia.language', 'en')
    
        # Chiama super().__init__() con i parametri richiesti
        super().__init__(
            name="wikipedia",
            base_url=f"https://{self.lang}.wikipedia.org"
        )
    
        # API URL
        self.api_url = f"https://{self.lang}.wikipedia.org/w/api.php"
    
        # Cache time-to-live in secondi
        self.cache_ttl = get_setting('scrapers.wikipedia.cache_ttl', 86400)  # Default 24 hours
    
        logger.info(f"WikipediaScraper inizializzato con lingua: {self.lang}")
    
    def _make_api_request(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Effettua una richiesta all'API di Wikipedia.
        
        Args:
            params: Parametri per la richiesta API
            
        Returns:
            Risposta JSON dell'API
        """
        # Parametri di base per l'API
        base_params = {
            "format": "json",
            "action": "query",
            "formatversion": "2"
        }
        
        # Combina i parametri di base con quelli specifici
        all_params = {**base_params, **params}
        
        try:
            response = make_request(
                "GET", 
                self.api_url, 
                params=all_params,
                headers=self.headers
            )
            
            return response.json()
        except Exception as e:
            logger.error(f"Errore nella richiesta API Wikipedia: {str(e)}")
            return {}
    
    @cached(ttl=86400)  # 24 ore
    def search(self, query: str, limit: int = 5) -> List[Dict[str, str]]:
        """
        Cerca articoli su Wikipedia.
        
        Args:
            query: Termine di ricerca
            limit: Numero massimo di risultati
            
        Returns:
            Lista di risultati (titolo, descrizione, URL)
        """
        params = {
            "action": "query",
            "list": "search",
            "srsearch": query,
            "srlimit": limit
        }
        
        data = self._make_api_request(params)
        
        results = []
        for item in data.get("query", {}).get("search", []):
            title = item.get("title", "")
            snippet = re.sub(r'<[^>]+>', '', item.get("snippet", ""))  # Rimuovi HTML
            
            results.append({
                "title": title,
                "description": snippet,
                "url": f"{self.base_url}/wiki/{title.replace(' ', '_')}"
            })
        
        return results
    
    @cached(ttl=86400)  # 24 ore
    def get_page_content(self, title: str) -> Optional[str]:
        """
        Ottiene il contenuto di una pagina Wikipedia.
        
        Args:
            title: Titolo della pagina
            
        Returns:
            HTML della pagina o None se non trovata
        """
        try:
            url = f"{self.base_url}/wiki/{title.replace(' ', '_')}"
            
            response = make_request("GET", url, headers=self.headers)
            
            return response.text
        except Exception as e:
            logger.error(f"Errore nel recupero della pagina '{title}': {str(e)}")
            return None
    
    @cached(ttl=86400)  # 24 ore
    def get_page_sections(self, title: str) -> List[Dict[str, Any]]:
        """
        Ottiene le sezioni di una pagina Wikipedia.
        
        Args:
            title: Titolo della pagina
            
        Returns:
            Lista di sezioni (livello, titolo, indice)
        """
        params = {
            "action": "parse",
            "page": title,
            "prop": "sections"
        }
        
        data = self._make_api_request(params)
        
        sections = []
        for section in data.get("parse", {}).get("sections", []):
            sections.append({
                "index": section.get("index", ""),
                "level": section.get("level", ""),
                "title": section.get("line", "")
            })
        
        return sections
    
    @cached(ttl=86400)  # 24 ore
    def get_section_content(self, title: str, section_index: int) -> Optional[str]:
        """
        Ottiene il contenuto di una sezione specifica.
        
        Args:
            title: Titolo della pagina
            section_index: Indice della sezione
            
        Returns:
            HTML della sezione o None se non trovata
        """
        params = {
            "action": "parse",
            "page": title,
            "section": section_index,
            "prop": "text"
        }
        
        data = self._make_api_request(params)
        
        if "parse" in data and "text" in data["parse"]:
            return data["parse"]["text"].get("*", "")
        
        return None
    
    @cached(ttl=86400)  # 24 ore
    def get_team_info(self, team_name: str) -> Dict[str, Any]:
        """
        Estrae informazioni su una squadra di calcio.
        
        Args:
            team_name: Nome della squadra
            
        Returns:
            Informazioni sulla squadra
        """
        # Cerca la pagina della squadra
        search_results = self.search(f"{team_name} football club", limit=5)
        
        if not search_results:
            logger.warning(f"Nessun risultato trovato per '{team_name}'")
            return {}
        
        # Prendi il primo risultato (migliore corrispondenza)
        page_title = search_results[0]["title"]
        page_html = self.get_page_content(page_title)
        
        if not page_html:
            logger.warning(f"Impossibile recuperare la pagina per '{page_title}'")
            return {}
        
        soup = BeautifulSoup(page_html, 'html.parser')
        
        # Estrai informazioni di base dall'infobox
        info = self._extract_infobox(soup)
        
        # Aggiungi il titolo della pagina
        info["title"] = page_title
        info["url"] = f"{self.base_url}/wiki/{page_title.replace(' ', '_')}"
        
        # Estrai altre informazioni rilevanti
        info["history"] = self._extract_history(soup)
        info["honours"] = self._extract_honours(soup)
        info["stadium"] = self._extract_stadium(soup)
        
        return info
    
    def _extract_infobox(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Estrae informazioni dall'infobox di una pagina Wikipedia.
        
        Args:
            soup: BeautifulSoup object della pagina
            
        Returns:
            Dati estratti dall'infobox
        """
        info = {}
        
        # Trova l'infobox (tabella)
        infobox = soup.select_one(".infobox")
        
        if not infobox:
            return info
        
        # Estrai le righe dall'infobox
        rows = infobox.select("tr")
        
        for row in rows:
            # Trova l'intestazione della riga
            header = row.select_one("th")
            if not header:
                continue
            
            header_text = header.text.strip()
            
            # Trova il valore della riga
            value = row.select_one("td")
            if not value:
                continue
            
            value_text = value.text.strip()
            
            # Normalizza alcune chiavi comuni
            key = header_text.lower().replace(" ", "_")
            
            # Archivia il valore
            info[key] = value_text
            
            # Ottieni l'URL dell'immagine se presente
            if key == "image" or key == "logo":
                img = value.select_one("img")
                if img and img.get("src"):
                    info[f"{key}_url"] = img["src"]
        
        return info
    
    def _extract_history(self, soup: BeautifulSoup) -> str:
        """
        Estrae la sezione della storia della squadra.
        
        Args:
            soup: BeautifulSoup object della pagina
            
        Returns:
            Testo della sezione storia
        """
        # Cerca sezioni comuni per la storia
        history_sections = ["History", "Club history", "Storia", "Histoire"]
        
        for section in history_sections:
            history_section = soup.find(lambda tag: tag.name == "h2" and section in tag.text)
            
            if history_section:
                # Ottieni tutto il contenuto fino alla prossima sezione h2
                content = []
                for elem in history_section.next_siblings:
                    if elem.name == "h2":
                        break
                    if elem.name == "p":
                        content.append(elem.text.strip())
                
                return "\n\n".join(content)
        
        return ""
    
    def _extract_honours(self, soup: BeautifulSoup) -> Dict[str, List[str]]:
        """
        Estrae i trofei e onorificenze vinte dalla squadra.
        
        Args:
            soup: BeautifulSoup object della pagina
            
        Returns:
            Dizionario con categorie di trofei
        """
        honours = {}
        
        # Cerca sezioni comuni per trofei
        trophy_sections = ["Honours", "Palmarès", "Trophies", "Trofei"]
        
        for section in trophy_sections:
            honours_section = soup.find(lambda tag: tag.name in ["h2", "h3"] and section in tag.text)
            
            if honours_section:
                # Cerca tutte le liste che seguono
                current_category = "Domestic"
                
                for elem in honours_section.next_siblings:
                    if elem.name == "h2":
                        break
                    
                    # Controlla se è un nuovo sottotitolo
                    if elem.name == "h3" or elem.name == "h4":
                        current_category = elem.text.strip()
                        honours[current_category] = []
                    
                    # Estrai elementi della lista
                    if elem.name == "ul":
                        if current_category not in honours:
                            honours[current_category] = []
                        
                        for item in elem.select("li"):
                            honours[current_category].append(item.text.strip())
        
        return honours
    
    def _extract_stadium(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Estrae informazioni sullo stadio della squadra.
        
        Args:
            soup: BeautifulSoup object della pagina
            
        Returns:
            Informazioni sullo stadio
        """
        stadium_info = {}
        
        # Cerca lo stadio nell'infobox
        infobox = soup.select_one(".infobox")
        
        if infobox:
            stadium_row = infobox.find(lambda tag: tag.name == "th" and "Stadium" in tag.text)
            
            if stadium_row and stadium_row.find_next("td"):
                stadium_cell = stadium_row.find_next("td")
                stadium_name = stadium_cell.text.strip()
                
                stadium_info["name"] = stadium_name
                
                # Cerca il link allo stadio
                stadium_link = stadium_cell.select_one("a")
                if stadium_link and stadium_link.get("href"):
                    stadium_info["url"] = f"{self.base_url}{stadium_link['href']}"
        
        return stadium_info
    
    @cached(ttl=86400)  # 24 ore
    def get_league_info(self, league_name: str, country: Optional[str] = None) -> Dict[str, Any]:
        """
        Estrae informazioni su un campionato di calcio.
        
        Args:
            league_name: Nome del campionato
            country: Nome del paese (opzionale, migliora l'accuratezza della ricerca)
            
        Returns:
            Informazioni sul campionato
        """
        # Costruisci il termine di ricerca
        search_term = f"{country} {league_name}" if country else league_name
        search_term = f"{search_term} football league"
        
        # Cerca la pagina del campionato
        search_results = self.search(search_term, limit=5)
        
        if not search_results:
            logger.warning(f"Nessun risultato trovato per '{search_term}'")
            return {}
        
        # Prendi il primo risultato (migliore corrispondenza)
        page_title = search_results[0]["title"]
        page_html = self.get_page_content(page_title)
        
        if not page_html:
            logger.warning(f"Impossibile recuperare la pagina per '{page_title}'")
            return {}
        
        soup = BeautifulSoup(page_html, 'html.parser')
        
        # Estrai informazioni di base dall'infobox
        info = self._extract_infobox(soup)
        
        # Aggiungi il titolo della pagina
        info["title"] = page_title
        info["url"] = f"{self.base_url}/wiki/{page_title.replace(' ', '_')}"
        
        # Estrai le attuali squadre partecipanti
        info["current_teams"] = self._extract_teams_list(soup)
        
        # Estrai i campioni in carica
        info["current_champions"] = self._extract_current_champions(soup)
        
        # Estrai statistiche storiche
        info["statistics"] = self._extract_league_statistics(soup)
        
        return info
    
    def _extract_teams_list(self, soup: BeautifulSoup) -> List[Dict[str, str]]:
        """
        Estrae la lista delle squadre partecipanti al campionato.
        
        Args:
            soup: BeautifulSoup object della pagina
            
        Returns:
            Lista di squadre
        """
        teams = []
        
        # Cerca tabella con liste di squadre attuali
        tables = soup.find_all("table", class_="wikitable")
        
        for table in tables:
            # Controlla se la tabella contiene riferimenti a club/team attuali
            header = table.select_one("caption, th")
            if not header:
                continue
                
            header_text = header.text.lower()
            if any(term in header_text for term in ["current", "teams", "clubs", "participants", "squadre", "attuali"]):
                for row in table.select("tr")[1:]:  # Salta la riga di intestazione
                    cells = row.select("td")
                    
                    if not cells:
                        continue
                    
                    # Estrai nome squadra
                    team_cell = cells[0]  # Assume che la prima colonna contenga il nome
                    team_name = team_cell.text.strip()
                    
                    team_data = {"name": team_name}
                    
                    # Estrai link
                    team_link = team_cell.select_one("a")
                    if team_link and team_link.get("href"):
                        team_data["url"] = f"{self.base_url}{team_link['href']}"
                    
                    teams.append(team_data)
                
                # Se abbiamo trovato le squadre, interrompi
                if teams:
                    break
        
        return teams
    
    def _extract_current_champions(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Estrae informazioni sui campioni in carica.
        
        Args:
            soup: BeautifulSoup object della pagina
            
        Returns:
            Informazioni sui campioni in carica
        """
        champions = {}
        
        # Cerca nell'infobox
        infobox = soup.select_one(".infobox")
        
        if infobox:
            champion_row = infobox.find(lambda tag: tag.name == "th" and 
                                       any(term in tag.text.lower() for term in ["champion", "winner", "current", "holders"]))
            
            if champion_row and champion_row.find_next("td"):
                champion_cell = champion_row.find_next("td")
                champion_name = champion_cell.text.strip()
                
                champions["name"] = champion_name
                
                # Cerca l'anno o la stagione
                season_pattern = r"(19|20)\d{2}[–-](19|20)\d{2}"
                season_match = re.search(season_pattern, champion_cell.text)
                
                if season_match:
                    champions["season"] = season_match.group(0)
                
                # Cerca il link alla squadra
                champion_link = champion_cell.select_one("a")
                if champion_link and champion_link.get("href"):
                    champions["url"] = f"{self.base_url}{champion_link['href']}"
        
        return champions
    
    def _extract_league_statistics(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Estrae statistiche storiche del campionato.
        
        Args:
            soup: BeautifulSoup object della pagina
            
        Returns:
            Statistiche storiche
        """
        stats = {}
        
        # Cerca sezioni di statistiche
        stats_sections = ["Records", "Statistics", "Statistiche", "All-time", "Classifica perpetua"]
        
        for section in stats_sections:
            stats_section = soup.find(lambda tag: tag.name in ["h2", "h3"] and section in tag.text)
            
            if stats_section:
                # Cerca tabelle con statistiche
                table = None
                for elem in stats_section.next_siblings:
                    if elem.name == "h2":
                        break
                    if elem.name == "table":
                        table = elem
                        break
                
                if table:
                    # Estrai dati dalla tabella
                    records = []
                    
                    headers = [th.text.strip() for th in table.select("th")]
                    
                    for row in table.select("tr")[1:]:  # Salta la riga di intestazione
                        cells = row.select("td")
                        
                        if not cells:
                            continue
                        
                        record = {}
                        for i, cell in enumerate(cells):
                            if i < len(headers):
                                record[headers[i]] = cell.text.strip()
                        
                        records.append(record)
                    
                    stats[section.lower()] = records
        
        return stats
    
    @cached(ttl=86400)  # 24 ore
    def get_player_info(self, player_name: str) -> Dict[str, Any]:
        """
        Estrae informazioni su un giocatore di calcio.
        
        Args:
            player_name: Nome del giocatore
            
        Returns:
            Informazioni sul giocatore
        """
        # Cerca la pagina del giocatore
        search_results = self.search(f"{player_name} footballer", limit=5)
        
        if not search_results:
            logger.warning(f"Nessun risultato trovato per '{player_name}'")
            return {}
        
        # Prendi il primo risultato (migliore corrispondenza)
        page_title = search_results[0]["title"]
        page_html = self.get_page_content(page_title)
        
        if not page_html:
            logger.warning(f"Impossibile recuperare la pagina per '{page_title}'")
            return {}
        
        soup = BeautifulSoup(page_html, 'html.parser')
        
        # Estrai informazioni di base dall'infobox
        info = self._extract_infobox(soup)
        
        # Aggiungi il titolo della pagina
        info["title"] = page_title
        info["url"] = f"{self.base_url}/wiki/{page_title.replace(' ', '_')}"
        
        # Estrai informazioni sulla carriera
        info["career"] = self._extract_player_career(soup)
        
        # Estrai statistiche
        info["statistics"] = self._extract_player_statistics(soup)
        
        # Estrai informazioni sulla nazionale
        info["international"] = self._extract_player_international(soup)
        
        return info
    
    def _extract_player_career(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """
        Estrae informazioni sulla carriera di club di un giocatore.
        
        Args:
            soup: BeautifulSoup object della pagina
            
        Returns:
            Lista di club con periodi e statistiche
        """
        career = []
        
        # Cerca tabelle con la carriera del giocatore
        tables = soup.find_all("table", class_="wikitable")
        
        for table in tables:
            # Controlla se la tabella contiene riferimenti alla carriera di club
            header = table.select_one("caption, th")
            if not header:
                continue
                
            header_text = header.text.lower()
            if any(term in header_text for term in ["club", "career", "carriera", "senior"]):
                headers = [th.text.strip() for th in table.select("th")]
                
                for row in table.select("tr")[1:]:  # Salta la riga di intestazione
                    cells = row.select("td")
                    
                    if not cells:
                        continue
                    
                    club_data = {}
                    for i, cell in enumerate(cells):
                        if i < len(headers):
                            # Normalizza le intestazioni comuni
                            key = headers[i].lower()
                            if "club" in key:
                                key = "club"
                            elif "year" in key or "period" in key:
                                key = "years"
                            elif "app" in key or "game" in key:
                                key = "appearances"
                            elif "goal" in key:
                                key = "goals"
                            
                            club_data[key] = cell.text.strip()
                    
                    career.append(club_data)
                
                # Se abbiamo trovato la carriera, interrompi
                if career:
                    break
        
        return career
    
    def _extract_player_statistics(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Estrae statistiche dettagliate di un giocatore.
        
        Args:
            soup: BeautifulSoup object della pagina
            
        Returns:
            Statistiche del giocatore per stagione/club
        """
        stats = {}
        
        # Cerca sezioni di statistiche
        stats_sections = ["Career statistics", "Club statistics", "Statistiche", "Club career"]
        
        for section in stats_sections:
            stats_section = soup.find(lambda tag: tag.name in ["h2", "h3", "h4"] and section in tag.text)
            
            if stats_section:
                # Cerca tabelle con statistiche dettagliate
                table = None
                for elem in stats_section.next_siblings:
                    if elem.name in ["h2", "h3", "h4"]:
                        break
                    if elem.name == "table":
                        table = elem
                        break
                
                if table:
                    # Estrai dati dalla tabella
                    seasons = []
                    
                    headers = [th.text.strip() for th in table.select("th")]
                    
                    for row in table.select("tr")[1:]:  # Salta la riga di intestazione
                        cells = row.select("td")
                        
                        if not cells:
                            continue
                        
                        season_data = {}
                        for i, cell in enumerate(cells):
                            if i < len(headers):
                                season_data[headers[i]] = cell.text.strip()
                        
                        seasons.append(season_data)
                    
                    stats[section.lower()] = seasons
        
        return stats
    
    def _extract_player_international(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Estrae informazioni sulla carriera internazionale di un giocatore.
        
        Args:
            soup: BeautifulSoup object della pagina
            
        Returns:
            Informazioni sulla nazionale
        """
        international = {}
        
        # Cerca nell'infobox
        infobox = soup.select_one(".infobox")
        
        if infobox:
            # Cerca riferimenti alla nazionale
            national_team_row = infobox.find(lambda tag: tag.name == "th" and 
                                          any(term in tag.text.lower() for term in 
                                             ["national team", "nazionale", "international"]))
            
            if national_team_row and national_team_row.find_next("td"):
                team_cell = national_team_row.find_next("td")
                international["team"] = team_cell.text.strip()
                
                # Cerca il link
                team_link = team_cell.select_one("a")
                if team_link and team_link.get("href"):
                    international["team_url"] = f"{self.base_url}{team_link['href']}"
        
        # Cerca tabelle con la carriera internazionale
        tables = soup.find_all("table", class_="wikitable")
        
        for table in tables:
            # Controlla se la tabella contiene riferimenti alla carriera internazionale
            header = table.select_one("caption, th")
            if not header:
                continue
                
            header_text = header.text.lower()
            if any(term in header_text for term in ["international", "national team", "nazionale"]):
                # Estrai statistiche internazionali
                headers = [th.text.strip() for th in table.select("th")]
                
                international_stats = []
                for row in table.select("tr")[1:]:  # Salta la riga di intestazione
                    cells = row.select("td")
                    
                    if not cells:
                        continue
                    
                    stat = {}
                    for i, cell in enumerate(cells):
                        if i < len(headers):
                            stat[headers[i]] = cell.text.strip()
                    
                    international_stats.append(stat)
                
                international["statistics"] = international_stats
                break
        
        return international
    
    @cached(ttl=86400)  # 24 ore
    def get_match_history(self, team1: str, team2: str) -> List[Dict[str, Any]]:
        """
        Cerca la storia degli scontri diretti tra due squadre.
        
        Args:
            team1: Nome della prima squadra
            team2: Nome della seconda squadra
            
        Returns:
            Lista di partite precedenti
        """
        # Cerca pagine che potrebbero contenere la rivalry
        search_term = f"{team1} {team2} rivalry football"
        search_results = self.search(search_term, limit=5)
        
        matches = []
        
        if search_results:
            # Prova la prima pagina
            page_title = search_results[0]["title"]
            page_html = self.get_page_content(page_title)
            
            if page_html:
                soup = BeautifulSoup(page_html, 'html.parser')
                
                # Cerca tabelle con partite passate
                tables = soup.find_all("table", class_="wikitable")
                
                for table in tables:
                    # Controlla se la tabella contiene risultati di partite
                    header = table.select_one("caption, th")
                    if not header:
                        continue
                        
                    header_text = header.text.lower()
                    if any(term in header_text for term in ["results", "matches", "history", "recent", "partite", "risultati"]):
                        headers = [th.text.strip() for th in table.select("th")]
                        
                        for row in table.select("tr")[1:]:  # Salta la riga di intestazione
                            cells = row.select("td")
                            
                            if not cells or len(cells) < 3:  # Minimo data, squadre, risultato
                                continue
                            
                            match_data = {}
                            for i, cell in enumerate(cells):
                                if i < len(headers):
                                    match_data[headers[i]] = cell.text.strip()
                            
                            matches.append(match_data)
        
        return matches
    
    @cached(ttl=86400)  # 24 ore
    def get_competition_winners(self, competition_name: str) -> List[Dict[str, Any]]:
        """
        Ottiene la lista dei vincitori di una competizione.
        
        Args:
            competition_name: Nome della competizione
            
        Returns:
            Lista di vincitori con anni
        """
        # Cerca la pagina della competizione
        search_results = self.search(f"{competition_name} football tournament winners", limit=5)
        
        if not search_results:
            logger.warning(f"Nessun risultato trovato per '{competition_name}'")
            return []
        
        # Prendi il primo risultato (migliore corrispondenza)
        page_title = search_results[0]["title"]
        page_html = self.get_page_content(page_title)
        
        if not page_html:
            logger.warning(f"Impossibile recuperare la pagina per '{page_title}'")
            return []
        
        soup = BeautifulSoup(page_html, 'html.parser')
        
        winners = []
        
        # Cerca tabelle con vincitori
        tables = soup.find_all("table", class_="wikitable")
        
        for table in tables:
            # Controlla se la tabella contiene riferimenti a vincitori
            header = table.select_one("caption, th")
            if not header:
                continue
                
            header_text = header.text.lower()
            if any(term in header_text for term in ["winner", "champions", "vincitori", "vainqueurs", "season"]):
                headers = [th.text.strip() for th in table.select("th")]
                
                for row in table.select("tr")[1:]:  # Salta la riga di intestazione
                    cells = row.select("td")
                    
                    if not cells or len(cells) < 2:  # Minimo stagione e vincitore
                        continue
                    
                    # Estrai anno/stagione e vincitore
                    winner_data = {}
                    
                    # Cerca colonne per anno/stagione e vincitore
                    season_idx = -1
                    winner_idx = -1
                    
                    for i, header in enumerate(headers):
                        if any(term in header.lower() for term in ["season", "year", "stagione", "anno"]):
                            season_idx = i
                        if any(term in header.lower() for term in ["winner", "champion", "vincitore", "vainqueur"]):
                            winner_idx = i
                    
                    # Se non troviamo intestazioni specifiche, assumiamo che le prime due colonne siano stagione e vincitore
                    if season_idx == -1:
                        season_idx = 0
                    if winner_idx == -1:
                        winner_idx = 1
                    
                    # Estrai i dati
                    if len(cells) > season_idx:
                        winner_data["season"] = cells[season_idx].text.strip()
                    
                    if len(cells) > winner_idx:
                        winner_data["winner"] = cells[winner_idx].text.strip()
                        
                        # Cerca link alla squadra
                        winner_link = cells[winner_idx].select_one("a")
                        if winner_link and winner_link.get("href"):
                            winner_data["winner_url"] = f"{self.base_url}{winner_link['href']}"
                    
                    # Aggiungi altre colonne disponibili
                    for i, cell in enumerate(cells):
                        if i != season_idx and i != winner_idx and i < len(headers):
                            winner_data[headers[i]] = cell.text.strip()
                    
                    winners.append(winner_data)
                
                # Se abbiamo trovato vincitori, interrompi
                if winners:
                    break
        
        return winners

    @cached(ttl=86400)  # 24 ore
    def get_tournament_format(self, tournament_name: str) -> Dict[str, Any]:
        """
        Ottiene informazioni sul formato di un torneo.
        
        Args:
            tournament_name: Nome del torneo
            
        Returns:
            Informazioni sul formato (promozioni, retrocessioni, qualificazioni)
        """
        # Cerca la pagina del torneo
        search_results = self.search(f"{tournament_name} football tournament format", limit=5)
        
        if not search_results:
            logger.warning(f"Nessun risultato trovato per '{tournament_name}'")
            return {}
        
        # Prendi il primo risultato (migliore corrispondenza)
        page_title = search_results[0]["title"]
        page_html = self.get_page_content(page_title)
        
        if not page_html:
            logger.warning(f"Impossibile recuperare la pagina per '{page_title}'")
            return {}
        
        soup = BeautifulSoup(page_html, 'html.parser')
        
        # Estrai informazioni sul formato
        format_info = {
            "title": page_title,
            "url": f"{self.base_url}/wiki/{page_title.replace(' ', '_')}"
        }
        
        # Cerca sezioni sul formato
        format_sections = ["Format", "Competition format", "Structure", "System", "Formato", "Règlement"]
        
        for section in format_sections:
            format_section = soup.find(lambda tag: tag.name in ["h2", "h3", "h4"] and section in tag.text)
            
            if format_section:
                format_text = []
                
                # Ottieni tutto il contenuto fino alla prossima sezione
                for elem in format_section.next_siblings:
                    if elem.name in ["h2", "h3", "h4"]:
                        break
                    if elem.name == "p":
                        format_text.append(elem.text.strip())
                
                format_info["description"] = "\n\n".join(format_text)
                break
        
        # Estrai informazioni specifiche
        format_info["teams_count"] = self._extract_teams_count(soup)
        format_info["promotion"] = self._extract_promotion_info(soup)
        format_info["relegation"] = self._extract_relegation_info(soup)
        format_info["qualification"] = self._extract_qualification_info(soup)
        
        return format_info
    
    def _extract_teams_count(self, soup: BeautifulSoup) -> int:
        """
        Estrae il numero di squadre partecipanti al torneo.
        
        Args:
            soup: BeautifulSoup object della pagina
            
        Returns:
            Numero di squadre
        """
        # Cerca nell'infobox
        infobox = soup.select_one(".infobox")
        
        if infobox:
            teams_row = infobox.find(lambda tag: tag.name == "th" and 
                                  any(term in tag.text.lower() for term in ["teams", "clubs", "participants", "squadre"]))
            
            if teams_row and teams_row.find_next("td"):
                teams_text = teams_row.find_next("td").text.strip()
                
                # Estrai il numero dal testo
                numbers = re.findall(r'\d+', teams_text)
                if numbers:
                    return int(numbers[0])
        
        # Se non troviamo nell'infobox, cerca nel testo
        teams_pattern = r'(\d+)(?:\s+|-|)(?:teams|clubs|participants|squadre)'
        teams_match = re.search(teams_pattern, soup.text, re.IGNORECASE)
        
        if teams_match:
            return int(teams_match.group(1))
        
        return 0
    
    def _extract_promotion_info(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Estrae informazioni sulla promozione.
        
        Args:
            soup: BeautifulSoup object della pagina
            
        Returns:
            Informazioni sulla promozione
        """
        promotion = {}
        
        # Cerca sezioni sulla promozione
        promotion_sections = ["Promotion", "Promoted", "Promozione", "Promotion et relégation"]
        
        for section in promotion_sections:
            promotion_section = soup.find(lambda tag: tag.name in ["h2", "h3", "h4"] and section in tag.text)
            
            if promotion_section:
                promotion_text = []
                
                # Ottieni tutto il contenuto fino alla prossima sezione
                for elem in promotion_section.next_siblings:
                    if elem.name in ["h2", "h3", "h4"]:
                        break
                    if elem.name == "p":
                        promotion_text.append(elem.text.strip())
                
                promotion["description"] = "\n\n".join(promotion_text)
                break
        
        # Se non troviamo una sezione specifica, cerca nel testo
        if "description" not in promotion:
            # Cerca paragrafi che menzionano la promozione
            promotion_paras = soup.find_all("p", string=lambda text: 
                                         text and any(term in text.lower() for term in 
                                                   ["promoted", "promotion", "move up", "promozione"]))
            
            if promotion_paras:
                promotion["description"] = promotion_paras[0].text.strip()
        
        return promotion
    
    def _extract_relegation_info(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Estrae informazioni sulla retrocessione.
        
        Args:
            soup: BeautifulSoup object della pagina
            
        Returns:
            Informazioni sulla retrocessione
        """
        relegation = {}
        
        # Cerca sezioni sulla retrocessione
        relegation_sections = ["Relegation", "Relegated", "Retrocessione", "Relégation"]
        
        for section in relegation_sections:
            relegation_section = soup.find(lambda tag: tag.name in ["h2", "h3", "h4"] and section in tag.text)
            
            if relegation_section:
                relegation_text = []
                
                # Ottieni tutto il contenuto fino alla prossima sezione
                for elem in relegation_section.next_siblings:
                    if elem.name in ["h2", "h3", "h4"]:
                        break
                    if elem.name == "p":
                        relegation_text.append(elem.text.strip())
                
                relegation["description"] = "\n\n".join(relegation_text)
                break
        
        # Se non troviamo una sezione specifica, cerca nel testo
        if "description" not in relegation:
            # Cerca paragrafi che menzionano la retrocessione
            relegation_paras = soup.find_all("p", string=lambda text: 
                                         text and any(term in text.lower() for term in 
                                                   ["relegated", "relegation", "drop down", "retrocessione"]))
            
            if relegation_paras:
                relegation["description"] = relegation_paras[0].text.strip()
        
        return relegation
    
    def _extract_qualification_info(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Estrae informazioni sulle qualificazioni a competizioni europee.
        
        Args:
            soup: BeautifulSoup object della pagina
            
        Returns:
            Informazioni sulle qualificazioni
        """
        qualification = {}
        
        # Cerca sezioni sulle qualificazioni
        qualification_sections = ["Qualification", "European qualification", "Qualificazione", "Qualification européenne"]
        
        for section in qualification_sections:
            qualification_section = soup.find(lambda tag: tag.name in ["h2", "h3", "h4"] and section in tag.text)
            
            if qualification_section:
                qualification_text = []
                
                # Ottieni tutto il contenuto fino alla prossima sezione
                for elem in qualification_section.next_siblings:
                    if elem.name in ["h2", "h3", "h4"]:
                        break
                    if elem.name == "p":
                        qualification_text.append(elem.text.strip())
                
                qualification["description"] = "\n\n".join(qualification_text)
                break
        
        # Se non troviamo una sezione specifica, cerca nel testo
        if "description" not in qualification:
            # Cerca paragrafi che menzionano le qualificazioni
            qualification_paras = soup.find_all("p", string=lambda text: 
                                             text and any(term in text.lower() for term in 
                                                       ["champions league", "europa league", "uefa", "european"]))
            
            if qualification_paras:
                qualification["description"] = qualification_paras[0].text.strip()
        
        return qualification

# Funzioni di utilità globali
def search_wikipedia(query: str, limit: int = 5) -> List[Dict[str, str]]:
    """
    Cerca articoli su Wikipedia.
    
    Args:
        query: Termine di ricerca
        limit: Numero massimo di risultati
        
    Returns:
        Lista di risultati (titolo, descrizione, URL)
    """
    scraper = WikipediaScraper()
    return scraper.search(query, limit)

def get_team_info(team_name: str) -> Dict[str, Any]:
    """
    Ottiene informazioni su una squadra di calcio da Wikipedia.
    
    Args:
        team_name: Nome della squadra
        
    Returns:
        Informazioni sulla squadra
    """
    scraper = WikipediaScraper()
    return scraper.get_team_info(team_name)

def get_league_info(league_name: str, country: Optional[str] = None) -> Dict[str, Any]:
    """
    Ottiene informazioni su un campionato di calcio da Wikipedia.
    
    Args:
        league_name: Nome del campionato
        country: Nome del paese (opzionale)
        
    Returns:
        Informazioni sul campionato
    """
    scraper = WikipediaScraper()
    return scraper.get_league_info(league_name, country)

def get_player_info(player_name: str) -> Dict[str, Any]:
    """
    Ottiene informazioni su un giocatore di calcio da Wikipedia.
    
    Args:
        player_name: Nome del giocatore
        
    Returns:
        Informazioni sul giocatore
    """
    scraper = WikipediaScraper()
    return scraper.get_player_info(player_name)

def get_competition_winners(competition_name: str) -> List[Dict[str, Any]]:
    """
    Ottiene la lista dei vincitori di una competizione da Wikipedia.
    
    Args:
        competition_name: Nome della competizione
        
    Returns:
        Lista di vincitori con anni
    """
    scraper = WikipediaScraper()
    return scraper.get_competition_winners(competition_name)

def get_match_history(team1: str, team2: str) -> List[Dict[str, Any]]:
    """
    Ottiene la storia degli scontri diretti tra due squadre da Wikipedia.
    
    Args:
        team1: Nome della prima squadra
        team2: Nome della seconda squadra
        
    Returns:
        Lista di partite precedenti
    """
    scraper = WikipediaScraper()
    return scraper.get_match_history(team1, team2)

# Alla fine del file wikipedia.py

# Istanza globale
_scraper_instance = None

def get_scraper():
    """
    Ottiene l'istanza globale dello scraper Wikipedia.
    
    Returns:
        Istanza di WikipediaScraper.
    """
    global _scraper_instance
    if _scraper_instance is None:
        _scraper_instance = WikipediaScraper()
    return _scraper_instance
