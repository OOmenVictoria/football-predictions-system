""" 
Modulo per l'estrazione di dati dal RSSSF.
Questo modulo fornisce funzionalità per ottenere dati calcistici dal
Rec.Sport.Soccer Statistics Foundation (http://www.rsssf.com/).
"""
import os
import re
import logging
import requests
import time
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union, Tuple

from src.utils.cache import cached
from src.utils.http import make_request
from src.config.settings import get_setting
from src.utils.database import FirebaseManager

logger = logging.getLogger(__name__)

class RSSFFScraper:
    """
    Scraper per l'estrazione di dati dal Rec.Sport.Soccer Statistics Foundation (RSSSF).
    
    RSSSF è un'ampia raccolta di statistiche calcistiche storiche, inclusi risultati,
    classifiche e record di vari campionati e competizioni di tutto il mondo.
    """
    
    def __init__(self):
        """Inizializza lo scraper RSSSF."""
        self.base_url = "http://www.rsssf.com"
        self.archive_url = "http://www.rsssf.com/archive.html"
        self.country_indices = {
            "england": "tableng",
            "italy": "tablital",
            "spain": "tablespa",
            "germany": "tableger",
            "france": "tablefra",
            "netherlands": "tablenel",
            "portugal": "tablepor",
            "brazil": "tablebra",
            "argentina": "tablearg"
        }
        self.cache_ttl = get_setting('open_data.rsssf.cache_ttl', 86400 * 7)  # 7 giorni
        self.db = FirebaseManager()
        
        logger.info("RSSFFScraper inizializzato")
    
    @cached(ttl=86400 * 7)  # Cache di 7 giorni
    def get_country_archive_url(self, country_id: str) -> Optional[str]:
        """
        Ottiene l'URL dell'archivio di un paese specifico.
        
        Args:
            country_id: ID del paese (es. 'england')
            
        Returns:
            URL dell'archivio del paese, o None se non trovato
        """
        index_id = self.country_indices.get(country_id.lower())
        if not index_id:
            logger.warning(f"Nessun indice trovato per il paese {country_id}")
            return None
        
        try:
            # Ottieni la pagina principale degli archivi
            response = make_request(self.archive_url)
            if not response:
                logger.error("Impossibile accedere alla pagina degli archivi RSSSF")
                return None
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Cerca il link all'archivio del paese
            for link in soup.find_all('a'):
                href = link.get('href')
                if href and index_id in href.lower():
                    return f"{self.base_url}/{href}"
            
            logger.warning(f"Link all'archivio non trovato per {country_id}")
            return None
            
        except Exception as e:
            logger.error(f"Errore nel recupero dell'URL dell'archivio per {country_id}: {e}")
            return None
    
    @cached(ttl=86400 * 7)  # Cache di 7 giorni
    def get_seasons_for_country(self, country_id: str) -> List[Dict[str, Any]]:
        """
        Ottiene la lista delle stagioni disponibili per un paese.
        
        Args:
            country_id: ID del paese (es. 'england')
            
        Returns:
            Lista delle stagioni disponibili con URL
        """
        archive_url = self.get_country_archive_url(country_id)
        if not archive_url:
            return []
        
        try:
            # Ottieni la pagina dell'archivio del paese
            response = make_request(archive_url)
            if not response:
                logger.error(f"Impossibile accedere alla pagina dell'archivio per {country_id}")
                return []
            
            soup = BeautifulSoup(response.text, 'html.parser')
            seasons = []
            
            # Estrai i link alle stagioni
            # I pattern tipici per RSSSF sono "country{year}.html" o "country{year}-{year+1}.html"
            for link in soup.find_all('a'):
                href = link.get('href')
                text = link.get_text().strip()
                
                if not href or not text:
                    continue
                
                # Pattern per le stagioni (anno o intervallo di anni)
                year_pattern = r'(\d{4})(?:-(\d{2,4}))?'
                match = re.search(year_pattern, text)
                
                # Pattern tipico per i link RSSSF
                url_pattern = r'(' + country_id.lower() + r'\d+)'
                url_match = re.search(url_pattern, href.lower())
                
                if match and url_match:
                    start_year = match.group(1)
                    end_year = match.group(2)
                    
                    # Normalizza l'anno finale se necessario (es. 96 -> 1996)
                    if end_year and len(end_year) == 2:
                        prefix = start_year[:2]  # Prendi le prime due cifre dell'anno iniziale
                        end_year = prefix + end_year
                    
                    season_label = start_year
                    if end_year:
                        season_label = f"{start_year}-{end_year}"
                    
                    # Costruisci l'URL completo
                    if href.startswith('http'):
                        season_url = href
                    else:
                        # Gestisci path relativi
                        if '/' in href:
                            season_url = f"{self.base_url}/{href}"
                        else:
                            # Se è nella stessa directory dell'archivio
                            archive_base = '/'.join(archive_url.split('/')[:-1])
                            season_url = f"{archive_base}/{href}"
                    
                    seasons.append({
                        "id": url_match.group(1),
                        "country": country_id,
                        "season": season_label,
                        "url": season_url,
                        "title": text
                    })
            
            # Ordina per stagione (dalla più recente)
            seasons.sort(key=lambda x: x["season"], reverse=True)
            
            return seasons
            
        except Exception as e:
            logger.error(f"Errore nel recupero delle stagioni per {country_id}: {e}")
            return []
    
    @cached(ttl=86400)  # Cache di 1 giorno
    def get_league_table(self, season_url: str) -> Dict[str, Any]:
        """
        Ottiene la classifica di un campionato da una pagina RSSSF.
        
        Args:
            season_url: URL della pagina della stagione
            
        Returns:
            Dati della classifica e altre informazioni
        """
        try:
            # Ottieni la pagina della stagione
            response = make_request(season_url)
            if not response:
                logger.error(f"Impossibile accedere alla pagina della stagione {season_url}")
                return {}
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Estrai titolo
            title = soup.find('title')
            title_text = title.get_text().strip() if title else "Unknown Season"
            
            # Estrai il contenuto principale
            content = soup.find('pre')
            if not content:
                content = soup.find('body')
            
            content_text = content.get_text() if content else ""
            
            # Inizia l'analisi della classifica
            # Nota: il formato RSSSF varia considerevolmente, quindi questa è un'implementazione base
            # che dovrà essere adattata per casi specifici
            standings = self._extract_standings_table(content_text)
            
            # Estrai informazioni sul campionato
            season_info = self._extract_season_info(content_text, title_text)
            
            # Estrai i risultati (se presenti)
            results = self._extract_results(content_text)
            
            return {
                "title": title_text,
                "url": season_url,
                "season_info": season_info,
                "standings": standings,
                "results": results,
                "source": "rsssf",
                "last_updated": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Errore nell'estrazione della classifica da {season_url}: {e}")
            return {}
    
    def _extract_standings_table(self, content: str) -> List[Dict[str, Any]]:
        """
        Estrae la classifica dal contenuto testuale.
        
        Args:
            content: Testo della pagina
            
        Returns:
            Classifica estratta
        """
        standings = []
        try:
            # Cerca sezioni che potrebbero contenere la classifica
            # Pattern tipici includono "FINAL TABLE", "STANDINGS", ecc.
            table_markers = ["FINAL TABLE", "FINAL STANDINGS", "TABLE", "STANDINGS", "CLASSEMENT"]
            
            # Trova l'inizio della tabella
            table_start = -1
            for marker in table_markers:
                pos = content.find(marker)
                if pos >= 0:
                    table_start = pos
                    break
            
            if table_start < 0:
                logger.warning("Nessuna classifica trovata nel contenuto")
                return []
            
            # Estrai la sezione che potrebbe contenere la classifica
            # Prendiamo fino a 50 linee dopo il marker
            table_section = content[table_start:table_start + 5000]
            lines = table_section.split('\n')
            
            # Pattern per righe di classifica, es: "1.Team Name    34  20  10   4  60-30  70"
            team_pattern = r'(\d+)[.\)\s]+([A-Za-z0-9\s\-\']+)(?:\s+)(\d+)(?:\s+)(\d+)(?:\s+)(\d+)(?:\s+)(\d+)'
            
            # Analizza le righe fino a 30 righe dopo il marker o fino a un altro marker
            max_lines = min(30, len(lines))
            for i in range(max_lines):
                line = lines[i].strip()
                
                # Salta linee vuote e intestazioni
                if not line or any(m in line for m in table_markers):
                    continue
                
                # Cerca match con il pattern
                match = re.search(team_pattern, line)
                if match:
                    position = int(match.group(1))
                    team_name = match.group(2).strip()
                    played = int(match.group(3))
                    won = int(match.group(4))
                    drawn = int(match.group(5))
                    lost = int(match.group(6))
                    
                    # Cerca di estrarre goal e punti
                    goals_for = 0
                    goals_against = 0
                    points = 0
                    
                    # Cerca pattern di goal, es: "60-30"
                    goals_pattern = r'(\d+)-(\d+)'
                    goals_match = re.search(goals_pattern, line)
                    if goals_match:
                        goals_for = int(goals_match.group(1))
                        goals_against = int(goals_match.group(2))
                    
                    # Cerca punti (solitamente l'ultimo numero nella riga)
                    points_pattern = r'(\d+)$'
                    points_match = re.search(points_pattern, line)
                    if points_match:
                        points = int(points_match.group(1))
                    
                    standings.append({
                        "position": position,
                        "team": team_name,
                        "played": played,
                        "won": won,
                        "drawn": drawn,
                        "lost": lost,
                        "goals_for": goals_for,
                        "goals_against": goals_against,
                        "goal_difference": goals_for - goals_against,
                        "points": points
                    })
            
            return standings
            
        except Exception as e:
            logger.error(f"Errore nell'estrazione della classifica: {e}")
            return []
    
    def _extract_season_info(self, content: str, title: str) -> Dict[str, Any]:
        """
        Estrae informazioni sulla stagione dal contenuto.
        
        Args:
            content: Testo della pagina
            title: Titolo della pagina
            
        Returns:
            Informazioni sulla stagione
        """
        info = {
            "title": title,
            "country": "",
            "league": "",
            "season": "",
            "champion": ""
        }
        
        try:
            # Estrai paese e campionato dal titolo
            country_leagues = {
                "England": ["Premier League", "First Division", "Championship"],
                "Italy": ["Serie A", "Calcio"],
                "Spain": ["La Liga", "Primera Division"],
                "Germany": ["Bundesliga"],
                "France": ["Ligue 1", "Division 1"],
                "Netherlands": ["Eredivisie"],
                "Portugal": ["Primeira Liga", "Primeira Divisao"],
                "Brazil": ["Campeonato Brasileiro", "Serie A"],
                "Argentina": ["Primera Division"]
            }
            
            for country, leagues in country_leagues.items():
                if country in title:
                    info["country"] = country
                    for league in leagues:
                        if league in title:
                            info["league"] = league
                            break
                    break
            
            # Estrai stagione dal titolo
            season_pattern = r'(\d{4})(?:[/-](\d{2,4}))?'
            match = re.search(season_pattern, title)
            if match:
                start_year = match.group(1)
                end_year = match.group(2)
                
                if end_year:
                    # Normalizza l'anno finale
                    if len(end_year) == 2:
                        prefix = start_year[:2]
                        end_year = prefix + end_year
                    info["season"] = f"{start_year}/{end_year}"
                else:
                    info["season"] = start_year
            
            # Cerca il campione
            champion_markers = ["Champion", "Winner", "Champions", "1st"]
            for marker in champion_markers:
                pattern = rf'{marker}[:\s]+([A-Za-z0-9\s\-\']+)'
                match = re.search(pattern, content, re.IGNORECASE)
                if match:
                    info["champion"] = match.group(1).strip()
                    break
            
            return info
            
        except Exception as e:
            logger.error(f"Errore nell'estrazione delle informazioni sulla stagione: {e}")
            return info
    
    def _extract_results(self, content: str) -> List[Dict[str, Any]]:
        """
        Estrae i risultati delle partite dal contenuto.
        
        Args:
            content: Testo della pagina
            
        Returns:
            Lista dei risultati delle partite
        """
        results = []
        try:
            # Cerca sezioni che potrebbero contenere risultati
            result_markers = ["RESULTS", "MATCHES", "GAMES", "ROUND", "WEEK"]
            
            # Trova l'inizio della sezione dei risultati
            result_start = -1
            for marker in result_markers:
                pos = content.find(marker)
                if pos >= 0:
                    result_start = pos
                    break
            
            if result_start < 0:
                logger.warning("Nessuna sezione di risultati trovata nel contenuto")
                return []
            
            # Estrai la sezione che potrebbe contenere i risultati
            result_section = content[result_start:result_start + 10000]  # Limita a 10000 caratteri
            lines = result_section.split('\n')
            
            # Pattern per risultati, es: "TeamA - TeamB  3-1"  o  "TeamA  3-1  TeamB"
            result_patterns = [
                r'([A-Za-z0-9\s\-\']+)\s+(\d+)\s*[-:]\s*(\d+)\s+([A-Za-z0-9\s\-\']+)',  # TeamA  3-1  TeamB
                r'([A-Za-z0-9\s\-\']+)\s+[-:]\s+([A-Za-z0-9\s\-\']+)\s+(\d+)\s*[-:]\s*(\d+)'  # TeamA - TeamB  3-1
            ]
            
            current_round = None
            current_date = None
            
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                
                # Cerca intestazioni di round/giornata
                round_pattern = r'(Round|Week|Matchday|Giornata|Journée)\s+(\d+)'
                round_match = re.search(round_pattern, line, re.IGNORECASE)
                if round_match:
                    current_round = round_match.group(2)
                    continue
                
                # Cerca date (formati vari)
                date_patterns = [
                    r'(\d{1,2})[./](\d{1,2})[./](\d{2,4})',  # 01/01/2000 o 1.1.2000
                    r'(\d{1,2})(?:th|st|nd|rd)?\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})'  # 1st January 2000
                ]
                
                date_found = False
                for pattern in date_patterns:
                    date_match = re.search(pattern, line, re.IGNORECASE)
                    if date_match:
                        # Formato semplificato per ora
                        current_date = line
                        date_found = True
                        break
                
                if date_found:
                    continue
                
                # Cerca risultati
                for pattern in result_patterns:
                    match = re.search(pattern, line)
                    if match:
                        if len(match.groups()) == 4:
                            # Pattern: TeamA  3-1  TeamB
                            if match.group(2).isdigit() and match.group(3).isdigit():
                                home_team = match.group(1).strip()
                                away_team = match.group(4).strip()
                                home_score = int(match.group(2))
                                away_score = int(match.group(3))
                            # Pattern: TeamA - TeamB  3-1
                            else:
                                home_team = match.group(1).strip()
                                away_team = match.group(2).strip()
                                home_score = int(match.group(3))
                                away_score = int(match.group(4))
                                
                            results.append({
                                "home_team": home_team,
                                "away_team": away_team,
                                "home_score": home_score,
                                "away_score": away_score,
                                "round": current_round,
                                "date": current_date
                            })
                            break
            
            return results
            
        except Exception as e:
            logger.error(f"Errore nell'estrazione dei risultati: {e}")
            return []
    
    def update_firebase_data(self, country_id: str) -> Dict[str, Any]:
        """
        Aggiorna i dati su Firebase per un paese specifico.
        
        Args:
            country_id: ID del paese
            
        Returns:
            Risultato dell'operazione
        """
        logger.info(f"Aggiornamento dati RSSSF su Firebase per {country_id}")
        
        result = {
            "seasons": {"success": 0, "error": None},
            "standings": {"success": 0, "error": None}
        }
        
        try:
            # Ottieni le stagioni disponibili
            seasons = self.get_seasons_for_country(country_id)
            if not seasons:
                logger.warning(f"Nessuna stagione trovata per {country_id}")
                result["seasons"]["error"] = "Nessuna stagione trovata"
                return result
            
            # Salva le stagioni su Firebase
            seasons_ref = self.db.get_reference(f"open_data/rsssf/{country_id}/seasons")
            seasons_ref.set({i: season for i, season in enumerate(seasons)})
            result["seasons"]["success"] = len(seasons)
            
            # Limita il numero di stagioni da elaborare (per evitare troppe richieste)
            max_seasons = get_setting('open_data.rsssf.max_seasons', 3)
            seasons_to_process = seasons[:max_seasons]
            
            # Ottieni e salva le classifiche per ogni stagione
            for i, season in enumerate(seasons_to_process):
                try:
                    logger.info(f"Elaborazione stagione {season['season']} per {country_id}")
                    
                    # Ottieni la classifica
                    league_data = self.get_league_table(season['url'])
                    if not league_data:
                        logger.warning(f"Nessun dato trovato per stagione {season['season']}")
                        continue
                    
                    # Salva su Firebase
                    season_key = season['id']
                    league_ref = self.db.get_reference(f"open_data/rsssf/{country_id}/league_data/{season_key}")
                    league_ref.set(league_data)
                    
                    result["standings"]["success"] += 1
                    
                    # Pausa per evitare troppe richieste
                    time.sleep(3)
                    
                except Exception as e:
                    logger.error(f"Errore nell'elaborazione della stagione {season['season']}: {e}")
            
            # Aggiorna il timestamp
            meta_ref = self.db.get_reference(f"open_data/rsssf/{country_id}/meta")
            meta_ref.set({
                "last_update": datetime.now().isoformat(),
                "source": "rsssf",
                "country_id": country_id
            })
            
            logger.info(f"Dati RSSSF aggiornati con successo: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Errore nell'aggiornamento dei dati RSSSF: {e}")
            for key in result:
                if result[key]["error"] is None:
                    result[key]["error"] = str(e)
            return result
    
    def update_all_countries(self) -> Dict[str, Any]:
        """
        Aggiorna i dati per tutti i paesi disponibili.
        
        Returns:
            Risultato dell'operazione per paese
        """
        results = {}
        
        for country_id in self.country_indices:
            logger.info(f"Aggiornamento dati per {country_id}")
            try:
                results[country_id] = self.update_firebase_data(country_id)
                # Pausa tra i paesi per evitare sovraccarico
                time.sleep(5)
            except Exception as e:
                logger.error(f"Errore nell'aggiornamento di {country_id}: {e}")
                results[country_id] = {"error": str(e)}
        
        return results

# Funzioni di utilità globali
def get_rsssf_scraper() -> RSSFFScraper:
    """
    Ottiene un'istanza dello scraper RSSSF.
    
    Returns:
        Istanza di RSSFFScraper
    """
    return RSSFFScraper()

def get_seasons_for_country(country_id: str) -> List[Dict[str, Any]]:
    """
    Ottiene le stagioni disponibili per un paese.
    
    Args:
        country_id: ID del paese
        
    Returns:
        Lista delle stagioni disponibili
    """
    scraper = get_rsssf_scraper()
    return scraper.get_seasons_for_country(country_id)

def get_league_table(season_url: str) -> Dict[str, Any]:
    """
    Ottiene la classifica per una stagione specifica.
    
    Args:
        season_url: URL della pagina della stagione
        
    Returns:
        Dati della classifica
    """
    scraper = get_rsssf_scraper()
    return scraper.get_league_table(season_url)

def update_country_data(country_id: str) -> Dict[str, Any]:
    """
    Aggiorna i dati per un paese specifico.
    
    Args:
        country_id: ID del paese
        
    Returns:
        Risultato dell'operazione
    """
    scraper = get_rsssf_scraper()
    return scraper.update_firebase_data(country_id)

def update_all_countries() -> Dict[str, Any]:
    """
    Aggiorna i dati per tutti i paesi disponibili.
    
    Returns:
        Risultato dell'operazione
    """
    scraper = get_rsssf_scraper()
    return scraper.update_all_countries()
