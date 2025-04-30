"""
Modulo per l'acquisizione di dati da SofaScore.
SofaScore (https://www.sofascore.com) offre statistiche dettagliate per partite, squadre e giocatori,
inclusi dati in tempo reale e storici.
"""
import logging
import re
import json
import time
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from typing import Dict, List, Any, Optional, Union, Tuple

from ..scrapers.base_scraper import BaseScraper

logger = logging.getLogger(__name__)

class SofaScoreScraper(BaseScraper):
    """
    Scraper per dati da SofaScore.
    Fornisce accesso a statistiche dettagliate per partite, squadre e giocatori.
    """
    
    def __init__(self):
        """Inizializza lo scraper SofaScore."""
        super().__init__(
            name="SofaScore",
            base_url="https://www.sofascore.com",
            cache_ttl=3*3600,  # 3 ore di cache
            respect_robots=True,
            delay_range=(3.0, 6.0)  # Più lento per evitare blocchi
        )
        # Mapping tra ID sport e nome
        self.sport_mapping = {
            "football": 1,
            "basketball": 2,
            "tennis": 5,
            "hockey": 4,
            "baseball": 3
        }
        
        # Aggiungi header specifici per SofaScore
        self.session.headers.update({
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Pragma": "no-cache",
            "Cache-Control": "no-cache"
        })
    
    def get_matches_by_date(self, date_str: str = None, sport: str = "football") -> Optional[Dict[str, Any]]:
        """
        Ottiene le partite in programma per una data specifica.
        
        Args:
            date_str: Data nel formato YYYY-MM-DD (default: oggi)
            sport: Sport da cercare (default: football)
            
        Returns:
            Dizionario con partite o None se errore
        """
        # Usa la data odierna se non specificata
        if not date_str:
            date_str = datetime.now().strftime("%Y-%m-%d")
        
        # Verifica che lo sport sia valido
        sport_id = self.sport_mapping.get(sport, 1)
        
        # Costruisci URL
        url = f"{self.base_url}/football//{date_str}"
        html = self.get(url)
        
        if not html:
            self.logger.error(f"Impossibile ottenere partite per {date_str}")
            return None
        
        try:
            soup = self.parse(html)
            if not soup:
                return None
            
            # Estrai script con dati partite
            matches_data = self._extract_json_from_html(html, "window.__INITIAL_STATE__")
            
            if not matches_data:
                self.logger.error("Nessun dato partite trovato nella pagina")
                # Fallback a parsing HTML
                return self._extract_matches_from_html(soup, date_str)
            
            # Estrai dati dalle partite
            result = {
                "date": date_str,
                "sport": sport,
                "source": "sofascore",
                "last_updated": datetime.now().isoformat(),
                "matches": self._extract_matches_from_data(matches_data)
            }
            
            return result
            
        except Exception as e:
            self.logger.error(f"Errore nell'estrazione partite: {str(e)}")
            return None
    
    def get_match_stats(self, match_id: str) -> Optional[Dict[str, Any]]:
        """
        Ottiene statistiche dettagliate per una partita.
        
        Args:
            match_id: ID partita SofaScore
            
        Returns:
            Dizionario con statistiche o None se errore
        """
        url = f"{self.base_url}/event/{match_id}"
        html = self.get(url)
        
        if not html:
            self.logger.error(f"Impossibile ottenere statistiche per partita {match_id}")
            return None
        
        try:
            soup = self.parse(html)
            if not soup:
                return None
            
            # Estrai script con dati partita
            match_data = self._extract_json_from_html(html, "window.__INITIAL_STATE__")
            
            if not match_data:
                self.logger.error("Nessun dato trovato nella pagina")
                # Fallback a parsing HTML
                return self._extract_match_stats_from_html(soup, match_id)
            
            # Verifica dati match
            event_data = self._extract_event_data(match_data)
            if not event_data:
                self.logger.error("Nessun dato evento trovato")
                return None
            
            # Estrai statistiche
            stats = {
                "match_id": match_id,
                "source": "sofascore",
                "last_updated": datetime.now().isoformat(),
                "info": self._extract_match_info(event_data),
                "statistics": self._extract_statistics(event_data),
                "lineups": self._extract_lineups(event_data),
                "incidents": self._extract_incidents(event_data)
            }
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Errore nell'estrazione statistiche partita: {str(e)}")
            return None
    
    def get_team_stats(self, team_id: str) -> Optional[Dict[str, Any]]:
        """
        Ottiene statistiche dettagliate per una squadra.
        
        Args:
            team_id: ID squadra SofaScore
            
        Returns:
            Dizionario con statistiche o None se errore
        """
        url = f"{self.base_url}/team/{team_id}"
        html = self.get(url)
        
        if not html:
            self.logger.error(f"Impossibile ottenere statistiche per squadra {team_id}")
            return None
        
        try:
            soup = self.parse(html)
            if not soup:
                return None
            
            # Estrai script con dati squadra
            team_data = self._extract_json_from_html(html, "window.__INITIAL_STATE__")
            
            if not team_data:
                self.logger.error("Nessun dato trovato nella pagina")
                # Fallback a parsing HTML
                return self._extract_team_stats_from_html(soup, team_id)
            
            # Estrai dati squadra
            team_info = self._extract_team_info(team_data)
            if not team_info:
                self.logger.error("Nessun dato squadra trovato")
                return None
            
            # Ottieni ultime partite
            last_matches = self._extract_team_matches(team_data)
            
            # Ottieni statistiche stagione
            season_stats = self._extract_team_season_stats(team_data)
            
            # Crea risultato
            stats = {
                "team_id": team_id,
                "source": "sofascore",
                "last_updated": datetime.now().isoformat(),
                "info": team_info,
                "last_matches": last_matches,
                "season_stats": season_stats
            }
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Errore nell'estrazione statistiche squadra: {str(e)}")
            return None
    
    def get_player_stats(self, player_id: str) -> Optional[Dict[str, Any]]:
        """
        Ottiene statistiche dettagliate per un giocatore.
        
        Args:
            player_id: ID giocatore SofaScore
            
        Returns:
            Dizionario con statistiche o None se errore
        """
        url = f"{self.base_url}/player/{player_id}"
        html = self.get(url)
        
        if not html:
            self.logger.error(f"Impossibile ottenere statistiche per giocatore {player_id}")
            return None
        
        try:
            soup = self.parse(html)
            if not soup:
                return None
            
            # Estrai script con dati giocatore
            player_data = self._extract_json_from_html(html, "window.__INITIAL_STATE__")
            
            if not player_data:
                self.logger.error("Nessun dato trovato nella pagina")
                # Fallback a parsing HTML
                return self._extract_player_stats_from_html(soup, player_id)
            
            # Estrai dati giocatore
            player_info = self._extract_player_info(player_data)
            if not player_info:
                self.logger.error("Nessun dato giocatore trovato")
                return None
            
            # Ottieni ultime partite
            last_matches = self._extract_player_matches(player_data)
            
            # Ottieni statistiche stagione
            season_stats = self._extract_player_season_stats(player_data)
            
            # Crea risultato
            stats = {
                "player_id": player_id,
                "source": "sofascore",
                "last_updated": datetime.now().isoformat(),
                "info": player_info,
                "last_matches": last_matches,
                "season_stats": season_stats
            }
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Errore nell'estrazione statistiche giocatore: {str(e)}")
            return None
    
    def search_team(self, team_name: str) -> List[Dict[str, Any]]:
        """
        Cerca una squadra per nome.
        
        Args:
            team_name: Nome squadra da cercare
            
        Returns:
            Lista di risultati (id e nome)
        """
        url = f"{self.base_url}/search"
        params = {
            "q": team_name
        }
        
        html = self.get(url, params=params)
        if not html:
            return []
        
        try:
            soup = self.parse(html)
            if not soup:
                return []
                
            results = []
            
            # Estrai risultati ricerca
            team_sections = soup.select('div.SearchTeam')
            
            for section in team_sections:
                team_links = section.select('div[itemtype="http://schema.org/SportsTeam"] a')
                
                for link in team_links:
                    # Estrai nome dal titolo
                    title_elem = link.select_one('div[title]')
                    if not title_elem:
                        continue
                        
                    team_name = title_elem.get('title', '').strip()
                    href = link.get('href', '')
                    
                    # Estrai ID dalla URL
                    team_id_match = re.search(r'/team/(\d+)/', href)
                    if team_id_match:
                        team_id = team_id_match.group(1)
                        
                        # Estrai info lega/nazione dalla label
                        meta_elem = link.select_one('div.Cell-label')
                        meta = meta_elem.text.strip() if meta_elem else ""
                        
                        results.append({
                            "id": team_id,
                            "name": team_name,
                            "meta": meta
                        })
            
            return results
            
        except Exception as e:
            self.logger.error(f"Errore nella ricerca squadra: {str(e)}")
            return []
    
    def search_player(self, player_name: str) -> List[Dict[str, Any]]:
        """
        Cerca un giocatore per nome.
        
        Args:
            player_name: Nome giocatore da cercare
            
        Returns:
            Lista di risultati (id e nome)
        """
        url = f"{self.base_url}/search"
        params = {
            "q": player_name
        }
        
        html = self.get(url, params=params)
        if not html:
            return []
        
        try:
            soup = self.parse(html)
            if not soup:
                return []
                
            results = []
            
            # Estrai risultati ricerca
            player_sections = soup.select('div.SearchPlayer')
            
            for section in player_sections:
                player_links = section.select('div[itemtype="http://schema.org/Person"] a')
                
                for link in player_links:
                    # Estrai nome dal titolo
                    title_elem = link.select_one('div[title]')
                    if not title_elem:
                        continue
                        
                    player_name = title_elem.get('title', '').strip()
                    href = link.get('href', '')
                    
                    # Estrai ID dalla URL
                    player_id_match = re.search(r'/player/(\d+)/', href)
                    if player_id_match:
                        player_id = player_id_match.group(1)
                        
                        # Estrai info squadra dalla label
                        meta_elem = link.select_one('div.Cell-label')
                        meta = meta_elem.text.strip() if meta_elem else ""
                        
                        results.append({
                            "id": player_id,
                            "name": player_name,
                            "team": meta
                        })
            
            return results
            
        except Exception as e:
            self.logger.error(f"Errore nella ricerca giocatore: {str(e)}")
            return []
    
    # Metodi privati per estrazione dati
    
    def _extract_json_from_html(self, html: str, variable_name: str) -> Any:
        """Estrae dati JSON da script nella pagina."""
        try:
            pattern = re.compile(f"{variable_name} = (.*?)(?:;|</script>)", re.DOTALL)
            match = pattern.search(html)
            
            if match:
                json_str = match.group(1).strip()
                return json.loads(json_str)
            else:
                return None
        except Exception as e:
            self.logger.warning(f"Errore nell'estrazione JSON: {str(e)}")
            return None
    
    def _extract_matches_from_data(self, data: Dict) -> List[Dict[str, Any]]:
        """Estrae dati partite dal JSON di SofaScore."""
        matches = []
        
        try:
            # I dati partite possono essere in diverse strutture, proviamo i percorsi comuni
            events = None
            
            # Tenta prima la struttura dei dati del giorno
            if "dateState" in data and "events" in data["dateState"]:
                events = data["dateState"]["events"]
            
            # Tenta la struttura della pagina sport
            elif "eventState" in data and "events" in data["eventState"]:
                events = data["eventState"]["events"]
            
            # Tenta la struttura di ricerca
            elif "searchState" in data and "events" in data["searchState"]:
                events = data["searchState"]["events"]
            
            if not events:
                return []
            
            # Iteriamo sugli eventi
            for event_id, event in events.items():
                try:
                    if not isinstance(event, dict):
                        continue
                    
                    # Estrai dati base partita
                    tournament = event.get("tournament", {})
                    home_team = event.get("homeTeam", {})
                    away_team = event.get("awayTeam", {})
                    
                    match_info = {
                        "id": event_id,
                        "status": event.get("status", {}).get("type", "notstarted"),
                        "start_time": event.get("startTimestamp", 0),
                        "tournament": {
                            "id": tournament.get("id"),
                            "name": tournament.get("name"),
                            "category": tournament.get("category", {}).get("name")
                        },
                        "home_team": {
                            "id": home_team.get("id"),
                            "name": home_team.get("name"),
                            "short_name": home_team.get("shortName")
                        },
                        "away_team": {
                            "id": away_team.get("id"),
                            "name": away_team.get("name"),
                            "short_name": away_team.get("shortName")
                        }
                    }
                    
                    # Aggiungi il punteggio se disponibile
                    if "homeScore" in event and "awayScore" in event:
                        match_info["score"] = {
                            "home": event.get("homeScore", {}).get("display", 0),
                            "away": event.get("awayScore", {}).get("display", 0)
                        }
                        
                        # Aggiungi half-time score se disponibile
                        if "period" in event.get("homeScore", {}) and "period" in event.get("awayScore", {}):
                            ht_home = 0
                            ht_away = 0
                            for period, score in event["homeScore"]["period"].items():
                                if period == "1":
                                    ht_home = score
                                    break
                            for period, score in event["awayScore"]["period"].items():
                                if period == "1":
                                    ht_away = score
                                    break
                            
                            match_info["ht_score"] = {
                                "home": ht_home,
                                "away": ht_away
                            }
                    
                    # Aggiungi data/ora formattata
                    if "start_time" in match_info and match_info["start_time"]:
                        try:
                            dt = datetime.fromtimestamp(match_info["start_time"])
                            match_info["start_time_iso"] = dt.isoformat()
                            match_info["start_date"] = dt.strftime("%Y-%m-%d")
                            match_info["start_time_str"] = dt.strftime("%H:%M")
                        except:
                            pass
                    
                    matches.append(match_info)
                except Exception as e:
                    self.logger.warning(f"Errore nell'estrazione dati partita {event_id}: {str(e)}")
                    continue
            
            # Ordina per orario di inizio
            matches.sort(key=lambda x: x.get("start_time", 0))
            
        except Exception as e:
            self.logger.error(f"Errore nell'estrazione dati partite: {str(e)}")
        
        return matches
    
    def _extract_matches_from_html(self, soup: BeautifulSoup, date_str: str) -> Dict[str, Any]:
        """Estrae partite dal HTML (fallback)."""
        result = {
            "date": date_str,
            "source": "sofascore",
            "last_updated": datetime.now().isoformat(),
            "matches": []
        }
        
        try:
            # SofaScore mostra le partite in gruppi per campionato
            tournament_blocks = soup.select('div.sc-fqkvVR')
            
            for block in tournament_blocks:
                # Ottieni nome torneo
                tournament_name = ""
                tournament_country = ""
                
                # Trova l'header del torneo
                tournament_header = block.select_one('div.sc-dcJsrY')
                if tournament_header:
                    name_elem = tournament_header.select_one('span')
                    if name_elem:
                        tournament_name = name_elem.text.strip()
                    
                    # Cerca l'elemento con la bandiera/paese
                    country_elem = tournament_header.select_one('span[title]')
                    if country_elem:
                        tournament_country = country_elem.get('title', '')
                
                # Trova le partite in questo torneo
                match_rows = block.select('div.sc-gEvEer a')
                
                for row in match_rows:
                    try:
                        # Estrai ID partita dalla URL
                        href = row.get('href', '')
                        match_id_match = re.search(r'/event/(\d+)/', href)
                        if not match_id_match:
                            continue
                            
                        match_id = match_id_match.group(1)
                        
                        # Estrai team names
                        team_elems = row.select('div.sc-fqkvVR span')
                        if len(team_elems) < 2:
                            continue
                            
                        home_team = team_elems[0].text.strip()
                        away_team = team_elems[1].text.strip()
                        
                        # Estrai orario
                        time_elem = row.select_one('span.sc-kAyceB')
                        start_time_str = time_elem.text.strip() if time_elem else ""
                        
                        # Estrai punteggio se disponibile
                        score_elems = row.select('span.sc-imWYAI')
                        home_score = away_score = None
                        
                        if len(score_elems) >= 2:
                            home_score = self.to_numeric(score_elems[0].text)
                            away_score = self.to_numeric(score_elems[1].text)
                        
                        # Crea oggetto partita
                        match_info = {
                            "id": match_id,
                            "tournament": {
                                "name": tournament_name,
                                "category": tournament_country
                            },
                            "home_team": {
                                "name": home_team
                            },
                            "away_team": {
                                "name": away_team
                            },
                            "start_date": date_str,
                            "start_time_str": start_time_str
                        }
                        
                        # Aggiungi punteggio se disponibile
                        if home_score is not None and away_score is not None:
                            match_info["score"] = {
                                "home": home_score,
                                "away": away_score
                            }
                        
                        result["matches"].append(match_info)
                        
                    except Exception as e:
                        self.logger.warning(f"Errore nell'estrazione dati partita: {str(e)}")
                        continue
        except Exception as e:
            self.logger.error(f"Errore nell'estrazione partite HTML: {str(e)}")
        
        return result
    
    def _extract_event_data(self, data: Dict) -> Optional[Dict]:
        """Estrae i dati dell'evento dal JSON."""
        try:
            # Tenta diverse strutture comuni
            if "eventState" in data and "event" in data["eventState"]:
                return data["eventState"]["event"]
            elif "matchState" in data and "event" in data["matchState"]:
                return data["matchState"]["event"]
            return None
        except Exception as e:
            self.logger.warning(f"Errore nell'estrazione dati evento: {str(e)}")
            return None
    
    def _extract_match_info(self, event_data: Dict) -> Dict[str, Any]:
        """Estrae le informazioni base di una partita."""
        match_info = {}
        
        try:
            # Torneo
            if "tournament" in event_data:
                tournament = event_data["tournament"]
                match_info["tournament"] = {
                    "id": tournament.get("id"),
                    "name": tournament.get("name"),
                    "category": tournament.get("category", {}).get("name")
                }
            
            # Squadre e punteggi
            if "homeTeam" in event_data and "awayTeam" in event_data:
                match_info["home_team"] = {
                    "id": event_data["homeTeam"].get("id"),
                    "name": event_data["homeTeam"].get("name")
                }
                match_info["away_team"] = {
                    "id": event_data["awayTeam"].get("id"),
                    "name": event_data["awayTeam"].get("name")
                }
            
            # Punteggio
            if "homeScore" in event_data and "awayScore" in event_data:
                match_info["score"] = {
                    "home": event_data["homeScore"].get("display", 0),
                    "away": event_data["awayScore"].get("display", 0)
                }
                
                # Punteggi per periodo
                if "period" in event_data["homeScore"] and "period" in event_data["awayScore"]:
                    match_info["periods"] = {}
                    for period, score in event_data["homeScore"]["period"].items():
                        away_score = event_data["awayScore"]["period"].get(period, 0)
                        match_info["periods"][period] = {
                            "home": score,
                            "away": away_score
                        }
            
            # Data/ora
            if "startTimestamp" in event_data:
                try:
                    dt = datetime.fromtimestamp(event_data["startTimestamp"])
                    match_info["start_time"] = dt.isoformat()
                except:
                    match_info["start_time"] = event_data.get("formattedStartDate", "")
            
            # Luogo
            if "venue" in event_data:
                match_info["venue"] = {
                    "name": event_data["venue"].get("stadium", {}).get("name", ""),
                    "city": event_data["venue"].get("city", {}).get("name", ""),
                    "country": event_data["venue"].get("country", {}).get("name", "")
                }
            
            # Arbitro
            if "referee" in event_data:
                match_info["referee"] = event_data["referee"].get("name", "")
            
            # Stato partita
            if "status" in event_data:
                match_info["status"] = {
                    "type": event_data["status"].get("type", ""),
                    "description": event_data["status"].get("description", "")
                }
                
        except Exception as e:
            self.logger.warning(f"Errore nell'estrazione info partita: {str(e)}")
        
        return match_info
    
    def _extract_statistics(self, event_data: Dict) -> Dict[str, Any]:
        """Estrae le statistiche di una partita."""
        stats = {
            "home": {},
            "away": {}
        }
        
        try:
            # Cerca statistiche in strutture diverse
            statistics = None
            
            if "statistics" in event_data:
                statistics = event_data["statistics"]
            elif "statisticsState" in event_data and "statistics" in event_data["statisticsState"]:
                statistics = event_data["statisticsState"]["statistics"]
                
            if not statistics:
                return stats
            
            # Estrai statistiche per casa/ospiti
            if "home" in statistics:
                for group in statistics["home"].get("groups", []):
                    group_name = group.get("groupName", "").lower().replace(" ", "_")
                    group_stats = {}
                    
                    for stat in group.get("statisticsItems", []):
                        stat_name = stat.get("name", "").lower().replace(" ", "_")
                        stat_value = stat.get("home", 0)
                        group_stats[stat_name] = stat_value
                    
                    stats["home"][group_name] = group_stats
            
            if "away" in statistics:
                for group in statistics["away"].get("groups", []):
                    group_name = group.get("groupName", "").lower().replace(" ", "_")
                    group_stats = {}
                    
                    for stat in group.get("statisticsItems", []):
                        stat_name = stat.get("name", "").lower().replace(" ", "_")
                        stat_value = stat.get("away", 0)
                        group_stats[stat_name] = stat_value
                    
                    stats["away"][group_name] = group_stats
                    
        except Exception as e:
            self.logger.warning(f"Errore nell'estrazione statistiche: {str(e)}")
        
        return stats
    
    def _extract_lineups(self, event_data: Dict) -> Dict[str, Any]:
        """Estrae le formazioni di una partita."""
        lineups = {
            "home": {
                "formation": "",
                "players": [],
                "substitutes": []
            },
            "away": {
                "formation": "",
                "players": [],
                "substitutes": []
            }
        }
        
        try:
            # Cerca formazioni in strutture diverse
            if "lineups" in event_data:
                lineup_data = event_data["lineups"]
            elif "lineupsState" in event_data and "lineups" in event_data["lineupsState"]:
                lineup_data = event_data["lineupsState"]["lineups"]
            else:
                return lineups
            
            # Formazione Casa
            if "home" in lineup_data:
                home = lineup_data["home"]
                lineups["home"]["formation"] = home.get("formation", "")
                
                # Giocatori titolari
                for player in home.get("players", []):
                    player_info = {
                        "id": player.get("player", {}).get("id"),
                        "name": player.get("player", {}).get("name"),
                        "position": player.get("player", {}).get("position", {}).get("name", ""),
                        "shirt_number": player.get("shirtNumber"),
                        "rating": player.get("statistics", {}).get("rating"),
                        "minutes_played": player.get("statistics", {}).get("minutesPlayed")
                    }
                    lineups["home"]["substitutes"].append(player_info)
            
            # Formazione Trasferta
            if "away" in lineup_data:
                away = lineup_data["away"]
                lineups["away"]["formation"] = away.get("formation", "")
                
                # Giocatori titolari
                for player in away.get("players", []):
                    player_info = {
                        "id": player.get("player", {}).get("id"),
                        "name": player.get("player", {}).get("name"),
                        "position": player.get("player", {}).get("position", {}).get("name", ""),
                        "shirt_number": player.get("shirtNumber"),
                        "rating": player.get("statistics", {}).get("rating"),
                        "minutes_played": player.get("statistics", {}).get("minutesPlayed")
                    }
                    lineups["away"]["players"].append(player_info)
                
                # Sostituti
                for player in away.get("substitutions", []):
                    player_info = {
                        "id": player.get("player", {}).get("id"),
                        "name": player.get("player", {}).get("name"),
                        "position": player.get("player", {}).get("position", {}).get("name", ""),
                        "shirt_number": player.get("shirtNumber"),
                        "rating": player.get("statistics", {}).get("rating"),
                        "minutes_played": player.get("statistics", {}).get("minutesPlayed")
                    }
                    lineups["away"]["substitutes"].append(player_info)
                    
        except Exception as e:
            self.logger.warning(f"Errore nell'estrazione formazioni: {str(e)}")
        
        return lineups
    
    def _extract_incidents(self, event_data: Dict) -> List[Dict[str, Any]]:
        """Estrae gli eventi chiave di una partita (goal, cartellini, etc.)."""
        incidents = []
        
        try:
            # Cerca eventi in strutture diverse
            if "incidents" in event_data:
                incidents_data = event_data["incidents"]
            elif "incidentsState" in event_data and "incidents" in event_data["incidentsState"]:
                incidents_data = event_data["incidentsState"]["incidents"]
            else:
                return incidents
            
            # Processa ogni incidente
            for incident in incidents_data:
                try:
                    incident_info = {
                        "id": incident.get("id"),
                        "time": incident.get("time"),
                        "type": incident.get("incidentType"),
                        "is_home": incident.get("isHome", False)
                    }
                    
                    # Aggiungi dati specifici per tipo
                    if incident["incidentType"] == "goal":
                        incident_info["scorer"] = {
                            "id": incident.get("player", {}).get("id"),
                            "name": incident.get("player", {}).get("name")
                        }
                        incident_info["assist"] = {
                            "id": incident.get("assist", {}).get("id"),
                            "name": incident.get("assist", {}).get("name")
                        } if incident.get("assist") else None
                        incident_info["goal_type"] = incident.get("incidentClass")
                        incident_info["score"] = {
                            "home": incident.get("homeScore"),
                            "away": incident.get("awayScore")
                        }
                    elif incident["incidentType"] in ["card", "yellowCard", "redCard"]:
                        incident_info["card_type"] = incident.get("incidentClass", "").lower()
                        incident_info["player"] = {
                            "id": incident.get("player", {}).get("id"),
                            "name": incident.get("player", {}).get("name")
                        }
                        incident_info["reason"] = incident.get("reason")
                    elif incident["incidentType"] == "substitution":
                        incident_info["player_in"] = {
                            "id": incident.get("playerIn", {}).get("id"),
                            "name": incident.get("playerIn", {}).get("name")
                        }
                        incident_info["player_out"] = {
                            "id": incident.get("playerOut", {}).get("id"),
                            "name": incident.get("playerOut", {}).get("name")
                        }
                    
                    incidents.append(incident_info)
                except Exception as e:
                    self.logger.warning(f"Errore nell'estrazione incidente: {str(e)}")
                    continue
                    
        except Exception as e:
            self.logger.warning(f"Errore nell'estrazione incidenti: {str(e)}")
        
        # Ordina per tempo
        incidents.sort(key=lambda x: x.get("time", 0))
        return incidents
    
    def _extract_match_stats_from_html(self, soup: BeautifulSoup, match_id: str) -> Dict[str, Any]:
        """Estrae statistiche partita dal HTML (fallback)."""
        stats = {
            "match_id": match_id,
            "source": "sofascore",
            "last_updated": datetime.now().isoformat(),
            "info": {},
            "statistics": {
                "home": {},
                "away": {}
            },
            "lineups": {
                "home": {
                    "formation": "",
                    "players": [],
                    "substitutes": []
                },
                "away": {
                    "formation": "",
                    "players": [],
                    "substitutes": []
                }
            },
            "incidents": []
        }
        
        try:
            # Estrai info base partita
            header = soup.select_one('div.sc-fqkvVR')
            if header:
                # Team names
                team_names = header.select('a.sc-dcJsrY')
                if len(team_names) >= 2:
                    stats["info"]["home_team"] = {
                        "name": team_names[0].text.strip()
                    }
                    stats["info"]["away_team"] = {
                        "name": team_names[1].text.strip()
                    }
                
                # Score
                score_elems = header.select('div.sc-imWYAI')
                if len(score_elems) >= 2:
                    stats["info"]["score"] = {
                        "home": self.to_numeric(score_elems[0].text),
                        "away": self.to_numeric(score_elems[1].text)
                    }
            
            # Estrai statistiche
            stats_section = soup.select_one('div[data-testid="wcl-statistics"]')
            if stats_section:
                stat_items = stats_section.select('div.sc-eDPEul')
                
                for item in stat_items:
                    # Estrai nome statistica
                    stat_name_elem = item.select_one('div.sc-eldPxv')
                    if not stat_name_elem:
                        continue
                        
                    stat_name = stat_name_elem.text.strip().lower().replace(" ", "_")
                    
                    # Estrai valori
                    values = item.select('div.sc-fqkvVR')
                    if len(values) >= 2:
                        home_value = self.to_numeric(values[0].text)
                        away_value = self.to_numeric(values[1].text)
                        
                        # Aggiungi a statistics
                        if "attack" not in stats["statistics"]["home"]:
                            stats["statistics"]["home"]["attack"] = {}
                            stats["statistics"]["away"]["attack"] = {}
                            
                        stats["statistics"]["home"]["attack"][stat_name] = home_value
                        stats["statistics"]["away"]["attack"][stat_name] = away_value
                        
        except Exception as e:
            self.logger.error(f"Errore nell'estrazione statistiche HTML: {str(e)}")
            
        return stats
    
    def _extract_team_info(self, team_data: Dict) -> Dict[str, Any]:
        """Estrae le informazioni base di una squadra."""
        team_info = {}
        
        try:
            # Cerca info team nelle diverse strutture
            team = None
            
            if "teamState" in team_data and "team" in team_data["teamState"]:
                team = team_data["teamState"]["team"]
            elif "teamDetailsState" in team_data and "team" in team_data["teamDetailsState"]:
                team = team_data["teamDetailsState"]["team"]
                
            if not team:
                return team_info
            
            # Informazioni base
            team_info["id"] = team.get("id")
            team_info["name"] = team.get("name")
            team_info["short_name"] = team.get("shortName")
            team_info["country"] = team.get("country", {}).get("name")
            
            # Immagini
            if "logos" in team:
                logos = team["logos"]
                if isinstance(logos, list) and len(logos) > 0:
                    team_info["logo"] = logos[0]
                elif isinstance(logos, dict):
                    team_info["logo"] = logos.get("default")
            
            # Dati torneo
            if "tournament" in team:
                tournament = team["tournament"]
                team_info["tournament"] = {
                    "id": tournament.get("id"),
                    "name": tournament.get("name"),
                    "category": tournament.get("category", {}).get("name")
                }
                
                # Posizione in classifica
                if "uniqueTournament" in tournament and "standings" in tournament["uniqueTournament"]:
                    standings = tournament["uniqueTournament"]["standings"]
                    
                    for row in standings:
                        if row.get("id") == team_info["id"]:
                            team_info["standing"] = {
                                "position": row.get("position"),
                                "points": row.get("points"),
                                "played": row.get("played"),
                                "wins": row.get("wins"),
                                "draws": row.get("draws"),
                                "losses": row.get("losses"),
                                "goals_for": row.get("goalsFor"),
                                "goals_against": row.get("goalsAgainst")
                            }
                            break
                            
        except Exception as e:
            self.logger.warning(f"Errore nell'estrazione info squadra: {str(e)}")
        
        return team_info
    
    def _extract_team_matches(self, team_data: Dict) -> List[Dict[str, Any]]:
        """Estrae le ultime partite di una squadra."""
        matches = []
        
        try:
            # Cerca partite nelle diverse strutture
            events = None
            
            if "teamState" in team_data and "lastMatches" in team_data["teamState"]:
                events = team_data["teamState"]["lastMatches"]
            elif "teamDetailsState" in team_data and "events" in team_data["teamDetailsState"]:
                events = team_data["teamDetailsState"]["events"]
                
            if not events:
                return matches
            
            team_id = None
            if "teamState" in team_data and "team" in team_data["teamState"]:
                team_id = team_data["teamState"]["team"].get("id")
            elif "teamDetailsState" in team_data and "team" in team_data["teamDetailsState"]:
                team_id = team_data["teamDetailsState"]["team"].get("id")
            
            # Processa ogni partita
            for match in events:
                try:
                    match_info = {
                        "id": match.get("id"),
                        "home_team": {
                            "id": match.get("homeTeam", {}).get("id"),
                            "name": match.get("homeTeam", {}).get("name")
                        },
                        "away_team": {
                            "id": match.get("awayTeam", {}).get("id"),
                            "name": match.get("awayTeam", {}).get("name")
                        },
                        "score": {
                            "home": match.get("homeScore", {}).get("display", 0),
                            "away": match.get("awayScore", {}).get("display", 0)
                        },
                        "start_time": match.get("startTimestamp", 0),
                        "status": match.get("status", {}).get("type", ""),
                        "tournament": {
                            "id": match.get("tournament", {}).get("id"),
                            "name": match.get("tournament", {}).get("name")
                        }
                    }
                    
                    # Determina se la squadra ha giocato in casa o trasferta
                    if team_id:
                        if match["homeTeam"]["id"] == team_id:
                            match_info["played_at"] = "home"
                        else:
                            match_info["played_at"] = "away"
                            
                        # Determina risultato dal punto di vista della squadra
                        if match_info["score"]["home"] > match_info["score"]["away"]:
                            match_info["result"] = "W" if match_info["played_at"] == "home" else "L"
                        elif match_info["score"]["home"] < match_info["score"]["away"]:
                            match_info["result"] = "L" if match_info["played_at"] == "home" else "W"
                        else:
                            match_info["result"] = "D"
                    
                    # Formatta datetime
                    if "start_time" in match_info and match_info["start_time"]:
                        try:
                            dt = datetime.fromtimestamp(match_info["start_time"])
                            match_info["start_time_iso"] = dt.isoformat()
                        except:
                            pass
                    
                    matches.append(match_info)
                except Exception as e:
                    self.logger.warning(f"Errore nell'estrazione partita squadra: {str(e)}")
                    continue
                    
        except Exception as e:
            self.logger.warning(f"Errore nell'estrazione partite squadra: {str(e)}")
        
        # Ordina per data (più recenti prima)
        matches.sort(key=lambda x: x.get("start_time", 0), reverse=True)
        return matches
    
    def _extract_team_season_stats(self, team_data: Dict) -> Dict[str, Any]:
        """Estrae le statistiche stagionali di una squadra."""
        stats = {}
        
        try:
            # Cerca statistiche nelle diverse strutture
            statistics = None
            
            if "teamState" in team_data and "statistics" in team_data["teamState"]:
                statistics = team_data["teamState"]["statistics"]
            elif "teamDetailsState" in team_data and "statistics" in team_data["teamDetailsState"]:
                statistics = team_data["teamDetailsState"]["statistics"]
                
            if not statistics:
                return stats
            
            # Processa statistiche per categoria
            for category, category_stats in statistics.items():
                stats[category] = {}
                
                # Processa statistiche della categoria
                for stat_name, stat_value in category_stats.items():
                    stats[category][stat_name.lower().replace(" ", "_")] = stat_value
                    
        except Exception as e:
            self.logger.warning(f"Errore nell'estrazione statistiche squadra: {str(e)}")
        
        return stats
    
    def _extract_team_stats_from_html(self, soup: BeautifulSoup, team_id: str) -> Dict[str, Any]:
        """Estrae statistiche squadra dal HTML (fallback)."""
        stats = {
            "team_id": team_id,
            "source": "sofascore",
            "last_updated": datetime.now().isoformat(),
            "info": {},
            "last_matches": [],
            "season_stats": {}
        }
        
        try:
            # Estrai info base squadra
            header = soup.select_one('div.sc-hLBbgP')
            if header:
                # Nome squadra
                name_elem = header.select_one('h2')
                if name_elem:
                    stats["info"]["name"] = name_elem.text.strip()
                
                # Logo squadra
                logo_elem = header.select_one('img')
                if logo_elem:
                    stats["info"]["logo"] = logo_elem.get('src', '')
            
            # Estrai ultime partite
            matches_section = soup.select_one('div[data-testid="wcl-eventlist"]')
            if matches_section:
                match_items = matches_section.select('a')
                
                for item in match_items:
                    try:
                        # Estrai URL per ID partita
                        href = item.get('href', '')
                        match_id_match = re.search(r'/event/(\d+)/', href)
                        if not match_id_match:
                            continue
                            
                        match_id = match_id_match.group(1)
                        
                        # Estrai squadre
                        team_names = item.select('div.sc-fqkvVR span')
                        if len(team_names) < 2:
                            continue
                            
                        home_team = team_names[0].text.strip()
                        away_team = team_names[1].text.strip()
                        
                        # Estrai punteggio
                        score_elems = item.select('span.sc-imWYAI')
                        home_score = away_score = 0
                        
                        if len(score_elems) >= 2:
                            home_score = self.to_numeric(score_elems[0].text)
                            away_score = self.to_numeric(score_elems[1].text)
                        
                        # Crea oggetto partita
                        match_info = {
                            "id": match_id,
                            "home_team": {
                                "name": home_team
                            },
                            "away_team": {
                                "name": away_team
                            },
                            "score": {
                                "home": home_score,
                                "away": away_score
                            }
                        }
                        
                        # Determina se la squadra ha giocato in casa o trasferta
                        if stats["info"].get("name") == home_team:
                            match_info["played_at"] = "home"
                        else:
                            match_info["played_at"] = "away"
                            
                        # Determina risultato dal punto di vista della squadra
                        if home_score > away_score:
                            match_info["result"] = "W" if match_info["played_at"] == "home" else "L"
                        elif home_score < away_score:
                            match_info["result"] = "L" if match_info["played_at"] == "home" else "W"
                        else:
                            match_info["result"] = "D"
                        
                        stats["last_matches"].append(match_info)
                    except Exception as e:
                        self.logger.warning(f"Errore nell'estrazione partita da HTML: {str(e)}")
                        continue
                        
        except Exception as e:
            self.logger.error(f"Errore nell'estrazione dati squadra HTML: {str(e)}")
            
        return stats
    
    def _extract_player_info(self, player_data: Dict) -> Dict[str, Any]:
        """Estrae le informazioni base di un giocatore."""
        player_info = {}
        
        try:
            # Cerca info giocatore nelle diverse strutture
            player = None
            
            if "playerState" in player_data and "player" in player_data["playerState"]:
                player = player_data["playerState"]["player"]
            elif "playerDetailsState" in player_data and "player" in player_data["playerDetailsState"]:
                player = player_data["playerDetailsState"]["player"]
                
            if not player:
                return player_info
            
            # Informazioni base
            player_info["id"] = player.get("id")
            player_info["name"] = player.get("name")
            player_info["position"] = player.get("position", {}).get("name", "")
            player_info["country"] = player.get("country", {}).get("name", "")
            
            # Dati fisici
            if "height" in player:
                player_info["height"] = player["height"]
            if "age" in player:
                player_info["age"] = player["age"]
            
            # Immagini
            if "image" in player:
                player_info["image"] = player["image"]
            
            # Squadra attuale
            if "team" in player:
                player_info["team"] = {
                    "id": player["team"].get("id"),
                    "name": player["team"].get("name"),
                }
                
        except Exception as e:
            self.logger.warning(f"Errore nell'estrazione info giocatore: {str(e)}")
        
        return player_info
    
    def _extract_player_matches(self, player_data: Dict) -> List[Dict[str, Any]]:
        """Estrae le ultime partite di un giocatore."""
        matches = []
        
        try:
            # Cerca partite nelle diverse strutture
            events = None
            
            if "playerState" in player_data and "events" in player_data["playerState"]:
                events = player_data["playerState"]["events"]
            elif "playerDetailsState" in player_data and "events" in player_data["playerDetailsState"]:
                events = player_data["playerDetailsState"]["events"]
                
            if not events:
                return matches
            
            # Processa ogni partita
            for match in events:
                try:
                    match_info = {
                        "id": match.get("id"),
                        "home_team": {
                            "id": match.get("homeTeam", {}).get("id"),
                            "name": match.get("homeTeam", {}).get("name")
                        },
                        "away_team": {
                            "id": match.get("awayTeam", {}).get("id"),
                            "name": match.get("awayTeam", {}).get("name")
                        },
                        "score": {
                            "home": match.get("homeScore", {}).get("display", 0),
                            "away": match.get("awayScore", {}).get("display", 0)
                        },
                        "start_time": match.get("startTimestamp", 0),
                        "status": match.get("status", {}).get("type", ""),
                        "tournament": {
                            "id": match.get("tournament", {}).get("id"),
                            "name": match.get("tournament", {}).get("name")
                        }
                    }
                    
                    # Estrai statistiche del giocatore in questa partita
                    if "playerStatistics" in match:
                        stats = match["playerStatistics"]
                        match_info["statistics"] = {
                            "rating": stats.get("rating"),
                            "minutes_played": stats.get("minutesPlayed"),
                            "goals": stats.get("goals", 0),
                            "assists": stats.get("assists", 0),
                            "yellow_cards": stats.get("yellowCards", 0),
                            "red_cards": stats.get("redCards", 0)
                        }
                    
                    # Formatta datetime
                    if "start_time" in match_info and match_info["start_time"]:
                        try:
                            dt = datetime.fromtimestamp(match_info["start_time"])
                            match_info["start_time_iso"] = dt.isoformat()
                        except:
                            pass
                    
                    matches.append(match_info)
                except Exception as e:
                    self.logger.warning(f"Errore nell'estrazione partita giocatore: {str(e)}")
                    continue
                    
        except Exception as e:
            self.logger.warning(f"Errore nell'estrazione partite giocatore: {str(e)}")
        
        # Ordina per data (più recenti prima)
        matches.sort(key=lambda x: x.get("start_time", 0), reverse=True)
        return matches
    
    def _extract_player_season_stats(self, player_data: Dict) -> Dict[str, Any]:
        """Estrae le statistiche stagionali di un giocatore."""
        stats = {}
        
        try:
            # Cerca statistiche nelle diverse strutture
            statistics = None
            
            if "playerState" in player_data and "statistics" in player_data["playerState"]:
                statistics = player_data["playerState"]["statistics"]
            elif "playerDetailsState" in player_data and "statistics" in player_data["playerDetailsState"]:
                statistics = player_data["playerDetailsState"]["statistics"]
                
            if not statistics:
                return stats
            
            # Processa statistiche per categoria
            for category, category_stats in statistics.items():
                stats[category] = {}
                
                # Processa statistiche della categoria
                for stat_name, stat_value in category_stats.items():
                    stats[category][stat_name.lower().replace(" ", "_")] = stat_value
                    
        except Exception as e:
            self.logger.warning(f"Errore nell'estrazione statistiche giocatore: {str(e)}")
        
        return stats
    
    def _extract_player_stats_from_html(self, soup: BeautifulSoup, player_id: str) -> Dict[str, Any]:
        """Estrae statistiche giocatore dal HTML (fallback)."""
        stats = {
            "player_id": player_id,
            "source": "sofascore",
            "last_updated": datetime.now().isoformat(),
            "info": {},
            "last_matches": [],
            "season_stats": {}
        }
        
        try:
            # Estrai info base giocatore
            header = soup.select_one('div.sc-hLBbgP')
            if header:
                # Nome giocatore
                name_elem = header.select_one('h2')
                if name_elem:
                    stats["info"]["name"] = name_elem.text.strip()
                
                # Immagine giocatore
                img_elem = header.select_one('img')
                if img_elem:
                    stats["info"]["image"] = img_elem.get('src', '')
                
                # Posizione e altre info
                info_elems = header.select('div.sc-ikZpkk')
                if len(info_elems) > 0:
                    position_text = info_elems[0].text.strip()
                    stats["info"]["position"] = position_text
            
            # Estrai ultime partite
            matches_section = soup.select_one('div[data-testid="wcl-eventlist"]')
            if matches_section:
                match_items = matches_section.select('a')
                
                for item in match_items:
                    try:
                        # Estrai URL per ID partita
                        href = item.get('href', '')
                        match_id_match = re.search(r'/event/(\d+)/', href)
                        if not match_id_match:
                            continue
                            
                        match_id = match_id_match.group(1)
                        
                        # Estrai squadre
                        team_names = item.select('div.sc-fqkvVR span')
                        if len(team_names) < 2:
                            continue
                            
                        home_team = team_names[0].text.strip()
                        away_team = team_names[1].text.strip()
                        
                        # Estrai punteggio
                        score_elems = item.select('span.sc-imWYAI')
                        home_score = away_score = 0
                        
                        if len(score_elems) >= 2:
                            home_score = self.to_numeric(score_elems[0].text)
                            away_score = self.to_numeric(score_elems[1].text)
                        
                        # Estrai rating giocatore
                        rating_elem = item.select_one('span.sc-kAyceB')
                        rating = None
                        if rating_elem:
                            rating = self.to_numeric(rating_elem.text)
                        
                        # Crea oggetto partita
                        match_info = {
                            "id": match_id,
                            "home_team": {
                                "name": home_team
                            },
                            "away_team": {
                                "name": away_team
                            },
                            "score": {
                                "home": home_score,
                                "away": away_score
                            },
                            "statistics": {
                                "rating": rating
                            }
                        }
                        
                        stats["last_matches"].append(match_info)
                    except Exception as e:
                        self.logger.warning(f"Errore nell'estrazione partita da HTML: {str(e)}")
                        continue
                        
        except Exception as e:
            self.logger.error(f"Errore nell'estrazione dati giocatore HTML: {str(e)}")
            
        return stats
get("id"),
                        "name": player.get("player", {}).get("name"),
                        "position": player.get("player", {}).get("position", {}).get("name", ""),
                        "shirt_number": player.get("shirtNumber"),
                        "rating": player.get("statistics", {}).get("rating"),
                        "minutes_played": player.get("statistics", {}).get("minutesPlayed")
                    }
                    lineups["home"]["players"].append(player_info)
                
                # Sostituti
                for player in home.get("substitutions", []):
                    player_info = {
                        "id": player.get("player", {}).
