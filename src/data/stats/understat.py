"""
Modulo per l'acquisizione di dati Expected Goals (xG) da Understat.
Understat (https://understat.com) è specializzato in metriche avanzate xG e xA,
fornendo dati dettagliati su squadre, giocatori e partite.
"""
import logging
import re
import json
import math
from datetime import datetime
from bs4 import BeautifulSoup
from typing import Dict, List, Any, Optional, Union, Tuple

from ..scrapers.base_scraper import BaseScraper

logger = logging.getLogger(__name__)

class UnderstatScraper(BaseScraper):
    """
    Scraper per dati xG da Understat.
    Fornisce accesso a statistiche Expected Goals per squadre e giocatori.
    """
    
    def __init__(self):
        """Inizializza lo scraper Understat."""
        super().__init__(
            name="Understat",
            base_url="https://understat.com",
            cache_ttl=4*3600,  # 4 ore di cache
            respect_robots=True,
            delay_range=(2.5, 5.0)  # Più rispettoso per evitare blocchi
        )
        # Mappa dei league ID
        self.leagues = {
            "epl": "Premier League",
            "la_liga": "La Liga",
            "bundesliga": "Bundesliga",
            "serie_a": "Serie A",
            "ligue_1": "Ligue 1",
            "rfpl": "Russian Premier League"
        }
    
    def get_league_stats(self, league_id: str, season: str = "2023") -> Optional[Dict[str, Any]]:
        """
        Ottiene statistiche di squadra per un'intera competizione.
        
        Args:
            league_id: Identificatore lega (epl, la_liga, serie_a, bundesliga, ligue_1, rfpl)
            season: Stagione (anno, es: "2023" per 2023/2024)
            
        Returns:
            Dizionario con statistiche o None se errore
        """
        url = f"{self.base_url}/league/{league_id}/{season}"
        html = self.get(url)
        
        if not html:
            self.logger.error(f"Impossibile ottenere pagina lega {league_id} per stagione {season}")
            return None
        
        try:
            # Parse HTML
            soup = self.parse(html)
            if not soup:
                return None
            
            # Estrai dati JSON incorporati nella pagina
            team_data = self._extract_json_data(html, "teamsData")
            if not team_data:
                self.logger.error("Nessun dato squadra trovato nella pagina")
                return None
            
            # Formatta risultato
            league_name = self.leagues.get(league_id, league_id.replace("_", " ").title())
            
            stats = {
                "league_id": league_id,
                "league_name": league_name,
                "season": season,
                "source": "understat",
                "last_updated": datetime.now().isoformat(),
                "teams": self._process_team_data(team_data)
            }
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Errore nell'estrazione dati lega: {str(e)}")
            return None
    
    def get_team_stats(self, team_id: int, season: str = "2023") -> Optional[Dict[str, Any]]:
        """
        Ottiene statistiche complete per una squadra.
        
        Args:
            team_id: ID squadra in Understat
            season: Stagione (anno, es: "2023" per 2023/2024)
            
        Returns:
            Dizionario con statistiche o None se errore
        """
        url = f"{self.base_url}/team/{team_id}/{season}"
        html = self.get(url)
        
        if not html:
            self.logger.error(f"Impossibile ottenere pagina squadra {team_id} per stagione {season}")
            return None
        
        try:
            # Parse HTML
            soup = self.parse(html)
            if not soup:
                return None
            
            # Estrai info squadra
            team_name = self._extract_team_name(soup)
            
            # Estrai dati JSON incorporati nella pagina
            players_data = self._extract_json_data(html, "playersData")
            dates_data = self._extract_json_data(html, "datesData")
            team_history = self._extract_json_data(html, "statisticsData")
            
            # Formatta risultato
            stats = {
                "team_id": team_id,
                "team_name": team_name,
                "season": season,
                "source": "understat",
                "last_updated": datetime.now().isoformat(),
                "summary": self._get_team_summary(soup, team_history),
                "players": self._process_players_data(players_data) if players_data else [],
                "form": self._process_dates_data(dates_data) if dates_data else []
            }
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Errore nell'estrazione dati squadra: {str(e)}")
            return None
    
    def get_player_stats(self, player_id: int) -> Optional[Dict[str, Any]]:
        """
        Ottiene statistiche complete per un giocatore.
        
        Args:
            player_id: ID giocatore in Understat
            
        Returns:
            Dizionario con statistiche o None se errore
        """
        url = f"{self.base_url}/player/{player_id}"
        html = self.get(url)
        
        if not html:
            self.logger.error(f"Impossibile ottenere pagina giocatore {player_id}")
            return None
        
        try:
            # Parse HTML
            soup = self.parse(html)
            if not soup:
                return None
            
            # Estrai info giocatore
            player_name = self._extract_player_name(soup)
            
            # Estrai dati JSON incorporati nella pagina
            player_games = self._extract_json_data(html, "matchesData")
            player_shots = self._extract_json_data(html, "shotsData")
            player_groups = self._extract_json_data(html, "groupsData")
            
            # Formatta risultato
            stats = {
                "player_id": player_id,
                "player_name": player_name,
                "source": "understat",
                "last_updated": datetime.now().isoformat(),
                "summary": self._extract_player_summary(soup),
                "seasons": self._process_groups_data(player_groups) if player_groups else [],
                "shots": self._process_shots_data(player_shots) if player_shots else [],
                "matches": self._process_player_games(player_games) if player_games else []
            }
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Errore nell'estrazione dati giocatore: {str(e)}")
            return None
    
    def get_match_stats(self, match_id: int) -> Optional[Dict[str, Any]]:
        """
        Ottiene statistiche complete per una partita.
        
        Args:
            match_id: ID partita in Understat
            
        Returns:
            Dizionario con statistiche o None se errore
        """
        url = f"{self.base_url}/match/{match_id}"
        html = self.get(url)
        
        if not html:
            self.logger.error(f"Impossibile ottenere pagina partita {match_id}")
            return None
        
        try:
            # Parse HTML
            soup = self.parse(html)
            if not soup:
                return None
            
            # Estrai dati JSON incorporati nella pagina
            shots_data = self._extract_json_data(html, "shotsData")
            roster_data = self._extract_json_data(html, "rostersData")
            match_info = self._extract_json_data(html, "match_info")
            
            # Formatta risultato
            stats = {
                "match_id": match_id,
                "source": "understat",
                "last_updated": datetime.now().isoformat(),
                "info": self._process_match_info(match_info) if match_info else self._extract_match_info(soup),
                "shots": self._process_match_shots(shots_data) if shots_data else {},
                "players": self._process_roster_data(roster_data) if roster_data else {}
            }
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Errore nell'estrazione dati partita: {str(e)}")
            return None
    
    def search_team(self, team_name: str, league_id: str = None) -> List[Dict[str, Any]]:
        """
        Cerca squadre per nome.
        
        Args:
            team_name: Nome squadra da cercare
            league_id: ID lega per filtrare (opzionale)
            
        Returns:
            Lista di risultati con ID e nome
        """
        # Per Understat è meglio prima ottenere la lista completa delle squadre
        # da una pagina del campionato e poi filtrare localmente
        if not league_id:
            league_id = "epl"  # Default a Premier League
            
        url = f"{self.base_url}/league/{league_id}/2023"
        html = self.get(url)
        
        if not html:
            return []
        
        try:
            # Estrai dati JSON incorporati nella pagina
            team_data = self._extract_json_data(html, "teamsData")
            if not team_data:
                return []
                
            # Filtra per nome
            results = []
            for team_id, data in team_data.items():
                if team_name.lower() in data["title"].lower():
                    results.append({
                        "id": int(team_id),
                        "name": data["title"],
                        "league": self.leagues.get(league_id, league_id)
                    })
            
            return results
            
        except Exception as e:
            self.logger.error(f"Errore nella ricerca squadra: {str(e)}")
            return []
    
    def search_player(self, player_name: str) -> List[Dict[str, Any]]:
        """
        Cerca giocatori per nome.
        
        Args:
            player_name: Nome giocatore da cercare
            
        Returns:
            Lista di risultati con ID e nome
        """
        # Understat non ha un endpoint di ricerca, useremmo la Premier League
        # come base per cercare giocatori (limitazione dell'approccio)
        url = f"{self.base_url}/league/epl/2023"
        html = self.get(url)
        
        if not html:
            return []
        
        try:
            # Cerca link ai profili dei giocatori
            soup = self.parse(html)
            if not soup:
                return []
                
            results = []
            player_links = soup.select('a[href^="/player/"]')
            
            for link in player_links:
                name = link.text.strip()
                if player_name.lower() in name.lower():
                    href = link.get('href', '')
                    player_id_match = re.search(r'/player/(\d+)', href)
                    if player_id_match:
                        player_id = int(player_id_match.group(1))
                        # Evita duplicati
                        if not any(p['id'] == player_id for p in results):
                            results.append({
                                "id": player_id,
                                "name": name
                            })
            
            return results
            
        except Exception as e:
            self.logger.error(f"Errore nella ricerca giocatore: {str(e)}")
            return []
    
    # Metodi interni per estrazione dati
    
    def _extract_json_data(self, html: str, variable_name: str) -> Any:
        """
        Estrae dati JSON incorporati nelle pagine Understat.
        
        Args:
            html: Contenuto HTML della pagina
            variable_name: Nome della variabile JavaScript da estrarre
            
        Returns:
            Dati JSON estratti o None se non trovati
        """
        try:
            # Pattern per trovare l'assegnazione della variabile
            pattern = re.compile(f"var {variable_name} = JSON.parse\\('(.*?)'\\);")
            match = pattern.search(html)
            
            if match:
                # Decodifica i caratteri Unicode e i backslash
                json_str = match.group(1).encode().decode('unicode_escape')
                return json.loads(json_str)
            else:
                self.logger.warning(f"Variabile {variable_name} non trovata nella pagina")
                return None
        except Exception as e:
            self.logger.error(f"Errore nell'estrazione dati JSON per {variable_name}: {str(e)}")
            return None
    
    def _extract_team_name(self, soup: BeautifulSoup) -> str:
        """Estrae il nome della squadra."""
        try:
            name_elem = soup.select_one('div.page-wrapper div.header h1')
            if name_elem:
                return name_elem.text.strip()
        except Exception:
            pass
        return "Unknown Team"
    
    def _extract_player_name(self, soup: BeautifulSoup) -> str:
        """Estrae il nome del giocatore."""
        try:
            name_elem = soup.select_one('div.page-wrapper div.header h1')
            if name_elem:
                return name_elem.text.strip()
        except Exception:
            pass
        return "Unknown Player"
    
    def _extract_match_info(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Estrae informazioni sulla partita dal HTML."""
        match_info = {
            "home_team": "Unknown",
            "away_team": "Unknown",
            "score": [0, 0],
            "date": None
        }
        
        try:
            # Estrai squadre e punteggio
            teams = soup.select('div.team-block')
            if len(teams) >= 2:
                match_info["home_team"] = teams[0].select_one('a').text.strip()
                match_info["away_team"] = teams[1].select_one('a').text.strip()
                
                score_elems = soup.select('div.outcome-info div.outcome')
                if len(score_elems) >= 2:
                    match_info["score"] = [
                        int(score_elems[0].text.strip()),
                        int(score_elems[1].text.strip())
                    ]
            
            # Estrai data partita
            date_elem = soup.select_one('div.match-info div.match-info__date-time')
            if date_elem:
                date_text = date_elem.text.strip()
                try:
                    # Formato data: "2023-04-15, 15:00"
                    date_obj = datetime.strptime(date_text, "%Y-%m-%d, %H:%M")
                    match_info["date"] = date_obj.isoformat()
                except ValueError:
                    match_info["date"] = date_text
        except Exception as e:
            self.logger.warning(f"Errore nell'estrazione info partita: {str(e)}")
        
        return match_info
    
    def _get_team_summary(self, soup: BeautifulSoup, history_data: Optional[Dict] = None) -> Dict[str, Any]:
        """Estrae il riepilogo statistico della squadra."""
        summary = {}
        
        try:
            # Estrai dalle tabelle statistiche
            stats_blocks = soup.select('div.block-statistics')
            for block in stats_blocks:
                title_elem = block.select_one('div.header')
                if not title_elem:
                    continue
                
                title = title_elem.text.strip().lower()
                
                # Processa tabella
                table = block.select_one('table')
                if table:
                    rows = table.select('tr')
                    for row in rows[1:]:  # Salta l'intestazione
                        cells = row.select('td')
                        if len(cells) >= 2:
                            stat_name = cells[0].text.strip().lower().replace(' ', '_')
                            stat_value = self.to_numeric(cells[1].text.strip())
                            summary[stat_name] = stat_value
            
            # Aggiungi dati dallo storico JSON se disponibile
            if history_data:
                for stat_name, values in history_data.items():
                    if isinstance(values, list) and len(values) > 0:
                        # Calcola somma o media in base al tipo di statistica
                        if stat_name in ['xG', 'xGA', 'npxG', 'npxGA', 'ppda', 'ppda_allowed']:
                            summary[stat_name] = sum(float(item['stat']) for item in values) / len(values)
                        elif stat_name in ['deep', 'deep_allowed']:
                            summary[stat_name] = sum(int(item['stat']) for item in values)
            
            # Aggiungi statistiche aggiuntive
            if 'goals' in summary and 'xg' in summary:
                summary['goals_vs_xg'] = round(summary['goals'] - summary['xg'], 2)
            if 'goals_against' in summary and 'xga' in summary:
                summary['goals_against_vs_xga'] = round(summary['goals_against'] - summary['xga'], 2)
                
        except Exception as e:
            self.logger.warning(f"Errore nell'estrazione riepilogo squadra: {str(e)}")
        
        return summary
    
    def _extract_player_summary(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Estrae il riepilogo statistico del giocatore."""
        summary = {}
        
        try:
            # Estrai info base
            info_block = soup.select_one('div.player-info')
            if info_block:
                # Nazionalità
                country_elem = info_block.select_one('span.player-info__country')
                if country_elem:
                    summary['country'] = country_elem.text.strip()
                
                # Età
                age_elem = info_block.select_one('div.player-info__role-data')
                if age_elem:
                    age_text = age_elem.text.strip()
                    age_match = re.search(r'Age: (\d+)', age_text)
                    if age_match:
                        summary['age'] = int(age_match.group(1))
                        
                # Squadra attuale
                team_elem = info_block.select_one('a[href^="/team/"]')
                if team_elem:
                    summary['current_team'] = team_elem.text.strip()
                    team_id_match = re.search(r'/team/(\d+)', team_elem['href'])
                    if team_id_match:
                        summary['current_team_id'] = int(team_id_match.group(1))
            
            # Estrai statistiche
            stats_containers = soup.select('div.statistic-container')
            for container in stats_containers:
                # Nome statistica
                label_elem = container.select_one('div.statistic-title')
                if not label_elem:
                    continue
                
                stat_name = label_elem.text.strip().lower().replace(' ', '_')
                
                # Valore statistica
                value_elem = container.select_one('div.statistic-value')
                if value_elem:
                    summary[stat_name] = self.to_numeric(value_elem.text.strip())
                    
        except Exception as e:
            self.logger.warning(f"Errore nell'estrazione riepilogo giocatore: {str(e)}")
        
        return summary
    
    def _process_team_data(self, team_data: Dict) -> List[Dict[str, Any]]:
        """Processa i dati delle squadre dal JSON."""
        teams = []
        
        for team_id, data in team_data.items():
            try:
                team = {
                    "id": int(team_id),
                    "name": data["title"],
                    "matches": int(data["history"][-1]["matches"]) if data["history"] else 0,
                    "wins": int(data["history"][-1]["wins"]) if data["history"] else 0,
                    "draws": int(data["history"][-1]["draws"]) if data["history"] else 0,
                    "loses": int(data["history"][-1]["loses"]) if data["history"] else 0,
                    "points": int(data["history"][-1]["points"]) if data["history"] else 0,
                    "scored": int(data["history"][-1]["scored"]) if data["history"] else 0,
                    "missed": int(data["history"][-1]["missed"]) if data["history"] else 0,
                    "xG": float(data["history"][-1]["xG"]) if data["history"] else 0,
                    "xGA": float(data["history"][-1]["xGA"]) if data["history"] else 0,
                    "npxG": float(data["history"][-1]["npxG"]) if data["history"] else 0,
                    "npxGA": float(data["history"][-1]["npxGA"]) if data["history"] else 0,
                    "xpts": float(data["history"][-1]["xpts"]) if data["history"] else 0,
                    "position": int(data["history"][-1]["position"]) if data["history"] else 0
                }
                
                # Aggiungi statistiche calcolate
                if team["matches"] > 0:
                    team["xG_per_match"] = round(team["xG"] / team["matches"], 2)
                    team["xGA_per_match"] = round(team["xGA"] / team["matches"], 2)
                    team["goals_per_match"] = round(team["scored"] / team["matches"], 2)
                    team["conceded_per_match"] = round(team["missed"] / team["matches"], 2)
                    
                # Aggiungi differenze tra valori reali e attesi
                team["goals_minus_xG"] = round(team["scored"] - team["xG"], 2)
                team["conceded_minus_xGA"] = round(team["missed"] - team["xGA"], 2)
                
                teams.append(team)
            except Exception as e:
                self.logger.warning(f"Errore nell'elaborazione dati squadra {team_id}: {str(e)}")
                continue
        
        # Ordina per posizione
        teams.sort(key=lambda x: x["position"])
        return teams
    
    def _process_players_data(self, players_data: List) -> List[Dict[str, Any]]:
        """Processa i dati dei giocatori dal JSON."""
        players = []
        
        for player in players_data:
            try:
                player_info = {
                    "id": int(player["id"]),
                    "name": player["player_name"],
                    "position": player["position"],
                    "team_id": int(player["team_id"]),
                    "team_name": player["team_title"],
                    "games": int(player["games"]),
                    "time": int(player["time"]),
                    "goals": int(player["goals"]),
                    "assists": int(player["assists"]),
                    "shots": int(player["shots"]),
                    "key_passes": int(player["key_passes"]),
                    "yellow_cards": int(player["yellow_cards"]),
                    "red_cards": int(player["red_cards"]),
                    "xG": float(player["xG"]),
                    "xA": float(player["xA"]),
                    "npg": int(player["npg"]),
                    "npxG": float(player["npxG"])
                }
                
                # Calcola statistiche per 90 minuti
                if player_info["time"] > 0:
                    minutes = player_info["time"]
                    player_info["xG_per_90"] = round((player_info["xG"] / minutes) * 90, 2)
                    player_info["xA_per_90"] = round((player_info["xA"] / minutes) * 90, 2)
                    player_info["goals_per_90"] = round((player_info["goals"] / minutes) * 90, 2)
                    player_info["assists_per_90"] = round((player_info["assists"] / minutes) * 90, 2)
                    player_info["shots_per_90"] = round((player_info["shots"] / minutes) * 90, 2)
                
                # Calcola differenze tra valori reali e attesi
                player_info["goals_minus_xG"] = round(player_info["goals"] - player_info["xG"], 2)
                player_info["assists_minus_xA"] = round(player_info["assists"] - player_info["xA"], 2)
                
                players.append(player_info)
            except Exception as e:
                self.logger.warning(f"Errore nell'elaborazione dati giocatore: {str(e)}")
                continue
        
        # Ordina per xG (miglior attaccante in cima)
        players.sort(key=lambda x: x["xG"], reverse=True)
        return players
    
    def _process_dates_data(self, dates_data: List) -> List[Dict[str, Any]]:
        """Processa i dati delle partite per andamento temporale."""
        matches = []
        
        for match in dates_data:
            try:
                match_info = {
                    "id": int(match["id"]),
                    "home_team": match["h"]["title"],
                    "away_team": match["a"]["title"],
                    "date": match["date"],
                    "home_goals": int(match["goals"]["h"]),
                    "away_goals": int(match["goals"]["a"]),
                    "home_xG": float(match["xG"]["h"]),
                    "away_xG": float(match["xG"]["a"]),
                    "result": match["result"]
                }
                
                # Aggiungi statistiche calcolate
                match_info["total_goals"] = match_info["home_goals"] + match_info["away_goals"]
                match_info["total_xG"] = round(match_info["home_xG"] + match_info["away_xG"], 2)
                match_info["home_xG_diff"] = round(match_info["home_goals"] - match_info["home_xG"], 2)
                match_info["away_xG_diff"] = round(match_info["away_goals"] - match_info["away_xG"], 2)
                
                matches.append(match_info)
            except Exception as e:
                self.logger.warning(f"Errore nell'elaborazione dati partita: {str(e)}")
                continue
        
        # Ordina per data (più recenti prima)
        matches.sort(key=lambda x: x["date"], reverse=True)
        return matches
    
    def _process_groups_data(self, groups_data: Dict) -> List[Dict[str, Any]]:
        """Processa i dati delle stagioni raggruppati per un giocatore."""
        seasons = []
        
        for season_name, season_data in groups_data.items():
            try:
                season_info = {
                    "season": season_name,
                    "team_name": season_data[0]["team_title"] if len(season_data) > 0 else "",
                    "games": sum(int(match["time"]) > 0 for match in season_data),
                    "time": sum(int(match["time"]) for match in season_data),
                    "goals": sum(int(match["goals"]) for match in season_data),
                    "assists": sum(int(match["assists"]) for match in season_data),
                    "shots": sum(int(match["shots"]) for match in season_data),
                    "key_passes": sum(int(match["key_passes"]) for match in season_data),
                    "xG": sum(float(match["xG"]) for match in season_data),
                    "xA": sum(float(match["xA"]) for match in season_data),
                    "npxG": sum(float(match["npxG"]) for match in season_data)
                }
                
                # Calcola statistiche per 90 minuti
                if season_info["time"] > 0:
                    minutes = season_info["time"]
                    season_info["xG_per_90"] = round((season_info["xG"] / minutes) * 90, 2)
                    season_info["xA_per_90"] = round((season_info["xA"] / minutes) * 90, 2)
                    season_info["goals_per_90"] = round((season_info["goals"] / minutes) * 90, 2)
                    season_info["assists_per_90"] = round((season_info["assists"] / minutes) * 90, 2)
                
                # Calcola differenze tra valori reali e attesi
                season_info["goals_minus_xG"] = round(season_info["goals"] - season_info["xG"], 2)
                season_info["assists_minus_xA"] = round(season_info["assists"] - season_info["xA"], 2)
                
                seasons.append(season_info)
            except Exception as e:
                self.logger.warning(f"Errore nell'elaborazione dati stagione {season_name}: {str(e)}")
                continue
        
        # Ordina per stagione (più recente prima)
        seasons.sort(key=lambda x: x["season"], reverse=True)
        return seasons
    
    def _process_shots_data(self, shots_data: List) -> List[Dict[str, Any]]:
        """Processa i dati dei tiri di un giocatore."""
        shots = []
        
        for shot in shots_data:
            try:
                shot_info = {
                    "id": shot["id"],
                    "minute": int(shot["minute"]),
                    "result": shot["result"],
                    "X": float(shot["X"]),
                    "Y": float(shot["Y"]),
                    "xG": float(shot["xG"]),
                    "match_id": int(shot["match_id"]),
                    "h_team": shot["h_team"],
                    "a_team": shot["a_team"],
                    "h_goals": int(shot["h_goals"]),
                    "a_goals": int(shot["a_goals"]),
                    "date": shot["date"],
                    "player_id": int(shot["player_id"]),
                    "player": shot["player"],
                    "season": shot["season"],
                    "shotType": shot.get("shotType", ""),
                    "match_situation": shot.get("situation", ""),
                    "shot_distance": self._calculate_shot_distance(float(shot["X"]), float(shot["Y"]))
                }
                
                shots.append(shot_info)
            except Exception as e:
                self.logger.warning(f"Errore nell'elaborazione dati tiro: {str(e)}")
                continue
        
        # Ordina per data (più recenti prima)
        shots.sort(key=lambda x: x["date"], reverse=True)
        return shots
    
    def _process_player_games(self, games_data: List) -> List[Dict[str, Any]]:
        """Processa i dati delle partite di un giocatore."""
        games = []
        
        for game in games_data:
            try:
                game_info = {
                    "id": int(game["id"]),
                    "home_team": game["h"]["title"],
                    "away_team": game["a"]["title"],
                    "date": game["date"],
                    "goals": int(game["goals"]),
                    "assists": int(game["assists"]),
                    "time": int(game["time"]),
                    "shots": int(game["shots"]),
                    "key_passes": int(game["key_passes"]),
                    "xG": float(game["xG"]),
                    "xA": float(game["xA"]),
                    "position": game["position"],
                    "home_goals": int(game["h_goals"]),
                    "away_goals": int(game["a_goals"]),
                    "home_team_id": int(game["h_id"]),
                    "away_team_id": int(game["a_id"])
                }
                
                # Aggiungi la squadra per cui ha giocato
                if game_info["home_team_id"] == int(game["team"]["id"]):
                    game_info["played_for"] = "home"
                else:
                    game_info["played_for"] = "away"
                
                # Statistiche per 90 minuti
                if game_info["time"] > 0:
                    minutes = game_info["time"]
                    game_info["xG_per_90"] = round((game_info["xG"] / minutes) * 90, 2)
                    game_info["xA_per_90"] = round((game_info["xA"] / minutes) * 90, 2)
                
                # Differenze tra valori reali e attesi
                game_info["goals_minus_xG"] = round(game_info["goals"] - game_info["xG"], 2)
                game_info["assists_minus_xA"] = round(game_info["assists"] - game_info["xA"], 2)
                
                games.append(game_info)
            except Exception as e:
                self.logger.warning(f"Errore nell'elaborazione dati partita giocatore: {str(e)}")
                continue
        
        # Ordina per data (più recenti prima)
        games.sort(key=lambda x: x["date"], reverse=True)
        return games
    
    def _process_match_info(self, match_info: Dict) -> Dict[str, Any]:
        """Processa le informazioni base della partita."""
        try:
            info = {
                "home_team": match_info.get("h", {}).get("title", "Unknown"),
                "away_team": match_info.get("a", {}).get("title", "Unknown"),
                "home_team_id": int(match_info.get("h", {}).get("id", 0)),
                "away_team_id": int(match_info.get("a", {}).get("id", 0)),
                "score": [
                    int(match_info.get("goals", {}).get("h", 0)),
                    int(match_info.get("goals", {}).get("a", 0))
                ],
                "xG": [
                    float(match_info.get("xG", {}).get("h", 0)),
                    float(match_info.get("xG", {}).get("a", 0))
                ],
                "date": match_info.get("date"),
                "league": match_info.get("league", "Unknown"),
                "season": match_info.get("season")
            }
            
            # Calcola differenze xG
            info["home_xG_diff"] = round(info["score"][0] - info["xG"][0], 2)
            info["away_xG_diff"] = round(info["score"][1] - info["xG"][1], 2)
            info["total_xG"] = round(info["xG"][0] + info["xG"][1], 2)
            
            return info
        except Exception as e:
            self.logger.warning(f"Errore nell'elaborazione info partita: {str(e)}")
            return {}
    
    def _process_match_shots(self, shots_data: Dict) -> Dict[str, List[Dict[str, Any]]]:
        """Processa i dati dei tiri in una partita."""
        result = {
            "home": [],
            "away": []
        }
        
        try:
            # Processa tiri squadra casa
            for shot in shots_data.get("h", []):
                shot_info = {
                    "id": shot["id"],
                    "minute": int(shot["minute"]),
                    "player": shot["player"],
                    "player_id": int(shot["player_id"]),
                    "X": float(shot["X"]),
                    "Y": float(shot["Y"]),
                    "xG": float(shot["xG"]),
                    "result": shot["result"],
                    "shotType": shot.get("shotType", ""),
                    "situation": shot.get("situation", ""),
                    "distance": self._calculate_shot_distance(float(shot["X"]), float(shot["Y"]))
                }
                result["home"].append(shot_info)
                
            # Processa tiri squadra trasferta
            for shot in shots_data.get("a", []):
                shot_info = {
                    "id": shot["id"],
                    "minute": int(shot["minute"]),
                    "player": shot["player"],
                    "player_id": int(shot["player_id"]),
                    "X": float(shot["X"]),
                    "Y": float(shot["Y"]),
                    "xG": float(shot["xG"]),
                    "result": shot["result"],
                    "shotType": shot.get("shotType", ""),
                    "situation": shot.get("situation", ""),
                    "distance": self._calculate_shot_distance(float(shot["X"]), float(shot["Y"]))
                }
                result["away"].append(shot_info)
                
        except Exception as e:
            self.logger.warning(f"Errore nell'elaborazione tiri partita: {str(e)}")
            
        return result
    
    def _process_roster_data(self, roster_data: Dict) -> Dict[str, List[Dict[str, Any]]]:
        """Processa i dati dei giocatori in una partita."""
        result = {
            "home": [],
            "away": []
        }
        
        try:
            # Processa giocatori squadra casa
            for player in roster_data.get("h", []):
                player_info = {
                    "id": int(player["player_id"]),
                    "name": player["player"],
                    "position": player["position"],
                    "time": int(player["time"]),
                    "goals": int(player["goals"]),
                    "assists": int(player["assists"]),
                    "shots": int(player["shots"]),
                    "key_passes": int(player["key_passes"]),
                    "xG": float(player["xG"]),
                    "xA": float(player["xA"]),
                    "xGChain": float(player["xGChain"]),
                    "xGBuildup": float(player["xGBuildup"])
                }
                result["home"].append(player_info)
                
            # Processa giocatori squadra trasferta
            for player in roster_data.get("a", []):
                player_info = {
                    "id": int(player["player_id"]),
                    "name": player["player"],
                    "position": player["position"],
                    "time": int(player["time"]),
                    "goals": int(player["goals"]),
                    "assists": int(player["assists"]),
                    "shots": int(player["shots"]),
                    "key_passes": int(player["key_passes"]),
                    "xG": float(player["xG"]),
                    "xA": float(player["xA"]),
                    "xGChain": float(player["xGChain"]),
                    "xGBuildup": float(player["xGBuildup"])
                }
                result["away"].append(player_info)
                
        except Exception as e:
            self.logger.warning(f"Errore nell'elaborazione roster partita: {str(e)}")
            
        return result
    
    def _calculate_shot_distance(self, x: float, y: float) -> float:
        """
        Calcola la distanza approssimativa di un tiro dalla porta.
        In Understat, le coordinate sono normalizzate tra 0 e 1, con la porta a (1, 0.5).
        
        Args:
            x: Coordinata X normalizzata (0-1)
            y: Coordinata Y normalizzata (0-1)
            
        Returns:
            Distanza in metri approssimativa
        """
        try:
            # Converti le coordinate normalizzate (0-1) in metri
            # Assumendo un campo di 105m x 68m
            field_length = 105
            field_width = 68
            
            # Le coordinate in Understat hanno origine in basso a sinistra
            # e la porta è a destra (x=1, y=0.5)
            
            # Converti in metri dal centro campo
            x_meters = (x - 0.5) * field_length
            y_meters = (y - 0.5) * field_width
            
            # La porta si trova a x = field_length/2 = 52.5m dal centro
            goal_x = field_length / 2
            goal_y = 0  # La porta è al centro della larghezza
            
            # Calcola la distanza euclidea dalla porta
            dx = goal_x - x_meters
            dy = goal_y - y_meters
            distance = round(math.sqrt(dx*dx + dy*dy), 1)
            
            return distance
        except Exception:
            return 0.0

def get_team_xg(team_id: int, season: str = "2023") -> Optional[Dict[str, Any]]:
    """
    Ottiene statistiche Expected Goals (xG) di una squadra da Understat.
    
    Args:
        team_id: Identificatore squadra in Understat
        season: Stagione (anno, es: "2023" per 2023/2024)
        
    Returns:
        Dizionario con statistiche xG o None se errore
    """
    scraper = UnderstatScraper()
    return scraper.get_team_stats(team_id, season)

def get_player_xg(player_id: int) -> Optional[Dict[str, Any]]:
    """
    Ottiene statistiche Expected Goals (xG) di un giocatore da Understat.
    
    Args:
        player_id: Identificatore giocatore in Understat
        
    Returns:
        Dizionario con statistiche xG del giocatore o None se errore
    """
    scraper = UnderstatScraper()
    return scraper.get_player_stats(player_id)

def get_match_xg(match_id: int) -> Optional[Dict[str, Any]]:
    """
    Ottiene statistiche Expected Goals (xG) di una partita da Understat.
    
    Args:
        match_id: Identificatore partita in Understat
        
    Returns:
        Dizionario con statistiche xG della partita o None se errore
    """
    scraper = UnderstatScraper()
    return scraper.get_match_stats(match_id)

def get_scraper() -> UnderstatScraper:
    """Restituisce un'istanza dello scraper Understat.
    
    Returns:
        UnderstatScraper: Un'istanza configurata dello scraper Understat
    """
    return UnderstatScraper()
