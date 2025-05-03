"""
Modulo per l'acquisizione di statistiche avanzate da FBref.
FBref (https://fbref.com) è una fonte di statistiche avanzate per il calcio,
inclusi expected goals (xG), statistiche di passaggio, e dati difensivi.
"""
import logging
import re
import json
from datetime import datetime
from bs4 import BeautifulSoup
from typing import Dict, List, Any, Optional, Union, Tuple

from ..scrapers.base_scraper import BaseScraper

logger = logging.getLogger(__name__)

class FBrefScraper(BaseScraper):
    """
    Scraper per statistiche avanzate da FBref (Sports Reference).
    Fornisce accesso a dati come xG, passaggi, tiri, e altre statistiche.
    """
    
    def __init__(self):
        """Inizializza lo scraper FBref."""
        super().__init__(
            name="FBref",
            base_url="https://fbref.com",
            cache_ttl=6*3600,  # 6 ore di cache
            respect_robots=True,
            delay_range=(2.0, 4.0)  # Più rispettoso dei rate limits
        )
    
    def get_team_stats(self, team_id: str, season: str = "2023-2024") -> Optional[Dict[str, Any]]:
        """
        Ottiene statistiche di squadra complete.
        
        Args:
            team_id: Identificatore squadra in FBref (es. "fd962109")
            season: Stagione (formato "YYYY-YYYY")
            
        Returns:
            Dizionario con statistiche o None se errore
        """
        url = f"{self.base_url}/en/squads/{team_id}/{season}/all_comps/stats"
        html = self.get(url)
        
        if not html:
            self.logger.error(f"Impossibile ottenere statistiche per team {team_id}")
            return None
        
        try:
            # Parse HTML
            soup = self.parse(html)
            if not soup:
                return None
            
            # Ottieni info base squadra
            team_name = self._extract_team_name(soup)
            
            # Crea risultato
            stats = {
                "team_id": team_id,
                "team_name": team_name,
                "season": season,
                "source": "fbref",
                "last_updated": datetime.now().isoformat(),
                "standard_stats": self._extract_standard_stats(soup),
                "advanced_stats": {
                    "expected_goals": self._extract_xg_stats(soup),
                    "possession": self._extract_possession_stats(soup),
                    "passing": self._extract_passing_stats(soup),
                    "defensive": self._extract_defensive_stats(soup)
                }
            }
            
            return stats
        
        except Exception as e:
            self.logger.error(f"Errore nell'estrazione statistiche FBref: {str(e)}")
            return None
    
    def get_player_stats(self, player_id: str, season: str = "2023-2024") -> Optional[Dict[str, Any]]:
        """
        Ottiene statistiche per un singolo giocatore.
        
        Args:
            player_id: Identificatore giocatore in FBref
            season: Stagione (formato "YYYY-YYYY")
            
        Returns:
            Dizionario con statistiche o None se errore
        """
        url = f"{self.base_url}/en/players/{player_id}/stats/{season}"
        html = self.get(url)
        
        if not html:
            self.logger.error(f"Impossibile ottenere statistiche per giocatore {player_id}")
            return None
        
        try:
            soup = self.parse(html)
            if not soup:
                return None
            
            # Ottieni info base
            player_name = self._extract_player_name(soup)
            player_team = self._extract_player_team(soup)
            
            # Crea risultato
            stats = {
                "player_id": player_id,
                "player_name": player_name,
                "team": player_team,
                "season": season,
                "source": "fbref",
                "last_updated": datetime.now().isoformat(),
                "summary": self._extract_player_summary(soup),
                "shooting": self._extract_player_shooting(soup),
                "passing": self._extract_player_passing(soup),
                "defensive": self._extract_player_defensive(soup)
            }
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Errore nell'estrazione statistiche giocatore FBref: {str(e)}")
            return None
    
    def get_match_stats(self, match_id: str) -> Optional[Dict[str, Any]]:
        """
        Ottiene statistiche per una singola partita.
        
        Args:
            match_id: Identificatore partita in FBref
            
        Returns:
            Dizionario con statistiche o None se errore
        """
        url = f"{self.base_url}/en/matches/{match_id}/stats"
        html = self.get(url)
        
        if not html:
            self.logger.error(f"Impossibile ottenere statistiche per partita {match_id}")
            return None
        
        try:
            soup = self.parse(html)
            if not soup:
                return None
                
            # Estrai info match
            match_info = self._extract_match_info(soup)
            
            # Crea risultato
            stats = {
                "match_id": match_id,
                "info": match_info,
                "source": "fbref",
                "last_updated": datetime.now().isoformat(),
                "team_stats": {
                    "home": self._extract_match_team_stats(soup, home=True),
                    "away": self._extract_match_team_stats(soup, home=False)
                },
                "player_stats": self._extract_match_player_stats(soup)
            }
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Errore nell'estrazione statistiche partita FBref: {str(e)}")
            return None
    
    def get_league_standings(self, league_id: str, season: str = "2023-2024") -> Optional[Dict[str, Any]]:
        """
        Ottiene la classifica di un campionato.
        
        Args:
            league_id: Identificatore campionato in FBref
            season: Stagione (formato "YYYY-YYYY")
            
        Returns:
            Dizionario con classifica o None se errore
        """
        url = f"{self.base_url}/en/comps/{league_id}/{season}"
        html = self.get(url)
        
        if not html:
            self.logger.error(f"Impossibile ottenere classifica per campionato {league_id}")
            return None
        
        try:
            soup = self.parse(html)
            if not soup:
                return None
            
            standings = self._extract_standings(soup)
            
            return {
                "league_id": league_id,
                "season": season,
                "source": "fbref",
                "last_updated": datetime.now().isoformat(),
                "standings": standings
            }
            
        except Exception as e:
            self.logger.error(f"Errore nell'estrazione classifica FBref: {str(e)}")
            return None
    
    def search_team(self, team_name: str) -> List[Dict[str, str]]:
        """
        Cerca una squadra per nome.
        
        Args:
            team_name: Nome squadra da cercare
            
        Returns:
            Lista di risultati (id e nome)
        """
        url = f"{self.base_url}/en/search/search.fcgi"
        params = {
            "search": team_name
        }
        
        html = self.get(url, params=params)
        if not html:
            return []
        
        soup = self.parse(html)
        if not soup:
            return []
        
        results = []
        
        # Gestisci redirect diretti su risultato singolo
        if "squad" in soup.select_one("title").text.lower():
            team_id = self._extract_team_id_from_url(self.session.history[0].url)
            team_name = self._extract_team_name(soup)
            if team_id and team_name:
                return [{"id": team_id, "name": team_name}]
        
        # Gestisci lista risultati
        search_results = soup.select("div.search-item-name")
        for result in search_results:
            link = result.select_one("a")
            if link and "/squads/" in link.get("href", ""):
                team_id = self._extract_team_id_from_url(link["href"])
                team_name = link.text.strip()
                results.append({
                    "id": team_id,
                    "name": team_name
                })
        
        return results
    
    # Metodi privati per estrazione dati
    
    def _extract_team_name(self, soup: BeautifulSoup) -> str:
        """Estrae il nome della squadra."""
        title_elem = soup.select_one("h1[itemprop='name']")
        if title_elem:
            return title_elem.text.strip()
        return "Unknown Team"
    
    def _extract_standard_stats(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Estrae statistiche standard della squadra."""
        stats = {}
        
        # Trova tabella statistiche standard
        std_table = soup.select_one('table#stats_standard_squads')
        if not std_table:
            return stats
            
        # Ottieni righe della tabella
        rows = std_table.select('tbody tr')
        if not rows:
            return stats
            
        # Estrai valori dalla prima riga
        try:
            row = rows[0]
            
            stats["matches_played"] = self.to_numeric(self.extract_text(row, 'td[data-stat="games"]'))
            stats["goals"] = self.to_numeric(self.extract_text(row, 'td[data-stat="goals"]'))
            stats["assists"] = self.to_numeric(self.extract_text(row, 'td[data-stat="assists"]'))
            stats["goals_per90"] = self.to_numeric(self.extract_text(row, 'td[data-stat="goals_per90"]'))
            stats["assists_per90"] = self.to_numeric(self.extract_text(row, 'td[data-stat="assists_per90"]'))
            stats["goals_assists_per90"] = self.to_numeric(self.extract_text(row, 'td[data-stat="goals_assists_per90"]'))
            stats["xg"] = self.to_numeric(self.extract_text(row, 'td[data-stat="xg"]'))
            stats["npxg"] = self.to_numeric(self.extract_text(row, 'td[data-stat="npxg"]'))
            stats["xg_assist"] = self.to_numeric(self.extract_text(row, 'td[data-stat="xa"]'))
            
            # Calcola metriche aggiuntive
            if stats["matches_played"] > 0:
                stats["goals_per_match"] = round(stats["goals"] / stats["matches_played"], 2)
            else:
                stats["goals_per_match"] = 0
        except (IndexError, ValueError) as e:
            self.logger.warning(f"Errore nell'estrazione statistiche standard: {str(e)}")
            
        return stats
    
    def _extract_xg_stats(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Estrae statistiche Expected Goals."""
        stats = {}
        
        # Trova tabella xG
        xg_table = soup.select_one('table#stats_shooting_squads')
        if not xg_table:
            return stats
            
        # Ottieni righe della tabella
        rows = xg_table.select('tbody tr')
        if not rows:
            return stats
            
        # Estrai valori dalla prima riga
        try:
            row = rows[0]
            
            stats["shots"] = self.to_numeric(self.extract_text(row, 'td[data-stat="shots"]'))
            stats["shots_on_target"] = self.to_numeric(self.extract_text(row, 'td[data-stat="shots_on_target"]'))
            stats["shots_on_target_pct"] = self.to_numeric(self.extract_text(row, 'td[data-stat="shots_on_target_pct"]'))
            stats["shots_per90"] = self.to_numeric(self.extract_text(row, 'td[data-stat="shots_per90"]'))
            stats["shots_on_target_per90"] = self.to_numeric(self.extract_text(row, 'td[data-stat="shots_on_target_per90"]'))
            stats["xg"] = self.to_numeric(self.extract_text(row, 'td[data-stat="xg"]'))
            stats["npxg"] = self.to_numeric(self.extract_text(row, 'td[data-stat="npxg"]'))
            stats["xg_per_shot"] = self.to_numeric(self.extract_text(row, 'td[data-stat="xg_per_shot"]'))
            stats["goals_per_shot"] = self.to_numeric(self.extract_text(row, 'td[data-stat="goals_per_shot"]'))
            stats["goals_per_shot_on_target"] = self.to_numeric(self.extract_text(row, 'td[data-stat="goals_per_shot_on_target"]'))
        except (IndexError, ValueError) as e:
            self.logger.warning(f"Errore nell'estrazione statistiche xG: {str(e)}")
            
        return stats
    
    def _extract_possession_stats(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Estrae statistiche possesso palla."""
        stats = {}
        
        # Trova tabella possesso
        poss_table = soup.select_one('table#stats_possession_squads')
        if not poss_table:
            return stats
            
        # Ottieni righe della tabella
        rows = poss_table.select('tbody tr')
        if not rows:
            return stats
            
        # Estrai valori dalla prima riga
        try:
            row = rows[0]
            
            stats["possession"] = self.to_numeric(self.extract_text(row, 'td[data-stat="possession"]'))
            stats["touches"] = self.to_numeric(self.extract_text(row, 'td[data-stat="touches"]'))
            stats["touches_def_pen"] = self.to_numeric(self.extract_text(row, 'td[data-stat="touches_def_pen_area"]'))
            stats["touches_def_3rd"] = self.to_numeric(self.extract_text(row, 'td[data-stat="touches_def_3rd"]'))
            stats["touches_mid_3rd"] = self.to_numeric(self.extract_text(row, 'td[data-stat="touches_mid_3rd"]'))
            stats["touches_att_3rd"] = self.to_numeric(self.extract_text(row, 'td[data-stat="touches_att_3rd"]'))
            stats["touches_att_pen"] = self.to_numeric(self.extract_text(row, 'td[data-stat="touches_att_pen_area"]'))
            stats["carries"] = self.to_numeric(self.extract_text(row, 'td[data-stat="carries"]'))
            stats["progressive_carries"] = self.to_numeric(self.extract_text(row, 'td[data-stat="progressive_carries"]'))
        except (IndexError, ValueError) as e:
            self.logger.warning(f"Errore nell'estrazione statistiche possesso: {str(e)}")
            
        return stats
    
    def _extract_passing_stats(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Estrae statistiche passaggi."""
        stats = {}
        
        # Trova tabella passaggi
        pass_table = soup.select_one('table#stats_passing_squads')
        if not pass_table:
            return stats
            
        # Ottieni righe della tabella
        rows = pass_table.select('tbody tr')
        if not rows:
            return stats
            
        # Estrai valori dalla prima riga
        try:
            row = rows[0]
            
            stats["passes_completed"] = self.to_numeric(self.extract_text(row, 'td[data-stat="passes_completed"]'))
            stats["passes"] = self.to_numeric(self.extract_text(row, 'td[data-stat="passes"]'))
            stats["passes_pct"] = self.to_numeric(self.extract_text(row, 'td[data-stat="passes_pct"]'))
            stats["passes_total_distance"] = self.to_numeric(self.extract_text(row, 'td[data-stat="passes_total_distance"]'))
            stats["passes_progressive_distance"] = self.to_numeric(self.extract_text(row, 'td[data-stat="passes_progressive_distance"]'))
            stats["passes_completed_short"] = self.to_numeric(self.extract_text(row, 'td[data-stat="passes_completed_short"]'))
            stats["passes_short"] = self.to_numeric(self.extract_text(row, 'td[data-stat="passes_short"]'))
            stats["passes_pct_short"] = self.to_numeric(self.extract_text(row, 'td[data-stat="passes_pct_short"]'))
            stats["passes_completed_medium"] = self.to_numeric(self.extract_text(row, 'td[data-stat="passes_completed_medium"]'))
            stats["passes_medium"] = self.to_numeric(self.extract_text(row, 'td[data-stat="passes_medium"]'))
            stats["passes_pct_medium"] = self.to_numeric(self.extract_text(row, 'td[data-stat="passes_pct_medium"]'))
            stats["passes_completed_long"] = self.to_numeric(self.extract_text(row, 'td[data-stat="passes_completed_long"]'))
            stats["passes_long"] = self.to_numeric(self.extract_text(row, 'td[data-stat="passes_long"]'))
            stats["passes_pct_long"] = self.to_numeric(self.extract_text(row, 'td[data-stat="passes_pct_long"]'))
            stats["passes_into_final_third"] = self.to_numeric(self.extract_text(row, 'td[data-stat="passes_into_final_third"]'))
            stats["passes_into_penalty_area"] = self.to_numeric(self.extract_text(row, 'td[data-stat="passes_into_penalty_area"]'))
            stats["crosses_into_penalty_area"] = self.to_numeric(self.extract_text(row, 'td[data-stat="crosses_into_penalty_area"]'))
            stats["progressive_passes"] = self.to_numeric(self.extract_text(row, 'td[data-stat="progressive_passes"]'))
        except (IndexError, ValueError) as e:
            self.logger.warning(f"Errore nell'estrazione statistiche passaggi: {str(e)}")
            
        return stats
    
    def _extract_defensive_stats(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Estrae statistiche difensive."""
        stats = {}
        
        # Trova tabella difesa
        def_table = soup.select_one('table#stats_defense_squads')
        if not def_table:
            return stats
            
        # Ottieni righe della tabella
        rows = def_table.select('tbody tr')
        if not rows:
            return stats
            
        # Estrai valori dalla prima riga
        try:
            row = rows[0]
            
            stats["tackles"] = self.to_numeric(self.extract_text(row, 'td[data-stat="tackles"]'))
            stats["tackles_won"] = self.to_numeric(self.extract_text(row, 'td[data-stat="tackles_won"]'))
            stats["tackles_def_3rd"] = self.to_numeric(self.extract_text(row, 'td[data-stat="tackles_def_3rd"]'))
            stats["tackles_mid_3rd"] = self.to_numeric(self.extract_text(row, 'td[data-stat="tackles_mid_3rd"]'))
            stats["tackles_att_3rd"] = self.to_numeric(self.extract_text(row, 'td[data-stat="tackles_att_3rd"]'))
            stats["blocks"] = self.to_numeric(self.extract_text(row, 'td[data-stat="blocks"]'))
            stats["blocked_shots"] = self.to_numeric(self.extract_text(row, 'td[data-stat="blocked_shots"]'))
            stats["interceptions"] = self.to_numeric(self.extract_text(row, 'td[data-stat="interceptions"]'))
            stats["clearances"] = self.to_numeric(self.extract_text(row, 'td[data-stat="clearances"]'))
            stats["errors"] = self.to_numeric(self.extract_text(row, 'td[data-stat="errors"]'))
        except (IndexError, ValueError) as e:
            self.logger.warning(f"Errore nell'estrazione statistiche difensive: {str(e)}")
            
        return stats
    
    def _extract_player_name(self, soup: BeautifulSoup) -> str:
        """Estrae il nome del giocatore."""
        name_elem = soup.select_one("h1[itemprop='name']")
        if name_elem:
            return name_elem.text.strip()
        return "Unknown Player"
    
    def _extract_player_team(self, soup: BeautifulSoup) -> str:
        """Estrae la squadra del giocatore."""
        team_elem = soup.select_one("div#meta a[href*='/squads/']")
        if team_elem:
            return team_elem.text.strip()
        return "Unknown Team"
    
    def _extract_player_summary(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Estrae sommario statistico giocatore."""
        # Implementazione semplificata, estendi con tutti i campi necessari
        summary = {}
        
        summary_div = soup.select_one("div.stats_pullout")
        if not summary_div:
            return summary
            
        # Estrai valori dalle div
        try:
            divs = summary_div.select("div.p1")
            if divs:
                summary["matches"] = self.to_numeric(self.extract_text(divs[0], "div.stat"))
                summary["starts"] = self.to_numeric(self.extract_text(divs[1], "div.stat"))
                summary["minutes"] = self.to_numeric(self.extract_text(divs[2], "div.stat"))
                
            divs = summary_div.select("div.p2")
            if divs:
                summary["goals"] = self.to_numeric(self.extract_text(divs[0], "div.stat"))
                summary["assists"] = self.to_numeric(self.extract_text(divs[1], "div.stat"))
                
            # Calcola statistiche aggiuntive
            if summary.get("minutes", 0) > 0:
                minutes = summary["minutes"]
                summary["goals_per90"] = round((summary.get("goals", 0) / minutes) * 90, 2)
                summary["assists_per90"] = round((summary.get("assists", 0) / minutes) * 90, 2)
                
        except (IndexError, ValueError) as e:
            self.logger.warning(f"Errore nell'estrazione sommario giocatore: {str(e)}")
            
        return summary
    
    def _extract_player_shooting(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Estrae statistiche tiri giocatore."""
        # Implementazione semplificata, estendi con tutti i campi necessari
        shooting = {}
        
        # Trova tabella shooting
        shoot_table = soup.select_one('table#stats_shooting')
        if not shoot_table:
            return shooting
            
        # Ottieni righe della tabella
        rows = shoot_table.select('tbody tr')
        if not rows:
            return shooting
            
        # Estrai valori dalle righe (sommando tutte le competizioni)
        try:
            shots = goals = shots_on_target = xg = npxg = 0
            
            for row in rows:
                if "Season" in self.extract_text(row, 'th[data-stat="season"]'):
                    continue
                
                shots += self.to_numeric(self.extract_text(row, 'td[data-stat="shots"]'))
                goals += self.to_numeric(self.extract_text(row, 'td[data-stat="goals"]'))
                shots_on_target += self.to_numeric(self.extract_text(row, 'td[data-stat="shots_on_target"]'))
                xg += self.to_numeric(self.extract_text(row, 'td[data-stat="xg"]'))
                npxg += self.to_numeric(self.extract_text(row, 'td[data-stat="npxg"]'))
            
            shooting["shots"] = shots
            shooting["goals"] = goals
            shooting["shots_on_target"] = shots_on_target
            shooting["xg"] = xg
            shooting["npxg"] = npxg
            
            # Calcola percentuali
            if shots > 0:
                shooting["shot_accuracy"] = round((shots_on_target / shots) * 100, 1)
                shooting["conversion_rate"] = round((goals / shots) * 100, 1)
                shooting["xg_per_shot"] = round(xg / shots, 2)
                shooting["goals_minus_xg"] = round(goals - xg, 2)
            else:
                shooting["shot_accuracy"] = 0
                shooting["conversion_rate"] = 0
                shooting["xg_per_shot"] = 0
                shooting["goals_minus_xg"] = 0
                
        except (IndexError, ValueError) as e:
            self.logger.warning(f"Errore nell'estrazione statistiche tiri giocatore: {str(e)}")
            
        return shooting
    
    def _extract_player_passing(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Estrae statistiche passaggi giocatore."""
        # Implementazione semplificata, estendi con tutti i campi necessari
        passing = {}
        
        # Trova tabella passing
        pass_table = soup.select_one('table#stats_passing')
        if not pass_table:
            return passing
            
        # Ottieni righe della tabella
        rows = pass_table.select('tbody tr')
        if not rows:
            return passing
            
        # Estrai valori dalle righe (sommando tutte le competizioni)
        try:
            passes = passes_completed = key_passes = final_third = prog_passes = 0
            
            for row in rows:
                if "Season" in self.extract_text(row, 'th[data-stat="season"]'):
                    continue
                
                passes += self.to_numeric(self.extract_text(row, 'td[data-stat="passes"]'))
                passes_completed += self.to_numeric(self.extract_text(row, 'td[data-stat="passes_completed"]'))
                key_passes += self.to_numeric(self.extract_text(row, 'td[data-stat="assisted_shots"]'))
                final_third += self.to_numeric(self.extract_text(row, 'td[data-stat="passes_into_final_third"]'))
                prog_passes += self.to_numeric(self.extract_text(row, 'td[data-stat="progressive_passes"]'))
            
            passing["passes"] = passes
            passing["passes_completed"] = passes_completed
            passing["key_passes"] = key_passes
            passing["passes_into_final_third"] = final_third
            passing["progressive_passes"] = prog_passes
            
            # Calcola percentuali
            if passes > 0:
                passing["pass_completion"] = round((passes_completed / passes) * 100, 1)
            else:
                passing["pass_completion"] = 0
                
        except (IndexError, ValueError) as e:
            self.logger.warning(f"Errore nell'estrazione statistiche passaggi giocatore: {str(e)}")
            
        return passing
    
    def _extract_player_defensive(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Estrae statistiche difensive giocatore."""
        # Implementazione semplificata, estendi con tutti i campi necessari
        defensive = {}
        
        # Trova tabella defense
        def_table = soup.select_one('table#stats_defense')
        if not def_table:
            return defensive
            
        # Ottieni righe della tabella
        rows = def_table.select('tbody tr')
        if not rows:
            return defensive
            
        # Estrai valori dalle righe (sommando tutte le competizioni)
        try:
            tackles = tackles_won = interceptions = blocks = clearances = 0
            
            for row in rows:
                if "Season" in self.extract_text(row, 'th[data-stat="season"]'):
                    continue
                
                tackles += self.to_numeric(self.extract_text(row, 'td[data-stat="tackles"]'))
                tackles_won += self.to_numeric(self.extract_text(row, 'td[data-stat="tackles_won"]'))
                interceptions += self.to_numeric(self.extract_text(row, 'td[data-stat="interceptions"]'))
                blocks += self.to_numeric(self.extract_text(row, 'td[data-stat="blocks"]'))
                clearances += self.to_numeric(self.extract_text(row, 'td[data-stat="clearances"]'))
            
            defensive["tackles"] = tackles
            defensive["tackles_won"] = tackles_won
            defensive["interceptions"] = interceptions
            defensive["blocks"] = blocks
            defensive["clearances"] = clearances
            
            # Calcola percentuali
            if tackles > 0:
                defensive["tackle_success"] = round((tackles_won / tackles) * 100, 1)
            else:
                defensive["tackle_success"] = 0
                
        except (IndexError, ValueError) as e:
            self.logger.warning(f"Errore nell'estrazione statistiche difensive giocatore: {str(e)}")
            
        return defensive
    
    def _extract_match_info(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Estrae informazioni base della partita."""
        match_info = {}
        
        try:
            # Estrai squadre
            scorebox = soup.select_one("div.scorebox")
            if scorebox:
                teams = scorebox.select("div.scorebox_meta strong a")
                if len(teams) >= 2:
                    match_info["home_team"] = teams[0].text.strip()
                    match_info["away_team"] = teams[1].text.strip()
                
                # Estrai score
                scores = scorebox.select("div.score")
                if len(scores) >= 2:
                    match_info["home_score"] = self.to_numeric(scores[0].text.strip())
                    match_info["away_score"] = self.to_numeric(scores[1].text.strip())
                
                # Estrai data
                date_elem = scorebox.select_one("div.scorebox_meta div")
                if date_elem:
                    date_text = date_elem.text.strip()
                    try:
                        # Estrai solo la data con regex
                        date_match = re.search(r'([A-Za-z]+ \d+, \d{4})', date_text)
                        if date_match:
                            match_info["date"] = date_match.group(1)
                    except:
                        match_info["date"] = date_text
                
                # Estrai competizione
                venue_div = scorebox.select("div.scorebox_meta div")
                if len(venue_div) > 1:
                    venue_links = venue_div[1].select("a")
                    for link in venue_links:
                        if "/comps/" in link.get("href", ""):
                            match_info["competition"] = link.text.strip()
                            break
        except Exception as e:
            self.logger.warning(f"Errore nell'estrazione informazioni partita: {str(e)}")
            
        return match_info
    
    def _extract_match_team_stats(self, soup: BeautifulSoup, home: bool) -> Dict[str, Any]:
        """Estrae statistiche di squadra per una partita."""
        team_stats = {}
        
        try:
            # Trova tabella statistiche
            stats_tables = soup.select('table.stats_table')
            
            # Iteriamo sulle diverse tabelle statistiche
            for table in stats_tables:
                caption = table.select_one('caption')
                if not caption:
                    continue
                
                table_name = caption.text.strip().lower()
                
                if "summary" in table_name:
                    team_stats.update(self._extract_match_summary_stats(table, home))
                elif "passing" in table_name:
                    team_stats.update(self._extract_match_passing_stats(table, home))
                elif "defense" in table_name:
                    team_stats.update(self._extract_match_defensive_stats(table, home))
                elif "possession" in table_name:
                    team_stats.update(self._extract_match_possession_stats(table, home))
        except Exception as e:
            self.logger.warning(f"Errore nell'estrazione statistiche squadra partita: {str(e)}")
            
        return team_stats
    
    def _extract_match_summary_stats(self, table: BeautifulSoup, home: bool) -> Dict[str, Any]:
        """Estrae statistiche sommario partita."""
        stats = {}
        
        try:
            # Identifica l'indice corretto per casa/trasferta
            index = 0 if home else 1
            
            # Ottieni riga della tabella
            rows = table.select('tbody tr')
            if not rows or len(rows) <= index:
                return stats
                
            row = rows[index]
            
            stats["goals"] = self.to_numeric(self.extract_text(row, 'td[data-stat="goals"]'))
            stats["assists"] = self.to_numeric(self.extract_text(row, 'td[data-stat="assists"]'))
            stats["shots_total"] = self.to_numeric(self.extract_text(row, 'td[data-stat="shots_total"]'))
            stats["shots_on_target"] = self.to_numeric(self.extract_text(row, 'td[data-stat="shots_on_target"]'))
            stats["xg"] = self.to_numeric(self.extract_text(row, 'td[data-stat="xg"]'))
            stats["xg_assist"] = self.to_numeric(self.extract_text(row, 'td[data-stat="xg_assist"]'))
        except (IndexError, ValueError) as e:
            self.logger.warning(f"Errore nell'estrazione statistiche sommario: {str(e)}")
            
        return stats
    
    def _extract_match_passing_stats(self, table: BeautifulSoup, home: bool) -> Dict[str, Any]:
        """Estrae statistiche passaggi partita."""
        stats = {}
        
        try:
            # Identifica l'indice corretto per casa/trasferta
            index = 0 if home else 1
            
            # Ottieni riga della tabella
            rows = table.select('tbody tr')
            if not rows or len(rows) <= index:
                return stats
                
            row = rows[index]
            
            stats["passes_completed"] = self.to_numeric(self.extract_text(row, 'td[data-stat="passes_completed"]'))
            stats["passes"] = self.to_numeric(self.extract_text(row, 'td[data-stat="passes"]'))
            stats["passes_pct"] = self.to_numeric(self.extract_text(row, 'td[data-stat="passes_pct"]'))
            stats["passes_progressive"] = self.to_numeric(self.extract_text(row, 'td[data-stat="progressive_passes"]'))
        except (IndexError, ValueError) as e:
            self.logger.warning(f"Errore nell'estrazione statistiche passaggi: {str(e)}")
            
        return stats
    
    def _extract_match_defensive_stats(self, table: BeautifulSoup, home: bool) -> Dict[str, Any]:
        """Estrae statistiche difensive partita."""
        stats = {}
        
        try:
            # Identifica l'indice corretto per casa/trasferta
            index = 0 if home else 1
            
            # Ottieni riga della tabella
            rows = table.select('tbody tr')
            if not rows or len(rows) <= index:
                return stats
                
            row = rows[index]
            
            stats["tackles"] = self.to_numeric(self.extract_text(row, 'td[data-stat="tackles"]'))
            stats["tackles_won"] = self.to_numeric(self.extract_text(row, 'td[data-stat="tackles_won"]'))
            stats["interceptions"] = self.to_numeric(self.extract_text(row, 'td[data-stat="interceptions"]'))
            stats["blocks"] = self.to_numeric(self.extract_text(row, 'td[data-stat="blocks"]'))
        except (IndexError, ValueError) as e:
            self.logger.warning(f"Errore nell'estrazione statistiche difensive: {str(e)}")
            
        return stats
    
    def _extract_match_possession_stats(self, table: BeautifulSoup, home: bool) -> Dict[str, Any]:
        """Estrae statistiche possesso partita."""
        stats = {}
        
        try:
            # Identifica l'indice corretto per casa/trasferta
            index = 0 if home else 1
            
            # Ottieni riga della tabella
            rows = table.select('tbody tr')
            if not rows or len(rows) <= index:
                return stats
                
            row = rows[index]
            
            stats["possession"] = self.to_numeric(self.extract_text(row, 'td[data-stat="possession"]'))
            stats["touches"] = self.to_numeric(self.extract_text(row, 'td[data-stat="touches"]'))
            stats["touches_def_pen"] = self.to_numeric(self.extract_text(row, 'td[data-stat="touches_def_pen_area"]'))
            stats["touches_def_3rd"] = self.to_numeric(self.extract_text(row, 'td[data-stat="touches_def_3rd"]'))
            stats["touches_mid_3rd"] = self.to_numeric(self.extract_text(row, 'td[data-stat="touches_mid_3rd"]'))
            stats["touches_att_3rd"] = self.to_numeric(self.extract_text(row, 'td[data-stat="touches_att_3rd"]'))
            stats["touches_att_pen"] = self.to_numeric(self.extract_text(row, 'td[data-stat="touches_att_pen_area"]'))
        except (IndexError, ValueError) as e:
            self.logger.warning(f"Errore nell'estrazione statistiche possesso: {str(e)}")
            
        return stats
    
    def _extract_match_player_stats(self, soup: BeautifulSoup) -> Dict[str, List[Dict[str, Any]]]:
        """Estrae statistiche giocatori per una partita."""
        player_stats = {
            "home": [],
            "away": []
        }
        
        try:
            # Trova tabelle statistiche giocatori
            player_tables = soup.select('table:has(caption:contains("Player"))')
            
            for table in player_tables:
                caption = table.select_one('caption')
                if not caption:
                    continue
                
                caption_text = caption.text.strip().lower()
                
                # Determina se home o away basato sul testo della caption
                is_home = True
                if any(x in caption_text for x in ["away", "visitors", "visiting"]):
                    is_home = False
                
                # Estrai statistiche giocatori
                rows = table.select('tbody tr')
                for row in rows:
                    # Verifica che sia una riga giocatore (non sommario)
                    player_name = self.extract_text(row, 'th[data-stat="player"]')
                    if not player_name or player_name.lower() in ["reserves", "team"]:
                        continue
                    
                    # Estrai dati base giocatore
                    player_data = {
                        "name": player_name,
                        "position": self.extract_text(row, 'td[data-stat="position"]'),
                        "minutes": self.to_numeric(self.extract_text(row, 'td[data-stat="minutes"]')),
                        "goals": self.to_numeric(self.extract_text(row, 'td[data-stat="goals"]'), 0),
                        "assists": self.to_numeric(self.extract_text(row, 'td[data-stat="assists"]'), 0),
                        "shots": self.to_numeric(self.extract_text(row, 'td[data-stat="shots_total"]'), 0),
                        "shots_on_target": self.to_numeric(self.extract_text(row, 'td[data-stat="shots_on_target"]'), 0),
                        "xg": self.to_numeric(self.extract_text(row, 'td[data-stat="xg"]'), 0),
                        "passes_completed": self.to_numeric(self.extract_text(row, 'td[data-stat="passes_completed"]'), 0),
                        "passes_attempted": self.to_numeric(self.extract_text(row, 'td[data-stat="passes"]'), 0),
                    }
                    
                    # Aggiungi alla lista corretta
                    if is_home:
                        player_stats["home"].append(player_data)
                    else:
                        player_stats["away"].append(player_data)
        except Exception as e:
            self.logger.warning(f"Errore nell'estrazione statistiche giocatori partita: {str(e)}")
            
        return player_stats
    
    def _extract_standings(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Estrae classifica campionato."""
        standings = []
        
        try:
            # Trova tabella classifica
            table = soup.select_one('table#results')
            if not table:
                return standings
                
            # Ottieni righe della tabella
            rows = table.select('tbody tr')
            if not rows:
                return standings
                
            # Estrai dati di ogni squadra
            for row in rows:
                # Verifica che sia una riga squadra (non titolo o spazio)
                if not row.select_one('th[data-stat="rank"]'):
                    continue
                
                try:
                    team_data = {
                        "rank": self.to_numeric(self.extract_text(row, 'th[data-stat="rank"]'), 0),
                        "team": self.extract_text(row, 'td[data-stat="squad"]'),
                        "played": self.to_numeric(self.extract_text(row, 'td[data-stat="games"]'), 0),
                        "won": self.to_numeric(self.extract_text(row, 'td[data-stat="wins"]'), 0),
                        "drawn": self.to_numeric(self.extract_text(row, 'td[data-stat="draws"]'), 0),
                        "lost": self.to_numeric(self.extract_text(row, 'td[data-stat="losses"]'), 0),
                        "goals_for": self.to_numeric(self.extract_text(row, 'td[data-stat="goals_for"]'), 0),
                        "goals_against": self.to_numeric(self.extract_text(row, 'td[data-stat="goals_against"]'), 0),
                        "goal_diff": self.to_numeric(self.extract_text(row, 'td[data-stat="goal_diff"]'), 0),
                        "points": self.to_numeric(self.extract_text(row, 'td[data-stat="points"]'), 0),
                        "xg_for": self.to_numeric(self.extract_text(row, 'td[data-stat="xg_for"]'), 0),
                        "xg_against": self.to_numeric(self.extract_text(row, 'td[data-stat="xg_against"]'), 0),
                        "xg_diff": self.to_numeric(self.extract_text(row, 'td[data-stat="xg_diff"]'), 0),
                        "attendance": self.to_numeric(self.extract_text(row, 'td[data-stat="attendance"]'), 0),
                        "top_team_scorer": self.extract_text(row, 'td[data-stat="top_team_scorer"]'),
                        "goalkeeper": self.extract_text(row, 'td[data-stat="goalkeeper"]')
                    }
                    
                    # Estrai ID team dalla URL
                    team_link = row.select_one('td[data-stat="squad"] a')
                    if team_link and 'href' in team_link.attrs:
                        team_data["team_id"] = self._extract_team_id_from_url(team_link['href'])
                    
                    standings.append(team_data)
                except Exception as e:
                    self.logger.warning(f"Errore nell'estrazione dati squadra: {str(e)}")
                    continue
        except Exception as e:
            self.logger.warning(f"Errore nell'estrazione classifica: {str(e)}")
            
        return standings
    
    def _extract_team_id_from_url(self, url: str) -> Optional[str]:
        """Estrae ID squadra da URL."""
        try:
            # Formato URL: /en/squads/b8fd03ef/Team-Name
            match = re.search(r'/squads/([a-zA-Z0-9]+)/', url)
            if match:
                return match.group(1)
        except Exception:
            pass
        return None

def get_team_stats(team_id: str, season: str = "2023-2024") -> Optional[Dict[str, Any]]:
    """
    Ottiene statistiche di squadra complete da FBref.
    
    Args:
        team_id: Identificatore squadra in FBref (es. "fd962109")
        season: Stagione (formato "YYYY-YYYY")
        
    Returns:
        Dizionario con statistiche o None se errore
    """
    scraper = FBrefScraper()
    return scraper.get_team_stats(team_id, season)

def get_player_stats(player_id: str, season: str = "2023-2024") -> Optional[Dict[str, Any]]:
    """
    Ottiene statistiche di giocatore complete da FBref.
    
    Args:
        player_id: Identificatore giocatore in FBref
        season: Stagione (formato "YYYY-YYYY")
        
    Returns:
        Dizionario con statistiche o None se errore
    """
    scraper = FBrefScraper()
    return scraper.get_player_stats(player_id, season)

def get_match_stats(match_id: str) -> Optional[Dict[str, Any]]:
    """
    Ottiene statistiche complete per una partita da FBref.
    
    Args:
        match_id: Identificatore partita in FBref
        
    Returns:
        Dizionario con statistiche o None se errore
    """
    scraper = FBrefScraper()
    return scraper.get_match_stats(match_id)

def get_scraper() -> FBrefScraper:
    """Restituisce un'istanza dello scraper FBref.
    
    Returns:
        FBrefScraper: Un'istanza configurata dello scraper FBref
    """
    return FBrefScraper()

    
