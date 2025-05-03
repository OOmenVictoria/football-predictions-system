"""
Modulo per l'estrazione di statistiche da FootyStats.
Questo modulo fornisce funzionalità per estrarre statistiche, pronostici e
dati storici dal sito FootyStats.
"""
import os
import re
import json
import time
import logging
from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

from src.data.scrapers.base_scraper import BaseScraper
from src.utils.cache import cached
from src.utils.database import FirebaseManager
from src.config.settings import get_setting

logger = logging.getLogger(__name__)

class FootyStatsScraper(BaseScraper):
    """
    Scraper per estrarre statistiche e dati analitici da FootyStats.
    
    FootyStats fornisce statistiche dettagliate per vari campionati, squadre e giocatori,
    con particolare attenzione alle metriche per il betting e i pronostici.
    """
    
    def __init__(self):
        """Inizializza lo scraper FootyStats."""
        super().__init__()
        self.db = FirebaseManager()
        self.base_url = "https://footystats.org"
        self.min_wait_time = get_setting('scrapers.footystats.min_wait_time', 2)
        self.max_retries = get_setting('scrapers.footystats.max_retries', 3)
        
        # Lista di campionati supportati con i rispettivi ID
        self.leagues_map = {
            "premier_league": "england/premier-league",
            "championship": "england/championship",
            "serie_a": "italy/serie-a",
            "serie_b": "italy/serie-b",
            "la_liga": "spain/la-liga",
            "bundesliga": "germany/bundesliga",
            "ligue_1": "france/ligue-1",
            "eredivisie": "netherlands/eredivisie",
            "primeira_liga": "portugal/primeira-liga",
            "super_lig": "turkey/super-lig",
            # Aggiungi altri campionati supportati
        }
        
        logger.info("FootyStatsScraper inizializzato")
    
    @cached(ttl=86400)  # 24 ore
    def get_league_table(self, league_id: str, season: Optional[str] = None) -> Dict[str, Any]:
        """
        Ottiene la classifica corrente di un campionato.
        
        Args:
            league_id: ID del campionato (chiave da leagues_map)
            season: Stagione richiesta (es. "2023-2024", opzionale)
            
        Returns:
            Classifica completa con statistiche per squadra
        """
        logger.info(f"Richiesta classifica per {league_id}, stagione {season}")
        
        # Ottieni URL del campionato
        league_path = self.leagues_map.get(league_id)
        if not league_path:
            logger.warning(f"Campionato {league_id} non supportato")
            return {}
        
        # Costruisci URL
        url = f"{self.base_url}/leagues/{league_path}"
        if season:
            url += f"/{season}"
        
        try:
            # Ottieni pagina del campionato
            soup = self._get_soup(url)
            
            # Estrai metadati campionato
            league_info = self._extract_league_info(soup)
            
            # Estrai tabella classifica
            table_data = self._extract_league_table(soup)
            
            # Combinazione risultati
            result = {
                "league_id": league_id,
                "season": season or league_info.get("current_season", ""),
                "name": league_info.get("name", ""),
                "country": league_info.get("country", ""),
                "standings": table_data,
                "last_updated": datetime.now().isoformat()
            }
            
            return result
        
        except Exception as e:
            logger.error(f"Errore nell'estrazione della classifica per {league_id}: {str(e)}")
            return {}
    
    def _extract_league_info(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Estrae informazioni generali sul campionato.
        
        Args:
            soup: Oggetto BeautifulSoup della pagina
            
        Returns:
            Informazioni sul campionato
        """
        league_info = {}
        
        # Estrai nome campionato e paese
        title_elem = soup.select_one('h1.league-title')
        if title_elem:
            title_text = title_elem.text.strip()
            # Split title into country and league
            parts = title_text.split(' - ')
            if len(parts) > 1:
                league_info['country'] = parts[0].strip()
                league_info['name'] = parts[1].strip()
            else:
                league_info['name'] = title_text
        
        # Estrai stagione corrente
        season_elem = soup.select_one('div.season-selector span.selected')
        if season_elem:
            league_info['current_season'] = season_elem.text.strip()
        
        # Estrai altre info se disponibili
        info_box = soup.select_one('div.league-info-box')
        if info_box:
            info_items = info_box.select('div.info-item')
            for item in info_items:
                label = item.select_one('span.label')
                value = item.select_one('span.value')
                if label and value:
                    key = label.text.strip().lower().replace(' ', '_')
                    league_info[key] = value.text.strip()
        
        return league_info
    
    def _extract_league_table(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """
        Estrae la tabella di classifica.
        
        Args:
            soup: Oggetto BeautifulSoup della pagina
            
        Returns:
            Lista di squadre con posizione e statistiche
        """
        standings = []
        
        # Trova tabella classifica
        table = soup.select_one('table.league-table')
        if not table:
            logger.warning("Tabella classifica non trovata")
            return standings
        
        # Ottieni intestazioni colonne
        headers = []
        header_row = table.select_one('thead tr')
        if header_row:
            for th in header_row.select('th'):
                header = th.text.strip().lower().replace(' ', '_')
                # Pulizia intestazioni
                header = header.replace('#', 'position')
                headers.append(header)
        
        # Estrai righe delle squadre
        rows = table.select('tbody tr')
        for row in rows:
            team_data = {}
            
            # Estrai dati dalle celle
            cells = row.select('td')
            for i, cell in enumerate(cells):
                if i < len(headers):
                    header = headers[i]
                    
                    # Gestione speciale per alcune colonne
                    if header == 'team':
                        team_link = cell.select_one('a')
                        if team_link:
                            team_data['team_name'] = team_link.text.strip()
                            href = team_link.get('href', '')
                            team_data['team_url'] = href
                            # Estrai ID team dal URL
                            team_id_match = re.search(r'/team/([^/]+)', href)
                            if team_id_match:
                                team_data['team_id'] = team_id_match.group(1)
                    else:
                        value = cell.text.strip()
                        # Converti in numerico quando possibile
                        team_data[header] = self._parse_numeric(value)
            
            if team_data:
                standings.append(team_data)
        
        return standings
    
    @cached(ttl=43200)  # 12 ore
    def get_team_stats(self, team_id: str, league_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Ottiene statistiche dettagliate per una squadra.
        
        Args:
            team_id: ID della squadra o nome normalizzato
            league_id: ID del campionato (opzionale)
            
        Returns:
            Statistiche complete della squadra
        """
        logger.info(f"Richiesta statistiche per team {team_id}")
        
        # Controllo se è un nome o ID
        is_name = '/' not in team_id
        
        try:
            # Costruisci URL
            if is_name:
                # Normalizza nome squadra
                team_name = team_id.lower().replace(' ', '-').replace('.', '')
                url = f"{self.base_url}/team/{team_name}"
            else:
                url = f"{self.base_url}{team_id}"
            
            # Aggiungi campionato se specificato
            if league_id and league_id in self.leagues_map:
                league_path = self.leagues_map[league_id]
                url += f"/{league_path}"
            
            # Ottieni pagina squadra
            soup = self._get_soup(url)
            
            # Estrai informazioni squadra
            team_info = self._extract_team_info(soup)
            
            # Estrai statistiche generali
            general_stats = self._extract_team_general_stats(soup)
            
            # Estrai statistiche avanzate
            advanced_stats = self._extract_team_advanced_stats(soup)
            
            # Estrai forma recente
            form_data = self._extract_team_form(soup)
            
            # Estrai ultime partite
            recent_matches = self._extract_team_matches(soup)
            
            # Combinazione risultati
            result = {
                "team_id": team_id,
                "name": team_info.get("name", ""),
                "league": team_info.get("league", ""),
                "league_id": league_id or team_info.get("league_id", ""),
                "season": team_info.get("season", ""),
                "general_stats": general_stats,
                "advanced_stats": advanced_stats,
                "form": form_data,
                "recent_matches": recent_matches,
                "last_updated": datetime.now().isoformat()
            }
            
            return result
        
        except Exception as e:
            logger.error(f"Errore nell'estrazione statistiche per team {team_id}: {str(e)}")
            return {}
    
    def _extract_team_info(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Estrae informazioni generali sulla squadra.
        
        Args:
            soup: Oggetto BeautifulSoup della pagina
            
        Returns:
            Informazioni sulla squadra
        """
        team_info = {}
        
        # Estrai nome squadra
        title_elem = soup.select_one('h1.team-title')
        if title_elem:
            team_info['name'] = title_elem.text.strip()
        
        # Estrai campionato
        league_elem = soup.select_one('div.team-league a')
        if league_elem:
            team_info['league'] = league_elem.text.strip()
            href = league_elem.get('href', '')
            # Estrai ID campionato dal URL
            league_id_match = re.search(r'/leagues/([^/]+)', href)
            if league_id_match:
                team_info['league_id'] = league_id_match.group(1)
        
        # Estrai stagione
        season_elem = soup.select_one('div.season-selector span.selected')
        if season_elem:
            team_info['season'] = season_elem.text.strip()
        
        # Estrai altre info se disponibili
        info_box = soup.select_one('div.team-info-box')
        if info_box:
            info_items = info_box.select('div.info-item')
            for item in info_items:
                label = item.select_one('span.label')
                value = item.select_one('span.value')
                if label and value:
                    key = label.text.strip().lower().replace(' ', '_')
                    team_info[key] = value.text.strip()
        
        return team_info
    
    def _extract_team_general_stats(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Estrae statistiche generali della squadra.
        
        Args:
            soup: Oggetto BeautifulSoup della pagina
            
        Returns:
            Statistiche generali della squadra
        """
        stats = {}
        
        # Trova tabella statistiche generali
        tables = soup.select('table.team-stats-table')
        for table in tables:
            caption = table.select_one('caption')
            if not caption:
                continue
                
            table_name = caption.text.strip().lower().replace(' ', '_')
            stats[table_name] = {}
            
            rows = table.select('tbody tr')
            for row in rows:
                # Nome statistica
                label_cell = row.select_one('td.label')
                if not label_cell:
                    continue
                    
                stat_name = label_cell.text.strip().lower().replace(' ', '_')
                
                # Valore statistica
                value_cell = row.select_one('td.value')
                if not value_cell:
                    continue
                    
                value = self._parse_numeric(value_cell.text.strip())
                
                stats[table_name][stat_name] = value
        
        return stats
    
    def _extract_team_advanced_stats(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Estrae statistiche avanzate della squadra.
        
        Args:
            soup: Oggetto BeautifulSoup della pagina
            
        Returns:
            Statistiche avanzate della squadra
        """
        stats = {}
        
        # Trova sezione statistiche avanzate
        advanced_section = soup.select_one('div.advanced-stats-section')
        if not advanced_section:
            return stats
            
        # Estrai tabelle di statistiche avanzate
        tables = advanced_section.select('table.advanced-table')
        for table in tables:
            # Ottieni titolo tabella
            title_elem = table.select_one('caption') or table.select_one('th.table-title')
            if not title_elem:
                continue
                
            table_name = title_elem.text.strip().lower().replace(' ', '_')
            stats[table_name] = {}
            
            # Estrai righe
            rows = table.select('tr:not(.header-row)')
            for row in rows:
                # Salta righe senza dati
                cells = row.select('td')
                if len(cells) < 2:
                    continue
                
                # Estrai nome e valore
                if len(cells) == 2:
                    # Layout nome + valore
                    name_cell, value_cell = cells
                    stat_name = name_cell.text.strip().lower().replace(' ', '_')
                    value = self._parse_numeric(value_cell.text.strip())
                    stats[table_name][stat_name] = value
                else:
                    # Layout con categoria o altre colonne
                    for i, cell in enumerate(cells):
                        header = table.select('th')[i].text.strip().lower().replace(' ', '_')
                        if header:
                            value = self._parse_numeric(cell.text.strip())
                            stats[table_name][header] = value
        
        return stats
    
    def _extract_team_form(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Estrae dati sulla forma recente della squadra.
        
        Args:
            soup: Oggetto BeautifulSoup della pagina
            
        Returns:
            Dati sulla forma della squadra
        """
        form_data = {
            "form_string": "",
            "last_5": [],
            "home_form": [],
            "away_form": []
        }
        
        # Estrai forma generale
        form_div = soup.select_one('div.team-form')
        if form_div:
            form_items = form_div.select('span.form-item')
            form_string = ""
            form_list = []
            
            for item in form_items:
                result = item.text.strip()
                form_string += result
                
                # Converti risultato in W/D/L
                normalized = ""
                if result == "W":
                    normalized = "W"
                elif result == "D":
                    normalized = "D"
                elif result == "L":
                    normalized = "L"
                
                # Aggiungi a lista se valido
                if normalized:
                    form_list.append(normalized)
            
            form_data["form_string"] = form_string
            form_data["last_5"] = form_list[:5]  # Ultimi 5 risultati
        
        # Estrai forma casa/trasferta
        home_away_div = soup.select_one('div.home-away-form')
        if home_away_div:
            # Forma in casa
            home_div = home_away_div.select_one('div.home-form')
            if home_div:
                home_items = home_div.select('span.form-item')
                for item in home_items:
                    result = item.text.strip()
                    if result in ["W", "D", "L"]:
                        form_data["home_form"].append(result)
            
            # Forma in trasferta
            away_div = home_away_div.select_one('div.away-form')
            if away_div:
                away_items = away_div.select('span.form-item')
                for item in away_items:
                    result = item.text.strip()
                    if result in ["W", "D", "L"]:
                        form_data["away_form"].append(result)
        
        return form_data
    
    def _extract_team_matches(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """
        Estrae le ultime partite giocate dalla squadra.
        
        Args:
            soup: Oggetto BeautifulSoup della pagina
            
        Returns:
            Lista delle ultime partite
        """
        matches = []
        
        # Trova tabella partite recenti
        table = soup.select_one('table.fixtures-table')
        if not table:
            return matches
        
        # Estrai righe partite
        rows = table.select('tbody tr')
        for row in rows:
            match_data = {}
            
            # Data partita
            date_cell = row.select_one('td.date')
            if date_cell:
                match_data['date'] = date_cell.text.strip()
            
            # Competizione
            comp_cell = row.select_one('td.competition')
            if comp_cell:
                match_data['competition'] = comp_cell.text.strip()
            
            # Squadre e risultato
            home_cell = row.select_one('td.home-team')
            away_cell = row.select_one('td.away-team')
            score_cell = row.select_one('td.score')
            
            if home_cell:
                match_data['home_team'] = home_cell.text.strip()
            
            if away_cell:
                match_data['away_team'] = away_cell.text.strip()
            
            if score_cell:
                score_text = score_cell.text.strip()
                match_data['score'] = score_text
                
                # Estrai risultato numerico
                score_parts = score_text.split('-')
                if len(score_parts) == 2:
                    try:
                        home_score = int(score_parts[0].strip())
                        away_score = int(score_parts[1].strip())
                        match_data['home_score'] = home_score
                        match_data['away_score'] = away_score
                    except ValueError:
                        pass
            
            # Esito (W/D/L)
            result_cell = row.select_one('td.result')
            if result_cell:
                match_data['result'] = result_cell.text.strip()
            
            # Altri dati statistici se disponibili
            for cell in row.select('td'):
                cell_class = cell.get('class', [])
                if len(cell_class) > 0:
                    stat_name = cell_class[0]
                    if stat_name not in ['date', 'competition', 'home-team', 'away-team', 'score', 'result']:
                        match_data[stat_name] = self._parse_numeric(cell.text.strip())
            
            matches.append(match_data)
        
        return matches
    
    @cached(ttl=43200)  # 12 ore
    def get_head_to_head(self, team1_id: str, team2_id: str) -> Dict[str, Any]:
        """
        Ottiene statistiche testa a testa tra due squadre.
        
        Args:
            team1_id: ID o nome della prima squadra
            team2_id: ID o nome della seconda squadra
            
        Returns:
            Statistiche testa a testa complete
        """
        logger.info(f"Richiesta H2H per {team1_id} vs {team2_id}")
        
        # Normalizza nomi squadre se necessario
        team1_name = team1_id.lower().replace(' ', '-').replace('.', '') if '/' not in team1_id else team1_id
        team2_name = team2_id.lower().replace(' ', '-').replace('.', '') if '/' not in team2_id else team2_id
        
        try:
            # Costruisci URL testa a testa
            url = f"{self.base_url}/teams/{team1_name}-vs-{team2_name}"
            
            # Ottieni pagina H2H
            soup = self._get_soup(url)
            
            # Estrai informazioni generali
            h2h_info = self._extract_h2h_info(soup)
            
            # Estrai statistiche H2H
            h2h_stats = self._extract_h2h_stats(soup)
            
            # Estrai partite precedenti
            past_matches = self._extract_h2h_matches(soup)
            
            # Combinazione risultati
            result = {
                "team1_id": team1_id,
                "team2_id": team2_id,
                "team1_name": h2h_info.get("team1_name", ""),
                "team2_name": h2h_info.get("team2_name", ""),
                "total_matches": h2h_info.get("total_matches", 0),
                "stats": h2h_stats,
                "past_matches": past_matches,
                "last_updated": datetime.now().isoformat()
            }
            
            return result
        
        except Exception as e:
            logger.error(f"Errore nell'estrazione H2H per {team1_id} vs {team2_id}: {str(e)}")
            return {}
    
    def _extract_h2h_info(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Estrae informazioni generali sul confronto testa a testa.
        
        Args:
            soup: Oggetto BeautifulSoup della pagina
            
        Returns:
            Informazioni sul confronto H2H
        """
        h2h_info = {}
        
        # Estrai nomi squadre
        title_elem = soup.select_one('h1.h2h-title')
        if title_elem:
            title_text = title_elem.text.strip()
            teams = title_text.split(' vs ')
            if len(teams) == 2:
                h2h_info['team1_name'] = teams[0].strip()
                h2h_info['team2_name'] = teams[1].strip()
        
        # Estrai numero totale di partite
        total_matches_elem = soup.select_one('div.total-matches')
        if total_matches_elem:
            total_text = total_matches_elem.text.strip()
            matches_match = re.search(r'(\d+)\s+matches', total_text)
            if matches_match:
                h2h_info['total_matches'] = int(matches_match.group(1))
        
        return h2h_info
    
    def _extract_h2h_stats(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Estrae statistiche dettagliate del confronto testa a testa.
        
        Args:
            soup: Oggetto BeautifulSoup della pagina
            
        Returns:
            Statistiche del confronto H2H
        """
        stats = {
            "wins": {
                "team1": 0,
                "team2": 0,
                "draws": 0
            },
            "goals": {
                "team1": 0,
                "team2": 0,
                "per_match": 0,
                "both_teams_scored": 0
            },
            "cards": {
                "yellow_per_match": 0,
                "red_per_match": 0
            },
            "over_under": {}
        }
        
        # Estrai statistiche vittorie
        wins_section = soup.select_one('div.h2h-record')
        if wins_section:
            win_elements = wins_section.select('div.record-item')
            for elem in win_elements:
                label = elem.select_one('div.label')
                value = elem.select_one('div.value')
                
                if label and value:
                    label_text = label.text.strip().lower()
                    value_num = self._parse_numeric(value.text.strip())
                    
                    if "team 1 wins" in label_text:
                        stats["wins"]["team1"] = value_num
                    elif "team 2 wins" in label_text:
                        stats["wins"]["team2"] = value_num
                    elif "draws" in label_text:
                        stats["wins"]["draws"] = value_num
        
        # Estrai statistiche gol
        goals_section = soup.select_one('div.goals-stats')
        if goals_section:
            goal_elements = goals_section.select('div.stat-item')
            for elem in goal_elements:
                label = elem.select_one('div.label')
                value = elem.select_one('div.value')
                
                if label and value:
                    label_text = label.text.strip().lower()
                    value_num = self._parse_numeric(value.text.strip())
                    
                    if "team 1 goals" in label_text:
                        stats["goals"]["team1"] = value_num
                    elif "team 2 goals" in label_text:
                        stats["goals"]["team2"] = value_num
                    elif "goals per match" in label_text:
                        stats["goals"]["per_match"] = value_num
                    elif "btts" in label_text or "both teams" in label_text:
                        if "%" in value.text:
                            stats["goals"]["both_teams_scored"] = value_num
        
        # Estrai statistiche cartellini
        cards_section = soup.select_one('div.cards-stats')
        if cards_section:
            card_elements = cards_section.select('div.stat-item')
            for elem in card_elements:
                label = elem.select_one('div.label')
                value = elem.select_one('div.value')
                
                if label and value:
                    label_text = label.text.strip().lower()
                    value_num = self._parse_numeric(value.text.strip())
                    
                    if "yellow" in label_text:
                        stats["cards"]["yellow_per_match"] = value_num
                    elif "red" in label_text:
                        stats["cards"]["red_per_match"] = value_num
        
        # Estrai statistiche over/under
        ou_section = soup.select_one('div.over-under-stats')
        if ou_section:
            ou_elements = ou_section.select('div.stat-item')
            for elem in ou_elements:
                label = elem.select_one('div.label')
                value = elem.select_one('div.value')
                
                if label and value:
                    label_text = label.text.strip().lower()
                    value_text = value.text.strip()
                    
                    # Estrai linea over/under
                    ou_match = re.search(r'over ([\d\.]+)', label_text)
                    if ou_match:
                        line = ou_match.group(1)
                        percentage = self._parse_numeric(value_text)
                        stats["over_under"][f"over_{line}"] = percentage
                        stats["over_under"][f"under_{line}"] = 100 - percentage
        
        return stats
    
    def _extract_h2h_matches(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """
        Estrae le partite precedenti tra le due squadre.
        
        Args:
            soup: Oggetto BeautifulSoup della pagina
            
        Returns:
            Lista delle partite precedenti
        """
        matches = []
        
        # Trova tabella partite precedenti
        table = soup.select_one('table.past-matches')
        if not table:
            return matches
        
        # Estrai righe partite
        rows = table.select('tbody tr')
        for row in rows:
            match_data = {}
            
            # Data partita
            date_cell = row.select_one('td.date')
            if date_cell:
                match_data['date'] = date_cell.text.strip()
            
            # Competizione
            comp_cell = row.select_one('td.competition')
            if comp_cell:
                match_data['competition'] = comp_cell.text.strip()
            
            # Squadre e risultato
            home_cell = row.select_one('td.home-team')
            away_cell = row.select_one('td.away-team')
            score_cell = row.select_one('td.score')
            
            if home_cell:
                match_data['home_team'] = home_cell.text.strip()
            
            if away_cell:
                match_data['away_team'] = away_cell.text.strip()
            
            if score_cell:
                score_text = score_

def get_team_stats(team_id: str, season: str = "2023-2024") -> Optional[Dict[str, Any]]:
    """
    Ottiene statistiche di squadra complete da FootyStats.
    
    Args:
        team_id: Identificatore squadra in FootyStats
        season: Stagione (formato "YYYY-YYYY")
        
    Returns:
        Dizionario con statistiche o None se errore
    """
    scraper = FootyStatsScraper()
    return scraper.get_team_stats(team_id, season)
