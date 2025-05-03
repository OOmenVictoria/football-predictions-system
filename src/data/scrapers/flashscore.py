"""
Scraper per Flashscore (anche conosciuto come Livescore.in).

Questo modulo fornisce funzionalità per estrarre dati da Flashscore.com,
incluse partite, risultati, statistiche e quote.
"""

import re
import json
import time
import logging
import datetime
from typing import Dict, List, Any, Optional, Union, Tuple
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from src.data.scrapers.base_scraper import BaseScraper
from src.utils.time_utils import parse_date

# Configurazione logger
logger = logging.getLogger(__name__)

class FlashScoreScraper(BaseScraper):
    """
    Scraper per il sito Flashscore.com.
    
    Permette di estrarre dati su partite, risultati, statistiche e quote da Flashscore,
    una delle fonti più complete di informazioni calcistiche in tempo reale.
    """
    
    BASE_URL = "https://www.flashscore.com"
    
    def __init__(self):
        """Inizializza lo scraper per Flashscore."""
        super().__init__(
            name="flashscore",
            base_url=self.BASE_URL,
            cache_ttl=3600,  # Cache per 1 ora
            respect_robots=True,
            delay_range=(1.0, 3.0)  # Una richiesta ogni 1-3 secondi
        )
    
    def get_matches_by_date(self, date: str) -> List[Dict[str, Any]]:
        """
        Ottiene le partite programmate per una data specifica.
        
        Args:
            date: Data nel formato 'YYYY-MM-DD'.
        
        Returns:
            Lista di partite programmate per la data specificata.
        """
        try:
            # Converti la data nel formato richiesto da Flashscore (YYYYMMDD)
            if isinstance(date, str):
                try:
                    date_obj = datetime.datetime.strptime(date, "%Y-%m-%d")
                    formatted_date = date_obj.strftime("%Y%m%d")
                except ValueError:
                    # Gestisci formato data non valido
                    logger.error(f"Formato data non valido: {date}, deve essere 'YYYY-MM-DD'")
                    return []
            else:
                logger.error(f"Formato data non valido: {date}, deve essere una stringa 'YYYY-MM-DD'")
                return []
                
            url = f"{self.base_url}/matches/{formatted_date}/"
            
            logger.info(f"Ottenimento partite per la data {date} da Flashscore")
            html = self.get(url)
            soup = self.parse(html)
            
            matches = []
            match_blocks = soup.select('div.event__match')
            
            for match in match_blocks:
                try:
                    match_id = match.get('id', '').replace('g_1_', '')
                    
                    # Estrazione squadre
                    home_team = match.select_one('.event__participant--home')
                    away_team = match.select_one('.event__participant--away')
                    
                    # Estrazione orario
                    time_element = match.select_one('.event__time')
                    match_time = time_element.text.strip() if time_element else ""
                    
                    # Estrazione competizione
                    tournament_element = match.find_previous('div', class_='event__header')
                    tournament = tournament_element.select_one('.event__title').text.strip() if tournament_element else ""
                    
                    # Stato della partita
                    status_element = match.select_one('.event__stage')
                    status = status_element.text.strip() if status_element else ""
                    
                    # Risultato (se disponibile)
                    home_score_element = match.select_one('.event__score--home')
                    away_score_element = match.select_one('.event__score--away')
                    
                    home_score = home_score_element.text.strip() if home_score_element else None
                    away_score = away_score_element.text.strip() if away_score_element else None
                    
                    match_data = {
                        'id': match_id,
                        'home_team': home_team.text.strip() if home_team else "",
                        'away_team': away_team.text.strip() if away_team else "",
                        'time': match_time,
                        'tournament': tournament,
                        'status': status,
                        'url': f"{self.base_url}/match/{match_id}/#/match-summary",
                        'date': date
                    }
                    
                    if home_score is not None and away_score is not None:
                        match_data['score'] = {
                            'home': home_score,
                            'away': away_score
                        }
                    
                    matches.append(match_data)
                    
                except Exception as e:
                    logger.error(f"Errore nell'elaborazione di una partita: {e}")
                    continue
            
            logger.info(f"Trovate {len(matches)} partite per la data {date}")
            return matches
            
        except Exception as e:
            logger.error(f"Errore nell'ottenere partite per la data {date}: {e}")
            return []
    
    def get_match_details(self, match_id: str) -> Dict[str, Any]:
        """
        Ottiene dettagli completi per una partita specifica.
        
        Args:
            match_id: ID della partita su Flashscore.
        
        Returns:
            Dizionario con dettagli completi della partita.
        """
        try:
            url = f"{self.base_url}/match/{match_id}/#/match-summary"
            
            logger.info(f"Ottenimento dettagli per la partita {match_id}")
            html = self.get(url)
            soup = self.parse(html)
            
            # Estrazione informazioni di base
            home_team = soup.select_one('.team-name.home')
            away_team = soup.select_one('.team-name.away')
            
            tournament_element = soup.select_one('.tournamentHeader__name')
            tournament = tournament_element.text.strip() if tournament_element else ""
            
            # Data e orario
            date_element = soup.select_one('.duelParticipant__startTime')
            match_datetime = date_element.text.strip() if date_element else ""
            
            # Stato della partita
            status_element = soup.select_one('.status-text')
            status = status_element.text.strip() if status_element else ""
            
            # Risultato
            home_score_element = soup.select_one('.detailScore__wrapper span:first-child')
            away_score_element = soup.select_one('.detailScore__wrapper span:last-child')
            
            home_score = home_score_element.text.strip() if home_score_element else None
            away_score = away_score_element.text.strip() if away_score_element else None
            
            # Costruzione oggetto base
            match_details = {
                'id': match_id,
                'home_team': home_team.text.strip() if home_team else "",
                'away_team': away_team.text.strip() if away_team else "",
                'tournament': tournament,
                'datetime': match_datetime,
                'status': status,
                'url': url
            }
            
            if home_score is not None and away_score is not None:
                match_details['score'] = {
                    'home': home_score,
                    'away': away_score
                }
            
            # Aggiungi statistiche se disponibili
            match_details['statistics'] = self._extract_match_statistics(soup)
            
            # Aggiungi eventi (gol, cartellini, etc.)
            match_details['events'] = self._extract_match_events(soup)
            
            # Aggiungi formazioni se disponibili
            lineups = self._extract_match_lineups(match_id)
            if lineups:
                match_details['lineups'] = lineups
            
            # Aggiungi quote se disponibili
            odds = self._extract_match_odds(match_id)
            if odds:
                match_details['odds'] = odds
            
            logger.info(f"Ottenuti dettagli completi per la partita {match_id}")
            return match_details
            
        except Exception as e:
            logger.error(f"Errore nell'ottenere dettagli per la partita {match_id}: {e}")
            return {'id': match_id, 'error': str(e)}
    
    def _extract_match_statistics(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Estrae le statistiche della partita.
        
        Args:
            soup: Oggetto BeautifulSoup della pagina della partita.
        
        Returns:
            Dizionario con statistiche della partita.
        """
        statistics = {}
        
        try:
            stats_container = soup.select_one('.statsList')
            if not stats_container:
                return statistics
            
            stat_rows = stats_container.select('.stat')
            
            for row in stat_rows:
                try:
                    # Nome della statistica
                    stat_name_element = row.select_one('.statCategoryName')
                    if not stat_name_element:
                        continue
                    
                    stat_name = stat_name_element.text.strip().lower().replace(' ', '_')
                    
                    # Valori delle statistiche
                    home_value_element = row.select_one('.statHomeValue')
                    away_value_element = row.select_one('.statAwayValue')
                    
                    if not home_value_element or not away_value_element:
                        continue
                    
                    home_value_text = home_value_element.text.strip()
                    away_value_text = away_value_element.text.strip()
                    
                    # Converte in numeri quando possibile
                    try:
                        home_value = int(home_value_text) if home_value_text.isdigit() else home_value_text
                        away_value = int(away_value_text) if away_value_text.isdigit() else away_value_text
                    except:
                        home_value = home_value_text
                        away_value = away_value_text
                    
                    statistics[stat_name] = {
                        'home': home_value,
                        'away': away_value
                    }
                    
                except Exception as e:
                    logger.error(f"Errore nell'elaborazione di una statistica: {e}")
                    continue
            
            return statistics
            
        except Exception as e:
            logger.error(f"Errore nell'estrazione delle statistiche: {e}")
            return {}
    
    def _extract_match_events(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """
        Estrae gli eventi della partita (gol, cartellini, sostituzioni).
        
        Args:
            soup: Oggetto BeautifulSoup della pagina della partita.
        
        Returns:
            Lista di eventi della partita.
        """
        events = []
        
        try:
            events_container = soup.select('.matchEventsContainer .detailMS__incidentRow')
            
            for event in events_container:
                try:
                    # Tipo di evento
                    event_icons = event.select('.detailMS__incidentIcon svg')
                    event_type = ""
                    for icon in event_icons:
                        icon_class = icon.get('class', '')
                        if 'goal' in str(icon_class) or 'soccer-ball' in str(icon_class):
                            event_type = "goal"
                            break
                        elif 'card-yellow' in str(icon_class):
                            event_type = "yellow_card"
                            break
                        elif 'card-red' in str(icon_class) or 'card-yellow-red' in str(icon_class):
                            event_type = "red_card"
                            break
                        elif 'substitution' in str(icon_class):
                            event_type = "substitution"
                            break
                    
                    if not event_type:
                        continue
                    
                    # Minuto dell'evento
                    time_element = event.select_one('.detailMS__incidentTime')
                    minute = time_element.text.strip() if time_element else ""
                    
                    # Giocatore coinvolto
                    player_element = event.select_one('.detailMS__incidentName')
                    player = player_element.text.strip() if player_element else ""
                    
                    # Squadra (home o away)
                    is_home = 'parHome' in event.get('class', [])
                    team_side = 'home' if is_home else 'away'
                    
                    # Per le sostituzioni, ottieni anche il giocatore uscente
                    sub_out_player = ""
                    if event_type == "substitution":
                        sub_out_element = event.select_one('.substitutionOut')
                        sub_out_player = sub_out_element.text.strip() if sub_out_element else ""
                    
                    # Descrizione aggiuntiva (es. tipo di gol)
                    description_element = event.select_one('.detailMS__incidentText')
                    description = description_element.text.strip() if description_element else ""
                    
                    event_data = {
                        'type': event_type,
                        'minute': minute,
                        'player': player,
                        'team': team_side,
                        'description': description
                    }
                    
                    if event_type == "substitution" and sub_out_player:
                        event_data['player_out'] = sub_out_player
                    
                    events.append(event_data)
                    
                except Exception as e:
                    logger.error(f"Errore nell'elaborazione di un evento: {e}")
                    continue
            
            return events
            
        except Exception as e:
            logger.error(f"Errore nell'estrazione degli eventi: {e}")
            return []
    
    def _extract_match_lineups(self, match_id: str) -> Dict[str, Any]:
        """
        Estrae le formazioni delle squadre.
        
        Args:
            match_id: ID della partita.
        
        Returns:
            Dizionario con formazioni delle squadre.
        """
        try:
            url = f"{self.base_url}/match/{match_id}/#/lineups/lineups"
            
            html = self.get(url)
            soup = self.parse(html)
            
            lineups = {
                'home': {'starting': [], 'substitutes': []},
                'away': {'starting': [], 'substitutes': []}
            }
            
            # Estrazione titolari
            for team_side in ['home', 'away']:
                team_container = soup.select_one(f'.lineup--{team_side}')
                if not team_container:
                    continue
                
                # Titolari
                starters = team_container.select('.lineup__player')
                for player in starters:
                    player_name = player.select_one('.lineup__playerName')
                    player_number = player.select_one('.lineup__playerJerseyNumber')
                    
                    if player_name:
                        player_data = {
                            'name': player_name.text.strip(),
                            'number': player_number.text.strip() if player_number else "",
                            'position': ""  # Se disponibile
                        }
                        
                        lineups[team_side]['starting'].append(player_data)
                
                # Panchinari
                subs_container = soup.select_one(f'.lineup__substitutions--{team_side}')
                if subs_container:
                    substitutes = subs_container.select('.lineup__player')
                    for player in substitutes:
                        player_name = player.select_one('.lineup__playerName')
                        player_number = player.select_one('.lineup__playerJerseyNumber')
                        
                        if player_name:
                            player_data = {
                                'name': player_name.text.strip(),
                                'number': player_number.text.strip() if player_number else "",
                                'position': ""  # Se disponibile
                            }
                            
                            lineups[team_side]['substitutes'].append(player_data)
            
            return lineups
            
        except Exception as e:
            logger.error(f"Errore nell'estrazione delle formazioni per la partita {match_id}: {e}")
            return {}
    
    def _extract_match_odds(self, match_id: str) -> Dict[str, Any]:
        """
        Estrae le quote per la partita.
        
        Args:
            match_id: ID della partita.
        
        Returns:
            Dizionario con quote della partita.
        """
        try:
            url = f"{self.base_url}/match/{match_id}/#/odds-comparison/1x2"
            
            html = self.get(url)
            soup = self.parse(html)
            
            odds = {
                '1x2': {},
                'over_under': {},
                'btts': {}
            }
            
            # Estrazione quote 1X2
            odds_table = soup.select_one('.oddsTabSwithcer__panel--1')
            if odds_table:
                odds_rows = odds_table.select('tbody tr')
                for row in odds_rows:
                    bookmaker_element = row.select_one('.oddsCell__bookmakerPart')
                    home_odds_element = row.select_one('.odds__col--1')
                    draw_odds_element = row.select_one('.odds__col--X')
                    away_odds_element = row.select_one('.odds__col--2')
                    
                    if bookmaker_element and home_odds_element and draw_odds_element and away_odds_element:
                        bookmaker = bookmaker_element.text.strip()
                        home_odds = home_odds_element.text.strip()
                        draw_odds = draw_odds_element.text.strip()
                        away_odds = away_odds_element.text.strip()
                        
                        odds['1x2'][bookmaker] = {
                            'home': float(home_odds) if home_odds else None,
                            'draw': float(draw_odds) if draw_odds else None,
                            'away': float(away_odds) if away_odds else None
                        }
            
            # Estrazione quote Over/Under
            url_ou = f"{self.base_url}/match/{match_id}/#/odds-comparison/over-under"
            html_ou = self.get(url_ou)
            soup_ou = self.parse(html_ou)
            
            odds_table_ou = soup_ou.select_one('.oddsTabSwithcer__panel--1')
            if odds_table_ou:
                odds_rows = odds_table_ou.select('tbody tr')
                for row in odds_rows:
                    bookmaker_element = row.select_one('.oddsCell__bookmakerPart')
                    over_odds_element = row.select_one('.oddsCell__odds--over')
                    under_odds_element = row.select_one('.oddsCell__odds--under')
                    line_element = row.select_one('.oddsCell__noOddsCell')
                    
                    if bookmaker_element and over_odds_element and under_odds_element:
                        bookmaker = bookmaker_element.text.strip()
                        over_odds = over_odds_element.text.strip()
                        under_odds = under_odds_element.text.strip()
                        line = line_element.text.strip() if line_element else "2.5"  # Default
                        
                        if line not in odds['over_under']:
                            odds['over_under'][line] = {}
                        
                        odds['over_under'][line][bookmaker] = {
                            'over': float(over_odds) if over_odds else None,
                            'under': float(under_odds) if under_odds else None
                        }
            
            return odds
            
        except Exception as e:
            logger.error(f"Errore nell'estrazione delle quote per la partita {match_id}: {e}")
            return {}
    
    def get_team_matches(self, team_name: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Ottiene le partite recenti per una squadra.
        
        Args:
            team_name: Nome della squadra.
            limit: Numero massimo di partite da restituire.
        
        Returns:
            Lista di partite della squadra.
        """
        try:
            # Prima ottieni la pagina della squadra
            team_url = self._search_team(team_name)
            if not team_url:
                logger.error(f"Squadra non trovata: {team_name}")
                return []
            
            # Poi recupera le partite dalla pagina della squadra
            url = f"{team_url}/results/"
            
            logger.info(f"Ottenimento partite per la squadra {team_name}")
            html = self.get(url)
            soup = self.parse(html)
            
            matches = []
            match_blocks = soup.select('.event__match')
            
            for i, match in enumerate(match_blocks):
                if i >= limit:
                    break
                
                try:
                    match_id = match.get('id', '').replace('g_1_', '')
                    
                    # Estrazione squadre
                    home_team = match.select_one('.event__participant--home')
                    away_team = match.select_one('.event__participant--away')
                    
                    # Estrazione data
                    date_element = match.find_previous('div', class_='event__time')
                    match_date = date_element.text.strip() if date_element else ""
                    
                    # Estrazione competizione
                    tournament_element = match.find_previous('div', class_='event__header')
                    tournament = tournament_element.select_one('.event__title').text.strip() if tournament_element else ""
                    
                    # Risultato
                    home_score_element = match.select_one('.event__score--home')
                    away_score_element = match.select_one('.event__score--away')
                    
                    home_score = home_score_element.text.strip() if home_score_element else None
                    away_score = away_score_element.text.strip() if away_score_element else None
                    
                    match_data = {
                        'id': match_id,
                        'home_team': home_team.text.strip() if home_team else "",
                        'away_team': away_team.text.strip() if away_team else "",
                        'date': match_date,
                        'tournament': tournament,
                        'url': f"{self.base_url}/match/{match_id}/#/match-summary"
                    }
                    
                    if home_score is not None and away_score is not None:
                        match_data['score'] = {
                            'home': home_score,
                            'away': away_score
                        }
                    
                    matches.append(match_data)
                    
                except Exception as e:
                    logger.error(f"Errore nell'elaborazione di una partita: {e}")
                    continue
            
            logger.info(f"Trovate {len(matches)} partite per la squadra {team_name}")
            return matches
            
        except Exception as e:
            logger.error(f"Errore nell'ottenere partite per la squadra {team_name}: {e}")
            return []
    
    def get_league_matches(self, league_name: str, season: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Ottiene le partite di un campionato.
        
        Args:
            league_name: Nome del campionato.
            season: Stagione (es. "2023-2024").
        
        Returns:
            Lista di partite del campionato.
        """
        try:
            # Prima ottieni la pagina del campionato
            league_url = self._search_league(league_name)
            if not league_url:
                logger.error(f"Campionato non trovato: {league_name}")
                return []
            
            # Se specificata la stagione, modifica l'URL
            if season:
                league_url = f"{league_url}/archive/{season}/"
            
            # Recupera le partite dalla pagina del campionato
            url = f"{league_url}/fixtures/"
            
            logger.info(f"Ottenimento partite per il campionato {league_name}")
            html = self.get(url)
            soup = self.parse(html)
            
            matches = []
            match_blocks = soup.select('.event__match')
            
            for match in match_blocks:
                try:
                    match_id = match.get('id', '').replace('g_1_', '')
                    
                    # Estrazione squadre
                    home_team = match.select_one('.event__participant--home')
                    away_team = match.select_one('.event__participant--away')
                    
                    # Estrazione data
                    date_header = match.find_previous('div', class_='event__header')
                    date_text = date_header.select_one('.event__title').text.strip() if date_header else ""
                    
                    # Estrazione orario
                    time_element = match.select_one('.event__time')
                    match_time = time_element.text.strip() if time_element else ""
                    
                    # Stato della partita
                    status_element = match.select_one('.event__stage')
                    status = status_element.text.strip() if status_element else ""
                    
                    match_data = {
                        'id': match_id,
                        'home_team': home_team.text.strip() if home_team else "",
                        'away_team': away_team.text.strip() if away_team else "",
                        'date': date_text,
                        'time': match_time,
                        'status': status,
                        'tournament': league_name,
                        'url': f"{self.base_url}/match/{match_id}/#/match-summary"
                    }
                    
                    matches.append(match_data)
                    
                except Exception as e:
                    logger.error(f"Errore nell'elaborazione di una partita: {e}")
                    continue
            
            logger.info(f"Trovate {len(matches)} partite per il campionato {league_name}")
            return matches
            
        except Exception as e:
            logger.error(f"Errore nell'ottenere partite per il campionato {league_name}: {e}")
            return []
    
    def get_team_stats(self, team_name: str) -> Dict[str, Any]:
        """
        Ottiene statistiche dettagliate per una squadra.
        
        Args:
            team_name: Nome della squadra.
        
        Returns:
            Dizionario con statistiche della squadra.
        """
        try:
            # Prima ottieni la pagina della squadra
            team_url = self._search_team(team_name)
            if not team_url:
                logger.error(f"Squadra non trovata: {team_name}")
                return {}
            
            # Poi recupera le statistiche dalla pagina della squadra
            url = f"{team_url}/standings/"
            
            logger.info(f"Ottenimento statistiche per la squadra {team_name}")
            html = self.get(url)
            soup = self.parse(html)
            
            stats = {
                'team_name': team_name,
                'leagues': []
            }
            
            # Estrazione statistiche per ogni campionato
            standings_blocks = soup.select('.tableWrapper')
            
            for block in standings_blocks:
                try:
                    # Nome del campionato
                    league_header = block.find_previous('div', class_='tournament')
                    league_name = league_header.select_one('.tournamentHeader__name').text.strip() if league_header else "Unknown"
                    
                    # Trova la riga della squadra
                    team_row = block.select_one('tr.rowHighlighted')
                    if not team_row:
                        continue
                    
                    # Estrazione dati dalla riga
                    position = team_row.select_one('.table__cell--rank')
                    matches_played = team_row.select_one('.table__cell--matches_played')
                    wins = team_row.select_one('.table__cell--wins')
                    draws = team_row.select_one('.table__cell--draws')
                    losses = team_row.select_one('.table__cell--losses')
                    goals_for = team_row.select_one('.table__cell--goals_for')
                    goals_against = team_row.select_one('.table__cell--goals_against')
                    points = team_row.select_one('.table__cell--points')
                    
                    league_stats = {
                        'league': league_name,
                        'position': position.text.strip() if position else "",
                        'matches_played': matches_played.text.strip() if matches_played else "",
                        'wins': wins.text.strip() if wins else "",
                        'draws': draws.text.strip() if draws else "",
                        'losses': losses.text.strip() if losses else "",
                        'goals_for': goals_for.text.strip() if goals_for else "",
                        'goals_against': goals_against.text.strip() if goals_against else "",
                        'points': points.text.strip() if points else ""
                    }
                    
                    # Converti in numeri quando possibile
                    for key, value in league_stats.items():
                        if key != 'league' and value.isdigit():
                            league_stats[key] = int(value)
                    
                    stats['leagues'].append(league_stats)
                    
                except Exception as e:
                    logger.error(f"Errore nell'elaborazione di un campionato: {e}")
                    continue
            
            # Recupera anche le ultime partite per form
            try:
                recent_matches = self.get_team_matches(team_name, limit=5)
                form = []
                
                for match in recent_matches:
                    is_home = match.get('home_team') == team_name
                    score = match.get('score', {})
                    
                    if score:
                        home_score = int(score.get('home', 0))
                        away_score = int(score.get('away', 0))
                        
                        if is_home:
                            if home_score > away_score:
                                form.append('W')
                            elif home_score < away_score:
                                form.append('L')
                            else:
                                form.append('D')
                        else:
                            if away_score > home_score:
                                form.append('W')
                            elif away_score < home_score:
                                form.append('L')
                            else:
                                form.append('D')
                
                stats['form'] = ''.join(form)
                stats['recent_matches'] = recent_matches
                
            except Exception as e:
                logger.error(f"Errore nell'ottenere la forma della squadra: {e}")
            
            return stats
            
        except Exception as e:
            logger.error(f"Errore nell'ottenere statistiche per la squadra {team_name}: {e}")
            return {'team_name': team_name, 'error': str(e)}
    
    def _search_team(self, team_name: str) -> Optional[str]:
        """
        Cerca una squadra su Flashscore per nome.
        
        Args:
            team_name: Nome della squadra da cercare.
        
        Returns:
            URL della pagina della squadra se trovata, altrimenti None.
        """
        try:
            # Normalizza il nome della squadra
            normalized_name = team_name.lower().replace(' ', '-')
            
            # Prima prova la ricerca diretta (metodo più veloce)
            direct_url = f"{self.base_url}/team/{normalized_name}/"
            response = self.session.head(direct_url)
            
            if response.status_code == 200:
                return direct_url
            
            # Altrimenti usa la ricerca
            search_url = f"{self.base_url}/search/?q={team_name}"
            html = self.get(search_url)
            soup = self.parse(html)
            
            # Cerca risultati di tipo "team"
            team_results = soup.select('.searchTeam')
            
            if team_results:
                team_link = team_results[0].select_one('a')
                if team_link:
                    return urljoin(self.base_url, team_link['href'])
            
            return None
            
        except Exception as e:
            logger.error(f"Errore nella ricerca della squadra {team_name}: {e}")
            return None
    
    def _search_league(self, league_name: str) -> Optional[str]:
        """
        Cerca un campionato su Flashscore per nome.
        
        Args:
            league_name: Nome del campionato da cercare.
        
        Returns:
            URL della pagina del campionato se trovata, altrimenti None.
        """
        try:
            # Normalizza il nome del campionato
            normalized_name = league_name.lower().replace(' ', '-')
            
            # Prima prova la ricerca diretta (metodo più veloce)
            direct_url = f"{self.base_url}/tournament/{normalized_name}/"
            response = self.session.head(direct_url)
            
            if response.status_code == 200:
                return direct_url
            
            # Altrimenti usa la ricerca
            search_url = f"{self.base_url}/search/?q={league_name}"
            html = self.get(search_url)
            soup = self.parse(html)
            
            # Cerca risultati di tipo "tournament"
            league_results = soup.select('.searchResult__section--tournament .searchResult__item')
            
            if league_results:
                league_link = league_results[0].select_one('a')
                if league_link:
                    return urljoin(self.base_url, league_link['href'])
            
            return None
            
        except Exception as e:
            logger.error(f"Errore nella ricerca del campionato {league_name}: {e}")
            return None


# Istanza globale per un utilizzo più semplice
flashscore_scraper = FlashScoreScraper()

def get_scraper():
    """
    Ottiene l'istanza globale dello scraper.
    
    Returns:
        Istanza di FlashScoreScraper.
    """
    return flashscore_scraper
