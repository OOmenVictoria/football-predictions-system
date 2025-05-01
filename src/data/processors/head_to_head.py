"""
Processore per i dati degli scontri diretti (head-to-head).

Questo modulo gestisce l'analisi e l'elaborazione degli scontri diretti tra squadre,
standardizzando i dati provenienti da diverse fonti e calcolando statistiche rilevanti.
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union, Tuple
from collections import Counter

# API ufficiali
from src.data.api.football_data import get_api as get_football_data_api
from src.data.api.api_football import get_api as get_api_football

# Scraper per siti con statistiche
from src.data.scrapers.flashscore import get_scraper as get_flashscore
from src.data.stats.fbref import get_scraper as get_fbref
from src.data.stats.sofascore import get_scraper as get_sofascore

# Altri scraper
from src.data.scrapers.transfermarkt import get_scraper as get_transfermarkt
from src.data.scrapers.worldfootball import get_scraper as get_worldfootball
from src.data.scrapers.eleven_v_eleven import get_scraper as get_eleven_v_eleven

from src.data.processors.teams import get_processor as get_team_processor
from src.data.processors.matches import get_processor as get_match_processor

from src.utils.database import FirebaseManager
from src.utils.cache import cached
from src.config.sources import get_sources_for_data_type

# Configurazione logger
logger = logging.getLogger(__name__)

class HeadToHeadProcessor:
    """
    Processore per analizzare e standardizzare i dati degli scontri diretti.
    
    Gestisce il recupero, la fusione e l'analisi degli scontri diretti tra due squadre,
    con calcolo di statistiche e trend storici.
    """
    
    def __init__(self, db: Optional[FirebaseManager] = None):
        """
        Inizializza il processore degli scontri diretti.
        
        Args:
            db: Istanza di FirebaseManager. Se None, ne verrà creata una nuova.
        """
        self.db = db or FirebaseManager()
        
        # API ufficiali
        self.football_data_api = get_football_data_api()
        self.api_football = get_api_football()
        
        # Scraper
        self.flashscore = get_flashscore()
        self.fbref = get_fbref()
        self.sofascore = get_sofascore()
        self.transfermarkt = get_transfermarkt()
        self.worldfootball = get_worldfootball()
        self.eleven_v_eleven = get_eleven_v_eleven()
        
        # Altri processori
        self.team_processor = get_team_processor()
        self.match_processor = get_match_processor()
        
        # Cache per gli scontri diretti
        self.h2h_cache = {}
    
    def get_head_to_head(
        self, 
        team1_id: str, 
        team2_id: str, 
        max_matches: int = 10,
        min_matches: int = 1,
        sources: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Ottiene lo storico degli scontri diretti tra due squadre.
        
        Args:
            team1_id: ID della prima squadra.
            team2_id: ID della seconda squadra.
            max_matches: Numero massimo di partite da restituire.
            min_matches: Numero minimo di partite richieste.
            sources: Lista delle fonti da utilizzare. Se None, usa tutte.
            
        Returns:
            Dizionario con i dati degli scontri diretti e le statistiche associate.
        """
        cache_key = f"{team1_id}-{team2_id}"
        reverse_cache_key = f"{team2_id}-{team1_id}"
        
        # Controlla la cache
        if cache_key in self.h2h_cache:
            return self.h2h_cache[cache_key]
        if reverse_cache_key in self.h2h_cache:
            h2h_data = self.h2h_cache[reverse_cache_key]
            # Inverti i ruoli delle squadre
            return self._reverse_h2h_data(h2h_data)
        
        # Controlla il database
        stored_h2h = self.get_stored_h2h(team1_id, team2_id)
        if stored_h2h:
            # Verifica se i dati sono abbastanza recenti (ultimi 7 giorni)
            last_updated = datetime.fromisoformat(stored_h2h.get('last_updated', '2000-01-01'))
            if (datetime.now() - last_updated).days < 7:
                self.h2h_cache[cache_key] = stored_h2h
                return stored_h2h
        
        # Ottieni i dati delle squadre
        team1_data = self.team_processor.get_stored_team_data(team1_id)
        team2_data = self.team_processor.get_stored_team_data(team2_id)
        
        if not team1_data or not team2_data:
            logger.error(f"Impossibile trovare i dati per le squadre {team1_id} e/o {team2_id}")
            return {
                'team1_id': team1_id,
                'team2_id': team2_id,
                'matches': [],
                'stats': {},
                'error': 'Team data not found'
            }
        
        # Imposta le fonti
        if not sources:
            sources = get_sources_for_data_type('head_to_head')
        
        # Inizializza la struttura di dati
        h2h_data = {
            'team1_id': team1_id,
            'team2_id': team2_id,
            'team1_name': team1_data.get('name', ''),
            'team2_name': team2_data.get('name', ''),
            'matches': [],
            'stats': {},
            'trends': {},
            'last_updated': datetime.now().isoformat()
        }
        
        # Recupera gli scontri diretti da tutte le fonti
        all_matches = []
        
        for source in sources:
            source_matches = self._get_h2h_from_source(
                team1_data=team1_data,
                team2_data=team2_data,
                source=source
            )
            
            if source_matches:
                # Processa ogni partita
                for match in source_matches:
                    processed_match = self.match_processor.process_match(match, source)
                    all_matches.append(processed_match)
        
        # Rimuovi duplicati (stesse partite da fonti diverse)
        deduplicated_matches = self._deduplicate_matches(all_matches)
        
        # Limita al numero massimo di partite
        matches = deduplicated_matches[:max_matches]
        
        # Se non ci sono abbastanza partite, restituisci errore
        if len(matches) < min_matches:
            logger.warning(f"Insufficienti scontri diretti trovati tra {team1_id} e {team2_id}: {len(matches)}")
            return {
                'team1_id': team1_id,
                'team2_id': team2_id,
                'team1_name': team1_data.get('name', ''),
                'team2_name': team2_data.get('name', ''),
                'matches': matches,
                'stats': {},
                'trends': {},
                'error': 'Insufficient head-to-head data',
                'last_updated': datetime.now().isoformat()
            }
        
        # Imposta le partite
        h2h_data['matches'] = matches
        
        # Calcola le statistiche
        h2h_data['stats'] = self._calculate_h2h_stats(matches, team1_id, team2_id)
        
        # Calcola i trend
        h2h_data['trends'] = self._calculate_h2h_trends(matches, team1_id, team2_id)
        
        # Salva i dati nel database
        self.store_h2h_data(h2h_data)
        
        # Aggiorna la cache
        self.h2h_cache[cache_key] = h2h_data
        
        return h2h_data
    
    def _get_h2h_from_source(
        self, 
        team1_data: Dict[str, Any], 
        team2_data: Dict[str, Any],
        source: str
    ) -> List[Dict[str, Any]]:
        """
        Ottiene gli scontri diretti da una specifica fonte.
        
        Args:
            team1_data: Dati della prima squadra.
            team2_data: Dati della seconda squadra.
            source: Nome della fonte.
            
        Returns:
            Lista di partite tra le due squadre.
        """
        try:
            # Ottieni gli ID delle squadre per questa fonte
            team1_source_id = self._get_team_source_id(team1_data, source)
            team2_source_id = self._get_team_source_id(team2_data, source)
            
            # Se non abbiamo gli ID per questa fonte, salta
            if not team1_source_id or not team2_source_id:
                return []
            
            # Ottieni gli scontri diretti in base alla fonte
            if source == 'football_data':
                return self._get_h2h_football_data(team1_source_id, team2_source_id)
            elif source == 'api_football':
                return self._get_h2h_api_football(team1_source_id, team2_source_id)
            elif source == 'flashscore':
                return self._get_h2h_flashscore(team1_data.get('name', ''), team2_data.get('name', ''))
            elif source == 'fbref':
                return self._get_h2h_fbref(team1_source_id, team2_source_id)
            elif source == 'sofascore':
                return self._get_h2h_sofascore(team1_source_id, team2_source_id)
            elif source == 'transfermarkt':
                return self._get_h2h_transfermarkt(team1_source_id, team2_source_id)
            elif source == 'worldfootball':
                return self._get_h2h_worldfootball(team1_data.get('name', ''), team2_data.get('name', ''))
            elif source == 'eleven_v_eleven':
                return self._get_h2h_eleven_v_eleven(team1_data.get('name', ''), team2_data.get('name', ''))
            else:
                logger.warning(f"Fonte non supportata per scontri diretti: {source}")
                return []
                
        except Exception as e:
            logger.error(f"Errore nel recuperare gli scontri diretti da {source}: {e}")
            return []
    
    def _get_team_source_id(self, team_data: Dict[str, Any], source: str) -> Optional[str]:
        """
        Ottiene l'ID di una squadra per una specifica fonte.
        
        Args:
            team_data: Dati della squadra.
            source: Nome della fonte.
            
        Returns:
            ID della squadra per la fonte specificata, o None se non disponibile.
        """
        if 'source_ids' in team_data and source in team_data['source_ids']:
            return team_data['source_ids'][source]
        return None
    
    def _get_h2h_football_data(self, team1_id: str, team2_id: str) -> List[Dict[str, Any]]:
        """
        Ottiene gli scontri diretti da Football-Data API.
        
        Args:
            team1_id: ID della prima squadra.
            team2_id: ID della seconda squadra.
            
        Returns:
            Lista di partite tra le due squadre.
        """
        try:
            matches = self.football_data_api.get_match_head_to_head(
                int(team1_id), 
                int(team2_id),
                limit=20
            )
            return matches
        except Exception as e:
            logger.error(f"Errore nel recuperare gli scontri diretti da Football-Data: {e}")
            return []
    
    def _get_h2h_api_football(self, team1_id: str, team2_id: str) -> List[Dict[str, Any]]:
        """
        Ottiene gli scontri diretti da API-Football.
        
        Args:
            team1_id: ID della prima squadra.
            team2_id: ID della seconda squadra.
            
        Returns:
            Lista di partite tra le due squadre.
        """
        try:
            matches = self.api_football.get_match_head_to_head(
                int(team1_id), 
                int(team2_id),
                limit=20
            )
            return matches
        except Exception as e:
            logger.error(f"Errore nel recuperare gli scontri diretti da API-Football: {e}")
            return []
    
    def _get_h2h_flashscore(self, team1_name: str, team2_name: str) -> List[Dict[str, Any]]:
        """
        Ottiene gli scontri diretti da Flashscore.
        
        Args:
            team1_name: Nome della prima squadra.
            team2_name: Nome della seconda squadra.
            
        Returns:
            Lista di partite tra le due squadre.
        """
        try:
            matches = self.flashscore.get_match_head_to_head(team1_name, team2_name, limit=20)
            return matches
        except Exception as e:
            logger.error(f"Errore nel recuperare gli scontri diretti da Flashscore: {e}")
            return []
    
    def _get_h2h_fbref(self, team1_id: str, team2_id: str) -> List[Dict[str, Any]]:
        """
        Ottiene gli scontri diretti da FBref.
        
        Args:
            team1_id: ID della prima squadra.
            team2_id: ID della seconda squadra.
            
        Returns:
            Lista di partite tra le due squadre.
        """
        try:
            matches = self.fbref.get_head_to_head_matches(team1_id, team2_id, limit=20)
            return matches
        except Exception as e:
            logger.error(f"Errore nel recuperare gli scontri diretti da FBref: {e}")
            return []
    
    def _get_h2h_sofascore(self, team1_id: str, team2_id: str) -> List[Dict[str, Any]]:
        """
        Ottiene gli scontri diretti da SofaScore.
        
        Args:
            team1_id: ID della prima squadra.
            team2_id: ID della seconda squadra.
            
        Returns:
            Lista di partite tra le due squadre.
        """
        try:
            matches = self.sofascore.get_head_to_head_matches(team1_id, team2_id, limit=20)
            return matches
        except Exception as e:
            logger.error(f"Errore nel recuperare gli scontri diretti da SofaScore: {e}")
            return []
    
    def _get_h2h_transfermarkt(self, team1_id: str, team2_id: str) -> List[Dict[str, Any]]:
        """
        Ottiene gli scontri diretti da Transfermarkt.
        
        Args:
            team1_id: ID della prima squadra.
            team2_id: ID della seconda squadra.
            
        Returns:
            Lista di partite tra le due squadre.
        """
        try:
            matches = self.transfermarkt.get_head_to_head_matches(team1_id, team2_id, limit=20)
            return matches
        except Exception as e:
            logger.error(f"Errore nel recuperare gli scontri diretti da Transfermarkt: {e}")
            return []
    
    def _get_h2h_worldfootball(self, team1_name: str, team2_name: str) -> List[Dict[str, Any]]:
        """
        Ottiene gli scontri diretti da WorldFootball.
        
        Args:
            team1_name: Nome della prima squadra.
            team2_name: Nome della seconda squadra.
            
        Returns:
            Lista di partite tra le due squadre.
        """
        try:
            matches = self.worldfootball.get_head_to_head_matches(team1_name, team2_name, limit=20)
            return matches
        except Exception as e:
            logger.error(f"Errore nel recuperare gli scontri diretti da WorldFootball: {e}")
            return []
    
    def _get_h2h_eleven_v_eleven(self, team1_name: str, team2_name: str) -> List[Dict[str, Any]]:
        """
        Ottiene gli scontri diretti da 11v11.
        
        Args:
            team1_name: Nome della prima squadra.
            team2_name: Nome della seconda squadra.
            
        Returns:
            Lista di partite tra le due squadre.
        """
        try:
            matches = self.eleven_v_eleven.get_head_to_head_matches(team1_name, team2_name, limit=20)
            return matches
        except Exception as e:
            logger.error(f"Errore nel recuperare gli scontri diretti da 11v11: {e}")
            return []
    
    def _deduplicate_matches(self, matches: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Rimuove i duplicati dalle partite.
        
        Args:
            matches: Lista di partite potenzialmente duplicate.
            
        Returns:
            Lista di partite senza duplicati.
        """
        # Struttura per tenere traccia delle partite uniche
        unique_matches = {}
        
        for match in matches:
            # Estrai la data e le squadre per identificare univocamente la partita
            match_date = match.get('datetime', '').split('T')[0] if 'datetime' in match else ''
            if not match_date:
                continue
                
            home_team = match.get('home_team', {}).get('name', '')
            away_team = match.get('away_team', {}).get('name', '')
            
            if not home_team or not away_team:
                continue
            
            match_key = f"{match_date}_{home_team}_{away_team}"
            
            # Se la partita non è già presente o se la nuova ha più dati
            if match_key not in unique_matches or self._has_more_data(match, unique_matches[match_key]):
                unique_matches[match_key] = match
        
        # Converti il dizionario in lista e ordina per data (dalla più recente)
        result = list(unique_matches.values())
        result.sort(key=lambda m: m.get('datetime', ''), reverse=True)
        
        return result
    
    def _has_more_data(self, match1: Dict[str, Any], match2: Dict[str, Any]) -> bool:
        """
        Verifica se la prima partita ha più dati della seconda.
        
        Args:
            match1: Prima partita.
            match2: Seconda partita.
            
        Returns:
            True se la prima partita ha più dati, False altrimenti.
        """
        # Confronta la presenza di chiavi importanti
        important_keys = ['statistics', 'lineups', 'events', 'venue']
        score1 = sum(1 for key in important_keys if key in match1 and match1[key])
        score2 = sum(1 for key in important_keys if key in match2 and match2[key])
        
        return score1 > score2
    
    def _calculate_h2h_stats(
        self, 
        matches: List[Dict[str, Any]], 
        team1_id: str, 
        team2_id: str
    ) -> Dict[str, Any]:
        """
        Calcola le statistiche degli scontri diretti.
        
        Args:
            matches: Lista di partite tra le due squadre.
            team1_id: ID della prima squadra.
            team2_id: ID della seconda squadra.
            
        Returns:
            Dizionario con le statistiche degli scontri diretti.
        """
        stats = {
            'total_matches': len(matches),
            'team1_wins': 0,
            'team2_wins': 0,
            'draws': 0,
            'team1_goals': 0,
            'team2_goals': 0,
            'avg_goals': 0.0,
            'btts_count': 0,
            'over_2_5_count': 0,
            'clean_sheets_team1': 0,
            'clean_sheets_team2': 0,
            'first_goal_team1': 0,
            'first_goal_team2': 0,
            'home_wins_team1': 0,
            'away_wins_team1': 0,
            'home_wins_team2': 0,
            'away_wins_team2': 0
        }
        
        if not matches:
            return stats
        
        # Conteggi vittorie, gol, etc.
        total_goals = 0
        
        for match in matches:
            # Identifica quale squadra è quale
            home_team_id = match.get('home_team', {}).get('id', '')
            away_team_id = match.get('away_team', {}).get('id', '')
            
            is_team1_home = home_team_id == team1_id
            is_team2_home = home_team_id == team2_id
            
            # Punteggio
            home_score = match.get('score', {}).get('home')
            away_score = match.get('score', {}).get('away')
            
            if home_score is None or away_score is None:
                continue
            
            # Aggiorna gol totali
            stats['team1_goals'] += home_score if is_team1_home else away_score
            stats['team2_goals'] += home_score if is_team2_home else away_score
            
            total_goals += home_score + away_score
            
            # Determinazione del risultato
            if home_score > away_score:  # Vittoria in casa
                if is_team1_home:
                    stats['team1_wins'] += 1
                    stats['home_wins_team1'] += 1
                else:
                    stats['team2_wins'] += 1
                    stats['home_wins_team2'] += 1
            elif away_score > home_score:  # Vittoria in trasferta
                if is_team1_home:
                    stats['team2_wins'] += 1
                    stats['away_wins_team2'] += 1
                else:
                    stats['team1_wins'] += 1
                    stats['away_wins_team1'] += 1
            else:  # Pareggio
                stats['draws'] += 1
            
            # Both Teams To Score (BTTS)
            if home_score > 0 and away_score > 0:
                stats['btts_count'] += 1
            
            # Over 2.5
            if home_score + away_score > 2.5:
                stats['over_2_5_count'] += 1
            
            # Clean sheets
            if home_score == 0:
                if is_team1_home:
                    stats['clean_sheets_team2'] += 1
                else:
                    stats['clean_sheets_team1'] += 1
            if away_score == 0:
                if is_team1_home:
                    stats['clean_sheets_team1'] += 1
                else:
                    stats['clean_sheets_team2'] += 1
            
            # Primo gol (se disponibile nei dati degli eventi)
            if 'events' in match and match['events']:
                first_goal = self._find_first_goal(match['events'])
                if first_goal:
                    team_side = first_goal.get('team', '')
                    if team_side == 'home' and is_team1_home:
                        stats['first_goal_team1'] += 1
                    elif team_side == 'home' and is_team2_home:
                        stats['first_goal_team2'] += 1
                    elif team_side == 'away' and is_team1_home:
                        stats['first_goal_team2'] += 1
                    elif team_side == 'away' and is_team2_home:
                        stats['first_goal_team1'] += 1
        
        # Calcola media gol
        stats['avg_goals'] = round(total_goals / stats['total_matches'], 2) if stats['total_matches'] > 0 else 0.0
        
        # Converti conteggi in percentuali
        stats['btts_percentage'] = round(stats['btts_count'] / stats['total_matches'] * 100, 1) if stats['total_matches'] > 0 else 0.0
        stats['over_2_5_percentage'] = round(stats['over_2_5_count'] / stats['total_matches'] * 100, 1) if stats['total_matches'] > 0 else 0.0
        
        # Risultati in percentuale
        total_decided = stats['team1_wins'] + stats['team2_wins'] + stats['draws']
        if total_decided > 0:
            stats['team1_win_percentage'] = round(stats['team1_wins'] / total_decided * 100, 1)
            stats['team2_win_percentage'] = round(stats['team2_wins'] / total_decided * 100, 1)
            stats['draw_percentage'] = round(stats['draws'] / total_decided * 100, 1)
        
        return stats
    
    def _find_first_goal(self, events: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """
        Trova il primo gol negli eventi della partita.
        
        Args:
            events: Lista di eventi della partita.
            
        Returns:
            Evento del primo gol, o None se non trovato.
        """
        # Ordina gli eventi per minuto
        goal_events = [e for e in events if e.get('type') == 'goal']
        
        if not goal_events:
            return None
        
        # Ordina per minuto
        sorted_goals = sorted(goal_events, key=lambda e: int(e.get('minute', '90').split('+')[0]))
        
        return sorted_goals[0] if sorted_goals else None
    
    def _calculate_h2h_trends(
        self, 
        matches: List[Dict[str, Any]], 
        team1_id: str, 
        team2_id: str
    ) -> Dict[str, Any]:
        """
        Calcola i trend negli scontri diretti.
        
        Args:
            matches: Lista di partite tra le due squadre.
            team1_id: ID della prima squadra.
            team2_id: ID della seconda squadra.
            
        Returns:
            Dizionario con i trend degli scontri diretti.
        """
        trends = {
            'recent_form': {
                'team1': [],
                'team2': []
            },
            'goals_trend': {
                'team1': [],
                'team2': []
            },
            'competitions': Counter(),
            'scoring_minutes': {
                'team1': {},
                'team2': {}
            },
            'referees': Counter()
        }
        
        if not matches:
            return trends
        
        # Estrai solo le partite con punteggi validi
        valid_matches = [m for m in matches if 
                        'score' in m and 
                        m['score'].get('home') is not None and 
                        m['score'].get('away') is not None]
        
        # Ultimi 5 risultati
        recent_matches = valid_matches[:5]
        
        for match in recent_matches:
            # Identifica quale squadra è quale
            home_team_id = match.get('home_team', {}).get('id', '')
            away_team_id = match.get('away_team', {}).get('id', '')
            
            is_team1_home = home_team_id == team1_id
            
            # Punteggio
            home_score = match.get('score', {}).get('home')
            away_score = match.get('score', {}).get('away')
            
            # Determina il risultato dal punto di vista di ciascuna squadra
            if is_team1_home:
                # Team1 in casa
                if home_score > away_score:
                    trends['recent_form']['team1'].append('W')
                    trends['recent_form']['team2'].append('L')
                elif home_score < away_score:
                    trends['recent_form']['team1'].append('L')
                    trends['recent_form']['team2'].append('W')
                else:
                    trends['recent_form']['team1'].append('D')
                    trends['recent_form']['team2'].append('D')
                
                # Gol segnati
                trends['goals_trend']['team1'].append(home_score)
                trends['goals_trend']['team2'].append(away_score)
            else:
                # Team1 in trasferta
                if away_score > home_score:
                    trends['recent_form']['team1'].append('W')
                    trends['recent_form']['team2'].append('L')
                elif away_score < home_score:
                    trends['recent_form']['team1'].append('L')
                    trends['recent_form']['team2'].append('W')
                else:
                    trends['recent_form']['team1'].append('D')
                    trends['recent_form']['team2'].append('D')
                
                # Gol segnati
                trends['goals_trend']['team1'].append(away_score)
                trends['goals_trend']['team2'].append(home_score)
            
            # Competizioni
            if 'competition' in match and 'name' in match['competition']:
                trends['competitions'][match['competition']['name']] += 1
            
            # Arbitri
            if 'referees' in match:
                for referee in match['referees']:
                    ref_name = referee.get('name', '')
                    if ref_name:
                        trends['referees'][ref_name] += 1
            
            # Minuti dei gol (se disponibili negli eventi)
            if 'events' in match and match['events']:
                self._extract_scoring_minutes(
                    match['events'], 
                    is_team1_home,
                    trends['scoring_minutes']
                )
        
        # Converti i counter in dizionari per serializzazione JSON
        trends['competitions'] = dict(trends['competitions'])
        trends['referees'] = dict(trends['referees'])
        
        return trends
    
    def _extract_scoring_minutes(
        self, 
        events: List[Dict[str, Any]], 
        is_team1_home: bool,
        scoring_minutes: Dict[str, Dict[str, int]]
    ) -> None:
        """
        Estrae i minuti dei gol dagli eventi della partita.
        
        Args:
            events: Lista di eventi della partita.
            is_team1_home: True se la prima squadra è in casa, False altrimenti.
            scoring_minutes: Dizionario dove salvare i minuti dei gol.
        """
        for event in events:
            if event.get('type') != 'goal':
                continue
            
            minute_str = event.get('minute', '')
            if not minute_str:
                continue
            
            # Estrai il minuto di base (senza recupero)
            try:
                minute = int(minute_str.split('+')[0])
            except:
                continue
            
            # Determina il periodo (0-15, 16-30, etc.)
            period = min(90, minute) // 15 * 15
            period_key = f"{period}-{period+14}"
            
            # Assegna il gol alla squadra corretta
            team_side = event.get('team', '')
            
            if team_side == 'home' and is_team1_home:
                if period_key not in scoring_minutes['team1']:
                    scoring_minutes['team1'][period_key] = 0
                scoring_minutes['team1'][period_key] += 1
            elif team_side == 'away' and is_team1_home:
                if period_key not in scoring_minutes['team2']:
                    scoring_minutes['team2'][period_key] = 0
                scoring_minutes['team2'][period_key] += 1
            elif team_side == 'home' and not is_team1_home:
                if period_key not in scoring_minutes['team2']:
                    scoring_minutes['team2'][period_key] = 0
                scoring_minutes['team2'][period_key] += 1
            elif team_side == 'away' and not is_team1_home:
                if period_key not in scoring_minutes['team1']:
                    scoring_minutes['team1'][period_key] = 0
                scoring_minutes['team1'][period_key] += 1
    
    def _reverse_h2h_data(self, h2h_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Inverte i ruoli delle squadre nei dati head-to-head.
        
        Args:
            h2h_data: Dati head-to-head originali.
            
        Returns:
            Dati head-to-head con i ruoli delle squadre invertiti.
        """
        reversed_data = h2h_data.copy()
        
        # Inverti gli ID e i nomi
        reversed_data['team1_id'], reversed_data['team2_id'] = reversed_data['team2_id'], reversed_data['team1_id']
        reversed_data['team1_name'], reversed_data['team2_name'] = reversed_data['team2_name'], reversed_data['team1_name']
        
        # Inverti le statistiche
        if 'stats' in reversed_data:
            stats = reversed_data['stats']
            
            # Scambia vittorie
            stats['team1_wins'], stats['team2_wins'] = stats['team2_wins'], stats['team1_wins']
            stats['team1_win_percentage'], stats['team2_win_percentage'] = stats['team2_win_percentage'], stats['team1_win_percentage']
            
            # Scambia gol
            stats['team1_goals'], stats['team2_goals'] = stats['team2_goals'], stats['team1_goals']
            
            # Scambia clean sheets
            stats['clean_sheets_team1'], stats['clean_sheets_team2'] = stats['clean_sheets_team2'], stats['clean_sheets_team1']
            
            # Scambia primi gol
            stats['first_goal_team1'], stats['first_goal_team2'] = stats['first_goal_team2'], stats['first_goal_team1']
            
            # Scambia vittorie in casa/trasferta
            stats['home_wins_team1'], stats['home_wins_team2'] = stats['home_wins_team2'], stats['home_wins_team1']
            stats['away_wins_team1'], stats['away_wins_team2'] = stats['away_wins_team2'], stats['away_wins_team1']
        
        # Inverti i trend
        if 'trends' in reversed_data:
            trends = reversed_data['trends']
            
            # Scambia forme recenti
            if 'recent_form' in trends:
                trends['recent_form']['team1'], trends['recent_form']['team2'] = trends['recent_form']['team2'], trends['recent_form']['team1']
            
            # Scambia trend gol
            if 'goals_trend' in trends:
                trends['goals_trend']['team1'], trends['goals_trend']['team2'] = trends['goals_trend']['team2'], trends['goals_trend']['team1']
            
            # Scambia minuti dei gol
            if 'scoring_minutes' in trends:
                trends['scoring_minutes']['team1'], trends['scoring_minutes']['team2'] = trends['scoring_minutes']['team2'], trends['scoring_minutes']['team1']
        
        return reversed_data
    
    def get_stored_h2h(
        self, 
        team1_id: str, 
        team2_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Recupera i dati head-to-head salvati.
        
        Args:
            team1_id: ID della prima squadra.
            team2_id: ID della seconda squadra.
            
        Returns:
            Dati head-to-head, o None se non trovati.
        """
        try:
            # Prova prima con l'ordine originale
            h2h_key = f"{team1_id}_{team2_id}"
            h2h_data = self.db.get_reference(f"head_to_head/{h2h_key}")
            
            if h2h_data:
                return h2h_data
            
            # Prova con l'ordine inverso
            reverse_h2h_key = f"{team2_id}_{team1_id}"
            reverse_h2h_data = self.db.get_reference(f"head_to_head/{reverse_h2h_key}")
            
            if reverse_h2h_data:
                # Inverti i ruoli delle squadre
                return self._reverse_h2h_data(reverse_h2h_data)
            
            return None
            
        except Exception as e:
            logger.error(f"Errore nel recuperare i dati head-to-head per {team1_id} vs {team2_id}: {e}")
            return None
    
    def store_h2h_data(self, h2h_data: Dict[str, Any]) -> bool:
        """
        Salva i dati head-to-head nel database.
        
        Args:
            h2h_data: Dati head-to-head da salvare.
            
        Returns:
            True se salvato con successo, False altrimenti.
        """
        try:
            if 'team1_id' not in h2h_data or 'team2_id' not in h2h_data:
                logger.error("Impossibile salvare dati head-to-head senza ID delle squadre")
                return False
            
            # Costruisci la chiave
            h2h_key = f"{h2h_data['team1_id']}_{h2h_data['team2_id']}"
            
            # Aggiungi timestamp se non presente
            if 'last_updated' not in h2h_data:
                h2h_data['last_updated'] = datetime.now().isoformat()
            
            # Salva i dati
            self.db.set_reference(f"head_to_head/{h2h_key}", h2h_data)
            
            logger.info(f"Dati head-to-head per {h2h_key} salvati con successo")
            return True
            
        except Exception as e:
            logger.error(f"Errore nel salvare i dati head-to-head: {e}")
            return False
    
    def predict_h2h_outcome(
        self, 
        team1_id: str, 
        team2_id: str,
        home_team_id: str,
        include_probabilities: bool = True
    ) -> Dict[str, Any]:
        """
        Predice il risultato di una partita basandosi sugli scontri diretti.
        
        Args:
            team1_id: ID della prima squadra.
            team2_id: ID della seconda squadra.
            home_team_id: ID della squadra in casa.
            include_probabilities: Se includere le probabilità dei risultati.
            
        Returns:
            Dizionario con la predizione del risultato.
        """
        # Recupera i dati head-to-head
        h2h_data = self.get_head_to_head(team1_id, team2_id)
        
        if not h2h_data or 'error' in h2h_data:
            return {
                'prediction': 'Unknown',
                'confidence': 0,
                'error': h2h_data.get('error') if h2h_data else 'No data'
            }
        
        # Ottieni statistiche
        stats = h2h_data.get('stats', {})
        
        if not stats or stats.get('total_matches', 0) < 2:
            return {
                'prediction': 'Unknown',
                'confidence': 0,
                'error': 'Insufficient historical data'
            }
        
        # Determina quale squadra è in casa
        is_team1_home = home_team_id == team1_id
        
        # Predizione basata sugli scontri diretti passati
        prediction = {
            'prediction': 'Unknown',
            'confidence': 0,
            'reasoning': '',
            'statistics': {}
        }
        
        # Statistiche rilevanti
        team1_win_pct = stats.get('team1_win_percentage', 0)
        team2_win_pct = stats.get('team2_win_percentage', 0)
        draw_pct = stats.get('draw_percentage', 0)
        
        # Statistiche in casa/trasferta
        home_wins_team1 = stats.get('home_wins_team1', 0)
        away_wins_team1 = stats.get('away_wins_team1', 0)
        home_wins_team2 = stats.get('home_wins_team2', 0)
        away_wins_team2 = stats.get('away_wins_team2', 0)
        
        # Calcola le probabilità corrette in base a chi è in casa
        if is_team1_home:
            home_wins = home_wins_team1
            away_wins = away_wins_team2
        else:
            home_wins = home_wins_team2
            away_wins = away_wins_team1
        
        total_home_away_matches = home_wins + away_wins + stats.get('draws', 0)
        
        # Calcola probabilità home/away se ci sono abbastanza partite
        if total_home_away_matches > 0:
            home_win_prob = home_wins / total_home_away_matches
            away_win_prob = away_wins / total_home_away_matches
            draw_prob = 1 - home_win_prob - away_win_prob
        else:
            # Fallback alle probabilità generali
            if is_team1_home:
                home_win_prob = team1_win_pct / 100
                away_win_prob = team2_win_pct / 100
            else:
                home_win_prob = team2_win_pct / 100
                away_win_prob = team1_win_pct / 100
            draw_prob = draw_pct / 100
        
        # Normalizza le probabilità
        total_prob = home_win_prob + away_win_prob + draw_prob
        if total_prob > 0:
            home_win_prob /= total_prob
            away_win_prob /= total_prob
            draw_prob /= total_prob
        
        # Determina il risultato più probabile
        probs = [
            ('Home', home_win_prob),
            ('Draw', draw_prob),
            ('Away', away_win_prob)
        ]
        probs.sort(key=lambda x: x[1], reverse=True)
        
        # Imposta il risultato più probabile
        prediction['prediction'] = probs[0][0]
        prediction['confidence'] = round(probs[0][1] * 100, 1)
        
        # Aggiungi le probabilità se richiesto
        if include_probabilities:
            prediction['probabilities'] = {
                'home': round(home_win_prob * 100, 1),
                'draw': round(draw_prob * 100, 1),
                'away': round(away_win_prob * 100, 1)
            }
        
        # Aggiungi le statistiche rilevanti
        prediction['statistics'] = {
            'total_matches': stats.get('total_matches', 0),
            'home_team_wins': home_wins if is_team1_home else home_wins_team2,
            'away_team_wins': away_wins if is_team1_home else away_wins_team1,
            'draws': stats.get('draws', 0),
            'avg_goals': stats.get('avg_goals', 0),
            'btts_percentage': stats.get('btts_percentage', 0),
            'over_2_5_percentage': stats.get('over_2_5_percentage', 0)
        }
        
        # Aggiungi motivazione
        prediction['reasoning'] = self._generate_prediction_reasoning(prediction, is_team1_home, h2h_data)
        
        return prediction
    
    def _generate_prediction_reasoning(
        self, 
        prediction: Dict[str, Any], 
        is_team1_home: bool, 
        h2h_data: Dict[str, Any]
    ) -> str:
        """
        Genera una motivazione per la predizione.
        
        Args:
            prediction: Dizionario con la predizione.
            is_team1_home: True se la prima squadra è in casa, False altrimenti.
            h2h_data: Dati head-to-head.
            
        Returns:
            Motivazione testuale per la predizione.
        """
        team1_name = h2h_data.get('team1_name', 'Team 1')
        team2_name = h2h_data.get('team2_name', 'Team 2')
        
        # Determina la squadra in casa/trasferta
        home_team = team1_name if is_team1_home else team2_name
        away_team = team2_name if is_team1_home else team1_name
        
        stats = h2h_data.get('stats', {})
        result = prediction.get('prediction', 'Unknown')
        confidence = prediction.get('confidence', 0)
        
        if result == 'Unknown':
            return "Insufficient historical data to make a reliable prediction."
        
        reasoning = f"Based on {stats.get('total_matches', 0)} historical matches between these teams, "
        
        if result == 'Home':
            reasoning += f"{home_team} is likely to win with {confidence}% confidence. "
            
            # Aggiungi dettagli sulle vittorie in casa
            if is_team1_home:
                home_wins = stats.get('home_wins_team1', 0)
            else:
                home_wins = stats.get('home_wins_team2', 0)
            
            reasoning += f"{home_team} has won {home_wins} times when playing at home against {away_team}. "
            
        elif result == 'Away':
            reasoning += f"{away_team} is likely to win with {confidence}% confidence. "
            
            # Aggiungi dettagli sulle vittorie in trasferta
            if is_team1_home:
                away_wins = stats.get('away_wins_team2', 0)
            else:
                away_wins = stats.get('away_wins_team1', 0)
            
            reasoning += f"{away_team} has won {away_wins} times when playing away at {home_team}. "
            
        else:  # Draw
            reasoning += f"The match is likely to end in a draw with {confidence}% confidence. "
            reasoning += f"These teams have drawn {stats.get('draws', 0)} times in their head-to-head history. "
        
        # Aggiungi informazioni sui gol
        avg_goals = stats.get('avg_goals', 0)
        btts_pct = stats.get('btts_percentage', 0)
        over_pct = stats.get('over_2_5_percentage', 0)
        
        reasoning += f"There are on average {avg_goals} goals in matches between these teams. "
        
        if btts_pct > 60:
            reasoning += f"Both teams have scored in {btts_pct}% of their encounters. "
        
        if over_pct > 60:
            reasoning += f"Over 2.5 goals have been scored in {over_pct}% of their matches. "
        
        return reasoning


# Istanza globale per un utilizzo più semplice
h2h_processor = HeadToHeadProcessor()

def get_processor():
    """
    Ottiene l'istanza globale del processore.
    
    Returns:
        Istanza di HeadToHeadProcessor.
    """
    return h2h_processor
