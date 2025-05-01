"""
Modulo per l'analisi dei pattern di gol.

Questo modulo fornisce funzionalità per analizzare i pattern di gol delle squadre,
inclusi tempi di segnatura, tipologie di gol, e situazioni di gioco che portano al gol.
"""
import logging
import math
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple, Union, Counter

from src.utils.cache import cached
from src.utils.database import FirebaseManager
from src.config.settings import get_setting

logger = logging.getLogger(__name__)


class ScoringPatternsAnalyzer:
    """
    Analizzatore dei pattern di gol.
    
    Analizza quando e come le squadre segnano e subiscono gol, identificando
    tendenze e pattern significativi.
    """
    
    def __init__(self):
        """Inizializza l'analizzatore dei pattern di gol."""
        self.db = FirebaseManager()
        self.min_matches = get_setting('analytics.scoring.min_matches', 5)
        self.min_goals = get_setting('analytics.scoring.min_goals', 5)
        self.time_segments = [
            (1, 15), (16, 30), (31, 45), (46, 60), (61, 75), (76, 90)
        ]
        logger.info(f"ScoringPatternsAnalyzer inizializzato")
    
    @cached(ttl=3600)
    def get_team_scoring_patterns(self, team_id: str, matches_limit: int = 20) -> Dict[str, Any]:
        """
        Analizza i pattern di gol di una squadra.
        
        Args:
            team_id: ID della squadra
            matches_limit: Numero massimo di partite da considerare
            
        Returns:
            Pattern di gol completi
        """
        logger.info(f"Analizzando pattern di gol per team_id={team_id}")
        
        try:
            # Ottieni le ultime partite della squadra
            matches_ref = self.db.get_reference(f"data/matches")
            query_ref = matches_ref.order_by_child("datetime").limit_to_last(matches_limit * 2)
            all_matches = query_ref.get()
            
            if not all_matches:
                logger.warning(f"Nessuna partita trovata per team_id={team_id}")
                return self._create_empty_scoring_patterns(team_id)
            
            # Filtra le partite della squadra
            team_matches = []
            for match_id, match in all_matches.items():
                if 'home_team_id' not in match or 'away_team_id' not in match:
                    continue
                    
                if match['home_team_id'] == team_id or match['away_team_id'] == team_id:
                    # Considera solo partite già giocate
                    if match.get('status') != 'FINISHED':
                        continue
                        
                    # Verifica che ci siano eventi
                    if 'events' not in match or not match['events']:
                        continue
                        
                    team_matches.append({
                        'match_id': match_id,
                        'date': match['datetime'],
                        'home_team_id': match['home_team_id'],
                        'away_team_id': match['away_team_id'],
                        'home_score': match.get('home_score', 0),
                        'away_score': match.get('away_score', 0),
                        'home_team': match.get('home_team', ''),
                        'away_team': match.get('away_team', ''),
                        'events': match['events'],
                        'league_id': match.get('league_id', '')
                    })
            
            # Ordina le partite per data (dalla più recente alla più vecchia)
            team_matches.sort(key=lambda x: x['date'], reverse=True)
            
            # Limita alle ultime N partite
            team_matches = team_matches[:matches_limit]
            
            # Analizza il numero di gol totali
            total_goals = 0
            for match in team_matches:
                is_home = match['home_team_id'] == team_id
                total_goals += match['home_score'] if is_home else match['away_score']
            
            if len(team_matches) < self.min_matches or total_goals < self.min_goals:
                logger.warning(f"Dati insufficienti per team_id={team_id}: "
                             f"{len(team_matches)}/{self.min_matches} partite, "
                             f"{total_goals}/{self.min_goals} gol")
                return self._create_empty_scoring_patterns(team_id)
                
            # Analizza i pattern di gol
            return self._analyze_scoring_patterns(team_id, team_matches)
            
        except Exception as e:
            logger.error(f"Errore nell'analisi dei pattern di gol per team_id={team_id}: {e}")
            return self._create_empty_scoring_patterns(team_id)
    
    def _analyze_scoring_patterns(self, team_id: str, matches: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analizza i pattern di gol dalle partite fornite.
        
        Args:
            team_id: ID della squadra
            matches: Lista di partite con eventi
            
        Returns:
            Pattern di gol completi
        """
        # Inizializza contatori per i pattern
        goals_for_by_time = {segment: 0 for segment in self._get_time_segment_labels()}
        goals_against_by_time = {segment: 0 for segment in self._get_time_segment_labels()}
        
        goals_for_by_type = {}
        goals_against_by_type = {}
        
        goal_scorers = {}
        assistmen = {}
        
        first_goals_for = {
            'count': 0,
            'avg_minute': 0,
            'total_minutes': 0
        }
        first_goals_against = {
            'count': 0,
            'avg_minute': 0,
            'total_minutes': 0
        }
        
        comeback_stats = {
            'comebacks_completed': 0,
            'comebacks_failed': 0,
            'leads_kept': 0,
            'leads_lost': 0
        }
        
        matches_analyzed = 0
        goals_for_total = 0
        goals_against_total = 0
        
        # Analizza ogni partita
        for match in matches:
            matches_analyzed += 1
            is_home = match['home_team_id'] == team_id
            
            # Estrai gol segnati e subiti
            goals_for = match['home_score'] if is_home else match['away_score']
            goals_against = match['away_score'] if is_home else match['home_score']
            
            goals_for_total += goals_for
            goals_against_total += goals_against
            
            # Analizza gli eventi per estrarre i dettagli dei gol
            first_goal_for_minute = None
            first_goal_against_minute = None
            
            # Estrai gli eventi gol
            match_events = match.get('events', [])
            
            team_side = 'home' if is_home else 'away'
            opponent_side = 'away' if is_home else 'home'
            
            for event in match_events:
                # Verifica che sia un evento gol
                if event.get('type', '').lower() != 'goal':
                    continue
                
                # Verifica di quale squadra è il gol
                event_team = event.get('team', '')
                if not event_team or event_team not in [team_side, opponent_side]:
                    continue
                
                # Estrai il minuto del gol
                minute = event.get('minute', 0)
                if minute <= 0:
                    continue
                
                # Determina se è un gol segnato o subito
                is_goal_for = event_team == team_side
                
                # Aggiorna il conteggio per fascia oraria
                time_segment = self._get_time_segment(minute)
                
                if is_goal_for:
                    goals_for_by_time[time_segment] += 1
                    
                    # Traccia il primo gol segnato
                    if first_goal_for_minute is None:
                        first_goal_for_minute = minute
                        first_goals_for['count'] += 1
                        first_goals_for['total_minutes'] += minute
                    
                    # Traccia marcatore e assistman
                    scorer = event.get('player', 'Unknown')
                    if scorer and scorer != 'Unknown':
                        if scorer not in goal_scorers:
                            goal_scorers[scorer] = 0
                        goal_scorers[scorer] += 1
                    
                    assist_player = event.get('assist', '')
                    if assist_player:
                        if assist_player not in assistmen:
                            assistmen[assist_player] = 0
                        assistmen[assist_player] += 1
                    
                    # Traccia tipo di gol
                    goal_type = self._determine_goal_type(event)
                    if goal_type not in goals_for_by_type:
                        goals_for_by_type[goal_type] = 0
                    goals_for_by_type[goal_type] += 1
                
                else:
                    goals_against_by_time[time_segment] += 1
                    
                    # Traccia il primo gol subito
                    if first_goal_against_minute is None:
                        first_goal_against_minute = minute
                        first_goals_against['count'] += 1
                        first_goals_against['total_minutes'] += minute
                    
                    # Traccia tipo di gol subito
                    goal_type = self._determine_goal_type(event)
                    if goal_type not in goals_against_by_type:
                        goals_against_by_type[goal_type] = 0
                    goals_against_by_type[goal_type] += 1
            
            # Analizza i comeback / leads
            self._analyze_comeback_stats(comeback_stats, match, is_home, team_id)
        
        # Calcola medie
        if first_goals_for['count'] > 0:
            first_goals_for['avg_minute'] = first_goals_for['total_minutes'] / first_goals_for['count']
            
        if first_goals_against['count'] > 0:
            first_goals_against['avg_minute'] = first_goals_against['total_minutes'] / first_goals_against['count']
        
        # Calcola i pattern significativi
        scoring_patterns = self._identify_scoring_patterns(goals_for_by_time, goals_for_total)
        conceding_patterns = self._identify_scoring_patterns(goals_against_by_time, goals_against_total)
        
        # Calcola le percentuali di distribuzione dei gol per fascia oraria
        for segment in goals_for_by_time:
            if goals_for_total > 0:
                goals_for_by_time[segment] = {
                    'count': goals_for_by_time[segment],
                    'percentage': (goals_for_by_time[segment] / goals_for_total) * 100
                }
            else:
                goals_for_by_time[segment] = {'count': 0, 'percentage': 0}
                
        for segment in goals_against_by_time:
            if goals_against_total > 0:
                goals_against_by_time[segment] = {
                    'count': goals_against_by_time[segment],
                    'percentage': (goals_against_by_time[segment] / goals_against_total) * 100
                }
            else:
                goals_against_by_time[segment] = {'count': 0, 'percentage': 0}
        
        # Converti i tipi di gol in percentuali
        goals_for_by_type_pct = {}
        for goal_type, count in goals_for_by_type.items():
            goals_for_by_type_pct[goal_type] = {
                'count': count,
                'percentage': (count / goals_for_total) * 100 if goals_for_total > 0 else 0
            }
            
        goals_against_by_type_pct = {}
        for goal_type, count in goals_against_by_type.items():
            goals_against_by_type_pct[goal_type] = {
                'count': count,
                'percentage': (count / goals_against_total) * 100 if goals_against_total > 0 else 0
            }
        
        # Ordina i migliori marcatori e assistmen
        top_scorers = sorted(goal_scorers.items(), key=lambda x: x[1], reverse=True)[:10]
        top_scorers = [{'name': name, 'goals': goals} for name, goals in top_scorers]
        
        top_assistmen = sorted(assistmen.items(), key=lambda x: x[1], reverse=True)[:10]
        top_assistmen = [{'name': name, 'assists': assists} for name, assists in top_assistmen]
        
        # Calcola i momenti chiave
        key_moments = self._calculate_key_moments(
            goals_for_by_time, goals_against_by_time, 
            first_goals_for, first_goals_against, 
            goals_for_total, goals_against_total,
            comeback_stats
        )
        
        return {
            'team_id': team_id,
            'matches_analyzed': matches_analyzed,
            'goals_for': {
                'total': goals_for_total,
                'avg_per_game': goals_for_total / matches_analyzed if matches_analyzed > 0 else 0,
                'by_time': goals_for_by_time,
                'by_type': goals_for_by_type_pct,
                'first_goal': first_goals_for,
                'patterns': scoring_patterns
            },
            'goals_against': {
                'total': goals_against_total,
                'avg_per_game': goals_against_total / matches_analyzed if matches_analyzed > 0 else 0,
                'by_time': goals_against_by_time,
                'by_type': goals_against_by_type_pct,
                'first_goal': first_goals_against,
                'patterns': conceding_patterns
            },
            'players': {
                'top_scorers': top_scorers,
                'top_assistmen': top_assistmen
            },
            'comeback_stats': comeback_stats,
            'key_moments': key_moments,
            'analysis_date': datetime.now().isoformat(),
            'has_data': True
        }
    
    def _get_time_segment(self, minute: int) -> str:
        """
        Determina la fascia oraria di un minuto.
        
        Args:
            minute: Minuto della partita
            
        Returns:
            Etichetta della fascia oraria
        """
        # Gestisci minuti di recupero
        if minute > 90:
            return "76-90"
        elif minute > 45 and minute <= 50:
            return "46-60"
            
        for start, end in self.time_segments:
            if start <= minute <= end:
                return f"{start}-{end}"
        
        # Fallback per minuti anomali
        return "1-15"
    
    def _get_time_segment_labels(self) -> List[str]:
        """
        Ottiene le etichette di tutte le fasce orarie.
        
        Returns:
            Lista di etichette delle fasce orarie
        """
        return [f"{start}-{end}" for start, end in self.time_segments]
    
    def _determine_goal_type(self, event: Dict[str, Any]) -> str:
        """
        Determina il tipo di gol da un evento.
        
        Args:
            event: Evento di tipo gol
            
        Returns:
            Tipo di gol
        """
        if event.get('penalty', False):
            return 'penalty'
        elif event.get('own_goal', False):
            return 'own_goal'
        elif event.get('header', False):
            return 'header'
        elif event.get('free_kick', False):
            return 'free_kick'
        elif event.get('counter_attack', False):
            return 'counter_attack'
        elif event.get('set_piece', False):
            return 'set_piece'
        else:
            return 'open_play'
    
    def _analyze_comeback_stats(self, stats: Dict[str, int], match: Dict[str, Any], 
                              is_home: bool, team_id: str) -> None:
        """
        Analizza le statistiche sui comeback e i vantaggi.
        
        Args:
            stats: Dizionario delle statistiche sui comeback
            match: Dati della partita
            is_home: Se la squadra giocava in casa
            team_id: ID della squadra
        """
        # Estrai gli eventi ordinati cronologicamente
        events = sorted(match.get('events', []), key=lambda e: e.get('minute', 0))
        
        team_side = 'home' if is_home else 'away'
        opponent_side = 'away' if is_home else 'home'
        
        # Ricostruisci la sequenza di gol
        current_score = [0, 0]  # [team, opponent]
        was_leading = False
        was_trailing = False
        
        for event in events:
            if event.get('type', '').lower() != 'goal':
                continue
                
            event_team = event.get('team', '')
            
            if event_team == team_side:
                current_score[0] += 1
            elif event_team == opponent_side:
                current_score[1] += 1
            
            # Controlla lo stato attuale
            if current_score[0] > current_score[1]:
                # La squadra è in vantaggio
                if was_trailing:
                    # La squadra era in svantaggio, ora è in vantaggio (comeback completato)
                    stats['comebacks_completed'] += 1
                    was_trailing = False
                was_leading = True
            elif current_score[0] < current_score[1]:
                # La squadra è in svantaggio
                if was_leading:
                    # La squadra era in vantaggio, ora è in svantaggio (vantaggio perso)
                    stats['leads_lost'] += 1
                    was_leading = False
                was_trailing = True
        
        # Controlla lo stato finale
        final_score = [
            match['home_score'] if is_home else match['away_score'],
            match['away_score'] if is_home else match['home_score']
        ]
        
        # Verifica se la squadra ha mantenuto il vantaggio o non ha completato il comeback
        if was_leading and final_score[0] > final_score[1]:
            stats['leads_kept'] += 1
        elif was_trailing and final_score[0] < final_score[1]:
            stats['comebacks_failed'] += 1
    
    def _identify_scoring_patterns(self, goals_by_time: Dict[str, int], total_goals: int) -> List[str]:
        """
        Identifica pattern significativi nella distribuzione dei gol.
        
        Args:
            goals_by_time: Conteggio dei gol per fascia oraria
            total_goals: Totale dei gol
            
        Returns:
            Lista di pattern significativi
        """
        if total_goals == 0:
            return []
            
        patterns = []
        
        # Converti in percentuali
        percentages = {segment: (count / total_goals) * 100 for segment, count in goals_by_time.items()}
        
        # Calcola la distribuzione attesa uniforme
        expected_pct = 100 / len(goals_by_time)
        
        # Identifica fasce orarie con differenze significative
        for segment, percentage in percentages.items():
            if percentage > expected_pct * 1.5:
                patterns.append(f"high_scoring_{segment}")
            elif percentage < expected_pct * 0.5:
                patterns.append(f"low_scoring_{segment}")
        
        # Identifica pattern di inizio/fine partita
        early_pct = percentages.get("1-15", 0) + percentages.get("16-30", 0)
        late_pct = percentages.get("61-75", 0) + percentages.get("76-90", 0)
        
        if early_pct > 50:
            patterns.append("strong_start")
        elif early_pct < 20:
            patterns.append("weak_start")
            
        if late_pct > 50:
            patterns.append("strong_finish")
        elif late_pct < 20:
            patterns.append("weak_finish")
        
        # Identifica pattern specifici
        if percentages.get("46-60", 0) > 30:
            patterns.append("strong_after_break")
            
        return patterns
    
    def _calculate_key_moments(self, goals_for_by_time: Dict[str, Dict[str, float]],
                             goals_against_by_time: Dict[str, Dict[str, float]],
                             first_goals_for: Dict[str, Any],
                             first_goals_against: Dict[str, Any],
                             goals_for_total: int,
                             goals_against_total: int,
                             comeback_stats: Dict[str, int]) -> Dict[str, Any]:
        """
        Calcola i momenti chiave a partire dai pattern di gol.
        
        Args:
            goals_for_by_time: Gol segnati per fascia oraria
            goals_against_by_time: Gol subiti per fascia oraria
            first_goals_for: Dati sui primi gol segnati
            first_goals_against: Dati sui primi gol subiti
            goals_for_total: Totale gol segnati
            goals_against_total: Totale gol subiti
            comeback_stats: Statistiche sui comeback
            
        Returns:
            Momenti chiave
        """
        key_moments = {}
        
        # Analizza il timing del primo gol
        if first_goals_for['count'] > 0:
            key_moments['first_goal_timing'] = {
                'avg_minute': first_goals_for['avg_minute'],
                'significance': 'early' if first_goals_for['avg_minute'] < 25 else 
                                ('late' if first_goals_for['avg_minute'] > 60 else 'average')
            }
            
        # Identifica il momento di massima pericolosità offensiva
        if goals_for_total > 0:
            most_dangerous_period = max(goals_for_by_time.items(), key=lambda x: x[1]['percentage'])
            key_moments['most_dangerous_period'] = {
                'time_segment': most_dangerous_period[0],
                'percentage': most_dangerous_period[1]['percentage']
            }
            
        # Identifica il momento di massima vulnerabilità difensiva
        if goals_against_total > 0:
            most_vulnerable_period = max(goals_against_by_time.items(), key=lambda x: x[1]['percentage'])
            key_moments['most_vulnerable_period'] = {
                'time_segment': most_vulnerable_period[0],
                'percentage': most_vulnerable_period[1]['percentage']
            }
            
        # Analizza la resilienza (capacità di recupero da svantaggio)
        comeback_ratio = 0
        if comeback_stats['comebacks_completed'] + comeback_stats['comebacks_failed'] > 0:
            comeback_ratio = comeback_stats['comebacks_completed'] / (
                comeback_stats['comebacks_completed'] + comeback_stats['comebacks_failed']
            )
            key_moments['comeback_ability'] = {
                'ratio': comeback_ratio,
                'rating': 'high' if comeback_ratio > 0.5 else ('average' if comeback_ratio > 0.25 else 'low')
            }
            
        # Analizza la capacità di mantenere il vantaggio
        lead_protection_ratio = 0
        if comeback_stats['leads_kept'] + comeback_stats['leads_lost'] > 0:
            lead_protection_ratio = comeback_stats['leads_kept'] / (
                comeback_stats['leads_kept'] + comeback_stats['leads_lost']
            )
            key_moments['lead_protection'] = {
                'ratio': lead_protection_ratio,
                'rating': 'high' if lead_protection_ratio > 0.75 else 
                          ('average' if lead_protection_ratio > 0.5 else 'low')
            }
            
        return key_moments
    
    def _create_empty_scoring_patterns(self, team_id: str) -> Dict[str, Any]:
        """
        Crea un oggetto pattern di gol vuoto quando non ci sono dati sufficienti.
        
        Args:
            team_id: ID della squadra
            
        Returns:
            Oggetto pattern di gol vuoto
        """
        empty_time_segments = {segment: {'count': 0, 'percentage': 0} 
                              for segment in self._get_time_segment_labels()}
        
        return {
            'team_id': team_id,
            'matches_analyzed': 0,
            'goals_for': {
                'total': 0,
                'avg_per_game': 0,
                'by_time': empty_time_segments.copy(),
                'by_type': {},
                'first_goal': {'count': 0, 'avg_minute': 0, 'total_minutes': 0},
                'patterns': []
            },
            'goals_against': {
                'total': 0,
                'avg_per_game': 0,
                'by_time': empty_time_segments.copy(),
                'by_type': {},
                'first_goal': {'count': 0, 'avg_minute': 0, 'total_minutes': 0},
                'patterns': []
            },
            'players': {
                'top_scorers': [],
                'top_assistmen': []
            },
            'comeback_stats': {
                'comebacks_completed': 0,
                'comebacks_failed': 0,
                'leads_kept': 0,
                'leads_lost': 0
            },
            'key_moments': {},
            'analysis_date': datetime.now().isoformat(),
            'has_data': False
        }


# Funzioni di utilità per accesso globale
def get_team_scoring_patterns(team_id: str, matches_limit: int = 20) -> Dict[str, Any]:
    """
    Ottiene i pattern di gol completi di una squadra.
    
    Args:
        team_id: ID della squadra
        matches_limit: Numero massimo di partite da considerare
        
    Returns:
        Pattern di gol completi
    """
    analyzer = ScoringPatternsAnalyzer()
    return analyzer.get_team_scoring_patterns(team_id, matches_limit)


def analyze_match_scoring_patterns(match_id: str) -> Dict[str, Any]:
    """
    Analizza i pattern di gol di una singola partita.
    
    Args:
        match_id: ID della partita
        
    Returns:
        Pattern di gol della partita
    """
    try:
        db = FirebaseManager()
        match_ref = db.get_reference(f"data/matches/{match_id}")
        match = match_ref.get()
        
        if not match or not match.get('events'):
            logger.warning(f"Dati insufficienti per match_id={match_id}")
            return {'has_data': False}
        
        # Estrai i dati delle squadre
        home_team_id = match.get('home_team_id', '')
        away_team_id = match.get('away_team_id', '')
        
        if not home_team_id or not away_team_id:
            logger.warning(f"Dati delle squadre mancanti per match_id={match_id}")
            return {'has_data': False}
            
        # Crea un analizzatore temporaneo
        analyzer = ScoringPatternsAnalyzer()
        
        # Analizza i pattern di gol per questa partita
        match_data = {
            'match_id': match_id,
            'date': match['datetime'],
            'home_team_id': home_team_id,
            'away_team_id': away_team_id,
            'home_score': match.get('home_score', 0),
            'away_score': match.get('away_score', 0),
            'home_team': match.get('home_team', ''),
            'away_team': match.get('away_team', ''),
            'events': match['events'],
            'league_id': match.get('league_id', '')
        }
        
        # Analizza la partita per entrambe le squadre
        home_patterns = analyzer._analyze_scoring_patterns(home_team_id, [match_data])
        away_patterns = analyzer._analyze_scoring_patterns(away_team_id, [match_data])
        
        # Combina i risultati
        return {
            'match_id': match_id,
            'home_team_id': home_team_id,
            'away_team_id': away_team_id,
            'home_team': match.get('home_team', ''),
            'away_team': match.get('away_team', ''),
            'home_patterns': home_patterns['goals_for'],
            'away_patterns': away_patterns['goals_for'],
            'home_defense': home_patterns['goals_against'],
            'away_defense': away_patterns['goals_against'],
            'analysis_date': datetime.now().isoformat(),
            'has_data': True
        }
        
    except Exception as e:
        logger.error(f"Errore nell'analisi dei pattern di gol per match_id={match_id}: {e}")
        return {'has_data': False}
