# Funzioni di utilità per accesso globale
def get_team_performance_metrics(team_id: str, matches_limit: int = 10) -> Dict[str, Any]:
    """
    Ottiene le metriche di performance complete di una squadra.
    
    Args:
        team_id: ID della squadra
        matches_limit: Numero massimo di partite da considerare
        
    Returns:
        Metriche di performance complete
    """
    analyzer = PerformanceMetricsAnalyzer()
    return analyzer.get_team_performance_metrics(team_id, matches_limit)


def compare_teams_performance(team1_id: str, team2_id: str, matches_limit: int = 10) -> Dict[str, Any]:
    """
    Confronta le metriche di performance di due squadre.
    
    Args:
        team1_id: ID della prima squadra
        team2_id: ID della seconda squadra
        matches_limit: Numero massimo di partite da considerare per squadra
        
    Returns:
        Confronto delle metriche
    """
    analyzer = PerformanceMetricsAnalyzer()
    team1_metrics = analyzer.get_team_performance_metrics(team1_id, matches_limit)
    team2_metrics = analyzer.get_team_performance_metrics(team2_id, matches_limit)
    
    if not team1_metrics['has_data'] or not team2_metrics['has_data']:
        return {
            'team1_id': team1_id,
            'team2_id': team2_id,
            'sufficient_data': False,
            'team1_metrics': team1_metrics,
            'team2_metrics': team2_metrics,
            'comparison': {}
        }
    
    # Confronta le metriche principali
    comparison = {
        'overall': {
            'team1': team1_metrics['overall_rating']['overall_score'],
            'team2': team2_metrics['overall_rating']['overall_score'],
            'difference': team1_metrics['overall_rating']['overall_score'] - team2_metrics['overall_rating']['overall_score'],
            'winner': 'team1' if team1_metrics['overall_rating']['overall_score'] > team2_metrics['overall_rating']['overall_score'] else 
                      'team2' if team2_metrics['overall_rating']['overall_score'] > team1_metrics['overall_rating']['overall_score'] else 'equal'
        },
        'offensive': {
            'team1': team1_metrics['overall_rating']['scores']['offensive'],
            'team2': team2_metrics['overall_rating']['scores']['offensive'],
            'difference': team1_metrics['overall_rating']['scores']['offensive'] - team2_metrics['overall_rating']['scores']['offensive'],
            'winner': 'team1' if team1_metrics['overall_rating']['scores']['offensive'] > team2_metrics['overall_rating']['scores']['offensive'] else 
                      'team2' if team2_metrics['overall_rating']['scores']['offensive'] > team1_metrics['overall_rating']['scores']['offensive'] else 'equal'
        },
        'defensive': {
            'team1': team1_metrics['overall_rating']['scores']['defensive'],
            'team2': team2_metrics['overall_rating']['scores']['defensive'],
            'difference': team1_metrics['overall_rating']['scores']['defensive'] - team2_metrics['overall_rating']['scores']['defensive'],
            'winner': 'team1' if team1_metrics['overall_rating']['scores']['defensive'] > team2_metrics['overall_rating']['scores']['defensive'] else 
                      'team2' if team2_metrics['overall_rating']['scores']['defensive'] > team1_metrics['overall_rating']['scores']['defensive'] else 'equal'
        },
        'possession': {
            'team1': team1_metrics['overall_rating']['scores']['possession'],
            'team2': team2_metrics['overall_rating']['scores']['possession'],
            'difference': team1_metrics['overall_rating']['scores']['possession'] - team2_metrics['overall_rating']['scores']['possession'],
            'winner': 'team1' if team1_metrics['overall_rating']['scores']['possession'] > team2_metrics['overall_rating']['scores']['possession'] else 
                      'team2' if team2_metrics['overall_rating']['scores']['possession'] > team1_metrics['overall_rating']['scores']['possession'] else 'equal'
        },
        'intensity': {
            'team1': team1_metrics['overall_rating']['scores']['intensity'],
            'team2': team2_metrics['overall_rating']['scores']['intensity'],
            'difference': team1_metrics['overall_rating']['scores']['intensity'] - team2_metrics['overall_rating']['scores']['intensity'],
            'winner': 'team1' if team1_metrics['overall_rating']['scores']['intensity'] > team2_metrics['overall_rating']['scores']['intensity'] else 
                      'team2' if team2_metrics['overall_rating']['scores']['intensity'] > team1_metrics['overall_rating']['scores']['intensity'] else 'equal'
        },
        'xg_efficiency': {
            'team1': team1_metrics['overall_rating']['scores']['xg_efficiency'],
            'team2': team2_metrics['overall_rating']['scores']['xg_efficiency'],
            'difference': team1_metrics['overall_rating']['scores']['xg_efficiency'] - team2_metrics['overall_rating']['scores']['xg_efficiency'],
            'winner': 'team1' if team1_metrics['overall_rating']['scores']['xg_efficiency'] > team2_metrics['overall_rating']['scores']['xg_efficiency'] else 
                      'team2' if team2_metrics['overall_rating']['scores']['xg_efficiency'] > team1_metrics['overall_rating']['scores']['xg_efficiency'] else 'equal'
        },
        'shooting': {
            'shots_per_game': {
                'team1': team1_metrics['shooting']['shots_per_game'],
                'team2': team2_metrics['shooting']['shots_per_game'],
                'difference': team1_metrics['shooting']['shots_per_game'] - team2_metrics['shooting']['shots_per_game'],
                'winner': 'team1' if team1_metrics['shooting']['shots_per_game'] > team2_metrics['shooting']['shots_per_game'] else 
                          'team2' if team2_metrics['shooting']['shots_per_game'] > team1_metrics['shooting']['shots_per_game'] else 'equal'
            },
            'shot_accuracy': {
                'team1': team1_metrics['shooting']['shot_accuracy'],
                'team2': team2_metrics['shooting']['shot_accuracy'],
                'difference': team1_metrics['shooting']['shot_accuracy'] - team2_metrics['shooting']['shot_accuracy'],
                'winner': 'team1' if team1_metrics['shooting']['shot_accuracy'] > team2_metrics['shooting']['shot_accuracy'] else 
                          'team2' if team2_metrics['shooting']['shot_accuracy'] > team1_metrics['shooting']['shot_accuracy'] else 'equal'
            }
        },
        'passing': {
            'pass_accuracy': {
                'team1': team1_metrics['passing']['pass_accuracy'],
                'team2': team2_metrics['passing']['pass_accuracy'],
                'difference': team1_metrics['passing']['pass_accuracy'] - team2_metrics['passing']['pass_accuracy'],
                'winner': 'team1' if team1_metrics['passing']['pass_accuracy'] > team2_metrics['passing']['pass_accuracy'] else 
                          'team2' if team2_metrics['passing']['pass_accuracy'] > team1_metrics['passing']['pass_accuracy'] else 'equal'
            },
            'key_passes': {
                'team1': team1_metrics['passing']['key_passes'],
                'team2': team2_metrics['passing']['key_passes'],
                'difference': team1_metrics['passing']['key_passes'] - team2_metrics['passing']['key_passes'],
                'winner': 'team1' if team1_metrics['passing']['key_passes'] > team2_metrics['passing']['key_passes'] else 
                          'team2' if team2_metrics['passing']['key_passes'] > team1_metrics['passing']['key_passes'] else 'equal'
            }
        },
        'derived': {
            'threat_index': {
                'team1': team1_metrics['derived'].get('threat_index', 0),
                'team2': team2_metrics['derived'].get('threat_index', 0),
                'difference': team1_metrics['derived'].get('threat_index', 0) - team2_metrics['derived'].get('threat_index', 0),
                'winner': 'team1' if team1_metrics['derived'].get('threat_index', 0) > team2_metrics['derived'].get('threat_index', 0) else 
                          'team2' if team2_metrics['derived'].get('threat_index', 0) > team1_metrics['derived'].get('threat_index', 0) else 'equal'
            },
            'defensive_solidity': {
                'team1': team1_metrics['derived'].get('defensive_solidity', 0),
                'team2': team2_metrics['derived'].get('defensive_solidity', 0),
                'difference': team1_metrics['derived'].get('defensive_solidity', 0) - team2_metrics['derived'].get('defensive_solidity', 0),
                'winner': 'team1' if team1_metrics['derived'].get('defensive_solidity', 0) > team2_metrics['derived'].get('defensive_solidity', 0) else 
                          'team2' if team2_metrics['derived'].get('defensive_solidity', 0) > team1_metrics['derived'].get('defensive_solidity', 0) else 'equal'
            }
        }
    }
    
    # Conteggio delle categorie vinte da ciascuna squadra
    team1_wins = sum(1 for cat in comparison.values() if isinstance(cat, dict) and cat.get('winner') == 'team1')
    team2_wins = sum(1 for cat in comparison.values() if isinstance(cat, dict) and cat.get('winner') == 'team2')
    for subcategory in comparison['shooting'].values():
        if subcategory.get('winner') == 'team1':
            team1_wins += 1
        elif subcategory.get('winner') == 'team2':
            team2_wins += 1
    for subcategory in comparison['passing'].values():
        if subcategory.get('winner') == 'team1':
            team1_wins += 1
        elif subcategory.get('winner') == 'team2':
            team2_wins += 1
    for subcategory in comparison['derived'].values():
        if subcategory.get('winner') == 'team1':
            team1_wins += 1
        elif subcategory.get('winner') == 'team2':
            team2_wins += 1
    
    # Determina il vantaggio complessivo
    overall_advantage = 'equal'
    if team1_wins > team2_wins + 2:
        overall_advantage = 'team1'
    elif team2_wins > team1_wins + 2:
        overall_advantage = 'team2'
    
    # Genera analisi testuale
    analysis = []
    
    # Descrivi il vantaggio complessivo
    if overall_advantage == 'team1':
        analysis.append(f"Team 1 ha un vantaggio nelle performance complessive, vincendo in {team1_wins} categorie su {team1_wins + team2_wins}.")
    elif overall_advantage == 'team2':
        analysis.append(f"Team 2 ha un vantaggio nelle performance complessive, vincendo in {team2_wins} categorie su {team1_wins + team2_wins}.")
    else:
        analysis.append(f"Le squadre sono equilibrate nelle performance complessive: Team 1 vince in {team1_wins} categorie, Team 2 in {team2_wins}.")
    
    # Aggiungi dettagli sui principali vantaggi
    team1_advantages = []
    team2_advantages = []
    
    if comparison['offensive']['winner'] == 'team1' and abs(comparison['offensive']['difference']) > 10:
        team1_advantages.append(f"migliore in fase offensiva (+{comparison['offensive']['difference']:.1f})")
    elif comparison['offensive']['winner'] == 'team2' and abs(comparison['offensive']['difference']) > 10:
        team2_advantages.append(f"migliore in fase offensiva (+{-comparison['offensive']['difference']:.1f})")
        
    if comparison['defensive']['winner'] == 'team1' and abs(comparison['defensive']['difference']) > 10:
        team1_advantages.append(f"migliore in fase difensiva (+{comparison['defensive']['difference']:.1f})")
    elif comparison['defensive']['winner'] == 'team2' and abs(comparison['defensive']['difference']) > 10:
        team2_advantages.append(f"migliore in fase difensiva (+{-comparison['defensive']['difference']:.1f})")
        
    if comparison['possession']['winner'] == 'team1' and abs(comparison['possession']['difference']) > 10:
        team1_advantages.append(f"migliore nel possesso palla (+{comparison['possession']['difference']:.1f})")
    elif comparison['possession']['winner'] == 'team2' and abs(comparison['possession']['difference']) > 10:
        team2_advantages.append(f"migliore nel possesso palla (+{-comparison['possession']['difference']:.1f})")
        
    if team1_advantages:
        analysis.append(f"Team 1 è {', '.join(team1_advantages)}.")
    if team2_advantages:
        analysis.append(f"Team 2 è {', '.join(team2_advantages)}.")
    
    return {
        'team1_id': team1_id,
        'team2_id': team2_id,
        'sufficient_data': True,
        'team1_metrics': team1_metrics,
        'team2_metrics': team2_metrics,
        'comparison': comparison,
        'team1_wins': team1_wins,
        'team2_wins': team2_wins,
        'overall_advantage': overall_advantage,
        'analysis': ' '.join(analysis)
    }"""
Modulo per il calcolo di metriche di performance avanzate.

Questo modulo fornisce funzionalità per calcolare e analizzare metriche di performance
avanzate per squadre e giocatori, come efficienza di tiro, pressione, possesso palla, ecc.
"""
import logging
import math
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple, Union

from src.utils.cache import cached
from src.utils.database import FirebaseManager
from src.config.settings import get_setting
from src.analytics.statistics.team_form import get_team_form
from src.analytics.statistics.xg_analysis import get_team_xg_profile

logger = logging.getLogger(__name__)


class PerformanceMetricsAnalyzer:
    """
    Analizzatore di metriche di performance avanzate.
    
    Calcola e analizza varie metriche di performance come efficienza nel tiro,
    statistiche difensive, possesso palla e molto altro.
    """
    
    def __init__(self):
        """Inizializza l'analizzatore di metriche di performance."""
        self.db = FirebaseManager()
        self.min_matches = get_setting('analytics.performance.min_matches', 5)
        self.home_advantage_factor = get_setting('analytics.performance.home_advantage', 1.2)
        self.recency_factor = get_setting('analytics.performance.recency_factor', 0.9)
        logger.info(f"PerformanceMetricsAnalyzer inizializzato")
    
    @cached(ttl=3600)
    def get_team_performance_metrics(self, team_id: str, matches_limit: int = 10) -> Dict[str, Any]:
        """
        Calcola metriche di performance complete per una squadra.
        
        Args:
            team_id: ID della squadra
            matches_limit: Numero massimo di partite da considerare
            
        Returns:
            Metriche di performance complete
        """
        logger.info(f"Calcolando metriche di performance per team_id={team_id}")
        
        try:
            # Ottieni le ultime partite della squadra
            matches_ref = self.db.get_reference(f"data/matches")
            query_ref = matches_ref.order_by_child("datetime").limit_to_last(matches_limit * 2)
            all_matches = query_ref.get()
            
            if not all_matches:
                logger.warning(f"Nessuna partita trovata per team_id={team_id}")
                return self._create_empty_performance_metrics(team_id)
            
            # Filtra le partite della squadra
            team_matches = []
            for match_id, match in all_matches.items():
                if 'home_team_id' not in match or 'away_team_id' not in match:
                    continue
                    
                if match['home_team_id'] == team_id or match['away_team_id'] == team_id:
                    # Considera solo partite già giocate
                    if match.get('status') != 'FINISHED':
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
                        'league_id': match.get('league_id', ''),
                        'xg': match.get('xg', {}),
                        'stats': match.get('stats', {}),
                        'events': match.get('events', [])
                    })
            
            # Ordina le partite per data (dalla più recente alla più vecchia)
            team_matches.sort(key=lambda x: x['date'], reverse=True)
            
            # Limita alle ultime N partite
            team_matches = team_matches[:matches_limit]
            
            if len(team_matches) < self.min_matches:
                logger.warning(f"Dati insufficienti per team_id={team_id}: "
                             f"{len(team_matches)}/{self.min_matches}")
                return self._create_empty_performance_metrics(team_id)
                
            # Calcola le metriche
            return self._calculate_performance_metrics(team_id, team_matches)
            
        except Exception as e:
            logger.error(f"Errore nel calcolo delle metriche per team_id={team_id}: {e}")
            return self._create_empty_performance_metrics(team_id)
    
    def _calculate_performance_metrics(self, team_id: str, matches: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Calcola metriche di performance dettagliate dalle partite fornite.
        
        Args:
            team_id: ID della squadra
            matches: Lista di partite
            
        Returns:
            Metriche di performance complete
        """
        # Inizializza accumulatori
        shooting_metrics = self._init_shooting_metrics()
        possession_metrics = self._init_possession_metrics()
        passing_metrics = self._init_passing_metrics()
        defensive_metrics = self._init_defensive_metrics()
        match_details = []
        
        now = datetime.now()
        total_weight = 0
        
        # Analizza ogni partita
        for match in matches:
            # Determina se la squadra era in casa o in trasferta
            is_home = match['home_team_id'] == team_id
            
            # Calcola il peso in base alla recency
            match_date = datetime.fromisoformat(match['date'].replace('Z', '+00:00')) if 'T' in match['date'] else datetime.now()
            days_ago = (now - match_date).days
            recency_weight = self.recency_factor ** (days_ago / 30)  # decadimento mensile
            
            # Modifica il peso in base a casa/trasferta
            final_weight = recency_weight * (self.home_advantage_factor if is_home else 1.0)
            total_weight += final_weight
            
            # Estrai le statistiche della partita
            match_stats = match.get('stats', {})
            
            # Calcola le metriche
            self._process_shooting_metrics(shooting_metrics, match, team_id, is_home, final_weight)
            self._process_possession_metrics(possession_metrics, match, team_id, is_home, final_weight)
            self._process_passing_metrics(passing_metrics, match, team_id, is_home, final_weight)
            self._process_defensive_metrics(defensive_metrics, match, team_id, is_home, final_weight)
            
            # Aggiungi dettagli della partita
            opponent_id = match['away_team_id'] if is_home else match['home_team_id']
            opponent_name = match['away_team'] if is_home else match['home_team']
            team_goals = match['home_score'] if is_home else match['away_score']
            opponent_goals = match['away_score'] if is_home else match['home_score']
            
            match_details.append({
                'match_id': match['match_id'],
                'date': match['date'],
                'opponent_id': opponent_id,
                'opponent_name': opponent_name,
                'home_away': 'home' if is_home else 'away',
                'score': f"{team_goals}-{opponent_goals}",
                'result': 'win' if team_goals > opponent_goals else ('draw' if team_goals == opponent_goals else 'loss'),
                'weight': final_weight
            })
        
        # Normalizza i valori cumulativi in base ai pesi
        if total_weight > 0:
            self._normalize_metrics(shooting_metrics, total_weight)
            self._normalize_metrics(possession_metrics, total_weight)
            self._normalize_metrics(passing_metrics, total_weight)
            self._normalize_metrics(defensive_metrics, total_weight)
        
        # Calcola metriche derivate
        derived_metrics = self._calculate_derived_metrics(
            shooting_metrics, possession_metrics, passing_metrics, defensive_metrics
        )
        
        # Ottieni dati di forma e xG per una visione più completa
        form_data = get_team_form(team_id)
        xg_profile = get_team_xg_profile(team_id, matches_limit=len(matches))
        
        # Calcola un rating complessivo
        overall_rating = self._calculate_overall_rating(
            shooting_metrics, possession_metrics, passing_metrics, defensive_metrics,
            derived_metrics, form_data, xg_profile
        )
        
        strengths, weaknesses = self._identify_strengths_weaknesses(
            shooting_metrics, possession_metrics, passing_metrics, defensive_metrics,
            derived_metrics, form_data, xg_profile
        )
        
        return {
            'team_id': team_id,
            'matches_analyzed': len(matches),
            'shooting': shooting_metrics,
            'possession': possession_metrics,
            'passing': passing_metrics,
            'defensive': defensive_metrics,
            'derived': derived_metrics,
            'overall_rating': overall_rating,
            'strengths': strengths,
            'weaknesses': weaknesses,
            'match_details': match_details,
            'analysis_date': datetime.now().isoformat(),
            'has_data': True
        }
    
    def _init_shooting_metrics(self) -> Dict[str, Any]:
        """Inizializza le metriche di tiro."""
        return {
            'shots_total': 0,
            'shots_on_target': 0,
            'shots_off_target': 0,
            'shots_blocked': 0,
            'shots_per_game': 0,
            'shots_on_target_per_game': 0,
            'shot_accuracy': 0,  # % tiri in porta
            'shot_conversion': 0,  # % tiri che diventano gol
            'goals_per_shot': 0,
            'goals_per_shot_on_target': 0,
            'xg_per_shot': 0,
            'xg_overperformance': 0,  # Gol - xG
            'shots_inside_box': 0,
            'shots_outside_box': 0,
            'penalties_scored': 0,
            'penalties_missed': 0,
            'free_kicks_scored': 0,
            'free_kicks_total': 0,
            'accumulated_weight': 0  # Per calcolare media ponderata
        }
    
    def _init_possession_metrics(self) -> Dict[str, Any]:
        """Inizializza le metriche di possesso palla."""
        return {
            'possession_pct': 0,
            'touches': 0,
            'touches_in_box': 0,
            'touches_defensive_third': 0,
            'touches_middle_third': 0,
            'touches_final_third': 0,
            'dribbles_completed': 0,
            'dribbles_attempted': 0,
            'dribble_success_rate': 0,
            'progressive_carries': 0,  # Portare palla avanti significativamente
            'carries_into_final_third': 0,
            'carries_into_box': 0,
            'fouls_drawn': 0,
            'fouls_committed': 0,
            'accumulated_weight': 0
        }
    
    def _init_passing_metrics(self) -> Dict[str, Any]:
        """Inizializza le metriche di passaggio."""
        return {
            'passes_completed': 0,
            'passes_attempted': 0,
            'pass_accuracy': 0,
            'key_passes': 0,
            'passes_into_final_third': 0,
            'passes_into_box': 0,
            'crosses_total': 0,
            'crosses_completed': 0,
            'cross_accuracy': 0,
            'through_balls': 0,
            'long_balls_completed': 0,
            'long_balls_attempted': 0,
            'long_ball_accuracy': 0,
            'progressive_passes': 0,  # Passaggi che avanzano significativamente
            'passes_under_pressure': 0,
            'accumulated_weight': 0
        }
    
    def _init_defensive_metrics(self) -> Dict[str, Any]:
        """Inizializza le metriche difensive."""
        return {
            'tackles_attempted': 0,
            'tackles_won': 0,
            'tackle_success_rate': 0,
            'interceptions': 0,
            'blocks': 0,
            'clearances': 0,
            'errors_leading_to_shot': 0,
            'errors_leading_to_goal': 0,
            'duels_won': 0,
            'duels_attempted': 0,
            'duel_success_rate': 0,
            'aerial_duels_won': 0,
            'aerial_duels_attempted': 0,
            'aerial_success_rate': 0,
            'pressures': 0,  # Quante volte la squadra ha pressato
            'successful_pressures': 0,  # Quante volte il pressing ha portato a un turnover
            'pressure_success_rate': 0,
            'accumulated_weight': 0
        }
    
    def _process_shooting_metrics(self, metrics: Dict[str, Any], match: Dict[str, Any], 
                                team_id: str, is_home: bool, weight: float) -> None:
        """
        Elabora le metriche di tiro da una partita.
        
        Args:
            metrics: Dizionario delle metriche di tiro
            match: Dati della partita
            team_id: ID della squadra
            is_home: Se la squadra giocava in casa
            weight: Peso della partita
        """
        stats = match.get('stats', {})
        team_key = 'home' if is_home else 'away'
        
        # Estrai i dati di base
        shots_total = self._get_stat(stats, f'{team_key}_shots', 0)
        shots_on_target = self._get_stat(stats, f'{team_key}_shots_on_target', 0)
        shots_blocked = self._get_stat(stats, f'{team_key}_shots_blocked', 0)
        goals = match['home_score'] if is_home else match['away_score']
        
        # Calcola statistiche derivate
        shots_off_target = max(0, shots_total - shots_on_target - shots_blocked)
        
        # Estrai dati avanzati se disponibili
        shots_inside_box = self._get_stat(stats, f'{team_key}_shots_inside_box', 0)
        shots_outside_box = self._get_stat(stats, f'{team_key}_shots_outside_box', 0)
        
        # Se non sono disponibili, stima
        if shots_inside_box == 0 and shots_outside_box == 0 and shots_total > 0:
            shots_inside_box = int(shots_total * 0.7)  # Stima: 70% dei tiri in area
            shots_outside_box = shots_total - shots_inside_box
        
        # Estrai dati su calci piazzati
        penalties_scored = self._get_stat(stats, f'{team_key}_penalties_scored', 0)
        penalties_missed = self._get_stat(stats, f'{team_key}_penalties_missed', 0)
        free_kicks_scored = self._get_stat(stats, f'{team_key}_free_kicks_scored', 0)
        free_kicks_total = self._get_stat(stats, f'{team_key}_free_kicks_total', 0)
        
        # Dati xG
        team_xg = match.get('xg', {}).get(team_key, 0)
        
        # Aggiorna le metriche con valori ponderati
        metrics['shots_total'] += shots_total * weight
        metrics['shots_on_target'] += shots_on_target * weight
        metrics['shots_off_target'] += shots_off_target * weight
        metrics['shots_blocked'] += shots_blocked * weight
        metrics['shots_per_game'] += shots_total * weight
        metrics['shots_on_target_per_game'] += shots_on_target * weight
        
        # Aggiorna metriche di accuratezza se ci sono tiri
        if shots_total > 0:
            metrics['shot_accuracy'] += (shots_on_target / shots_total) * weight
            metrics['shot_conversion'] += (goals / shots_total) * weight
            metrics['goals_per_shot'] += (goals / shots_total) * weight
        
        if shots_on_target > 0:
            metrics['goals_per_shot_on_target'] += (goals / shots_on_target) * weight
        
        # Metriche xG
        if shots_total > 0 and team_xg > 0:
            metrics['xg_per_shot'] += (team_xg / shots_total) * weight
        
        if team_xg > 0:
            metrics['xg_overperformance'] += ((goals - team_xg) / team_xg) * weight
        
        # Aggiorna altre metriche
        metrics['shots_inside_box'] += shots_inside_box * weight
        metrics['shots_outside_box'] += shots_outside_box * weight
        metrics['penalties_scored'] += penalties_scored * weight
        metrics['penalties_missed'] += penalties_missed * weight
        metrics['free_kicks_scored'] += free_kicks_scored * weight
        metrics['free_kicks_total'] += free_kicks_total * weight
        
        # Aggiorna peso accumulato
        metrics['accumulated_weight'] += weight
    
    def _process_possession_metrics(self, metrics: Dict[str, Any], match: Dict[str, Any], 
                                  team_id: str, is_home: bool, weight: float) -> None:
        """
        Elabora le metriche di possesso palla da una partita.
        
        Args:
            metrics: Dizionario delle metriche di possesso
            match: Dati della partita
            team_id: ID della squadra
            is_home: Se la squadra giocava in casa
            weight: Peso della partita
        """
        stats = match.get('stats', {})
        team_key = 'home' if is_home else 'away'
        
        # Estrai i dati di base
        possession_pct = self._get_stat(stats, f'{team_key}_possession', 0)
        touches = self._get_stat(stats, f'{team_key}_touches', 0)
        
        # Estrai dati avanzati
        touches_in_box = self._get_stat(stats, f'{team_key}_touches_in_box', 0)
        touches_defensive_third = self._get_stat(stats, f'{team_key}_touches_defensive_third', 0)
        touches_middle_third = self._get_stat(stats, f'{team_key}_touches_middle_third', 0)
        touches_final_third = self._get_stat(stats, f'{team_key}_touches_final_third', 0)
        
        # Se non sono disponibili, stima
        if touches > 0 and (touches_defensive_third + touches_middle_third + touches_final_third) == 0:
            touches_defensive_third = int(touches * 0.3)
            touches_middle_third = int(touches * 0.4)
            touches_final_third = touches - touches_defensive_third - touches_middle_third
        
        # Dati su dribbling
        dribbles_completed = self._get_stat(stats, f'{team_key}_dribbles_completed', 0)
        dribbles_attempted = self._get_stat(stats, f'{team_key}_dribbles_attempted', 0)
        
        # Se abbiamo i dribbling completati ma non i tentati, stima
        if dribbles_completed > 0 and dribbles_attempted == 0:
            dribbles_attempted = int(dribbles_completed / 0.6)  # Assumiamo un tasso di successo del 60%
        
        # Dati su progressione
        progressive_carries = self._get_stat(stats, f'{team_key}_progressive_carries', 0)
        carries_into_final_third = self._get_stat(stats, f'{team_key}_carries_into_final_third', 0)
        carries_into_box = self._get_stat(stats, f'{team_key}_carries_into_box', 0)
        
        # Falli
        fouls_drawn = self._get_stat(stats, f'{team_key}_fouls_drawn', 0)
        fouls_committed = self._get_stat(stats, f'{team_key}_fouls_committed', 0)
        
        # Aggiorna le metriche con valori ponderati
        metrics['possession_pct'] += possession_pct * weight
        metrics['touches'] += touches * weight
        metrics['touches_in_box'] += touches_in_box * weight
        metrics['touches_defensive_third'] += touches_defensive_third * weight
        metrics['touches_middle_third'] += touches_middle_third * weight
        metrics['touches_final_third'] += touches_final_third * weight
        metrics['dribbles_completed'] += dribbles_completed * weight
        metrics['dribbles_attempted'] += dribbles_attempted * weight
        
        # Aggiorna il tasso di successo se ci sono tentativi di dribbling
        if dribbles_attempted > 0:
            metrics['dribble_success_rate'] += (dribbles_completed / dribbles_attempted) * weight
        
        # Aggiorna altre metriche
        metrics['progressive_carries'] += progressive_carries * weight
        metrics['carries_into_final_third'] += carries_into_final_third * weight
        metrics['carries_into_box'] += carries_into_box * weight
        metrics['fouls_drawn'] += fouls_drawn * weight
        metrics['fouls_committed'] += fouls_committed * weight
        
        # Aggiorna peso accumulato
        metrics['accumulated_weight'] += weight
    
    def _process_passing_metrics(self, metrics: Dict[str, Any], match: Dict[str, Any], 
                               team_id: str, is_home: bool, weight: float) -> None:
        """
        Elabora le metriche di passaggio da una partita.
        
        Args:
            metrics: Dizionario delle metriche di passaggio
            match: Dati della partita
            team_id: ID della squadra
            is_home: Se la squadra giocava in casa
            weight: Peso della partita
        """
        stats = match.get('stats', {})
        team_key = 'home' if is_home else 'away'
        
        # Estrai i dati di base
        passes_completed = self._get_stat(stats, f'{team_key}_passes_completed', 0)
        passes_attempted = self._get_stat(stats, f'{team_key}_passes_attempted', 0)
        
        # Se abbiamo solo i passaggi completati, stima quelli tentati
        if passes_completed > 0 and passes_attempted == 0:
            # Stima un tasso di completamento dell'80%
            passes_attempted = int(passes_completed / 0.8)
        
        # Estrai dati avanzati
        key_passes = self._get_stat(stats, f'{team_key}_key_passes', 0)
        passes_into_final_third = self._get_stat(stats, f'{team_key}_passes_into_final_third', 0)
        passes_into_box = self._get_stat(stats, f'{team_key}_passes_into_box', 0)
        
        # Dati su cross
        crosses_total = self._get_stat(stats, f'{team_key}_crosses_total', 0)
        crosses_completed = self._get_stat(stats, f'{team_key}_crosses_completed', 0)
        
        # Stima se necessario
        if crosses_total > 0 and crosses_completed == 0:
            # Stima un tasso di completamento del 25% per i cross
            crosses_completed = int(crosses_total * 0.25)
        
        # Dati su lanci lunghi
        long_balls_completed = self._get_stat(stats, f'{team_key}_long_balls_completed', 0)
        long_balls_attempted = self._get_stat(stats, f'{team_key}_long_balls_attempted', 0)
        
        # Stima se necessario
        if long_balls_completed > 0 and long_balls_attempted == 0:
            # Stima un tasso di completamento del 50% per i lanci lunghi
            long_balls_attempted = int(long_balls_completed / 0.5)
        
        # Dati avanzati
        progressive_passes = self._get_stat(stats, f'{team_key}_progressive_passes', 0)
        passes_under_pressure = self._get_stat(stats, f'{team_key}_passes_under_pressure', 0)
        through_balls = self._get_stat(stats, f'{team_key}_through_balls', 0)
        
        # Aggiorna le metriche con valori ponderati
        metrics['passes_completed'] += passes_completed * weight
        metrics['passes_attempted'] += passes_attempted * weight
        
        # Aggiorna l'accuratezza dei passaggi se ci sono tentativi
        if passes_attempted > 0:
            metrics['pass_accuracy'] += (passes_completed / passes_attempted) * weight
        
        # Aggiorna altre metriche
        metrics['key_passes'] += key_passes * weight
        metrics['passes_into_final_third'] += passes_into_final_third * weight
        metrics['passes_into_box'] += passes_into_box * weight
        metrics['crosses_total'] += crosses_total * weight
        metrics['crosses_completed'] += crosses_completed * weight
        
        # Aggiorna l'accuratezza dei cross se ci sono tentativi
        if crosses_total > 0:
            metrics['cross_accuracy'] += (crosses_completed / crosses_total) * weight
        
        # Aggiorna metriche sui lanci lunghi
        metrics['long_balls_completed'] += long_balls_completed * weight
        metrics['long_balls_attempted'] += long_balls_attempted * weight
        
        # Aggiorna l'accuratezza dei lanci lunghi se ci sono tentativi
        if long_balls_attempted > 0:
            metrics['long_ball_accuracy'] += (long_balls_completed / long_balls_attempted) * weight
        
        # Aggiorna metriche avanzate
        metrics['through_balls'] += through_balls * weight
        metrics['progressive_passes'] += progressive_passes * weight
        metrics['passes_under_pressure'] += passes_under_pressure * weight
        
        # Aggiorna peso accumulato
        metrics['accumulated_weight'] += weight
    
    def _process_defensive_metrics(self, metrics: Dict[str, Any], match: Dict[str, Any], 
                                 team_id: str, is_home: bool, weight: float) -> None:
        """
        Elabora le metriche difensive da una partita.
        
        Args:
            metrics: Dizionario delle metriche difensive
            match: Dati della partita
            team_id: ID della squadra
            is_home: Se la squadra giocava in casa
            weight: Peso della partita
        """
        stats = match.get('stats', {})
        team_key = 'home' if is_home else 'away'
        
        # Estrai i dati di base
        tackles_attempted = self._get_stat(stats, f'{team_key}_tackles_attempted', 0)
        tackles_won = self._get_stat(stats, f'{team_key}_tackles_won', 0)
        
        # Stima se necessario
        if tackles_won > 0 and tackles_attempted == 0:
            # Stima un tasso di successo del 70% per i tackle
            tackles_attempted = int(tackles_won / 0.7)
        
        # Estrai dati avanzati
        interceptions = self._get_stat(stats, f'{team_key}_interceptions', 0)
        blocks = self._get_stat(stats, f'{team_key}_blocks', 0)
        clearances = self._get_stat(stats, f'{team_key}_clearances', 0)
        
        # Dati errori
        errors_leading_to_shot = self._get_stat(stats, f'{team_key}_errors_leading_to_shot', 0)
        errors_leading_to_goal = self._get_stat(stats, f'{team_key}_errors_leading_to_goal', 0)
        
        # Dati duelli
        duels_won = self._get_stat(stats, f'{team_key}_duels_won', 0)
        duels_attempted = self._get_stat(stats, f'{team_key}_duels_attempted', 0)
        
        # Stima se necessario
        if duels_won > 0 and duels_attempted == 0:
            # Stima un tasso di successo del 50% per i duelli
            duels_attempted = duels_won * 2
        
        # Dati duelli aerei
        aerial_duels_won = self._get_stat(stats, f'{team_key}_aerial_duels_won', 0)
        aerial_duels_attempted = self._get_stat(stats, f'{team_key}_aerial_duels_attempted', 0)
        
        # Stima se necessario
        if aerial_duels_won > 0 and aerial_duels_attempted == 0:
            # Stima un tasso di successo del 50% per i duelli aerei
            aerial_duels_attempted = aerial_duels_won * 2
        
        # Dati pressing
        pressures = self._get_stat(stats, f'{team_key}_pressures', 0)
        successful_pressures = self._get_stat(stats, f'{team_key}_successful_pressures', 0)
        
        # Stima se necessario
        if successful_pressures > 0 and pressures == 0:
            # Stima un tasso di successo del 30% per i pressing
            pressures = int(successful_pressures / 0.3)
        
        # Aggiorna le metriche con valori ponderati
        metrics['tackles_attempted'] += tackles_attempted * weight
        metrics['tackles_won'] += tackles_won * weight
        
        # Aggiorna il tasso di successo dei tackle se ci sono tentativi
        if tackles_attempted > 0:
            metrics['tackle_success_rate'] += (tackles_won / tackles_attempted) * weight
        
        # Aggiorna altre metriche difensive
        metrics['interceptions'] += interceptions * weight
        metrics['blocks'] += blocks * weight
        metrics['clearances'] += clearances * weight
        metrics['errors_leading_to_shot'] += errors_leading_to_shot * weight
        metrics['errors_leading_to_goal'] += errors_leading_to_goal * weight
        
        # Aggiorna metriche sui duelli
        metrics['duels_won'] += duels_won * weight
        metrics['duels_attempted'] += duels_attempted * weight
        
        # Aggiorna il tasso di successo nei duelli se ci sono tentativi
        if duels_attempted > 0:
            metrics['duel_success_rate'] += (duels_won / duels_attempted) * weight
        
        # Aggiorna metriche sui duelli aerei
        metrics['aerial_duels_won'] += aerial_duels_won * weight
        metrics['aerial_duels_attempted'] += aerial_duels_attempted * weight
        
        # Aggiorna il tasso di successo nei duelli aerei se ci sono tentativi
        if aerial_duels_attempted > 0:
            metrics['aerial_success_rate'] += (aerial_duels_won / aerial_duels_attempted) * weight
        
        # Aggiorna metriche sul pressing
        metrics['pressures'] += pressures * weight
        metrics['successful_pressures'] += successful_pressures * weight
        
        # Aggiorna il tasso di successo nel pressing se ci sono tentativi
        if pressures > 0:
            metrics['pressure_success_rate'] += (successful_pressures / pressures) * weight
        
        # Aggiorna peso accumulato
        metrics['accumulated_weight'] += weight
