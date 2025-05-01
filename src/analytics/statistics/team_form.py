"""
Modulo per l'analisi della forma delle squadre.

Questo modulo fornisce funzionalità per calcolare e analizzare la forma recente
delle squadre di calcio in base ai risultati delle ultime partite.
"""
import logging
import math
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple, Union

from src.utils.cache import cached
from src.utils.database import FirebaseManager
from src.utils.time_utils import parse_date, date_to_str
from src.config.settings import get_setting

logger = logging.getLogger(__name__)


class TeamFormAnalyzer:
    """
    Analizzatore della forma delle squadre.
    
    Calcola e analizza la forma recente delle squadre in base ai risultati ottenuti,
    ai gol segnati/subiti, e ad altre metriche come xG e possesso palla.
    """
    
    def __init__(self):
        """Inizializza l'analizzatore della forma delle squadre."""
        self.db = FirebaseManager()
        self.form_window = get_setting('analytics.form.window', 10)  # Numero di partite da considerare
        self.league_form_weight = get_setting('analytics.form.league_weight', 1.0)  # Peso delle partite di campionato
        self.cup_form_weight = get_setting('analytics.form.cup_weight', 0.8)  # Peso delle partite di coppa
        self.friendly_form_weight = get_setting('analytics.form.friendly_weight', 0.5)  # Peso delle amichevoli
        self.recency_factor = get_setting('analytics.form.recency_factor', 0.9)  # Fattore per dare più peso alle partite recenti
        self.form_points = {
            'W': 3,  # Vittoria
            'D': 1,  # Pareggio
            'L': 0   # Sconfitta
        }
        logger.info(f"TeamFormAnalyzer inizializzato con {self.form_window} partite di forma")
    
    @cached(ttl=3600)  # Cache di 1 ora
    def get_team_form(self, team_id: str, league_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Calcola la forma recente di una squadra.
        
        Args:
            team_id: ID della squadra
            league_id: ID del campionato (opzionale, se si vuole la forma specifica in un campionato)
            
        Returns:
            Dati completi sulla forma della squadra
        """
        logger.info(f"Calcolando forma per team_id={team_id}")
        
        try:
            # Ottieni le ultime N partite della squadra
            matches_ref = self.db.get_reference(f"data/matches")
            query_ref = matches_ref.order_by_child("datetime").limit_to_last(self.form_window * 2)
            all_matches = query_ref.get()
            
            if not all_matches:
                logger.warning(f"Nessuna partita trovata per team_id={team_id}")
                return self._create_empty_form()
            
            # Filtra le partite della squadra
            team_matches = []
            for match_id, match in all_matches.items():
                if 'home_team_id' not in match or 'away_team_id' not in match:
                    continue
                    
                if match['home_team_id'] == team_id or match['away_team_id'] == team_id:
                    # Se è specificato league_id, filtra solo per quel campionato
                    if league_id and match.get('league_id') != league_id:
                        continue
                    
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
                        'competition_type': match.get('competition_type', 'LEAGUE'),
                        'league_id': match.get('league_id', ''),
                        'xg': match.get('xg', {})
                    })
            
            # Ordina le partite per data (dalla più recente alla più vecchia)
            team_matches.sort(key=lambda x: x['date'], reverse=True)
            
            # Limita alle ultime N partite
            team_matches = team_matches[:self.form_window]
            
            if not team_matches:
                logger.warning(f"Nessuna partita giocata trovata per team_id={team_id}")
                return self._create_empty_form()
                
            # Analizza la forma
            return self._analyze_form(team_id, team_matches)
            
        except Exception as e:
            logger.error(f"Errore nel calcolo della forma per team_id={team_id}: {e}")
            return self._create_empty_form()
    
    def _analyze_form(self, team_id: str, matches: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analizza la forma in base alle partite fornite.
        
        Args:
            team_id: ID della squadra
            matches: Lista di partite
            
        Returns:
            Analisi dettagliata della forma
        """
        # Inizializza i contatori
        total_points = 0
        weighted_points = 0
        total_weight = 0
        form_sequence = []
        goals_for = 0
        goals_against = 0
        wins = 0
        draws = 0
        losses = 0
        home_wins = 0
        home_draws = 0
        home_losses = 0
        away_wins = 0
        away_draws = 0
        away_losses = 0
        clean_sheets = 0
        failed_to_score = 0
        xg_for = 0
        xg_against = 0
        xg_matches = 0
        form_by_date = {}
        form_details = []
        
        now = datetime.now()
        
        # Analizza ogni partita
        for i, match in enumerate(matches):
            # Determina se la squadra era in casa o in trasferta
            is_home = match['home_team_id'] == team_id
            
            # Ottieni i gol segnati e subiti dalla squadra
            goals_scored = match['home_score'] if is_home else match['away_score']
            goals_conceded = match['away_score'] if is_home else match['home_score']
            
            # Aggiungi ai totali
            goals_for += goals_scored
            goals_against += goals_conceded
            
            # Determina il risultato (W, D, L)
            if goals_scored > goals_conceded:
                result = 'W'
                wins += 1
                if is_home:
                    home_wins += 1
                else:
                    away_wins += 1
            elif goals_scored < goals_conceded:
                result = 'L'
                losses += 1
                if is_home:
                    home_losses += 1
                else:
                    away_losses += 1
            else:
                result = 'D'
                draws += 1
                if is_home:
                    home_draws += 1
                else:
                    away_draws += 1
            
            # Aggiungi alla sequenza di forma
            form_sequence.append(result)
            
            # Clean sheet e Failed to score
            if goals_conceded == 0:
                clean_sheets += 1
            if goals_scored == 0:
                failed_to_score += 1
            
            # Peso della partita (basato sul tipo di competizione)
            match_weight = self.league_form_weight
            if match['competition_type'] == 'CUP':
                match_weight = self.cup_form_weight
            elif match['competition_type'] == 'FRIENDLY':
                match_weight = self.friendly_form_weight
            
            # Fattore di recency (partite più recenti hanno più peso)
            match_date = parse_date(match['date'])
            days_ago = (now - match_date).days if match_date else (i * 7)  # Stima 7 giorni per partita se data mancante
            recency_weight = self.recency_factor ** (days_ago / 30)  # decadimento mensile
            
            # Peso finale = tipo competizione * recency
            final_weight = match_weight * recency_weight
            
            # Aggiungi ai punti
            points = self.form_points[result]
            total_points += points
            weighted_points += points * final_weight
            total_weight += final_weight
            
            # Aggiungi xG se disponibile
            if 'xg' in match and match['xg']:
                xg_scored = match['xg']['home'] if is_home else match['xg']['away']
                xg_conceded = match['xg']['away'] if is_home else match['xg']['home']
                xg_for += xg_scored
                xg_against += xg_conceded
                xg_matches += 1
            
            # Aggiungi dettagli per questa partita
            opponent = match['away_team'] if is_home else match['home_team']
            form_details.append({
                'date': match['date'],
                'opponent': opponent,
                'home_away': 'home' if is_home else 'away',
                'result': result,
                'score': f"{goals_scored}-{goals_conceded}",
                'points': points,
                'competition_type': match['competition_type'],
                'weight': final_weight
            })
            
            # Aggiungi alla forma per data
            date_str = date_to_str(match_date) if match_date else str(i)
            form_by_date[date_str] = {
                'result': result,
                'points': points,
                'weight': final_weight,
                'opponent': opponent,
                'score': f"{goals_scored}-{goals_conceded}"
            }
        
        # Calcola medie
        matches_played = len(matches)
        average_points = total_points / matches_played if matches_played > 0 else 0
        weighted_average = weighted_points / total_weight if total_weight > 0 else 0
        average_goals_for = goals_for / matches_played if matches_played > 0 else 0
        average_goals_against = goals_against / matches_played if matches_played > 0 else 0
        average_xg_for = xg_for / xg_matches if xg_matches > 0 else 0
        average_xg_against = xg_against / xg_matches if xg_matches > 0 else 0
        
        # Calcola la qualità della forma (0-100)
        form_quality = self._calculate_form_quality(
            weighted_average, 
            wins, draws, losses, 
            clean_sheets, failed_to_score, 
            average_goals_for, average_goals_against,
            average_xg_for, average_xg_against,
            matches_played
        )
        
        # Costruisci il risultato
        return {
            'team_id': team_id,
            'matches_played': matches_played,
            'form_sequence': form_sequence,
            'form_string': ''.join(form_sequence),
            'total_points': total_points,
            'weighted_points': weighted_points,
            'average_points': average_points,
            'weighted_average': weighted_average,
            'form_quality': form_quality,
            'results': {
                'wins': wins,
                'draws': draws,
                'losses': losses,
                'home_wins': home_wins,
                'home_draws': home_draws,
                'home_losses': home_losses,
                'away_wins': away_wins,
                'away_draws': away_draws,
                'away_losses': away_losses,
                'win_percentage': (wins / matches_played) * 100 if matches_played > 0 else 0,
                'draw_percentage': (draws / matches_played) * 100 if matches_played > 0 else 0,
                'loss_percentage': (losses / matches_played) * 100 if matches_played > 0 else 0
            },
            'goals': {
                'for': goals_for,
                'against': goals_against,
                'average_for': average_goals_for,
                'average_against': average_goals_against,
                'clean_sheets': clean_sheets,
                'clean_sheet_percentage': (clean_sheets / matches_played) * 100 if matches_played > 0 else 0,
                'failed_to_score': failed_to_score,
                'failed_to_score_percentage': (failed_to_score / matches_played) * 100 if matches_played > 0 else 0
            },
            'xg': {
                'matches_with_xg': xg_matches,
                'for': xg_for,
                'against': xg_against,
                'average_for': average_xg_for,
                'average_against': average_xg_against,
                'xg_difference': average_xg_for - average_xg_against if xg_matches > 0 else 0
            },
            'form_by_date': form_by_date,
            'form_details': form_details,
            'has_data': True
        }
    
    def _calculate_form_quality(self, weighted_average: float, wins: int, draws: int, losses: int,
                              clean_sheets: int, failed_to_score: int, avg_goals_for: float,
                              avg_goals_against: float, avg_xg_for: float, avg_xg_against: float,
                              matches_played: int) -> float:
        """
        Calcola un punteggio di qualità della forma (0-100).
        
        Args:
            weighted_average: Media ponderata dei punti
            wins, draws, losses: Vittorie, pareggi, sconfitte
            clean_sheets, failed_to_score: Clean sheet e partite senza segnare
            avg_goals_for, avg_goals_against: Media gol segnati e subiti
            avg_xg_for, avg_xg_against: Media xG per e contro
            matches_played: Numero di partite giocate
            
        Returns:
            Punteggio di qualità della forma (0-100)
        """
        quality = 0
        
        # Punti medi (max 3 punti a partita = 100%)
        quality += (weighted_average / 3) * 35
        
        # Rapporto vittorie-sconfitte (tutte vittorie = 100%)
        if wins + losses > 0:
            win_ratio = wins / (wins + losses)
            quality += win_ratio * 20
        
        # Differenza media gol
        if matches_played > 0:
            goal_diff = avg_goals_for - avg_goals_against
            # Normalizza da -3 a +3
            normalized_goal_diff = max(-3, min(3, goal_diff)) + 3  # Da 0 a 6
            quality += (normalized_goal_diff / 6) * 15
        
        # Differenza media xG
        if avg_xg_for > 0 or avg_xg_against > 0:
            xg_diff = avg_xg_for - avg_xg_against
            # Normalizza da -3 a +3
            normalized_xg_diff = max(-3, min(3, xg_diff)) + 3  # Da 0 a 6
            quality += (normalized_xg_diff / 6) * 15
        
        # Clean sheets
        if matches_played > 0:
            quality += (clean_sheets / matches_played) * 10
        
        # Fallimenti in attacco (inverso)
        if matches_played > 0:
            quality += (1 - (failed_to_score / matches_played)) * 5
        
        return min(100, max(0, quality))
    
    def _create_empty_form(self) -> Dict[str, Any]:
        """
        Crea un oggetto forma vuoto quando non ci sono dati disponibili.
        
        Returns:
            Oggetto forma vuoto
        """
        return {
            'matches_played': 0,
            'form_sequence': [],
            'form_string': '',
            'total_points': 0,
            'weighted_points': 0,
            'average_points': 0,
            'weighted_average': 0,
            'form_quality': 0,
            'results': {
                'wins': 0,
                'draws': 0,
                'losses': 0,
                'home_wins': 0,
                'home_draws': 0,
                'home_losses': 0,
                'away_wins': 0,
                'away_draws': 0,
                'away_losses': 0,
                'win_percentage': 0,
                'draw_percentage': 0,
                'loss_percentage': 0
            },
            'goals': {
                'for': 0,
                'against': 0,
                'average_for': 0,
                'average_against': 0,
                'clean_sheets': 0,
                'clean_sheet_percentage': 0,
                'failed_to_score': 0,
                'failed_to_score_percentage': 0
            },
            'xg': {
                'matches_with_xg': 0,
                'for': 0,
                'against': 0,
                'average_for': 0,
                'average_against': 0,
                'xg_difference': 0
            },
            'form_by_date': {},
            'form_details': [],
            'has_data': False
        }
    
    @cached(ttl=3600 * 24)  # Cache di 24 ore
    def compare_teams_form(self, team1_id: str, team2_id: str, league_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Confronta la forma di due squadre.
        
        Args:
            team1_id: ID della prima squadra
            team2_id: ID della seconda squadra
            league_id: ID del campionato (opzionale)
            
        Returns:
            Analisi comparativa della forma
        """
        team1_form = self.get_team_form(team1_id, league_id)
        team2_form = self.get_team_form(team2_id, league_id)
        
        # Verifica che entrambe le squadre abbiano dati
        if not team1_form['has_data'] or not team2_form['has_data']:
            return {
                'team1_id': team1_id,
                'team2_id': team2_id,
                'sufficient_data': False,
                'team1_form': team1_form,
                'team2_form': team2_form,
                'comparison': {}
            }
        
        # Confronta le statistiche principali
        comparison = {
            'points': {
                'team1': team1_form['average_points'],
                'team2': team2_form['average_points'],
                'difference': team1_form['average_points'] - team2_form['average_points'],
                'winner': 'team1' if team1_form['average_points'] > team2_form['average_points'] else 
                          'team2' if team2_form['average_points'] > team1_form['average_points'] else 'equal'
            },
            'weighted_points': {
                'team1': team1_form['weighted_average'],
                'team2': team2_form['weighted_average'],
                'difference': team1_form['weighted_average'] - team2_form['weighted_average'],
                'winner': 'team1' if team1_form['weighted_average'] > team2_form['weighted_average'] else 
                          'team2' if team2_form['weighted_average'] > team1_form['weighted_average'] else 'equal'
            },
            'form_quality': {
                'team1': team1_form['form_quality'],
                'team2': team2_form['form_quality'],
                'difference': team1_form['form_quality'] - team2_form['form_quality'],
                'winner': 'team1' if team1_form['form_quality'] > team2_form['form_quality'] else 
                          'team2' if team2_form['form_quality'] > team1_form['form_quality'] else 'equal'
            },
            'goals_for': {
                'team1': team1_form['goals']['average_for'],
                'team2': team2_form['goals']['average_for'],
                'difference': team1_form['goals']['average_for'] - team2_form['goals']['average_for'],
                'winner': 'team1' if team1_form['goals']['average_for'] > team2_form['goals']['average_for'] else 
                          'team2' if team2_form['goals']['average_for'] > team1_form['goals']['average_for'] else 'equal'
            },
            'goals_against': {
                'team1': team1_form['goals']['average_against'],
                'team2': team2_form['goals']['average_against'],
                'difference': team1_form['goals']['average_against'] - team2_form['goals']['average_against'],
                # Per gol subiti, il vincitore è chi ne subisce meno
                'winner': 'team2' if team1_form['goals']['average_against'] > team2_form['goals']['average_against'] else 
                          'team1' if team2_form['goals']['average_against'] > team1_form['goals']['average_against'] else 'equal'
            },
            'xg_for': {
                'team1': team1_form['xg']['average_for'],
                'team2': team2_form['xg']['average_for'],
                'difference': team1_form['xg']['average_for'] - team2_form['xg']['average_for'],
                'winner': 'team1' if team1_form['xg']['average_for'] > team2_form['xg']['average_for'] else 
                          'team2' if team2_form['xg']['average_for'] > team1_form['xg']['average_for'] else 'equal'
            },
            'xg_against': {
                'team1': team1_form['xg']['average_against'],
                'team2': team2_form['xg']['average_against'],
                'difference': team1_form['xg']['average_against'] - team2_form['xg']['average_against'],
                # Per xG subiti, il vincitore è chi ne subisce meno
                'winner': 'team2' if team1_form['xg']['average_against'] > team2_form['xg']['average_against'] else 
                          'team1' if team2_form['xg']['average_against'] > team1_form['xg']['average_against'] else 'equal'
            },
            'clean_sheets': {
                'team1': team1_form['goals']['clean_sheet_percentage'],
                'team2': team2_form['goals']['clean_sheet_percentage'],
                'difference': team1_form['goals']['clean_sheet_percentage'] - team2_form['goals']['clean_sheet_percentage'],
                'winner': 'team1' if team1_form['goals']['clean_sheet_percentage'] > team2_form['goals']['clean_sheet_percentage'] else 
                          'team2' if team2_form['goals']['clean_sheet_percentage'] > team1_form['goals']['clean_sheet_percentage'] else 'equal'
            },
            'win_percentage': {
                'team1': team1_form['results']['win_percentage'],
                'team2': team2_form['results']['win_percentage'],
                'difference': team1_form['results']['win_percentage'] - team2_form['results']['win_percentage'],
                'winner': 'team1' if team1_form['results']['win_percentage'] > team2_form['results']['win_percentage'] else 
                          'team2' if team2_form['results']['win_percentage'] > team1_form['results']['win_percentage'] else 'equal'
            }
        }
        
        # Contiamo quante categorie vince ciascuna squadra
        team1_wins = sum(1 for cat in comparison.values() if cat['winner'] == 'team1')
        team2_wins = sum(1 for cat in comparison.values() if cat['winner'] == 'team2')
        
        # Calcola il punteggio di forma complessivo (-100 a +100, positivo favorisce team1)
        form_score = 0
        for key, comparison_data in comparison.items():
            weight = 0
            if key == 'form_quality':
                weight = 0.3
            elif key in ['weighted_points', 'goals_for', 'goals_against']:
                weight = 0.15
            elif key in ['xg_for', 'xg_against', 'win_percentage']:
                weight = 0.1
            else:
                weight = 0.05
                
            # I valori sono già normalizzati nelle rispettive scale
            # Normalizziamo la differenza in base alla scala del valore
            if key in ['form_quality', 'win_percentage', 'clean_sheets']:
                # Sono già in percentuale (0-100)
                normalized_diff = comparison_data['difference']
            elif key in ['points', 'weighted_points']:
                # Scala da 0 a 3
                normalized_diff = (comparison_data['difference'] / 3) * 100
            elif key in ['goals_for', 'goals_against', 'xg_for', 'xg_against']:
                # Scala tipicamente da 0 a 3, ma può essere maggiore
                normalized_diff = (comparison_data['difference'] / 3) * 100
                normalized_diff = max(-100, min(100, normalized_diff))  # Limitiamo a ±100
            
            # Per gol subiti e xG subiti, un valore negativo è positivo per team1
            if key in ['goals_against', 'xg_against']:
                normalized_diff = -normalized_diff
                
            form_score += normalized_diff * weight
            
        # Limitiamo il punteggio finale a ±100
        form_score = max(-100, min(100, form_score))
            
        # Decidiamo un vincitore complessivo
        overall_advantage = 'equal'
        if form_score > 10:
            overall_advantage = 'team1'
        elif form_score < -10:
            overall_advantage = 'team2'
        
        # Motivazioni testuali
        reasoning = []
        if team1_wins > team2_wins:
            reasoning.append(f"Team 1 è superiore in {team1_wins} categorie su {len(comparison)}.")
        elif team2_wins > team1_wins:
            reasoning.append(f"Team 2 è superiore in {team2_wins} categorie su {len(comparison)}.")
        else:
            reasoning.append("Le squadre sono equilibrate nelle statistiche di forma.")
            
        # Aggiungi dettagli sulle differenze più significative
        for key, data in comparison.items():
            if abs(data['difference']) > 0.5:  # Solo differenze significative
                if key == 'goals_for' and data['winner'] == 'team1':
                    reasoning.append(f"Team 1 segna {data['difference']:.1f} gol in più per partita.")
                elif key == 'goals_for' and data['winner'] == 'team2':
                    reasoning.append(f"Team 2 segna {-data['difference']:.1f} gol in più per partita.")
                elif key == 'goals_against' and data['winner'] == 'team1':
                    reasoning.append(f"Team 1 subisce {-data['difference']:.1f} gol in meno per partita.")
                elif key == 'goals_against' and data['winner'] == 'team2':
                    reasoning.append(f"Team 2 subisce {data['difference']:.1f} gol in meno per partita.")
                elif key == 'form_quality' and data['winner'] == 'team1':
                    reasoning.append(f"Team 1 ha una qualità di forma superiore di {data['difference']:.1f} punti.")
                elif key == 'form_quality' and data['winner'] == 'team2':
                    reasoning.append(f"Team 2 ha una qualità di forma superiore di {-data['difference']:.1f} punti.")
        
        return {
            'team1_id': team1_id,
            'team2_id': team2_id,
            'sufficient_data': True,
            'team1_form': team1_form,
            'team2_form': team2_form,
            'comparison': comparison,
            'team1_wins': team1_wins,
            'team2_wins': team2_wins,
            'form_score': form_score,
            'overall_advantage': overall_advantage,
            'reasoning': reasoning
        }


# Funzione per creare un'istanza dell'analizzatore
def create_form_analyzer() -> TeamFormAnalyzer:
    """
    Crea un'istanza dell'analizzatore della forma delle squadre.
    
    Returns:
        Istanza dell'analizzatore della forma
    """
    return TeamFormAnalyzer()


# Funzione di utilità per accesso globale
def get_team_form(team_id: str, league_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Ottiene la forma di una squadra.
    
    Args:
        team_id: ID della squadra
        league_id: ID del campionato (opzionale)
        
    Returns:
        Dati sulla forma della squadra
    """
    analyzer = create_form_analyzer()
    return analyzer.get_team_form(team_id, league_id)


def compare_form(team1_id: str, team2_id: str, league_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Confronta la forma di due squadre.
    
    Args:
        team1_id: ID della prima squadra
        team2_id: ID della seconda squadra
        league_id: ID del campionato (opzionale)
        
    Returns:
        Confronto della forma delle squadre
    """
    analyzer = create_form_analyzer()
    return analyzer.compare_teams_form(team1_id, team2_id, league_id)
