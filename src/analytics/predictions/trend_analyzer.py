"""
Modulo per l'analisi delle tendenze nelle partite e nei risultati.
Questo modulo identifica pattern e tendenze nei dati calcistici per generare 
insight statistici su andamenti temporali, stagionali, e di confronto.
"""
import logging
import math
import json
import time
from collections import defaultdict
from typing import Dict, List, Any, Optional, Tuple, Union
from datetime import datetime, timedelta

from src.utils.cache import cached
from src.utils.database import FirebaseManager
from src.utils.time_utils import parse_date, date_to_str
from src.config.settings import get_setting

logger = logging.getLogger(__name__)

class TrendAnalyzer:
    """
    Analizzatore di tendenze calcistiche.
    
    Identifica e analizza pattern statistici e tendenze nei risultati e
    nelle prestazioni delle squadre di calcio, fornendo insight utili
    per le previsioni.
    """
    
    def __init__(self):
        """Inizializza l'analizzatore di tendenze."""
        self.db = FirebaseManager()
        self.min_matches = get_setting('analytics.trends.min_matches', 5)
        self.recency_factor = get_setting('analytics.trends.recency_factor', 0.9)
        self.significance_threshold = get_setting('analytics.trends.significance_threshold', 0.6)
        logger.info("TrendAnalyzer inizializzato con min_matches=%d", self.min_matches)
    
    @cached(ttl=3600*6)  # Cache di 6 ore
    def analyze_team_trends(self, team_id: str, matches_limit: int = 20) -> Dict[str, Any]:
        """
        Analizza le tendenze di una squadra specifica.
        
        Args:
            team_id: ID della squadra
            matches_limit: Numero massimo di partite da considerare
            
        Returns:
            Dizionario con le tendenze identificate
        """
        logger.info(f"Analizzando tendenze per team_id={team_id}")
        
        try:
            # Ottieni le ultime partite della squadra
            matches_ref = self.db.get_reference(f"data/matches")
            query_ref = matches_ref.order_by_child("datetime").limit_to_last(matches_limit * 2)
            all_matches = query_ref.get()
            
            if not all_matches:
                logger.warning(f"Nessuna partita trovata per team_id={team_id}")
                return self._create_empty_trends(team_id)
            
            # Filtra le partite della squadra
            team_matches = []
            for match_id, match in all_matches.items():
                if 'home_team_id' not in match or 'away_team_id' not in match:
                    continue
                
                if match['home_team_id'] == team_id or match['away_team_id'] == team_id:
                    # Considera solo partite già giocate
                    if match.get('status') != 'FINISHED':
                        continue
                    
                    match_data = {
                        'match_id': match_id,
                        'date': match.get('datetime', ''),
                        'home_team_id': match['home_team_id'],
                        'away_team_id': match['away_team_id'],
                        'home_team': match.get('home_team', ''),
                        'away_team': match.get('away_team', ''),
                        'home_score': match.get('home_score', 0),
                        'away_score': match.get('away_score', 0),
                        'league_id': match.get('league_id', ''),
                        'is_home': match['home_team_id'] == team_id,
                        'xg': match.get('xg', {}),
                        'stats': match.get('stats', {})
                    }
                    
                    # Determina il risultato dal punto di vista della squadra
                    if match_data['is_home']:
                        match_data['team_score'] = match_data['home_score']
                        match_data['opponent_score'] = match_data['away_score']
                        match_data['opponent_id'] = match_data['away_team_id']
                        match_data['opponent_name'] = match_data['away_team']
                        
                        if match_data['home_score'] > match_data['away_score']:
                            match_data['result'] = 'W'
                        elif match_data['home_score'] < match_data['away_score']:
                            match_data['result'] = 'L'
                        else:
                            match_data['result'] = 'D'
                    else:
                        match_data['team_score'] = match_data['away_score']
                        match_data['opponent_score'] = match_data['home_score']
                        match_data['opponent_id'] = match_data['home_team_id']
                        match_data['opponent_name'] = match_data['home_team']
                        
                        if match_data['away_score'] > match_data['home_score']:
                            match_data['result'] = 'W'
                        elif match_data['away_score'] < match_data['home_score']:
                            match_data['result'] = 'L'
                        else:
                            match_data['result'] = 'D'
                    
                    # Aggiungi dati partita
                    team_matches.append(match_data)
            
            # Ordina le partite per data (dalla più recente alla più vecchia)
            team_matches.sort(key=lambda x: x['date'], reverse=True)
            
            # Limita alle ultime N partite
            team_matches = team_matches[:matches_limit]
            
            if len(team_matches) < self.min_matches:
                logger.warning(f"Dati insufficienti per team_id={team_id}: {len(team_matches)}/{self.min_matches}")
                return self._create_empty_trends(team_id)
            
            # Analizza le tendenze
            return self._analyze_trends(team_id, team_matches)
            
        except Exception as e:
            logger.error(f"Errore nell'analisi delle tendenze per team_id={team_id}: {e}")
            return self._create_empty_trends(team_id)
    
    def _analyze_trends(self, team_id: str, matches: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analizza le tendenze dalle partite fornite.
        
        Args:
            team_id: ID della squadra
            matches: Lista di partite
            
        Returns:
            Dizionario con le tendenze identificate
        """
        # Estrai il nome della squadra dalla prima partita
        team_name = ''
        if matches:
            if matches[0]['home_team_id'] == team_id:
                team_name = matches[0]['home_team']
            else:
                team_name = matches[0]['away_team']
        
        # Analizza diverse categorie di tendenze
        result_trends = self._analyze_result_trends(matches)
        scoring_trends = self._analyze_scoring_trends(matches)
        time_trends = self._analyze_time_trends(matches)
        opponent_trends = self._analyze_opponent_trends(matches)
        
        # Identifica le tendenze più significative
        significant_trends = self._identify_significant_trends(
            result_trends, scoring_trends, time_trends, opponent_trends
        )
        
        # Prepara i dettagli delle partite
        match_details = []
        for match in matches:
            match_detail = {
                'match_id': match['match_id'],
                'date': match['date'],
                'opponent': match['opponent_name'],
                'is_home': match['is_home'],
                'result': match['result'],
                'score': f"{match['team_score']}-{match['opponent_score']}",
                'league_id': match['league_id']
            }
            match_details.append(match_detail)
        
        # Prepara il risultato
        return {
            'team_id': team_id,
            'team_name': team_name,
            'matches_analyzed': len(matches),
            'result_trends': result_trends,
            'scoring_trends': scoring_trends,
            'time_trends': time_trends,
            'opponent_trends': opponent_trends,
            'significant_trends': significant_trends,
            'match_details': match_details,
            'has_data': True,
            'analysis_date': datetime.now().isoformat()
        }
    
    def _analyze_result_trends(self, matches: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analizza le tendenze nei risultati.
        
        Args:
            matches: Lista di partite
            
        Returns:
            Tendenze nei risultati
        """
        total_matches = len(matches)
        home_matches = sum(1 for m in matches if m['is_home'])
        away_matches = total_matches - home_matches
        
        # Conteggio risultati
        results = {'W': 0, 'D': 0, 'L': 0}
        home_results = {'W': 0, 'D': 0, 'L': 0}
        away_results = {'W': 0, 'D': 0, 'L': 0}
        
        # Ultime N partite
        recent_results = []
        streaks = {'current': '', 'win': 0, 'draw': 0, 'loss': 0, 'unbeaten': 0, 'winless': 0}
        
        # Analizza le partite
        for i, match in enumerate(matches):
            result = match['result']
            results[result] += 1
            
            if match['is_home']:
                home_results[result] += 1
            else:
                away_results[result] += 1
            
            # Aggiungi alle ultime 10 partite
            if i < 10:
                recent_results.append(result)
            
            # Calcola streak
            if i == 0:
                # Prima partita (la più recente)
                streaks['current'] = result
                streaks[result.lower()] = 1
                if result != 'L':
                    streaks['unbeaten'] = 1
                if result != 'W':
                    streaks['winless'] = 1
            else:
                # Partite successive
                prev_result = matches[i-1]['result']
                if result == prev_result:
                    # Continua lo streak corrente
                    if result == streaks['current']:
                        streaks[result.lower()] += 1
                    
                # Streak unbeaten/winless
                if result != 'L' and streaks['unbeaten'] == i:
                    streaks['unbeaten'] += 1
                if result != 'W' and streaks['winless'] == i:
                    streaks['winless'] += 1
        
        # Calcola percentuali
        win_pct = results['W'] / total_matches if total_matches > 0 else 0
        draw_pct = results['D'] / total_matches if total_matches > 0 else 0
        loss_pct = results['L'] / total_matches if total_matches > 0 else 0
        
        home_win_pct = home_results['W'] / home_matches if home_matches > 0 else 0
        home_draw_pct = home_results['D'] / home_matches if home_matches > 0 else 0
        home_loss_pct = home_results['L'] / home_matches if home_matches > 0 else 0
        
        away_win_pct = away_results['W'] / away_matches if away_matches > 0 else 0
        away_draw_pct = away_results['D'] / away_matches if away_matches > 0 else 0
        away_loss_pct = away_results['L'] / away_matches if away_matches > 0 else 0
        
        # Identifica le tendenze
        trends = []
        
        # Tendenza generale
        if win_pct > 0.6:
            trends.append(f"Squadra in forma con {results['W']} vittorie su {total_matches} partite ({win_pct:.0%})")
        elif loss_pct > 0.6:
            trends.append(f"Squadra in difficoltà con {results['L']} sconfitte su {total_matches} partite ({loss_pct:.0%})")
        
        # Tendenza casa/trasferta
        if home_matches >= 3 and home_win_pct > 0.6:
            trends.append(f"Forte in casa con {home_results['W']} vittorie su {home_matches} partite ({home_win_pct:.0%})")
        if away_matches >= 3 and away_win_pct > 0.6:
            trends.append(f"Forte in trasferta con {away_results['W']} vittorie su {away_matches} partite ({away_win_pct:.0%})")
        if home_matches >= 3 and home_loss_pct > 0.6:
            trends.append(f"Debole in casa con {home_results['L']} sconfitte su {home_matches} partite ({home_loss_pct:.0%})")
        if away_matches >= 3 and away_loss_pct > 0.6:
            trends.append(f"Debole in trasferta con {away_results['L']} sconfitte su {away_matches} partite ({away_loss_pct:.0%})")
        
        # Streak
        if streaks['win'] >= 3:
            trends.append(f"In serie di {streaks['win']} vittorie consecutive")
        if streaks['loss'] >= 3:
            trends.append(f"In serie di {streaks['loss']} sconfitte consecutive")
        if streaks['draw'] >= 2:
            trends.append(f"In serie di {streaks['draw']} pareggi consecutivi")
        if streaks['unbeaten'] >= 5:
            trends.append(f"Serie positiva di {streaks['unbeaten']} partite senza sconfitte")
        if streaks['winless'] >= 5:
            trends.append(f"Serie negativa di {streaks['winless']} partite senza vittorie")
        
        return {
            'overall': {
                'total': total_matches,
                'wins': results['W'],
                'draws': results['D'],
                'losses': results['L'],
                'win_pct': win_pct,
                'draw_pct': draw_pct,
                'loss_pct': loss_pct
            },
            'home': {
                'total': home_matches,
                'wins': home_results['W'],
                'draws': home_results['D'],
                'losses': home_results['L'],
                'win_pct': home_win_pct,
                'draw_pct': home_draw_pct,
                'loss_pct': home_loss_pct
            },
            'away': {
                'total': away_matches,
                'wins': away_results['W'],
                'draws': away_results['D'],
                'losses': away_results['L'],
                'win_pct': away_win_pct,
                'draw_pct': away_draw_pct,
                'loss_pct': away_loss_pct
            },
            'recent_results': ''.join(recent_results),
            'streaks': streaks,
            'trends': trends
        }
    
    def _analyze_scoring_trends(self, matches: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analizza le tendenze nei gol.
        
        Args:
            matches: Lista di partite
            
        Returns:
            Tendenze nei gol
        """
        total_matches = len(matches)
        
        # Contatori
        total_goals_scored = sum(m['team_score'] for m in matches)
        total_goals_conceded = sum(m['opponent_score'] for m in matches)
        
        # Per casa/trasferta
        home_matches = [m for m in matches if m['is_home']]
        away_matches = [m for m in matches if not m['is_home']]
        
        home_goals_scored = sum(m['team_score'] for m in home_matches)
        home_goals_conceded = sum(m['opponent_score'] for m in home_matches)
        
        away_goals_scored = sum(m['team_score'] for m in away_matches)
        away_goals_conceded = sum(m['opponent_score'] for m in away_matches)
        
        # Medie
        avg_goals_scored = total_goals_scored / total_matches if total_matches > 0 else 0
        avg_goals_conceded = total_goals_conceded / total_matches if total_matches > 0 else 0
        
        avg_home_goals_scored = home_goals_scored / len(home_matches) if home_matches else 0
        avg_home_goals_conceded = home_goals_conceded / len(home_matches) if home_matches else 0
        
        avg_away_goals_scored = away_goals_scored / len(away_matches) if away_matches else 0
        avg_away_goals_conceded = away_goals_conceded / len(away_matches) if away_matches else 0
        
        # BTTS e Over/Under
        btts_count = sum(1 for m in matches if m['team_score'] > 0 and m['opponent_score'] > 0)
        over_1_5_count = sum(1 for m in matches if m['team_score'] + m['opponent_score'] > 1)
        over_2_5_count = sum(1 for m in matches if m['team_score'] + m['opponent_score'] > 2)
        over_3_5_count = sum(1 for m in matches if m['team_score'] + m['opponent_score'] > 3)
        
        btts_pct = btts_count / total_matches if total_matches > 0 else 0
        over_1_5_pct = over_1_5_count / total_matches if total_matches > 0 else 0
        over_2_5_pct = over_2_5_count / total_matches if total_matches > 0 else 0
        over_3_5_pct = over_3_5_count / total_matches if total_matches > 0 else 0
        
        # Clean sheet e Fail to score
        clean_sheets = sum(1 for m in matches if m['opponent_score'] == 0)
        failed_to_score = sum(1 for m in matches if m['team_score'] == 0)
        
        clean_sheet_pct = clean_sheets / total_matches if total_matches > 0 else 0
        failed_to_score_pct = failed_to_score / total_matches if total_matches > 0 else 0
        
        # Identifica le tendenze
        trends = []
        
        # Tendenza offensiva
        if avg_goals_scored > 2:
            trends.append(f"Attacco prolifico con media di {avg_goals_scored:.1f} gol a partita")
        elif avg_goals_scored < 0.8:
            trends.append(f"Difficoltà offensive con media di {avg_goals_scored:.1f} gol a partita")
        
        # Tendenza difensiva
        if avg_goals_conceded > 2:
            trends.append(f"Difesa vulnerabile con media di {avg_goals_conceded:.1f} gol subiti a partita")
        elif avg_goals_conceded < 0.8:
            trends.append(f"Difesa solida con media di {avg_goals_conceded:.1f} gol subiti a partita")
        
        # BTTS e Over/Under
        if btts_pct > 0.7:
            trends.append(f"Alta tendenza a BTTS ({btts_pct:.0%} delle partite)")
        elif btts_pct < 0.3:
            trends.append(f"Bassa tendenza a BTTS ({btts_pct:.0%} delle partite)")
        
        if over_2_5_pct > 0.7:
            trends.append(f"Alta tendenza a Over 2.5 ({over_2_5_pct:.0%} delle partite)")
        elif over_2_5_pct < 0.3:
            trends.append(f"Alta tendenza a Under 2.5 ({(1-over_2_5_pct):.0%} delle partite)")
        
        # Clean sheet e Fail to score
        if clean_sheet_pct > 0.5:
            trends.append(f"Mantiene la porta inviolata spesso ({clean_sheet_pct:.0%} delle partite)")
        if failed_to_score_pct > 0.5:
            trends.append(f"Fatica a segnare ({failed_to_score_pct:.0%} delle partite senza gol)")
        
        return {
            'overall': {
                'goals_scored': total_goals_scored,
                'goals_conceded': total_goals_conceded,
                'avg_goals_scored': avg_goals_scored,
                'avg_goals_conceded': avg_goals_conceded,
                'avg_total_goals': avg_goals_scored + avg_goals_conceded
            },
            'home': {
                'goals_scored': home_goals_scored,
                'goals_conceded': home_goals_conceded,
                'avg_goals_scored': avg_home_goals_scored,
                'avg_goals_conceded': avg_home_goals_conceded,
                'avg_total_goals': avg_home_goals_scored + avg_home_goals_conceded
            },
            'away': {
                'goals_scored': away_goals_scored,
                'goals_conceded': away_goals_conceded,
                'avg_goals_scored': avg_away_goals_scored,
                'avg_goals_conceded': avg_away_goals_conceded,
                'avg_total_goals': avg_away_goals_scored + avg_away_goals_conceded
            },
            'btts': {
                'count': btts_count,
                'percentage': btts_pct
            },
            'over_under': {
                'over_1_5': {'count': over_1_5_count, 'percentage': over_1_5_pct},
                'over_2_5': {'count': over_2_5_count, 'percentage': over_2_5_pct},
                'over_3_5': {'count': over_3_5_count, 'percentage': over_3_5_pct}
            },
            'clean_sheets': {
                'count': clean_sheets,
                'percentage': clean_sheet_pct
            },
            'failed_to_score': {
                'count': failed_to_score,
                'percentage': failed_to_score_pct
            },
            'trends': trends
        }
    
    def _analyze_time_trends(self, matches: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analizza le tendenze temporali (quando si segnano i gol, etc.).
        
        Args:
            matches: Lista di partite
            
        Returns:
            Tendenze temporali
        """
        # In una implementazione completa, qui si analizzerebbero
        # i tempi dei gol (primo tempo vs secondo tempo, etc.)
        # Per semplicità, restituiamo un oggetto base

        trends = []
        
        # Analisi di primo/secondo tempo possibile solo se abbiamo i dati dei gol
        # Per questo esempio, simuliamo alcuni dati
        first_half_goals = {"scored": 12, "conceded": 8}
        second_half_goals = {"scored": 18, "conceded": 14}
        
        total_goals_scored = first_half_goals["scored"] + second_half_goals["scored"]
        total_goals_conceded = first_half_goals["conceded"] + second_half_goals["conceded"]
        
        # Calcola percentuali
        first_half_scored_pct = first_half_goals["scored"] / total_goals_scored if total_goals_scored > 0 else 0
        second_half_scored_pct = second_half_goals["scored"] / total_goals_scored if total_goals_scored > 0 else 0
        
        first_half_conceded_pct = first_half_goals["conceded"] / total_goals_conceded if total_goals_conceded > 0 else 0
        second_half_conceded_pct = second_half_goals["conceded"] / total_goals_conceded if total_goals_conceded > 0 else 0
        
        # Identifica tendenze
        if second_half_scored_pct > 0.7 and total_goals_scored >= 10:
            trends.append(f"Segna principalmente nel secondo tempo ({second_half_scored_pct:.0%} dei gol)")
        elif first_half_scored_pct > 0.7 and total_goals_scored >= 10:
            trends.append(f"Segna principalmente nel primo tempo ({first_half_scored_pct:.0%} dei gol)")
        
        if second_half_conceded_pct > 0.7 and total_goals_conceded >= 10:
            trends.append(f"Subisce principalmente nel secondo tempo ({second_half_conceded_pct:.0%} dei gol)")
        elif first_half_conceded_pct > 0.7 and total_goals_conceded >= 10:
            trends.append(f"Subisce principalmente nel primo tempo ({first_half_conceded_pct:.0%} dei gol)")
        
        return {
            'first_half': {
                'goals_scored': first_half_goals["scored"],
                'goals_conceded': first_half_goals["conceded"],
                'goals_scored_pct': first_half_scored_pct,
                'goals_conceded_pct': first_half_conceded_pct
            },
            'second_half': {
                'goals_scored': second_half_goals["scored"],
                'goals_conceded': second_half_goals["conceded"],
                'goals_scored_pct': second_half_scored_pct,
                'goals_conceded_pct': second_half_conceded_pct
            },
            'trends': trends
        }
    
    def _analyze_opponent_trends(self, matches: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analizza le tendenze in base agli avversari.
        
        Args:
            matches: Lista di partite
            
        Returns:
            Tendenze in base agli avversari
        """
        # Raggruppa per tipi di avversari
        top_teams = []  # Implementazione reale: lista di ID delle squadre top
        bottom_teams = []  # Implementazione reale: lista di ID delle squadre in fondo
        
        # Per semplicità, in questo esempio consideriamo ogni avversario come una squadra generica
        
        # Calcola prestazioni contro vari tipi di avversari
        vs_top = [m for m in matches if m['opponent_id'] in top_teams]
        vs_bottom = [m for m in matches if m['opponent_id'] in bottom_teams]
        
        # Analisi vs top team
        vs_top_results = {'W': 0, 'D': 0, 'L': 0}
        for match in vs_top:
            vs_top_results[match['result']] += 1
        
        vs_top_total = len(vs_top)
        vs_top_win_pct = vs_top_results['W'] / vs_top_total if vs_top_total > 0 else 0
        
        # Analisi vs bottom team
        vs_bottom_results = {'W': 0, 'D': 0, 'L': 0}
        for match in vs_bottom:
            vs_bottom_results[match['result']] += 1
        
        vs_bottom_total = len(vs_bottom)
        vs_bottom_win_pct = vs_bottom_results['W'] / vs_bottom_total if vs_bottom_total > 0 else 0
        
        # Identifica tendenze
        trends = []
        
        if vs_top_total >= 3:
            if vs_top_win_pct > 0.5:
                trends.append(f"Buone prestazioni contro squadre di alta classifica ({vs_top_results['W']} vittorie su {vs_top_total} partite)")
            elif vs_top_win_pct < 0.2:
                trends.append(f"Difficoltà contro squadre di alta classifica ({vs_top_results['L']} sconfitte su {vs_top_total} partite)")
        
        if vs_bottom_total >= 3:
            if vs_bottom_win_pct > 0.7:
                trends.append(f"Domina contro squadre di bassa classifica ({vs_bottom_results['W']} vittorie su {vs_bottom_total} partite)")
            elif vs_bottom_win_pct < 0.5:
                trends.append(f"Fatica anche contro squadre di bassa classifica ({vs_bottom_results['W']} vittorie su {vs_bottom_total} partite)")
        
        return {
            'vs_top_teams': {
                'total': vs_top_total,
                'wins': vs_top_results['W'],
                'draws': vs_top_results['D'],
                'losses': vs_top_results['L'],
                'win_percentage': vs_top_win_pct
            },
            'vs_bottom_teams': {
                'total': vs_bottom_total,
                'wins': vs_bottom_results['W'],
                'draws': vs_bottom_results['D'],
                'losses': vs_bottom_results['L'],
                'win_percentage': vs_bottom_win_pct
            },
            'trends': trends
        }
    
    def _identify_significant_trends(self, 
                                    result_trends: Dict[str, Any],
                                    scoring_trends: Dict[str, Any],
                                    time_trends: Dict[str, Any],
                                    opponent_trends: Dict[str, Any]) -> List[str]:
        """
        Identifica le tendenze più significative tra tutte quelle rilevate.
        
        Args:
            result_trends: Tendenze nei risultati
            scoring_trends: Tendenze nei gol
            time_trends: Tendenze temporali
            opponent_trends: Tendenze in base agli avversari
            
        Returns:
            Lista delle tendenze più significative
        """
        # Raccogli tutte le tendenze
        all_trends = []
        all_trends.extend(result_trends.get('trends', []))
        all_trends.extend(scoring_trends.get('trends', []))
        all_trends.extend(time_trends.get('trends', []))
        all_trends.extend(opponent_trends.get('trends', []))
        
        # Limita alle tendenze più significative (massimo 10)
        # In un'implementazione più avanzata, potremmo dare un punteggio di significatività
        if len(all_trends) > 10:
            # Per ora prendiamo semplicemente le prime 10 tendenze
            significant_trends = all_trends[:10]
        else:
            significant_trends = all_trends
        
        return significant_trends
    
    def _create_empty_trends(self, team_id: str) -> Dict[str, Any]:
        """
        Crea un oggetto tendenze vuoto quando non ci sono dati sufficienti.
        
        Args:
            team_id: ID della squadra
            
        Returns:
            Oggetto tendenze vuoto
        """
        return {
            'team_id': team_id,
            'team_name': '',
            'matches_analyzed': 0,
            'result_trends': {'trends': []},
            'scoring_trends': {'trends': []},
            'time_trends': {'trends': []},
            'opponent_trends': {'trends': []},
            'significant_trends': [],
            'match_details': [],
            'has_data': False,
            'analysis_date': datetime.now().isoformat()
        }
    
    @cached(ttl=3600*12)  # Cache di 12 ore
    def analyze_league_trends(self, league_id: str, matches_limit: int = 50) -> Dict[str, Any]:
        """
        Analizza le tendenze di un campionato specifico.
        
        Args:
            league_id: ID del campionato
            matches_limit: Numero massimo di partite da considerare
            
        Returns:
            Dizionario con le tendenze del campionato
        """
        logger.info(f"Analizzando tendenze per league_id={league_id}")
        
        try:
            # Ottieni le ultime partite del campionato
            matches_ref = self.db.get_reference(f"data/matches")
            query_ref = matches_ref.order_by_child("datetime").limit_to_last(matches_limit * 2)
            all_matches = query_ref.get()
            
            if not all_matches:
                logger.warning(f"Nessuna partita trovata per league_id={league_id}")
                return self._create_empty_league_trends(league_id)
            
            # Filtra le partite del campionato
            league_matches = []
            for match_id, match in all_matches.items():
                if match.get('league_id') != league_id:
                    continue
                
                # Considera solo partite già giocate
                if match.get('status') != 'FINISHED':
                    continue
                
                match_data = {
                    'match_id': match_id,
                    'date': match.get('datetime', ''),
                    'home_team_id': match.get('home_team_id', ''),
                    'away_team_id': match.get('away_team_id', ''),
                    'home_team': match.get('home_team', ''),
                    'away_team': match.get('away_team', ''),
                    'home_score': match.get('home_score', 0),
                    'away_score': match.get('away_score', 0),
                    'xg': match.get('xg', {}),
                    'stats': match.get('stats', {})
                }
                
                league_matches.append(match_data)
            
            # Ordina le partite per data (dalla più recente alla più vecchia)
            league_matches.sort(key=lambda x: x['date'], reverse=True)
            
            # Limita alle ultime N partite
            league_matches = league_matches[:matches_limit]
            
            if len(league_matches) < self.min_matches:
                logger.warning(f"Dati insufficienti per league_id={league_id}: {len(league_matches)}/{self.min_matches}")
                return self._create_empty_league_trends(league_id)
            
            # Analizza le tendenze
            return self._analyze_league_trends(league_id, league_matches)
            
        except Exception as e:
            logger.error(f"Errore nell'analisi delle tendenze per league_id={league_id}: {e}")
            return self._create_empty_league_trends(league_id)
    
    def _analyze_league_trends(self, league_id: str, matches: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analizza le tendenze di un campionato dalle partite fornite.
        
        Args:
            league_id: ID del campionato
            matches: Lista di partite
            
        Returns:
            Dizionario con le tendenze del campionato
        """
        total_matches = len(matches)
        
        # Statistiche per home/away
        home_wins = sum(1 for m in matches if m['home_score'] > m['away_score'])
        away_wins = sum(1 for m in matches if m['home_score'] < m['away_score'])
        draws = sum(1 for m in matches if m['home_score'] == m['away_score'])
        
        home_win_pct = home_wins / total_matches if total_matches > 0 else 0
        away_win_pct = away_wins / total_matches if total_matches > 0 else 0
        draw_pct = draws / total_matches if total_matches > 0 else 0
        
        # Statistiche per gol
        total_goals = sum(m['home_score'] + m['away_score'] for m in matches)
        home_goals = sum(m['home_score'] for m in matches)
        away_goals = sum(m['away_score'] for m in matches)
        
        avg_goals_per_match = total_goals / total_matches if total_matches > 0 else 0
        avg_home_goals = home_goals / total_matches if total_matches > 0 else 0
        avg_away_goals = away_goals / total_matches if total_matches > 0 else 0
        
        # BTTS e Over/Under
        btts_count = sum(1 for m in matches if m['home_score'] > 0 and m['away_score'] > 0)
        over_1_5_count = sum(1 for m in matches if m['home_score'] + m['away_score'] > 1)
        over_2_5_count = sum(1 for m in matches if m['home_score'] + m['away_score'] > 2)
        over_3_5_count = sum(1 for m in matches if m['home_score'] + m['away_score'] > 3)
        
        btts_pct = btts_count / total_matches if total_matches > 0 else 0
        over_1_5_pct = over_1_5_count / total_matches if total_matches > 0 else 0
        over_2_5_pct = over_2_5_count / total_matches if total_matches > 0 else 0
        over_3_5_pct = over_3_5_count / total_matches if total_matches > 0 else 0
        
        # Identifica le tendenze
        trends = []
        
        # Tendenza risultati
        if home_win_pct > 0.5:
            trends.append(f"Forte vantaggio casalingo con {home_win_pct:.0%} di vittorie interne")
        elif away_win_pct > 0.4:
            trends.append(f"Vantaggio trasferta insolito con {away_win_pct:.0%} di vittorie esterne")
        
        if draw_pct > 0.3:
            trends.append(f"Alto tasso di pareggi ({draw_pct:.0%} delle partite)")
        elif draw_pct < 0.2:
            trends.append(f"Basso tasso di pareggi ({draw_pct:.0%} delle partite)")
        
        # Tendenza gol
        if avg_goals_per_match > 3:
            trends.append(f"Campionato ad alto scoring con {avg_goals_per_match:.1f} gol a partita")
        elif avg_goals_per_match < 2:
            trends.append(f"Campionato a basso scoring con {avg_goals_per_match:.1f} gol a partita")
        
        # BTTS e Over/Under
        if btts_pct > 0.6:
            trends.append(f"Alta incidenza di BTTS ({btts_pct:.0%} delle partite)")
        elif btts_pct < 0.4:
            trends.append(f"Bassa incidenza di BTTS ({btts_pct:.0%} delle partite)")
        
        if over_2_5_pct > 0.6:
            trends.append(f"Alta incidenza di Over 2.5 ({over_2_5_pct:.0%} delle partite)")
        elif over_2_5_pct < 0.4:
            trends.append(f"Bassa incidenza di Over 2.5 ({over_2_5_pct:.0%} delle partite)")
        
        return {
            'league_id': league_id,
            'matches_analyzed': total_matches,
            'results': {
                'home_wins': home_wins,
                'draws': draws,
                'away_wins': away_wins,
                'home_win_percentage': home_win_pct,
                'draw_percentage': draw_pct,
                'away_win_percentage': away_win_pct
            },
            'goals': {
                'total_goals': total_goals,
                'home_goals': home_goals,
                'away_goals': away_goals,
                'avg_goals_per_match': avg_goals_per_match,
                'avg_home_goals': avg_home_goals,
                'avg_away_goals': avg_away_goals
            },
            'btts': {
                'count': btts_count,
                'percentage': btts_pct
            },
            'over_under': {
                'over_1_5': {'count': over_1_5_count, 'percentage': over_1_5_pct},
                'over_2_5': {'count': over_2_5_count, 'percentage': over_2_5_pct},
                'over_3_5': {'count': over_3_5_count, 'percentage': over_3_5_pct},
            },
            'trends': trends,
            'has_data': True,
            'analysis_date': datetime.now().isoformat()
        }
    
    def _create_empty_league_trends(self, league_id: str) -> Dict[str, Any]:
        """
        Crea un oggetto tendenze del campionato vuoto quando non ci sono dati sufficienti.
        
        Args:
            league_id: ID del campionato
            
        Returns:
            Oggetto tendenze vuoto
        """
        return {
            'league_id': league_id,
            'matches_analyzed': 0,
            'results': {
                'home_wins': 0,
                'draws': 0,
                'away_wins': 0,
                'home_win_percentage': 0,
                'draw_percentage': 0,
                'away_win_percentage': 0
            },
            'goals': {
                'total_goals': 0,
                'home_goals': 0,
                'away_goals': 0,
                'avg_goals_per_match': 0,
                'avg_home_goals': 0,
                'avg_away_goals': 0
            },
            'btts': {
                'count': 0,
                'percentage': 0
            },
            'over_under': {
                'over_1_5': {'count': 0, 'percentage': 0},
                'over_2_5': {'count': 0, 'percentage': 0},
                'over_3_5': {'count': 0, 'percentage': 0},
            },
            'trends': [],
            'has_data': False,
            'analysis_date': datetime.now().isoformat()
        }

    @cached(ttl=3600*3)  # Cache di 3 ore
    def analyze_match_trends(self, match_id: str) -> Dict[str, Any]:
        """
        Analizza le tendenze specifiche per una partita.
        
        Args:
            match_id: ID della partita
            
        Returns:
            Dizionario con tendenze rilevanti per la partita
        """
        logger.info(f"Analizzando tendenze per match_id={match_id}")
        
        try:
            # Ottieni i dati della partita
            match_ref = self.db.get_reference(f"data/matches/{match_id}")
            match_data = match_ref.get()
            
            if not match_data:
                logger.warning(f"Nessun dato trovato per match_id={match_id}")
                return self._create_empty_match_trends(match_id)
            
            # Ottieni le tendenze delle due squadre
            home_team_id = match_data.get('home_team_id', '')
            away_team_id = match_data.get('away_team_id', '')
            
            if not home_team_id or not away_team_id:
                logger.warning(f"Dati squadra mancanti per match_id={match_id}")
                return self._create_empty_match_trends(match_id)
            
            home_trends = self.analyze_team_trends(home_team_id)
            away_trends = self.analyze_team_trends(away_team_id)
            
            # Ottieni le tendenze del campionato
            league_id = match_data.get('league_id', '')
            league_trends = self.analyze_league_trends(league_id) if league_id else {}
            
            # Confronta le tendenze e identifica quelle rilevanti
            match_trends = self._identify_match_relevant_trends(
                match_data, home_trends, away_trends, league_trends
            )
            
            return match_trends
            
        except Exception as e:
            logger.error(f"Errore nell'analisi delle tendenze per match_id={match_id}: {e}")
            return self._create_empty_match_trends(match_id)
    
    def _identify_match_relevant_trends(self, match_data: Dict[str, Any],
                                       home_trends: Dict[str, Any],
                                       away_trends: Dict[str, Any],
                                       league_trends: Dict[str, Any]) -> Dict[str, Any]:
        """
        Identifica tendenze rilevanti per una partita specifica.
        
        Args:
            match_data: Dati della partita
            home_trends: Tendenze della squadra di casa
            away_trends: Tendenze della squadra in trasferta
            league_trends: Tendenze del campionato
            
        Returns:
            Dizionario con tendenze rilevanti per la partita
        """
        home_team = match_data.get('home_team', 'Squadra casa')
        away_team = match_data.get('away_team', 'Squadra trasferta')
        
        # Verifica che ci siano abbastanza dati
        if not home_trends.get('has_data', False) or not away_trends.get('has_data', False):
            return self._create_empty_match_trends(match_data.get('match_id', ''))
        
        # Identifica tendenze rilevanti
        relevant_trends = []
        
        # Tendenze risultati
        home_home_win_pct = home_trends.get('result_trends', {}).get('home', {}).get('win_pct', 0)
        away_away_win_pct = away_trends.get('result_trends', {}).get('away', {}).get('win_pct', 0)
        
        if home_home_win_pct > 0.6:
            relevant_trends.append(f"{home_team} forte in casa ({home_home_win_pct:.0%} di vittorie)")
        if away_away_win_pct > 0.5:  # Soglia più bassa per trasferta
            relevant_trends.append(f"{away_team} forte in trasferta ({away_away_win_pct:.0%} di vittorie)")
        
        # Streak
        home_streaks = home_trends.get('result_trends', {}).get('streaks', {})
        away_streaks = away_trends.get('result_trends', {}).get('streaks', {})
        
        if home_streaks.get('win', 0) >= 3:
            relevant_trends.append(f"{home_team} in serie di {home_streaks.get('win', 0)} vittorie consecutive")
        if away_streaks.get('win', 0) >= 3:
            relevant_trends.append(f"{away_team} in serie di {away_streaks.get('win', 0)} vittorie consecutive")
        
        if home_streaks.get('loss', 0) >= 3:
            relevant_trends.append(f"{home_team} in serie di {home_streaks.get('loss', 0)} sconfitte consecutive")
        if away_streaks.get('loss', 0) >= 3:
            relevant_trends.append(f"{away_team} in serie di {away_streaks.get('loss', 0)} sconfitte consecutive")
        
        # BTTS
        home_btts_pct = home_trends.get('scoring_trends', {}).get('btts', {}).get('percentage', 0)
        away_btts_pct = away_trends.get('scoring_trends', {}).get('btts', {}).get('percentage', 0)
        
        if home_btts_pct > 0.7 and away_btts_pct > 0.7:
            relevant_trends.append(f"Entrambe le squadre con alta tendenza BTTS: {home_team} ({home_btts_pct:.0%}), {away_team} ({away_btts_pct:.0%})")
        elif home_btts_pct < 0.3 and away_btts_pct < 0.3:
            relevant_trends.append(f"Entrambe le squadre con bassa tendenza BTTS: {home_team} ({home_btts_pct:.0%}), {away_team} ({away_btts_pct:.0%})")
        
        # Over/Under
        home_over_2_5_pct = home_trends.get('scoring_trends', {}).get('over_under', {}).get('over_2_5', {}).get('percentage', 0)
        away_over_2_5_pct = away_trends.get('scoring_trends', {}).get('over_under', {}).get('over_2_5', {}).get('percentage', 0)
        
        if home_over_2_5_pct > 0.7 and away_over_2_5_pct > 0.7:
            relevant_trends.append(f"Entrambe le squadre con alta tendenza Over 2.5: {home_team} ({home_over_2_5_pct:.0%}), {away_team} ({away_over_2_5_pct:.0%})")
        elif home_over_2_5_pct < 0.3 and away_over_2_5_pct < 0.3:
            relevant_trends.append(f"Entrambe le squadre con alta tendenza Under 2.5: {home_team} ({1-home_over_2_5_pct:.0%}), {away_team} ({1-away_over_2_5_pct:.0%})")
        
        # Tendenze opposte
        if home_trends.get('significant_trends') and away_trends.get('significant_trends'):
            home_significant = home_trends.get('significant_trends', [])
            away_significant = away_trends.get('significant_trends', [])
            
            # Aggiungi le tendenze significative ma limita per evitare duplicati
            home_added = 0
            for trend in home_significant:
                if home_added >= 3:
                    break
                if trend not in relevant_trends and home_team not in trend:
                    trend_with_team = f"{home_team}: {trend}"
                    relevant_trends.append(trend_with_team)
                    home_added += 1
            
            away_added = 0
            for trend in away_significant:
                if away_added >= 3:
                    break
                if trend not in relevant_trends and away_team not in trend:
                    trend_with_team = f"{away_team}: {trend}"
                    relevant_trends.append(trend_with_team)
                    away_added += 1
        
        # Prepara il risultato
        return {
            'match_id': match_data.get('match_id', ''),
            'home_team': home_team,
            'away_team': away_team,
            'league_id': match_data.get('league_id', ''),
            'match_datetime': match_data.get('datetime', ''),
            'relevant_trends': relevant_trends,
            'has_data': len(relevant_trends) > 0,
            'home_team_trends': home_trends.get('significant_trends', []),
            'away_team_trends': away_trends.get('significant_trends', []),
            'analysis_date': datetime.now().isoformat()
        }
    
    def _create_empty_match_trends(self, match_id: str) -> Dict[str, Any]:
        """
        Crea un oggetto tendenze della partita vuoto quando non ci sono dati sufficienti.
        
        Args:
            match_id: ID della partita
            
        Returns:
            Oggetto tendenze vuoto
        """
        return {
            'match_id': match_id,
            'home_team': '',
            'away_team': '',
            'league_id': '',
            'match_datetime': '',
            'relevant_trends': [],
            'has_data': False,
            'home_team_trends': [],
            'away_team_trends': [],
            'analysis_date': datetime.now().isoformat()
        }

# Funzioni di utilità per accesso globale
def analyze_team_trends(team_id: str, matches_limit: int = 20) -> Dict[str, Any]:
    """
    Analizza le tendenze di una squadra specifica.
    
    Args:
        team_id: ID della squadra
        matches_limit: Numero massimo di partite da considerare
        
    Returns:
        Dizionario con le tendenze identificate
    """
    analyzer = TrendAnalyzer()
    return analyzer.analyze_team_trends(team_id, matches_limit)

def analyze_league_trends(league_id: str, matches_limit: int = 50) -> Dict[str, Any]:
    """
    Analizza le tendenze di un campionato specifico.
    
    Args:
        league_id: ID del campionato
        matches_limit: Numero massimo di partite da considerare
        
    Returns:
        Dizionario con le tendenze del campionato
    """
    analyzer = TrendAnalyzer()
    return analyzer.analyze_league_trends(league_id, matches_limit)

def analyze_match_trends(match_id: str) -> Dict[str, Any]:
    """
    Analizza le tendenze specifiche per una partita.
    
    Args:
        match_id: ID della partita
        
    Returns:
        Dizionario con tendenze rilevanti per la partita
    """
    analyzer = TrendAnalyzer()
    return analyzer.analyze_match_trends(match_id)
