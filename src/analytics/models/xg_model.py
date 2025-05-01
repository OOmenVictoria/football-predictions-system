""" 
Modello di previsione basato su Expected Goals (xG).
Questo modulo implementa un modello statistico avanzato che utilizza i dati Expected Goals (xG)
per generare previsioni più accurate sui risultati delle partite di calcio.
"""
import logging
import math
import json
import time
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple, Union

from src.utils.cache import cached
from src.utils.database import FirebaseManager
from src.config.settings import get_setting
from src.data.collector import collect_match_data
from src.analytics.models.basic_model import BasicModel

logger = logging.getLogger(__name__)


class XGModel(BasicModel):
    """
    Modello di previsione basato su Expected Goals (xG).
    
    Estende il modello base per incorporare dati xG da fonti come Understat, FBref e SofaScore
    per generare previsioni più accurate.
    """

    def __init__(self):
        """Inizializza il modello xG."""
        super().__init__()
        self.db = FirebaseManager()
        self.xg_weight = get_setting('models.xg.weight', 0.7)
        self.recency_factor = get_setting('models.xg.recency_factor', 0.9)
        self.min_xg_matches = get_setting('models.xg.min_matches', 5)
        self.xg_sources = get_setting('models.xg.sources', 
                                      ['understat', 'fbref', 'sofascore', 'whoscored'])
        logger.info(f"Inizializzato modello xG con peso={self.xg_weight}")

    @cached(ttl=3600)
    def predict_match(self, match_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Genera previsioni per una partita usando dati xG.
        
        Args:
            match_data: Dati della partita, inclusi team e statistiche

        Returns:
            Previsione completa con probabilità e xG
        """
        logger.info(f"Generando previsione xG per {match_data['home_team']} vs {match_data['away_team']}")
        
        # 1. Ottieni predizione base
        basic_prediction = super().predict_match(match_data)
        
        # 2. Ottieni dati xG storici per entrambe le squadre
        home_xg_data = self._get_team_xg_data(match_data['home_team_id'])
        away_xg_data = self._get_team_xg_data(match_data['away_team_id'])
        
        # 3. Se non abbiamo abbastanza dati xG, usa solo il modello base
        if (not home_xg_data or not away_xg_data or 
            home_xg_data['match_count'] < self.min_xg_matches or
            away_xg_data['match_count'] < self.min_xg_matches):
            logger.warning(f"Dati xG insufficienti, usando solo modello base")
            basic_prediction['model_type'] = 'basic'
            basic_prediction['xg_data_sufficient'] = False
            return basic_prediction
        
        # 4. Calcola xG della partita in base alle statistiche storiche
        home_xg, away_xg = self._calculate_match_xg(
            home_xg_data, away_xg_data, match_data['is_neutral']
        )
        
        # 5. Aggiusta le probabilità base con xG
        adjusted_probabilities = self._adjust_probabilities_with_xg(
            basic_prediction['probabilities'], home_xg, away_xg
        )
        
        # 6. Aggiorna la previsione
        prediction = {
            **basic_prediction,
            'probabilities': adjusted_probabilities,
            'home_xg': home_xg,
            'away_xg': away_xg,
            'model_type': 'xg',
            'xg_data_sufficient': True,
            'xg_sources': self._get_used_xg_sources(match_data)
        }
        
        # 7. Ricalcola mercati derivati con le nuove probabilità
        prediction['btts'] = self._calculate_btts(home_xg, away_xg)
        prediction['over_under'] = self._calculate_over_under(home_xg, away_xg)
        prediction['asian_handicap'] = self._calculate_asian_handicaps(home_xg, away_xg)
        prediction['exact_score'] = self._calculate_exact_score_probabilities(home_xg, away_xg)
        
        # 8. Aggiungi motivazioni basate su xG
        prediction['reasoning'] = self._add_xg_reasoning(prediction, home_xg_data, away_xg_data)
        
        logger.info(f"Previsione xG completata: 1:{prediction['probabilities']['1']:.2f}, "
                    f"X:{prediction['probabilities']['X']:.2f}, "
                    f"2:{prediction['probabilities']['2']:.2f}")
        
        return prediction

    @cached(ttl=21600)  # 6 ore
    def _get_team_xg_data(self, team_id: str) -> Dict[str, Any]:
        """
        Ottiene statistiche xG storiche per una squadra.
        
        Args:
            team_id: ID della squadra
            
        Returns:
            Statistiche xG aggregate
        """
        try:
            # Ottieni le ultime 20 partite della squadra
            matches_ref = self.db.get_reference(f"data/matches")
            team_matches = matches_ref.order_by_child("teams").equal_to(team_id).limit_to_last(20).get()
            
            if not team_matches:
                logger.warning(f"Nessuna partita trovata per team_id={team_id}")
                return None
            
            # Inizializza accumulatori
            home_xg_for = []  # xG quando gioca in casa
            home_xg_against = []  # xG concessi quando gioca in casa
            away_xg_for = []  # xG quando gioca in trasferta  
            away_xg_against = []  # xG concessi quando gioca in trasferta
            
            # Elabora i dati delle partite
            for match_id, match in team_matches.items():
                # Verifica che abbiamo dati xG
                if 'xg' not in match:
                    continue
                    
                match_date = datetime.strptime(match['datetime'], "%Y-%m-%dT%H:%M:%SZ")
                days_ago = (datetime.now() - match_date).days
                
                # Fattore di recency per dare più peso alle partite recenti
                recency_weight = self.recency_factor ** (days_ago / 30)  # decadimento mensile
                
                # Determina se la squadra giocava in casa o in trasferta
                if match['home_team_id'] == team_id:
                    home_xg_for.append((match['xg']['home'] * recency_weight, recency_weight))
                    home_xg_against.append((match['xg']['away'] * recency_weight, recency_weight))
                else:
                    away_xg_for.append((match['xg']['away'] * recency_weight, recency_weight))
                    away_xg_against.append((match['xg']['home'] * recency_weight, recency_weight))
            
            # Calcola le medie ponderate
            home_xg_for_avg = self._weighted_average(home_xg_for) if home_xg_for else 0
            home_xg_against_avg = self._weighted_average(home_xg_against) if home_xg_against else 0
            away_xg_for_avg = self._weighted_average(away_xg_for) if away_xg_for else 0
            away_xg_against_avg = self._weighted_average(away_xg_against) if away_xg_against else 0
            
            # Conta quante partite abbiamo
            match_count = len(home_xg_for) + len(away_xg_for)
            
            return {
                'home_xg_for': home_xg_for_avg,
                'home_xg_against': home_xg_against_avg,
                'away_xg_for': away_xg_for_avg,
                'away_xg_against': away_xg_against_avg,
                'overall_xg_for': (sum(x[0] for x in home_xg_for + away_xg_for) / 
                                  sum(w[1] for w in home_xg_for + away_xg_for)) if home_xg_for + away_xg_for else 0,
                'overall_xg_against': (sum(x[0] for x in home_xg_against + away_xg_against) / 
                                      sum(w[1] for w in home_xg_against + away_xg_against)) if home_xg_against + away_xg_against else 0,
                'match_count': match_count,
                'home_matches': len(home_xg_for),
                'away_matches': len(away_xg_for)
            }
            
        except Exception as e:
            logger.error(f"Errore nel recuperare dati xG per team_id={team_id}: {e}")
            return None
    
    def _weighted_average(self, values: List[Tuple[float, float]]) -> float:
        """
        Calcola la media ponderata di una lista di valori.
        
        Args:
            values: Lista di tuple (valore, peso)
            
        Returns:
            Media ponderata
        """
        if not values:
            return 0
        return sum(v * w for v, w in values) / sum(w for _, w in values)
    
    def _calculate_match_xg(self, home_xg_data: Dict[str, Any], 
                          away_xg_data: Dict[str, Any],
                          is_neutral: bool) -> Tuple[float, float]:
        """
        Calcola l'xG previsto per una partita.
        
        Args:
            home_xg_data: Dati xG squadra di casa
            away_xg_data: Dati xG squadra in trasferta
            is_neutral: Se la partita è in campo neutro
            
        Returns:
            Coppia di xG previsti (casa, trasferta)
        """
        if is_neutral:
            # In campo neutro, usa la media di casa e trasferta
            home_xg = (home_xg_data['home_xg_for'] + home_xg_data['away_xg_for']) / 2
            away_xg = (away_xg_data['home_xg_for'] + away_xg_data['away_xg_for']) / 2
        else:
            # Attacco casa vs difesa trasferta
            home_xg = (home_xg_data['home_xg_for'] + away_xg_data['away_xg_against']) / 2
            # Attacco trasferta vs difesa casa
            away_xg = (away_xg_data['away_xg_for'] + home_xg_data['home_xg_against']) / 2
        
        return home_xg, away_xg
    
    def _adjust_probabilities_with_xg(self, basic_probs: Dict[str, float], 
                                     home_xg: float, away_xg: float) -> Dict[str, float]:
        """
        Aggiusta le probabilità di base usando xG.
        
        Args:
            basic_probs: Probabilità dal modello base
            home_xg: xG previsto per la squadra di casa
            away_xg: xG previsto per la squadra in trasferta
            
        Returns:
            Probabilità aggiustate
        """
        # Calcoliamo le probabilità basate su Poisson usando xG
        xg_probs = {}
        
        # Calcola probabilità 1
        home_win_prob = 0
        for h in range(10):  # Consideriamo fino a 9 gol
            home_score_prob = math.exp(-home_xg) * (home_xg ** h) / math.factorial(h)
            for a in range(h):  # Casa vince
                away_score_prob = math.exp(-away_xg) * (away_xg ** a) / math.factorial(a)
                home_win_prob += home_score_prob * away_score_prob
        xg_probs['1'] = home_win_prob
        
        # Calcola probabilità X
        draw_prob = 0
        for score in range(10):  # Consideriamo fino a 9 gol
            home_score_prob = math.exp(-home_xg) * (home_xg ** score) / math.factorial(score)
            away_score_prob = math.exp(-away_xg) * (away_xg ** score) / math.factorial(score)
            draw_prob += home_score_prob * away_score_prob
        xg_probs['X'] = draw_prob
        
        # Calcola probabilità 2
        away_win_prob = 0
        for a in range(10):  # Consideriamo fino a 9 gol
            away_score_prob = math.exp(-away_xg) * (away_xg ** a) / math.factorial(a)
            for h in range(a):  # Trasferta vince
                home_score_prob = math.exp(-home_xg) * (home_xg ** h) / math.factorial(h)
                away_win_prob += home_score_prob * away_score_prob
        xg_probs['2'] = away_win_prob
        
        # Normalizza le probabilità perché potrebbero non sommare esattamente a 1
        total = sum(xg_probs.values())
        for k in xg_probs:
            xg_probs[k] /= total
        
        # Combina le probabilità di base con quelle basate su xG
        adjusted_probs = {}
        for k in basic_probs:
            adjusted_probs[k] = (1 - self.xg_weight) * basic_probs[k] + self.xg_weight * xg_probs[k]
        
        # Normalizza le probabilità finali
        total = sum(adjusted_probs.values())
        for k in adjusted_probs:
            adjusted_probs[k] /= total
            
        return adjusted_probs
    
    def _calculate_btts(self, home_xg: float, away_xg: float) -> Dict[str, float]:
        """
        Calcola le probabilità di Both Teams To Score.
        
        Args:
            home_xg: xG previsto per la squadra di casa
            away_xg: xG previsto per la squadra in trasferta
            
        Returns:
            Probabilità BTTS
        """
        # Probabilità che la squadra di casa non segni
        home_no_goal = math.exp(-home_xg)
        # Probabilità che la squadra in trasferta non segni
        away_no_goal = math.exp(-away_xg)
        
        # Probabilità BTTS Yes = 1 - (prob nessun gol casa + prob nessun gol trasferta - prob entrambe non segnano)
        btts_yes = 1 - (home_no_goal + away_no_goal - home_no_goal * away_no_goal)
        btts_no = 1 - btts_yes
        
        return {
            'Yes': btts_yes,
            'No': btts_no
        }
    
    def _calculate_over_under(self, home_xg: float, away_xg: float) -> Dict[str, Dict[str, float]]:
        """
        Calcola le probabilità Over/Under.
        
        Args:
            home_xg: xG previsto per la squadra di casa
            away_xg: xG previsto per la squadra in trasferta
            
        Returns:
            Probabilità Over/Under per diverse linee
        """
        total_xg = home_xg + away_xg
        result = {}
        
        # Calcola per linee comuni
        for line in [0.5, 1.5, 2.5, 3.5, 4.5]:
            # Probabilità che ci siano esattamente k gol
            prob_under = 0
            for k in range(int(line) + 1):
                prob_under += math.exp(-total_xg) * (total_xg ** k) / math.factorial(k)
            
            # Over = 1 - Under
            prob_over = 1 - prob_under
            
            result[str(line)] = {
                'Over': prob_over,
                'Under': prob_under
            }
            
        return result
    
    def _calculate_exact_score_probabilities(self, home_xg: float, away_xg: float) -> Dict[str, float]:
        """
        Calcola le probabilità dei risultati esatti.
        
        Args:
            home_xg: xG previsto per la squadra di casa
            away_xg: xG previsto per la squadra in trasferta
            
        Returns:
            Probabilità per ogni risultato esatto fino a 5-5
        """
        results = {}
        total_prob = 0
        
        # Calcola probabilità per ogni score fino a 5-5
        for h in range(6):
            for a in range(6):
                # Probabilità Poisson per questo score
                home_prob = math.exp(-home_xg) * (home_xg ** h) / math.factorial(h)
                away_prob = math.exp(-away_xg) * (away_xg ** a) / math.factorial(a)
                score_prob = home_prob * away_prob
                
                score_key = f"{h}-{a}"
                results[score_key] = score_prob
                total_prob += score_prob
        
        # Aggiungi una categoria "Altro" per altri risultati
        results["Altro"] = 1 - total_prob
        
        # Ordina per probabilità decrescente e mantieni solo i top 10
        sorted_results = dict(sorted(results.items(), key=lambda x: x[1], reverse=True)[:10])
        
        return sorted_results
    
    def _calculate_asian_handicaps(self, home_xg: float, away_xg: float) -> Dict[str, Dict[str, float]]:
        """
        Calcola le probabilità per handicap asiatici.
        
        Args:
            home_xg: xG previsto per la squadra di casa
            away_xg: xG previsto per la squadra in trasferta
            
        Returns:
            Probabilità per vari handicap asiatici
        """
        handicaps = {}
        
        # Calcola per le linee comuni
        for line in [-2.0, -1.5, -1.0, -0.5, 0, 0.5, 1.0, 1.5, 2.0]:
            results = {}
            
            # Calcola percentuale vittoria con handicap per ogni possibile score
            home_prob = 0
            away_prob = 0
            draw_prob = 0
            
            for h in range(10):
                home_score_prob = math.exp(-home_xg) * (home_xg ** h) / math.factorial(h)
                for a in range(10):
                    away_score_prob = math.exp(-away_xg) * (away_xg ** a) / math.factorial(a)
                    
                    score_prob = home_score_prob * away_score_prob
                    adjusted_diff = (h - a) - line
                    
                    if adjusted_diff > 0:  # Squadra di casa vince con l'handicap
                        home_prob += score_prob
                    elif adjusted_diff < 0:  # Squadra in trasferta vince con l'handicap  
                        away_prob += score_prob
                    else:  # Pareggio con l'handicap (solo per linee intere)
                        draw_prob += score_prob
            
            if line == int(line):  # Linea intera
                # Per linee intere, il pareggio viene diviso
                home_prob += draw_prob / 2
                away_prob += draw_prob / 2
                results = {
                    'home': home_prob,
                    'away': away_prob
                }
            else:  # Linea con mezzo gol
                results = {
                    'home': home_prob,
                    'away': away_prob
                }
                
            handicaps[str(line)] = results
            
        return handicaps
    
    def _get_used_xg_sources(self, match_data: Dict[str, Any]) -> List[str]:
        """
        Determina quali fonti xG sono state utilizzate per questa partita.
        
        Args:
            match_data: Dati della partita
            
        Returns:
            Lista di fonti xG utilizzate
        """
        used_sources = []
        for source in self.xg_sources:
            if source in match_data.get('xg_sources', []):
                used_sources.append(source)
        return used_sources
    
    def _add_xg_reasoning(self, prediction: Dict[str, Any], 
                         home_xg_data: Dict[str, Any],
                         away_xg_data: Dict[str, Any]) -> List[str]:
        """
        Aggiunge motivazioni basate su xG alla previsione.
        
        Args:
            prediction: Previsione corrente
            home_xg_data: Dati xG squadra di casa
            away_xg_data: Dati xG squadra in trasferta
            
        Returns:
            Lista di motivazioni
        """
        reasoning = prediction.get('reasoning', [])
        
        # Aggiungi motivazioni xG
        home_team = prediction['home_team']
        away_team = prediction['away_team']
        
        # Qualità offensiva in base a xG
        reasoning.append(f"{home_team} ha creato una media di {home_xg_data['home_xg_for']:.2f} xG in casa.")
        reasoning.append(f"{away_team} ha creato una media di {away_xg_data['away_xg_for']:.2f} xG in trasferta.")
        
        # Qualità difensiva in base a xG
        reasoning.append(f"{home_team} ha concesso una media di {home_xg_data['home_xg_against']:.2f} xG in casa.")
        reasoning.append(f"{away_team} ha concesso una media di {away_xg_data['away_xg_against']:.2f} xG in trasferta.")
        
        # Previsione xG
        reasoning.append(f"Il modello prevede {prediction['home_xg']:.2f} xG per {home_team} e "
                         f"{prediction['away_xg']:.2f} xG per {away_team}.")
        
        # Valore mercati
        if prediction['btts']['Yes'] > 0.6:
            reasoning.append(f"Alta probabilità ({prediction['btts']['Yes']:.0%}) che entrambe le squadre segnino.")
        
        main_ou_line = "2.5"
        if main_ou_line in prediction['over_under']:
            if prediction['over_under'][main_ou_line]['Over'] > 0.6:
                reasoning.append(f"Alta probabilità ({prediction['over_under'][main_ou_line]['Over']:.0%}) "
                                f"di Over {main_ou_line} gol.")
            elif prediction['over_under'][main_ou_line]['Under'] > 0.6:
                reasoning.append(f"Alta probabilità ({prediction['over_under'][main_ou_line]['Under']:.0%}) "
                                f"di Under {main_ou_line} gol.")
        
        return reasoning


# Funzione per creare un'istanza del modello
def create_xg_model() -> XGModel:
    """
    Crea un'istanza del modello xG.
    
    Returns:
        Istanza del modello xG
    """
    return XGModel()
