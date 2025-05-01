"""
Modello di base per la previsione dei risultati delle partite.

Questo modulo implementa un modello statistico base che utilizza dati storici
per generare previsioni sui risultati delle partite di calcio.
"""

import logging
import math
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union, Tuple
from collections import defaultdict

from src.data.processors.matches import get_processor as get_match_processor
from src.data.processors.teams import get_processor as get_team_processor
from src.data.processors.head_to_head import get_processor as get_h2h_processor
from src.utils.database import FirebaseManager
from src.utils.cache import cached

# Configurazione logger
logger = logging.getLogger(__name__)

class BasicModel:
    """
    Implementa un modello base per la previsione dei risultati delle partite.
    
    Questo modello utilizza principalmente la forma recente delle squadre, i risultati
    degli scontri diretti e il fattore campo per prevedere il risultato di una partita.
    """
    
    def __init__(self, db: Optional[FirebaseManager] = None):
        """
        Inizializza il modello base.
        
        Args:
            db: Istanza di FirebaseManager. Se None, ne verrà creata una nuova.
        """
        self.db = db or FirebaseManager()
        self.match_processor = get_match_processor()
        self.team_processor = get_team_processor()
        self.h2h_processor = get_h2h_processor()
        
        # Pesi di default per i diversi fattori
        self.weights = {
            'home_advantage': 0.15,       # Vantaggio di giocare in casa
            'recent_form': 0.30,          # Forma recente (ultimi 5 match)
            'head_to_head': 0.25,         # Scontri diretti
            'league_position': 0.15,      # Posizione in classifica
            'attack_defense': 0.15,       # Forza offensiva e difensiva
        }
        
        # Cache per i risultati
        self.prediction_cache = {}
    
    def predict_match(
        self, 
        match_id: str,
        home_team_id: str,
        away_team_id: str,
        match_date: Optional[str] = None,
        league_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Prevede il risultato di una partita.
        
        Args:
            match_id: ID della partita.
            home_team_id: ID della squadra di casa.
            away_team_id: ID della squadra in trasferta.
            match_date: Data della partita (opzionale).
            league_id: ID del campionato (opzionale).
            
        Returns:
            Dizionario con la previsione completa.
        """
        # Controlla se esiste già una previsione aggiornata
        existing_prediction = self._get_stored_prediction(match_id)
        if existing_prediction:
            return existing_prediction
        
        # Carica i dati delle squadre
        home_team = self.team_processor.get_stored_team_data(home_team_id)
        away_team = self.team_processor.get_stored_team_data(away_team_id)
        
        if not home_team or not away_team:
            logger.error(f"Dati squadre mancanti per {home_team_id} e/o {away_team_id}")
            return {
                'match_id': match_id,
                'home_team_id': home_team_id,
                'away_team_id': away_team_id,
                'prediction': 'Unknown',
                'error': 'Missing team data'
            }
        
        # Ottieni i dati degli scontri diretti
        h2h_data = self.h2h_processor.get_head_to_head(
            home_team_id, 
            away_team_id, 
            min_matches=0  # Accetta anche se non ci sono match h2h
        )
        
        # Inizializza la previsione
        prediction = {
            'match_id': match_id,
            'home_team_id': home_team_id,
            'away_team_id': away_team_id,
            'home_team_name': home_team.get('name', ''),
            'away_team_name': away_team.get('name', ''),
            'match_date': match_date,
            'league_id': league_id,
            'prediction': '',
            'probabilities': {
                'home_win': 0.0,
                'draw': 0.0,
                'away_win': 0.0
            },
            'expected_goals': {
                'home': 0.0,
                'away': 0.0
            },
            'score_probabilities': {},
            'factors': {},
            'additional_predictions': {},
            'last_updated': datetime.now().isoformat()
        }
        
        # Calcola le probabilità di base
        try:
            self._calculate_1x2_probabilities(prediction, home_team, away_team, h2h_data)
            
            # Solo se sono disponibili le probabilità principali, procedi con le altre
            if prediction['probabilities']['home_win'] > 0:
                self._calculate_expected_goals(prediction, home_team, away_team, h2h_data)
                self._calculate_score_probabilities(prediction)
                self._calculate_additional_predictions(prediction, home_team, away_team, h2h_data)
            
            # Imposta la previsione principale in base alla probabilità più alta
            probs = prediction['probabilities']
            if probs['home_win'] >= probs['draw'] and probs['home_win'] >= probs['away_win']:
                prediction['prediction'] = 'Home Win'
            elif probs['away_win'] >= probs['draw'] and probs['away_win'] >= probs['home_win']:
                prediction['prediction'] = 'Away Win'
            else:
                prediction['prediction'] = 'Draw'
            
            # Aggiungi confidenza
            max_prob = max(probs.values())
            prediction['confidence'] = round(max_prob * 100, 1)
            
            # Salva la previsione
            self._store_prediction(prediction)
            
        except Exception as e:
            logger.error(f"Errore nella generazione della previsione: {e}")
            prediction['error'] = f"Prediction error: {str(e)}"
        
        return prediction
    
    def _calculate_1x2_probabilities(
        self, 
        prediction: Dict[str, Any],
        home_team: Dict[str, Any],
        away_team: Dict[str, Any],
        h2h_data: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Calcola le probabilità 1X2 (Vittoria Casa, Pareggio, Vittoria Trasferta).
        
        Args:
            prediction: Dizionario della previsione da aggiornare.
            home_team: Dati della squadra di casa.
            away_team: Dati della squadra in trasferta.
            h2h_data: Dati degli scontri diretti (opzionale).
        """
        # Calcola i fattori individuali
        factors = {}
        
        # 1. Vantaggio del fattore campo
        factors['home_advantage'] = self._calculate_home_advantage(home_team, away_team)
        
        # 2. Forma recente
        factors['recent_form'] = self._calculate_recent_form(home_team, away_team)
        
        # 3. Scontri diretti
        factors['head_to_head'] = self._calculate_head_to_head(h2h_data, home_team['team_id'])
        
        # 4. Posizione in classifica
        factors['league_position'] = self._calculate_league_position(home_team, away_team)
        
        # 5. Forza offensiva/difensiva
        factors['attack_defense'] = self._calculate_attack_defense(home_team, away_team)
        
        # Salva i fattori nella previsione
        prediction['factors'] = factors
        
        # Calcola una probabilità complessiva pesata
        home_win_prob = 0.0
        away_win_prob = 0.0
        draw_prob = 0.0
        
        for factor_name, factor_probs in factors.items():
            weight = self.weights.get(factor_name, 0.0)
            home_win_prob += factor_probs.get('home_win', 0.0) * weight
            away_win_prob += factor_probs.get('away_win', 0.0) * weight
            draw_prob += factor_probs.get('draw', 0.0) * weight
        
        # Normalizza le probabilità
        total_prob = home_win_prob + away_win_prob + draw_prob
        if total_prob > 0:
            home_win_prob /= total_prob
            away_win_prob /= total_prob
            draw_prob /= total_prob
        
        # Aggiorna la previsione
        prediction['probabilities'] = {
            'home_win': round(home_win_prob, 3),
            'draw': round(draw_prob, 3),
            'away_win': round(away_win_prob, 3)
        }
    
    def _calculate_home_advantage(
        self, 
        home_team: Dict[str, Any],
        away_team: Dict[str, Any]
    ) -> Dict[str, float]:
        """
        Calcola il fattore del vantaggio casalingo.
        
        Args:
            home_team: Dati della squadra di casa.
            away_team: Dati della squadra in trasferta.
            
        Returns:
            Probabilità basate sul vantaggio casalingo.
        """
        # Probabilità di base: il fattore campo dà un vantaggio alla squadra di casa
        home_win_prob = 0.45  # Probabilità base
        away_win_prob = 0.30
        draw_prob = 0.25
        
        # Se ci sono statistiche più specifiche, usale
        if 'statistics' in home_team and 'home_stats' in home_team['statistics']:
            home_stats = home_team['statistics']['home_stats']
            
            # Forma in casa
            if 'win_rate' in home_stats:
                home_win_prob = home_stats['win_rate']
                draw_prob = home_stats.get('draw_rate', 0.25)
                away_win_prob = 1.0 - home_win_prob - draw_prob
        
        # Allo stesso modo, se ci sono statistiche per la squadra in trasferta
        if 'statistics' in away_team and 'away_stats' in away_team['statistics']:
            away_stats = away_team['statistics']['away_stats']
            
            # Forma in trasferta
            if 'win_rate' in away_stats:
                away_win_rate = away_stats['win_rate']
                # Combina con le probabilità precedenti
                away_win_prob = (away_win_prob + away_win_rate) / 2
                
                # Aggiusta le altre probabilità
                total = home_win_prob + away_win_prob + draw_prob
                if total != 1.0:
                    factor = 1.0 / total
                    home_win_prob *= factor
                    away_win_prob *= factor
                    draw_prob *= factor
        
        return {
            'home_win': home_win_prob,
            'draw': draw_prob,
            'away_win': away_win_prob
        }
    
    def _calculate_recent_form(
        self, 
        home_team: Dict[str, Any],
        away_team: Dict[str, Any]
    ) -> Dict[str, float]:
        """
        Calcola le probabilità basate sulla forma recente.
        
        Args:
            home_team: Dati della squadra di casa.
            away_team: Dati della squadra in trasferta.
            
        Returns:
            Probabilità basate sulla forma recente.
        """
        # Estrai la forma recente (se disponibile)
        home_form = home_team.get('form', '')
        away_form = away_team.get('form', '')
        
        # Se non ci sono dati recenti, usa probabilità base
        if not home_form and not away_form:
            return {
                'home_win': 0.40,
                'draw': 0.25,
                'away_win': 0.35
            }
        
        # Calcola il punteggio di forma per ogni squadra
        home_form_score = self._calculate_form_score(home_form)
        away_form_score = self._calculate_form_score(away_form)
        
        # Converti i punteggi di forma in probabilità
        total_score = home_form_score + away_form_score
        
        if total_score > 0:
            home_win_prob = home_form_score / (total_score * 1.2)  # Ridotto per considerare la possibilità di pareggio
            away_win_prob = away_form_score / (total_score * 1.2)
            draw_prob = 1.0 - home_win_prob - away_win_prob
        else:
            # Caso fallback
            home_win_prob = 0.40
            away_win_prob = 0.35
            draw_prob = 0.25
        
        # Limita le probabilità
        home_win_prob = max(0.1, min(0.8, home_win_prob))
        away_win_prob = max(0.1, min(0.8, away_win_prob))
        draw_prob = max(0.1, min(0.8, draw_prob))
        
        # Normalizza
        total = home_win_prob + away_win_prob + draw_prob
        if total != 1.0:
            factor = 1.0 / total
            home_win_prob *= factor
            away_win_prob *= factor
            draw_prob *= factor
        
        return {
            'home_win': home_win_prob,
            'draw': draw_prob,
            'away_win': away_win_prob
        }
    
    def _calculate_form_score(self, form_string: str) -> float:
        """
        Calcola un punteggio dalla stringa di forma.
        
        Args:
            form_string: Stringa di forma (es. "WWDLW").
            
        Returns:
            Punteggio di forma.
        """
        if not form_string:
            return 50.0  # Punteggio neutro
        
        # Punteggi per ogni risultato
        scores = {
            'W': 3.0,   # Vittoria
            'D': 1.0,   # Pareggio
            'L': 0.0    # Sconfitta
        }
        
        # Il peso diminuisce con l'anzianità del risultato
        weights = [1.0, 0.8, 0.6, 0.4, 0.2]
        
        total_score = 0.0
        total_weight = 0.0
        
        for i, result in enumerate(form_string[:5]):
            if result in scores:
                weight = weights[i] if i < len(weights) else 0.1
                total_score += scores[result] * weight
                total_weight += weight
        
        if total_weight > 0:
            # Normalizza a 100
            normalized_score = (total_score / total_weight) * (100/3)
            return normalized_score
        
        return 50.0  # Punteggio neutro
    
    def _calculate_head_to_head(
        self, 
        h2h_data: Optional[Dict[str, Any]],
        home_team_id: str
    ) -> Dict[str, float]:
        """
        Calcola le probabilità basate sugli scontri diretti.
        
        Args:
            h2h_data: Dati degli scontri diretti.
            home_team_id: ID della squadra di casa.
            
        Returns:
            Probabilità basate sugli scontri diretti.
        """
        # Se non ci sono dati h2h, usa probabilità neutrali
        if not h2h_data or 'error' in h2h_data or not h2h_data.get('stats'):
            return {
                'home_win': 0.35,
                'draw': 0.30,
                'away_win': 0.35
            }
        
        # Estrai le statistiche h2h
        stats = h2h_data['stats']
        
        # Identifica quale squadra è quale negli h2h
        is_home_team_team1 = h2h_data['team1_id'] == home_team_id
        
        if is_home_team_team1:
            home_wins = stats.get('team1_wins', 0)
            away_wins = stats.get('team2_wins', 0)
        else:
            home_wins = stats.get('team2_wins', 0)
            away_wins = stats.get('team1_wins', 0)
        
        draws = stats.get('draws', 0)
        total = home_wins + away_wins + draws
        
        # Previeni divisione per zero
        if total == 0:
            return {
                'home_win': 0.35,
                'draw': 0.30,
                'away_win': 0.35
            }
        
        # Calcola le probabilità basate sulle frequenze
        home_win_prob = home_wins / total
        away_win_prob = away_wins / total
        draw_prob = draws / total
        
        # Aggiusta leggermente verso la media
        home_win_prob = 0.8 * home_win_prob + 0.2 * 0.45  # 45% è media storica vittorie in casa
        away_win_prob = 0.8 * away_win_prob + 0.2 * 0.30  # 30% è media storica vittorie in trasferta
        draw_prob = 0.8 * draw_prob + 0.2 * 0.25         # 25% è media storica pareggi
        
        # Normalizza
        total = home_win_prob + away_win_prob + draw_prob
        if total != 1.0:
            factor = 1.0 / total
            home_win_prob *= factor
            away_win_prob *= factor
            draw_prob *= factor
        
        return {
            'home_win': home_win_prob,
            'draw': draw_prob,
            'away_win': away_win_prob
        }
    
    def _calculate_league_position(
        self, 
        home_team: Dict[str, Any],
        away_team: Dict[str, Any]
    ) -> Dict[str, float]:
        """
        Calcola le probabilità basate sulle posizioni in classifica.
        
        Args:
            home_team: Dati della squadra di casa.
            away_team: Dati della squadra in trasferta.
            
        Returns:
            Probabilità basate sulle posizioni in classifica.
        """
        # Estrai le posizioni (se disponibili)
        home_pos = home_team.get('current_position')
        away_pos = away_team.get('current_position')
        
        # Se non ci sono dati di posizione, usa probabilità neutrali
        if home_pos is None or away_pos is None:
            return {
                'home_win': 0.40,
                'draw': 0.25,
                'away_win': 0.35
            }
        
        # Calcola la differenza di posizione (valore positivo = home_team è migliore)
        # Nota: posizioni più basse = migliori
        pos_diff = away_pos - home_pos
        
        # Converti la differenza di posizione in probabilità
        if pos_diff > 0:  # Home team è in posizione migliore
            # Più grande è la differenza, più alta è la probabilità di vittoria in casa
            advantage = min(pos_diff / 10.0, 0.4)  # Limita il vantaggio
            home_win_prob = 0.40 + advantage
            away_win_prob = 0.35 - advantage * 0.7  # Riduci meno della vittoria in casa
            draw_prob = 1.0 - home_win_prob - away_win_prob
        elif pos_diff < 0:  # Away team è in posizione migliore
            # Differenza negativa = vantaggio per la squadra in trasferta
            advantage = min(abs(pos_diff) / 10.0, 0.3)  # Vantaggio leggermente inferiore per la trasferta
            away_win_prob = 0.35 + advantage
            home_win_prob = 0.40 - advantage * 0.8
            draw_prob = 1.0 - home_win_prob - away_win_prob
        else:  # Stessa posizione
            home_win_prob = 0.40
            away_win_prob = 0.35
            draw_prob = 0.25
        
        # Limita e normalizza le probabilità
        home_win_prob = max(0.1, min(0.8, home_win_prob))
        away_win_prob = max(0.1, min(0.8, away_win_prob))
        draw_prob = max(0.1, min(0.8, draw_prob))
        
        total = home_win_prob + away_win_prob + draw_prob
        if total != 1.0:
            factor = 1.0 / total
            home_win_prob *= factor
            away_win_prob *= factor
            draw_prob *= factor
        
        return {
            'home_win': home_win_prob,
            'draw': draw_prob,
            'away_win': away_win_prob
        }
    
    def _calculate_attack_defense(
        self, 
        home_team: Dict[str, Any],
        away_team: Dict[str, Any]
    ) -> Dict[str, float]:
        """
        Calcola le probabilità basate sulla forza offensiva e difensiva.
        
        Args:
            home_team: Dati della squadra di casa.
            away_team: Dati della squadra in trasferta.
            
        Returns:
            Probabilità basate sulla forza offensiva e difensiva.
        """
        # Estrai statistiche offensive/difensive (se disponibili)
        home_stats = home_team.get('statistics', {})
        away_stats = away_team.get('statistics', {})
        
        # Valori predefiniti
        home_attack = 100.0  # Valori neutri
        home_defense = 100.0
        away_attack = 100.0
        away_defense = 100.0
        
        # Estrai valori da statistiche (se disponibili)
        if 'attack_strength' in home_stats:
            home_attack = home_stats['attack_strength']
        elif 'goals_for' in home_stats and 'matches_played' in home_stats and home_stats['matches_played'] > 0:
            home_attack = (home_stats['goals_for'] / home_stats['matches_played']) * 100
        
        if 'defense_strength' in home_stats:
            home_defense = home_stats['defense_strength']
        elif 'goals_against' in home_stats and 'matches_played' in home_stats and home_stats['matches_played'] > 0:
            # Meno gol subiti = difesa migliore, quindi invertiamo
            home_defense = 100 / ((home_stats['goals_against'] / home_stats['matches_played']) + 0.1)
        
        # Ripeti per la squadra in trasferta
        if 'attack_strength' in away_stats:
            away_attack = away_stats['attack_strength']
        elif 'goals_for' in away_stats and 'matches_played' in away_stats and away_stats['matches_played'] > 0:
            away_attack = (away_stats['goals_for'] / away_stats['matches_played']) * 100
        
        if 'defense_strength' in away_stats:
            away_defense = away_stats['defense_strength']
        elif 'goals_against' in away_stats and 'matches_played' in away_stats and away_stats['matches_played'] > 0:
            away_defense = 100 / ((away_stats['goals_against'] / away_stats['matches_played']) + 0.1)
        
        # Calcola il rapporto di forza
        home_strength = (home_attack * 0.6 + home_defense * 0.4) * 1.1  # 10% in più per il fattore campo
        away_strength = away_attack * 0.6 + away_defense * 0.4
        
        total_strength = home_strength + away_strength
        
        if total_strength > 0:
            # Converti in probabilità, lasciando spazio per i pareggi
            home_win_prob = (home_strength / total_strength) * 0.75
            away_win_prob = (away_strength / total_strength) * 0.75
            draw_prob = 1.0 - home_win_prob - away_win_prob
        else:
            # Fallback
            home_win_prob = 0.40
            away_win_prob = 0.35
            draw_prob = 0.25
        
        # Limita e normalizza
        home_win_prob = max(0.1, min(0.8, home_win_prob))
        away_win_prob = max(0.1, min(0.8, away_win_prob))
        draw_prob = max(0.1, min(0.8, draw_prob))
        
        total = home_win_prob + away_win_prob + draw_prob
        if total != 1.0:
            factor = 1.0 / total
            home_win_prob *= factor
            away_win_prob *= factor
            draw_prob *= factor
        
        return {
            'home_win': home_win_prob,
            'draw': draw_prob,
            'away_win': away_win_prob
        }
    
    def _calculate_expected_goals(
        self, 
        prediction: Dict[str, Any],
        home_team: Dict[str, Any],
        away_team: Dict[str, Any],
        h2h_data: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Calcola i gol attesi per entrambe le squadre.
        
        Args:
            prediction: Dizionario della previsione da aggiornare.
            home_team: Dati della squadra di casa.
            away_team: Dati della squadra in trasferta.
            h2h_data: Dati degli scontri diretti (opzionale).
        """
        # Valori base per i gol attesi
        home_xg = 1.35  # Media storica dei gol segnati in casa
        away_xg = 1.05  # Media storica dei gol segnati in trasferta
        
        # 1. Considera la forza offensiva e difensiva
        home_stats = home_team.get('statistics', {})
        away_stats = away_team.get('statistics', {})
        
        # Forza offensiva casa vs difensiva trasferta
        if 'goals_for' in home_stats and 'matches_played' in home_stats and home_stats['matches_played'] > 0:
            home_off = home_stats['goals_for'] / home_stats['matches_played']
            home_xg = home_xg * (home_off / 1.35)  # Normalizza rispetto alla media
        
        if 'goals_against' in away_stats and 'matches_played' in away_stats and away_stats['matches_played'] > 0:
            away_def = away_stats['goals_against'] / away_stats['matches_played']
            home_xg = home_xg * (away_def / 1.35)  # Più gol subiti = più gol attesi
        
        # Forza offensiva trasferta vs difensiva casa
        if 'goals_for' in away_stats and 'matches_played' in away_stats and away_stats['matches_played'] > 0:
            away_off = away_stats['goals_for'] / away_stats['matches_played']
            away_xg = away_xg * (away_off / 1.05)  # Normalizza rispetto alla media
        
        if 'goals_against' in home_stats and 'matches_played' in home_stats and home_stats['matches_played'] > 0:
            home_def = home_stats['goals_against'] / home_stats['matches_played']
            away_xg = away_xg * (home_def / 1.05)
        
        # 2. Considera i dati Understat xG (se disponibili)
        if 'expected_goals' in home_team:
            home_xg_stats = home_team['expected_goals']
            if 'xG' in home_xg_stats:
                home_xg = (home_xg + float(home_xg_stats['xG'])) / 2
        
        if 'expected_goals' in away_team:
            away_xg_stats = away_team['expected_goals']
            if 'xG' in away_xg_stats:
                away_xg = (away_xg + float(away_xg_stats['xG'])) / 2
        
        # 3. Considera gli scontri diretti
        if h2h_data and 'stats' in h2h_data:
            h2h_stats = h2h_data['stats']
            
            if 'team1_goals' in h2h_stats and 'team2_goals' in h2h_stats and 'total_matches' in h2h_stats:
                total_matches = h2h_stats['total_matches']
                
                if total_matches > 0:
                    # Identifica quale squadra è quale
                    is_home_team_team1 = h2h_data['team1_id'] == home_team['team_id']
                    
                    if is_home_team_team1:
                        h2h_home_goals = h2h_stats['team1_goals'] / total_matches
                        h2h_away_goals = h2h_stats['team2_goals'] / total_matches
                    else:
                        h2h_home_goals = h2h_stats['team2_goals'] / total_matches
                        h2h_away_goals = h2h_stats['team1_goals'] / total_matches
                    
                    # Combina con xG attuale (peso 30% per h2h)
                    home_xg = 0.7 * home_xg + 0.3 * h2h_home_goals
                    away_xg = 0.7 * away_xg + 0.3 * h2h_away_goals
        
        # Limita i valori a un range ragionevole
        home_xg = max(0.3, min(5.0, home_xg))
        away_xg = max(0.2, min(4.0, away_xg))
        
        # Aggiorna la previsione
        prediction['expected_goals'] = {
            'home': round(home_xg, 2),
            'away': round(away_xg, 2)
        }
    
    def _calculate_score_probabilities(self, prediction: Dict[str, Any]) -> None:
        """
        Calcola le probabilità dei vari risultati esatti.
        
        Args:
            prediction: Dizionario della previsione da aggiornare.
        """
        # Ottieni i gol attesi
        home_xg = prediction['expected_goals']['home']
        away_xg = prediction['expected_goals']['away']
        
        # Calcola la probabilità di diversi risultati esatti usando la distribuzione di Poisson
        score_probs = {}
        
        # Calcola probabilità per punteggi fino a 5-5
        max_goals = 5
        
        for home_goals in range(max_goals + 1):
            for away_goals in range(max_goals + 1):
                score = f"{home_goals}-{away_goals}"
                probability = self._poisson_probability(home_goals, home_xg) * self._poisson_probability(away_goals, away_xg)
                score_probs[score] = round(probability, 4)
        
        # Calcola la probabilità residua (tutti gli altri risultati)
        total_prob = sum(score_probs.values())
        if total_prob < 1.0:
            remaining = 1.0 - total_prob
            score_probs['other'] = round(remaining, 4)
        
        # Ordina per probabilità (decrescente)
        sorted_scores = sorted(score_probs.items(), key=lambda x: x[1], reverse=True)
        
        # Utilizza un dizionario ordinato
        prediction['score_probabilities'] = {score: prob for score, prob in sorted_scores[:10]}  # Top 10 risultati
    
    def _poisson_probability(self, k: int, mean: float) -> float:
        """
        Calcola la probabilità di ottenere esattamente k eventi in una distribuzione di Poisson.
        
        Args:
            k: Numero di eventi.
            mean: Media attesa.
            
        Returns:
            Probabilità di ottenere esattamente k eventi.
        """
        if mean <= 0:
            return 0.0 if k > 0 else 1.0
        
        return math.exp(-mean) * (mean ** k) / math.factorial(k)
    
    def _calculate_additional_predictions(
        self, 
        prediction: Dict[str, Any],
        home_team: Dict[str, Any],
        away_team: Dict[str, Any],
        h2h_data: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Calcola previsioni aggiuntive (BTTS, Over/Under, etc.).
        
        Args:
            prediction: Dizionario della previsione da aggiornare.
            home_team: Dati della squadra di casa.
            away_team: Dati della squadra in trasferta.
            h2h_data: Dati degli scontri diretti (opzionale).
        """
        additional = {}
        
        # Ottieni i gol attesi
        home_xg = prediction['expected_goals']['home']
        away_xg = prediction['expected_goals']['away']
        total_xg = home_xg + away_xg
        
        # 1. Both Teams To Score (BTTS)
        # Probabilità che entrambe le squadre segnino (1 - P(home=0) - P(away=0) + P(home=0 & away=0))
        p_home_0 = self._poisson_probability(0, home_xg)
        p_away_0 = self._poisson_probability(0, away_xg)
        p_btts_yes = 1 - p_home_0 - p_away_0 + (p_home_0 * p_away_0)
        
        additional['btts'] = {
            'yes': round(p_btts_yes, 3),
            'no': round(1 - p_btts_yes, 3)
        }
        
        # 2. Over/Under 2.5 Goals
        # Probabilità che ci siano più/meno di 2.5 gol
        p_under_2_5 = 0.0
        for h in range(3):
            for a in range(3):
                if h + a < 3:
                    p_under_2_5 += self._poisson_probability(h, home_xg) * self._poisson_probability(a, away_xg)
        
        additional['over_under_2_5'] = {
            'over': round(1 - p_under_2_5, 3),
            'under': round(p_under_2_5, 3)
        }
        
        # 3. Doppia Chance (1X, X2, 12)
        home_win = prediction['probabilities']['home_win']
        draw = prediction['probabilities']['draw']
        away_win = prediction['probabilities']['away_win']
        
        additional['double_chance'] = {
            '1X': round(home_win + draw, 3),
            'X2': round(draw + away_win, 3),
            '12': round(home_win + away_win, 3)
        }
        
        # 4. Draw No Bet
        # Normalizza le probabilità di vittoria escludendo il pareggio
        total_wins = home_win + away_win
        if total_wins > 0:
            additional['draw_no_bet'] = {
                'home': round(home_win / total_wins, 3),
                'away': round(away_win / total_wins, 3)
            }
        else:
            additional['draw_no_bet'] = {
                'home': 0.5,
                'away': 0.5
            }
        
        # 5. Handicap asiatico
        # Calcola le probabilità con diversi handicap
        additional['asian_handicap'] = self._calculate_asian_handicaps(home_xg, away_xg)
        
        # 6. Margine di vittoria
        additional['win_margin'] = self._calculate_win_margins(home_xg, away_xg)
        
        # 7. Numero di gol
        additional['total_goals'] = self._calculate_total_goals(home_xg, away_xg)
        
        # Aggiorna la previsione
        prediction['additional_predictions'] = additional
    
    def _calculate_asian_handicaps(self, home_xg: float, away_xg: float) -> Dict[str, Dict[str, float]]:
        """
        Calcola le probabilità per diversi handicap asiatici.
        
        Args:
            home_xg: Gol attesi della squadra di casa.
            away_xg: Gol attesi della squadra in trasferta.
            
        Returns:
            Dizionario con le probabilità per diversi handicap.
        """
        handicaps = {}
        
        # Consideriamo alcuni handicap comuni
        for handicap in [-2.0, -1.5, -1.0, -0.5, 0, 0.5, 1.0, 1.5, 2.0]:
            home_prob = 0.0
            away_prob = 0.0
            push_prob = 0.0  # Probabilità di pareggio (rimborso)
            
            # Calcola la probabilità per ogni possibile combinazione di gol
            for h in range(10):
                for a in range(10):
                    p = self._poisson_probability(h, home_xg) * self._poisson_probability(a, away_xg)
                    
                    # Applica l'handicap
                    adjusted_h = h - handicap
                    
                    if adjusted_h > a:
                        home_prob += p
                    elif adjusted_h < a:
                        away_prob += p
                    else:
                        push_prob += p
            
            # Per handicap con decimali (es. -1.5), non c'è possibilità di pareggio
            if handicap % 1 != 0:
                # Redistribuisci la probabilità di push
                if push_prob > 0:
                    home_prob += push_prob / 2
                    away_prob += push_prob / 2
                    push_prob = 0
            
            # Normalizza
            total = home_prob + away_prob + push_prob
            if total > 0 and total != 1.0:
                factor = 1.0 / total
                home_prob *= factor
                away_prob *= factor
                push_prob *= factor
            
            key = f"{handicap:+.1f}"
            handicaps[key] = {
                'home': round(home_prob, 3),
                'away': round(away_prob, 3)
            }
            
            if push_prob > 0:
                handicaps[key]['push'] = round(push_prob, 3)
        
        return handicaps
    
    def _calculate_win_margins(self, home_xg: float, away_xg: float) -> Dict[str, float]:
        """
        Calcola le probabilità per diversi margini di vittoria.
        
        Args:
            home_xg: Gol attesi della squadra di casa.
            away_xg: Gol attesi della squadra in trasferta.
            
        Returns:
            Dizionario con le probabilità per diversi margini di vittoria.
        """
        margins = {}
        
        # Margini considerati
        for margin in range(1, 5):
            # Vittoria in casa per X gol
            home_by_margin = 0.0
            # Vittoria in trasferta per X gol
            away_by_margin = 0.0
            
            # Calcola per ogni possibile punteggio
            for h in range(10):
                for a in range(10):
                    p = self._poisson_probability(h, home_xg) * self._poisson_probability(a, away_xg)
                    
                    if h - a == margin:
                        home_by_margin += p
                    elif a - h == margin:
                        away_by_margin += p
            
            margins[f"home_by_{margin}"] = round(home_by_margin, 3)
            margins[f"away_by_{margin}"] = round(away_by_margin, 3)
        
        # Aggiungi anche la probabilità di pareggio
        draw_prob = 0.0
        for g in range(10):
            p = self._poisson_probability(g, home_xg) * self._poisson_probability(g, away_xg)
            draw_prob += p
        
        margins['draw'] = round(draw_prob, 3)
        
        # Margini più ampi (3+)
        home_big_win = 0.0
        away_big_win = 0.0
        
        for h in range(10):
            for a in range(10):
                p = self._poisson_probability(h, home_xg) * self._poisson_probability(a, away_xg)
                
                if h - a >= 3:
                    home_big_win += p
                elif a - h >= 3:
                    away_big_win += p
        
        margins['home_by_3+'] = round(home_big_win, 3)
        margins['away_by_3+'] = round(away_big_win, 3)
        
        return margins
    
    def _calculate_total_goals(self, home_xg: float, away_xg: float) -> Dict[str, float]:
        """
        Calcola le probabilità per diversi numeri di gol totali.
        
        Args:
            home_xg: Gol attesi della squadra di casa.
            away_xg: Gol attesi della squadra in trasferta.
            
        Returns:
            Dizionario con le probabilità per diversi numeri di gol.
        """
        goals = {}
        total_xg = home_xg + away_xg
        
        # Calcola la probabilità per ogni numero di gol
        for g in range(8):
            prob = 0.0
            
            # Somma le probabilità di tutte le combinazioni che danno g gol totali
            for h in range(g + 1):
                a = g - h
                prob += self._poisson_probability(h, home_xg) * self._poisson_probability(a, away_xg)
            
            goals[str(g)] = round(prob, 3)
        
        # 7+ gol
        prob_7_plus = 0.0
        for g in range(7, 15):
            for h in range(g + 1):
                a = g - h
                prob_7_plus += self._poisson_probability(h, home_xg) * self._poisson_probability(a, away_xg)
        
        goals['7+'] = round(prob_7_plus, 3)
        
        # Over/Under standard
        goals['over_0.5'] = round(1 - goals['0'], 3)
        goals['under_0.5'] = round(goals['0'], 3)
        
        goals['over_1.5'] = round(1 - goals['0'] - goals['1'], 3)
        goals['under_1.5'] = round(goals['0'] + goals['1'], 3)
        
        goals['over_2.5'] = round(1 - goals['0'] - goals['1'] - goals['2'], 3)
        goals['under_2.5'] = round(goals['0'] + goals['1'] + goals['2'], 3)
        
        goals['over_3.5'] = round(1 - goals['0'] - goals['1'] - goals['2'] - goals['3'], 3)
        goals['under_3.5'] = round(goals['0'] + goals['1'] + goals['2'] + goals['3'], 3)
        
        return goals
    
    def _get_stored_prediction(self, match_id: str) -> Optional[Dict[str, Any]]:
        """
        Ottiene una previsione salvata dal database.
        
        Args:
            match_id: ID della partita.
            
        Returns:
            Dizionario con la previsione, o None se non trovata o obsoleta.
        """
        try:
            # Controlla prima la cache
            if match_id in self.prediction_cache:
                prediction = self.prediction_cache[match_id]
                
                # Controlla se è recente (ultime 24 ore)
                last_updated = datetime.fromisoformat(prediction.get('last_updated', '2000-01-01'))
                if (datetime.now() - last_updated).total_seconds() < 86400:
                    return prediction
            
            # Altrimenti controlla il database
            prediction = self.db.get_reference(f"predictions/{match_id}")
            
            if prediction:
                # Controlla se è recente (ultime 24 ore)
                last_updated = datetime.fromisoformat(prediction.get('last_updated', '2000-01-01'))
                if (datetime.now() - last_updated).total_seconds() < 86400:
                    # Aggiorna la cache
                    self.prediction_cache[match_id] = prediction
                    return prediction
            
            return None
            
        except Exception as e:
            logger.error(f"Errore nel recuperare la previsione per {match_id}: {e}")
            return None
    
    def _store_prediction(self, prediction: Dict[str, Any]) -> bool:
        """
        Salva una previsione nel database.
        
        Args:
            prediction: Dizionario con la previsione.
            
        Returns:
            True se salvato con successo, False altrimenti.
        """
        try:
            match_id = prediction.get('match_id')
            if not match_id:
                logger.error("Impossibile salvare previsione senza match_id")
                return False
            
            # Aggiorna il timestamp
            prediction['last_updated'] = datetime.now().isoformat()
            
            # Salva nel database
            self.db.set_reference(f"predictions/{match_id}", prediction)
            
            # Aggiorna la cache
            self.prediction_cache[match_id] = prediction
            
            logger.info(f"Previsione per la partita {match_id} salvata con successo")
            return True
            
        except Exception as e:
            logger.error(f"Errore nel salvare la previsione: {e}")
            return False
    
    def update_weights(self, new_weights: Dict[str, float]) -> None:
        """
        Aggiorna i pesi utilizzati dal modello.
        
        Args:
            new_weights: Nuovi pesi da applicare.
        """
        # Verifica che i pesi siano validi
        if sum(new_weights.values()) != 1.0:
            logger.warning("I pesi forniti non sommano a 1.0, verranno normalizzati")
            
            # Normalizza i pesi
            total = sum(new_weights.values())
            if total > 0:
                new_weights = {k: v / total for k, v in new_weights.items()}
        
        # Aggiorna i pesi
        for key, value in new_weights.items():
            if key in self.weights:
                self.weights[key] = value
                
        logger.info(f"Pesi aggiornati: {self.weights}")
    
    def batch_predict(self, matches: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Genera previsioni per un gruppo di partite.
        
        Args:
            matches: Lista di partite da prevedere.
            
        Returns:
            Lista di previsioni.
        """
        predictions = []
        
        for match in matches:
            match_id = match.get('match_id', '')
            home_team_id = match.get('home_team', {}).get('id', '')
            away_team_id = match.get('away_team', {}).get('id', '')
            match_date = match.get('datetime', '')
            league_id = match.get('competition', {}).get('id', '')
            
            if not match_id or not home_team_id or not away_team_id:
                logger.warning(f"Dati insufficienti per la previsione: {match}")
                continue
            
            prediction = self.predict_match(
                match_id=match_id,
                home_team_id=home_team_id,
                away_team_id=away_team_id,
                match_date=match_date,
                league_id=league_id
            )
            
            predictions.append(prediction)
        
        return predictions


# Istanza globale per un utilizzo più semplice
basic_model = BasicModel()

def get_model():
    """
    Ottiene l'istanza globale del modello.
    
    Returns:
        Istanza di BasicModel.
    """
    return basic_model
