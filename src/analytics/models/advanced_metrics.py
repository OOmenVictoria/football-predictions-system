"""
Modulo per il calcolo e l'analisi di metriche calcistiche avanzate.
Questo modulo estende i modelli base con metriche più sofisticate per
migliorare l'accuratezza delle previsioni.
"""
import os
import sys
import math
import logging
import numpy as np
from typing import Dict, List, Any, Optional, Union, Tuple

from src.utils.cache import cached
from src.utils.database import FirebaseManager
from src.config.settings import get_setting

logger = logging.getLogger(__name__)

class AdvancedMetricsModel:
    """
    Implementazione di metriche calcistiche avanzate per previsioni.
    
    Questo modello estende i modelli di base con metriche più sofisticate
    come expected goals (xG), efficienza offensiva/difensiva, ecc.
    """
    
    def __init__(self):
        """Inizializza il modello di metriche avanzate."""
        self.db = FirebaseManager()
        
        # Configurazione
        self.xg_weight = get_setting("analytics.metrics.xg_weight", 0.7)
        self.form_weight = get_setting("analytics.metrics.form_weight", 0.6)
        self.performance_weight = get_setting("analytics.metrics.performance_weight", 0.5)
        self.rating_scale = 100  # Scala 0-100 per i rating
        
        logger.info(f"AdvancedMetricsModel inizializzato: xG={self.xg_weight}, form={self.form_weight}")
    
    def calculate_xg_efficiency(self, goals: float, xg: float) -> float:
        """
        Calcola l'efficienza di conversione xG.
        
        Args:
            goals: Gol effettivi segnati
            xg: Expected Goals (xG)
            
        Returns:
            Indice di efficienza (>1 = sovraperformance, <1 = sottoperformance)
        """
        if xg <= 0:
            return 1.0  # Valore neutro se non ci sono dati xG
        
        return goals / xg
    
    def calculate_defensive_efficiency(self, goals_conceded: float, xg_against: float) -> float:
        """
        Calcola l'efficienza difensiva.
        
        Args:
            goals_conceded: Gol subiti
            xg_against: Expected Goals concessi
            
        Returns:
            Indice di efficienza difensiva (>1 = sottoperformance, <1 = sovraperformance)
        """
        if xg_against <= 0:
            return 1.0  # Valore neutro se non ci sono dati xG
        
        return goals_conceded / xg_against
    
    def calculate_shot_quality(self, xg: float, shots: int) -> float:
        """
        Calcola la qualità media dei tiri.
        
        Args:
            xg: Expected Goals (xG) totali
            shots: Numero di tiri totali
            
        Returns:
            xG medio per tiro (maggiore = tiri di migliore qualità)
        """
        if shots <= 0:
            return 0.0
        
        return xg / shots
    
    def calculate_possession_efficiency(self, xg: float, possession: float) -> float:
        """
        Calcola l'efficienza del possesso palla.
        
        Args:
            xg: Expected Goals (xG) generati
            possession: Percentuale di possesso palla (0-100)
            
        Returns:
            xG per percentuale di possesso (maggiore = possesso più efficace)
        """
        if possession <= 0:
            return 0.0
        
        # Normalizza il possesso a 0-1
        possession_norm = possession / 100.0
        
        return xg / possession_norm
    
    def calculate_defensive_strength(self, xg_against: float, opponent_possession: float) -> float:
        """
        Calcola la solidità difensiva.
        
        Args:
            xg_against: Expected Goals concessi
            opponent_possession: Percentuale di possesso dell'avversario (0-100)
            
        Returns:
            Indice di solidità difensiva (minore = difesa più solida)
        """
        if opponent_possession <= 0:
            return 0.0
        
        # Normalizza il possesso a 0-1
        opponent_possession_norm = opponent_possession / 100.0
        
        # Inverti per ottenere un indicatore di forza (più basso è xg_against, più forte è la difesa)
        if xg_against <= 0:
            return 10.0  # Valore massimo per difesa perfetta
        
        return 1.0 / (xg_against * opponent_possession_norm)
    
    def calculate_team_strength_index(self, team_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calcola un indice di forza complessivo della squadra.
        
        Args:
            team_data: Dati della squadra con statistiche di performance
            
        Returns:
            Indici di forza della squadra in varie categorie
        """
        # Estrai statistiche rilevanti
        xg_for = team_data.get("xg", {}).get("overall", {}).get("xg_for", 0.0)
        xg_against = team_data.get("xg", {}).get("overall", {}).get("xg_against", 0.0)
        goals_for = team_data.get("performance", {}).get("overall", {}).get("goals_for", 0.0)
        goals_against = team_data.get("performance", {}).get("overall", {}).get("goals_against", 0.0)
        shots_for = team_data.get("performance", {}).get("overall", {}).get("shots_for", 0.0)
        shots_against = team_data.get("performance", {}).get("overall", {}).get("shots_against", 0.0)
        possession = team_data.get("performance", {}).get("overall", {}).get("possession", 50.0)
        opponent_possession = 100.0 - possession
        form_quality = team_data.get("form", {}).get("form_quality", 50.0)
        
        # Calcola metriche avanzate
        offensive_efficiency = self.calculate_xg_efficiency(goals_for, xg_for)
        defensive_efficiency = self.calculate_defensive_efficiency(goals_against, xg_against)
        shot_quality = self.calculate_shot_quality(xg_for, shots_for)
        possession_efficiency = self.calculate_possession_efficiency(xg_for, possession)
        defensive_strength = self.calculate_defensive_strength(xg_against, opponent_possession)
        
        # Normalizza le metriche a un punteggio 0-100
        offensive_rating = min(100, max(0, 50 + (offensive_efficiency - 1.0) * 25))
        defensive_rating = min(100, max(0, 50 + (1.0 - defensive_efficiency) * 25))
        shot_quality_rating = min(100, max(0, shot_quality * 500))  # xG per tiro tipicamente 0.1-0.2
        possession_efficiency_rating = min(100, max(0, possession_efficiency * 200))
        defensive_strength_rating = min(100, max(0, defensive_strength * 10))
        
        # Calcola l'indice complessivo, dando più peso all'efficienza
        overall_rating = (
            offensive_rating * 0.25 +
            defensive_rating * 0.25 +
            shot_quality_rating * 0.15 +
            possession_efficiency_rating * 0.15 +
            defensive_strength_rating * 0.1 +
            form_quality * 0.1
        )
        
        # Qualifica la forza in base al rating
        strength_level = self._get_rating_text(overall_rating)
        offensive_level = self._get_rating_text(offensive_rating)
        defensive_level = self._get_rating_text(defensive_rating)
        
        return {
            "overall": {
                "rating": overall_rating,
                "level": strength_level
            },
            "offense": {
                "rating": offensive_rating,
                "level": offensive_level,
                "efficiency": offensive_efficiency,
                "shot_quality": shot_quality,
                "shot_quality_rating": shot_quality_rating,
                "possession_efficiency": possession_efficiency,
                "possession_efficiency_rating": possession_efficiency_rating
            },
            "defense": {
                "rating": defensive_rating,
                "level": defensive_level,
                "efficiency": defensive_efficiency,
                "strength": defensive_strength,
                "strength_rating": defensive_strength_rating
            },
            "form": {
                "rating": form_quality,
                "level": self._get_rating_text(form_quality)
            }
        }
    
    def _get_rating_text(self, rating: float) -> str:
        """
        Converte un rating numerico in una classificazione testuale.
        
        Args:
            rating: Rating numerico (0-100)
            
        Returns:
            Classificazione testuale
        """
        if rating >= 90:
            return "eccellente"
        elif rating >= 80:
            return "molto buono"
        elif rating >= 70:
            return "buono"
        elif rating >= 60:
            return "sopra la media"
        elif rating >= 45:
            return "nella media"
        elif rating >= 35:
            return "sotto la media"
        elif rating >= 25:
            return "scarso"
        else:
            return "molto scarso"
    
    def calculate_match_metrics(self, match_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calcola metriche avanzate per una partita specifica.
        
        Args:
            match_data: Dati della partita con statistiche preliminari
            
        Returns:
            Metriche avanzate per la partita
        """
        # Estrai i dati delle squadre
        home_team_id = match_data.get("home_team_id", "")
        away_team_id = match_data.get("away_team_id", "")
        home_team = match_data.get("home_team", "")
        away_team = match_data.get("away_team", "")
        
        # Ottieni statistiche xG e performance dalle sezioni corrispondenti
        home_xg = match_data.get("xg", {}).get("home", 0.0)
        away_xg = match_data.get("xg", {}).get("away", 0.0)
        
        # Statistiche di performance se disponibili
        home_shots = match_data.get("stats", {}).get("home", {}).get("shots", 0)
        away_shots = match_data.get("stats", {}).get("away", {}).get("shots", 0)
        home_shots_on_target = match_data.get("stats", {}).get("home", {}).get("shotsOnTarget", 0)
        away_shots_on_target = match_data.get("stats", {}).get("away", {}).get("shotsOnTarget", 0)
        
        # Calcola metriche avanzate per la partita
        home_shot_quality = self.calculate_shot_quality(home_xg, home_shots) if home_shots > 0 else 0.0
        away_shot_quality = self.calculate_shot_quality(away_xg, away_shots) if away_shots > 0 else 0.0
        
        # Calcola rating di qualità dei tiri
        home_shot_quality_rating = min(100, max(0, home_shot_quality * 500))
        away_shot_quality_rating = min(100, max(0, away_shot_quality * 500))
        
        # Calcola rating di accuratezza dei tiri
        home_shot_accuracy = home_shots_on_target / home_shots if home_shots > 0 else 0.0
        away_shot_accuracy = away_shots_on_target / away_shots if away_shots > 0 else 0.0
        home_shot_accuracy_rating = min(100, max(0, home_shot_accuracy * 100))
        away_shot_accuracy_rating = min(100, max(0, away_shot_accuracy * 100))
        
        # Calcola indice di minaccia offensiva
        home_threat_index = (home_xg + (home_shots_on_target * 0.1)) / 2
        away_threat_index = (away_xg + (away_shots_on_target * 0.1)) / 2
        
        # Normalizza gli indici di minaccia a rating 0-100
        home_threat_rating = min(100, max(0, home_threat_index * 33.3))
        away_threat_rating = min(100, max(0, away_threat_index * 33.3))
        
        # Calcola efficienza prevista
        home_efficiency = match_data.get("stats", {}).get("home", {}).get("efficiency", 1.0)
        away_efficiency = match_data.get("stats", {}).get("away", {}).get("efficiency", 1.0)
        
        # Calcola un rating complessivo di attacco
        home_attack_rating = (
            home_threat_rating * 0.6 +
            home_shot_quality_rating * 0.25 +
            home_shot_accuracy_rating * 0.15
        )
        
        away_attack_rating = (
            away_threat_rating * 0.6 +
            away_shot_quality_rating * 0.25 +
            away_shot_accuracy_rating * 0.15
        )
        
        # Determina vantaggio
        attack_advantage = "home" if home_attack_rating > away_attack_rating else \
                         "away" if away_attack_rating > home_attack_rating else "equal"
        attack_advantage_value = abs(home_attack_rating - away_attack_rating)
        
        # Qualifica i rating in testo
        home_attack_level = self._get_rating_text(home_attack_rating)
        away_attack_level = self._get_rating_text(away_attack_rating)
        
        return {
            "teams": {
                "home": {
                    "team_id": home_team_id,
                    "team": home_team,
                    "xg": home_xg,
                    "shot_quality": home_shot_quality,
                    "shot_quality_rating": home_shot_quality_rating,
                    "shot_accuracy": home_shot_accuracy,
                    "shot_accuracy_rating": home_shot_accuracy_rating,
                    "threat_index": home_threat_index,
                    "threat_rating": home_threat_rating,
                    "efficiency": home_efficiency,
                    "attack_rating": home_attack_rating,
                    "attack_level": home_attack_level
                },
                "away": {
                    "team_id": away_team_id,
                    "team": away_team,
                    "xg": away_xg,
                    "shot_quality": away_shot_quality,
                    "shot_quality_rating": away_shot_quality_rating,
                    "shot_accuracy": away_shot_accuracy,
                    "shot_accuracy_rating": away_shot_accuracy_rating,
                    "threat_index": away_threat_index,
                    "threat_rating": away_threat_rating,
                    "efficiency": away_efficiency,
                    "attack_rating": away_attack_rating,
                    "attack_level": away_attack_level
                }
            },
            "comparison": {
                "attack_advantage": attack_advantage,
                "attack_advantage_value": attack_advantage_value,
                "xg_difference": home_xg - away_xg,
                "shot_quality_difference": home_shot_quality - away_shot_quality,
                "threat_difference": home_threat_index - away_threat_index
            }
        }
    
    def evaluate_model_performance(self, matches: List[Dict[str, Any]], 
                                predictions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Valuta le performance del modello con metriche avanzate.
        
        Args:
            matches: Lista di partite completate con risultati
            predictions: Lista di previsioni fatte per le partite
            
        Returns:
            Statistiche di accuratezza e altre metriche
        """
        if not matches or not predictions:
            return {
                "total": 0,
                "accuracy": 0.0,
                "accuracy_1x2": 0.0,
                "accuracy_btts": 0.0,
                "accuracy_over_under": 0.0,
                "avg_error": 0.0
            }
        
        # Crea un dizionario per le previsioni per un accesso rapido
        prediction_map = {p.get("match_id", ""): p for p in predictions}
        
        # Contatori
        total = 0
        correct_1x2 = 0
        correct_btts = 0
        correct_over_under = 0
        total_error = 0.0
        
        for match in matches:
            match_id = match.get("match_id", "")
            
            # Salta se non c'è previsione
            if match_id not in prediction_map:
                continue
            
            # Ottieni la previsione
            prediction = prediction_map[match_id]
            
            # Salta se il match non è finito
            if match.get("status") != "FINISHED":
                continue
            
            # Ottieni risultati effettivi
            home_score = match.get("home_score", 0)
            away_score = match.get("away_score", 0)
            total_goals = home_score + away_score
            
            # Risultato 1X2 effettivo
            actual_result = "1" if home_score > away_score else \
                          "X" if home_score == away_score else "2"
            
            # BTTS effettivo
            actual_btts = home_score > 0 and away_score > 0
            
            # Over/Under 2.5 effettivo
            actual_over = total_goals > 2.5
            
            # Ottieni previsioni
            predicted_result = prediction.get("prediction", {}).get("1x2", "")
            predicted_btts = prediction.get("prediction", {}).get("btts", {}).get("prediction", "") == "Yes"
            predicted_over = prediction.get("prediction", {}).get("over_under", {}).get("prediction", "").startswith("Over")
            
            # Ottieni probabilità previste
            probabilities = prediction.get("probabilities", {})
            prob_1 = probabilities.get("1", 0.0)
            prob_x = probabilities.get("X", 0.0)
            prob_2 = probabilities.get("2", 0.0)
            
            # Converti risultato effettivo in indice
            result_index = {"1": 0, "X": 1, "2": 2}.get(actual_result, 0)
            
            # Calcola errore (distanza dalla probabilità corretta)
            pred_probs = [prob_1, prob_x, prob_2]
            if len(pred_probs) > result_index:
                error = 1.0 - pred_probs[result_index]
                total_error += error
            
            # Incrementa contatori
            total += 1
            if predicted_result == actual_result:
                correct_1x2 += 1
            if predicted_btts == actual_btts:
                correct_btts += 1
            if predicted_over == actual_over:
                correct_over_under += 1
        
        # Calcola accuratezza
        accuracy_1x2 = correct_1x2 / total if total > 0 else 0.0
        accuracy_btts = correct_btts / total if total > 0 else 0.0
        accuracy_over_under = correct_over_under / total if total > 0 else 0.0
        avg_error = total_error / total if total > 0 else 0.0
        
        # Accuratezza complessiva (media delle 3 metriche)
        overall_accuracy = (accuracy_1x2 + accuracy_btts + accuracy_over_under) / 3
        
        return {
            "total": total,
            "correct_1x2": correct_1x2,
            "correct_btts": correct_btts,
            "correct_over_under": correct_over_under,
            "accuracy": overall_accuracy,
            "accuracy_1x2": accuracy_1x2,
            "accuracy_btts": accuracy_btts,
            "accuracy_over_under": accuracy_over_under,
            "avg_error": avg_error
        }

# Funzioni di utilità globali
def calculate_team_strength(team_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Calcola l'indice di forza complessivo di una squadra.
    
    Args:
        team_data: Dati della squadra con statistiche di performance
        
    Returns:
        Indici di forza della squadra in varie categorie
    """
    model = AdvancedMetricsModel()
    return model.calculate_team_strength_index(team_data)

def calculate_match_metrics(match_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Calcola metriche avanzate per una partita specifica.
    
    Args:
        match_data: Dati della partita con statistiche preliminari
        
    Returns:
        Metriche avanzate per la partita
    """
    model = AdvancedMetricsModel()
    return model.calculate_match_metrics(match_data)

def evaluate_model_performance(matches: List[Dict[str, Any]], 
                             predictions: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Valuta le performance del modello con metriche avanzate.
    
    Args:
        matches: Lista di partite completate con risultati
        predictions: Lista di previsioni fatte per le partite
        
    Returns:
        Statistiche di accuratezza e altre metriche
    """
    model = AdvancedMetricsModel()
    return model.evaluate_model_performance(matches, predictions)
