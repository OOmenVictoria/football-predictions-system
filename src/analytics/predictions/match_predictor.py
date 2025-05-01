"""
Modulo per la predizione dei risultati delle partite.

Questo modulo coordina i diversi modelli e analisi statistiche per generare 
previsioni complete sui risultati delle partite di calcio.
"""
import logging
import math
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple, Union, Set

from src.utils.cache import cached
from src.utils.database import FirebaseManager
from src.config.settings import get_setting
from src.analytics.models.basic_model import BasicModel
from src.analytics.models.poisson_model import PoissonModel
from src.analytics.models.xg_model import XGModel
from src.analytics.statistics.team_form import get_team_form, compare_form
from src.analytics.statistics.xg_analysis import get_team_xg_profile
from src.analytics.statistics.performance_metrics import get_team_performance_metrics
from src.analytics.statistics.scoring_patterns import get_team_scoring_patterns

logger = logging.getLogger(__name__)


class MatchPredictor:
    """
    Predittore dei risultati delle partite.
    
    Coordina diversi modelli predittivi e analisi statistiche per generare
    previsioni complete sui risultati delle partite di calcio.
    """
    
    def __init__(self):
        """Inizializza il predittore delle partite."""
        self.db = FirebaseManager()
        self.model_weights = {
            'basic': get_setting('predictions.model_weights.basic', 0.2),
            'poisson': get_setting('predictions.model_weights.poisson', 0.4),
            'xg': get_setting('predictions.model_weights.xg', 0.4)
        }
        self.prediction_window = get_setting('predictions.window_hours', 48)  # Finestra di predizione in ore
        
        # Inizializza i modelli predittivi
        self.basic_model = BasicModel()
        self.poisson_model = PoissonModel()
        self.xg_model = XGModel()
        
        logger.info(f"MatchPredictor inizializzato con pesi: {self.model_weights}")
    
    @cached(ttl=1800)  # Cache di 30 minuti
    def predict_match(self, match_id: str) -> Dict[str, Any]:
        """
        Genera una previsione completa per una partita.
        
        Args:
            match_id: ID della partita
            
        Returns:
            Previsione completa con probabilità, pronostici e motivazioni
        """
        logger.info(f"Generando previsione per match_id={match_id}")
        
        try:
            # Ottieni i dati della partita
            match_ref = self.db.get_reference(f"data/matches/{match_id}")
            match_data = match_ref.get()
            
            if not match_data:
                logger.warning(f"Nessun dato trovato per match_id={match_id}")
                return self._create_empty_prediction(match_id)
            
            # Verifica che la partita sia futura o in corso
            match_status = match_data.get('status', '')
            if match_status == 'FINISHED':
                logger.info(f"La partita {match_id} è già conclusa")
                return self._create_post_match_analysis(match_data)
            
            # Estrai gli ID delle squadre
            home_team_id = match_data.get('home_team_id', '')
            away_team_id = match_data.get('away_team_id', '')
            
            if not home_team_id or not away_team_id:
                logger.warning(f"ID squadre mancanti per match_id={match_id}")
                return self._create_empty_prediction(match_id)
            
            # Raccogli dati aggiuntivi per la predizione
            home_form = get_team_form(home_team_id)
            away_form = get_team_form(away_team_id)
            home_xg = get_team_xg_profile(home_team_id)
            away_xg = get_team_xg_profile(away_team_id)
            home_metrics = get_team_performance_metrics(home_team_id)
            away_metrics = get_team_performance_metrics(away_team_id)
            home_scoring = get_team_scoring_patterns(home_team_id)
            away_scoring = get_team_scoring_patterns(away_team_id)
            
            # Arricchisci i dati della partita con le statistiche aggiuntive
            enriched_match_data = self._enrich_match_data(
                match_data, 
                home_form, away_form,
                home_xg, away_xg,
                home_metrics, away_metrics,
                home_scoring, away_scoring
            )
            
            # Genera le previsioni con i diversi modelli
            basic_prediction = self.basic_model.predict_match(enriched_match_data)
            poisson_prediction = self.poisson_model.predict_match(enriched_match_data)
            xg_prediction = self.xg_model.predict_match(enriched_match_data)
            
            # Combina le previsioni
            combined_prediction = self._combine_predictions(
                basic_prediction, poisson_prediction, xg_prediction
            )
            
            # Aggiungi analisi comparative
            combined_prediction['team_comparison'] = self._analyze_team_comparison(
                home_form, away_form,
                home_xg, away_xg,
                home_metrics, away_metrics,
                home_scoring, away_scoring
            )
            
            # Genera pronostici dettagliati
            detailed_prediction = self._generate_detailed_prediction(
                combined_prediction, enriched_match_data
            )
            
            # Aggiungi informazioni aggiuntive
            detailed_prediction['match_id'] = match_id
            detailed_prediction['home_team'] = match_data.get('home_team', '')
            detailed_prediction['away_team'] = match_data.get('away_team', '')
            detailed_prediction['match_date'] = match_data.get('datetime', '')
            detailed_prediction['league_id'] = match_data.get('league_id', '')
            detailed_prediction['prediction_date'] = datetime.now().isoformat()
            detailed_prediction['has_data'] = True
            
            # Salva la previsione nel database
            self._save_prediction(match_id, detailed_prediction)
            
            return detailed_prediction
            
        except Exception as e:
            logger.error(f"Errore nella previsione per match_id={match_id}: {e}")
            return self._create_empty_prediction(match_id)
    
    def _enrich_match_data(self, match_data: Dict[str, Any], 
                         home_form: Dict[str, Any], away_form: Dict[str, Any],
                         home_xg: Dict[str, Any], away_xg: Dict[str, Any],
                         home_metrics: Dict[str, Any], away_metrics: Dict[str, Any],
                         home_scoring: Dict[str, Any], away_scoring: Dict[str, Any]) -> Dict[str, Any]:
        """
        Arricchisce i dati della partita con statistiche aggiuntive.
        
        Args:
            match_data: Dati di base della partita
            home_form: Dati forma squadra casa
            away_form: Dati forma squadra trasferta
            home_xg: Profilo xG squadra casa
            away_xg: Profilo xG squadra trasferta
            home_metrics: Metriche performance squadra casa
            away_metrics: Metriche performance squadra trasferta
            home_scoring: Pattern di gol squadra casa
            away_scoring: Pattern di gol squadra trasferta
            
        Returns:
            Dati della partita arricchiti
        """
        enriched_data = match_data.copy()
        
        # Aggiungi dati di forma
        enriched_data['home_form'] = home_form
        enriched_data['away_form'] = away_form
        
        # Prepara i dati xG
        if home_xg.get('has_data', False) and home_xg.get('home', {}).get('xg') is not None:
            # Usa i dati specifici per casa
            expected_home_goals = home_xg['home']['xg']
            home_xg_performance = home_xg['home']['attack_performance']
        elif home_xg.get('has_data', False):
            # Fallback su dati complessivi
            expected_home_goals = home_xg['overall']['xg']
            home_xg_performance = home_xg['overall']['attack_performance']
        else:
            # Nessun dato disponibile
            expected_home_goals = None
            home_xg_performance = 0
            
        if away_xg.get('has_data', False) and away_xg.get('away', {}).get('xg') is not None:
            # Usa i dati specifici per trasferta
            expected_away_goals = away_xg['away']['xg']
            away_xg_performance = away_xg['away']['attack_performance']
        elif away_xg.get('has_data', False):
            # Fallback su dati complessivi
            expected_away_goals = away_xg['overall']['xg']
            away_xg_performance = away_xg['overall']['attack_performance']
        else:
            # Nessun dato disponibile
            expected_away_goals = None
            away_xg_performance = 0
        
        # Aggiungi predizione xG
        enriched_data['expected_goals'] = {
            'home': expected_home_goals,
            'away': expected_away_goals,
            'home_performance': home_xg_performance,
            'away_performance': away_xg_performance
        }
        
        # Aggiungi metriche di performance
        if home_metrics.get('has_data', False) and home_metrics.get('overall_rating', {}).get('overall_score') is not None:
            enriched_data['home_metrics'] = {
                'attack': home_metrics['overall_rating']['scores']['offensive'],
                'defense': home_metrics['overall_rating']['scores']['defensive'],
                'possession': home_metrics['overall_rating']['scores']['possession'],
                'overall': home_metrics['overall_rating']['overall_score'],
                'strengths': home_metrics.get('strengths', []),
                'weaknesses': home_metrics.get('weaknesses', [])
            }
        else:
            enriched_data['home_metrics'] = {
                'attack': 50,
                'defense': 50,
                'possession': 50,
                'overall': 50,
                'strengths': [],
                'weaknesses': []
            }
            
        if away_metrics.get('has_data', False) and away_metrics.get('overall_rating', {}).get('overall_score') is not None:
            enriched_data['away_metrics'] = {
                'attack': away_metrics['overall_rating']['scores']['offensive'],
                'defense': away_metrics['overall_rating']['scores']['defensive'],
                'possession': away_metrics['overall_rating']['scores']['possession'],
                'overall': away_metrics['overall_rating']['overall_score'],
                'strengths': away_metrics.get('strengths', []),
                'weaknesses': away_metrics.get('weaknesses', [])
            }
        else:
            enriched_data['away_metrics'] = {
                'attack': 50,
                'defense': 50,
                'possession': 50,
                'overall': 50,
                'strengths': [],
                'weaknesses': []
            }
        
        # Aggiungi pattern di gol
        if home_scoring.get('has_data', False):
            enriched_data['home_scoring'] = {
                'avg_goals': home_scoring['goals_for']['avg_per_game'],
                'avg_conceded': home_scoring['goals_against']['avg_per_game'],
                'patterns': home_scoring['goals_for']['patterns'],
                'vulnerability': home_scoring['goals_against']['patterns'],
                'most_dangerous_period': home_scoring.get('key_moments', {}).get('most_dangerous_period', {}),
                'most_vulnerable_period': home_scoring.get('key_moments', {}).get('most_vulnerable_period', {})
            }
        else:
            enriched_data['home_scoring'] = {
                'avg_goals': 0,
                'avg_conceded': 0,
                'patterns': [],
                'vulnerability': [],
                'most_dangerous_period': {},
                'most_vulnerable_period': {}
            }
            
        if away_scoring.get('has_data', False):
            enriched_data['away_scoring'] = {
                'avg_goals': away_scoring['goals_for']['avg_per_game'],
                'avg_conceded': away_scoring['goals_against']['avg_per_game'],
                'patterns': away_scoring['goals_for']['patterns'],
                'vulnerability': away_scoring['goals_against']['patterns'],
                'most_dangerous_period': away_scoring.get('key_moments', {}).get('most_dangerous_period', {}),
                'most_vulnerable_period': away_scoring.get('key_moments', {}).get('most_vulnerable_period', {})
            }
        else:
            enriched_data['away_scoring'] = {
                'avg_goals': 0,
                'avg_conceded': 0,
                'patterns': [],
                'vulnerability': [],
                'most_dangerous_period': {},
                'most_vulnerable_period': {}
            }
        
        return enriched_data
    
    def _combine_predictions(self, basic_prediction: Dict[str, Any], 
                          poisson_prediction: Dict[str, Any], 
                          xg_prediction: Dict[str, Any]) -> Dict[str, Any]:
        """
        Combina le previsioni dei diversi modelli.
        
        Args:
            basic_prediction: Previsione del modello base
            poisson_prediction: Previsione del modello Poisson
            xg_prediction: Previsione del modello xG
            
        Returns:
            Previsione combinata
        """
        # Inizializza la previsione combinata
        combined = {
            'probabilities': {},
            'asian_handicap': {},
            'btts': {},
            'over_under': {},
            'exact_score': {},
            'reasoning': []
        }
        
        # Pesi normalizzati
        total_weight = sum(self.model_weights.values())
        normalized_weights = {
            model: weight / total_weight 
            for model, weight in self.model_weights.items()
        }
        
        # Combina le probabilità 1X2
        combined['probabilities']['1'] = (
            basic_prediction['probabilities']['1'] * normalized_weights['basic'] +
            poisson_prediction['probabilities']['1'] * normalized_weights['poisson'] +
            xg_prediction['probabilities']['1'] * normalized_weights['xg']
        )
        
        combined['probabilities']['X'] = (
            basic_prediction['probabilities']['X'] * normalized_weights['basic'] +
            poisson_prediction['probabilities']['X'] * normalized_weights['poisson'] +
            xg_prediction['probabilities']['X'] * normalized_weights['xg']
        )
        
        combined['probabilities']['2'] = (
            basic_prediction['probabilities']['2'] * normalized_weights['basic'] +
            poisson_prediction['probabilities']['2'] * normalized_weights['poisson'] +
            xg_prediction['probabilities']['2'] * normalized_weights['xg']
        )
        
        # Combina Asian Handicap
        all_ah_lines = set()
        for prediction in [basic_prediction, poisson_prediction, xg_prediction]:
            if 'asian_handicap' in prediction:
                all_ah_lines.update(prediction['asian_handicap'].keys())
        
        for line in all_ah_lines:
            combined['asian_handicap'][line] = {'home': 0, 'away': 0}
            
            for model, prediction in [
                ('basic', basic_prediction), 
                ('poisson', poisson_prediction), 
                ('xg', xg_prediction)
            ]:
                if 'asian_handicap' in prediction and line in prediction['asian_handicap']:
                    weight = normalized_weights[model]
                    combined['asian_handicap'][line]['home'] += prediction['asian_handicap'][line]['home'] * weight
                    combined['asian_handicap'][line]['away'] += prediction['asian_handicap'][line]['away'] * weight
        
        # Combina BTTS
        combined['btts']['Yes'] = (
            basic_prediction.get('btts', {}).get('Yes', 0) * normalized_weights['basic'] +
            poisson_prediction.get('btts', {}).get('Yes', 0) * normalized_weights['poisson'] +
            xg_prediction.get('btts', {}).get('Yes', 0) * normalized_weights['xg']
        )
        combined['btts']['No'] = 1 - combined['btts']['Yes']
        
        # Combina Over/Under
        all_ou_lines = set()
        for prediction in [basic_prediction, poisson_prediction, xg_prediction]:
            if 'over_under' in prediction:
                all_ou_lines.update(prediction['over_under'].keys())
        
        for line in all_ou_lines:
            combined['over_under'][line] = {'Over': 0, 'Under': 0}
            
            for model, prediction in [
                ('basic', basic_prediction), 
                ('poisson', poisson_prediction), 
                ('xg', xg_prediction)
            ]:
                if 'over_under' in prediction and line in prediction['over_under']:
                    weight = normalized_weights[model]
                    combined['over_under'][line]['Over'] += prediction['over_under'][line]['Over'] * weight
                    combined['over_under'][line]['Under'] += prediction['over_under'][line]['Under'] * weight
        
        # Combina risultati esatti
        all_scores = set()
        for prediction in [basic_prediction, poisson_prediction, xg_prediction]:
            if 'exact_score' in prediction:
                all_scores.update(prediction['exact_score'].keys())
        
        for score in all_scores:
            combined['exact_score'][score] = 0
            
            for model, prediction in [
                ('basic', basic_prediction), 
                ('poisson', poisson_prediction), 
                ('xg', xg_prediction)
            ]:
                if 'exact_score' in prediction and score in prediction['exact_score']:
                    weight = normalized_weights[model]
                    combined['exact_score'][score] += prediction['exact_score'][score] * weight
        
        # Ordina i risultati esatti per probabilità
        combined['exact_score'] = dict(sorted(
            combined['exact_score'].items(), 
            key=lambda x: x[1], 
            reverse=True
        )[:10])  # Mantieni solo i top 10
        
        # Combina le motivazioni
        all_reasoning = set()
        for prediction in [basic_prediction, poisson_prediction, xg_prediction]:
            if 'reasoning' in prediction:
                all_reasoning.update(prediction['reasoning'])
        
        combined['reasoning'] = list(all_reasoning)
        
        return combined
    
    def _analyze_team_comparison(self, home_form: Dict[str, Any], away_form: Dict[str, Any],
                              home_xg: Dict[str, Any], away_xg: Dict[str, Any],
                              home_metrics: Dict[str, Any], away_metrics: Dict[str, Any],
                              home_scoring: Dict[str, Any], away_scoring: Dict[str, Any]
                              ) -> Dict[str, Any]:
        """
        Genera un'analisi comparativa tra le due squadre.
        
        Args:
            home_form: Dati forma squadra casa
            away_form: Dati forma squadra trasferta
            home_xg: Profilo xG squadra casa
            away_xg: Profilo xG squadra trasferta
            home_metrics: Metriche performance squadra casa
            away_metrics: Metriche performance squadra trasferta
            home_scoring: Pattern di gol squadra casa
            away_scoring: Pattern di gol squadra trasferta
            
        Returns:
            Analisi comparativa
        """
        comparison = {}
        
        # Confronto forma
        if home_form.get('has_data', False) and away_form.get('has_data', False):
            comparison['form'] = {
                'home_points': home_form['weighted_average'],
                'away_points': away_form['weighted_average'],
                'home_quality': home_form['form_quality'],
                'away_quality': away_form['form_quality'],
                'advantage': 'home' if home_form['form_quality'] > away_form['form_quality'] else 
                            ('away' if away_form['form_quality'] > home_form['form_quality'] else 'equal'),
                'description': self._generate_form_comparison(home_form, away_form)
            }
        
        # Confronto xG
        if home_xg.get('has_data', False) and away_xg.get('has_data', False):
            home_xg_rating = home_xg['performance_rating']['score']
            away_xg_rating = away_xg['performance_rating']['score']
            
            comparison['xg'] = {
                'home_rating': home_xg_rating,
                'away_rating': away_xg_rating,
                'home_attack': home_xg['home']['xg'],
                'away_attack': away_xg['away']['xg'],
                'home_defense': home_xg['home']['xg_against'],
                'away_defense': away_xg['away']['xg_against'],
                'advantage': 'home' if home_xg_rating > away_xg_rating else 
                            ('away' if away_xg_rating > home_xg_rating else 'equal'),
                'description': self._generate_xg_comparison(home_xg, away_xg)
            }
        
        # Confronto metriche di performance
        if home_metrics.get('has_data', False) and away_metrics.get('has_data', False):
            home_overall = home_metrics['overall_rating']['overall_score']
            away_overall = away_metrics['overall_rating']['overall_score']
            
            comparison['metrics'] = {
                'home_overall': home_overall,
                'away_overall': away_overall,
                'home_attack': home_metrics['overall_rating']['scores']['offensive'],
                'away_attack': away_metrics['overall_rating']['scores']['offensive'],
                'home_defense': home_metrics['overall_rating']['scores']['defensive'],
                'away_defense': away_metrics['overall_rating']['scores']['defensive'],
                'home_possession': home_metrics['overall_rating']['scores']['possession'],
                'away_possession': away_metrics['overall_rating']['scores']['possession'],
                'home_strengths': home_metrics.get('strengths', []),
                'away_strengths': away_metrics.get('strengths', []),
                'advantage': 'home' if home_overall > away_overall else 
                            ('away' if away_overall > home_overall else 'equal'),
                'description': self._generate_metrics_comparison(home_metrics, away_metrics)
            }
        
        # Confronto pattern di gol
        if home_scoring.get('has_data', False) and away_scoring.get('has_data', False):
            comparison['scoring'] = {
                'home_avg_goals': home_scoring['goals_for']['avg_per_game'],
                'away_avg_goals': away_scoring['goals_for']['avg_per_game'],
                'home_avg_conceded': home_scoring['goals_against']['avg_per_game'],
                'away_avg_conceded': away_scoring['goals_against']['avg_per_game'],
                'home_patterns': home_scoring['goals_for']['patterns'],
                'away_patterns': away_scoring['goals_for']['patterns'],
                'home_vulnerability': home_scoring['goals_against']['patterns'],
                'away_vulnerability': away_scoring['goals_against']['patterns'],
                'advantage': self._determine_scoring_advantage(home_scoring, away_scoring),
                'description': self._generate_scoring_comparison(home_scoring, away_scoring)
            }
        
        # Sintesi complessiva
        comparison['overall'] = self._calculate_overall_advantage(comparison)
        
        return comparison
    
    def _generate_form_comparison(self, home_form: Dict[str, Any], 
                               away_form: Dict[str, Any]) -> str:
        """
        Genera una descrizione testuale del confronto della forma.
        
        Args:
            home_form: Dati forma squadra casa
            away_form: Dati forma squadra trasferta
            
        Returns:
            Descrizione testuale
        """
        home_quality = home_form['form_quality']
        away_quality = away_form['form_quality']
        
        if abs(home_quality - away_quality) < 5:
            return "Entrambe le squadre mostrano una forma simile."
            
        if home_quality > away_quality:
            if home_quality - away_quality > 20:
                return "La squadra di casa è in una forma nettamente migliore."
            else:
                return "La squadra di casa è in una forma leggermente migliore."
        else:
            if away_quality - home_quality > 20:
                return "La squadra in trasferta è in una forma nettamente migliore."
            else:
                return "La squadra in trasferta è in una forma leggermente migliore."
    
    def _generate_xg_comparison(self, home_xg: Dict[str, Any], 
                            away_xg: Dict[str, Any]) -> str:
        """
        Genera una descrizione testuale del confronto xG.
        
        Args:
            home_xg: Profilo xG squadra casa
            away_xg: Profilo xG squadra trasferta
            
        Returns:
            Descrizione testuale
        """
        home_attack = home_xg['home']['xg'] if 'home' in home_xg else home_xg['overall']['xg']
        away_attack = away_xg['away']['xg'] if 'away' in away_xg else away_xg['overall']['xg']
        
        home_defense = home_xg['home']['xg_against'] if 'home' in home_xg else home_xg['overall']['xg_against']
        away_defense = away_xg['away']['xg_against'] if 'away' in away_xg else away_xg['overall']['xg_against']
        
        description = []
        
        # Confronto attacchi
        if abs(home_attack - away_attack) < 0.3:
            description.append("Gli attacchi delle due squadre mostrano valori xG simili.")
        elif home_attack > away_attack:
            description.append(f"L'attacco della squadra di casa crea più xG ({home_attack:.2f} vs {away_attack:.2f}).")
        else:
            description.append(f"L'attacco della squadra in trasferta crea più xG ({away_attack:.2f} vs {home_attack:.2f}).")
        
        # Confronto difese
        if abs(home_defense - away_defense) < 0.3:
            description.append("Le difese delle due squadre concedono xG simili.")
        elif home_defense < away_defense:
            description.append(f"La difesa della squadra di casa concede meno xG ({home_defense:.2f} vs {away_defense:.2f}).")
        else:
            description.append(f"La difesa della squadra in trasferta concede meno xG ({away_defense:.2f} vs {home_defense:.2f}).")
        
        return " ".join(description)
    
    def _generate_metrics_comparison(self, home_metrics: Dict[str, Any], 
                                  away_metrics: Dict[str, Any]) -> str:
        """
        Genera una descrizione testuale del confronto delle metriche di performance.
        
        Args:
            home_metrics: Metriche performance squadra casa
            away_metrics: Metriche performance squadra trasferta
            
        Returns:
            Descrizione testuale
        """
        home_attack = home_metrics['overall_rating']['scores']['offensive']
        away_attack = away_metrics['overall_rating']['scores']['offensive']
        
        home_defense = home_metrics['overall_rating']['scores']['defensive']
        away_defense = away_metrics['overall_rating']['scores']['defensive']
        
        home_possession = home_metrics['overall_rating']['scores']['possession']
        away_possession = away_metrics['overall_rating']['scores']['possession']
        
        description = []
        
        # Confronto attacchi
        if abs(home_attack - away_attack) < 5:
            description.append("Le due squadre hanno qualità offensive simili.")
        elif home_attack > away_attack:
            if home_attack - away_attack > 15:
                description.append("La squadra di casa è nettamente più forte in fase offensiva.")
            else:
                description.append("La squadra di casa è più forte in fase offensiva.")
        else:
            if away_attack - home_attack > 15:
                description.append("La squadra in trasferta è nettamente più forte in fase offensiva.")
            else:
                description.append("La squadra in trasferta è più forte in fase offensiva.")
        
        # Confronto difese
        if abs(home_defense - away_defense) < 5:
            description.append("Le due squadre hanno qualità difensive simili.")
        elif home_defense > away_defense:
            if home_defense - away_defense > 15:
                description.append("La squadra di casa è nettamente più forte in fase difensiva.")
            else:
                description.append("La squadra di casa è più forte in fase difensiva.")
        else:
            if away_defense - home_defense > 15:
                description.append("La squadra in trasferta è nettamente più forte in fase difensiva.")
            else:
                description.append("La squadra in trasferta è più forte in fase difensiva.")
        
        # Confronto possesso
        if abs(home_possession - away_possession) < 5:
            description.append("Le due squadre hanno capacità di possesso simili.")
        elif home_possession > away_possession:
            if home_possession - away_possession > 15:
                description.append("La squadra di casa ha un netto vantaggio nel controllo del possesso.")
            else:
                description.append("La squadra di casa ha un vantaggio nel controllo del possesso.")
        else:
            if away_possession - home_possession > 15:
                description.append("La squadra in trasferta ha un netto vantaggio nel controllo del possesso.")
            else:
                description.append("La squadra in trasferta ha un vantaggio nel controllo del possesso.")
        
        return " ".join(description)
