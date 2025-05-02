""" 
Test per i moduli di previsione del sistema di pronostici.
Questo modulo contiene test per verificare l'accuratezza e la funzionalità
dei modelli predittivi utilizzati per generare pronostici calcistici.
"""
import os
import sys
import unittest
from unittest.mock import patch, MagicMock
import json
import datetime

# Aggiungi il percorso radice al PYTHONPATH per importare moduli del progetto
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.analytics.predictions.match_predictor import MatchPredictor
from src.analytics.predictions.bet_analyzer import BetAnalyzer
from src.analytics.predictions.value_finder import ValueFinder
from src.analytics.models.basic_model import BasicModel
from src.analytics.models.poisson_model import PoissonModel
from src.analytics.models.xg_model import XGModel


class TestMatchPredictor(unittest.TestCase):
    """Test per il predittore di partite."""
    
    def setUp(self):
        """Inizializza i dati per i test."""
        # Mock dell'oggetto database
        self.db_mock = MagicMock()
        
        # Dati di esempio per una partita
        self.sample_match = {
            "match_id": "test_match_123",
            "home_team": "AC Milan",
            "away_team": "Inter",
            "home_team_id": "milan",
            "away_team_id": "inter",
            "league_id": "serie_a",
            "datetime": "2025-05-15T20:45:00Z",
            "status": "SCHEDULED"
        }
        
        # Mock dei modelli predittivi
        self.basic_model_mock = MagicMock()
        self.basic_model_mock.predict_match.return_value = {
            "probabilities": {"1": 0.40, "X": 0.30, "2": 0.30},
            "btts": {"Yes": 0.65, "No": 0.35},
            "over_under": {"2.5": {"Over": 0.55, "Under": 0.45}},
            "reasoning": ["Reason 1", "Reason 2"]
        }
        
        # Crea il predittore con mock
        with patch('src.analytics.predictions.match_predictor.FirebaseManager', return_value=self.db_mock):
            with patch('src.analytics.predictions.match_predictor.BasicModel', return_value=self.basic_model_mock):
                self.predictor = MatchPredictor()
    
    def test_predict_match(self):
        """Testa la funzione di previsione per una singola partita."""
        # Configura il mock per il database
        self.db_mock.get_reference.return_value.child.return_value.get.return_value = self.sample_match
        
        # Esegui la previsione
        prediction = self.predictor.predict_match("test_match_123")
        
        # Verifica che la previsione contenga i dati attesi
        self.assertIsNotNone(prediction)
        self.assertIn("probabilities", prediction)
        self.assertIn("btts", prediction)
        self.assertIn("over_under", prediction)
        
        # Verifica che i valori delle probabilità siano coerenti
        self.assertAlmostEqual(sum(prediction["probabilities"].values()), 1.0, places=2)
    
    def test_generate_detailed_prediction(self):
        """Testa la generazione di una previsione dettagliata."""
        # Configura il mock per il database
        self.db_mock.get_reference.return_value.child.return_value.get.return_value = self.sample_match
        
        # Mock per form e xG data
        with patch.object(self.predictor, '_get_team_form', return_value={"form_quality": 75}):
            with patch.object(self.predictor, '_get_team_xg_profile', return_value={"overall": {"xg": 1.5}}):
                # Esegui la previsione dettagliata
                prediction = self.predictor._generate_detailed_prediction(
                    {"probabilities": {"1": 0.40, "X": 0.30, "2": 0.30}},
                    self.sample_match
                )
                
                # Verifica i dati della previsione
                self.assertIn("specific_bets", prediction)
                self.assertIn("reasoning", prediction)
                self.assertTrue(len(prediction["reasoning"]) > 0)


class TestBetAnalyzer(unittest.TestCase):
    """Test per l'analizzatore di scommesse."""
    
    def setUp(self):
        """Inizializza i dati per i test."""
        # Mock dell'oggetto database
        self.db_mock = MagicMock()
        
        # Crea l'analizzatore con mock
        with patch('src.analytics.predictions.bet_analyzer.FirebaseManager', return_value=self.db_mock):
            self.analyzer = BetAnalyzer()
        
        # Previsione di esempio
        self.sample_prediction = {
            "probabilities": {"1": 0.45, "X": 0.30, "2": 0.25},
            "btts": {"Yes": 0.60, "No": 0.40},
            "over_under": {"2.5": {"Over": 0.55, "Under": 0.45}}
        }
        
        # Quote di esempio
        self.sample_odds = {
            "1X2": {"1": 2.00, "X": 3.40, "2": 4.00},
            "btts": {"Yes": 1.85, "No": 1.95},
            "over_under": {"2.5": {"Over": 1.90, "Under": 1.90}}
        }
    
    def test_calculate_value(self):
        """Testa il calcolo del valore atteso delle quote."""
        expected_values = self.analyzer.calculate_expected_values(
            self.sample_prediction, self.sample_odds
        )
        
        # Verifica che il valore atteso sia calcolato correttamente
        self.assertIn("1X2", expected_values)
        self.assertIn("btts", expected_values)
        
        # Calcolo manuale per confronto: EV = (probabilità * quota) - 1
        expected_ev_1 = (self.sample_prediction["probabilities"]["1"] * 
                         self.sample_odds["1X2"]["1"]) - 1
        self.assertAlmostEqual(expected_values["1X2"]["1"], expected_ev_1, places=2)
    
    def test_find_value_bets(self):
        """Testa l'identificazione delle value bet."""
        value_bets = self.analyzer.find_value_bets(
            self.sample_prediction, self.sample_odds
        )
        
        # Verifica che siano state identificate delle value bet
        self.assertIsInstance(value_bets, list)
        
        # Se ci sono value bet, controlla che abbiano i campi necessari
        if value_bets:
            self.assertIn("market", value_bets[0])
            self.assertIn("selection", value_bets[0])
            self.assertIn("odds", value_bets[0])
            self.assertIn("expected_value", value_bets[0])


class TestValueFinder(unittest.TestCase):
    """Test per il trovatore di value bet."""
    
    def setUp(self):
        """Inizializza i dati per i test."""
        # Mock dell'oggetto database
        self.db_mock = MagicMock()
        
        # Crea il value finder con mock
        with patch('src.analytics.predictions.value_finder.FirebaseManager', return_value=self.db_mock):
            with patch('src.analytics.predictions.value_finder.MatchPredictor'):
                with patch('src.analytics.predictions.value_finder.BetAnalyzer'):
                    self.value_finder = ValueFinder()
    
    def test_find_value_for_match(self):
        """Testa la ricerca di value bet per una singola partita."""
        # Mock dei dati necessari
        match_data = {
            "match_id": "test_match_123",
            "home_team": "AC Milan",
            "away_team": "Inter"
        }
        
        prediction = {
            "probabilities": {"1": 0.45, "X": 0.30, "2": 0.25},
            "btts": {"Yes": 0.60, "No": 0.40},
            "over_under": {"2.5": {"Over": 0.55, "Under": 0.45}}
        }
        
        odds = {
            "1X2": {"1": 2.00, "X": 3.40, "2": 4.00},
            "btts": {"Yes": 1.85, "No": 1.95},
            "over_under": {"2.5": {"Over": 1.90, "Under": 1.90}}
        }
        
        # Mock dei metodi interni
        with patch.object(self.value_finder, '_get_match_prediction', return_value=prediction):
            with patch.object(self.value_finder, '_get_odds', return_value=odds):
                with patch.object(self.value_finder, '_analyze_value', return_value=[{"market": "1X2", "selection": "1", "odds": 2.00, "expected_value": 0.10}]):
                    # Esegui il test
                    result = self.value_finder.find_value_for_match("test_match_123")
                    
                    # Verifica il risultato
                    self.assertIsNotNone(result)
                    self.assertIn("match_id", result)
                    self.assertIn("value_bets", result)
                    self.assertTrue(len(result["value_bets"]) > 0)


if __name__ == "__main__":
    unittest.main()
