"""
Pacchetto per la generazione di pronostici calcistici.
Questo pacchetto fornisce moduli per prevedere risultati di partite,
analizzare quote di scommessa e identificare tendenze.
"""

# Importa le funzioni principali per renderle disponibili direttamente
from src.analytics.predictions.match_predictor import predict_match, generate_match_prediction
from src.analytics.predictions.bet_analyzer import analyze_bet, find_value_bets, calculate_expected_value
from src.analytics.predictions.value_finder import find_value_opportunities, get_best_value_bets
from src.analytics.predictions.trend_analyzer import analyze_trends, get_match_trends
