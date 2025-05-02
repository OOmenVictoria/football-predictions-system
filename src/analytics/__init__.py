"""
Pacchetto per l'analisi dei dati e la generazione di pronostici.
Questo pacchetto fornisce moduli per l'analisi statistica dei dati calcistici,
la creazione di modelli predittivi e la generazione di pronostici.
"""

# Importa le funzioni principali per renderle disponibili direttamente da src.analytics
from src.analytics.predictions.match_predictor import predict_match
from src.analytics.predictions.bet_analyzer import analyze_bet, find_value_bets
from src.analytics.statistics.team_form import get_team_form, compare_form
