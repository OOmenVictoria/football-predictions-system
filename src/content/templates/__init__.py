"""
Package per i template degli articoli di pronostico.
Questo package fornisce moduli per generare diverse sezioni degli articoli,
come anteprima partita, pronostici e analisi statistiche.
"""

from src.content.templates.match_preview import generate_match_preview
from src.content.templates.prediction import generate_prediction_section
from src.content.templates.stats_analysis import generate_stats_analysis
