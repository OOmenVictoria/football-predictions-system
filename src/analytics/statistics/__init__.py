"""
Pacchetto per il calcolo e l'analisi delle statistiche calcistiche.
Questo pacchetto fornisce moduli per analizzare vari aspetti delle statistiche
di squadre e giocatori, come forma, pattern di gol, e metriche avanzate.
"""

# Importa le funzioni principali per renderle disponibili direttamente
from src.analytics.statistics.team_form import get_team_form, compare_form
from src.analytics.statistics.xg_analysis import get_team_xg_profile, analyze_match_xg
from src.analytics.statistics.performance_metrics import get_team_performance_metrics
from src.analytics.statistics.scoring_patterns import get_team_scoring_patterns, analyze_goal_patterns
