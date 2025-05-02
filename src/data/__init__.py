"""
Pacchetto per la raccolta e l'elaborazione dei dati calcistici.
Questo pacchetto fornisce moduli per raccogliere dati da varie fonti (API, scraper, open data)
e processarli in un formato standardizzato per l'analisi.
"""

# Importa le principali funzioni per renderle disponibili direttamente da src.data
from src.data.collector import collect_data, collect_matches, collect_team_stats, collect_head_to_head
