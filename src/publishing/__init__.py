"""
Pacchetto per la pubblicazione di contenuti su piattaforme web.
Questo pacchetto fornisce moduli per pubblicare, aggiornare e rimuovere
articoli su un sito WordPress tramite l'API REST.
"""

from src.publishing.publisher import publish_articles, cleanup_articles, run_publication_cycle
from src.publishing.wordpress import publish_to_wordpress, delete_article
