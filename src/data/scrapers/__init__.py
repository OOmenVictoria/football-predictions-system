"""
Package per gli scraper web per l'estrazione di dati calcistici.
Questo package fornisce implementazioni di vari scraper per estrarre
dati da siti web di statistiche calcistiche.
"""

from src.data.scrapers.base_scraper import BaseScraper
from src.data.scrapers.flashscore import FlashScoreScraper
from src.data.scrapers.soccerway import SoccerwayScraper
from src.data.scrapers.worldfootball import WorldFootballScraper
from src.data.scrapers.transfermarkt import TransfermarktScraper
from src.data.scrapers.wikipedia import WikipediaScraper
from src.data.scrapers.eleven_v_eleven import ElevenVElevenScraper

def get_scraper(scraper_type):
    """
    Ottiene un'istanza dello scraper specificato.
    
    Args:
        scraper_type: Tipo di scraper ('flashscore', 'soccerway', ecc.)
    
    Returns:
        Istanza dello scraper richiesto
    """
    if scraper_type == 'flashscore':
        return FlashScoreScraper()
    elif scraper_type == 'soccerway':
        return SoccerwayScraper()
    elif scraper_type == 'worldfootball':
        return WorldFootballScraper()
    elif scraper_type == 'transfermarkt':
        return TransfermarktScraper()
    elif scraper_type == 'wikipedia':
        return WikipediaScraper()
    elif scraper_type == 'eleven_v_eleven':
        return ElevenVElevenScraper()
    else:
        raise ValueError(f"Tipo di scraper non supportato: {scraper_type}")
