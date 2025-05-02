"""
Test per i moduli scraper per l'estrazione di dati calcistici.
Questo modulo contiene test per verificare il corretto funzionamento
degli scraper che estraggono dati da vari siti web.
"""
import os
import sys
import unittest
from unittest.mock import patch, MagicMock
import json

# Aggiungi la directory radice al path di Python per permettere import relativi
test_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(test_dir))
sys.path.insert(0, root_dir)

from src.data.scrapers.base_scraper import BaseScraper
from src.data.scrapers.flashscore import FlashScoreScraper
from src.data.scrapers.soccerway import SoccerwayScraper
from src.data.scrapers.transfermarkt import TransfermarktScraper
from src.data.scrapers.wikipedia import WikipediaScraper

class TestBaseScraper(unittest.TestCase):
    """Test per la classe base BaseScraper."""
    
    def setUp(self):
        """Setup per i test."""
        self.scraper = BaseScraper()
    
    def test_init(self):
        """Test inizializzazione scraper."""
        self.assertIsInstance(self.scraper, BaseScraper)
    
    def test_rate_limiting(self):
        """Test del rate limiting."""
        with patch('time.sleep') as mock_sleep:
            self.scraper.request("https://example.com")
            self.scraper.request("https://example.com")
            
            # Verifica che sleep sia stato chiamato
            mock_sleep.assert_called()
    
    def test_caching(self):
        """Test del sistema di cache."""
        with patch('requests.get') as mock_get:
            # Configura il mock
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.text = "Test content"
            mock_get.return_value = mock_response
            
            # Prima richiesta (dovrebbe fare la chiamata)
            result1 = self.scraper.request("https://example.com", use_cache=True)
            
            # Seconda richiesta (dovrebbe usare la cache)
            result2 = self.scraper.request("https://example.com", use_cache=True)
            
            # Verifica che requests.get sia stato chiamato solo una volta
            self.assertEqual(mock_get.call_count, 1)
            
            # Verifica che i risultati siano gli stessi
            self.assertEqual(result1.text, result2.text)

class TestFlashScoreScraper(unittest.TestCase):
    """Test per FlashScoreScraper."""
    
    def setUp(self):
        """Setup per i test."""
        self.scraper = FlashScoreScraper()
    
    @patch('src.data.scrapers.flashscore.FlashScoreScraper.request')
    def test_get_matches_by_date(self, mock_request):
        """Test recupero partite per data."""
        # Configura il mock per simulare la risposta HTML
        with open(os.path.join(test_dir, 'fixtures', 'flashscore_matches.html'), 'r', encoding='utf-8') as f:
            mock_response = MagicMock()
            mock_response.text = f.read()
            mock_request.return_value = mock_response
        
        # Esegui il metodo
        matches = self.scraper.get_matches_by_date("2023-05-20")
        
        # Verifiche
        self.assertIsInstance(matches, list)
        self.assertGreater(len(matches), 0)
        
        # Verifica struttura di un match
        if matches:
            match = matches[0]
            self.assertIn('home_team', match)
            self.assertIn('away_team', match)
            self.assertIn('time', match)
    
    @patch('src.data.scrapers.flashscore.FlashScoreScraper.request')
    def test_get_match_details(self, mock_request):
        """Test recupero dettagli partita."""
        # Configura il mock per simulare la risposta HTML
        with open(os.path.join(test_dir, 'fixtures', 'flashscore_match_details.html'), 'r', encoding='utf-8') as f:
            mock_response = MagicMock()
            mock_response.text = f.read()
            mock_request.return_value = mock_response
        
        # Esegui il metodo
        details = self.scraper.get_match_details("ABC123")
        
        # Verifiche
        self.assertIsInstance(details, dict)
        self.assertIn('home_team', details)
        self.assertIn('away_team', details)
        self.assertIn('score', details)
        self.assertIn('statistics', details)

class TestSoccerwayScraper(unittest.TestCase):
    """Test per SoccerwayScraper."""
    
    def setUp(self):
        """Setup per i test."""
        self.scraper = SoccerwayScraper()
    
    @patch('src.data.scrapers.soccerway.SoccerwayScraper.request')
    def test_search_team(self, mock_request):
        """Test ricerca squadra."""
        # Configura il mock per simulare la risposta HTML
        with open(os.path.join(test_dir, 'fixtures', 'soccerway_search.html'), 'r', encoding='utf-8') as f:
            mock_response = MagicMock()
            mock_response.text = f.read()
            mock_request.return_value = mock_response
        
        # Esegui il metodo
        results = self.scraper.search_team("AC Milan")
        
        # Verifiche
        self.assertIsInstance(results, list)
        self.assertGreater(len(results), 0)
        
        # Verifica struttura di un risultato
        if results:
            result = results[0]
            self.assertIn('name', result)
            self.assertIn('url', result)
    
    @patch('src.data.scrapers.soccerway.SoccerwayScraper.request')
    def test_get_team_info(self, mock_request):
        """Test recupero info squadra."""
        # Configura il mock per simulare la risposta HTML
        with open(os.path.join(test_dir, 'fixtures', 'soccerway_team.html'), 'r', encoding='utf-8') as f:
            mock_response = MagicMock()
            mock_response.text = f.read()
            mock_request.return_value = mock_response
        
        # Esegui il metodo
        info = self.scraper.get_team_info("https://example.com/teams/123")
        
        # Verifiche
        self.assertIsInstance(info, dict)
        self.assertIn('name', info)
        self.assertIn('country', info)
        self.assertIn('stadium', info)
        self.assertIn('manager', info)

class TestTransfermarktScraper(unittest.TestCase):
    """Test per TransfermarktScraper."""
    
    def setUp(self):
        """Setup per i test."""
        self.scraper = TransfermarktScraper()
    
    @patch('src.data.scrapers.transfermarkt.TransfermarktScraper.request')
    def test_get_team_info(self, mock_request):
        """Test recupero info squadra."""
        # Configura il mock per simulare la risposta HTML
        with open(os.path.join(test_dir, 'fixtures', 'transfermarkt_team.html'), 'r', encoding='utf-8') as f:
            mock_response = MagicMock()
            mock_response.text = f.read()
            mock_request.return_value = mock_response
        
        # Esegui il metodo
        info = self.scraper.get_team_info("AC-Milan")
        
        # Verifiche
        self.assertIsInstance(info, dict)
        self.assertIn('name', info)
        self.assertIn('market_value', info)
        self.assertIn('squad_size', info)
        self.assertIn('average_age', info)
    
    @patch('src.data.scrapers.transfermarkt.TransfermarktScraper.request')
    def test_get_player_info(self, mock_request):
        """Test recupero info giocatore."""
        # Configura il mock per simulare la risposta HTML
        with open(os.path.join(test_dir, 'fixtures', 'transfermarkt_player.html'), 'r', encoding='utf-8') as f:
            mock_response = MagicMock()
            mock_response.text = f.read()
            mock_request.return_value = mock_response
        
        # Esegui il metodo
        info = self.scraper.get_player_info("Lionel-Messi")
        
        # Verifiche
        self.assertIsInstance(info, dict)
        self.assertIn('name', info)
        self.assertIn('position', info)
        self.assertIn('market_value', info)
        self.assertIn('team', info)

class TestWikipediaScraper(unittest.TestCase):
    """Test per WikipediaScraper."""
    
    def setUp(self):
        """Setup per i test."""
        self.scraper = WikipediaScraper()
    
    @patch('src.data.scrapers.wikipedia.WikipediaScraper._make_api_request')
    def test_search(self, mock_api_request):
        """Test ricerca su Wikipedia."""
        # Configura il mock per simulare la risposta JSON
        mock_api_request.return_value = {
            "query": {
                "search": [
                    {
                        "title": "AC Milan",
                        "snippet": "AC Milan is a professional football club..."
                    },
                    {
                        "title": "Milan",
                        "snippet": "Milan is a city in northern Italy..."
                    }
                ]
            }
        }
        
        # Esegui il metodo
        results = self.scraper.search("AC Milan")
        
        # Verifiche
        self.assertIsInstance(results, list)
        self.assertEqual(len(results), 2)
        
        # Verifica struttura di un risultato
        result = results[0]
        self.assertIn('title', result)
        self.assertIn('description', result)
        self.assertIn('url', result)
    
    @patch('src.data.scrapers.wikipedia.WikipediaScraper.get_page_content')
    def test_get_team_info(self, mock_get_page):
        """Test recupero info squadra."""
        # Configura il mock per simulare la risposta HTML
        with open(os.path.join(test_dir, 'fixtures', 'wikipedia_team.html'), 'r', encoding='utf-8') as f:
            mock_get_page.return_value = f.read()
        
        # Patch anche il metodo search per controllare il flusso
        with patch('src.data.scrapers.wikipedia.WikipediaScraper.search') as mock_search:
            mock_search.return_value = [
                {
                    "title": "AC Milan",
                    "description": "AC Milan is a professional football club...",
                    "url": "https://en.wikipedia.org/wiki/AC_Milan"
                }
            ]
            
            # Esegui il metodo
            info = self.scraper.get_team_info("AC Milan")
            
            # Verifiche
            self.assertIsInstance(info, dict)
            self.assertIn('title', info)
            self.assertIn('url', info)
            self.assertIn('history', info)
            self.assertIn('honours', info)
            self.assertIn('stadium', info)

if __name__ == "__main__":
    unittest.main()
