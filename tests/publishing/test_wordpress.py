""" 
Test per il modulo di pubblicazione su WordPress.
Questo modulo contiene test per verificare la corretta funzionalità 
dell'integrazione con WordPress per la pubblicazione degli articoli.
"""
import os
import sys
import unittest
from unittest.mock import patch, MagicMock
import json
import datetime

# Aggiungi il percorso radice al PYTHONPATH per importare moduli del progetto
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.publishing.wordpress import WordPressPublisher
from src.utils.database import FirebaseManager


class TestWordPressPublisher(unittest.TestCase):
    """Test per la classe di pubblicazione su WordPress."""
    
    def setUp(self):
        """Inizializza i dati per i test."""
        # Mock delle configurazioni
        self.config_patcher = patch('src.publishing.wordpress.get_setting')
        self.mock_get_setting = self.config_patcher.start()
        self.mock_get_setting.side_effect = lambda key, default=None: {
            'wordpress.url': 'https://test-blog.com/wp-json/wp/v2',
            'wordpress.username': 'test_user',
            'wordpress.password': 'test_password',
            'wordpress.categories': {'serie_a': 1, 'premier_league': 2},
            'wordpress.tags': {'prediction': 3, 'football': 4}
        }.get(key, default)
        
        # Inizializza il publisher
        self.publisher = WordPressPublisher()
        
        # Dati di esempio per gli articoli
        self.sample_article = {
            'title': 'AC Milan vs Inter - Serie A Preview',
            'content': '<p>Test content for the article</p>',
            'excerpt': 'Preview of the Milan derby',
            'categories': ['serie_a', 'prediction'],
            'tags': ['milan', 'inter', 'derby'],
            'metadata': {
                'match_id': 'milan_inter_20250515',
                'league_id': 'serie_a',
                'match_datetime': '2025-05-15T20:45:00Z',
                'expiry_time': '2025-05-16T06:45:00Z'
            }
        }
    
    def tearDown(self):
        """Pulizia dopo i test."""
        self.config_patcher.stop()
    
    @patch('src.publishing.wordpress.requests.post')
    def test_publish_article(self, mock_post):
        """Testa la pubblicazione di un articolo."""
        # Configura il mock per simulare una risposta positiva
        mock_response = MagicMock()
        mock_response.status_code = 201
        mock_response.json.return_value = {
            'id': 12345,
            'link': 'https://test-blog.com/2025/05/15/ac-milan-vs-inter'
        }
        mock_post.return_value = mock_response
        
        # Esegui il test di pubblicazione
        result = self.publisher.publish_article(
            title=self.sample_article['title'],
            content=self.sample_article['content'],
            excerpt=self.sample_article['excerpt'],
            categories=self.sample_article['categories'],
            tags=self.sample_article['tags'],
            metadata=self.sample_article['metadata']
        )
        
        # Verifica che la richiesta POST sia stata chiamata correttamente
        mock_post.assert_called_once()
        
        # Verifica i dati della richiesta
        call_args = mock_post.call_args[1]
        self.assertIn('json', call_args)
        post_data = call_args['json']
        
        # Controlla che i dati inviati siano corretti
        self.assertEqual(post_data['title'], self.sample_article['title'])
        self.assertEqual(post_data['content'], self.sample_article['content'])
        self.assertEqual(post_data['excerpt'], self.sample_article['excerpt'])
        self.assertIn('categories', post_data)
        self.assertIn('tags', post_data)
        self.assertIn('meta', post_data)
        
        # Verifica il risultato
        self.assertTrue(result['success'])
        self.assertEqual(result['post_id'], 12345)
        self.assertEqual(result['url'], 'https://test-blog.com/2025/05/15/ac-milan-vs-inter')
    
    @patch('src.publishing.wordpress.requests.post')
    def test_publish_failure(self, mock_post):
        """Testa la gestione degli errori durante la pubblicazione."""
        # Configura il mock per simulare un errore
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.text = '{"code":"rest_cannot_create","message":"Sorry, you are not allowed to create posts."}'
        mock_post.return_value = mock_response
        
        # Esegui il test
        result = self.publisher.publish_article(
            title=self.sample_article['title'],
            content=self.sample_article['content'],
            excerpt=self.sample_article['excerpt'],
            categories=self.sample_article['categories'],
            tags=self.sample_article['tags'],
            metadata=self.sample_article['metadata']
        )
        
        # Verifica la gestione dell'errore
        self.assertFalse(result['success'])
        self.assertIn('error', result)
        self.assertIn('401', result['error'])
    
    @patch('src.publishing.wordpress.requests.delete')
    def test_delete_article(self, mock_delete):
        """Testa la cancellazione di un articolo."""
        # Configura il mock per simulare una risposta positiva
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_delete.return_value = mock_response
        
        # Esegui il test di cancellazione
        result = self.publisher.delete_article(12345)
        
        # Verifica che la richiesta DELETE sia stata chiamata correttamente
        mock_delete.assert_called_once()
        
        # Verifica il risultato
        self.assertTrue(result)
    
    @patch('src.publishing.wordpress.requests.get')
    def test_get_expired_articles(self, mock_get):
        """Testa il recupero degli articoli scaduti."""
        # Configura il mock per simulare una risposta con articoli
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {
                'id': 12345,
                'title': {'rendered': 'AC Milan vs Inter - Serie A Preview'},
                'meta': {'expiry_time': '2025-05-15T06:45:00Z'}
            },
            {
                'id': 12346,
                'title': {'rendered': 'Juventus vs Roma - Serie A Preview'},
                'meta': {'expiry_time': '2025-05-16T06:45:00Z'}
            }
        ]
        mock_get.return_value = mock_response
        
        # Imposta un'ora di test
        test_time = datetime.datetime.fromisoformat('2025-05-15T07:00:00+00:00')
        
        # Esegui il test
        with patch('src.publishing.wordpress.datetime') as mock_datetime:
            mock_datetime.now.return_value = test_time
            mock_datetime.fromisoformat = datetime.datetime.fromisoformat
            
            expired = self.publisher.get_expired_articles()
            
            # Verifica che siano stati identificati gli articoli scaduti corretti
            self.assertEqual(len(expired), 1)
            self.assertEqual(expired[0]['id'], 12345)
    
    @patch('src.publishing.wordpress.requests.get')
    def test_test_connection(self, mock_get):
        """Testa la verifica della connessione a WordPress."""
        # Configura il mock per simulare una risposta positiva
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response
        
        # Esegui il test
        result = self.publisher.test_connection()
        
        # Verifica che la richiesta GET sia stata chiamata correttamente
        mock_get.assert_called_once()
        
        # Verifica il risultato
        self.assertTrue(result)


if __name__ == "__main__":
    unittest.main()
