#!/usr/bin/env python3
""" 
Script per il test delle connessioni del sistema.
Questo script verifica la connettività con tutti i servizi esterni utilizzati dal sistema,
inclusi API, siti web per scraping, Firebase e WordPress.
"""
import os
import sys
import argparse
import logging
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Aggiungi il percorso radice al PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from src.utils.database import FirebaseManager
from src.utils.http import make_request
from src.config.settings import get_setting
from src.publishing.wordpress import WordPressPublisher
from src.data.api.football_data import FootballDataAPI
from src.data.api.api_football import APIFootballClient
from src.translation.services.libre_translate import LibreTranslateService
from src.translation.services.lingva import LingvaService

# Configurazione logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(os.path.dirname(__file__), '../logs/connection_test.log'))
    ]
)
logger = logging.getLogger('connection_test')

class ConnectionTester:
    """Classe per il test delle connessioni a servizi esterni."""
    
    def __init__(self):
        """Inizializza il tester di connessione."""
        self.results = {}
        self.db = None
        
        try:
            self.db = FirebaseManager()
            logger.info("FirebaseManager inizializzato con successo.")
        except Exception as e:
            logger.error(f"Errore durante l'inizializzazione di FirebaseManager: {str(e)}")
    
    def test_firebase(self):
        """Testa la connessione a Firebase."""
        start_time = time.time()
        try:
            if self.db is None:
                self.db = FirebaseManager()
            
            # Prova a leggere un valore dal database
            test_ref = self.db.get_reference('connection_test')
            test_ref.set({
                'timestamp': time.time(),
                'success': True
            })
            test_result = test_ref.get()
            
            # Verifica il risultato
            if test_result and 'success' in test_result and test_result['success']:
                status = "OK"
                message = "Connessione a Firebase riuscita"
            else:
                status = "ERROR"
                message = "Test fallito: dati non corrispondenti"
        except Exception as e:
            status = "ERROR"
            message = f"Errore durante il test di Firebase: {str(e)}"
            logger.error(message)
        
        duration = time.time() - start_time
        return {
            'service': 'Firebase Realtime Database',
            'status': status,
            'message': message,
            'duration': duration
        }
    
    def test_wordpress(self):
        """Testa la connessione all'API WordPress."""
        start_time = time.time()
        try:
            wp = WordPressPublisher()
            if wp.test_connection():
                status = "OK"
                message = "Connessione a WordPress riuscita"
            else:
                status = "ERROR"
                message = "Test fallito: impossibile connettersi a WordPress"
        except Exception as e:
            status = "ERROR"
            message = f"Errore durante il test di WordPress: {str(e)}"
            logger.error(message)
        
        duration = time.time() - start_time
        return {
            'service': 'WordPress API',
            'status': status,
            'message': message,
            'duration': duration
        }
    
    def test_football_api(self):
        """Testa la connessione alle API calcistiche."""
        results = []
        
        # Test Football-Data.org
        start_time = time.time()
        try:
            football_data = FootballDataAPI()
            competitions = football_data.get_competitions()
            
            if competitions and len(competitions) > 0:
                status = "OK"
                message = f"Connessione a Football-Data.org riuscita: {len(competitions)} competizioni trovate"
            else:
                status = "WARNING"
                message = "Connessione riuscita ma nessun dato ricevuto"
        except Exception as e:
            status = "ERROR"
            message = f"Errore durante il test di Football-Data.org: {str(e)}"
            logger.error(message)
        
        duration = time.time() - start_time
        results.append({
            'service': 'Football-Data.org API',
            'status': status,
            'message': message,
            'duration': duration
        })
        
        # Test API-Football
        start_time = time.time()
        try:
            api_football = APIFootballClient()
            status_result = api_football.get_status()
            
            if status_result and 'response' in status_result:
                status = "OK"
                message = "Connessione a API-Football riuscita"
            else:
                status = "WARNING"
                message = "Connessione riuscita ma risposta non valida"
        except Exception as e:
            status = "ERROR"
            message = f"Errore durante il test di API-Football: {str(e)}"
            logger.error(message)
        
        duration = time.time() - start_time
        results.append({
            'service': 'API-Football',
            'status': status,
            'message': message,
            'duration': duration
        })
        
        return results
    
    def test_translation_services(self):
        """Testa la connessione ai servizi di traduzione."""
        results = []
        
        # Test LibreTranslate
        start_time = time.time()
        try:
            libre = LibreTranslateService()
            translation = libre.translate("Hello, world!", "en", "it")
            
            if translation and translation.lower().startswith("ciao"):
                status = "OK"
                message = "Connessione a LibreTranslate riuscita"
            else:
                status = "WARNING"
                message = "Connessione riuscita ma traduzione non valida"
        except Exception as e:
            status = "ERROR"
            message = f"Errore durante il test di LibreTranslate: {str(e)}"
            logger.error(message)
        
        duration = time.time() - start_time
        results.append({
            'service': 'LibreTranslate',
            'status': status,
            'message': message,
            'duration': duration
        })
        
        # Test Lingva
        start_time = time.time()
        try:
            lingva = LingvaService()
            translation = lingva.translate("Hello, world!", "en", "it")
            
            if translation and translation.lower().startswith("ciao"):
                status = "OK"
                message = "Connessione a Lingva riuscita"
            else:
                status = "WARNING"
                message = "Connessione riuscita ma traduzione non valida"
        except Exception as e:
            status = "ERROR"
            message = f"Errore durante il test di Lingva: {str(e)}"
            logger.error(message)
        
        duration = time.time() - start_time
        results.append({
            'service': 'Lingva',
            'status': status,
            'message': message,
            'duration': duration
        })
        
        return results
    
    def test_web_scraping(self):
        """Testa l'accesso ai siti web utilizzati per lo scraping."""
        urls = {
            'Flashscore': 'https://www.flashscore.com',
            'Soccerway': 'https://int.soccerway.com',
            'Transfermarkt': 'https://www.transfermarkt.com',
            'FBref': 'https://fbref.com/en/',
            'Understat': 'https://understat.com',
            'FootyStats': 'https://footystats.org',
            'SofaScore': 'https://www.sofascore.com'
        }
        
        results = []
        
        for site_name, url in urls.items():
            start_time = time.time()
            try:
                response = make_request(
                    url,
                    headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
                )
                
                if response and response.status_code == 200:
                    status = "OK"
                    message = f"Accesso a {site_name} riuscito"
                else:
                    status = "WARNING"
                    message = f"Accesso a {site_name} fallito con status code: {response.status_code if response else 'N/A'}"
            except Exception as e:
                status = "ERROR"
                message = f"Errore durante l'accesso a {site_name}: {str(e)}"
                logger.error(message)
            
            duration = time.time() - start_time
            results.append({
                'service': f"{site_name} (Web Scraping)",
                'status': status,
                'message': message,
                'duration': duration
            })
        
        return results
    
    def run_all_tests(self):
        """Esegue tutti i test di connessione."""
        logger.info("Avvio test di connessione completo...")
        
        # Definizione dei test da eseguire
        tests = [
            self.test_firebase,
            self.test_wordpress,
            lambda: self.test_football_api(),
            lambda: self.test_translation_services(),
            lambda: self.test_web_scraping()
        ]
        
        all_results = []
        start_time = time.time()
        
        # Esecuzione parallela dei test
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_test = {executor.submit(test): test for test in tests}
            
            for future in as_completed(future_to_test):
                try:
                    result = future.result()
                    if isinstance(result, list):
                        all_results.extend(result)
                    else:
                        all_results.append(result)
                except Exception as e:
                    logger.error(f"Errore durante l'esecuzione di un test: {str(e)}")
        
        # Calcolo durata totale
        total_duration = time.time() - start_time
        
        # Conteggio risultati per tipo
        status_counts = {"OK": 0, "WARNING": 0, "ERROR": 0}
        for result in all_results:
            if 'status' in result:
                status_counts[result['status']] = status_counts.get(result['status'], 0) + 1
        
        # Ordina i risultati per stato (errori prima) e poi per durata
        all_results.sort(key=lambda x: (0 if x['status'] == 'ERROR' else 1 if x['status'] == 'WARNING' else 2, x['duration']))
        
        summary = {
            'total_tests': len(all_results),
            'status_counts': status_counts,
            'total_duration': total_duration,
            'timestamp': time.time(),
            'results': all_results
        }
        
        # Salva risultati su Firebase
        if self.db:
            try:
                self.db.get_reference('system_health/connection_tests').set(summary)
                logger.info("Risultati salvati su Firebase")
            except Exception as e:
                logger.error(f"Errore durante il salvataggio dei risultati su Firebase: {str(e)}")
        
        return summary
    
    def print_results(self, results):
        """Stampa i risultati dei test in modo leggibile."""
        print("\n===== TEST DI CONNESSIONE - RISULTATI =====")
        print(f"Test completati: {results['total_tests']}")
        print(f"Successi: {results['status_counts'].get('OK', 0)}")
        print(f"Avvisi: {results['status_counts'].get('WARNING', 0)}")
        print(f"Errori: {results['status_counts'].get('ERROR', 0)}")
        print(f"Durata totale: {results['total_duration']:.2f} secondi")
        print("\nDettaglio test:")
        
        for result in results['results']:
            status_symbol = "✅" if result['status'] == "OK" else "⚠️" if result['status'] == "WARNING" else "❌"
            print(f"{status_symbol} {result['service']}: {result['message']} ({result['duration']:.2f}s)")
        
        print("\n=========================================")


def main():
    """Funzione principale per l'esecuzione del test di connessione."""
    parser = argparse.ArgumentParser(description='Test di connessione per il sistema di pronostici calcistici')
    parser.add_argument('--service', help='Testa solo un servizio specifico (firebase, wordpress, football_api, translation, scraping)')
    parser.add_argument('--json', action='store_true', help='Output in formato JSON')
    parser.add_argument('--save', action='store_true', help='Salva i risultati su Firebase')
    
    args = parser.parse_args()
    
    tester = ConnectionTester()
    
    if args.service:
        # Esegui un test specifico
        if args.service == 'firebase':
            results = {'results': [tester.test_firebase()]}
        elif args.service == 'wordpress':
            results = {'results': [tester.test_wordpress()]}
        elif args.service == 'football_api':
            results = {'results': tester.test_football_api()}
        elif args.service == 'translation':
            results = {'results': tester.test_translation_services()}
        elif args.service == 'scraping':
            results = {'results': tester.test_web_scraping()}
        else:
            print(f"Servizio sconosciuto: {args.service}")
            sys.exit(1)
        
        results['total_tests'] = len(results['results'])
        status_counts = {"OK": 0, "WARNING": 0, "ERROR": 0}
        for result in results['results']:
            status_counts[result['status']] = status_counts.get(result['status'], 0) + 1
        results['status_counts'] = status_counts
    else:
        # Esegui tutti i test
        results = tester.run_all_tests()
    
    if args.json:
        # Output in formato JSON
        print(json.dumps(results, indent=2))
    else:
        # Output leggibile
        tester.print_results(results)


if __name__ == "__main__":
    main()
