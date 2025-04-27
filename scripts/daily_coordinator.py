#!/usr/bin/env python3
"""
Script coordinatore giornaliero - Eseguito una volta al giorno
Coordina i vari componenti del sistema
"""
import os
import sys
import logging
import subprocess
import time
from datetime import datetime
import firebase_admin
from firebase_admin import credentials, db
from dotenv import load_dotenv

# Configurazione logging
log_dir = os.path.expanduser('~/football-predictions/logs')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"coordinator_{datetime.now().strftime('%Y%m%d')}.log")

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Variabili globali
load_dotenv()

def initialize_firebase():
    """Inizializza connessione Firebase"""
    try:
        firebase_admin.get_app()
    except ValueError:
        cred_path = os.path.expanduser('~/football-predictions/creds/firebase-credentials.json')
        cred = credentials.Certificate(cred_path)
        firebase_admin.initialize_app(cred, {
            'databaseURL': os.getenv('FIREBASE_DB_URL')
        })
    return True

def run_script(script_path, script_name):
    """Esegue uno script Python e gestisce l'output"""
    full_path = os.path.expanduser(script_path)
    logging.info(f"Avvio script: {script_name}")
    
    try:
        env = os.environ.copy()
        env["PYTHONPATH"] = os.path.expanduser("~/football-predictions")
        
        start_time = datetime.now()
        result = subprocess.run(
            [sys.executable, full_path],
            capture_output=True,
            text=True,
            env=env
        )
        
        duration = (datetime.now() - start_time).total_seconds()
        
        if result.returncode == 0:
            logging.info(f"Script {script_name} completato con successo in {duration:.2f} secondi")
            return True
        else:
            logging.error(f"Script {script_name} fallito con errore: {result.stderr}")
            return False
            
    except Exception as e:
        logging.error(f"Errore nell'esecuzione di {script_name}: {str(e)}")
        return False

def check_system_health():
    """Verifica lo stato di salute del sistema"""
    try:
        # Verifica Firebase
        ref = db.reference('health')
        timestamp = datetime.now().isoformat()
        ref.child('coordinator').set({
            'last_check': timestamp,
            'status': 'ok'
        })
        logging.info("Health check completato")
        return True
    except Exception as e:
        logging.error(f"Health check fallito: {str(e)}")
        return False

def main():
    """Funzione principale"""
    start_time = datetime.now()
    logging.info(f"Avvio coordinatore - {start_time.isoformat()}")
    
    try:
        # 1. Inizializza sistema
        initialize_firebase()
        
        # 2. Verifica stato sistema
        system_healthy = check_system_health()
        if not system_healthy:
            logging.error("Sistema non in salute, operazioni limitate")
            # Continua comunque per tentare recupero
        
        # 3. Esegui script di raccolta dati
        data_script = "~/football-predictions/scripts/data_collection/fetch_matches.py"
        fetch_success = run_script(data_script, "fetch_matches")
        
        if not fetch_success:
            logging.warning("Raccolta dati fallita, le operazioni successive potrebbero essere compromesse")
            
        # Pausa per evitare sovraccarichi
        time.sleep(5)
        
        # 4. Esegui script di statistiche squadre (se esiste)
        stats_script = "~/football-predictions/scripts/data_collection/fetch_team_stats.py"
        if os.path.exists(os.path.expanduser(stats_script)):
            run_script(stats_script, "fetch_team_stats")
            time.sleep(5)
        
        # 5. Esegui script di statistiche h2h (se esiste)
        h2h_script = "~/football-predictions/scripts/data_collection/fetch_h2h_stats.py"
        if os.path.exists(os.path.expanduser(h2h_script)):
            run_script(h2h_script, "fetch_h2h_stats")
            time.sleep(5)
        
        # 6. Esegui generazione articoli (se esiste)
        articles_script = "~/football-predictions/scripts/content_generation/generate_articles.py"
        if os.path.exists(os.path.expanduser(articles_script)):
            run_script(articles_script, "generate_articles")
            time.sleep(5)
            
        # 7. Esegui traduzione articoli (se esiste)
        translation_script = "~/football-predictions/scripts/translation/translate_articles.py"
        if os.path.exists(os.path.expanduser(translation_script)):
            run_script(translation_script, "translate_articles")
            time.sleep(5)
            
        # 8. Esegui pubblicazione WordPress (se esiste)
        publishing_script = "~/football-predictions/scripts/publishing/publish_to_wordpress.py"
        if os.path.exists(os.path.expanduser(publishing_script)):
            run_script(publishing_script, "publish_to_wordpress")
        
        # 9. Aggiorna stato salute finale
        ref = db.reference('health/coordinator')
        ref.update({
            'completed_at': datetime.now().isoformat(),
            'status': 'success'
        })
        
        logging.info("Coordinatore completato con successo")
        
    except Exception as e:
        logging.error(f"Errore generale: {str(e)}")
        
        # Aggiorna stato health con errore
        try:
            ref = db.reference('health/coordinator')
            ref.update({
                'completed_at': datetime.now().isoformat(),
                'status': 'error',
                'error_message': str(e)
            })
        except:
            pass
            
        return 1
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    logging.info(f"Esecuzione completata in {duration} secondi")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
