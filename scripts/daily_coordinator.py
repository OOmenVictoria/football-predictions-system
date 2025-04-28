#!/usr/bin/env python3
"""
Script coordinatore giornaliero - Eseguito una volta al giorno
Coordina i vari componenti del sistema e supporta sia PythonAnywhere che GitHub Actions
"""
import os
import sys
import logging
import subprocess
import time
from datetime import datetime
import firebase_admin
from firebase_admin import credentials, db
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # Se dotenv non è disponibile, procedi comunque
    pass

# Configurazione logging
# Configura il logging in base all'ambiente
if os.environ.get('GITHUB_ACTIONS'):
    # Per GitHub Actions, usa il logging su console
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )
else:
    # Per PythonAnywhere, usa il logging su file
    log_dir = os.path.expanduser('~/football-predictions/logs')
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"coordinator_{datetime.now().strftime('%Y%m%d')}.log")
    
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

logger = logging.getLogger("coordinator")

def initialize_firebase():
    """Inizializza connessione Firebase"""
    try:
        firebase_admin.get_app()
    except ValueError:
        # GitHub Actions o ambiente CI
        if os.environ.get('GITHUB_ACTIONS') or os.environ.get('CI'):
            firebase_credentials = os.environ.get('FIREBASE_CREDENTIALS')
            if firebase_credentials:
                with open('firebase-credentials.json', 'w') as f:
                    f.write(firebase_credentials)
                cred = credentials.Certificate('firebase-credentials.json')
            else:
                raise Exception("FIREBASE_CREDENTIALS not found")
        else:
            # Uso locale o PythonAnywhere
            cred_path = os.path.expanduser('~/football-predictions/creds/firebase-credentials.json')
            cred = credentials.Certificate(cred_path)
        
        firebase_admin.initialize_app(cred, {
            'databaseURL': os.getenv('FIREBASE_DB_URL')
        })
    return True

def run_script(script_path, script_name):
    """Esegue uno script Python e gestisce l'output"""
    # Determina il percorso completo in base all'ambiente
    if os.environ.get('GITHUB_ACTIONS'):
        # Per GitHub Actions, usa percorsi relativi
        if script_path.startswith('~/football-predictions/'):
            script_path = script_path.replace('~/football-predictions/', 'scripts/')
        full_path = script_path
    else:
        # Per PythonAnywhere, usa percorsi assoluti
        full_path = os.path.expanduser(script_path)
    
    logger.info(f"Avvio script: {script_name} - {full_path}")
    
    try:
        env = os.environ.copy()
        
        # Configura PYTHONPATH in base all'ambiente
        if not os.environ.get('GITHUB_ACTIONS'):
            env["PYTHONPATH"] = os.path.expanduser("~/football-predictions")
        
        # Verifica esistenza script
        if not os.path.exists(full_path):
            logger.warning(f"Script {full_path} non trovato")
            return False
        
        # Rendi eseguibile se possibile
        try:
            os.chmod(full_path, 0o755)
        except:
            pass
        
        start_time = datetime.now()
        result = subprocess.run(
            [sys.executable, full_path],
            capture_output=True,
            text=True,
            env=env
        )
        
        duration = (datetime.now() - start_time).total_seconds()
        
        # Registra output indipendentemente dal successo/fallimento
        if result.stdout:
            logger.info(f"{script_name} stdout: {result.stdout[:500]}...")
        
        if result.returncode == 0:
            logger.info(f"Script {script_name} completato con successo in {duration:.2f} secondi")
            return True
        else:
            logger.error(f"Script {script_name} fallito con errore: {result.stderr}")
            return False
            
    except Exception as e:
        logger.error(f"Errore nell'esecuzione di {script_name}: {str(e)}")
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
        logger.info("Health check completato")
        return True
    except Exception as e:
        logger.error(f"Health check fallito: {str(e)}")
        return False

def update_component_status(component, status='success', details=None):
    """Aggiorna lo stato di un componente in Firebase"""
    try:
        ref = db.reference(f'health/{component}')
        update_data = {
            'last_run': datetime.now().isoformat(),
            'status': status
        }
        
        if details:
            update_data['details'] = details
            
        ref.update(update_data)
        logger.info(f"Aggiornato stato {component}: {status}")
    except Exception as e:
        logger.error(f"Errore aggiornamento stato {component}: {str(e)}")

def main():
    """Funzione principale"""
    start_time = datetime.now()
    logger.info(f"Avvio coordinatore - {start_time.isoformat()}")
    
    try:
        # 1. Inizializza sistema
        initialize_firebase()
        
        # 2. Verifica stato sistema
        system_healthy = check_system_health()
        if not system_healthy:
            logger.error("Sistema non in salute, operazioni limitate")
            # Continua comunque per tentare recupero
        
        # Definisci script da eseguire con i percorsi corretti
        scripts = [
            {
                'path': "~/football-predictions/scripts/data_collection/fetch_matches.py",
                'name': "fetch_matches",
                'component': "data_collection",
                'critical': True
            },
            {
                'path': "~/football-predictions/scripts/data_collection/fetch_team_stats.py",
                'name': "fetch_team_stats",
                'component': "team_stats",
                'critical': False
            },
            {
                'path': "~/football-predictions/scripts/data_collection/fetch_h2h_stats.py",
                'name': "fetch_h2h_stats",
                'component': "h2h_stats",
                'critical': False
            },
            {
                'path': "~/football-predictions/scripts/content_generation/generate_articles.py",
                'name': "generate_articles",
                'component': "content_generation",
                'critical': True
            },
            {
                'path': "~/football-predictions/scripts/translation/translator.py",  # Percorso aggiornato
                'name': "translator",
                'component': "translation",
                'critical': True
            },
            {
                'path': "~/football-predictions/scripts/publishing/publish_to_wordpress.py",
                'name': "publish_to_wordpress",
                'component': "publishing",
                'critical': True
            }
        ]
        
        # Esegui ogni script
        results = {}
        
        for script in scripts:
            path = script['path']
            name = script['name']
            component = script['component']
            critical = script['critical']
            
            # Esegui lo script se esiste
            if os.environ.get('GITHUB_ACTIONS') or os.path.exists(os.path.expanduser(path)):
                success = run_script(path, name)
                results[component] = success
                
                # Aggiorna lo stato del componente
                if success:
                    update_component_status(component, 'success')
                else:
                    update_component_status(component, 'error', f"Script {name} failed")
                    
                    # Se un componente critico fallisce, registra avviso
                    if critical:
                        logger.warning(f"Componente critico {component} fallito, operazioni successive potrebbero essere compromesse")
                
                # Pausa tra script
                time.sleep(5)
            else:
                logger.info(f"Script {path} non trovato, saltando")
        
        # Calcola riepilogo
        components_ran = len(results)
        components_succeeded = sum(1 for success in results.values() if success)
        components_failed = sum(1 for success in results.values() if not success)
        
        # Aggiorna stato finale coordinatore
        if components_failed == 0:
            status = 'success'
            details = f"Eseguiti {components_ran} componenti, tutti con successo"
        elif components_succeeded > 0:
            status = 'warning'
            details = f"Eseguiti {components_ran} componenti, {components_succeeded} con successo, {components_failed} falliti"
        else:
            status = 'error'
            details = f"Eseguiti {components_ran} componenti, tutti falliti"
        
        ref = db.reference('health/coordinator')
        ref.update({
            'completed_at': datetime.now().isoformat(),
            'status': status,
            'details': details
        })
        
        logger.info(f"Coordinatore completato: {details}")
        
    except Exception as e:
        logger.error(f"Errore generale: {str(e)}")
        
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
    logger.info(f"Esecuzione completata in {duration:.2f} secondi")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
