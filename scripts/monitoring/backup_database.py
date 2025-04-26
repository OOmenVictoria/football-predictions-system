#!/usr/bin/env python3
"""
Script per il backup del database Firebase
"""
import os
import sys
import json
import firebase_admin
from firebase_admin import credentials, db
from datetime import datetime
from dotenv import load_dotenv

# Carica variabili d'ambiente
load_dotenv()

def initialize_firebase():
    """Inizializza connessione Firebase"""
    try:
        firebase_admin.get_app()
    except ValueError:
        # Come sopra, gestisci diversamente per GitHub Actions
        if os.environ.get('GITHUB_ACTIONS'):
            firebase_credentials = os.environ.get('FIREBASE_CREDENTIALS')
            if firebase_credentials:
                with open('firebase-credentials.json', 'w') as f:
                    f.write(firebase_credentials)
                cred = credentials.Certificate('firebase-credentials.json')
            else:
                cred = None
        else:
            cred_path = os.path.expanduser('~/football-predictions/creds/firebase-credentials.json')
            cred = credentials.Certificate(cred_path)
        
        firebase_admin.initialize_app(cred, {
            'databaseURL': os.getenv('FIREBASE_DB_URL')
        })
    return True

def backup_database():
    """Effettua backup del database"""
    print("Avvio backup database...")
    
    # Ottieni tutto il database
    ref = db.reference('/')
    data = ref.get()
    
    if not data:
        print("Nessun dato nel database")
        return False
    
    # Crea directory backups
    backup_dir = "backups"
    if os.environ.get('GITHUB_ACTIONS'):
        # Su GitHub Actions, salva nella directory artifacts
        backup_dir = os.path.join(os.environ.get('GITHUB_WORKSPACE', '.'), 'backups')
    else:
        # Uso locale
        backup_dir = os.path.expanduser('~/football-predictions/backups')
    
    os.makedirs(backup_dir, exist_ok=True)
    
    # Nome file con timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_file = os.path.join(backup_dir, f"db_backup_{timestamp}.json")
    
    # Salva dati in JSON
    with open(backup_file, 'w') as f:
        json.dump(data, f, indent=2)
    
    print(f"Backup database completato: {backup_file}")
    
    # Aggiorna stato
    ref = db.reference('health/backup')
    ref.update({
        'last_backup': datetime.now().isoformat(),
        'backup_file': backup_file
    })
    
    return True

def main():
    """Funzione principale"""
    initialize_firebase()
    success = backup_database()
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())
