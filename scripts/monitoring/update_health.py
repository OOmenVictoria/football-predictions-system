#!/usr/bin/env python3
"""
Script per aggiornare lo stato di salute dei componenti del sistema
"""
import os
import sys
import argparse
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
        # Se GITHUB_ACTIONS è impostato, usare segreti come variabili d'ambiente
        if os.environ.get('GITHUB_ACTIONS'):
            firebase_credentials = os.environ.get('FIREBASE_CREDENTIALS')
            if firebase_credentials:
                with open('firebase-credentials.json', 'w') as f:
                    f.write(firebase_credentials)
                cred = credentials.Certificate('firebase-credentials.json')
            else:
                cred = None  # Credenziali non disponibili
        else:
            # Uso locale
            cred_path = os.path.expanduser('~/football-predictions/creds/firebase-credentials.json')
            cred = credentials.Certificate(cred_path)
        
        firebase_admin.initialize_app(cred, {
            'databaseURL': os.getenv('FIREBASE_DB_URL')
        })
    return True

def update_component_status(component, status="success", details=None):
    """Aggiorna lo stato di un componente"""
    ref = db.reference(f'health/{component}')
    
    update_data = {
        'last_run': datetime.now().isoformat(),
        'status': status
    }
    
    if details:
        update_data.update(details)
    
    ref.update(update_data)
    print(f"Stato {component} aggiornato: {status}")
    return True

def main():
    """Funzione principale"""
    parser = argparse.ArgumentParser(description='Aggiorna stato componente')
    parser.add_argument('--component', required=True, help='Nome componente')
    parser.add_argument('--status', default='success', help='Stato (success/error)')
    args = parser.parse_args()
    
    # Inizializza Firebase
    initialize_firebase()
    
    # Aggiorna stato
    update_component_status(args.component, args.status)
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
