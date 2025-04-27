#!/usr/bin/env python3
"""
Test semplificato per translator.py
"""
import os
import sys
import firebase_admin
from firebase_admin import credentials, db
from datetime import datetime

def main():
    print("Test script per traduzione articoli")
    
    # Verifica variabili d'ambiente
    print("Verifica variabili d'ambiente:")
    print(f"FIREBASE_DB_URL: {'✓ presente' if os.getenv('FIREBASE_DB_URL') else '✗ mancante'}")
    print(f"FIREBASE_CREDENTIALS: {'✓ presente (primi 10 caratteri): ' + os.getenv('FIREBASE_CREDENTIALS')[:10] + '...' if os.getenv('FIREBASE_CREDENTIALS') else '✗ mancante'}")
    
    # Prova a inizializzare Firebase
    print("\nInitializing Firebase...")
    try:
        if os.environ.get('FIREBASE_CREDENTIALS'):
            print("Scriviamo le credenziali in un file temporaneo...")
            with open('firebase-credentials.json', 'w') as f:
                f.write(os.environ.get('FIREBASE_CREDENTIALS'))
            
            print("Credenziali scritte, inizializzazione Firebase...")
            cred = credentials.Certificate('firebase-credentials.json')
            firebase_admin.initialize_app(cred, {
                'databaseURL': os.getenv('FIREBASE_DB_URL')
            })
            print("Firebase inizializzato con successo!")
            
            # Aggiorna lo stato
            ref = db.reference('health/translation')
            ref.update({
                'last_run': datetime.now().isoformat(),
                'status': 'success'
            })
            print("Stato aggiornato in Firebase")
        else:
            print("FIREBASE_CREDENTIALS non trovato nelle variabili d'ambiente")
            return 1
    except Exception as e:
        print(f"Errore durante l'inizializzazione di Firebase: {str(e)}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
