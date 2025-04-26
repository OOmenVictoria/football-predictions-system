#!/usr/bin/env python3
"""
Script per verificare lo stato di salute del sistema
"""
import os
import sys
import requests
import json
import firebase_admin
from firebase_admin import credentials, db
from datetime import datetime, timedelta
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
                raise Exception("FIREBASE_CREDENTIALS not found in environment variables")
        else:
            cred_path = os.path.expanduser('~/football-predictions/creds/firebase-credentials.json')
            cred = credentials.Certificate(cred_path)
        
        firebase_admin.initialize_app(cred, {
            'databaseURL': os.getenv('FIREBASE_DB_URL')
        })
    return True

def check_component_health(component_name, max_hours=24):
    """Verifica salute di un componente"""
    ref = db.reference(f'health/{component_name}')
    component_data = ref.get()
    
    if not component_data:
        return {
            'status': 'unknown',
            'message': f'Component {component_name} not found'
        }
    
    # Verifica timestamp
    last_run = component_data.get('last_run')
    if not last_run:
        return {
            'status': 'error',
            'message': 'No last_run timestamp found'
        }
    
    # Calcola tempo trascorso
    try:
        last_run_time = datetime.fromisoformat(last_run.replace('Z', '+00:00'))
        hours_since_run = (datetime.now() - last_run_time).total_seconds() / 3600
        
        if hours_since_run > max_hours:
            return {
                'status': 'error',
                'message': f'Component not run for {hours_since_run:.1f} hours (max {max_hours})'
            }
    except Exception as e:
        return {
            'status': 'error',
            'message': f'Error parsing timestamp: {str(e)}'
        }
    
    # Controlla stato esplicito
    component_status = component_data.get('status')
    if component_status != 'success':
        return {
            'status': 'error',
            'message': f'Component status is {component_status}'
        }
    
    return {
        'status': 'ok',
        'message': f'Last run {hours_since_run:.1f} hours ago'
    }

def send_notification(message):
    """Invia notifica tramite webhook"""
    webhook_url = os.getenv('NOTIFICATION_WEBHOOK')
    if not webhook_url:
        print("Nessun webhook configurato per notifiche")
        return False
    
    payload = {
        'text': f"🚨 Football Predictions System Alert:\n{message}",
        'username': 'System Monitor'
    }
    
    try:
        response = requests.post(webhook_url, json=payload)
        return response.status_code == 200
    except Exception as e:
        print(f"Errore invio notifica: {str(e)}")
        return False

def main():
    """Funzione principale"""
    print("Esecuzione health check sistema")
    
    # Inizializza Firebase
    initialize_firebase()
    
    # Componenti da verificare
    components = [
        {'name': 'data_collection', 'max_hours': 12},
        {'name': 'content_generation', 'max_hours': 12},
        {'name': 'publishing', 'max_hours': 6}
    ]
    
    issues = []
    
    for component in components:
        result = check_component_health(component['name'], component['max_hours'])
        
        if result['status'] != 'ok':
            issues.append(f"{component['name']}: {result['message']}")
            print(f"⚠️ Problema: {component['name']} - {result['message']}")
        else:
            print(f"✅ {component['name']}: {result['message']}")
    
    # Aggiorna stato generale
    ref = db.reference('health/system')
    ref.update({
        'last_check': datetime.now().isoformat(),
        'status': 'error' if issues else 'healthy',
        'issues': issues
    })
    
    # Invia notifica se ci sono problemi
    if issues:
        message = "System Health Check rilevato problemi:\n- " + "\n- ".join(issues)
        send_notification(message)
    
    return 1 if issues else 0

if __name__ == "__main__":
    sys.exit(main())
