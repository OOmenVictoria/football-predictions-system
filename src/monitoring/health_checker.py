""" Sistema di monitoraggio della salute del sistema di pronostici calcistici.
Controlla lo stato di vari componenti del sistema, verifica il funzionamento
delle API esterne, e genera report sullo stato di salute generale.
"""

import os
import time
import json
import logging
import requests
import psutil
import firebase_admin
from firebase_admin import db
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple, Union, Set

# Importa il logger personalizzato
from src.monitoring.logger import get_logger
from src.utils.database import FirebaseManager
from src.utils.http import make_request

logger = get_logger('health_checker')

class HealthChecker:
    """Monitora lo stato di salute del sistema di pronostici calcistici."""
    
    def __init__(self, firebase_manager: Optional[FirebaseManager] = None):
        """
        Inizializza il sistema di monitoraggio.
        
        Args:
            firebase_manager: Gestore Firebase per la persistenza dei dati di salute
        """
        self.firebase_manager = firebase_manager or FirebaseManager()
        self.health_ref = self.firebase_manager.get_reference('monitoring/health')
        self.last_check_time = None
        self.external_services = {
            'api_football': 'https://api.football-data.org/v4/competitions',
            'fbref': 'https://fbref.com/en/',
            'understat': 'https://understat.com/',
            'sofascore': 'https://www.sofascore.com/',
            'wordpress': os.environ.get('WORDPRESS_URL', 'https://example.com/wp-json')
        }
    
    def check_system_resources(self) -> Dict[str, Any]:
        """
        Controlla le risorse di sistema (CPU, memoria, disco).
        
        Returns:
            Dizionario con lo stato delle risorse di sistema
        """
        try:
            # Utilizzo CPU
            cpu_percent = psutil.cpu_percent(interval=1)
            
            # Utilizzo memoria
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            memory_available_mb = memory.available / 1024 / 1024
            
            # Utilizzo disco
            disk = psutil.disk_usage('/')
            disk_percent = disk.percent
            disk_free_gb = disk.free / 1024 / 1024 / 1024
            
            return {
                'cpu': {
                    'percent': cpu_percent,
                    'status': 'ok' if cpu_percent < 80 else 'warning' if cpu_percent < 90 else 'critical'
                },
                'memory': {
                    'percent': memory_percent,
                    'available_mb': round(memory_available_mb, 2),
                    'status': 'ok' if memory_percent < 80 else 'warning' if memory_percent < 90 else 'critical'
                },
                'disk': {
                    'percent': disk_percent,
                    'free_gb': round(disk_free_gb, 2),
                    'status': 'ok' if disk_percent < 80 else 'warning' if disk_percent < 90 else 'critical'
                }
            }
        except Exception as e:
            logger.error(f"Errore nel controllo delle risorse di sistema: {e}")
            return {
                'cpu': {'status': 'error', 'message': str(e)},
                'memory': {'status': 'error', 'message': str(e)},
                'disk': {'status': 'error', 'message': str(e)}
            }
    
    def check_external_services(self) -> Dict[str, Any]:
        """
        Verifica la disponibilità dei servizi esterni.
        
        Returns:
            Dizionario con lo stato dei servizi esterni
        """
        results = {}
        
        for service_name, url in self.external_services.items():
            try:
                start_time = time.time()
                response = make_request(url, method='GET', timeout=10)
                elapsed_time = time.time() - start_time
                
                status_code = response.status_code if response else 0
                
                if status_code >= 200 and status_code < 300:
                    status = 'ok'
                elif status_code >= 300 and status_code < 400:
                    status = 'redirect'
                elif status_code >= 400 and status_code < 500:
                    status = 'client_error'
                elif status_code >= 500:
                    status = 'server_error'
                else:
                    status = 'unknown'
                
                results[service_name] = {
                    'status': status,
                    'response_time_ms': round(elapsed_time * 1000, 2),
                    'status_code': status_code,
                    'last_checked': datetime.now().isoformat()
                }
            except requests.RequestException as e:
                logger.warning(f"Errore nella verifica del servizio {service_name}: {e}")
                results[service_name] = {
                    'status': 'error',
                    'message': str(e),
                    'last_checked': datetime.now().isoformat()
                }
            except Exception as e:
                logger.error(f"Errore imprevisto nella verifica del servizio {service_name}: {e}")
                results[service_name] = {
                    'status': 'error',
                    'message': str(e),
                    'last_checked': datetime.now().isoformat()
                }
        
        return results
    
    def check_data_freshness(self) -> Dict[str, Any]:
        """
        Verifica la freschezza dei dati nel sistema.
        
        Returns:
            Dizionario con lo stato di freschezza dei dati
        """
        try:
            # Controlla timestamp ultimi dati raccolti
            data_refs = {
                'matches': 'data/matches/last_updated',
                'team_stats': 'data/team_stats/last_updated',
                'player_stats': 'data/player_stats/last_updated',
                'predictions': 'predictions/last_updated',
                'articles': 'content/articles/last_updated'
            }
            
            results = {}
            now = datetime.now()
            
            for data_type, ref_path in data_refs.items():
                try:
                    timestamp = self.firebase_manager.get_data(ref_path)
                    if timestamp:
                        last_updated = datetime.fromisoformat(timestamp)
                        age_hours = (now - last_updated).total_seconds() / 3600
                        
                        # Soglie di freschezza (personalizzabili)
                        if data_type == 'matches':
                            status = 'ok' if age_hours < 24 else 'warning' if age_hours < 48 else 'critical'
                        elif data_type == 'team_stats':
                            status = 'ok' if age_hours < 48 else 'warning' if age_hours < 96 else 'critical'
                        elif data_type == 'player_stats':
                            status = 'ok' if age_hours < 72 else 'warning' if age_hours < 120 else 'critical'
                        elif data_type == 'predictions':
                            status = 'ok' if age_hours < 24 else 'warning' if age_hours < 48 else 'critical'
                        elif data_type == 'articles':
                            status = 'ok' if age_hours < 12 else 'warning' if age_hours < 24 else 'critical'
                        else:
                            status = 'ok' if age_hours < 48 else 'warning' if age_hours < 96 else 'critical'
                        
                        results[data_type] = {
                            'last_updated': timestamp,
                            'age_hours': round(age_hours, 2),
                            'status': status
                        }
                    else:
                        results[data_type] = {
                            'status': 'unknown',
                            'message': 'Timestamp non trovato'
                        }
                except Exception as e:
                    results[data_type] = {
                        'status': 'error',
                        'message': str(e)
                    }
            
            return results
        except Exception as e:
            logger.error(f"Errore nel controllo della freschezza dei dati: {e}")
            return {'status': 'error', 'message': str(e)}
    
    def check_recent_errors(self, hours: int = 24) -> Dict[str, Any]:
        """
        Verifica errori recenti nei log.
        
        Args:
            hours: Numero di ore indietro da controllare
            
        Returns:
            Riepilogo degli errori recenti
        """
        try:
            # Ottieni errori da Firebase
            errors_ref = self.firebase_manager.get_reference('logs/errors')
            since = datetime.now() - timedelta(hours=hours)
            since_str = since.isoformat()
            
            # Cerca errori più recenti della soglia
            recent_errors = errors_ref.order_by_child('timestamp').start_at(since_str).get() or {}
            
            # Raggruppa errori per tipo
            error_types = {}
            for error_id, error_data in recent_errors.items():
                message = error_data.get('message', 'Unknown error')
                source = error_data.get('source', 'unknown')
                timestamp = error_data.get('timestamp', '')
                
                # Crea chiave per il tipo di errore
                error_key = f"{source}: {message[:50]}..." if len(message) > 50 else f"{source}: {message}"
                
                if error_key not in error_types:
                    error_types[error_key] = {
                        'count': 0,
                        'latest': '',
                        'sources': set(),
                        'example_id': error_id
                    }
                
                error_types[error_key]['count'] += 1
                error_types[error_key]['sources'].add(source)
                
                # Aggiorna latest se questo errore è più recente
                if timestamp > error_types[error_key]['latest']:
                    error_types[error_key]['latest'] = timestamp
            
            # Converti per JSON (set non sono serializzabili)
            for error_key in error_types:
                error_types[error_key]['sources'] = list(error_types[error_key]['sources'])
            
            return {
                'total_count': len(recent_errors),
                'unique_errors': len(error_types),
                'by_type': error_types,
                'period_hours': hours,
                'status': 'ok' if len(recent_errors) == 0 else 'warning' if len(recent_errors) < 10 else 'critical'
            }
        except Exception as e:
            logger.error(f"Errore nel controllo degli errori recenti: {e}")
            return {'status': 'error', 'message': str(e)}
    
    def check_workflow_executions(self) -> Dict[str, Any]:
        """
        Verifica lo stato delle esecuzioni di workflow.
        
        Returns:
            Stato delle esecuzioni recenti di workflow
        """
        try:
            # Recupera i dati sulle esecuzioni recenti
            executions_ref = self.firebase_manager.get_reference('monitoring/executions')
            recent_executions = executions_ref.order_by_child('timestamp').limit_to_last(100).get() or {}
            
            workflows = {}
            for exec_id, exec_data in recent_executions.items():
                workflow_name = exec_data.get('workflow', 'unknown')
                status = exec_data.get('status', 'unknown')
                timestamp = exec_data.get('timestamp', '')
                
                if workflow_name not in workflows:
                    workflows[workflow_name] = {
                        'total_runs': 0,
                        'successful_runs': 0,
                        'failed_runs': 0,
                        'latest_run': '',
                        'latest_status': '',
                        'average_duration': 0,
                        'total_duration': 0
                    }
                
                workflows[workflow_name]['total_runs'] += 1
                if status == 'success':
                    workflows[workflow_name]['successful_runs'] += 1
                elif status == 'failure':
                    workflows[workflow_name]['failed_runs'] += 1
                
                # Aggiorna latest se questa esecuzione è più recente
                if timestamp > workflows[workflow_name]['latest_run']:
                    workflows[workflow_name]['latest_run'] = timestamp
                    workflows[workflow_name]['latest_status'] = status
                
                # Aggiorna durata media
                if 'duration' in exec_data:
                    workflows[workflow_name]['total_duration'] += exec_data['duration']
            
            # Calcola durata media
            for workflow_name in workflows:
                if workflows[workflow_name]['total_runs'] > 0:
                    workflows[workflow_name]['average_duration'] = round(
                        workflows[workflow_name]['total_duration'] / workflows[workflow_name]['total_runs'], 2
                    )
                
                # Imposta stato
                success_rate = workflows[workflow_name]['successful_runs'] / workflows[workflow_name]['total_runs'] if workflows[workflow_name]['total_runs'] > 0 else 0
                workflows[workflow_name]['success_rate'] = round(success_rate * 100, 2)
                
                if workflows[workflow_name]['latest_status'] == 'failure':
                    workflows[workflow_name]['status'] = 'critical'
                elif success_rate < 0.7:
                    workflows[workflow_name]['status'] = 'critical'
                elif success_rate < 0.9:
                    workflows[workflow_name]['status'] = 'warning'
                else:
                    workflows[workflow_name]['status'] = 'ok'
            
            return {
                'workflows': workflows,
                'status': 'ok' if all(w['status'] == 'ok' for w in workflows.values()) else 'warning' if any(w['status'] == 'warning' for w in workflows.values()) else 'critical'
            }
        except Exception as e:
            logger.error(f"Errore nel controllo delle esecuzioni di workflow: {e}")
            return {'status': 'error', 'message': str(e)}
    
    def run_comprehensive_check(self) -> Dict[str, Any]:
        """
        Esegue un controllo completo dello stato del sistema.
        
        Returns:
            Report completo sullo stato del sistema
        """
        start_time = time.time()
        
        # Esegui tutti i controlli
        system_health = self.check_system_resources()
        services_health = self.check_external_services()
        data_health = self.check_data_freshness()
        errors_health = self.check_recent_errors()
        workflow_health = self.check_workflow_executions()
        
        # Calcola stato generale
        status_priority = {
            'error': 3,
            'critical': 2,
            'warning': 1,
            'ok': 0,
            'unknown': 0
        }
        
        component_statuses = [
            system_health.get('cpu', {}).get('status', 'unknown'),
            system_health.get('memory', {}).get('status', 'unknown'),
            system_health.get('disk', {}).get('status', 'unknown'),
            errors_health.get('status', 'unknown'),
            workflow_health.get('status', 'unknown')
        ]
        
        # Aggiungi stato dei servizi esterni
        for service_status in services_health.values():
            component_statuses.append(service_status.get('status', 'unknown'))
        
        # Aggiungi stato freschezza dati
        for data_status in data_health.values():
            if isinstance(data_status, dict):
                component_statuses.append(data_status.get('status', 'unknown'))
        
        # Determina lo stato peggiore
        worst_status = 'ok'
        for status in component_statuses:
            if status in status_priority and status_priority.get(status, 0) > status_priority.get(worst_status, 0):
                worst_status = status
        
        # Prepara report completo
        comprehensive_report = {
            'status': worst_status,
            'timestamp': datetime.now().isoformat(),
            'check_duration_ms': round((time.time() - start_time) * 1000, 2),
            'components': {
                'system': system_health,
                'external_services': services_health,
                'data_freshness': data_health,
                'recent_errors': errors_health,
                'workflows': workflow_health
            }
        }
        
        # Salva report su Firebase
        try:
            self.health_ref.child('latest_report').set(comprehensive_report)
            self.health_ref.child('reports').push(comprehensive_report)
            
            # Aggiorna timestamp ultimo controllo
            self.last_check_time = datetime.now()
            self.health_ref.child('last_check').set(self.last_check_time.isoformat())
        except Exception as e:
            logger.error(f"Errore nel salvare il report di salute su Firebase: {e}")
        
        return comprehensive_report
    
    def get_latest_report(self) -> Dict[str, Any]:
        """
        Ottiene l'ultimo report sullo stato del sistema.
        
        Returns:
            Ultimo report disponibile
        """
        try:
            latest_report = self.health_ref.child('latest_report').get()
            return latest_report or {'status': 'unknown', 'message': 'Nessun report disponibile'}
        except Exception as e:
            logger.error(f"Errore nel recuperare l'ultimo report di salute: {e}")
            return {'status': 'error', 'message': str(e)}
    
    def schedule_checks(self, interval_minutes: int = 30):
        """
        Schedula controlli periodici (da chiamare da un processo separato).
        
        Args:
            interval_minutes: Intervallo in minuti tra i controlli
        """
        logger.info(f"Avvio controlli di salute ogni {interval_minutes} minuti")
        
        try:
            while True:
                logger.info("Esecuzione controllo di salute del sistema")
                self.run_comprehensive_check()
                time.sleep(interval_minutes * 60)
        except KeyboardInterrupt:
            logger.info("Controlli di salute terminati dall'utente")
        except Exception as e:
            logger.error(f"Errore nei controlli di salute schedulati: {e}")


# Funzione di utilità per verificare rapidamente lo stato del sistema
def check_system_health(firebase_manager=None) -> Dict[str, Any]:
    """
    Verifica rapida dello stato del sistema.
    
    Args:
        firebase_manager: Istanza del gestore Firebase (opzionale)
        
    Returns:
        Report sullo stato del sistema
    """
    checker = HealthChecker(firebase_manager)
    return checker.run_comprehensive_check()

# Aggiungiamo run_health_check come alias di check_system_health per compatibilità
def run_health_check(firebase_manager=None) -> Dict[str, Any]:
    """
    Alias di check_system_health per compatibilità.
    
    Args:
        firebase_manager: Istanza del gestore Firebase (opzionale)
        
    Returns:
        Report sullo stato del sistema
    """
    return check_system_health(firebase_manager)
