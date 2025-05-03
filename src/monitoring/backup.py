""" Sistema di backup per il database Firebase.
Fornisce funzionalità per eseguire backup periodici del database
e per ripristinare da backup in caso di necessità.
"""

import os
import json
import time
import logging
import shutil
import firebase_admin
from firebase_admin import db
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple, Union

# Importa il logger personalizzato
from src.monitoring.logger import get_logger
from src.utils.database import FirebaseManager

# Configurazione logger
logger = get_logger('backup')

class BackupManager:
    """Gestisce i backup del database Firebase."""
    
    def __init__(self, firebase_manager: Optional[FirebaseManager] = None, 
                 backup_dir: str = None):
        """
        Inizializza il gestore dei backup.
        
        Args:
            firebase_manager: Gestore Firebase per l'accesso al database
            backup_dir: Directory in cui salvare i backup (default: ./backup)
        """
        self.firebase_manager = firebase_manager or FirebaseManager()
        
        # Configura directory di backup
        if backup_dir is None:
            # Directory di default: [repo_root]/backup
            repo_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
            self.backup_dir = os.path.join(repo_root, 'backup')
        else:
            self.backup_dir = backup_dir
        
        # Crea directory se non esiste
        os.makedirs(self.backup_dir, exist_ok=True)
        
        # Crea subdirectory per tipo di backup
        self.daily_dir = os.path.join(self.backup_dir, 'daily')
        self.weekly_dir = os.path.join(self.backup_dir, 'weekly')
        self.monthly_dir = os.path.join(self.backup_dir, 'monthly')
        self.manual_dir = os.path.join(self.backup_dir, 'manual')
        
        for directory in [self.daily_dir, self.weekly_dir, self.monthly_dir, self.manual_dir]:
            os.makedirs(directory, exist_ok=True)
        
        # Riferimento al nodo di monitoraggio dei backup
        self.backup_ref = self.firebase_manager.get_reference('monitoring/backups')
    
    def _generate_backup_filename(self, backup_type: str) -> str:
        """
        Genera un nome file per il backup.
        
        Args:
            backup_type: Tipo di backup (daily, weekly, monthly, manual)
            
        Returns:
            Nome del file di backup
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return f"firebase_backup_{backup_type}_{timestamp}.json"
    
    def _export_data(self, ref_path: str) -> Dict[str, Any]:
        """
        Esporta dati da un nodo specifico di Firebase.
        
        Args:
            ref_path: Percorso del nodo da esportare
            
        Returns:
            Dati esportati come dizionario
        """
        try:
            data = self.firebase_manager.get_data(ref_path)
            return data
        except Exception as e:
            logger.error(f"Errore nell'esportazione dei dati da {ref_path}: {e}")
            raise
    
    def create_backup(self, backup_type: str = 'manual') -> Tuple[bool, str]:
        """
        Crea un backup completo del database.
        
        Args:
            backup_type: Tipo di backup (daily, weekly, monthly, manual)
            
        Returns:
            Tupla (successo, percorso_file o messaggio_errore)
        """
        start_time = time.time()
        logger.info(f"Avvio backup {backup_type} del database Firebase...")
        
        try:
            # Genera nome file
            filename = self._generate_backup_filename(backup_type)
            
            # Determina directory di destinazione
            if backup_type == 'daily':
                backup_path = os.path.join(self.daily_dir, filename)
            elif backup_type == 'weekly':
                backup_path = os.path.join(self.weekly_dir, filename)
            elif backup_type == 'monthly':
                backup_path = os.path.join(self.monthly_dir, filename)
            else:
                backup_path = os.path.join(self.manual_dir, filename)
            
            # Esporta dati principali
            data = {}
            for main_node in ['data', 'content', 'predictions', 'stats']:
                try:
                    node_data = self._export_data(main_node)
                    if node_data:
                        data[main_node] = node_data
                except Exception as e:
                    logger.warning(f"Impossibile esportare il nodo {main_node}: {e}")
            
            # Esporta configurazioni
            try:
                config_data = self._export_data('config')
                if config_data:
                    data['config'] = config_data
            except Exception as e:
                logger.warning(f"Impossibile esportare le configurazioni: {e}")
            
            # Salva su file
            with open(backup_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            # Calcola dimensione file
            file_size_bytes = os.path.getsize(backup_path)
            file_size_mb = file_size_bytes / (1024 * 1024)
            
            # Calcola durata
            duration_seconds = time.time() - start_time
            
            # Registra metadata del backup
            backup_metadata = {
                'timestamp': datetime.now().isoformat(),
                'type': backup_type,
                'file_path': backup_path,
                'file_size_bytes': file_size_bytes,
                'file_size_mb': round(file_size_mb, 2),
                'duration_seconds': round(duration_seconds, 2),
                'status': 'success'
            }
            
            # Salva metadati su Firebase
            try:
                self.backup_ref.push(backup_metadata)
            except Exception as e:
                logger.warning(f"Impossibile salvare i metadati del backup su Firebase: {e}")
            
            logger.info(f"Backup {backup_type} completato: {backup_path} ({round(file_size_mb, 2)} MB in {round(duration_seconds, 2)} secondi)")
            return True, backup_path
            
        except Exception as e:
            error_message = f"Errore durante il backup {backup_type}: {e}"
            logger.error(error_message)
            
            # Registra il fallimento
            try:
                self.backup_ref.push({
                    'timestamp': datetime.now().isoformat(),
                    'type': backup_type,
                    'status': 'failed',
                    'error': str(e),
                    'duration_seconds': round(time.time() - start_time, 2)
                })
            except Exception:
                pass  # Se anche questo fallisce, passiamo oltre
            
            return False, error_message
    
    def restore_from_backup(self, backup_file: str, target_nodes: List[str] = None, 
                           dry_run: bool = False) -> Tuple[bool, str]:
        """
        Ripristina dati da un backup.
        
        Args:
            backup_file: Percorso al file di backup
            target_nodes: Nodi specifici da ripristinare (default: tutti)
            dry_run: Se True, simula il ripristino senza modificare il database
            
        Returns:
            Tupla (successo, messaggio)
        """
        start_time = time.time()
        logger.info(f"Avvio ripristino da backup: {backup_file}")
        
        if not os.path.exists(backup_file):
            return False, f"File di backup non trovato: {backup_file}"
        
        try:
            # Carica dati dal backup
            with open(backup_file, 'r', encoding='utf-8') as f:
                backup_data = json.load(f)
            
            if not backup_data:
                return False, "Il file di backup è vuoto o invalido"
            
            # Se nessun nodo è specificato, ripristina tutto
            if target_nodes is None:
                target_nodes = list(backup_data.keys())
            
            # Rimuovi nodi non presenti nel backup
            valid_nodes = [node for node in target_nodes if node in backup_data]
            if len(valid_nodes) < len(target_nodes):
                missing = set(target_nodes) - set(valid_nodes)
                logger.warning(f"Nodi non presenti nel backup: {missing}")
            
            if dry_run:
                operations = []
                for node in valid_nodes:
                    node_size = len(json.dumps(backup_data[node]))
                    operations.append(f"Ripristino {node} ({node_size} bytes)")
                
                return True, "Simulazione ripristino completata: " + ", ".join(operations)
            
            # Esegui ripristino effettivo
            success_count = 0
            for node in valid_nodes:
                try:
                    self.firebase_manager.set_data(node, backup_data[node])
                    success_count += 1
                    logger.info(f"Nodo {node} ripristinato con successo")
                except Exception as e:
                    logger.error(f"Errore nel ripristino del nodo {node}: {e}")
            
            duration_seconds = time.time() - start_time
            
            # Registra operazione di ripristino
            restore_metadata = {
                'timestamp': datetime.now().isoformat(),
                'operation': 'restore',
                'backup_file': backup_file,
                'target_nodes': target_nodes,
                'successful_nodes': success_count,
                'total_nodes': len(valid_nodes),
                'duration_seconds': round(duration_seconds, 2),
                'status': 'success' if success_count == len(valid_nodes) else 'partial'
            }
            
            try:
                self.firebase_manager.get_reference('monitoring/restores').push(restore_metadata)
            except Exception as e:
                logger.warning(f"Impossibile salvare i metadati del ripristino: {e}")
            
            result_message = f"Ripristino completato: {success_count}/{len(valid_nodes)} nodi ripristinati in {round(duration_seconds, 2)} secondi"
            logger.info(result_message)
            
            return success_count > 0, result_message
            
        except Exception as e:
            error_message = f"Errore durante il ripristino: {e}"
            logger.error(error_message)
            
            # Registra il fallimento
            try:
                self.firebase_manager.get_reference('monitoring/restores').push({
                    'timestamp': datetime.now().isoformat(),
                    'operation': 'restore',
                    'backup_file': backup_file,
                    'status': 'failed',
                    'error': str(e),
                    'duration_seconds': round(time.time() - start_time, 2)
                })
            except Exception:
                pass
            
            return False, error_message
    
    def cleanup_old_backups(self, retain_daily: int = 7, retain_weekly: int = 4, 
                          retain_monthly: int = 12) -> Dict[str, Any]:
        """
        Elimina backup vecchi secondo la policy di retention.
        
        Args:
            retain_daily: Numero di backup giornalieri da conservare
            retain_weekly: Numero di backup settimanali da conservare
            retain_monthly: Numero di backup mensili da conservare
            
        Returns:
            Statistica delle operazioni eseguite
        """
        logger.info("Avvio pulizia backup vecchi...")
        stats = {'deleted': 0, 'retained': 0, 'errors': 0}
        
        try:
            # Funzione per eliminare i backup più vecchi in una directory
            def cleanup_directory(directory, retain_count):
                if not os.path.exists(directory):
                    return 0
                
                # Lista tutti i file di backup
                backup_files = []
                for filename in os.listdir(directory):
                    file_path = os.path.join(directory, filename)
                    if os.path.isfile(file_path) and filename.startswith('firebase_backup_'):
                        file_stats = os.stat(file_path)
                        backup_files.append((file_path, file_stats.st_mtime))
                
                # Ordina per data (più recenti prima)
                backup_files.sort(key=lambda x: x[1], reverse=True)
                
                # Mantieni solo i più recenti
                deleted_count = 0
                for i, (file_path, _) in enumerate(backup_files):
                    if i >= retain_count:
                        try:
                            os.remove(file_path)
                            deleted_count += 1
                            logger.debug(f"Eliminato backup vecchio: {file_path}")
                        except Exception as e:
                            logger.error(f"Errore nell'eliminazione del backup {file_path}: {e}")
                            stats['errors'] += 1
                
                return deleted_count
            
            # Esegui pulizia per ogni tipo di backup
            stats['deleted'] += cleanup_directory(self.daily_dir, retain_daily)
            stats['deleted'] += cleanup_directory(self.weekly_dir, retain_weekly)
            stats['deleted'] += cleanup_directory(self.monthly_dir, retain_monthly)
            
            # Conta quanti backup sono stati mantenuti
            stats['retained'] = (
                min(retain_daily, len(os.listdir(self.daily_dir))) +
                min(retain_weekly, len(os.listdir(self.weekly_dir))) +
                min(retain_monthly, len(os.listdir(self.monthly_dir)))
            )
            
            logger.info(f"Pulizia backup completata: {stats['deleted']} eliminati, {stats['retained']} mantenuti, {stats['errors']} errori")
            
            # Registra operazione di pulizia
            try:
                self.firebase_manager.get_reference('monitoring/backup_cleanup').push({
                    'timestamp': datetime.now().isoformat(),
                    'deleted': stats['deleted'],
                    'retained': stats['retained'],
                    'errors': stats['errors'],
                    'policy': {
                        'daily': retain_daily,
                        'weekly': retain_weekly,
                        'monthly': retain_monthly
                    }
                })
            except Exception as e:
                logger.warning(f"Impossibile salvare i metadati della pulizia: {e}")
            
            return stats
            
        except Exception as e:
            logger.error(f"Errore durante la pulizia dei backup: {e}")
            stats['errors'] += 1
            return stats
    
    def list_available_backups(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Elenca tutti i backup disponibili.
        
        Returns:
            Dizionario con backup disponibili per tipo
        """
        result = {
            'daily': [],
            'weekly': [],
            'monthly': [],
            'manual': []
        }
        
        try:
            # Funzione per scansionare una directory
            def scan_directory(directory, backup_type):
                backups = []
                if not os.path.exists(directory):
                    return backups
                
                for filename in os.listdir(directory):
                    file_path = os.path.join(directory, filename)
                    if os.path.isfile(file_path) and filename.startswith('firebase_backup_'):
                        file_stats = os.stat(file_path)
                        file_size_bytes = file_stats.st_size
                        file_size_mb = file_size_bytes / (1024 * 1024)
                        
                        # Estrai timestamp dal nome file
                        try:
                            # Formato atteso: firebase_backup_TYPE_YYYYMMDD_HHMMSS.json
                            date_part = filename.split('_')[3]
                            time_part = filename.split('_')[4].split('.')[0]
                            timestamp = f"{date_part[0:4]}-{date_part[4:6]}-{date_part[6:8]} {time_part[0:2]}:{time_part[2:4]}:{time_part[4:6]}"
                        except:
                            # Fallback: usa data di modifica
                            timestamp = datetime.fromtimestamp(file_stats.st_mtime).strftime('%Y-%m-%d %H:%M:%S')
                        
                        backups.append({
                            'filename': filename,
                            'path': file_path,
                            'size_bytes': file_size_bytes,
                            'size_mb': round(file_size_mb, 2),
                            'timestamp': timestamp,
                            'type': backup_type
                        })
                
                # Ordina per timestamp (più recenti prima)
                backups.sort(key=lambda x: x['timestamp'], reverse=True)
                return backups
            
            # Scansiona tutte le directory
            result['daily'] = scan_directory(self.daily_dir, 'daily')
            result['weekly'] = scan_directory(self.weekly_dir, 'weekly')
            result['monthly'] = scan_directory(self.monthly_dir, 'monthly')
            result['manual'] = scan_directory(self.manual_dir, 'manual')
            
            return result
            
        except Exception as e:
            logger.error(f"Errore nell'elenco dei backup disponibili: {e}")
            return result


# Funzioni di utilità

def create_backup(backup_type: str = 'manual', firebase_manager=None) -> Tuple[bool, str]:
    """
    Crea un backup del database Firebase.
    
    Args:
        backup_type: Tipo di backup (daily, weekly, monthly, manual)
        firebase_manager: Istanza del gestore Firebase (opzionale)
        
    Returns:
        Tupla (successo, percorso_file o messaggio_errore)
    """
    manager = BackupManager(firebase_manager)
    return manager.create_backup(backup_type)

def restore_backup(backup_file: str, target_nodes: List[str] = None, 
                  dry_run: bool = False, firebase_manager=None) -> Tuple[bool, str]:
    """
    Ripristina un backup del database Firebase.
    
    Args:
        backup_file: Percorso al file di backup
        target_nodes: Nodi specifici da ripristinare (default: tutti)
        dry_run: Se True, simula il ripristino senza modificare il database
        firebase_manager: Istanza del gestore Firebase (opzionale)
        
    Returns:
        Tupla (successo, messaggio)
    """
    manager = BackupManager(firebase_manager)
    return manager.restore_from_backup(backup_file, target_nodes, dry_run)

def cleanup_backups(retain_daily: int = 7, retain_weekly: int = 4, 
                   retain_monthly: int = 12, firebase_manager=None) -> Dict[str, Any]:
    """
    Elimina backup vecchi secondo la policy di retention.
    
    Args:
        retain_daily: Numero di backup giornalieri da conservare
        retain_weekly: Numero di backup settimanali da conservare
        retain_monthly: Numero di backup mensili da conservare
        firebase_manager: Istanza del gestore Firebase (opzionale)
        
    Returns:
        Statistica delle operazioni eseguite
    """
    manager = BackupManager(firebase_manager)
    return manager.cleanup_old_backups(retain_daily, retain_weekly, retain_monthly)

# Aggiungiamo cleanup_old_backups come alias di cleanup_backups per compatibilità
def cleanup_old_backups(retain_daily: int = 7, retain_weekly: int = 4, 
                        retain_monthly: int = 12, firebase_manager=None) -> Dict[str, Any]:
    """
    Alias di cleanup_backups per compatibilità.
    
    Args:
        retain_daily: Numero di backup giornalieri da conservare
        retain_weekly: Numero di backup settimanali da conservare
        retain_monthly: Numero di backup mensili da conservare
        firebase_manager: Istanza del gestore Firebase (opzionale)
        
    Returns:
        Statistica delle operazioni eseguite
    """
    return cleanup_backups(retain_daily, retain_weekly, retain_monthly, firebase_manager)
