#!/usr/bin/env python3
"""
Script per la pulizia della cache del sistema.
Questo script fornisce funzionalità per cancellare la cache del sistema,
sia completa che selettiva, per garantire dati aggiornati.
"""

import os
import sys
import argparse
import logging
import json
import shutil
import sqlite3
from datetime import datetime, timedelta

# Aggiunge la directory radice al path di Python per permettere import relativi
script_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(script_dir)
sys.path.insert(0, root_dir)

from src.utils.cache import get_cache_dir
from src.config.settings import get_setting

# Configurazione logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("cache_cleaner")

def parse_args():
    """Parse gli argomenti da linea di comando."""
    parser = argparse.ArgumentParser(description="Pulisce la cache del sistema di pronostici calcistici.")
    
    parser.add_argument("--all", action="store_true", help="Cancella tutta la cache")
    parser.add_argument("--older-than", type=int, default=7, help="Cancella cache più vecchia di N giorni")
    parser.add_argument("--module", type=str, help="Cancella solo la cache di un modulo specifico")
    parser.add_argument("--dry-run", action="store_true", help="Simula l'operazione senza cancellare realmente")
    parser.add_argument("--verbose", action="store_true", help="Mostra dettagli aggiuntivi")
    
    return parser.parse_args()

def get_cache_info():
    """Ottiene informazioni sulla cache."""
    cache_dir = get_cache_dir()
    
    # Cache directory
    memory_cache_size = 0
    disk_cache_size = 0
    firebase_cache_size = 0
    
    cache_files = []
    
    # Verifica la cache su disco
    if os.path.exists(cache_dir):
        for root, dirs, files in os.walk(cache_dir):
            for file in files:
                file_path = os.path.join(root, file)
                file_size = os.path.getsize(file_path)
                file_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                
                rel_path = os.path.relpath(file_path, cache_dir)
                module = os.path.dirname(rel_path).replace(os.path.sep, ".")
                
                cache_files.append({
                    "path": file_path,
                    "size": file_size,
                    "time": file_time,
                    "module": module,
                    "type": "sqlite" if file.endswith(".db") else "json"
                })
                
                disk_cache_size += file_size
    
    # Verifica la cache SQLite
    sqlite_db = os.path.join(cache_dir, "cache.db")
    if os.path.exists(sqlite_db):
        try:
            conn = sqlite3.connect(sqlite_db)
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*), SUM(length(value)) FROM cache")
            count, size = cursor.fetchone()
            
            if size:
                disk_cache_size += size
            
            cursor.execute("SELECT key, timestamp, length(value) FROM cache")
            for key, timestamp, size in cursor.fetchall():
                module = key.split(":")[0] if ":" in key else ""
                cache_files.append({
                    "path": f"sqlite://{key}",
                    "size": size,
                    "time": datetime.fromtimestamp(timestamp),
                    "module": module,
                    "type": "sqlite"
                })
            
            conn.close()
        except Exception as e:
            logger.error(f"Errore nell'accesso alla cache SQLite: {e}")
    
    return {
        "total_size": disk_cache_size + memory_cache_size + firebase_cache_size,
        "disk_cache_size": disk_cache_size,
        "memory_cache_size": memory_cache_size,
        "firebase_cache_size": firebase_cache_size,
        "files": cache_files
    }

def clear_all_cache(dry_run=False):
    """Cancella tutta la cache."""
    cache_dir = get_cache_dir()
    
    logger.info(f"{'Simulazione cancellazione' if dry_run else 'Cancellazione'} della cache completa...")
    
    if os.path.exists(cache_dir):
        if not dry_run:
            for root, dirs, files in os.walk(cache_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    try:
                        os.remove(file_path)
                        logger.info(f"Rimosso file: {file_path}")
                    except Exception as e:
                        logger.error(f"Errore nella rimozione di {file_path}: {e}")
        
        logger.info(f"{'Simulata cancellazione' if dry_run else 'Cancellata'} cache da: {cache_dir}")
    else:
        logger.info(f"Directory cache non trovata: {cache_dir}")
    
    return True

def clear_old_cache(days=7, dry_run=False):
    """Cancella la cache più vecchia di N giorni."""
    cache_info = get_cache_info()
    cutoff_date = datetime.now() - timedelta(days=days)
    
    logger.info(f"{'Simulazione cancellazione' if dry_run else 'Cancellazione'} della cache più vecchia di {days} giorni...")
    
    deleted_count = 0
    deleted_size = 0
    
    for file_info in cache_info["files"]:
        if file_info["time"] < cutoff_date:
            path = file_info["path"]
            
            if not dry_run:
                if path.startswith("sqlite://"):
                    # Questa è una entry nel database SQLite
                    try:
                        cache_dir = get_cache_dir()
                        sqlite_db = os.path.join(cache_dir, "cache.db")
                        conn = sqlite3.connect(sqlite_db)
                        cursor = conn.cursor()
                        
                        key = path.replace("sqlite://", "")
                        cursor.execute("DELETE FROM cache WHERE key = ?", (key,))
                        
                        conn.commit()
                        conn.close()
                        
                        logger.info(f"Rimossa chiave SQLite: {key}")
                    except Exception as e:
                        logger.error(f"Errore nella rimozione della chiave SQLite {key}: {e}")
                else:
                    # Questo è un file su disco
                    try:
                        os.remove(path)
                        logger.info(f"Rimosso file: {path}")
                    except Exception as e:
                        logger.error(f"Errore nella rimozione di {path}: {e}")
            
            deleted_count += 1
            deleted_size += file_info["size"]
    
    logger.info(f"{'Simulata cancellazione' if dry_run else 'Cancellati'} {deleted_count} file cache per il modulo {module_name} ({deleted_size / (1024*1024):.2f} MB)")
    
    return True

def print_cache_stats(verbose=False):
    """Stampa statistiche sulla cache."""
    cache_info = get_cache_info()
    
    print("\n=== Statistiche Cache ===")
    print(f"Dimensione totale: {cache_info['total_size'] / (1024*1024):.2f} MB")
    print(f"Cache su disco: {cache_info['disk_cache_size'] / (1024*1024):.2f} MB")
    print(f"Cache in memoria: {cache_info['memory_cache_size'] / (1024*1024):.2f} MB")
    print(f"Cache su Firebase: {cache_info['firebase_cache_size'] / (1024*1024):.2f} MB")
    print(f"Numero file: {len(cache_info['files'])}")
    
    if verbose:
        print("\nDettaglio file in cache:")
        # Raggruppa per modulo
        modules = {}
        for file in cache_info["files"]:
            module = file["module"]
            if module not in modules:
                modules[module] = {"count": 0, "size": 0}
            
            modules[module]["count"] += 1
            modules[module]["size"] += file["size"]
        
        # Stampa i moduli ordinati per dimensione
        print("\nDimensione cache per modulo:")
        for module, stats in sorted(modules.items(), key=lambda x: x[1]["size"], reverse=True):
            if module:
                print(f"  {module}: {stats['count']} file, {stats['size'] / (1024*1024):.2f} MB")
        
        # Stampa i file più grandi
        print("\nFile più grandi in cache:")
        for file in sorted(cache_info["files"], key=lambda x: x["size"], reverse=True)[:10]:
            print(f"  {file['path']}: {file['size'] / (1024*1024):.2f} MB, {file['time']}")

def main():
    """Funzione principale dello script."""
    args = parse_args()
    
    # Mostra statistiche
    print_cache_stats(args.verbose)
    
    # Esegui operazioni di pulizia
    if args.all:
        clear_all_cache(args.dry_run)
    elif args.module:
        clear_module_cache(args.module, args.dry_run)
    elif args.older_than:
        clear_old_cache(args.older_than, args.dry_run)
    
    # Mostra statistiche aggiornate dopo la pulizia
    if args.all or args.module or args.older_than:
        print("\n=== Statistiche Cache dopo la pulizia ===")
        print_cache_stats(args.verbose)

if __name__ == "__main__":
    main()conn.close()
                        
                        logger.info(f"Rimossa chiave SQLite: {key}")
                    except Exception as e:
                        logger.error(f"Errore nella rimozione della chiave SQLite {key}: {e}")
                else:
                    # Questo è un file su disco
                    try:
                        os.remove(path)
                        logger.info(f"Rimosso file: {path}")
                    except Exception as e:
                        logger.error(f"Errore nella rimozione di {path}: {e}")
            
            deleted_count += 1
            deleted_size += file_info["size"]
    
    logger.info(f"{'Simulata cancellazione' if dry_run else 'Cancellati'} {deleted_count} file cache ({deleted_size / (1024*1024):.2f} MB)")
    
    return True

def clear_module_cache(module_name, dry_run=False):
    """Cancella la cache di un modulo specifico."""
    cache_info = get_cache_info()
    
    logger.info(f"{'Simulazione cancellazione' if dry_run else 'Cancellazione'} della cache per il modulo: {module_name}")
    
    deleted_count = 0
    deleted_size = 0
    
    for file_info in cache_info["files"]:
        if file_info["module"].startswith(module_name):
            path = file_info["path"]
            
            if not dry_run:
                if path.startswith("sqlite://"):
                    # Questa è una entry nel database SQLite
                    try:
                        cache_dir = get_cache_dir()
                        sqlite_db = os.path.join(cache_dir, "cache.db")
                        conn = sqlite3.connect(sqlite_db)
                        cursor = conn.cursor()
                        
                        key = path.replace("sqlite://", "")
                        cursor.execute("DELETE FROM cache WHERE key = ?", (key,))
                        
                        conn.commit()
