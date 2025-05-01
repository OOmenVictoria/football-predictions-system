""" 
Modulo coordinatore per la pubblicazione dei contenuti.
Questo modulo coordina il processo di pubblicazione degli articoli,
gestendo la tempistica di pubblicazione e rimozione, e tracciando lo stato della pubblicazione.
"""
import os
import sys
import time
import logging
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union, Tuple

from src.utils.cache import cached
from src.utils.database import FirebaseManager
from src.utils.time_utils import (
    parse_date, 
    date_to_str, 
    get_publication_window, 
    should_publish_now,
    should_expire_now
)
from src.config.settings import get_setting
from src.publishing.wordpress import WordPressPublisher
from src.content.generator import ContentGenerator

logger = logging.getLogger(__name__)

class ArticlePublisher:
    """
    Classe per la gestione della pubblicazione degli articoli di pronostico.
    
    Coordina la pubblicazione e rimozione degli articoli in base alla tempistica
    delle partite e lo stato di pubblicazione.
    """
    
    def __init__(self):
        """
        Inizializza il pubblicatore di articoli.
        """
        self.db = FirebaseManager()
        self.wp = WordPressPublisher()
        self.content_generator = ContentGenerator()
        
        # Configurazione orari (da settings o valori predefiniti)
        self.publish_hours_before = get_setting('publishing.hours_before_match', 12)
        self.expire_hours_after = get_setting('publishing.hours_after_match', 8)
        self.max_articles_per_run = get_setting('publishing.max_articles_per_run', 20)
        
        # Intervallo di sicurezza tra pubblicazioni (per evitare sovraccarichi)
        self.publish_interval_seconds = get_setting('publishing.interval_seconds', 30)
        
        logger.info(f"ArticlePublisher inizializzato: pubblicazione {self.publish_hours_before}h prima, " +
                   f"rimozione {self.expire_hours_after}h dopo")
    
    def publish_pending_articles(self, league_id: Optional[str] = None,
                               limit: Optional[int] = None) -> Dict[str, Any]:
        """
        Pubblica articoli in attesa che sono pronti per la pubblicazione.
        
        Args:
            league_id: ID del campionato specifico (opzionale)
            limit: Numero massimo di articoli da pubblicare (opzionale)
        
        Returns:
            Risultati dell'operazione con statistiche
        """
        logger.info("Avvio pubblicazione articoli in attesa")
        
        # Imposta limite predefinito se non specificato
        if limit is None:
            limit = self.max_articles_per_run
        
        results = {
            "success": 0,
            "failed": 0,
            "skipped": 0,
            "errors": [],
            "published": []
        }
        
        try:
            # Trova partite imminenti che richiedono articoli
            matches_ref = self.db.get_reference("data/matches")
            
            # Filtro per campionato specifico se richiesto
            if league_id:
                query_ref = matches_ref.order_by_child("league_id").equal_to(league_id)
            else:
                query_ref = matches_ref
            
            # Ottieni tutte le partite non ancora terminate
            matches = query_ref.order_by_child("datetime").get()
            
            if not matches:
                logger.info("Nessuna partita trovata per la pubblicazione")
                return results
            
            # Filtra le partite pronte per la pubblicazione
            pending_matches = []
            now = datetime.now()
            
            for match_id, match in matches.items():
                # Verifica se la partita è ancora in futuro o in corso
                if match.get("status") == "FINISHED":
                    continue
                
                # Verifica finestra di pubblicazione
                match_datetime = parse_date(match.get("datetime", ""))
                if not match_datetime:
                    continue
                
                # Calcola finestra di pubblicazione
                publish_after, publish_until = get_publication_window(
                    match_datetime, 
                    self.publish_hours_before,
                    self.expire_hours_after
                )
                
                # Verifica se è il momento di pubblicare
                if should_publish_now(now, publish_after, publish_until):
                    # Controlla se l'articolo è già stato pubblicato
                    if not match.get("article_published", False):
                        pending_matches.append({
                            "match_id": match_id,
                            "match": match,
                            "publish_time": publish_after
                        })
            
            # Ordina per data di pubblicazione (prima quelle più urgenti)
            pending_matches.sort(key=lambda x: x["publish_time"])
            
            # Limita il numero di articoli da pubblicare
            pending_matches = pending_matches[:limit]
            
            logger.info(f"Trovate {len(pending_matches)} partite in attesa di pubblicazione")
            
            # Pubblica gli articoli
            for match_info in pending_matches:
                match_id = match_info["match_id"]
                match = match_info["match"]
                
                try:
                    # Verifica se abbiamo già un articolo generato
                    article_ref = self.db.get_reference(f"articles/{match_id}")
                    article_data = article_ref.get()
                    
                    if not article_data:
                        # Genera l'articolo se non esiste
                        logger.info(f"Generazione articolo per partita {match_id}")
                        article_data = self.content_generator.generate_article(match_id)
                        
                        if not article_data:
                            logger.warning(f"Impossibile generare articolo per partita {match_id}")
                            results["errors"].append(f"Errore generazione articolo per {match_id}")
                            results["failed"] += 1
                            continue
                    
                    # Pubblica l'articolo su WordPress
                    logger.info(f"Pubblicazione articolo per partita {match_id}")
                    
                    # Imposta metadati per la scadenza
                    match_datetime = parse_date(match.get("datetime", ""))
                    _, expire_time = get_publication_window(
                        match_datetime, 
                        self.publish_hours_before,
                        self.expire_hours_after
                    )
                    
                    wp_result = self.wp.publish_article(
                        title=article_data.get("title", ""),
                        content=article_data.get("content", ""),
                        excerpt=article_data.get("excerpt", ""),
                        categories=article_data.get("categories", []),
                        tags=article_data.get("tags", []),
                        metadata={
                            "match_id": match_id,
                            "home_team": match.get("home_team", ""),
                            "away_team": match.get("away_team", ""),
                            "league_id": match.get("league_id", ""),
                            "match_datetime": match.get("datetime", ""),
                            "expiry_time": expire_time.isoformat()
                        }
                    )
                    
                    if wp_result and "post_id" in wp_result:
                        # Aggiorna lo stato nel database
                        match_updates = {
                            "article_published": True,
                            "article_id": wp_result["post_id"],
                            "article_url": wp_result.get("url", ""),
                            "article_publish_time": datetime.now().isoformat(),
                        }
                        
                        matches_ref.child(match_id).update(match_updates)
                        
                        # Aggiorna lo stato nell'articolo
                        article_data["published"] = True
                        article_data["wp_post_id"] = wp_result["post_id"]
                        article_data["publish_time"] = datetime.now().isoformat()
                        article_data["expiry_time"] = expire_time.isoformat()
                        
                        article_ref.set(article_data)
                        
                        results["success"] += 1
                        results["published"].append({
                            "match_id": match_id,
                            "post_id": wp_result["post_id"],
                            "title": article_data.get("title", ""),
                            "url": wp_result.get("url", "")
                        })
                        
                        logger.info(f"Articolo pubblicato con successo: {wp_result['post_id']}")
                    else:
                        results["failed"] += 1
                        error_msg = f"Errore nella pubblicazione su WordPress per {match_id}"
                        results["errors"].append(error_msg)
                        logger.error(error_msg)
                    
                    # Attendi intervallo di sicurezza
                    time.sleep(self.publish_interval_seconds)
                    
                except Exception as e:
                    results["failed"] += 1
                    error_msg = f"Errore nella pubblicazione per {match_id}: {str(e)}"
                    results["errors"].append(error_msg)
                    logger.error(error_msg, exc_info=True)
        
        except Exception as e:
            error_msg = f"Errore generale nella pubblicazione degli articoli: {str(e)}"
            results["errors"].append(error_msg)
            logger.error(error_msg, exc_info=True)
        
        logger.info(f"Pubblicazione completata: {results['success']} successi, " +
                   f"{results['failed']} falliti, {results['skipped']} saltati")
        
        return results
    
    def cleanup_expired_articles(self, limit: Optional[int] = None) -> Dict[str, Any]:
        """
        Rimuove articoli scaduti (oltre 8 ore dopo la fine della partita).
        
        Args:
            limit: Numero massimo di articoli da rimuovere (opzionale)
        
        Returns:
            Risultati dell'operazione con statistiche
        """
        logger.info("Avvio pulizia articoli scaduti")
        
        # Imposta limite predefinito se non specificato
        if limit is None:
            limit = self.max_articles_per_run
        
        results = {
            "success": 0,
            "failed": 0,
            "errors": [],
            "removed": []
        }
        
        try:
            # Trova articoli pubblicati
            matches_ref = self.db.get_reference("data/matches")
            query_ref = matches_ref.order_by_child("article_published").equal_to(True)
            
            published_matches = query_ref.get()
            
            if not published_matches:
                logger.info("Nessun articolo pubblicato trovato")
                return results
            
            # Filtra gli articoli scaduti
            expired_matches = []
            now = datetime.now()
            
            for match_id, match in published_matches.items():
                # Verifica scadenza
                match_datetime = parse_date(match.get("datetime", ""))
                if not match_datetime:
                    continue
                
                # Calcola finestra di pubblicazione
                _, publish_until = get_publication_window(
                    match_datetime, 
                    self.publish_hours_before,
                    self.expire_hours_after
                )
                
                # Verifica se è il momento di rimuovere
                if should_expire_now(now, publish_until):
                    expired_matches.append({
                        "match_id": match_id,
                        "match": match,
                        "article_id": match.get("article_id"),
                        "expire_time": publish_until
                    })
            
            # Limita il numero di articoli da rimuovere
            expired_matches = expired_matches[:limit]
            
            logger.info(f"Trovati {len(expired_matches)} articoli scaduti da rimuovere")
            
            # Rimuovi gli articoli
            for match_info in expired_matches:
                match_id = match_info["match_id"]
                article_id = match_info["article_id"]
                
                try:
                    # Rimuovi l'articolo da WordPress
                    logger.info(f"Rimozione articolo {article_id} per partita {match_id}")
                    result = self.wp.delete_article(article_id)
                    
                    if result:
                        # Aggiorna lo stato nel database
                        match_updates = {
                            "article_published": False,
                            "article_removed": True,
                            "article_remove_time": datetime.now().isoformat()
                        }
                        
                        matches_ref.child(match_id).update(match_updates)
                        
                        # Aggiorna stato anche nella collezione degli articoli
                        article_ref = self.db.get_reference(f"articles/{match_id}")
                        article_data = article_ref.get()
                        
                        if article_data:
                            article_data["published"] = False
                            article_data["removed"] = True
                            article_data["remove_time"] = datetime.now().isoformat()
                            article_ref.set(article_data)
                        
                        results["success"] += 1
                        results["removed"].append({
                            "match_id": match_id,
                            "article_id": article_id
                        })
                        
                        logger.info(f"Articolo rimosso con successo: {article_id}")
                    else:
                        results["failed"] += 1
                        error_msg = f"Errore nella rimozione dell'articolo {article_id} per {match_id}"
                        results["errors"].append(error_msg)
                        logger.error(error_msg)
                    
                    # Attendi intervallo di sicurezza
                    time.sleep(self.publish_interval_seconds)
                    
                except Exception as e:
                    results["failed"] += 1
                    error_msg = f"Errore nella rimozione dell'articolo {article_id} per {match_id}: {str(e)}"
                    results["errors"].append(error_msg)
                    logger.error(error_msg, exc_info=True)
        
        except Exception as e:
            error_msg = f"Errore generale nella rimozione degli articoli: {str(e)}"
            results["errors"].append(error_msg)
            logger.error(error_msg, exc_info=True)
        
        logger.info(f"Pulizia completata: {results['success']} rimossi, {results['failed']} falliti")
        
        return results
    
    def get_publishing_status(self) -> Dict[str, Any]:
        """
        Ottiene lo stato generale della pubblicazione.
        
        Returns:
            Stato corrente della pubblicazione con statistiche
        """
        try:
            # Statistiche articoli
            matches_ref = self.db.get_reference("data/matches")
            
            # Articoli pubblicati
            published_query = matches_ref.order_by_child("article_published").equal_to(True)
            published_count = len(published_query.get() or {})
            
            # Articoli rimossi
            removed_query = matches_ref.order_by_child("article_removed").equal_to(True)
            removed_count = len(removed_query.get() or {})
            
            # Articoli in attesa
            all_matches = matches_ref.get() or {}
            pending_count = 0
            now = datetime.now()
            
            for match_id, match in all_matches.items():
                if match.get("article_published", False):
                    continue
                
                if match.get("status") == "FINISHED":
                    continue
                
                # Verifica finestra di pubblicazione
                match_datetime = parse_date(match.get("datetime", ""))
                if not match_datetime:
                    continue
                
                # Calcola finestra di pubblicazione
                publish_after, publish_until = get_publication_window(
                    match_datetime, 
                    self.publish_hours_before,
                    self.expire_hours_after
                )
                
                # Verifica se è il momento di pubblicare
                if should_publish_now(now, publish_after, publish_until):
                    pending_count += 1
            
            # Articoli in scadenza
            expiring_count = 0
            
            for match_id, match in all_matches.items():
                if not match.get("article_published", False):
                    continue
                
                if match.get("article_removed", False):
                    continue
                
                # Verifica finestra di pubblicazione
                match_datetime = parse_date(match.get("datetime", ""))
                if not match_datetime:
                    continue
                
                # Calcola finestra di pubblicazione
                _, publish_until = get_publication_window(
                    match_datetime, 
                    self.publish_hours_before,
                    self.expire_hours_after
                )
                
                # Verifica se è il momento di rimuovere
                if should_expire_now(now, publish_until):
                    expiring_count += 1
            
            return {
                "total_articles": published_count + pending_count,
                "published": published_count,
                "removed": removed_count,
                "pending": pending_count,
                "expiring": expiring_count,
                "last_check": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Errore nel recupero dello stato di pubblicazione: {str(e)}", exc_info=True)
            return {
                "error": str(e),
                "last_check": datetime.now().isoformat()
            }
    
    def republish_article(self, match_id: str) -> Dict[str, Any]:
        """
        Ripubblica un articolo specifico.
        
        Args:
            match_id: ID della partita
        
        Returns:
            Risultato dell'operazione
        """
        logger.info(f"Ripubblicazione articolo per partita {match_id}")
        
        try:
            # Ottieni dati partita
            match_ref = self.db.get_reference(f"data/matches/{match_id}")
            match_data = match_ref.get()
            
            if not match_data:
                error_msg = f"Partita {match_id} non trovata"
                logger.error(error_msg)
                return {"success": False, "error": error_msg}
            
            # Verifica se c'è già un articolo pubblicato
            if match_data.get("article_published", False) and match_data.get("article_id"):
                # Rimuovi l'articolo esistente
                logger.info(f"Rimozione articolo esistente {match_data['article_id']}")
                self.wp.delete_article(match_data["article_id"])
            
            # Rigenera l'articolo
            logger.info(f"Generazione nuovo articolo per partita {match_id}")
            article_data = self.content_generator.generate_article(match_id, force=True)
            
            if not article_data:
                error_msg = f"Impossibile generare articolo per partita {match_id}"
                logger.error(error_msg)
                return {"success": False, "error": error_msg}
            
            # Pubblica nuovo articolo
            match_datetime = parse_date(match_data.get("datetime", ""))
            _, expire_time = get_publication_window(
                match_datetime, 
                self.publish_hours_before,
                self.expire_hours_after
            )
            
            wp_result = self.wp.publish_article(
                title=article_data.get("title", ""),
                content=article_data.get("content", ""),
                excerpt=article_data.get("excerpt", ""),
                categories=article_data.get("categories", []),
                tags=article_data.get("tags", []),
                metadata={
                    "match_id": match_id,
                    "home_team": match_data.get("home_team", ""),
                    "away_team": match_data.get("away_team", ""),
                    "league_id": match_data.get("league_id", ""),
                    "match_datetime": match_data.get("datetime", ""),
                    "expiry_time": expire_time.isoformat()
                }
            )
            
            if wp_result and "post_id" in wp_result:
                # Aggiorna lo stato nel database
                match_updates = {
                    "article_published": True,
                    "article_id": wp_result["post_id"],
                    "article_url": wp_result.get("url", ""),
                    "article_publish_time": datetime.now().isoformat(),
                    "article_removed": False,
                    "article_remove_time": None
                }
                
                match_ref.update(match_updates)
                
                # Aggiorna lo stato nell'articolo
                article_ref = self.db.get_reference(f"articles/{match_id}")
                
                article_data["published"] = True
                article_data["wp_post_id"] = wp_result["post_id"]
                article_data["publish_time"] = datetime.now().isoformat()
                article_data["expiry_time"] = expire_time.isoformat()
                article_data["removed"] = False
                article_data["remove_time"] = None
                
                article_ref.set(article_data)
                
                logger.info(f"Articolo ripubblicato con successo: {wp_result['post_id']}")
                
                return {
                    "success": True,
                    "post_id": wp_result["post_id"],
                    "url": wp_result.get("url", "")
                }
            else:
                error_msg = "Errore nella pubblicazione su WordPress"
                logger.error(error_msg)
                return {"success": False, "error": error_msg}
            
        except Exception as e:
            error_msg = f"Errore nella ripubblicazione: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return {"success": False, "error": error_msg}
    
    def run_publication_cycle(self, league_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Esegue un ciclo completo di pubblicazione: pubblica nuovi articoli e pulisce quelli scaduti.
        
        Args:
            league_id: ID del campionato specifico (opzionale)
        
        Returns:
            Risultati dell'operazione
        """
        logger.info(f"Avvio ciclo di pubblicazione" + (f" per league_id={league_id}" if league_id else ""))
        
        # Verifica connessione a WordPress
        if not self.wp.test_connection():
            error_msg = "Impossibile connettersi a WordPress, ciclo di pubblicazione annullato"
            logger.error(error_msg)
            return {"success": False, "error": error_msg}
        
        # Ottieni limiti dalle impostazioni
        publish_limit = get_setting('publishing.publish_limit_per_cycle', 20)
        cleanup_limit = get_setting('publishing.cleanup_limit_per_cycle', 20)
        
        results = {
            "publication": None,
            "cleanup": None,
            "status": None
        }
        
        # Pubblica nuovi articoli
        try:
            logger.info(f"Pubblicazione nuovi articoli (limite: {publish_limit})")
            results["publication"] = self.publish_pending_articles(
                league_id=league_id,
                limit=publish_limit
            )
        except Exception as e:
            logger.error(f"Errore nella pubblicazione: {str(e)}", exc_info=True)
            results["publication"] = {"success": 0, "failed": 0, "error": str(e)}
        
        # Pulizia articoli scaduti
        try:
            logger.info(f"Pulizia articoli scaduti (limite: {cleanup_limit})")
            results["cleanup"] = self.cleanup_expired_articles(
                limit=cleanup_limit
            )
        except Exception as e:
            logger.error(f"Errore nella pulizia: {str(e)}", exc_info=True)
            results["cleanup"] = {"success": 0, "failed": 0, "error": str(e)}
        
        # Aggiorna stato
        try:
            results["status"] = self.get_publishing_status()
        except Exception as e:
            logger.error(f"Errore nel recupero stato: {str(e)}", exc_info=True)
            results["status"] = {"error": str(e)}
        
        # Salva risultati nel database
        try:
            stats_ref = self.db.get_reference("stats/publishing")
            stats_data = {
                "last_run": datetime.now().isoformat(),
                "published": results["publication"]["success"] if "publication" in results and results["publication"] else 0,
                "cleaned": results["cleanup"]["success"] if "cleanup" in results and results["cleanup"] else 0,
                "status": results["status"]
            }
            stats_ref.set(stats_data)
        except Exception as e:
            logger.error(f"Errore nel salvataggio statistiche: {str(e)}", exc_info=True)
        
        logger.info("Ciclo di pubblicazione completato")
        
        return results

# Funzioni di utilità globali
def publish_articles(league_id: Optional[str] = None, limit: Optional[int] = None) -> Dict[str, Any]:
    """
    Pubblica articoli in attesa.
    
    Args:
        league_id: ID del campionato specifico (opzionale)
        limit: Numero massimo di articoli da pubblicare (opzionale)
    
    Returns:
        Risultati dell'operazione
    """
    publisher = ArticlePublisher()
    return publisher.publish_pending_articles(league_id, limit)

def cleanup_articles(limit: Optional[int] = None) -> Dict[str, Any]:
    """
    Rimuove articoli scaduti.
    
    Args:
        limit: Numero massimo di articoli da rimuovere (opzionale)
    
    Returns:
        Risultati dell'operazione
    """
    publisher = ArticlePublisher()
    return publisher.cleanup_expired_articles(limit)

def run_publication_cycle(league_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Esegue un ciclo completo di pubblicazione.
    
    Args:
        league_id: ID del campionato specifico (opzionale)
    
    Returns:
        Risultati dell'operazione
    """
    publisher = ArticlePublisher()
    return publisher.run_publication_cycle(league_id)

def get_publishing_status() -> Dict[str, Any]:
    """
    Ottiene lo stato generale della pubblicazione.
    
    Returns:
        Stato corrente della pubblicazione
    """
    publisher = ArticlePublisher()
    return publisher.get_publishing_status()

def republish_article(match_id: str) -> Dict[str, Any]:
    """
    Ripubblica un articolo specifico.
    
    Args:
        match_id: ID della partita
    
    Returns:
        Risultato dell'operazione
    """
    publisher = ArticlePublisher()
    return publisher.republish_article(match_id)
