""" 
Modulo per l'integrazione con LibreTranslate.
Questo modulo fornisce un'implementazione specializzata per il servizio
di traduzione LibreTranslate (https://libretranslate.com/).
"""
import os
import time
import json
import logging
import requests
from typing import Dict, List, Any, Optional, Union

from src.config.settings import get_setting
from src.utils.cache import cached

logger = logging.getLogger(__name__)

class LibreTranslateError(Exception):
    """Eccezione specifica per errori di LibreTranslate."""
    pass

class LibreTranslateService:
    """
    Servizio di traduzione basato su LibreTranslate.
    
    LibreTranslate è una soluzione open source per la traduzione automatica
    che può essere utilizzata tramite istanze pubbliche o ospitata localmente.
    """
    
    def __init__(self):
        """Inizializza il servizio LibreTranslate."""
        # Carica configurazioni
        self.api_url = get_setting("translation.libretranslate.url", 
                                  "https://translate.argosopentech.com/translate")
        self.detect_url = get_setting("translation.libretranslate.detect_url", 
                                     "https://translate.argosopentech.com/detect")
        self.languages_url = get_setting("translation.libretranslate.languages_url", 
                                        "https://translate.argosopentech.com/languages")
        
        self.api_key = get_setting("translation.libretranslate.api_key", "")
        self.timeout = get_setting("translation.libretranslate.timeout", 10)
        self.max_retries = get_setting("translation.libretranslate.max_retries", 3)
        self.retry_delay = get_setting("translation.libretranslate.retry_delay", 1)
        
        # Istanze di fallback
        self.fallback_instances = get_setting("translation.libretranslate.fallback_instances", [
            {
                "name": "LibreTranslate.de",
                "translate_url": "https://libretranslate.de/translate",
                "detect_url": "https://libretranslate.de/detect",
                "languages_url": "https://libretranslate.de/languages"
            },
            {
                "name": "TerraprintTranslate",
                "translate_url": "https://translate.terraprint.co/translate",
                "detect_url": "https://translate.terraprint.co/detect",
                "languages_url": "https://translate.terraprint.co/languages"
            }
        ])
        
        # Carica lingue supportate
        self._supported_languages = None
        
        logger.info(f"LibreTranslateService inizializzato con URL: {self.api_url}")
    
    @cached(ttl=3600 * 24)  # Cache per 24 ore
    def get_supported_languages(self) -> List[Dict[str, Any]]:
        """
        Ottiene le lingue supportate dal servizio.
        
        Returns:
            Lista di dizionari con informazioni sulle lingue supportate
        
        Raises:
            LibreTranslateError: Se non è possibile ottenere le lingue supportate
        """
        if self._supported_languages:
            return self._supported_languages
        
        # Prova con l'URL principale
        try:
            response = requests.get(
                self.languages_url,
                timeout=self.timeout
            )
            
            if response.status_code == 200:
                languages = response.json()
                self._supported_languages = languages
                return languages
        except Exception as e:
            logger.warning(f"Errore nell'ottenere lingue supportate: {e}")
        
        # Prova con istanze di fallback
        for instance in self.fallback_instances:
            try:
                languages_url = instance.get("languages_url")
                response = requests.get(
                    languages_url,
                    timeout=self.timeout
                )
                
                if response.status_code == 200:
                    languages = response.json()
                    self._supported_languages = languages
                    return languages
            except Exception as e:
                logger.warning(f"Errore nell'ottenere lingue da {instance.get('name', 'fallback')}: {e}")
        
        # Se arriviamo qui, tutti i tentativi sono falliti
        languages_fallback = [
            {"code": "en", "name": "English"},
            {"code": "it", "name": "Italian"},
            {"code": "es", "name": "Spanish"},
            {"code": "fr", "name": "French"},
            {"code": "de", "name": "German"},
            {"code": "pt", "name": "Portuguese"},
            {"code": "ru", "name": "Russian"}
        ]
        
        logger.warning("Utilizzando elenco lingue predefinito a causa di errori API")
        self._supported_languages = languages_fallback
        return languages_fallback
    
    def is_language_pair_supported(self, source_lang: str, target_lang: str) -> bool:
        """
        Verifica se una coppia di lingue è supportata.
        
        Args:
            source_lang: Codice lingua sorgente
            target_lang: Codice lingua di destinazione
            
        Returns:
            True se la coppia è supportata, False altrimenti
        """
        languages = self.get_supported_languages()
        
        source_supported = any(l.get("code") == source_lang for l in languages)
        target_supported = any(l.get("code") == target_lang for l in languages)
        
        return source_supported and target_supported
    
    def detect_language(self, text: str, retry: int = 0) -> str:
        """
        Rileva la lingua del testo.
        
        Args:
            text: Testo da analizzare
            retry: Contatore tentativi per ricorsione
            
        Returns:
            Codice lingua rilevato
            
        Raises:
            LibreTranslateError: Se non è possibile rilevare la lingua
        """
        if not text or text.strip() == "":
            return "en"  # Default a inglese per testo vuoto
        
        # Limita la lunghezza del testo per il rilevamento
        sample = text[:500]
        
        # Prepara i dati per la richiesta
        payload = {
            "q": sample
        }
        
        if self.api_key:
            payload["api_key"] = self.api_key
        
        headers = {
            "Content-Type": "application/json"
        }
        
        # Prova con l'URL principale
        try:
            response = requests.post(
                self.detect_url,
                headers=headers,
                data=json.dumps(payload),
                timeout=self.timeout
            )
            
            if response.status_code == 200:
                result = response.json()
                if isinstance(result, list) and len(result) > 0 and "language" in result[0]:
                    return result[0]["language"]
            
            logger.warning(f"Errore nel rilevamento lingua: {response.status_code} - {response.text}")
        except Exception as e:
            logger.warning(f"Errore nella chiamata per rilevamento lingua: {e}")
        
        # Prova con istanze di fallback
        for instance in self.fallback_instances:
            try:
                detect_url = instance.get("detect_url")
                response = requests.post(
                    detect_url,
                    headers=headers,
                    data=json.dumps(payload),
                    timeout=self.timeout
                )
                
                if response.status_code == 200:
                    result = response.json()
                    if isinstance(result, list) and len(result) > 0 and "language" in result[0]:
                        return result[0]["language"]
            except Exception as e:
                logger.warning(f"Errore nel rilevamento lingua con {instance.get('name', 'fallback')}: {e}")
        
        # Riprova in caso di errore
        if retry < self.max_retries:
            time.sleep(self.retry_delay)
            return self.detect_language(text, retry + 1)
        
        # Fallback a inglese se il rilevamento fallisce
        logger.warning("Rilevamento lingua fallito, utilizzo inglese come fallback")
        return "en"
    
    def translate(self, text: str, source_lang: str, target_lang: str, retry: int = 0) -> str:
        """
        Traduce il testo dalla lingua sorgente alla lingua di destinazione.
        
        Args:
            text: Testo da tradurre
            source_lang: Codice lingua sorgente (es. 'en', 'it')
            target_lang: Codice lingua di destinazione
            retry: Contatore tentativi per ricorsione
            
        Returns:
            Testo tradotto
            
        Raises:
            LibreTranslateError: Se non è possibile tradurre il testo
        """
        if not text or text.strip() == "":
            return ""
        
        # Verifica se la coppia di lingue è supportata
        if not self.is_language_pair_supported(source_lang, target_lang):
            raise LibreTranslateError(f"Coppia di lingue non supportata: {source_lang} -> {target_lang}")
        
        # Prepara i dati per la richiesta
        payload = {
            "q": text,
            "source": source_lang,
            "target": target_lang
        }
        
        if self.api_key:
            payload["api_key"] = self.api_key
        
        headers = {
            "Content-Type": "application/json"
        }
        
        # Prova con l'URL principale
        try:
            response = requests.post(
                self.api_url,
                headers=headers,
                data=json.dumps(payload),
                timeout=self.timeout
            )
            
            if response.status_code == 200:
                result = response.json()
                if "translatedText" in result:
                    return result["translatedText"]
            
            logger.warning(f"Errore LibreTranslate: {response.status_code} - {response.text}")
        except Exception as e:
            logger.warning(f"Errore nella chiamata a LibreTranslate: {e}")
        
        # Prova con istanze di fallback
        for instance in self.fallback_instances:
            try:
                translate_url = instance.get("translate_url")
                logger.info(f"Tentativo con URL di fallback: {instance.get('name', 'fallback')}")
                response = requests.post(
                    translate_url,
                    headers=headers,
                    data=json.dumps(payload),
                    timeout=self.timeout
                )
                
                if response.status_code == 200:
                    result = response.json()
                    if "translatedText" in result:
                        return result["translatedText"]
            except Exception as e:
                logger.warning(f"Errore con URL di fallback {instance.get('name', 'fallback')}: {e}")
        
        # Riprova in caso di errore
        if retry < self.max_retries:
            time.sleep(self.retry_delay)
            return self.translate(text, source_lang, target_lang, retry + 1)
        
        # Se arriviamo qui, tutti i tentativi sono falliti
        raise LibreTranslateError("Impossibile tradurre il testo con LibreTranslate")
    
    def is_available(self) -> bool:
        """
        Verifica se il servizio LibreTranslate è disponibile.
        
        Returns:
            True se il servizio è disponibile, False altrimenti
        """
        test_text = "Hello world"
        
        try:
            # Prova a tradurre una semplice frase di test
            result = self.translate(test_text, "en", "it")
            return result != test_text and result != ""
        except Exception as e:
            logger.warning(f"LibreTranslate non disponibile: {e}")
            return False
    
    def translate_batch(self, texts: List[str], source_lang: str, target_lang: str) -> List[str]:
        """
        Traduce un batch di testi.
        
        Args:
            texts: Lista di testi da tradurre
            source_lang: Codice lingua sorgente
            target_lang: Codice lingua di destinazione
            
        Returns:
            Lista di testi tradotti
            
        Raises:
            LibreTranslateError: Se non è possibile tradurre i testi
        """
        if not texts:
            return []
        
        # LibreTranslate non supporta traduzioni batch nelle API standard
        # Dobbiamo eseguire le traduzioni una per una
        results = []
        for text in texts:
            try:
                translated = self.translate(text, source_lang, target_lang)
                results.append(translated)
            except Exception as e:
                logger.error(f"Errore nella traduzione batch: {e}")
                # Aggiungi il testo originale in caso di errore
                results.append(text)
        
        return results

# Funzioni di utilità globali
def get_libre_translate_service() -> LibreTranslateService:
    """
    Ottiene un'istanza del servizio LibreTranslate.
    
    Returns:
        Istanza del servizio LibreTranslate
    """
    return LibreTranslateService()

def translate(text: str, source_lang: str, target_lang: str) -> str:
    """
    Funzione di utilità per tradurre un testo con LibreTranslate.
    
    Args:
        text: Testo da tradurre
        source_lang: Codice lingua sorgente
        target_lang: Codice lingua di destinazione
        
    Returns:
        Testo tradotto
        
    Raises:
        LibreTranslateError: Se non è possibile tradurre il testo
    """
    service = get_libre_translate_service()
    return service.translate(text, source_lang, target_lang)

def detect_language(text: str) -> str:
    """
    Funzione di utilità per rilevare la lingua con LibreTranslate.
    
    Args:
        text: Testo da analizzare
        
    Returns:
        Codice lingua rilevato
        
    Raises:
        LibreTranslateError: Se non è possibile rilevare la lingua
    """
    service = get_libre_translate_service()
    return service.detect_language(text)

def get_supported_languages() -> List[Dict[str, Any]]:
    """
    Funzione di utilità per ottenere le lingue supportate da LibreTranslate.
    
    Returns:
        Lista di dizionari con informazioni sulle lingue supportate
        
    Raises:
        LibreTranslateError: Se non è possibile ottenere le lingue supportate
    """
    service = get_libre_translate_service()
    return service.get_supported_languages()
