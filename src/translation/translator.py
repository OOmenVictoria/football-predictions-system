""" 
Modulo per la traduzione automatica dei contenuti.
Questo modulo fornisce funzionalità per tradurre articoli e contenuti in diverse lingue
utilizzando servizi di traduzione gratuiti.
"""
import os
import sys
import time
import logging
import json
import re
import threading
import requests
from typing import Dict, List, Any, Optional, Union, Tuple
from functools import lru_cache

from src.utils.cache import cached
from src.utils.database import FirebaseManager
from src.config.settings import get_setting

logger = logging.getLogger(__name__)

class TranslationError(Exception):
    """Eccezione specifica per errori di traduzione."""
    pass

class BaseTranslator:
    """
    Classe base per i servizi di traduzione.
    
    Definisce l'interfaccia comune che tutti i servizi di traduzione devono implementare.
    """
    
    def __init__(self):
        """Inizializza il traduttore."""
        self.name = "base"
    
    def translate(self, text: str, source_lang: str, target_lang: str) -> str:
        """
        Traduce il testo dalla lingua sorgente alla lingua di destinazione.
        
        Args:
            text: Testo da tradurre
            source_lang: Codice lingua sorgente (es. 'en', 'it')
            target_lang: Codice lingua di destinazione
            
        Returns:
            Testo tradotto
            
        Raises:
            TranslationError: Se si verifica un errore nella traduzione
        """
        raise NotImplementedError("I servizi di traduzione devono implementare questo metodo")
    
    def detect_language(self, text: str) -> str:
        """
        Rileva la lingua del testo.
        
        Args:
            text: Testo da analizzare
            
        Returns:
            Codice lingua rilevato
            
        Raises:
            TranslationError: Se si verifica un errore nel rilevamento
        """
        raise NotImplementedError("I servizi di traduzione devono implementare questo metodo")
    
    def is_available(self) -> bool:
        """
        Verifica se il servizio di traduzione è disponibile.
        
        Returns:
            True se il servizio è disponibile, False altrimenti
        """
        try:
            # Prova a tradurre una semplice frase di test
            result = self.translate("Hello", "en", "it")
            return result != "Hello" and result != ""
        except Exception as e:
            logger.warning(f"Servizio {self.name} non disponibile: {e}")
            return False

class LibreTranslator(BaseTranslator):
    """
    Implementazione del traduttore utilizzando LibreTranslate.
    
    LibreTranslate è un servizio di traduzione open source che può essere utilizzato
    gratuitamente tramite API pubbliche o ospitato localmente.
    """
    
    def __init__(self):
        """Inizializza il traduttore LibreTranslate."""
        super().__init__()
        self.name = "libretranslate"
        self.api_url = get_setting("translation.libretranslate.url", 
                                   "https://translate.argosopentech.com/translate")
        self.detect_url = get_setting("translation.libretranslate.detect_url", 
                                     "https://translate.argosopentech.com/detect")
        self.api_key = get_setting("translation.libretranslate.api_key", "")
        self.timeout = get_setting("translation.libretranslate.timeout", 10)
        
        # Opzioni di fallback (ospitati pubblicamente)
        self.fallback_urls = get_setting("translation.libretranslate.fallback_urls", [
            "https://libretranslate.de/translate",
            "https://translate.terraprint.co/translate"
        ])
        
        # Coppie di lingue supportate
        self.supported_pairs = get_setting("translation.libretranslate.supported_pairs", {
            "en": ["it", "es", "fr", "de", "pt", "ru"],
            "it": ["en", "es", "fr", "de", "pt"],
            "es": ["en", "it", "fr", "de", "pt"],
            "fr": ["en", "it", "es", "de", "pt"],
            "de": ["en", "it", "es", "fr", "pt"],
            "pt": ["en", "it", "es", "fr", "de"],
            "ru": ["en"]
        })
        
        logger.info(f"Inizializzato traduttore LibreTranslate: {self.api_url}")
    
    def translate(self, text: str, source_lang: str, target_lang: str) -> str:
        """
        Traduce il testo utilizzando LibreTranslate.
        
        Args:
            text: Testo da tradurre
            source_lang: Codice lingua sorgente (es. 'en', 'it')
            target_lang: Codice lingua di destinazione
            
        Returns:
            Testo tradotto
            
        Raises:
            TranslationError: Se si verifica un errore nella traduzione
        """
        if not text or text.strip() == "":
            return ""
        
        # Verifica se la coppia di lingue è supportata
        if source_lang not in self.supported_pairs or target_lang not in self.supported_pairs.get(source_lang, []):
            raise TranslationError(f"Coppia di lingue non supportata: {source_lang} -> {target_lang}")
        
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
                else:
                    logger.warning(f"Risposta LibreTranslate inattesa: {result}")
            
            logger.warning(f"Errore LibreTranslate: {response.status_code} - {response.text}")
        except Exception as e:
            logger.warning(f"Errore nella chiamata a LibreTranslate: {e}")
        
        # Prova con gli URL di fallback
        for fallback_url in self.fallback_urls:
            try:
                logger.info(f"Tentativo con URL di fallback: {fallback_url}")
                response = requests.post(
                    fallback_url,
                    headers=headers,
                    data=json.dumps(payload),
                    timeout=self.timeout
                )
                
                if response.status_code == 200:
                    result = response.json()
                    if "translatedText" in result:
                        return result["translatedText"]
            except Exception as e:
                logger.warning(f"Errore con URL di fallback {fallback_url}: {e}")
        
        # Se arriviamo qui, tutti i tentativi sono falliti
        raise TranslationError("Impossibile tradurre il testo con LibreTranslate")
    
    def detect_language(self, text: str) -> str:
        """
        Rileva la lingua del testo utilizzando LibreTranslate.
        
        Args:
            text: Testo da analizzare
            
        Returns:
            Codice lingua rilevato
            
        Raises:
            TranslationError: Se si verifica un errore nel rilevamento
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
        
        # Fallback a inglese se il rilevamento fallisce
        return "en"

class LingvaTranslator(BaseTranslator):
    """
    Implementazione del traduttore utilizzando Lingva.
    
    Lingva è un'alternativa a Google Translate che può essere utilizzata gratuitamente
    attraverso istanze pubbliche.
    """
    
    def __init__(self):
        """Inizializza il traduttore Lingva."""
        super().__init__()
        self.name = "lingva"
        self.api_url = get_setting("translation.lingva.url", 
                                  "https://lingva.ml/api/v1/{source}/{target}/{query}")
        self.timeout = get_setting("translation.lingva.timeout", 15)
        
        # Istanze pubbliche alternative
        self.fallback_urls = get_setting("translation.lingva.fallback_urls", [
            "https://lingva.pussthecat.org/api/v1/{source}/{target}/{query}",
            "https://lingva.lunar.icu/api/v1/{source}/{target}/{query}"
        ])
        
        # Lingva supporta tutte le lingue di Google Translate
        self.supported_pairs = get_setting("translation.lingva.supported_pairs", {
            "en": ["it", "es", "fr", "de", "pt", "ru", "ar", "ja", "zh", "ko"],
            "it": ["en", "es", "fr", "de", "pt", "ru"],
            "es": ["en", "it", "fr", "de", "pt"],
            "fr": ["en", "it", "es", "de", "pt"],
            "de": ["en", "it", "es", "fr", "pt"],
            "pt": ["en", "it", "es", "fr", "de"],
            "ru": ["en", "it"],
            "ar": ["en"],
            "ja": ["en"],
            "zh": ["en"],
            "ko": ["en"]
        })
        
        logger.info(f"Inizializzato traduttore Lingva")
    
    def translate(self, text: str, source_lang: str, target_lang: str) -> str:
        """
        Traduce il testo utilizzando Lingva.
        
        Args:
            text: Testo da tradurre
            source_lang: Codice lingua sorgente (es. 'en', 'it')
            target_lang: Codice lingua di destinazione
            
        Returns:
            Testo tradotto
            
        Raises:
            TranslationError: Se si verifica un errore nella traduzione
        """
        if not text or text.strip() == "":
            return ""
        
        # Verifica se la coppia di lingue è supportata
        if source_lang not in self.supported_pairs or target_lang not in self.supported_pairs.get(source_lang, []):
            raise TranslationError(f"Coppia di lingue non supportata: {source_lang} -> {target_lang}")
        
        # Le API Lingva hanno limiti di lunghezza, dividiamo il testo se necessario
        if len(text) > 5000:
            return self._batch_translate(text, source_lang, target_lang)
        
        # URL encode del testo
        import urllib.parse
        query = urllib.parse.quote(text)
        
        # Costruisci l'URL
        url = self.api_url.format(source=source_lang, target=target_lang, query=query)
        
        # Prova con l'URL principale
        try:
            response = requests.get(url, timeout=self.timeout)
            
            if response.status_code == 200:
                result = response.json()
                if "translation" in result:
                    return result["translation"]
            
            logger.warning(f"Errore Lingva: {response.status_code} - {response.text}")
        except Exception as e:
            logger.warning(f"Errore nella chiamata a Lingva: {e}")
        
        # Prova con gli URL di fallback
        for fallback_template in self.fallback_urls:
            try:
                fallback_url = fallback_template.format(source=source_lang, target=target_lang, query=query)
                logger.info(f"Tentativo con URL di fallback Lingva")
                response = requests.get(fallback_url, timeout=self.timeout)
                
                if response.status_code == 200:
                    result = response.json()
                    if "translation" in result:
                        return result["translation"]
            except Exception as e:
                logger.warning(f"Errore con URL di fallback Lingva: {e}")
        
        # Se arriviamo qui, tutti i tentativi sono falliti
        raise TranslationError("Impossibile tradurre il testo con Lingva")
    
    def _batch_translate(self, text: str, source_lang: str, target_lang: str) -> str:
        """
        Traduce testi lunghi dividendoli in pezzi più piccoli.
        
        Args:
            text: Testo da tradurre
            source_lang: Codice lingua sorgente
            target_lang: Codice lingua di destinazione
            
        Returns:
            Testo tradotto completo
        """
        # Dividi il testo in paragrafi
        paragraphs = text.split("\n")
        
        translated_paragraphs = []
        current_batch = []
        current_batch_size = 0
        
        # Dimensione massima per batch
        MAX_BATCH_SIZE = 4000
        
        for paragraph in paragraphs:
            # Se il paragrafo è troppo grande, dividilo ulteriormente
            if len(paragraph) > MAX_BATCH_SIZE:
                # Traduci e aggiungi tutti i batch correnti
                if current_batch:
                    batch_text = "\n".join(current_batch)
                    translated_batch = self.translate(batch_text, source_lang, target_lang)
                    translated_paragraphs.append(translated_batch)
                    current_batch = []
                    current_batch_size = 0
                
                # Dividi il paragrafo lungo in frasi
                sentences = re.split(r'(?<=[.!?])\s+', paragraph)
                sentence_batch = []
                sentence_batch_size = 0
                
                for sentence in sentences:
                    if sentence_batch_size + len(sentence) > MAX_BATCH_SIZE:
                        # Traduci batch di frasi
                        sentence_text = " ".join(sentence_batch)
                        translated_sentence_batch = self.translate(sentence_text, source_lang, target_lang)
                        translated_paragraphs.append(translated_sentence_batch)
                        sentence_batch = []
                        sentence_batch_size = 0
                    
                    sentence_batch.append(sentence)
                    sentence_batch_size += len(sentence) + 1  # +1 per lo spazio
                
                # Traduci l'ultimo batch di frasi
                if sentence_batch:
                    sentence_text = " ".join(sentence_batch)
                    translated_sentence_batch = self.translate(sentence_text, source_lang, target_lang)
                    translated_paragraphs.append(translated_sentence_batch)
            
            # Gestisci paragrafi di dimensione normale
            elif current_batch_size + len(paragraph) > MAX_BATCH_SIZE:
                # Traduci batch corrente
                batch_text = "\n".join(current_batch)
                translated_batch = self.translate(batch_text, source_lang, target_lang)
                translated_paragraphs.append(translated_batch)
                
                # Inizia nuovo batch
                current_batch = [paragraph]
                current_batch_size = len(paragraph) + 1  # +1 per il newline
            else:
                # Aggiungi al batch corrente
                current_batch.append(paragraph)
                current_batch_size += len(paragraph) + 1  # +1 per il newline
        
        # Traduci l'ultimo batch
        if current_batch:
            batch_text = "\n".join(current_batch)
            translated_batch = self.translate(batch_text, source_lang, target_lang)
            translated_paragraphs.append(translated_batch)
        
        # Combinare i risultati
        return "\n".join(translated_paragraphs)
    
    def detect_language(self, text: str) -> str:
        """
        Lingva non supporta direttamente il rilevamento della lingua.
        Questa è una implementazione semplificata che rileva lingue comuni.
        
        Args:
            text: Testo da analizzare
            
        Returns:
            Codice lingua rilevato
        """
        # Implementazione semplificata che rileva lingue comuni
        # basata su caratteri frequenti e parole comuni
        
        # Campiona il testo
        sample = text[:1000].lower()
        
        # Caratteri specifici per lingue diverse
        if re.search(r'[а-яА-Я]', sample):
            return "ru"  # Russo (caratteri cirillici)
        elif re.search(r'[一-龯]', sample):
            return "zh"  # Cinese
        elif re.search(r'[ぁ-んァ-ン]', sample):
            return "ja"  # Giapponese
        elif re.search(r'[가-힣]', sample):
            return "ko"  # Coreano
        elif re.search(r'[ا-ي]', sample):
            return "ar"  # Arabo
        
        # Contatori per parole frequenti in lingue europee
        language_scores = {
            "en": 0,  # Inglese
            "it": 0,  # Italiano
            "es": 0,  # Spagnolo
            "fr": 0,  # Francese
            "de": 0,  # Tedesco
            "pt": 0   # Portoghese
        }
        
        # Parole frequenti per ogni lingua
        common_words = {
            "en": ["the", "and", "is", "in", "to", "it", "of", "that", "you", "for"],
            "it": ["il", "la", "di", "e", "che", "è", "un", "per", "non", "sono"],
            "es": ["el", "la", "de", "que", "y", "en", "un", "por", "con", "no"],
            "fr": ["le", "la", "de", "et", "les", "des", "en", "un", "du", "une"],
            "de": ["der", "die", "und", "den", "von", "zu", "das", "mit", "sich", "des"],
            "pt": ["de", "a", "o", "que", "e", "do", "da", "em", "um", "para"]
        }
        
        # Estrai parole
        words = re.findall(r'\b\w+\b', sample)
        
        # Calcola punteggi
        for word in words:
            for lang, word_list in common_words.items():
                if word in word_list:
                    language_scores[lang] += 1
        
        # Trova la lingua con il punteggio più alto
        max_score = 0
        detected_lang = "en"  # Default a inglese
        
        for lang, score in language_scores.items():
            if score > max_score:
                max_score = score
                detected_lang = lang
        
        return detected_lang

class TranslationManager:
    """
    Gestore di traduzione che coordina diversi servizi di traduzione.
    
    Questa classe utilizza vari servizi di traduzione in ordine di priorità
    e gestisce fallback, cache, e altre funzionalità di alto livello.
    """
    
    def __init__(self):
        """Inizializza il gestore di traduzione."""
        self.db = FirebaseManager()
        
        # Carica configurazione
        self.default_source_lang = get_setting("translation.default_source_lang", "en")
        self.default_target_lang = get_setting("translation.default_target_lang", "it")
        self.cache_ttl = get_setting("translation.cache_ttl", 86400 * 7)  # 7 giorni
        self.max_retries = get_setting("translation.max_retries", 3)
        self.retry_delay = get_setting("translation.retry_delay", 1)  # secondi
        
        # Supporto lingue
        self.supported_languages = get_setting("translation.supported_languages", {
            "en": "English",
            "it": "Italiano",
            "es": "Español",
            "fr": "Français",
            "de": "Deutsch",
            "pt": "Português"
        })
        
        # Inizializza traduttori in ordine di priorità
        self.translators = []
        
        # Configura i traduttori in base alle impostazioni
        translator_priority = get_setting("translation.priority", ["libretranslate", "lingva"])
        
        for translator_name in translator_priority:
            if translator_name == "libretranslate":
                self.translators.append(LibreTranslator())
            elif translator_name == "lingva":
                self.translators.append(LingvaTranslator())
        
        # Se non ci sono traduttori configurati, aggiungi quelli di default
        if not self.translators:
            self.translators.append(LibreTranslator())
            self.translators.append(LingvaTranslator())
        
        logger.info(f"TranslationManager inizializzato con {len(self.translators)} traduttori")
    
    def get_available_languages(self) -> Dict[str, str]:
        """
        Ottiene le lingue supportate.
        
        Returns:
            Dizionario con codici lingua come chiavi e nomi come valori
        """
        return self.supported_languages
    
    def translate(self, text: str, source_lang: Optional[str] = None, target_lang: Optional[str] = None,
                 retry: int = 0) -> Dict[str, Any]:
        """
        Traduce il testo utilizzando il primo traduttore disponibile.
        
        Args:
            text: Testo da tradurre
            source_lang: Codice lingua sorgente (opzionale, default a rilevamento automatico)
            target_lang: Codice lingua di destinazione (opzionale, default da configurazione)
            retry: Contatore di tentativi interni (usato per la ricorsione)
            
        Returns:
            Dizionario con risultato della traduzione:
            {
                "text": testo originale,
                "translated": testo tradotto,
                "source_lang": lingua sorgente (rilevata o specificata),
                "target_lang": lingua di destinazione,
                "service": nome del servizio utilizzato
            }
            
        Raises:
            TranslationError: Se non è possibile tradurre il testo
        """
        # Controlla parametri
        if not text or text.strip() == "":
            return {
                "text": "",
                "translated": "",
                "source_lang": source_lang or self.default_source_lang,
                "target_lang": target_lang or self.default_target_lang,
                "service": "none"
            }
        
        try:
            # Imposta lingue di default se non specificate
            if target_lang is None:
                target_lang = self.default_target_lang
            
            # Cerca nella cache
            cache_key = f"translation:{source_lang or 'auto'}:{target_lang}:{hash(text)}"
            cached_result = self.db.get_reference(f"cache/translation/{cache_key}").get()
            
            if cached_result:
                logger.info(f"Traduzione trovata in cache per chiave {cache_key}")
                return cached_result
            
            # Prova ogni traduttore nell'ordine
            last_error = None
            
            for translator in self.translators:
                try:
                    # Verifica disponibilità
                    if not translator.is_available():
                        logger.warning(f"Traduttore {translator.name} non disponibile, provo il prossimo")
                        continue
                    
                    # Rileva la lingua se non specificata
                    actual_source_lang = source_lang
                    if not actual_source_lang or actual_source_lang == "auto":
                        actual_source_lang = translator.detect_language(text)
                        logger.info(f"Lingua rilevata: {actual_source_lang}")
                    
                    # Non tradurre se la lingua sorgente è uguale alla destinazione
                    if actual_source_lang == target_lang:
                        return {
                            "text": text,
                            "translated": text,
                            "source_lang": actual_source_lang,
                            "target_lang": target_lang,
                            "service": translator.name
                        }
                    
                    # Effettua la traduzione
                    translated_text = translator.translate(text, actual_source_lang, target_lang)
                    
                    # Prepara risultato
                    result = {
                        "text": text,
                        "translated": translated_text,
                        "source_lang": actual_source_lang,
                        "target_lang": target_lang,
                        "service": translator.name,
                        "timestamp": time.time()
                    }
                    
                    # Salva in cache
                    self.db.get_reference(f"cache/translation/{cache_key}").set(result)
                    
                    logger.info(f"Traduzione completata usando {translator.name} ({actual_source_lang} -> {target_lang})")
                    return result
                    
                except Exception as e:
                    logger.warning(f"Errore con traduttore {translator.name}: {e}")
                    last_error = e
            
            # Se arriviamo qui, tutti i traduttori hanno fallito
            if retry < self.max_retries:
                logger.info(f"Riprovo traduzione (tentativo {retry+1}/{self.max_retries})")
                time.sleep(self.retry_delay)
                return self.translate(text, source_lang, target_lang, retry + 1)
            
            # Tutti i tentativi falliti
            error_msg = f"Tutti i traduttori hanno fallito: {str(last_error)}"
            logger.error(error_msg)
            raise TranslationError(error_msg)
            
        except Exception as e:
            logger.error(f"Errore durante la traduzione: {e}", exc_info=True)
            raise TranslationError(f"Errore durante la traduzione: {str(e)}")
    
    def batch_translate(self, items: List[Dict[str, Any]], text_key: str, 
                       source_lang: Optional[str] = None, target_lang: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Traduce un batch di elementi.
        
        Args:
            items: Lista di dizionari contenenti il testo da tradurre
            text_key: Chiave del dizionario che contiene il testo
            source_lang: Codice lingua sorgente (opzionale)
            target_lang: Codice lingua di destinazione (opzionale)
            
        Returns:
            Lista di dizionari con un nuovo campo 'translated_text' aggiunto
        """
        results = []
        
        for item in items:
            try:
                if text_key in item and item[text_key]:
                    translation_result = self.translate(
                        item[text_key], source_lang, target_lang
                    )
                    
                    # Aggiungi il testo tradotto all'elemento
                    item_copy = item.copy()
                    item_copy["translated_text"] = translation_result["translated"]
                    item_copy["translation_service"] = translation_result["service"]
                    results.append(item_copy)
                else:
                    # Se il testo non esiste, aggiungi l'elemento originale
                    results.append(item)
            except Exception as e:
                logger.error(f"Errore traducendo elemento: {e}")
                # Aggiungi l'elemento originale in caso di errore
                results.append(item)
        
        return results
    
    def translate_article(self, article_data: Dict[str, Any], target_lang: str) -> Dict[str, Any]:
        """
        Traduce un articolo completo in un'altra lingua.
        
        Args:
            article_data: Dati dell'articolo
            target_lang: Codice lingua di destinazione
            
        Returns:
            Articolo tradotto
        """
        logger.info(f"Traduzione articolo in {target_lang}")
        
        # Ottieni lingua sorgente
        source_lang = article_data.get("language", self.default_source_lang)
        
        # Non tradurre se già nella lingua giusta
        if source_lang == target_lang:
            return article_data
        
        # Crea copia per la traduzione
        translated_article = article_data.copy()
        
        # Traduci i campi di testo principali
        for field in ["title", "excerpt", "content"]:
            if field in article_data and article_data[field]:
                try:
                    translation_result = self.translate(article_data[field], source_lang, target_lang)
                    translated_article[field] = translation_result["translated"]
                except Exception as e:
                    logger.error(f"Errore traducendo campo {field}: {e}")
        
        # Traduci tag se presenti
        if "tags" in article_data and article_data["tags"]:
            try:
                translated_tags = []
                for tag in article_data["tags"]:
                    translation_result = self.translate(tag, source_lang, target_lang)
                    translated_tags.append(translation_result["translated"])
                translated_article["tags"] = translated_tags
            except Exception as e:
                logger.error(f"Errore traducendo tag: {e}")
        
        # Traduzione categorie
        if "categories" in article_data and article_data["categories"]:
            try:
                translated_categories = []
                for category in article_data["categories"]:
                    translation_result = self.translate(category, source_lang, target_lang)
                    translated_categories.append(translation_result["translated"])
                translated_article["categories"] = translated_categories
            except Exception as e:
                logger.error(f"Errore traducendo categorie: {e}")
        
        # Aggiorna metadati
        translated_article["language"] = target_lang
        translated_article["original_language"] = source_lang
        translated_article["translation_timestamp"] = time.time()
        
        logger.info(f"Articolo tradotto con successo in {target_lang}")
        
        return translated_article
    
    def translate_match_prediction(self, prediction_data: Dict[str, Any], target_lang: str) -> Dict[str, Any]:
        """
        Traduce i dati di previsione di una partita.
        
        Args:
            prediction_data: Dati del pronostico
            target_lang: Codice lingua di destinazione
            
        Returns:
            Pronostico tradotto
        """
        logger.info(f"Traduzione pronostico in {target_lang}")
        
        # Ottieni lingua sorgente
        source_lang = prediction_data.get("language", self.default_source_lang)
        
        # Non tradurre se già nella lingua giusta
        if source_lang == target_lang:
            return prediction_data
        
        # Crea copia per la traduzione
        translated_prediction = prediction_data.copy()
        
        # Traduci i campi di testo principali
        text_fields = [
            "description", "summary", "recommendation", "value_bet_description"
        ]
        
        for field in text_fields:
            if field in prediction_data and prediction_data[field]:
                try:
                    translation_result = self.translate(prediction_data[field], source_lang, target_lang)
                    translated_prediction[field] = translation_result["translated"]
                except Exception as e:
                    logger.error(f"Errore traducendo campo {field}: {e}")
        
        # Traduci motivazioni
        if "reasoning" in prediction_data and prediction_data["reasoning"]:
            try:
                translated_reasoning = []
                for reason in prediction_data["reasoning"]:
                    translation_result = self.translate(reason, source_lang, target_lang)
                    translated_reasoning.append(translation_result["translated"])
                translated_prediction["reasoning"] = translated_reasoning
            except Exception as e:
                logger.error(f"Errore traducendo motivazioni: {e}")
        
        # Traduci tendenze
        if "trends" in prediction_data and prediction_data["trends"]:
            try:
                translated_trends = []
                for trend in prediction_data["trends"]:
                    translation_result = self.translate(trend, source_lang, target_lang)
                    translated_trends.append(translation_result["translated"])
                translated_prediction["trends"] = translated_trends
            except Exception as e:
                logger.error(f"Errore traducendo tendenze: {e}")
        
        # Aggiorna metadati
        translated_prediction["language"] = target_lang
        translated_prediction["original_language"] = source_lang
        translated_prediction["translation_timestamp"] = time.time()
        
        logger.info(f"Pronostico tradotto con successo in {target_lang}")
        
        return translated_prediction

# Funzioni di utilità globali
def translate_text(text: str, source_lang: Optional[str] = None, 
                 target_lang: Optional[str] = None) -> Dict[str, Any]:
    """
    Traduce un testo.
    
    Args:
        text: Testo da tradurre
        source_lang: Codice lingua sorgente (opzionale)
        target_lang: Codice lingua di destinazione (opzionale)
        
    Returns:
        Risultato della traduzione con testo originale e tradotto
    """
    translator = TranslationManager()
    return translator.translate(text, source_lang, target_lang)

def get_supported_languages() -> Dict[str, str]:
    """
    Ottiene le lingue supportate.
    
    Returns:
        Dizionario con codici lingua come chiavi e nomi come valori
    """
    translator = TranslationManager()
    return translator.get_available_languages()

def translate_article(article_data: Dict[str, Any], target_lang: str) -> Dict[str, Any]:
    """
    Traduce un articolo completo.
    
    Args:
        article_data: Dati dell'articolo
        target_lang: Codice lingua di destinazione
        
    Returns:
        Articolo tradotto
    """
    translator = TranslationManager()
    return translator.translate_article(article_data, target_lang)

def translate_match_prediction(prediction_data: Dict[str, Any], target_lang: str) -> Dict[str, Any]:
    """
    Traduce i dati di previsione di una partita.
    
    Args:
        prediction_data: Dati del pronostico
        target_lang: Codice lingua di destinazione
        
    Returns:
        Pronostico tradotto
    """
    translator = TranslationManager()
    return translator.translate_match_prediction(prediction_data, target_lang)
