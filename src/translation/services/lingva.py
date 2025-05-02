""" 
Modulo per l'integrazione con Lingva.
Questo modulo fornisce un'implementazione specializzata per il servizio
di traduzione Lingva (https://github.com/TheDavidDelta/lingva-translate).
"""
import os
import time
import json
import logging
import re
import urllib.parse
import requests
from typing import Dict, List, Any, Optional, Union

from src.config.settings import get_setting
from src.utils.cache import cached

logger = logging.getLogger(__name__)

class LingvaError(Exception):
    """Eccezione specifica per errori di Lingva."""
    pass

class LingvaService:
    """
    Servizio di traduzione basato su Lingva.
    
    Lingva è un'alternativa a Google Translate che può essere utilizzata gratuitamente 
    attraverso istanze pubbliche. Non richiede API key e supporta molte lingue.
    """
    
    def __init__(self):
        """Inizializza il servizio Lingva."""
        # Carica configurazioni
        self.api_url = get_setting("translation.lingva.url", 
                                  "https://lingva.ml/api/v1/{source}/{target}/{query}")
        self.timeout = get_setting("translation.lingva.timeout", 15)
        self.max_retries = get_setting("translation.lingva.max_retries", 3)
        self.retry_delay = get_setting("translation.lingva.retry_delay", 1)
        
        # Istanze di fallback
        self.fallback_instances = get_setting("translation.lingva.fallback_instances", [
            {
                "name": "PussTheCat",
                "url": "https://lingva.pussthecat.org/api/v1/{source}/{target}/{query}"
            },
            {
                "name": "LingvaLunar",
                "url": "https://lingva.lunar.icu/api/v1/{source}/{target}/{query}"
            }
        ])
        
        # Coppie di lingue supportate
        # Lingva supporta le stesse lingue di Google Translate
        self.supported_language_pairs = self._create_language_pairs()
        
        logger.info(f"LingvaService inizializzato con URL di base: {self.api_url}")
    
    def _create_language_pairs(self) -> Dict[str, List[str]]:
        """
        Crea un dizionario delle coppie di lingue supportate.
        
        Returns:
            Dizionario con le coppie di lingue supportate
        """
        # Elenco delle lingue supportate da Google Translate
        languages = [
            "af", "am", "ar", "az", "be", "bg", "bn", "bs", "ca", "ceb", "co", "cs", "cy", 
            "da", "de", "el", "en", "eo", "es", "et", "eu", "fa", "fi", "fr", "fy", "ga", 
            "gd", "gl", "gu", "ha", "haw", "he", "hi", "hmn", "hr", "ht", "hu", "hy", "id", 
            "ig", "is", "it", "ja", "jw", "ka", "kk", "km", "kn", "ko", "ku", "ky", "la", 
            "lb", "lo", "lt", "lv", "mg", "mi", "mk", "ml", "mn", "mr", "ms", "mt", "my", 
            "ne", "nl", "no", "ny", "pa", "pl", "ps", "pt", "ro", "ru", "sd", "si", "sk", 
            "sl", "sm", "sn", "so", "sq", "sr", "st", "su", "sv", "sw", "ta", "te", "tg", 
            "th", "tl", "tr", "uk", "ur", "uz", "vi", "xh", "yi", "yo", "zh", "zu"
        ]
        
        # In Lingva, ogni lingua può essere tradotta in qualsiasi altra lingua
        pairs = {}
        for source in languages:
            pairs[source] = [target for target in languages if target != source]
        
        return pairs
    
    def is_language_pair_supported(self, source_lang: str, target_lang: str) -> bool:
        """
        Verifica se una coppia di lingue è supportata.
        
        Args:
            source_lang: Codice lingua sorgente
            target_lang: Codice lingua di destinazione
            
        Returns:
            True se la coppia è supportata, False altrimenti
        """
        if source_lang in self.supported_language_pairs:
            return target_lang in self.supported_language_pairs[source_lang]
        return False
    
    def detect_language(self, text: str) -> str:
        """
        Rileva la lingua del testo. 
        Lingva non ha un endpoint dedicato al rilevamento, quindi usiamo un'implementazione semplificata.
        
        Args:
            text: Testo da analizzare
            
        Returns:
            Codice lingua rilevato
        """
        # Implementazione semplificata che rileva lingue comuni
        # basata su caratteri frequenti e parole comuni
        
        if not text or text.strip() == "":
            return "en"  # Default a inglese per testo vuoto
            
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
            LingvaError: Se non è possibile tradurre il testo
        """
        if not text or text.strip() == "":
            return ""
        
        # Verifica se la coppia di lingue è supportata
        if not self.is_language_pair_supported(source_lang, target_lang):
            raise LingvaError(f"Coppia di lingue non supportata: {source_lang} -> {target_lang}")
        
        # Le API Lingva hanno limiti di lunghezza, dividiamo il testo se necessario
        if len(text) > 5000:
            return self._batch_translate(text, source_lang, target_lang)
        
        # URL encode del testo
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
        
        # Prova con istanze di fallback
        for instance in self.fallback_instances:
            try:
                fallback_url = instance["url"].format(source=source_lang, target=target_lang, query=query)
                logger.info(f"Tentativo con istanza di fallback Lingva: {instance['name']}")
                response = requests.get(fallback_url, timeout=self.timeout)
                
                if response.status_code == 200:
                    result = response.json()
                    if "translation" in result:
                        return result["translation"]
            except Exception as e:
                logger.warning(f"Errore con istanza di fallback {instance['name']}: {e}")
        
        # Riprova in caso di errore
        if retry < self.max_retries:
            time.sleep(self.retry_delay)
            return self.translate(text, source_lang, target_lang, retry + 1)
        
        # Se arriviamo qui, tutti i tentativi sono falliti
        raise LingvaError("Impossibile tradurre il testo con Lingva")
    
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
    
    def is_available(self) -> bool:
        """
        Verifica se il servizio Lingva è disponibile.
        
        Returns:
            True se il servizio è disponibile, False altrimenti
        """
        test_text = "Hello world"
        
        try:
            # Prova a tradurre una semplice frase di test
            result = self.translate(test_text, "en", "it")
            return result != test_text and result != ""
        except Exception as e:
            logger.warning(f"Lingva non disponibile: {e}")
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
            LingvaError: Se non è possibile tradurre i testi
        """
        if not texts:
            return []
        
        # Lingva non supporta traduzioni batch native
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
def get_lingva_service() -> LingvaService:
    """
    Ottiene un'istanza del servizio Lingva.
    
    Returns:
        Istanza del servizio Lingva
    """
    return LingvaService()

def translate(text: str, source_lang: str, target_lang: str) -> str:
    """
    Funzione di utilità per tradurre un testo con Lingva.
    
    Args:
        text: Testo da tradurre
        source_lang: Codice lingua sorgente
        target_lang: Codice lingua di destinazione
        
    Returns:
        Testo tradotto
        
    Raises:
        LingvaError: Se non è possibile tradurre il testo
    """
    service = get_lingva_service()
    return service.translate(text, source_lang, target_lang)

def detect_language(text: str) -> str:
    """
    Funzione di utilità per rilevare la lingua.
    
    Args:
        text: Testo da analizzare
        
    Returns:
        Codice lingua rilevato
    """
    service = get_lingva_service()
    return service.detect_language(text)

def is_available() -> bool:
    """
    Funzione di utilità per verificare se il servizio è disponibile.
    
    Returns:
        True se il servizio è disponibile, False altrimenti
    """
    service = get_lingva_service()
    return service.is_available()
