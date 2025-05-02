"""
Modulo per la traduzione automatica dei contenuti.
Fornisce funzionalità per tradurre articoli e contenuti in diverse lingue
utilizzando servizi di traduzione gratuiti come LibreTranslate e Lingva.
"""

from typing import Dict, List, Any, Optional

from src.translation.translator import (
    translate_text,
    get_supported_languages,
    translate_article,
    translate_match_prediction
)

# Funzioni principali esposte dal modulo
__all__ = [
    'translate_text',
    'get_supported_languages',
    'translate_article',
    'translate_match_prediction',
    'translate_to_language',
    'is_language_supported'
]

def translate_to_language(content: Dict[str, Any], target_language: str) -> Dict[str, Any]:
    """
    Traduce un contenuto nella lingua specificata.
    Funzione di utilità che determina automaticamente il tipo di contenuto
    e utilizza la funzione di traduzione appropriata.
    
    Args:
        content: Dizionario con il contenuto da tradurre
        target_language: Codice lingua di destinazione
        
    Returns:
        Contenuto tradotto
    """
    # Determina il tipo di contenuto
    if 'match_id' in content and 'prediction' in content:
        # È un pronostico di partita
        return translate_match_prediction(content, target_language)
    elif 'title' in content and 'content' in content:
        # È un articolo
        return translate_article(content, target_language)
    else:
        # È un contenuto generico, traduciamo i campi di testo principali
        result = content.copy()
        for key, value in content.items():
            if isinstance(value, str) and len(value) > 3:  # Ignora campi troppo brevi
                translation = translate_text(value, target_lang=target_language)
                result[key] = translation.get('translated', value)
        return result

def is_language_supported(language_code: str) -> bool:
    """
    Verifica se una lingua è supportata dal sistema di traduzione.
    
    Args:
        language_code: Codice lingua da verificare
        
    Returns:
        True se la lingua è supportata, False altrimenti
    """
    supported_languages = get_supported_languages()
    return language_code in supported_languages
