"""
Package per i servizi di traduzione.
Fornisce implementazioni specifiche per diversi servizi di traduzione gratuiti.
"""

from typing import Dict, List, Any, Optional, Union

from src.translation.services.libre_translate import (
    get_libre_translate_service,
    translate as libre_translate,
    detect_language as libre_detect_language,
    get_supported_languages as libre_get_supported_languages
)

from src.translation.services.lingva import (
    get_lingva_service,
    translate as lingva_translate,
    detect_language as lingva_detect_language,
    is_available as lingva_is_available
)

# Funzioni principali esposte dal package
__all__ = [
    'get_translation_service',
    'translate',
    'detect_language',
    'get_available_services',
    'get_service_by_name'
]

def get_available_services() -> List[Dict[str, Any]]:
    """
    Ottiene la lista dei servizi di traduzione disponibili.
    
    Returns:
        Lista di dizionari con informazioni sui servizi disponibili
    """
    services = []
    
    # Verifica LibreTranslate
    try:
        libre_service = get_libre_translate_service()
        if libre_service.is_available():
            services.append({
                'name': 'libretranslate',
                'display_name': 'LibreTranslate',
                'languages': len(libre_get_supported_languages()),
                'available': True
            })
        else:
            services.append({
                'name': 'libretranslate',
                'display_name': 'LibreTranslate',
                'available': False
            })
    except Exception:
        services.append({
            'name': 'libretranslate',
            'display_name': 'LibreTranslate',
            'available': False
        })
    
    # Verifica Lingva
    try:
        if lingva_is_available():
            services.append({
                'name': 'lingva',
                'display_name': 'Lingva Translate',
                'languages': 'multiple',  # Lingva supporta tutte le lingue di Google Translate
                'available': True
            })
        else:
            services.append({
                'name': 'lingva',
                'display_name': 'Lingva Translate',
                'available': False
            })
    except Exception:
        services.append({
            'name': 'lingva',
            'display_name': 'Lingva Translate',
            'available': False
        })
    
    return services

def get_service_by_name(service_name: str) -> Any:
    """
    Ottiene un servizio di traduzione specifico per nome.
    
    Args:
        service_name: Nome del servizio ('libretranslate', 'lingva')
        
    Returns:
        Istanza del servizio di traduzione richiesto
        
    Raises:
        ValueError: Se il servizio non è supportato
    """
    if service_name == 'libretranslate':
        return get_libre_translate_service()
    elif service_name == 'lingva':
        return get_lingva_service()
    else:
        raise ValueError(f"Servizio di traduzione '{service_name}' non supportato")

def get_translation_service() -> Any:
    """
    Ottiene il primo servizio di traduzione disponibile.
    
    Returns:
        Istanza del primo servizio di traduzione disponibile
        
    Raises:
        RuntimeError: Se nessun servizio è disponibile
    """
    # Prova prima LibreTranslate
    try:
        libre_service = get_libre_translate_service()
        if libre_service.is_available():
            return libre_service
    except Exception:
        pass
    
    # Prova Lingva
    try:
        lingva_service = get_lingva_service()
        if lingva_is_available():
            return lingva_service
    except Exception:
        pass
    
    # Nessun servizio disponibile
    raise RuntimeError("Nessun servizio di traduzione disponibile")

def translate(text: str, source_lang: str, target_lang: str, 
             service_name: Optional[str] = None) -> str:
    """
    Traduce un testo utilizzando il servizio specificato o il primo disponibile.
    
    Args:
        text: Testo da tradurre
        source_lang: Codice lingua sorgente
        target_lang: Codice lingua di destinazione
        service_name: Nome del servizio da utilizzare (opzionale)
        
    Returns:
        Testo tradotto
        
    Raises:
        RuntimeError: Se nessun servizio è disponibile
    """
    if service_name:
        # Usa il servizio specificato
        service = get_service_by_name(service_name)
        return service.translate(text, source_lang, target_lang)
    else:
        # Prova servizi in ordine
        errors = []
        
        # Prova LibreTranslate
        try:
            return libre_translate(text, source_lang, target_lang)
        except Exception as e:
            errors.append(f"LibreTranslate: {str(e)}")
        
        # Prova Lingva
        try:
            return lingva_translate(text, source_lang, target_lang)
        except Exception as e:
            errors.append(f"Lingva: {str(e)}")
        
        # Tutti i servizi hanno fallito
        raise RuntimeError(f"Impossibile tradurre il testo: {', '.join(errors)}")

def detect_language(text: str, service_name: Optional[str] = None) -> str:
    """
    Rileva la lingua di un testo utilizzando il servizio specificato o il primo disponibile.
    
    Args:
        text: Testo da analizzare
        service_name: Nome del servizio da utilizzare (opzionale)
        
    Returns:
        Codice lingua rilevato
        
    Raises:
        RuntimeError: Se nessun servizio è disponibile
    """
    if service_name:
        # Usa il servizio specificato
        service = get_service_by_name(service_name)
        return service.detect_language(text)
    else:
        # Prova servizi in ordine
        errors = []
        
        # Prova LibreTranslate
        try:
            return libre_detect_language(text)
        except Exception as e:
            errors.append(f"LibreTranslate: {str(e)}")
        
        # Prova Lingva
        try:
            return lingva_detect_language(text)
        except Exception as e:
            errors.append(f"Lingva: {str(e)}")
        
        # Tutti i servizi hanno fallito, fallback a inglese
        return "en"
