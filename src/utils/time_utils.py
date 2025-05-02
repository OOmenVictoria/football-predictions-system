"""
Modulo con utilità per la gestione del tempo e delle date.
Fornisce funzioni per formattare, convertire e manipolare date e timestamp.
"""
import os
import time
import logging
import pytz
from typing import Dict, Any, Optional, Union, List, Tuple
from datetime import datetime, timedelta, date, timezone

# Configurazione logging
logger = logging.getLogger(__name__)

def format_date(dt: Optional[Union[datetime, date, str]] = None, 
                format_str: str = "%Y-%m-%d", 
                locale: str = "it") -> str:
    """
    Formatta una data nel formato specificato.
    
    Args:
        dt: Oggetto datetime, date o stringa ISO. Se None, usa oggi
        format_str: Formato stringa per l'output (default: YYYY-MM-DD)
        locale: Locale per la formattazione (default: italiano)
        
    Returns:
        Stringa formattata
    """
    if dt is None:
        dt = datetime.now()
    
    # Se è una stringa, converti in datetime
    if isinstance(dt, str):
        try:
            dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))
        except ValueError:
            try:
                dt = datetime.strptime(dt, "%Y-%m-%d")
            except ValueError:
                logger.error(f"Formato data non supportato: {dt}")
                return str(dt)
    
    # Formattazione standard
    try:
        if locale.lower() == "it":
            # Formati personalizzati per italiano
            if format_str == "%B":  # Nome mese
                months_it = ["Gennaio", "Febbraio", "Marzo", "Aprile", "Maggio", "Giugno",
                           "Luglio", "Agosto", "Settembre", "Ottobre", "Novembre", "Dicembre"]
                return months_it[dt.month - 1]
            
            if format_str == "%A":  # Nome giorno
                days_it = ["Lunedì", "Martedì", "Mercoledì", "Giovedì", 
                          "Venerdì", "Sabato", "Domenica"]
                return days_it[dt.weekday()]
                
            if format_str == "%d %B %Y":  # 01 Gennaio 2025
                months_it = ["Gennaio", "Febbraio", "Marzo", "Aprile", "Maggio", "Giugno",
                           "Luglio", "Agosto", "Settembre", "Ottobre", "Novembre", "Dicembre"]
                return f"{dt.day:02d} {months_it[dt.month - 1]} {dt.year}"
                
            if format_str == "%A %d %B %Y":  # Lunedì 01 Gennaio 2025
                days_it = ["Lunedì", "Martedì", "Mercoledì", "Giovedì", 
                          "Venerdì", "Sabato", "Domenica"]
                months_it = ["Gennaio", "Febbraio", "Marzo", "Aprile", "Maggio", "Giugno",
                           "Luglio", "Agosto", "Settembre", "Ottobre", "Novembre", "Dicembre"]
                return f"{days_it[dt.weekday()]} {dt.day:02d} {months_it[dt.month - 1]} {dt.year}"
        
        # Formattazione standard per tutti gli altri casi
        return dt.strftime(format_str)
    except Exception as e:
        logger.error(f"Errore formattazione data: {str(e)}")
        return str(dt)

def parse_date(date_str: str, formats: List[str] = None) -> Optional[datetime]:
    """
    Converte una stringa in un oggetto datetime.
    
    Args:
        date_str: Stringa data da convertire
        formats: Lista di formati da provare, se None usa formati predefiniti
        
    Returns:
        Oggetto datetime o None se la conversione fallisce
    """
    if not date_str:
        return None
        
    if formats is None:
        formats = [
            "%Y-%m-%d",            # 2024-05-01
            "%Y-%m-%dT%H:%M:%S",   # 2024-05-01T15:30:00
            "%Y-%m-%dT%H:%M:%SZ",  # 2024-05-01T15:30:00Z
            "%Y-%m-%d %H:%M:%S",   # 2024-05-01 15:30:00
            "%d/%m/%Y",            # 01/05/2024
            "%d/%m/%Y %H:%M",      # 01/05/2024 15:30
            "%d/%m/%Y %H:%M:%S",   # 01/05/2024 15:30:00
            "%d-%m-%Y",            # 01-05-2024
            "%d-%m-%Y %H:%M",      # 01-05-2024 15:30
            "%d-%m-%Y %H:%M:%S",   # 01-05-2024 15:30:00
            "%d %B %Y",            # 01 Maggio 2024
            "%A %d %B %Y"          # Mercoledì 01 Maggio 2024
        ]
    
    # Prova con formato ISO (gestisce automaticamente timezone)
    try:
        return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
    except ValueError:
        pass
    
    # Prova con tutti gli altri formati
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    
    logger.warning(f"Impossibile interpretare formato data: {date_str}")
    return None

def get_time_until(target_date: Union[datetime, str], short: bool = False, locale: str = "it") -> str:
    """
    Calcola e formatta il tempo rimanente fino a una data target.
    
    Args:
        target_date: Data target come datetime o stringa ISO
        short: Se usare formato breve (es. "2g 5h" invece di "2 giorni e 5 ore")
        locale: Locale per la formattazione (default: italiano)
        
    Returns:
        Stringa con tempo rimanente formattato
    """
    # Converti stringa in datetime se necessario
    if isinstance(target_date, str):
        target_date = parse_date(target_date)
        
    if not target_date:
        return "data non valida"
    
    # Assicura che il datetime abbia timezone
    if target_date.tzinfo is None:
        target_date = target_date.replace(tzinfo=timezone.utc)
    
    # Ottieni datetime attuale
    now = datetime.now(timezone.utc)
    
    # Calcola differenza
    diff = target_date - now
    
    # Se la data è nel passato
    if diff.total_seconds() < 0:
        if locale.lower() == "it":
            return "passato" if short else "evento già passato"
        else:
            return "past" if short else "event already passed"
    
    # Calcola componenti
    days = diff.days
    hours, remainder = divmod(diff.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    # Formatta risultato
    if locale.lower() == "it":
        if short:
            result = []
            if days > 0:
                result.append(f"{days}g")
            if hours > 0 or days > 0:
                result.append(f"{hours}h")
            if days == 0:  # Mostra minuti solo se meno di un giorno
                result.append(f"{minutes}m")
            return " ".join(result)
        else:
            if days > 0:
                if hours > 0:
                    return f"{days} {'giorno' if days == 1 else 'giorni'} e {hours} {'ora' if hours == 1 else 'ore'}"
                return f"{days} {'giorno' if days == 1 else 'giorni'}"
            if hours > 0:
                if minutes > 0:
                    return f"{hours} {'ora' if hours == 1 else 'ore'} e {minutes} {'minuto' if minutes == 1 else 'minuti'}"
                return f"{hours} {'ora' if hours == 1 else 'ore'}"
            if minutes > 0:
                if seconds > 0:
                    return f"{minutes} {'minuto' if minutes == 1 else 'minuti'} e {seconds} {'secondo' if seconds == 1 else 'secondi'}"
                return f"{minutes} {'minuto' if minutes == 1 else 'minuti'}"
            return f"{seconds} {'secondo' if seconds == 1 else 'secondi'}"
    else:
        # English format
        if short:
            result = []
            if days > 0:
                result.append(f"{days}d")
            if hours > 0 or days > 0:
                result.append(f"{hours}h")
            if days == 0:  # Mostra minuti solo se meno di un giorno
                result.append(f"{minutes}m")
            return " ".join(result)
        else:
            if days > 0:
                if hours > 0:
                    return f"{days} day{'s' if days != 1 else ''} and {hours} hour{'s' if hours != 1 else ''}"
                return f"{days} day{'s' if days != 1 else ''}"
            if hours > 0:
                if minutes > 0:
                    return f"{hours} hour{'s' if hours != 1 else ''} and {minutes} minute{'s' if minutes != 1 else ''}"
                return f"{hours} hour{'s' if hours != 1 else ''}"
            if minutes > 0:
                if seconds > 0:
                    return f"{minutes} minute{'s' if minutes != 1 else ''} and {seconds} second{'s' if seconds != 1 else ''}"
                return f"{minutes} minute{'s' if minutes != 1 else ''}"
            return f"{seconds} second{'s' if seconds != 1 else ''}"

def get_datetime_now(tz: str = "Europe/Rome") -> datetime:
    """
    Ottiene il datetime attuale con timezone.
    
    Args:
        tz: Timezone (default: Europa/Roma)
        
    Returns:
        Datetime attuale con timezone
    """
    try:
        timezone = pytz.timezone(tz)
        return datetime.now(timezone)
    except Exception as e:
        logger.warning(f"Errore nell'ottenere datetime corrente: {str(e)}")
        return datetime.now(timezone.utc)

def timestamp_to_datetime(timestamp: Union[int, float]) -> datetime:
    """
    Converte un timestamp Unix in datetime.
    
    Args:
        timestamp: Timestamp Unix (secondi da epoch)
        
    Returns:
        Oggetto datetime
    """
    return datetime.fromtimestamp(timestamp, tz=timezone.utc)

def datetime_to_timestamp(dt: datetime) -> int:
    """
    Converte un datetime in timestamp Unix.
    
    Args:
        dt: Oggetto datetime
        
    Returns:
        Timestamp Unix (secondi da epoch)
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    
    return int(dt.timestamp())

def format_timeago(dt: datetime, locale: str = "it") -> str:
    """
    Formatta una data in stile "time ago" (es. "2 ore fa").
    
    Args:
        dt: Datetime da formattare
        locale: Locale per la formattazione (default: italiano)
        
    Returns:
        Stringa "time ago"
    """
    now = datetime.now(timezone.utc)
    
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    
    diff = now - dt
    seconds = diff.total_seconds()
    
    if locale.lower() == "it":
        if seconds < 60:
            return "appena ora"
        if seconds < 3600:
            minutes = int(seconds / 60)
            return f"{minutes} minut{'o' if minutes == 1 else 'i'} fa"
        if seconds < 86400:
            hours = int(seconds / 3600)
            return f"{hours} {'ora' if hours == 1 else 'ore'} fa"
        if seconds < 604800:
            days = int(seconds / 86400)
            return f"{days} {'giorno' if days == 1 else 'giorni'} fa"
        if seconds < 2592000:
            weeks = int(seconds / 604800)
            return f"{weeks} {'settimana' if weeks == 1 else 'settimane'} fa"
        if seconds < 31536000:
            months = int(seconds / 2592000)
            return f"{months} {'mese' if months == 1 else 'mesi'} fa"
        
        years = int(seconds / 31536000)
        return f"{years} {'anno' if years == 1 else 'anni'} fa"
    else:
        # English fallback
        if seconds < 60:
            return "just now"
        if seconds < 3600:
            minutes = int(seconds / 60)
            return f"{minutes} minute{'s' if minutes != 1 else ''} ago"
        if seconds < 86400:
            hours = int(seconds / 3600)
            return f"{hours} hour{'s' if hours != 1 else ''} ago"
        if seconds < 604800:
            days = int(seconds / 86400)
            return f"{days} day{'s' if days != 1 else ''} ago"
        if seconds < 2592000:
            weeks = int(seconds / 604800)
            return f"{weeks} week{'s' if weeks != 1 else ''} ago"
        if seconds < 31536000:
            months = int(seconds / 2592000)
            return f"{months} month{'s' if months != 1 else ''} ago"
        
        years = int(seconds / 31536000)
        return f"{years} year{'s' if years != 1 else ''} ago"

def get_date_range(date_str: str, range_type: str = "week") -> Tuple[datetime, datetime]:
    """
    Ottiene l'intervallo di date basato su una data.
    
    Args:
        date_str: Data di riferimento (YYYY-MM-DD)
        range_type: Tipo di intervallo ('day', 'week', 'month', 'year')
        
    Returns:
        Tupla con data inizio e fine
    """
    dt = parse_date(date_str)
    
    if not dt:
        dt = datetime.now()
    
    if range_type == "day":
        start = datetime(dt.year, dt.month, dt.day, 0, 0, 0)
        end = start + timedelta(days=1) - timedelta(seconds=1)
    elif range_type == "week":
        # Trova inizio settimana (lunedì)
        start = datetime(dt.year, dt.month, dt.day, 0, 0, 0)
        weekday = dt.weekday()
        start = start - timedelta(days=weekday)
        end = start + timedelta(days=7) - timedelta(seconds=1)
    elif range_type == "month":
        start = datetime(dt.year, dt.month, 1, 0, 0, 0)
        if dt.month == 12:
            end = datetime(dt.year + 1, 1, 1, 0, 0, 0) - timedelta(seconds=1)
        else:
            end = datetime(dt.year, dt.month + 1, 1, 0, 0, 0) - timedelta(seconds=1)
    elif range_type == "year":
        start = datetime(dt.year, 1, 1, 0, 0, 0)
        end = datetime(dt.year + 1, 1, 1, 0, 0, 0) - timedelta(seconds=1)
    else:
        # Default: giorno
        start = datetime(dt.year, dt.month, dt.day, 0, 0, 0)
        end = start + timedelta(days=1) - timedelta(seconds=1)
    
    return start, end

def get_current_datetime(tz: str = "Europe/Rome") -> datetime:
    """Alias per compatibilità con codice esistente"""
    return get_datetime_now(tz)
