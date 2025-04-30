"""
Utilità per la gestione del tempo e delle date.
Fornisce funzioni per convertire, formattare e manipolare date e orari.
"""
import time
import logging
import pytz
from datetime import datetime, timedelta, timezone
from typing import Optional, Union, Dict, List, Tuple

# Configurazione logging
logger = logging.getLogger(__name__)

# Timezone di default
DEFAULT_TIMEZONE = pytz.timezone('Europe/Rome')  # UTC+1/UTC+2 (ora legale)

def get_current_time(tz: Optional[pytz.timezone] = None) -> datetime:
    """
    Ottiene l'ora corrente nella timezone specificata.
    
    Args:
        tz: Timezone, default UTC
        
    Returns:
        Datetime corrente
    """
    if tz is None:
        tz = pytz.UTC
        
    return datetime.now(tz)

def get_current_timestamp() -> int:
    """
    Ottiene il timestamp unix corrente in secondi.
    
    Returns:
        Timestamp unix in secondi
    """
    return int(time.time())

def timestamp_to_datetime(timestamp: Union[int, float], 
                          tz: Optional[pytz.timezone] = None) -> datetime:
    """
    Converte un timestamp unix in oggetto datetime.
    
    Args:
        timestamp: Timestamp unix in secondi
        tz: Timezone, default UTC
        
    Returns:
        Oggetto datetime
    """
    if tz is None:
        tz = pytz.UTC
        
    return datetime.fromtimestamp(timestamp, tz)

def datetime_to_timestamp(dt: datetime) -> int:
    """
    Converte un oggetto datetime in timestamp unix.
    
    Args:
        dt: Oggetto datetime
        
    Returns:
        Timestamp unix in secondi
    """
    # Ensure the datetime is timezone-aware
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=pytz.UTC)
        
    return int(dt.timestamp())

def format_datetime(dt: datetime, 
                    format_str: str = "%Y-%m-%d %H:%M:%S",
                    tz: Optional[pytz.timezone] = None) -> str:
    """
    Formatta un oggetto datetime in stringa.
    
    Args:
        dt: Oggetto datetime
        format_str: Formato di output
        tz: Timezone, se None usa quella dell'oggetto datetime
        
    Returns:
        Stringa formattata
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=pytz.UTC)
        
    if tz is not None:
        dt = dt.astimezone(tz)
        
    return dt.strftime(format_str)

def parse_datetime(date_str: str, 
                   format_str: str = "%Y-%m-%d %H:%M:%S",
                   tz: Optional[pytz.timezone] = None) -> Optional[datetime]:
    """
    Converte una stringa in oggetto datetime.
    
    Args:
        date_str: Stringa data/ora
        format_str: Formato di input
        tz: Timezone, default UTC
        
    Returns:
        Oggetto datetime o None se errore
    """
    if tz is None:
        tz = pytz.UTC
        
    try:
        dt = datetime.strptime(date_str, format_str)
        return tz.localize(dt) if dt.tzinfo is None else dt.astimezone(tz)
    except Exception as e:
        logger.warning(f"Errore parsing datetime '{date_str}': {str(e)}")
        return None

def get_datetime_range(start_days: int = 0, 
                       end_days: int = 0,
                       tz: Optional[pytz.timezone] = None) -> Tuple[datetime, datetime]:
    """
    Ottiene un intervallo di date relativamente a oggi.
    
    Args:
        start_days: Giorni indietro (negativo) o avanti (positivo) dalla data odierna
        end_days: Giorni indietro (negativo) o avanti (positivo) dalla data odierna
        tz: Timezone, default UTC
        
    Returns:
        Tupla (data inizio, data fine)
    """
    if tz is None:
        tz = pytz.UTC
        
    now = get_current_time(tz)
    start_date = (now + timedelta(days=start_days)).replace(
        hour=0, minute=0, second=0, microsecond=0)
    end_date = (now + timedelta(days=end_days)).replace(
        hour=23, minute=59, second=59, microsecond=999999)
        
    return start_date, end_date

def get_date_range_str(start_days: int = 0, 
                       end_days: int = 0,
                       format_str: str = "%Y-%m-%d",
                       tz: Optional[pytz.timezone] = None) -> Tuple[str, str]:
    """
    Ottiene un intervallo di date come stringhe.
    
    Args:
        start_days: Giorni indietro (negativo) o avanti (positivo) dalla data odierna
        end_days: Giorni indietro (negativo) o avanti (positivo) dalla data odierna
        format_str: Formato di output
        tz: Timezone, default UTC
        
    Returns:
        Tupla (stringa data inizio, stringa data fine)
    """
    start_date, end_date = get_datetime_range(start_days, end_days, tz)
    return format_datetime(start_date, format_str), format_datetime(end_date, format_str)

def get_start_of_day(dt: Optional[datetime] = None, 
                     tz: Optional[pytz.timezone] = None) -> datetime:
    """
    Ottiene l'inizio del giorno (00:00:00).
    
    Args:
        dt: Datetime di riferimento, default now
        tz: Timezone, default UTC
        
    Returns:
        Datetime inizio giorno
    """
    if tz is None:
        tz = pytz.UTC
        
    if dt is None:
        dt = get_current_time(tz)
    elif dt.tzinfo is None:
        dt = tz.localize(dt)
        
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)

def get_end_of_day(dt: Optional[datetime] = None, 
                   tz: Optional[pytz.timezone] = None) -> datetime:
    """
    Ottiene la fine del giorno (23:59:59.999999).
    
    Args:
        dt: Datetime di riferimento, default now
        tz: Timezone, default UTC
        
    Returns:
        Datetime fine giorno
    """
    if tz is None:
        tz = pytz.UTC
        
    if dt is None:
        dt = get_current_time(tz)
    elif dt.tzinfo is None:
        dt = tz.localize(dt)
        
    return dt.replace(hour=23, minute=59, second=59, microsecond=999999)

def get_start_of_week(dt: Optional[datetime] = None, 
                      tz: Optional[pytz.timezone] = None,
                      week_start: int = 0) -> datetime:
    """
    Ottiene l'inizio della settimana corrente.
    
    Args:
        dt: Datetime di riferimento, default now
        tz: Timezone, default UTC
        week_start: Giorno inizio settimana (0=lunedì, 6=domenica)
        
    Returns:
        Datetime inizio settimana
    """
    if tz is None:
        tz = pytz.UTC
        
    if dt is None:
        dt = get_current_time(tz)
    elif dt.tzinfo is None:
        dt = tz.localize(dt)
        
    start = dt - timedelta(days=dt.weekday() - week_start)
    return get_start_of_day(start)

def get_start_of_month(dt: Optional[datetime] = None, 
                       tz: Optional[pytz.timezone] = None) -> datetime:
    """
    Ottiene l'inizio del mese corrente.
    
    Args:
        dt: Datetime di riferimento, default now
        tz: Timezone, default UTC
        
    Returns:
        Datetime inizio mese
    """
    if tz is None:
        tz = pytz.UTC
        
    if dt is None:
        dt = get_current_time(tz)
    elif dt.tzinfo is None:
        dt = tz.localize(dt)
        
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

def get_end_of_month(dt: Optional[datetime] = None, 
                     tz: Optional[pytz.timezone] = None) -> datetime:
    """
    Ottiene la fine del mese corrente.
    
    Args:
        dt: Datetime di riferimento, default now
        tz: Timezone, default UTC
        
    Returns:
        Datetime fine mese
    """
    if tz is None:
        tz = pytz.UTC
        
    if dt is None:
        dt = get_current_time(tz)
    elif dt.tzinfo is None:
        dt = tz.localize(dt)
        
    # Primo giorno del mese successivo meno un microsecondo
    if dt.month == 12:
        next_month = dt.replace(year=dt.year+1, month=1, day=1, 
                                hour=0, minute=0, second=0, microsecond=0)
    else:
        next_month = dt.replace(month=dt.month+1, day=1,
                                hour=0, minute=0, second=0, microsecond=0)
        
    return next_month - timedelta(microseconds=1)

def time_since(dt: datetime, include_seconds: bool = False) -> str:
    """
    Restituisce una stringa che indica il tempo trascorso.
    Esempio: "2 giorni fa", "3 ore fa", "5 minuti fa", "pochi secondi fa"
    
    Args:
        dt: Datetime di riferimento
        include_seconds: Se includere i secondi nel risultato
        
    Returns:
        Stringa tempo trascorso
    """
    now = get_current_time(dt.tzinfo)
    diff = now - dt
    
    seconds = diff.total_seconds()
    
    if seconds < 0:
        return "nel futuro"
        
    intervals = [
        ('anno', 'anni', 60*60*24*365),
        ('mese', 'mesi', 60*60*24*30),
        ('settimana', 'settimane', 60*60*24*7),
        ('giorno', 'giorni', 60*60*24),
        ('ora', 'ore', 60*60),
        ('minuto', 'minuti', 60),
    ]
    
    if include_seconds:
        intervals.append(('secondo', 'secondi', 1))
    
    for singular, plural, count in intervals:
        if seconds >= count:
            n = int(seconds / count)
            if n == 1:
                return f"1 {singular} fa"
            else:
                return f"{n} {plural} fa"
    
    return "pochi secondi fa"

def format_match_time(match_time: datetime, 
                      match_date: Optional[datetime] = None,
                      tz: Optional[pytz.timezone] = DEFAULT_TIMEZONE) -> str:
    """
    Formatta l'ora di una partita in modo appropriato.
    Se la partita è oggi, mostra solo l'ora.
    Se la partita è domani, mostra "Domani" seguito dall'ora.
    Altrimenti mostra la data completa.
    
    Args:
        match_time: Datetime della partita
        match_date: Data di riferimento opzionale (default: oggi)
        tz: Timezone, default Europe/Rome
        
    Returns:
        Stringa formattata
    """
    if match_time.tzinfo is None:
        match_time = pytz.UTC.localize(match_time)
        
    # Converti alla timezone desiderata
    match_time = match_time.astimezone(tz)
    
    # Ottieni oggi e domani nella timezone desiderata
    today = get_start_of_day(get_current_time(tz), tz)
    tomorrow = today + timedelta(days=1)
    
    # Usa la data della partita per confrontare
    match_date = get_start_of_day(match_time, tz)
    
    # Formatta in base alla data
    if match_date.date() == today.date():
        return f"Oggi, {match_time.strftime('%H:%M')}"
    elif match_date.date() == tomorrow.date():
        return f"Domani, {match_time.strftime('%H:%M')}"
    else:
        # Formato esteso per le altre date
        return match_time.strftime("%A %d %B, %H:%M")

def time_until_match(match_time: datetime, 
                     short_format: bool = False) -> str:
    """
    Restituisce una stringa che indica il tempo mancante alla partita.
    
    Args:
        match_time: Datetime della partita
        short_format: Se usare un formato abbreviato
        
    Returns:
        Stringa tempo mancante
    """
    if match_time.tzinfo is None:
        match_time = pytz.UTC.localize(match_time)
        
    now = get_current_time(match_time.tzinfo)
    diff = match_time - now
    
    if diff.total_seconds() < 0:
        return "Partita iniziata" if not short_format else "Iniziata"
        
    days = diff.days
    hours, remainder = divmod(diff.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    if short_format:
        if days > 0:
            return f"{days}g {hours}h"
        elif hours > 0:
            return f"{hours}h {minutes}m"
        else:
            return f"{minutes}m"
    else:
        parts = []
        if days > 0:
            parts.append(f"{days} {'giorno' if days == 1 else 'giorni'}")
        if hours > 0:
            parts.append(f"{hours} {'ora' if hours == 1 else 'ore'}")
        if minutes > 0:
            parts.append(f"{minutes} {'minuto' if minutes == 1 else 'minuti'}")
            
        if not parts:
            return "meno di un minuto"
            
        return ", ".join(parts)

def is_match_soon(match_time: datetime, hours_before: int = 12) -> bool:
    """
    Verifica se una partita inizierà entro un certo numero di ore.
    
    Args:
        match_time: Datetime della partita
        hours_before: Numero di ore prima
        
    Returns:
        True se la partita inizierà entro hours_before ore
    """
    if match_time.tzinfo is None:
        match_time = pytz.UTC.localize(match_time)
        
    now = get_current_time(match_time.tzinfo)
    diff = match_time - now
    
    return 0 <= diff.total_seconds() <= hours_before * 3600

def get_match_status(match_time: datetime, 
                     match_duration: int = 90,
                     extra_time: int = 15) -> str:
    """
    Determina lo stato di una partita (non iniziata, in corso, terminata).
    
    Args:
        match_time: Datetime inizio partita
        match_duration: Durata partita in minuti
        extra_time: Extra time stimato in minuti
        
    Returns:
        Stato partita ('not_started', 'in_progress', 'finished')
    """
    if match_time.tzinfo is None:
        match_time = pytz.UTC.localize(match_time)
        
    now = get_current_time(match_time.tzinfo)
    
    if now < match_time:
        # La partita non è ancora iniziata
        return 'not_started'
        
    # Calcola fine partita stimata (incluso recupero)
    match_end = match_time + timedelta(minutes=match_duration + extra_time)
    
    if now > match_end:
        # La partita è finita
        return 'finished'
        
    # La partita è in corso
    return 'in_progress'

def format_duration(seconds: int) -> str:
    """
    Formatta una durata in secondi in formato leggibile.
    
    Args:
        seconds: Durata in secondi
        
    Returns:
        Stringa formattata
    """
    if seconds < 60:
        return f"{seconds} secondi"
        
    minutes, seconds = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes} min, {seconds} sec"
        
    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        return f"{hours} ore, {minutes} min"
        
    days, hours = divmod(hours, 24)
    return f"{days} giorni, {hours} ore"

def get_publication_window(match_time: datetime, 
                           hours_before: int = 12,
                           hours_after_end: int = 8) -> Tuple[datetime, datetime]:
    """
    Calcola la finestra di pubblicazione per un articolo sulla partita.
    
    Args:
        match_time: Datetime inizio partita
        hours_before: Ore prima della partita per pubblicare
        hours_after_end: Ore dopo fine partita per rimuovere
        
    Returns:
        Tupla (datetime pubblicazione, datetime rimozione)
    """
    if match_time.tzinfo is None:
        match_time = pytz.UTC.localize(match_time)
    
    # Orario di pubblicazione
    publish_time = match_time - timedelta(hours=hours_before)
    
    # Stima fine partita (90 minuti + 15 di recupero)
    match_end = match_time + timedelta(minutes=90 + 15)
    
    # Orario di rimozione
    expire_time = match_end + timedelta(hours=hours_after_end)
    
    return publish_time, expire_time

def should_publish_now(match_time: datetime,
                       hours_before: int = 12) -> bool:
    """
    Verifica se è il momento di pubblicare un articolo sulla partita.
    
    Args:
        match_time: Datetime inizio partita
        hours_before: Ore prima della partita per pubblicare
        
    Returns:
        True se è il momento di pubblicare
    """
    if match_time.tzinfo is None:
        match_time = pytz.UTC.localize(match_time)
        
    publish_time, _ = get_publication_window(match_time, hours_before)
    now = get_current_time(match_time.tzinfo)
    
    # Verifica se siamo dopo il tempo di pubblicazione ma prima dell'inizio partita
    return publish_time <= now < match_time

def should_expire_now(match_time: datetime,
                      hours_before: int = 12,
                      hours_after_end: int = 8) -> bool:
    """
    Verifica se è il momento di rimuovere un articolo sulla partita.
    
    Args:
        match_time: Datetime inizio partita
        hours_before: Ore prima della partita per pubblicare
        hours_after_end: Ore dopo fine partita per rimuovere
        
    Returns:
        True se è il momento di rimuovere
    """
    if match_time.tzinfo is None:
        match_time = pytz.UTC.localize(match_time)
        
    _, expire_time = get_publication_window(
        match_time, hours_before, hours_after_end)
    now = get_current_time(match_time.tzinfo)
    
    # Verifica se siamo dopo il tempo di rimozione
    return now >= expire_time
