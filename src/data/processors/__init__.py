""" 
Package processors per l'elaborazione e la normalizzazione dei dati calcistici.
Questo package fornisce funzionalità per elaborare, normalizzare e arricchire
i dati raccolti da varie fonti, producendo un formato standardizzato per l'analisi.
"""
from typing import Dict, List, Any, Optional, Union

# Importa le funzioni dai moduli
from src.data.processors.matches import (
    process_match as process_match_data,
    process_matches as process_matches_data,
    enrich_match as enrich_match_data,
    normalize_match as normalize_match_data,
    get_matches_for_date as get_matches_for_date_data,
    get_matches_for_team as get_matches_for_team_data
)

from src.data.processors.teams import (
    process_team as process_team_data,
    process_teams as process_teams_data,
    enrich_team as enrich_team_data,
    normalize_team as normalize_team_data,
    get_team_by_id as get_team_by_id_data,
    get_team_by_name as get_team_by_name_data
)

from src.data.processors.head_to_head import (
    process_head_to_head as process_h2h_data,
    get_head_to_head_stats as get_h2h_stats,
    get_last_meetings as get_last_meetings_data,
    analyze_head_to_head as analyze_h2h_data
)

from src.data.processors.xg_processor import (
    get_xg_data as get_xg_data_func,
    calculate_xg as calculate_xg_func,
    analyze_xg_performance as analyze_xg_performance_func
)

from src.data.processors.standings import (
    get_current_standings as get_current_standings_func,
    get_standings_history as get_standings_history_func,
    calculate_form_table as calculate_form_table_func,
    normalize_standings as normalize_standings_func
)

# Funzioni di utilità generali

def get_filtered_matches(filters: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Ottiene le partite filtrate in base a vari criteri.
    
    Args:
        filters: Dizionario con i criteri di filtro (data, squadra, stato, ecc.)
    
    Returns:
        Lista delle partite filtrate
    """
    # Estrai i filtri più comuni
    date = filters.get("date")
    team_id = filters.get("team_id")
    league_id = filters.get("league_id")
    status = filters.get("status")
    
    # Applica i filtri in sequenza
    matches = []
    
    if date:
        matches = get_matches_for_date_data(date)
    elif team_id:
        matches = get_matches_for_team_data(team_id)
    else:
        # Ottieni tutte le partite e filtra dopo
        from src.data.collector import collect_matches
        matches = collect_matches(league_id=league_id)
    
    # Filtra ulteriormente per stato se specificato
    if status and matches:
        matches = [match for match in matches if match.get("status") == status]
    
    # Filtra per altri criteri specificati
    for key, value in filters.items():
        if key not in ["date", "team_id", "league_id", "status"] and matches:
            matches = [match for match in matches if match.get(key) == value]
    
    return matches

def get_upcoming_matches(days: int = 7, league_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Ottiene le partite imminenti in un determinato intervallo di giorni.
    
    Args:
        days: Numero di giorni futuri da considerare
        league_id: ID del campionato (opzionale)
    
    Returns:
        Lista delle partite imminenti
    """
    from datetime import datetime, timedelta
    
    # Calcola l'intervallo di date
    today = datetime.now()
    end_date = today + timedelta(days=days)
    
    # Ottieni le partite per ogni giorno nell'intervallo
    matches = []
    current_date = today
    
    while current_date <= end_date:
        day_matches = get_matches_for_date_data(current_date.strftime("%Y-%m-%d"))
        
        # Filtra per campionato se specificato
        if league_id and day_matches:
            day_matches = [match for match in day_matches if match.get("league_id") == league_id]
        
        matches.extend(day_matches)
        current_date += timedelta(days=1)
    
    # Ordina per data e ora
    matches.sort(key=lambda x: x.get("datetime", ""))
    
    return matches

def get_recent_matches(days: int = 7, league_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Ottiene le partite recenti in un determinato intervallo di giorni passati.
    
    Args:
        days: Numero di giorni passati da considerare
        league_id: ID del campionato (opzionale)
    
    Returns:
        Lista delle partite recenti
    """
    from datetime import datetime, timedelta
    
    # Calcola l'intervallo di date
    today = datetime.now()
    start_date = today - timedelta(days=days)
    
    # Ottieni le partite per ogni giorno nell'intervallo
    matches = []
    current_date = start_date
    
    while current_date <= today:
        day_matches = get_matches_for_date_data(current_date.strftime("%Y-%m-%d"))
        
        # Filtra per campionato se specificato
        if league_id and day_matches:
            day_matches = [match for match in day_matches if match.get("league_id") == league_id]
        
        # Filtra solo per partite completate
        if day_matches:
            day_matches = [match for match in day_matches if match.get("status") == "FINISHED"]
        
        matches.extend(day_matches)
        current_date += timedelta(days=1)
    
    # Ordina per data e ora (più recenti prima)
    matches.sort(key=lambda x: x.get("datetime", ""), reverse=True)
    
    return matches

def get_team_info(team_identifier: str, by_id: bool = False) -> Dict[str, Any]:
    """
    Ottiene le informazioni complete su una squadra.
    
    Args:
        team_identifier: Nome o ID della squadra
        by_id: Se True, l'identificatore è un ID, altrimenti un nome
    
    Returns:
        Informazioni complete sulla squadra
    """
    if by_id:
        team = get_team_by_id_data(team_identifier)
    else:
        team = get_team_by_name_data(team_identifier)
    
    if team:
        # Arricchisci con informazioni aggiuntive
        return enrich_team_data(team)
    
    return {}

def get_match_by_id(match_id: str) -> Dict[str, Any]:
    """
    Ottiene le informazioni complete su una partita.
    
    Args:
        match_id: ID della partita
    
    Returns:
        Informazioni complete sulla partita
    """
    # Importa funzioni da altri moduli se necessario
    from src.data.collector import get_match_by_id as collector_get_match
    
    match = collector_get_match(match_id)
    
    if match:
        # Arricchisci con informazioni aggiuntive
        return enrich_match_data(match)
    
    return {}

def get_head_to_head(team1_id: str, team2_id: str, limit: int = 10) -> Dict[str, Any]:
    """
    Ottiene le statistiche testa a testa tra due squadre.
    
    Args:
        team1_id: ID della prima squadra
        team2_id: ID della seconda squadra
        limit: Numero massimo di partite da considerare
    
    Returns:
        Statistiche testa a testa
    """
    return get_h2h_stats(team1_id, team2_id, limit)

def get_standings(league_id: str, type: str = "current") -> List[Dict[str, Any]]:
    """
    Ottiene la classifica di un campionato.
    
    Args:
        league_id: ID del campionato
        type: Tipo di classifica ('current' o 'form')
    
    Returns:
        Classifica del campionato
    """
    if type == "current":
        return get_current_standings_func(league_id)
    elif type == "form":
        return calculate_form_table_func(league_id)
    else:
        return []

def analyze_match(match_id: str) -> Dict[str, Any]:
    """
    Esegue un'analisi completa di una partita.
    
    Args:
        match_id: ID della partita
    
    Returns:
        Analisi completa della partita
    """
    # Ottieni i dati di base della partita
    match = get_match_by_id(match_id)
    
    if not match:
        return {"error": "Partita non trovata"}
    
    # Ottieni squadre
    home_team_id = match.get("home_team_id", "")
    away_team_id = match.get("away_team_id", "")
    
    home_team = get_team_info(home_team_id, by_id=True)
    away_team = get_team_info(away_team_id, by_id=True)
    
    # Analisi testa a testa
    head_to_head = get_head_to_head(home_team_id, away_team_id)
    
    # Analisi xG
    xg_data = get_xg_data_func(match_id)
    
    # Classifica attuale
    league_id = match.get("league_id", "")
    standings = get_standings(league_id) if league_id else []
    
    # Componi analisi completa
    analysis = {
        "match": match,
        "home_team": home_team,
        "away_team": away_team,
        "head_to_head": head_to_head,
        "xg_data": xg_data,
        "standings": standings,
        "analysis_time": datetime.now().isoformat()
    }
    
    return analysis

def batch_process_matches(match_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Elabora un batch di partite in parallelo.
    
    Args:
        match_ids: Lista di ID delle partite
    
    Returns:
        Dizionario con i risultati dell'elaborazione per ogni partita
    """
    results = {}
    
    for match_id in match_ids:
        try:
            results[match_id] = process_match_data(match_id)
        except Exception as e:
            results[match_id] = {"error": str(e)}
    
    return results

def batch_process_teams(team_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Elabora un batch di squadre in parallelo.
    
    Args:
        team_ids: Lista di ID delle squadre
    
    Returns:
        Dizionario con i risultati dell'elaborazione per ogni squadra
    """
    results = {}
    
    for team_id in team_ids:
        try:
            results[team_id] = process_team_data(team_id)
        except Exception as e:
            results[team_id] = {"error": str(e)}
    
    return results

# Importa l'oggetto datetime per la funzione analyze_match
from datetime import datetime
