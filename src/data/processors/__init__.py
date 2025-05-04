""" 
Package processors per l'elaborazione e la normalizzazione dei dati calcistici.
Questo package fornisce funzionalità per elaborare, normalizzare e arricchire
i dati raccolti da varie fonti, producendo un formato standardizzato per l'analisi.
"""
from typing import Dict, List, Any, Optional, Union

# Importa le funzioni principali dai moduli
from src.data.processors.matches import get_processor as get_match_processor
from src.data.processors.teams import get_processor as get_team_processor
from src.data.processors.head_to_head import get_processor as get_h2h_processor
from src.data.processors.xg_processor import (
    process_match_xg as get_xg_data_func,
    get_team_xg_history as analyze_xg_performance_func
)
from src.data.processors.standings import (
    process_league_standings as get_current_standings_func,
    get_team_standing as get_standings_history_func,
    get_team_form as calculate_form_table_func
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
        # Usa get_processor per accedere ai metodi della classe
        processor = get_match_processor()
        matches = processor.get_matches_for_date(date)
    elif team_id:
        processor = get_match_processor()
        matches = processor.get_matches_for_team(team_id)
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
    processor = get_match_processor()
    
    while current_date <= end_date:
        day_matches = processor.get_matches_for_date(current_date.strftime("%Y-%m-%d"))
        
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
    processor = get_match_processor()
    
    while current_date <= today:
        day_matches = processor.get_matches_for_date(current_date.strftime("%Y-%m-%d"))
        
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
    processor = get_team_processor()
    
    if by_id:
        team = processor.get_stored_team_data(team_identifier)
    else:
        team = processor.find_team_by_name(team_identifier)
    
    if team:
        # Arricchisci con informazioni aggiuntive
        return processor.enrich_team_data(team)
    
    return {}

def get_match_by_id(match_id: str) -> Dict[str, Any]:
    """
    Ottiene le informazioni complete su una partita.
    
    Args:
        match_id: ID della partita
    
    Returns:
        Informazioni complete sulla partita
    """
    processor = get_match_processor()
    
    match = processor.get_stored_match_data(match_id)
    
    if match:
        # Arricchisci con informazioni aggiuntive
        return processor.enrich_match_data(match)
    
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
    processor = get_h2h_processor()
    return processor.get_head_to_head(team1_id, team2_id, max_matches=limit)

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
        standings_data = get_current_standings_func(league_id)
        return standings_data.get('standings', []) if standings_data else []
    elif type == "form":
        # Questa funzione non esiste nei file condivisi, ritorna lista vuota
        return []
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
    from datetime import datetime
    
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
            results[match_id] = {"error": "Function needs to be adapted"}
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
            results[team_id] = {"error": "Function needs to be adapted"}
        except Exception as e:
            results[team_id] = {"error": str(e)}
    
    return results
