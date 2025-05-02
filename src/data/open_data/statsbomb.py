""" 
Modulo per l'estrazione di dati da StatsBomb Open Data.
Questo modulo fornisce funzionalità per ottenere dati calcistici dettagliati
dal repository open data di StatsBomb su GitHub (https://github.com/statsbomb/open-data).
"""
import os
import json
import logging
import requests
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union, Tuple

from src.utils.cache import cached
from src.utils.http import make_request, download_file
from src.config.settings import get_setting
from src.utils.database import FirebaseManager

logger = logging.getLogger(__name__)

class StatsBombClient:
    """
    Cliente per l'accesso ai dati open source di StatsBomb.
    
    StatsBomb Open Data è un repository GitHub che fornisce dati dettagliati su eventi
    delle partite per diverse competizioni calcistiche, tra cui la Champions League femminile,
    la Coppa del Mondo FIFA maschile e femminile, e altre competizioni selezionate.
    """
    
    def __init__(self):
        """Inizializza il client StatsBomb."""
        self.base_url = "https://raw.githubusercontent.com/statsbomb/open-data/master/data"
        self.github_api_url = "https://api.github.com/repos/statsbomb/open-data/contents/data"
        self.cache_ttl = get_setting('open_data.statsbomb.cache_ttl', 86400)  # 24 ore
        self.db = FirebaseManager()
        
        logger.info("StatsBombClient inizializzato")
    
    @cached(ttl=86400 * 7)  # Cache di 7 giorni
    def get_competitions(self) -> List[Dict[str, Any]]:
        """
        Ottiene la lista delle competizioni disponibili.
        
        Returns:
            Lista delle competizioni
        """
        url = f"{self.base_url}/competitions.json"
        
        try:
            response = make_request(url)
            if not response:
                logger.error("Impossibile accedere alla lista delle competizioni")
                return []
            
            competitions = response.json()
            logger.info(f"Trovate {len(competitions)} competizioni in StatsBomb Open Data")
            return competitions
            
        except Exception as e:
            logger.error(f"Errore nel recupero delle competizioni: {e}")
            return []
    
    @cached(ttl=86400)  # Cache di 1 giorno
    def get_matches(self, competition_id: int, season_id: int) -> List[Dict[str, Any]]:
        """
        Ottiene la lista delle partite per una competizione e stagione.
        
        Args:
            competition_id: ID della competizione
            season_id: ID della stagione
            
        Returns:
            Lista delle partite
        """
        url = f"{self.base_url}/matches/{competition_id}/{season_id}.json"
        
        try:
            response = make_request(url)
            if not response:
                logger.error(f"Impossibile accedere alle partite per competizione {competition_id}, stagione {season_id}")
                return []
            
            matches = response.json()
            logger.info(f"Trovate {len(matches)} partite per competizione {competition_id}, stagione {season_id}")
            return matches
            
        except Exception as e:
            logger.error(f"Errore nel recupero delle partite: {e}")
            return []
    
    @cached(ttl=86400)  # Cache di 1 giorno
    def get_lineups(self, match_id: int) -> List[Dict[str, Any]]:
        """
        Ottiene le formazioni per una partita.
        
        Args:
            match_id: ID della partita
            
        Returns:
            Lista delle formazioni (una per squadra)
        """
        url = f"{self.base_url}/lineups/{match_id}.json"
        
        try:
            response = make_request(url)
            if not response:
                logger.error(f"Impossibile accedere alle formazioni per la partita {match_id}")
                return []
            
            lineups = response.json()
            logger.info(f"Trovate formazioni per la partita {match_id}")
            return lineups
            
        except Exception as e:
            logger.error(f"Errore nel recupero delle formazioni: {e}")
            return []
    
    @cached(ttl=86400)  # Cache di 1 giorno
    def get_events(self, match_id: int) -> List[Dict[str, Any]]:
        """
        Ottiene gli eventi per una partita.
        
        Args:
            match_id: ID della partita
            
        Returns:
            Lista degli eventi della partita
        """
        url = f"{self.base_url}/events/{match_id}.json"
        
        try:
            response = make_request(url)
            if not response:
                logger.error(f"Impossibile accedere agli eventi per la partita {match_id}")
                return []
            
            events = response.json()
            logger.info(f"Trovati {len(events)} eventi per la partita {match_id}")
            return events
            
        except Exception as e:
            logger.error(f"Errore nel recupero degli eventi: {e}")
            return []
    
    def get_match_details(self, match_id: int) -> Dict[str, Any]:
        """
        Ottiene tutti i dettagli per una partita (inclusi eventi e formazioni).
        
        Args:
            match_id: ID della partita
            
        Returns:
            Dettagli completi della partita
        """
        try:
            # Ottieni formazioni
            lineups = self.get_lineups(match_id)
            
            # Ottieni eventi
            events = self.get_events(match_id)
            
            # Analizza eventi per statistiche di base
            stats = self._analyze_events(events, lineups)
            
            return {
                "match_id": match_id,
                "lineups": lineups,
                "events": events,
                "stats": stats,
                "last_updated": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Errore nel recupero dei dettagli della partita {match_id}: {e}")
            return {}
    
    def _analyze_events(self, events: List[Dict[str, Any]], lineups: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analizza gli eventi di una partita per estrarre statistiche.
        
        Args:
            events: Lista degli eventi
            lineups: Lista delle formazioni
            
        Returns:
            Statistiche estratte
        """
        stats = {
            "home_team": {
                "goals": 0,
                "shots": 0,
                "shots_on_target": 0,
                "passes": 0,
                "pass_accuracy": 0,
                "tackles": 0,
                "fouls": 0,
                "corners": 0,
                "free_kicks": 0,
                "penalties": 0,
                "yellow_cards": 0,
                "red_cards": 0,
                "xg": 0.0
            },
            "away_team": {
                "goals": 0,
                "shots": 0,
                "shots_on_target": 0,
                "passes": 0,
                "pass_accuracy": 0,
                "tackles": 0,
                "fouls": 0,
                "corners": 0,
                "free_kicks": 0,
                "penalties": 0,
                "yellow_cards": 0,
                "red_cards": 0,
                "xg": 0.0
            }
        }
        
        # Identifica gli ID delle squadre
        home_team_id = lineups[0]["team_id"] if len(lineups) > 0 else None
        away_team_id = lineups[1]["team_id"] if len(lineups) > 1 else None
        
        if not home_team_id or not away_team_id:
            logger.warning("Impossibile identificare le squadre dalle formazioni")
            return stats
        
        # Contatori per calcolo accuratezza passaggi
        pass_attempts = {"home": 0, "away": 0}
        pass_completions = {"home": 0, "away": 0}
        
        # Analizza gli eventi
        for event in events:
            team_id = event.get("team", {}).get("id")
            if not team_id:
                continue
                
            team_key = "home_team" if team_id == home_team_id else "away_team"
            event_type = event.get("type", {}).get("name")
            
            if not event_type:
                continue
                
            # Gol
            if event_type == "Shot" and event.get("shot", {}).get("outcome", {}).get("name") == "Goal":
                stats[team_key]["goals"] += 1
            
            # Tiri
            if event_type == "Shot":
                stats[team_key]["shots"] += 1
                
                # Tiri in porta
                outcome = event.get("shot", {}).get("outcome", {}).get("name")
                if outcome in ["Goal", "Saved"]:
                    stats[team_key]["shots_on_target"] += 1
                
                # Expected Goals
                xg = event.get("shot", {}).get("statsbomb_xg", 0)
                if xg:
                    stats[team_key]["xg"] += float(xg)
            
            # Passaggi
            if event_type == "Pass":
                team_short = "home" if team_key == "home_team" else "away"
                pass_attempts[team_short] += 1
                
                # Passaggio completato
                if not event.get("pass", {}).get("outcome"):
                    pass_completions[team_short] += 1
                    stats[team_key]["passes"] += 1
            
            # Tackle
            if event_type == "Duel" and event.get("duel", {}).get("type", {}).get("name") == "Tackle":
                stats[team_key]["tackles"] += 1
            
            # Falli
            if event_type == "Foul Committed":
                stats[team_key]["fouls"] += 1
            
            # Calcio d'angolo
            if event_type == "Pass" and event.get("pass", {}).get("type", {}).get("name") == "Corner":
                stats[team_key]["corners"] += 1
            
            # Calci di punizione
            if event_type == "Pass" and event.get("pass", {}).get("type", {}).get("name") == "Free Kick":
                stats[team_key]["free_kicks"] += 1
            
            # Rigori
            if event_type == "Shot" and event.get("shot", {}).get("type", {}).get("name") == "Penalty":
                stats[team_key]["penalties"] += 1
            
            # Cartellini
            if event_type == "Card":
                card_type = event.get("card", {}).get("type", {}).get("name")
                if card_type == "Yellow Card":
                    stats[team_key]["yellow_cards"] += 1
                elif card_type == "Red Card":
                    stats[team_key]["red_cards"] += 1
        
        # Calcola accuratezza passaggi
        if pass_attempts["home"] > 0:
            stats["home_team"]["pass_accuracy"] = round((pass_completions["home"] / pass_attempts["home"]) * 100, 1)
        if pass_attempts["away"] > 0:
            stats["away_team"]["pass_accuracy"] = round((pass_completions["away"] / pass_attempts["away"]) * 100, 1)
        
        # Arrotonda xG
        stats["home_team"]["xg"] = round(stats["home_team"]["xg"], 2)
        stats["away_team"]["xg"] = round(stats["away_team"]["xg"], 2)
        
        return stats
    
    def get_team_matches(self, team_id: int) -> List[Dict[str, Any]]:
        """
        Ottiene tutte le partite di una squadra.
        
        Args:
            team_id: ID della squadra
            
        Returns:
            Lista delle partite della squadra
        """
        team_matches = []
        
        try:
            # Ottieni tutte le competizioni
            competitions = self.get_competitions()
            
            for competition in competitions:
                competition_id = competition.get("competition_id")
                season_id = competition.get("season_id")
                
                if not competition_id or not season_id:
                    continue
                
                # Ottieni partite per questa competizione/stagione
                matches = self.get_matches(competition_id, season_id)
                
                # Filtra per team_id
                for match in matches:
                    home_id = match.get("home_team", {}).get("home_team_id")
                    away_id = match.get("away_team", {}).get("away_team_id")
                    
                    if home_id == team_id or away_id == team_id:
                        # Aggiungi informazioni sulla competizione
                        match["competition"] = {
                            "id": competition_id,
                            "name": competition.get("competition_name"),
                            "season_id": season_id,
                            "season_name": competition.get("season_name")
                        }
                        team_matches.append(match)
            
            return team_matches
            
        except Exception as e:
            logger.error(f"Errore nel recupero delle partite per la squadra {team_id}: {e}")
            return []
    
    def get_player_events(self, player_id: int, match_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Ottiene gli eventi di un giocatore.
        
        Args:
            player_id: ID del giocatore
            match_id: ID della partita (opzionale)
            
        Returns:
            Lista degli eventi del giocatore
        """
        player_events = []
        
        try:
            if match_id:
                # Se è specificata una partita, ottieni solo gli eventi di quella partita
                events = self.get_events(match_id)
                
                # Filtra per player_id
                for event in events:
                    if event.get("player", {}).get("id") == player_id:
                        player_events.append(event)
            else:
                # Ottieni tutte le competizioni
                competitions = self.get_competitions()
                
                for competition in competitions:
                    competition_id = competition.get("competition_id")
                    season_id = competition.get("season_id")
                    
                    if not competition_id or not season_id:
                        continue
                    
                    # Ottieni partite per questa competizione/stagione
                    matches = self.get_matches(competition_id, season_id)
                    
                    # Limita a un numero ragionevole di partite (max 10)
                    for match in matches[:10]:
                        match_id = match.get("match_id")
                        if not match_id:
                            continue
                        
                        # Ottieni eventi della partita
                        events = self.get_events(match_id)
                        
                        # Filtra per player_id
                        for event in events:
                            if event.get("player", {}).get("id") == player_id:
                                # Aggiungi informazioni sulla partita
                                event["match"] = {
                                    "id": match_id,
                                    "home_team": match.get("home_team", {}).get("home_team_name"),
                                    "away_team": match.get("away_team", {}).get("away_team_name"),
                                    "competition_id": competition_id,
                                    "competition_name": competition.get("competition_name")
                                }
                                player_events.append(event)
            
            return player_events
            
        except Exception as e:
            logger.error(f"Errore nel recupero degli eventi per il giocatore {player_id}: {e}")
            return []
    
    def update_firebase_competition(self, competition_id: int, season_id: int) -> Dict[str, Any]:
        """
        Aggiorna i dati su Firebase per una competizione/stagione.
        
        Args:
            competition_id: ID della competizione
            season_id: ID della stagione
            
        Returns:
            Risultato dell'operazione
        """
        logger.info(f"Aggiornamento dati StatsBomb su Firebase per competizione {competition_id}, stagione {season_id}")
        
        result = {
            "matches": {"count": 0, "error": None},
            "details": {"count": 0, "error": None}
        }
        
        try:
            # Ottieni partite
            matches = self.get_matches(competition_id, season_id)
            if not matches:
                logger.warning(f"Nessuna partita trovata per competizione {competition_id}, stagione {season_id}")
                result["matches"]["error"] = "Nessuna partita trovata"
                return result
            
            # Salva le partite su Firebase
            matches_ref = self.db.get_reference(f"open_data/statsbomb/competitions/{competition_id}/seasons/{season_id}/matches")
            matches_ref.set({str(match.get("match_id", i)): match for i, match in enumerate(matches)})
            result["matches"]["count"] = len(matches)
            
            # Limita il numero di partite per i dettagli (per evitare troppe richieste)
            max_details = get_setting('open_data.statsbomb.max_details', 5)
            matches_to_process = matches[:max_details]
            
            # Ottieni dettagli per ogni partita
            for match in matches_to_process:
                match_id = match.get("match_id")
                if not match_id:
                    continue
                
                try:
                    logger.info(f"Elaborazione dettagli partita {match_id}")
                    
                    # Ottieni dettagli
                    details = self.get_match_details(match_id)
                    if not details:
                        logger.warning(f"Nessun dettaglio trovato per partita {match_id}")
                        continue
                    
                    # Salva su Firebase (senza eventi completi per risparmiare spazio)
                    details_without_events = {k: v for k, v in details.items() if k != 'events'}
                    details_without_events["events_count"] = len(details.get("events", []))
                    
                    match_ref = self.db.get_reference(f"open_data/statsbomb/matches/{match_id}")
                    match_ref.set(details_without_events)
                    
                    result["details"]["count"] += 1
                    
                    # Pausa per evitare troppo carico
                    time.sleep(1)
                    
                except Exception as e:
                    logger.error(f"Errore nell'elaborazione dei dettagli della partita {match_id}: {e}")
            
            # Aggiorna il timestamp
            meta_ref = self.db.get_reference(f"open_data/statsbomb/competitions/{competition_id}/seasons/{season_id}/meta")
            meta_ref.set({
                "last_update": datetime.now().isoformat(),
                "source": "statsbomb",
                "competition_id": competition_id,
                "season_id": season_id
            })
            
            logger.info(f"Dati StatsBomb aggiornati con successo: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Errore nell'aggiornamento dei dati StatsBomb: {e}")
            for key in result:
                if result[key]["error"] is None:
                    result[key]["error"] = str(e)
            return result
    
    def update_firebase_competitions(self) -> Dict[str, Dict[str, Any]]:
        """
        Aggiorna i dati su Firebase per tutte le competizioni.
        
        Returns:
            Risultato dell'operazione per competizione
        """
        results = {}
        
        try:
            # Ottieni le competizioni
            competitions = self.get_competitions()
            
            # Salva meta-informazioni delle competizioni
            competitions_ref = self.db.get_reference("open_data/statsbomb/competitions_meta")
            competitions_ref.set({i: comp for i, comp in enumerate(competitions)})
            
            # Limita il numero di competizioni da elaborare
            max_competitions = get_setting('open_data.statsbomb.max_competitions', 3)
            competitions_to_process = competitions[:max_competitions]
            
            for competition in competitions_to_process:
                competition_id = competition.get("competition_id")
                season_id = competition.get("season_id")
                
                if not competition_id or not season_id:
                    continue
                
                competition_name = competition.get("competition_name", "Unknown")
                season_name = competition.get("season_name", "Unknown")
                
                logger.info(f"Aggiornamento competizione: {competition_name} {season_name}")
                
                key = f"{competition_id}_{season_id}"
                try:
                    results[key] = self.update_firebase_competition(competition_id, season_id)
                    
                    # Pausa tra competizioni
                    time.sleep(3)
                    
                except Exception as e:
                    logger.error(f"Errore nell'aggiornamento della competizione {competition_name}: {e}")
                    results[key] = {"error": str(e)}
        
        except Exception as e:
            logger.error(f"Errore nell'aggiornamento delle competizioni: {e}")
            results["general"] = {"error": str(e)}
        
        return results

# Funzioni di utilità globali
def get_statsbomb_client() -> StatsBombClient:
    """
    Ottiene un'istanza del client StatsBomb.
    
    Returns:
        Istanza di StatsBombClient
    """
    return StatsBombClient()

def get_competitions() -> List[Dict[str, Any]]:
    """
    Ottiene la lista delle competizioni disponibili.
    
    Returns:
        Lista delle competizioni
    """
    client = get_statsbomb_client()
    return client.get_competitions()

def get_matches(competition_id: int, season_id: int) -> List[Dict[str, Any]]:
    """
    Ottiene la lista delle partite per una competizione e stagione.
    
    Args:
        competition_id: ID della competizione
        season_id: ID della stagione
        
    Returns:
        Lista delle partite
    """
    client = get_statsbomb_client()
    return client.get_matches(competition_id, season_id)

def get_match_details(match_id: int) -> Dict[str, Any]:
    """
    Ottiene tutti i dettagli per una partita.
    
    Args:
        match_id: ID della partita
        
    Returns:
        Dettagli completi della partita
    """
    client = get_statsbomb_client()
    return client.get_match_details(match_id)

def get_team_matches(team_id: int) -> List[Dict[str, Any]]:
    """
    Ottiene tutte le partite di una squadra.
    
    Args:
        team_id: ID della squadra
        
    Returns:
        Lista delle partite della squadra
    """
    client = get_statsbomb_client()
    return client.get_team_matches(team_id)

def get_player_events(player_id: int, match_id: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Ottiene gli eventi di un giocatore.
    
    Args:
        player_id: ID del giocatore
        match_id: ID della partita (opzionale)
        
    Returns:
        Lista degli eventi del giocatore
    """
    client = get_statsbomb_client()
    return client.get_player_events(player_id, match_id)

def update_competition_data(competition_id: int, season_id: int) -> Dict[str, Any]:
    """
    Aggiorna i dati per una competizione specifica.
    
    Args:
        competition_id: ID della competizione
        season_id: ID della stagione
        
    Returns:
        Risultato dell'operazione
    """
    client = get_statsbomb_client()
    return client.update_firebase_competition(competition_id, season_id)

def update_all_competitions() -> Dict[str, Dict[str, Any]]:
    """
    Aggiorna i dati per tutte le competizioni disponibili.
    
    Returns:
        Risultato dell'operazione
    """
    client = get_statsbomb_client()
    return client.update_firebase_competitions()
