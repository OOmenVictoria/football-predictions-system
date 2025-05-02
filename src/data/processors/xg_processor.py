"""
Processore per i dati Expected Goals (xG).
Questo modulo fornisce funzionalità per elaborare, normalizzare e arricchire
i dati Expected Goals (xG) provenienti da varie fonti.
"""
import os
import sys
import time
import logging
import math
from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime, timedelta

from src.utils.cache import cached
from src.utils.database import FirebaseManager
from src.config.settings import get_setting
from src.data.stats.understat import get_match_xg as get_understat_xg
from src.data.stats.fbref import get_match_xg as get_fbref_xg
from src.data.stats.sofascore import get_match_xg as get_sofascore_xg
from src.data.stats.whoscored import get_match_statistics

logger = logging.getLogger(__name__)

class XGProcessor:
    """
    Processore per normalizzare e arricchire i dati Expected Goals (xG).
    
    Gestisce la standardizzazione dei dati xG da diverse fonti, risolve
    i conflitti, e arricchisce i dati con statistiche aggiuntive.
    """
    
    def __init__(self):
        """Inizializza il processore xG."""
        self.db = FirebaseManager()
        self.xg_sources = get_setting('processors.xg.sources', 
                                    ['understat', 'fbref', 'sofascore', 'whoscored'])
        self.min_required_sources = get_setting('processors.xg.min_required_sources', 1)
        self.confidence_threshold = get_setting('processors.xg.confidence_threshold', 0.6)
        
        # Pesi relativi per le fonti (usati per la media ponderata)
        self.source_weights = {
            'understat': get_setting('processors.xg.weights.understat', 1.0),
            'fbref': get_setting('processors.xg.weights.fbref', 0.9),
            'sofascore': get_setting('processors.xg.weights.sofascore', 0.8),
            'whoscored': get_setting('processors.xg.weights.whoscored', 0.7)
        }
        
        logger.info(f"XGProcessor inizializzato con {len(self.xg_sources)} fonti")
    
    @cached(ttl=3600 * 12)  # 12 ore
    def process_match_xg(self, match_id: str, force_update: bool = False) -> Dict[str, Any]:
        """
        Processa i dati xG per una partita, normalizzandoli e arricchendoli.
        
        Args:
            match_id: ID della partita
            force_update: Forza aggiornamento anche se dati recenti
            
        Returns:
            Dati xG elaborati e normalizzati
        """
        logger.info(f"Elaborazione xG per match_id={match_id}")
        
        # Recupera dati partita di base
        match_data = self.db.get_reference(f"data/matches/{match_id}").get()
        
        if not match_data:
            logger.warning(f"Partita {match_id} non trovata nel database")
            return {}
        
        # Verifica se abbiamo già dati xG nel database
        if 'xg' in match_data and not force_update:
            xg_data = match_data.get('xg', {})
            
            if xg_data and 'confidence' in xg_data and xg_data.get('confidence', 0) >= self.confidence_threshold:
                logger.info(f"Usando dati xG esistenti con confidenza {xg_data.get('confidence')}")
                return xg_data
        
        # Ottieni dati xG da varie fonti
        all_xg_data = self._collect_xg_from_sources(match_id, match_data)
        
        if not all_xg_data:
            logger.warning(f"Nessun dato xG disponibile per match_id={match_id}")
            return {}
        
        # Estrai i riferimenti alle squadre
        home_team_id = match_data.get('home_team_id', '')
        away_team_id = match_data.get('away_team_id', '')
        home_team = match_data.get('home_team', '')
        away_team = match_data.get('away_team', '')
        
        # Normalizza e combina i dati
        combined_xg = self._combine_xg_data(all_xg_data, home_team_id, away_team_id)
        
        # Se non abbiamo abbastanza fonti o confidenza troppo bassa, ritorna vuoto
        if (len(combined_xg.get('sources', [])) < self.min_required_sources or 
            combined_xg.get('confidence', 0) < self.confidence_threshold):
            logger.warning(f"Dati xG non sufficientemente affidabili: {len(combined_xg.get('sources', []))} fonti, " 
                         f"confidenza {combined_xg.get('confidence', 0)}")
            return {}
        
        # Aggiorna i dati della partita con i nuovi xG
        match_ref = self.db.get_reference(f"data/matches/{match_id}")
        match_ref.child('xg').set(combined_xg)
        match_ref.child('xg_sources').set(combined_xg.get('sources', []))
        match_ref.child('xg_last_updated').set(datetime.now().isoformat())
        
        logger.info(f"Dati xG elaborati e salvati per match_id={match_id}: " 
                  f"{combined_xg.get('home')} - {combined_xg.get('away')}")
        
        return combined_xg
    
    def _collect_xg_from_sources(self, match_id: str, 
                               match_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Raccoglie dati xG da varie fonti.
        
        Args:
            match_id: ID della partita
            match_data: Dati base della partita
            
        Returns:
            Dati xG da varie fonti
        """
        all_xg_data = {}
        
        # Estrai riferimenti squadre
        home_team_id = match_data.get('home_team_id', '')
        away_team_id = match_data.get('away_team_id', '')
        
        # Prova ogni fonte configurata
        for source in self.xg_sources:
            source_xg = self._get_xg_from_source(source, match_id, match_data)
            
            if source_xg and 'home' in source_xg and 'away' in source_xg:
                # Valori validi, aggiungi alla raccolta
                all_xg_data[source] = source_xg
                logger.info(f"Ottenuti xG da {source}: {source_xg.get('home')} - {source_xg.get('away')}")
        
        return all_xg_data
    
    def _get_xg_from_source(self, source: str, match_id: str, 
                          match_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Ottiene dati xG da una fonte specifica.
        
        Args:
            source: Nome della fonte
            match_id: ID della partita
            match_data: Dati base della partita
            
        Returns:
            Dati xG dalla fonte specificata
        """
        try:
            source_data = {}
            
            # Chiamata alla funzione appropriata in base alla fonte
            if source == 'understat':
                source_data = get_understat_xg(match_id)
            elif source == 'fbref':
                source_data = get_fbref_xg(match_id)
            elif source == 'sofascore':
                source_data = get_sofascore_xg(match_id)
            elif source == 'whoscored':
                stats = get_match_statistics(match_id)
                if stats and 'xg' in stats:
                    source_data = stats['xg']
            else:
                logger.warning(f"Fonte xG non supportata: {source}")
                return {}
            
            # Verifica validità dei dati
            if not source_data or 'home' not in source_data or 'away' not in source_data:
                logger.warning(f"Dati xG non validi da {source} per match_id={match_id}")
                return {}
            
            # Verifica che i valori siano numeri positivi
            home_xg = source_data.get('home', 0)
            away_xg = source_data.get('away', 0)
            
            if not isinstance(home_xg, (int, float)) or not isinstance(away_xg, (int, float)):
                logger.warning(f"Valori xG non numerici da {source}: {home_xg} - {away_xg}")
                return {}
            
            if home_xg < 0 or away_xg < 0:
                logger.warning(f"Valori xG negativi da {source}: {home_xg} - {away_xg}")
                return {}
            
            # Aggiungi metadati alla fonte
            source_data['source'] = source
            source_data['timestamp'] = datetime.now().isoformat()
            
            return source_data
            
        except Exception as e:
            logger.error(f"Errore nel recupero xG da {source}: {str(e)}")
            return {}
    
    def _combine_xg_data(self, all_xg_data: Dict[str, Dict[str, Any]], 
                       home_team_id: str, away_team_id: str) -> Dict[str, Any]:
        """
        Combina e normalizza i dati xG da diverse fonti.
        
        Args:
            all_xg_data: Dati xG da varie fonti
            home_team_id: ID squadra casa
            away_team_id: ID squadra trasferta
            
        Returns:
            Dati xG combinati con confidenza
        """
        if not all_xg_data:
            return {}
        
        # Prepara valori aggregati
        home_values = []
        away_values = []
        total_weight = 0
        
        sources = []
        
        # Raccogli valori ponderati
        for source, data in all_xg_data.items():
            # Ottieni peso per questa fonte
            weight = self.source_weights.get(source, 0.5)
            
            home_xg = data.get('home', 0)
            away_xg = data.get('away', 0)
            
            # Aggiungi alla lista valori
            home_values.append((home_xg, weight))
            away_values.append((away_xg, weight))
            
            total_weight += weight
            sources.append(source)
        
        # Calcola media ponderata
        home_xg = sum(val * weight for val, weight in home_values) / total_weight if total_weight > 0 else 0
        away_xg = sum(val * weight for val, weight in away_values) / total_weight if total_weight > 0 else 0
        
        # Calcola varianza (per determinare la confidenza)
        if len(home_values) > 1:
            home_variance = sum(weight * ((val - home_xg) ** 2) for val, weight in home_values) / total_weight
            away_variance = sum(weight * ((val - away_xg) ** 2) for val, weight in away_values) / total_weight
            
            # Deviazione standard
            home_std = math.sqrt(home_variance)
            away_std = math.sqrt(away_variance)
            
            # Coefficiente di variazione
            cv_home = home_std / home_xg if home_xg > 0 else 0
            cv_away = away_std / away_xg if away_xg > 0 else 0
            
            # Media dei coefficienti di variazione (inversa = consistenza)
            avg_cv = (cv_home + cv_away) / 2
            
            # Confidenza basata sulla consistenza delle fonti e numero di fonti
            consistency = 1 - min(avg_cv, 1)  # 0 = inconstistente, 1 = consistente
            sources_factor = min(len(sources) / 3, 1)  # 0.33 = 1 fonte, 0.67 = 2 fonti, 1 = 3+ fonti
            
            confidence = (consistency * 0.7) + (sources_factor * 0.3)
        else:
            # Con una sola fonte, confidenza basata solo sul numero di fonti
            confidence = min(len(sources) / 3, 1) * 0.7  # max 0.7 con una sola fonte
        
        # Risultato combinato
        combined_result = {
            'home': round(home_xg, 2),
            'away': round(away_xg, 2),
            'total': round(home_xg + away_xg, 2),
            'difference': round(home_xg - away_xg, 2),
            'sources': sources,
            'sources_count': len(sources),
            'confidence': round(confidence, 2),
            'last_updated': datetime.now().isoformat()
        }
        
        return combined_result
    
    @cached(ttl=3600 * 24)  # 24 ore
    def process_team_xg_history(self, team_id: str, 
                              matches_limit: int = 10) -> Dict[str, Any]:
        """
        Processa la storia xG di una squadra.
        
        Args:
            team_id: ID della squadra
            matches_limit: Numero massimo di partite da considerare
            
        Returns:
            Statistiche xG storiche della squadra
        """
        logger.info(f"Elaborazione storia xG per team_id={team_id}")
        
        # Ottieni le ultime partite della squadra
        matches_ref = self.db.get_reference("data/matches")
        
        # Query partite in casa
        home_query = matches_ref.order_by_child("home_team_id").equal_to(team_id)
        home_matches = home_query.get() or {}
        
        # Query partite in trasferta
        away_query = matches_ref.order_by_child("away_team_id").equal_to(team_id)
        away_matches = away_query.get() or {}
        
        # Unisci risultati
        all_matches = {**home_matches, **away_matches}
        
        if not all_matches:
            logger.warning(f"Nessuna partita trovata per team_id={team_id}")
            return {}
        
        # Filtra partite con dati xG
        matches_with_xg = []
        for match_id, match in all_matches.items():
            # Controlla se la partita ha dati xG
            if 'xg' in match and match['xg']:
                # Aggiungi metadati
                match['match_id'] = match_id
                match['is_home'] = match.get('home_team_id') == team_id
                match['team_xg'] = match['xg']['home'] if match['is_home'] else match['xg']['away']
                match['opponent_xg'] = match['xg']['away'] if match['is_home'] else match['xg']['home']
                
                # Aggiungi solo se ha una data valida
                if 'datetime' in match:
                    try:
                        match['date_obj'] = datetime.fromisoformat(match['datetime'].replace('Z', '+00:00'))
                        matches_with_xg.append(match)
                    except:
                        pass
        
        # Ordina per data (più recenti prima)
        matches_with_xg.sort(key=lambda x: x.get('date_obj', datetime.min), reverse=True)
        
        # Limita al numero richiesto
        matches_with_xg = matches_with_xg[:matches_limit]
        
        if not matches_with_xg:
            logger.warning(f"Nessuna partita con dati xG trovata per team_id={team_id}")
            return {}
        
        # Calcola statistiche xG
        return self._calculate_team_xg_stats(team_id, matches_with_xg)
    
    def _calculate_team_xg_stats(self, team_id: str, 
                               matches: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Calcola statistiche xG per una squadra.
        
        Args:
            team_id: ID della squadra
            matches: Lista di partite con dati xG
            
        Returns:
            Statistiche xG della squadra
        """
        # Inizializza contatori
        total_own_xg = 0
        total_opponent_xg = 0
        home_own_xg = 0
        home_opponent_xg = 0
        away_own_xg = 0
        away_opponent_xg = 0
        total_home_matches = 0
        total_away_matches = 0
        
        # xG per partita specifica
        match_details = []
        
        # Processa ogni partita
        for match in matches:
            is_home = match.get('is_home', False)
            team_xg = match.get('team_xg', 0)
            opponent_xg = match.get('opponent_xg', 0)
            
            # Totali generali
            total_own_xg += team_xg
            total_opponent_xg += opponent_xg
            
            # Totali casa/trasferta
            if is_home:
                home_own_xg += team_xg
                home_opponent_xg += opponent_xg
                total_home_matches += 1
            else:
                away_own_xg += team_xg
                away_opponent_xg += opponent_xg
                total_away_matches += 1
            
            # Aggiungi dettagli partita
            match_detail = {
                'match_id': match.get('match_id', ''),
                'date': match.get('datetime', ''),
                'home_team': match.get('home_team', ''),
                'away_team': match.get('away_team', ''),
                'is_home': is_home,
                'team_xg': team_xg,
                'opponent_xg': opponent_xg,
                'xg_difference': team_xg - opponent_xg
            }
            
            # Aggiungi risultato reale se disponibile
            if 'home_score' in match and 'away_score' in match:
                team_score = match['home_score'] if is_home else match['away_score']
                opponent_score = match['away_score'] if is_home else match['home_score']
                
                match_detail['team_score'] = team_score
                match_detail['opponent_score'] = opponent_score
                match_detail['score_difference'] = team_score - opponent_score
                
                # Calcola differenza tra xG e gol realizzati
                match_detail['xg_overperformance'] = team_score - team_xg
            
            match_details.append(match_detail)
        
        # Calcola medie
        total_matches = len(matches)
        
        # Statistiche complessive
        avg_own_xg = total_own_xg / total_matches if total_matches > 0 else 0
        avg_opponent_xg = total_opponent_xg / total_matches if total_matches > 0 else 0
        
        # Statistiche casa
        avg_home_own_xg = home_own_xg / total_home_matches if total_home_matches > 0 else 0
        avg_home_opponent_xg = home_opponent_xg / total_home_matches if total_home_matches > 0 else 0
        
        # Statistiche trasferta
        avg_away_own_xg = away_own_xg / total_away_matches if total_away_matches > 0 else 0
        avg_away_opponent_xg = away_opponent_xg / total_away_matches if total_away_matches > 0 else 0
        
        # Risultato complessivo
        result = {
            'team_id': team_id,
            'matches_analyzed': total_matches,
            'home_matches': total_home_matches,
            'away_matches': total_away_matches,
            'overall': {
                'xg_for': round(avg_own_xg, 2),
                'xg_against': round(avg_opponent_xg, 2),
                'xg_difference': round(avg_own_xg - avg_opponent_xg, 2)
            },
            'home': {
                'xg_for': round(avg_home_own_xg, 2),
                'xg_against': round(avg_home_opponent_xg, 2),
                'xg_difference': round(avg_home_own_xg - avg_home_opponent_xg, 2)
            },
            'away': {
                'xg_for': round(avg_away_own_xg, 2),
                'xg_against': round(avg_away_opponent_xg, 2),
                'xg_difference': round(avg_away_own_xg - avg_away_opponent_xg, 2)
            },
            'match_details': match_details,
            'last_updated': datetime.now().isoformat()
        }
        
        return result
    
    @cached(ttl=3600 * 12)  # 12 ore
    def calculate_match_xg_prediction(self, match_id: str) -> Dict[str, Any]:
        """
        Calcola una previsione xG per una partita futura.
        
        Args:
            match_id: ID della partita
            
        Returns:
            Previsione xG per la partita
        """
        logger.info(f"Calcolo previsione xG per match_id={match_id}")
        
        # Recupera dati partita di base
        match_data = self.db.get_reference(f"data/matches/{match_id}").get()
        
        if not match_data:
            logger.warning(f"Partita {match_id} non trovata nel database")
            return {}
        
        # Estrai riferimenti squadre
        home_team_id = match_data.get('home_team_id', '')
        away_team_id = match_data.get('away_team_id', '')
        
        if not home_team_id or not away_team_id:
            logger.warning(f"Dati squadre mancanti per match_id={match_id}")
            return {}
        
        # Ottieni storico xG per entrambe le squadre
        home_xg_history = self.process_team_xg_history(home_team_id)
        away_xg_history = self.process_team_xg_history(away_team_id)
        
        if not home_xg_history or not away_xg_history:
            logger.warning("Dati storici xG insufficienti per previsione")
            return {}
        
        # Calcola valori previsti
        predicted_home_xg = (home_xg_history['home']['xg_for'] + away_xg_history['away']['xg_against']) / 2
        predicted_away_xg = (away_xg_history['away']['xg_for'] + home_xg_history['home']['xg_against']) / 2
        
        # Verifica campo neutro
        is_neutral = match_data.get('is_neutral', False)
        if is_neutral:
            # Media tra casa e trasferta per entrambe le squadre
            home_xg_neutral = (home_xg_history['home']['xg_for'] + home_xg_history['away']['xg_for']) / 2
            away_xg_neutral = (away_xg_history['home']['xg_for'] + away_xg_history['away']['xg_for']) / 2
            
            predicted_home_xg = home_xg_neutral
            predicted_away_xg = away_xg_neutral
        
        # Calcola predizioni aggiuntive
        total_xg = predicted_home_xg + predicted_away_xg
        btts_prob = self._calculate_btts_probability(predicted_home_xg, predicted_away_xg)
        over_under = self._calculate_over_under_probabilities(total_xg)
        
        # Risultato
        prediction = {
            'match_id': match_id,
            'home_team_id': home_team_id,
            'away_team_id': away_team_id,
            'predicted_xg': {
                'home': round(predicted_home_xg, 2),
                'away': round(predicted_away_xg, 2),
                'total': round(total_xg, 2),
                'difference': round(predicted_home_xg - predicted_away_xg, 2)
            },
            'probabilities': {
                'btts': round(btts_prob, 2),
                'over_under': {k: round(v, 2) for k, v in over_under.items()}
            },
            'context': {
                'home_xg_history': {
                    'home': home_xg_history['home'],
                    'overall': home_xg_history['overall']
                },
                'away_xg_history': {
                    'away': away_xg_history['away'],
                    'overall': away_xg_history['overall']
                }
            },
            'is_neutral': is_neutral,
            'last_updated': datetime.now().isoformat()
        }
        
        return prediction
    
    def _calculate_btts_probability(self, home_xg: float, away_xg: float) -> float:
        """
        Calcola la probabilità di Both Teams To Score.
        
        Args:
            home_xg: xG previsto squadra casa
            away_xg: xG previsto squadra trasferta
            
        Returns:
            Probabilità BTTS (0-1)
        """
        # Probabilità che la squadra di casa non segni
        p_home_no_goal = math.exp(-home_xg)
        
        # Probabilità che la squadra in trasferta non segni
        p_away_no_goal = math.exp(-away_xg)
        
        # Probabilità BTTS = 1 - (probabilità che almeno una non segni)
        p_btts = 1 - (p_home_no_goal + p_away_no_goal - p_home_no_goal * p_away_no_goal)
        
        return p_btts
    
    def _calculate_over_under_probabilities(self, total_xg: float) -> Dict[str, float]:
        """
        Calcola probabilità Over/Under per diverse linee.
        
        Args:
            total_xg: xG totale previsto
            
        Returns:
            Dizionario di probabilità per varie linee Over/Under
        """
        result = {}
        
        # Calcola per linee comuni
        lines = [0.5, 1.5, 2.5, 3.5, 4.5]
        
        for line in lines:
            # Probabilità under = somma delle probabilità dei gol fino alla linea
            p_under = 0
            for goals in range(int(line) + 1):
                p_under += (math.exp(-total_xg) * (total_xg ** goals)) / math.factorial(goals)
            
            # Probabilità over = 1 - probabilità under
            p_over = 1 - p_under
            
            result[f"over_{line}"] = p_over
            result[f"under_{line}"] = p_under
        
        return result

# Funzioni di utilità globali
def process_match_xg(match_id: str, force_update: bool = False) -> Dict[str, Any]:
    """
    Processa i dati xG per una partita.
    
    Args:
        match_id: ID della partita
        force_update: Forza aggiornamento dai dati origine
        
    Returns:
        Dati xG elaborati
    """
    processor = XGProcessor()
    return processor.process_match_xg(match_id, force_update)

def get_team_xg_history(team_id: str, matches_limit: int = 10) -> Dict[str, Any]:
    """
    Ottiene la storia xG di una squadra.
    
    Args:
        team_id: ID della squadra
        matches_limit: Numero massimo di partite da considerare
        
    Returns:
        Statistiche xG storiche della squadra
    """
    processor = XGProcessor()
    return processor.process_team_xg_history(team_id, matches_limit)

def predict_match_xg(match_id: str) -> Dict[str, Any]:
    """
    Predice gli xG per una partita futura.
    
    Args:
        match_id: ID della partita
        
    Returns:
        Previsione xG per la partita
    """
    processor = XGProcessor()
    return processor.calculate_match_xg_prediction(match_id)
