"""
Modello di previsione basato sulla distribuzione di Poisson.

Questo modulo implementa un modello di previsione che utilizza la distribuzione
di Poisson per stimare la probabilità dei risultati delle partite, basandosi sulla
forza d'attacco e difesa delle squadre calcolata dai dati storici.
"""

import logging
import math
import numpy as np
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union, Tuple
from collections import defaultdict

from src.data.processors.matches import get_processor as get_match_processor
from src.data.processors.teams import get_processor as get_team_processor
from src.data.processors.head_to_head import get_processor as get_h2h_processor
from src.utils.database import FirebaseManager
from src.utils.cache import cached
from src.analytics.models.basic_model import BasicModel

# Configurazione logger
logger = logging.getLogger(__name__)

class PoissonModel(BasicModel):
    """
    Implementa un modello di previsione basato sulla distribuzione di Poisson.
    
    Questo modello calcola la forza d'attacco e difesa di ogni squadra basandosi
    sui risultati storici, e utilizza questi valori per stimare la distribuzione
    di probabilità dei gol per ogni partita. Si basa sull'approccio di Dixon e Coles
    ma con alcune semplificazioni.
    """
    
    def __init__(self, db: Optional[FirebaseManager] = None):
        """
        Inizializza il modello Poisson.
        
        Args:
            db: Istanza di FirebaseManager. Se None, ne verrà creata una nuova.
        """
        super().__init__(db)
        
        # Parametri del modello
        self.league_strength = {}  # Forza media delle leghe
        self.team_attack = {}      # Forza d'attacco delle squadre per lega
        self.team_defense = {}     # Forza difensiva delle squadre per lega
        self.home_advantage = 1.3  # Vantaggio medio del fattore campo
        
        # Medie predefinite per una lega generica
        self.default_home_goals = 1.35
        self.default_away_goals = 1.05
        
        # Ultimo aggiornamento
        self.last_update = datetime.now() - timedelta(days=2)
        
        # Carica i parametri salvati
        self.load_parameters()
    
    def load_parameters(self) -> None:
        """
        Carica i parametri del modello dal database.
        """
        try:
            # Carica i parametri
            parameters = self.db.get_reference("model_parameters/poisson")
            
            if not parameters:
                logger.info("Nessun parametro salvato, verranno inizializzati con i valori predefiniti")
                return
            
            # Carica i dati
            if 'league_strength' in parameters:
                self.league_strength = parameters['league_strength']
            
            if 'team_attack' in parameters:
                self.team_attack = parameters['team_attack']
            
            if 'team_defense' in parameters:
                self.team_defense = parameters['team_defense']
            
            if 'home_advantage' in parameters:
                self.home_advantage = parameters['home_advantage']
            
            if 'last_update' in parameters:
                self.last_update = datetime.fromisoformat(parameters['last_update'])
            
            logger.info(f"Parametri del modello caricati con successo (ultimo aggiornamento: {self.last_update})")
            
        except Exception as e:
            logger.error(f"Errore nel caricare i parametri del modello: {e}")
    
    def save_parameters(self) -> None:
        """
        Salva i parametri del modello nel database.
        """
        try:
            # Prepara i parametri
            parameters = {
                'league_strength': self.league_strength,
                'team_attack': self.team_attack,
                'team_defense': self.team_defense,
                'home_advantage': self.home_advantage,
                'last_update': datetime.now().isoformat()
            }
            
            # Salva nel database
            self.db.set_reference("model_parameters/poisson", parameters)
            
            logger.info("Parametri del modello salvati con successo")
            
        except Exception as e:
            logger.error(f"Errore nel salvare i parametri del modello: {e}")
    
    def update_parameters(self, days_back: int = 180) -> None:
        """
        Aggiorna i parametri del modello analizzando i risultati recenti.
        
        Args:
            days_back: Numero di giorni indietro da considerare.
        """
        try:
            logger.info(f"Aggiornamento parametri del modello in corso (dati degli ultimi {days_back} giorni)...")
            
            # Data di inizio
            start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
            
            # Recupera tutte le partite completate da start_date
            matches = self._get_completed_matches(start_date)
            
            if not matches:
                logger.warning("Nessuna partita trovata per aggiornare i parametri")
                return
            
            logger.info(f"Trovate {len(matches)} partite completate per l'analisi")
            
            # Raggruppa le partite per lega
            leagues = self._group_matches_by_league(matches)
            
            # Calcola i parametri per ogni lega
            for league_id, league_matches in leagues.items():
                self._calculate_league_parameters(league_id, league_matches)
            
            # Salva i parametri aggiornati
            self.save_parameters()
            
            # Aggiorna l'ultimo aggiornamento
            self.last_update = datetime.now()
            
            logger.info("Aggiornamento parametri del modello completato con successo")
            
        except Exception as e:
            logger.error(f"Errore nell'aggiornamento dei parametri del modello: {e}")
    
    def _get_completed_matches(self, start_date: str) -> List[Dict[str, Any]]:
        """
        Recupera tutte le partite completate a partire da una data.
        
        Args:
            start_date: Data di inizio nel formato YYYY-MM-DD.
            
        Returns:
            Lista di partite completate.
        """
        try:
            # Recupera tutte le partite dal database
            all_matches = self.db.get_reference("matches")
            
            if not all_matches:
                return []
            
            # Filtra le partite
            completed_matches = []
            
            for match_id, match_data in all_matches.items():
                # Verifica che sia completata
                if match_data.get('status') != 'finished':
                    continue
                
                # Verifica che abbia una data valida
                match_date = match_data.get('datetime', '').split('T')[0]
                if not match_date or match_date < start_date:
                    continue
                
                # Verifica che abbia un punteggio valido
                score = match_data.get('score', {})
                if 'home' not in score or 'away' not in score:
                    continue
                
                # Verifica che abbia ID squadre validi
                home_team = match_data.get('home_team', {})
                away_team = match_data.get('away_team', {})
                
                if not home_team.get('id') or not away_team.get('id'):
                    continue
                
                # Verifica che abbia l'ID del campionato
                competition = match_data.get('competition', {})
                if not competition.get('id'):
                    continue
                
                # Aggiungi alla lista
                completed_matches.append(match_data)
            
            return completed_matches
            
        except Exception as e:
            logger.error(f"Errore nel recuperare le partite completate: {e}")
            return []
    
    def _group_matches_by_league(self, matches: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Raggruppa le partite per lega.
        
        Args:
            matches: Lista di partite.
            
        Returns:
            Dizionario con le partite raggruppate per ID lega.
        """
        leagues = defaultdict(list)
        
        for match in matches:
            league_id = match.get('competition', {}).get('id', '')
            if league_id:
                leagues[league_id].append(match)
        
        return leagues
    
    def _calculate_league_parameters(self, league_id: str, matches: List[Dict[str, Any]]) -> None:
        """
        Calcola i parametri del modello per una lega specifica.
        
        Args:
            league_id: ID della lega.
            matches: Lista di partite della lega.
        """
        if not matches:
            return
        
        # Calcola il numero medio di gol per partita
        total_home_goals = 0
        total_away_goals = 0
        total_matches = len(matches)
        
        # Conta i gol totali e le partite per squadra
        team_home_matches = defaultdict(int)
        team_away_matches = defaultdict(int)
        team_home_goals = defaultdict(int)
        team_away_goals = defaultdict(int)
        team_home_conceded = defaultdict(int)
        team_away_conceded = defaultdict(int)
        
        # Estrai tutti i team IDs
        team_ids = set()
        
        for match in matches:
            home_id = match.get('home_team', {}).get('id', '')
            away_id = match.get('away_team', {}).get('id', '')
            
            if not home_id or not away_id:
                continue
            
            team_ids.add(home_id)
            team_ids.add(away_id)
            
            # Estrai il punteggio
            home_goals = match.get('score', {}).get('home', 0)
            away_goals = match.get('score', {}).get('away', 0)
            
            # Aggiorna i totali
            total_home_goals += home_goals
            total_away_goals += away_goals
            
            # Aggiorna i contatori per squadra
            team_home_matches[home_id] += 1
            team_away_matches[away_id] += 1
            
            team_home_goals[home_id] += home_goals
            team_away_goals[away_id] += away_goals
            
            team_home_conceded[home_id] += away_goals
            team_away_conceded[away_id] += home_goals
        
        # Calcola le medie della lega
        avg_home_goals = total_home_goals / total_matches if total_matches > 0 else self.default_home_goals
        avg_away_goals = total_away_goals / total_matches if total_matches > 0 else self.default_away_goals
        
        # Salva la forza della lega
        self.league_strength[league_id] = {
            'avg_home_goals': avg_home_goals,
            'avg_away_goals': avg_away_goals,
            'matches_analyzed': total_matches
        }
        
        # Prepara dizionari per la forza di attacco e difesa
        if league_id not in self.team_attack:
            self.team_attack[league_id] = {}
        
        if league_id not in self.team_defense:
            self.team_defense[league_id] = {}
        
        # Calcola la forza di attacco e difesa per ogni squadra
        for team_id in team_ids:
            # Partite giocate
            home_matches = team_home_matches[team_id]
            away_matches = team_away_matches[team_id]
            
            # Se una squadra ha giocato troppo poche partite, la saltiamo
            if home_matches + away_matches < 3:
                continue
            
            # Gol segnati
            home_goals = team_home_goals[team_id]
            away_goals = team_away_goals[team_id]
            
            # Gol subiti
            home_conceded = team_home_conceded[team_id]
            away_conceded = team_away_conceded[team_id]
            
            # Calcola la forza di attacco
            # Formula: (gol segnati in casa / partite in casa) / media gol in casa della lega
            home_attack = ((home_goals / home_matches) / avg_home_goals) if home_matches > 0 else 1.0
            
            # Formula: (gol segnati in trasferta / partite in trasferta) / media gol in trasferta della lega
            away_attack = ((away_goals / away_matches) / avg_away_goals) if away_matches > 0 else 1.0
            
            # Media pesata (in casa conta di più)
            attack_strength = (home_attack * home_matches + away_attack * away_matches) / (home_matches + away_matches)
            
            # Calcola la forza di difesa (valori più bassi = difesa migliore)
            # Formula: (gol subiti in casa / partite in casa) / media gol in trasferta della lega
            home_defense = ((home_conceded / home_matches) / avg_away_goals) if home_matches > 0 else 1.0
            
            # Formula: (gol subiti in trasferta / partite in trasferta) / media gol in casa della lega
            away_defense = ((away_conceded / away_matches) / avg_home_goals) if away_matches > 0 else 1.0
            
            # Media pesata (in casa conta di più)
            defense_strength = (home_defense * home_matches + away_defense * away_matches) / (home_matches + away_matches)
            
            # Salva i parametri
            self.team_attack[league_id][team_id] = attack_strength
            self.team_defense[league_id][team_id] = defense_strength
    
    def predict_match(
        self, 
        match_id: str,
        home_team_id: str,
        away_team_id: str,
        match_date: Optional[str] = None,
        league_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Predice il risultato di una partita utilizzando il modello Poisson.
        
        Args:
            match_id: ID della partita.
            home_team_id: ID della squadra di casa.
            away_team_id: ID della squadra in trasferta.
            match_date: Data della partita (opzionale).
            league_id: ID del campionato (opzionale).
            
        Returns:
            Dizionario con la previsione completa.
        """
        # Controlla se esiste già una previsione aggiornata
        existing_prediction = self._get_stored_prediction(match_id)
        if existing_prediction:
            return existing_prediction
        
        # Verifica se i parametri del modello sono aggiornati
        if (datetime.now() - self.last_update).days > 7:
            logger.info("Parametri del modello non aggiornati, aggiornamento in corso...")
            self.update_parameters()
        
        # Carica i dati delle squadre
        home_team = self.team_processor.get_stored_team_data(home_team_id)
        away_team = self.team_processor.get_stored_team_data(away_team_id)
        
        if not home_team or not away_team:
            logger.error(f"Dati squadre mancanti per {home_team_id} e/o {away_team_id}")
            # Usa il modello base se mancano i dati
            return super().predict_match(match_id, home_team_id, away_team_id, match_date, league_id)
        
        # Ottieni i dati degli scontri diretti
        h2h_data = self.h2h_processor.get_head_to_head(
            home_team_id, 
            away_team_id, 
            min_matches=0  # Accetta anche se non ci sono match h2h
        )
        
        # Inizializza la previsione
        prediction = {
            'match_id': match_id,
            'home_team_id': home_team_id,
            'away_team_id': away_team_id,
            'home_team_name': home_team.get('name', ''),
            'away_team_name': away_team.get('name', ''),
            'match_date': match_date,
            'league_id': league_id,
            'prediction': '',
            'probabilities': {
                'home_win': 0.0,
                'draw': 0.0,
                'away_win': 0.0
            },
            'expected_goals': {
                'home': 0.0,
                'away': 0.0
            },
            'score_probabilities': {},
            'model_parameters': {},
            'additional_predictions': {},
            'last_updated': datetime.now().isoformat()
        }
        
        # Calcola le previsioni
        try:
            # Calcola i gol attesi
            self._calculate_expected_goals_poisson(prediction, home_team, away_team, league_id, h2h_data)
            
            # Calcola le probabilità 1X2
            self._calculate_1x2_probabilities_poisson(prediction)
            
            # Calcola probabilità dei risultati esatti
            self._calculate_score_probabilities_poisson(prediction)
            
            # Calcola previsioni aggiuntive
            self._calculate_additional_predictions_poisson(prediction)
            
            # Imposta la previsione principale in base alla probabilità più alta
            probs = prediction['probabilities']
            if probs['home_win'] >= probs['draw'] and probs['home_win'] >= probs['away_win']:
                prediction['prediction'] = 'Home Win'
            elif probs['away_win'] >= probs['draw'] and probs['away_win'] >= probs['home_win']:
                prediction['prediction'] = 'Away Win'
            else:
                prediction['prediction'] = 'Draw'
            
            # Aggiungi confidenza
            max_prob = max(probs.values())
            prediction['confidence'] = round(max_prob * 100, 1)
            
            # Salva i parametri del modello utilizzati
            model_params = self._get_model_parameters(home_team_id, away_team_id, league_id)
            prediction['model_parameters'] = model_params
            
            # Salva la previsione
            self._store_prediction(prediction)
            
        except Exception as e:
            logger.error(f"Errore nella generazione della previsione Poisson: {e}")
            prediction['error'] = f"Prediction error: {str(e)}"
            
            # Fallback al modello base
            logger.info("Fallback al modello base...")
            return super().predict_match(match_id, home_team_id, away_team_id, match_date, league_id)
        
        return prediction
    
    def _calculate_expected_goals_poisson(
        self, 
        prediction: Dict[str, Any],
        home_team: Dict[str, Any],
        away_team: Dict[str, Any],
        league_id: Optional[str] = None,
        h2h_data: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Calcola i gol attesi utilizzando il modello Poisson.
        
        Args:
            prediction: Dizionario della previsione da aggiornare.
            home_team: Dati della squadra di casa.
            away_team: Dati della squadra in trasferta.
            league_id: ID del campionato.
            h2h_data: Dati degli scontri diretti (opzionale).
        """
        # Se non è stato specificato il league_id, cerca di ottenerlo dai dati delle squadre
        if not league_id:
            league_id = home_team.get('current_league', {}).get('id', '')
            
            if not league_id:
                # Prova con la squadra in trasferta
                league_id = away_team.get('current_league', {}).get('id', '')
        
        # Se ancora non abbiamo un league_id, usa il modello base
        if not league_id or league_id not in self.league_strength:
            logger.warning(f"League ID non disponibile o non analizzato: {league_id}")
            self._calculate_expected_goals(prediction, home_team, away_team, h2h_data)
            return
        
        # Ottieni i parametri necessari
        home_team_id = home_team['team_id']
        away_team_id = away_team['team_id']
        
        # Ottieni la forza media della lega
        league_avg_home = self.league_strength[league_id]['avg_home_goals']
        league_avg_away = self.league_strength[league_id]['avg_away_goals']
        
        # Ottieni la forza di attacco e difesa
        home_attack = self.team_attack.get(league_id, {}).get(home_team_id, 1.0)
        away_attack = self.team_attack.get(league_id, {}).get(away_team_id, 1.0)
        home_defense = self.team_defense.get(league_id, {}).get(home_team_id, 1.0)
        away_defense = self.team_defense.get(league_id, {}).get(away_team_id, 1.0)
        
        # Calcola i gol attesi
        # Formula: media gol in casa lega * forza attacco casa * forza difesa trasferta
        home_xg = league_avg_home * home_attack * away_defense
        
        # Formula: media gol in trasferta lega * forza attacco trasferta * forza difesa casa
        away_xg = league_avg_away * away_attack * home_defense
        
        # Considera gli scontri diretti (se disponibili)
        if h2h_data and 'stats' in h2h_data:
            h2h_stats = h2h_data['stats']
            
            if 'team1_goals' in h2h_stats and 'team2_goals' in h2h_stats and 'total_matches' in h2h_stats:
                total_matches = h2h_stats['total_matches']
                
                if total_matches >= 3:  # Solo se abbiamo abbastanza dati
                    # Identifica quale squadra è quale
                    is_home_team_team1 = h2h_data['team1_id'] == home_team['team_id']
                    
                    if is_home_team_team1:
                        h2h_home_goals = h2h_stats['team1_goals'] / total_matches
                        h2h_away_goals = h2h_stats['team2_goals'] / total_matches
                    else:
                        h2h_home_goals = h2h_stats['team2_goals'] / total_matches
                        h2h_away_goals = h2h_stats['team1_goals'] / total_matches
                    
                    # Combina con xG attuale (peso 25% per h2h)
                    weight = min(0.25, total_matches / 20)  # Peso cresce con più partite, ma max 25%
                    home_xg = (1 - weight) * home_xg + weight * h2h_home_goals
                    away_xg = (1 - weight) * away_xg + weight * h2h_away_goals
        
        # Limita i valori a un range ragionevole
        home_xg = max(0.3, min(5.0, home_xg))
        away_xg = max(0.2, min(4.0, away_xg))
        
        # Aggiorna la previsione
        prediction['expected_goals'] = {
            'home': round(home_xg, 2),
            'away': round(away_xg, 2)
        }
    
    def _calculate_1x2_probabilities_poisson(self, prediction: Dict[str, Any]) -> None:
        """
        Calcola le probabilità 1X2 utilizzando la distribuzione di Poisson.
        
        Args:
            prediction: Dizionario della previsione da aggiornare.
        """
        # Ottieni i gol attesi
        home_xg = prediction['expected_goals']['home']
        away_xg = prediction['expected_goals']['away']
        
        # Calcola le probabilità di vittoria/pareggio/sconfitta
        home_win_prob = 0.0
        away_win_prob = 0.0
        draw_prob = 0.0
        
        # Considera tutti i possibili punteggi fino a 10-10
        max_goals = 10
        
        for home_goals in range(max_goals + 1):
            for away_goals in range(max_goals + 1):
                # Probabilità di questo punteggio
                score_prob = self._poisson_probability(home_goals, home_xg) * self._poisson_probability(away_goals, away_xg)
                
                # Aggiorna le probabilità 1X2
                if home_goals > away_goals:
                    home_win_prob += score_prob
                elif home_goals < away_goals:
                    away_win_prob += score_prob
                else:
                    draw_prob += score_prob
        
        # Aggiorna la previsione
        prediction['probabilities'] = {
            'home_win': round(home_win_prob, 3),
            'draw': round(draw_prob, 3),
            'away_win': round(away_win_prob, 3)
        }
    
    def _calculate_score_probabilities_poisson(self, prediction: Dict[str, Any]) -> None:
        """
        Calcola le probabilità dei vari risultati esatti.
        
        Args:
            prediction: Dizionario della previsione da aggiornare.
        """
        # Ottieni i gol attesi
        home_xg = prediction['expected_goals']['home']
        away_xg = prediction['expected_goals']['away']
        
        # Calcola la probabilità di diversi risultati esatti usando la distribuzione di Poisson
        score_probs = {}
        
        # Calcola probabilità per punteggi fino a 5-5
        max_goals = 5
        
        for home_goals in range(max_goals + 1):
            for away_goals in range(max_goals + 1):
                score = f"{home_goals}-{away_goals}"
                probability = self._poisson_probability(home_goals, home_xg) * self._poisson_probability(away_goals, away_xg)
                score_probs[score] = round(probability, 4)
        
        # Calcola la probabilità residua (tutti gli altri risultati)
        total_prob = sum(score_probs.values())
        if total_prob < 1.0:
            remaining = 1.0 - total_prob
            score_probs['other'] = round(remaining, 4)
        
        # Ordina per probabilità (decrescente)
        sorted_scores = sorted(score_probs.items(), key=lambda x: x[1], reverse=True)
        
        # Usa solo i 10 risultati più probabili
        prediction['score_probabilities'] = {score: prob for score, prob in sorted_scores[:10]}
    
    def _calculate_additional_predictions_poisson(self, prediction: Dict[str, Any]) -> None:
        """
        Calcola previsioni aggiuntive (BTTS, Over/Under, etc.).
        
        Args:
            prediction: Dizionario della previsione da aggiornare.
        """
        # Ottieni i gol attesi
        home_xg = prediction['expected_goals']['home']
        away_xg = prediction['expected_goals']['away']
        
        additional = {}
        
        # 1. Both Teams To Score (BTTS)
        p_home_0 = self._poisson_probability(0, home_xg)
        p_away_0 = self._poisson_probability(0, away_xg)
        p_btts_yes = 1 - p_home_0 - p_away_0 + (p_home_0 * p_away_0)
        
        additional['btts'] = {
            'yes': round(p_btts_yes, 3),
            'no': round(1 - p_btts_yes, 3)
        }
        
        # 2. Over/Under per diversi valori
        thresholds = [0.5, 1.5, 2.5, 3.5, 4.5]
        
        for threshold in thresholds:
            key = f"over_under_{threshold:.1f}".replace('.0', '')
            additional[key] = self._calculate_over_under(home_xg, away_xg, threshold)
        
        # 3. Goal/No Goal Primo Tempo
        # Aggiusta i gol attesi per il primo tempo (60% dei gol sono nel secondo tempo)
        ht_home_xg = home_xg * 0.4
        ht_away_xg = away_xg * 0.4
        
        p_ht_home_0 = self._poisson_probability(0, ht_home_xg)
        p_ht_away_0 = self._poisson_probability(0, ht_away_xg)
        p_ht_btts_yes = 1 - p_ht_home_0 - p_ht_away_0 + (p_ht_home_0 * p_ht_away_0)
        
        additional['first_half_btts'] = {
            'yes': round(p_ht_btts_yes, 3),
            'no': round(1 - p_ht_btts_yes, 3)
        }
        
        # 4. Risultato Primo Tempo
        additional['first_half_result'] = self._calculate_first_half_result(ht_home_xg, ht_away_xg)
        
        # 5. Double Chance
        additional['double_chance'] = self._calculate_double_chance(prediction['probabilities'])
        
        # 6. Asian Handicap
        additional['asian_handicap'] = self._calculate_asian_handicaps(home_xg, away_xg)
        
        # 7. Clean sheet
        p_home_clean = p_away_0
        p_away_clean = p_home_0
        
        additional['clean_sheet'] = {
            'home': round(p_home_clean, 3),
            'away': round(p_away_clean, 3)
        }
        
        # 8. Win to nil
        p_home_win_to_nil = 0
        p_away_win_to_nil = 0
        
        # Calcola le probabilità sommando i punteggi rilevanti
        for home_goals in range(1, 6):
            p_home_win_to_nil += self._poisson_probability(home_goals, home_xg) * self._poisson_probability(0, away_xg)
        
        for away_goals in range(1, 6):
            p_away_win_to_nil += self._poisson_probability(0, home_xg) * self._poisson_probability(away_goals, away_xg)
        
        additional['win_to_nil'] = {
            'home': round(p_home_win_to_nil, 3),
            'away': round(p_away_win_to_nil, 3)
        }
        
        # 9. Team to score
        p_home_to_score = 1 - p_home_0
        p_away_to_score = 1 - p_away_0
        
        additional['team_to_score'] = {
            'home': round(p_home_to_score, 3),
            'away': round(p_away_to_score, 3)
        }
        
        # 10. Goals ranges
        additional['goals_ranges'] = {
            '0-1': round(self._calculate_total_goals_range(home_xg, away_xg, 0, 1), 3),
            '2-3': round(self._calculate_total_goals_range(home_xg, away_xg, 2, 3), 3),
            '4-6': round(self._calculate_total_goals_range(home_xg, away_xg, 4, 6), 3),
            '7+': round(self._calculate_total_goals_range(home_xg, away_xg, 7, 99), 3)
        }
        
        # Aggiorna la previsione
        prediction['additional_predictions'] = additional
    
    def _calculate_over_under(self, home_xg: float, away_xg: float, threshold: float) -> Dict[str, float]:
        """
        Calcola le probabilità Over/Under per una soglia specifica.
        
        Args:
            home_xg: Gol attesi della squadra di casa.
            away_xg: Gol attesi della squadra in trasferta.
            threshold: Soglia di gol (es. 2.5).
            
        Returns:
            Dizionario con le probabilità Over/Under.
        """
        p_under = 0.0
        
        # Calcola la probabilità sommando tutti i risultati sotto la soglia
        max_goals = int(threshold) + 1
        
        for home_goals in range(max_goals):
            for away_goals in range(max_goals):
                if home_goals + away_goals < threshold:
                    p_under += self._poisson_probability(home_goals, home_xg) * self._poisson_probability(away_goals, away_xg)
        
        return {
            'over': round(1 - p_under, 3),
            'under': round(p_under, 3)
        }
    
    def _calculate_first_half_result(self, ht_home_xg: float, ht_away_xg: float) -> Dict[str, float]:
        """
        Calcola le probabilità del risultato del primo tempo.
        
        Args:
            ht_home_xg: Gol attesi della squadra di casa nel primo tempo.
            ht_away_xg: Gol attesi della squadra in trasferta nel primo tempo.
            
        Returns:
            Dizionario con le probabilità 1X2 del primo tempo.
        """
        home_win_prob = 0.0
        away_win_prob = 0.0
        draw_prob = 0.0
        
        # Considera punteggi fino a 3-3 nel primo tempo
        max_goals = 3
        
        for home_goals in range(max_goals + 1):
            for away_goals in range(max_goals + 1):
                prob = self._poisson_probability(home_goals, ht_home_xg) * self._poisson_probability(away_goals, ht_away_xg)
                
                if home_goals > away_goals:
                    home_win_prob += prob
                elif home_goals < away_goals:
                    away_win_prob += prob
                else:
                    draw_prob += prob
        
        return {
            'home_win': round(home_win_prob, 3),
            'draw': round(draw_prob, 3),
            'away_win': round(away_win_prob, 3)
        }
    
    def _calculate_double_chance(self, probabilities: Dict[str, float]) -> Dict[str, float]:
        """
        Calcola le probabilità per la Doppia Chance.
        
        Args:
            probabilities: Dizionario con le probabilità 1X2.
            
        Returns:
            Dizionario con le probabilità Doppia Chance.
        """
        home_win = probabilities['home_win']
        draw = probabilities['draw']
        away_win = probabilities['away_win']
        
        return {
            '1X': round(home_win + draw, 3),
            'X2': round(draw + away_win, 3),
            '12': round(home_win + away_win, 3)
        }
    
    def _calculate_total_goals_range(
        self, 
        home_xg: float, 
        away_xg: float, 
        min_goals: int, 
        max_goals: int
    ) -> float:
        """
        Calcola la probabilità che il numero totale di gol sia in un intervallo.
        
        Args:
            home_xg: Gol attesi della squadra di casa.
            away_xg: Gol attesi della squadra in trasferta.
            min_goals: Numero minimo di gol (incluso).
            max_goals: Numero massimo di gol (incluso).
            
        Returns:
            Probabilità che il numero totale di gol sia nell'intervallo.
        """
        probability = 0.0
        
        # Somma le probabilità di tutti i punteggi che danno un totale nell'intervallo
        for total_goals in range(min_goals, max_goals + 1):
            # Per ogni totale, considera tutte le combinazioni possibili
            for home_goals in range(total_goals + 1):
                away_goals = total_goals - home_goals
                
                # Aggiungi la probabilità di questo punteggio
                probability += self._poisson_probability(home_goals, home_xg) * self._poisson_probability(away_goals, away_xg)
        
        return probability
    
    def _get_model_parameters(
        self, 
        home_team_id: str, 
        away_team_id: str, 
        league_id: Optional[str]
    ) -> Dict[str, Any]:
        """
        Ottiene i parametri del modello utilizzati per questa previsione.
        
        Args:
            home_team_id: ID della squadra di casa.
            away_team_id: ID della squadra in trasferta.
            league_id: ID del campionato.
            
        Returns:
            Dizionario con i parametri del modello.
        """
        params = {
            'home_advantage': self.home_advantage
        }
        
        # Aggiungi i parametri specifici della lega se disponibili
        if league_id and league_id in self.league_strength:
            params['league'] = {
                'id': league_id,
                'avg_home_goals': self.league_strength[league_id]['avg_home_goals'],
                'avg_away_goals': self.league_strength[league_id]['avg_away_goals'],
                'matches_analyzed': self.league_strength[league_id]['matches_analyzed']
            }
        
        # Aggiungi la forza delle squadre
        if league_id:
            # Squadra di casa
            if league_id in self.team_attack and home_team_id in self.team_attack[league_id]:
                if 'teams' not in params:
                    params['teams'] = {}
                
                if home_team_id not in params['teams']:
                    params['teams'][home_team_id] = {}
                
                params['teams'][home_team_id]['attack'] = self.team_attack[league_id][home_team_id]
            
            if league_id in self.team_defense and home_team_id in self.team_defense[league_id]:
                if 'teams' not in params:
                    params['teams'] = {}
                
                if home_team_id not in params['teams']:
                    params['teams'][home_team_id] = {}
                
                params['teams'][home_team_id]['defense'] = self.team_defense[league_id][home_team_id]
            
            # Squadra in trasferta
            if league_id in self.team_attack and away_team_id in self.team_attack[league_id]:
                if 'teams' not in params:
                    params['teams'] = {}
                
                if away_team_id not in params['teams']:
                    params['teams'][away_team_id] = {}
                
                params['teams'][away_team_id]['attack'] = self.team_attack[league_id][away_team_id]
            
            if league_id in self.team_defense and away_team_id in self.team_defense[league_id]:
                if 'teams' not in params:
                    params['teams'] = {}
                
                if away_team_id not in params['teams']:
                    params['teams'][away_team_id] = {}
                
                params['teams'][away_team_id]['defense'] = self.team_defense[league_id][away_team_id]
        
        return params
    
    def compare_models(
        self, 
        match_id: str,
        home_team_id: str,
        away_team_id: str,
        match_date: Optional[str] = None,
        league_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Confronta le previsioni del modello Poisson con il modello base.
        
        Args:
            match_id: ID della partita.
            home_team_id: ID della squadra di casa.
            away_team_id: ID della squadra in trasferta.
            match_date: Data della partita (opzionale).
            league_id: ID del campionato (opzionale).
            
        Returns:
            Dizionario con il confronto delle previsioni.
        """
        # Ottieni le previsioni di entrambi i modelli
        poisson_prediction = self.predict_match(match_id, home_team_id, away_team_id, match_date, league_id)
        base_prediction = super().predict_match(match_id, home_team_id, away_team_id, match_date, league_id)
        
        # Crea il confronto
        comparison = {
            'match_id': match_id,
            'home_team_id': home_team_id,
            'away_team_id': away_team_id,
            'home_team_name': poisson_prediction.get('home_team_name', ''),
            'away_team_name': poisson_prediction.get('away_team_name', ''),
            'match_date': match_date,
            'league_id': league_id,
            'poisson_model': {
                'prediction': poisson_prediction.get('prediction', ''),
                'probabilities': poisson_prediction.get('probabilities', {}),
                'expected_goals': poisson_prediction.get('expected_goals', {})
            },
            'basic_model': {
                'prediction': base_prediction.get('prediction', ''),
                'probabilities': base_prediction.get('probabilities', {}),
                'expected_goals': base_prediction.get('expected_goals', {})
            },
            'differences': {
                'probabilities': {},
                'expected_goals': {}
            },
            'last_updated': datetime.now().isoformat()
        }
        
        # Calcola le differenze
        # Probabilità
        for outcome in ['home_win', 'draw', 'away_win']:
            poisson_prob = poisson_prediction.get('probabilities', {}).get(outcome, 0.0)
            base_prob = base_prediction.get('probabilities', {}).get(outcome, 0.0)
            
            comparison['differences']['probabilities'][outcome] = round(poisson_prob - base_prob, 3)
        
        # Expected Goals
        for team in ['home', 'away']:
            poisson_xg = poisson_prediction.get('expected_goals', {}).get(team, 0.0)
            base_xg = base_prediction.get('expected_goals', {}).get(team, 0.0)
            
            comparison['differences']['expected_goals'][team] = round(poisson_xg - base_xg, 2)
        
        return comparison


# Istanza globale per un utilizzo più semplice
poisson_model = PoissonModel()

def get_model():
    """
    Ottiene l'istanza globale del modello.
    
    Returns:
        Istanza di PoissonModel.
    """
    return poisson_model
