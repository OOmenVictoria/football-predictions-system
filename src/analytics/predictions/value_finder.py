"""
Modulo per l'identificazione di value bet basate su analisi statistiche avanzate.
Questo modulo combina previsioni statistiche e quote di mercato per identificare 
opportunità di scommessa con valore positivo atteso.
"""
import logging
import math
import json
import time
from typing import Dict, List, Any, Optional, Tuple, Union
from datetime import datetime, timedelta

from src.utils.cache import cached
from src.utils.database import FirebaseManager
from src.config.settings import get_setting
from src.analytics.models.basic_model import create_basic_model
from src.analytics.models.poisson_model import create_poisson_model
from src.analytics.models.xg_model import create_xg_model
from src.analytics.predictions.bet_analyzer import BetAnalyzer

logger = logging.getLogger(__name__)

class ValueFinder:
    """
    Finder per value bet basate su analisi statistiche avanzate.
    
    Identifica opportunità di scommessa con valore positivo atteso confrontando
    le probabilità calcolate dai modelli statistici con le quote di mercato.
    """
    
    def __init__(self):
        """Inizializza il value finder."""
        self.db = FirebaseManager()
        self.bet_analyzer = BetAnalyzer()
        
        # Carica modelli per le previsioni
        self.basic_model = create_basic_model()
        self.poisson_model = create_poisson_model()
        self.xg_model = create_xg_model()
        
        # Configurazione
        self.min_value_threshold = get_setting('predictions.value_finder.min_value_threshold', 0.05)
        self.min_edge_threshold = get_setting('predictions.value_finder.min_edge_threshold', 0.02)
        self.min_probability = get_setting('predictions.value_finder.min_probability', 0.25)
        self.max_probability = get_setting('predictions.value_finder.max_probability', 0.75)
        self.model_weights = get_setting('predictions.value_finder.model_weights', {
            'basic': 0.2,
            'poisson': 0.4,
            'xg': 0.4
        })
        self.bookmaker_ratings = get_setting('predictions.value_finder.bookmaker_ratings', {
            'bet365': 0.9,
            'williamhill': 0.85,
            'bwin': 0.8,
            'pinnacle': 0.95
        })
        
        logger.info("ValueFinder inizializzato con threshold di valore %.2f", self.min_value_threshold)
    
    @cached(ttl=3600)  # Cache di 1 ora
    def find_value_bets(self, match_id: str) -> Dict[str, Any]:
        """
        Identifica value bet per una specifica partita.
        
        Args:
            match_id: ID della partita
            
        Returns:
            Dizionario con value bet identificate e dettagli
        """
        logger.info(f"Cercando value bet per match_id={match_id}")
        
        try:
            # Ottieni dati partita
            match_ref = self.db.get_reference(f"data/matches/{match_id}")
            match_data = match_ref.get()
            
            if not match_data:
                logger.warning(f"Nessun dato trovato per match_id={match_id}")
                return self._create_empty_value_bets(match_id)
            
            # Verifica che la partita non sia già conclusa
            if match_data.get('status') == 'FINISHED':
                logger.warning(f"La partita {match_id} è già conclusa, nessuna value bet disponibile")
                return self._create_empty_value_bets(match_id)
            
            # Verifica che ci siano quote disponibili
            if 'odds' not in match_data or not match_data['odds']:
                logger.warning(f"Nessuna quota disponibile per match_id={match_id}")
                return self._create_empty_value_bets(match_id)
            
            # Genera previsioni dai diversi modelli
            predictions = self._generate_model_predictions(match_data)
            
            # Determina l'accuratezza storica dei diversi modelli
            model_accuracies = self._get_model_accuracies(
                match_data.get('league_id'),
                match_data.get('home_team_id'),
                match_data.get('away_team_id')
            )
            
            # Combina le previsioni dai diversi modelli
            combined_prediction = self._combine_predictions(predictions, model_accuracies)
            
            # Identifica value bet
            value_bets = self._identify_value_bets(combined_prediction, match_data['odds'])
            
            # Calcola rating e ranking per le value bet
            value_bets = self._rate_value_bets(value_bets, combined_prediction, match_data)
            
            # Prepara il risultato
            result = {
                'match_id': match_id,
                'home_team': match_data.get('home_team', ''),
                'away_team': match_data.get('away_team', ''),
                'league_id': match_data.get('league_id', ''),
                'match_datetime': match_data.get('datetime', ''),
                'value_bets': value_bets,
                'has_value_bets': len(value_bets) > 0,
                'models_used': list(predictions.keys()),
                'market_types_analyzed': self._get_analyzed_markets(match_data['odds']),
                'bookmakers_analyzed': self._get_analyzed_bookmakers(match_data['odds']),
                'analysis_time': datetime.now().isoformat(),
                'leagues_analyzed': league_ids if league_ids else []
            }
            
            return result
            
        except Exception as e:
            logger.error(f"Errore nella ricerca di value bet giornaliere: {e}")
            return {'value_bets': [], 'has_value_bets': False}

# Funzioni di utilità per accesso globale
def find_value_bets(match_id: str) -> Dict[str, Any]:
    """
    Identifica value bet per una specifica partita.
    
    Args:
        match_id: ID della partita
        
    Returns:
        Dizionario con value bet identificate
    """
    finder = ValueFinder()
    return finder.find_value_bets(match_id)

def find_daily_value_bets(league_ids: Optional[List[str]] = None, limit: int = 10) -> Dict[str, Any]:
    """
    Identifica le migliori value bet per le partite della giornata.
    
    Args:
        league_ids: Lista di ID di campionati da considerare (opzionale)
        limit: Numero massimo di value bet da restituire
        
    Returns:
        Dizionario con le migliori value bet
    """
    finder = ValueFinder()
    return finder.find_daily_value_bets(league_ids, limit)
            }
            
            logger.info(f"Trovate {len(value_bets)} value bet per match_id={match_id}")
            return result
            
        except Exception as e:
            logger.error(f"Errore nella ricerca di value bet per match_id={match_id}: {e}")
            return self._create_empty_value_bets(match_id)
    
    def _generate_model_predictions(self, match_data: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        """
        Genera previsioni dai diversi modelli.
        
        Args:
            match_data: Dati della partita
            
        Returns:
            Dizionario con previsioni dai diversi modelli
        """
        predictions = {}
        
        try:
            # Genera previsione dal modello base
            predictions['basic'] = self.basic_model.predict_match(match_data)
            
            # Genera previsione dal modello Poisson
            predictions['poisson'] = self.poisson_model.predict_match(match_data)
            
            # Genera previsione dal modello xG se ci sono dati sufficienti
            xg_prediction = self.xg_model.predict_match(match_data)
            if xg_prediction.get('xg_data_sufficient', False):
                predictions['xg'] = xg_prediction
            
        except Exception as e:
            logger.error(f"Errore nella generazione delle previsioni: {e}")
        
        return predictions
    
    def _get_model_accuracies(self, league_id: Optional[str], 
                              home_team_id: Optional[str],
                              away_team_id: Optional[str]) -> Dict[str, float]:
        """
        Ottiene accuratezza storica dei diversi modelli per questo tipo di partita.
        
        In una implementazione completa, recupererebbe dati storici sulle performance dei modelli
        nel campionato specifico o con squadre simili. Per semplicità, usa valori predefiniti.
        
        Args:
            league_id: ID del campionato
            home_team_id: ID squadra di casa
            away_team_id: ID squadra in trasferta
            
        Returns:
            Dizionario con accuratezze dei modelli
        """
        # Implementazione semplificata: usa i pesi configurati
        return self.model_weights
    
    def _combine_predictions(self, predictions: Dict[str, Dict[str, Any]], 
                            model_accuracies: Dict[str, float]) -> Dict[str, Any]:
        """
        Combina le previsioni dai diversi modelli in una previsione unificata.
        
        Args:
            predictions: Previsioni dai diversi modelli
            model_accuracies: Accuratezza di ciascun modello
            
        Returns:
            Previsione combinata
        """
        if not predictions:
            return {}
        
        # Normalizza i pesi in base ai modelli disponibili
        total_weight = sum(model_accuracies.get(model, 0) for model in predictions.keys())
        if total_weight == 0:
            # Pesi uguali se nessun modello ha accuratezza nota
            weights = {model: 1.0 / len(predictions) for model in predictions.keys()}
        else:
            weights = {model: model_accuracies.get(model, 0) / total_weight 
                      for model in predictions.keys()}
        
        # Inizializza i contenitori per la previsione combinata
        combined = {
            'probabilities': {'1': 0.0, 'X': 0.0, '2': 0.0},
            'btts': {'Yes': 0.0, 'No': 0.0},
            'over_under': {
                '0.5': {'Over': 0.0, 'Under': 0.0},
                '1.5': {'Over': 0.0, 'Under': 0.0},
                '2.5': {'Over': 0.0, 'Under': 0.0},
                '3.5': {'Over': 0.0, 'Under': 0.0}
            },
            'asian_handicap': {},
            'exact_score': {}
        }
        
        # Combina le probabilità dai diversi modelli
        for model, prediction in predictions.items():
            weight = weights.get(model, 0)
            
            # Risultato 1X2
            if 'probabilities' in prediction:
                for key in combined['probabilities']:
                    if key in prediction['probabilities']:
                        combined['probabilities'][key] += prediction['probabilities'][key] * weight
            
            # BTTS
            if 'btts' in prediction:
                for key in combined['btts']:
                    if key in prediction['btts']:
                        combined['btts'][key] += prediction['btts'][key] * weight
            
            # Over/Under
            if 'over_under' in prediction:
                for line in combined['over_under']:
                    if line in prediction['over_under']:
                        for key in combined['over_under'][line]:
                            if key in prediction['over_under'][line]:
                                combined['over_under'][line][key] += prediction['over_under'][line][key] * weight
            
            # Asian Handicap (più complesso, richiede mappatura delle linee)
            if 'asian_handicap' in prediction:
                for line, values in prediction['asian_handicap'].items():
                    if line not in combined['asian_handicap']:
                        combined['asian_handicap'][line] = {'home': 0.0, 'away': 0.0}
                    for key in values:
                        combined['asian_handicap'][line][key] += values[key] * weight
            
            # Exact Score (aggregare solo i più probabili)
            if 'exact_score' in prediction:
                for score, prob in prediction['exact_score'].items():
                    if score not in combined['exact_score']:
                        combined['exact_score'][score] = 0.0
                    combined['exact_score'][score] += prob * weight
        
        # Normalizza le probabilità
        total = sum(combined['probabilities'].values())
        if total > 0:
            for key in combined['probabilities']:
                combined['probabilities'][key] /= total
        
        total = sum(combined['btts'].values())
        if total > 0:
            for key in combined['btts']:
                combined['btts'][key] /= total
        
        for line in combined['over_under']:
            total = sum(combined['over_under'][line].values())
            if total > 0:
                for key in combined['over_under'][line]:
                    combined['over_under'][line][key] /= total
        
        for line in combined['asian_handicap']:
            total = sum(combined['asian_handicap'][line].values())
            if total > 0:
                for key in combined['asian_handicap'][line]:
                    combined['asian_handicap'][line][key] /= total
        
        # Ordina e limita i risultati esatti ai più probabili (top 10)
        combined['exact_score'] = dict(sorted(
            combined['exact_score'].items(), 
            key=lambda x: x[1], 
            reverse=True
        )[:10])
        
        # Normalizza i risultati esatti
        total = sum(combined['exact_score'].values())
        if total > 0:
            for score in combined['exact_score']:
                combined['exact_score'][score] /= total
        
        return combined
    
    def _identify_value_bets(self, prediction: Dict[str, Any], 
                            odds_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Identifica value bet confrontando le probabilità previste con le quote di mercato.
        
        Args:
            prediction: Previsione combinata
            odds_data: Quote di mercato
            
        Returns:
            Lista di value bet identificate
        """
        value_bets = []
        
        # Analizza risultato 1X2
        if 'match_winner' in odds_data and 'probabilities' in prediction:
            for bookmaker, odds in odds_data['match_winner'].items():
                for outcome in ['1', 'X', '2']:
                    if outcome in odds and outcome in prediction['probabilities']:
                        decimal_odds = float(odds[outcome])
                        model_prob = prediction['probabilities'][outcome]
                        
                        # Calcola il valore atteso
                        implied_prob = 1.0 / decimal_odds
                        value = (decimal_odds * model_prob) - 1
                        edge = model_prob - implied_prob
                        
                        # Verifica se è una value bet secondo i criteri
                        if (value >= self.min_value_threshold and 
                            edge >= self.min_edge_threshold and
                            self.min_probability <= model_prob <= self.max_probability):
                            value_bets.append({
                                'market': 'match_winner',
                                'selection': outcome,
                                'bookmaker': bookmaker,
                                'odds': decimal_odds,
                                'model_probability': model_prob,
                                'implied_probability': implied_prob,
                                'value': value,
                                'edge': edge
                            })
        
        # Analizza BTTS
        if 'btts' in odds_data and 'btts' in prediction:
            for bookmaker, odds in odds_data['btts'].items():
                for outcome in ['Yes', 'No']:
                    if outcome in odds and outcome in prediction['btts']:
                        decimal_odds = float(odds[outcome])
                        model_prob = prediction['btts'][outcome]
                        
                        implied_prob = 1.0 / decimal_odds
                        value = (decimal_odds * model_prob) - 1
                        edge = model_prob - implied_prob
                        
                        if (value >= self.min_value_threshold and 
                            edge >= self.min_edge_threshold and
                            self.min_probability <= model_prob <= self.max_probability):
                            value_bets.append({
                                'market': 'btts',
                                'selection': outcome,
                                'bookmaker': bookmaker,
                                'odds': decimal_odds,
                                'model_probability': model_prob,
                                'implied_probability': implied_prob,
                                'value': value,
                                'edge': edge
                            })
        
        # Analizza Over/Under
        if 'over_under' in odds_data and 'over_under' in prediction:
            for line in prediction['over_under']:
                if line in odds_data['over_under']:
                    for bookmaker, odds in odds_data['over_under'][line].items():
                        for outcome in ['Over', 'Under']:
                            if outcome in odds and outcome in prediction['over_under'][line]:
                                decimal_odds = float(odds[outcome])
                                model_prob = prediction['over_under'][line][outcome]
                                
                                implied_prob = 1.0 / decimal_odds
                                value = (decimal_odds * model_prob) - 1
                                edge = model_prob - implied_prob
                                
                                if (value >= self.min_value_threshold and 
                                    edge >= self.min_edge_threshold and
                                    self.min_probability <= model_prob <= self.max_probability):
                                    value_bets.append({
                                        'market': 'over_under',
                                        'line': line,
                                        'selection': outcome,
                                        'bookmaker': bookmaker,
                                        'odds': decimal_odds,
                                        'model_probability': model_prob,
                                        'implied_probability': implied_prob,
                                        'value': value,
                                        'edge': edge
                                    })
        
        # Analizza Asian Handicap
        if 'asian_handicap' in odds_data and 'asian_handicap' in prediction:
            for line in prediction['asian_handicap']:
                if line in odds_data['asian_handicap']:
                    for bookmaker, odds in odds_data['asian_handicap'][line].items():
                        for outcome in ['home', 'away']:
                            if outcome in odds and outcome in prediction['asian_handicap'][line]:
                                decimal_odds = float(odds[outcome])
                                model_prob = prediction['asian_handicap'][line][outcome]
                                
                                implied_prob = 1.0 / decimal_odds
                                value = (decimal_odds * model_prob) - 1
                                edge = model_prob - implied_prob
                                
                                if (value >= self.min_value_threshold and 
                                    edge >= self.min_edge_threshold and
                                    self.min_probability <= model_prob <= self.max_probability):
                                    value_bets.append({
                                        'market': 'asian_handicap',
                                        'line': line,
                                        'selection': outcome,
                                        'bookmaker': bookmaker,
                                        'odds': decimal_odds,
                                        'model_probability': model_prob,
                                        'implied_probability': implied_prob,
                                        'value': value,
                                        'edge': edge
                                    })
        
        return value_bets
    
    def _rate_value_bets(self, value_bets: List[Dict[str, Any]], 
                         prediction: Dict[str, Any],
                         match_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Calcola rating e ranking per le value bet identificate.
        
        Args:
            value_bets: Lista di value bet
            prediction: Previsione combinata
            match_data: Dati della partita
            
        Returns:
            Value bet con rating e ranking
        """
        if not value_bets:
            return []
        
        # Calcola un punteggio per ogni value bet
        for bet in value_bets:
            # Base: valore atteso
            score = bet['value'] * 50
            
            # Bonus per edge più elevato
            score += bet['edge'] * 20
            
            # Bonus/malus per probabilità di vincita (preferiamo 40-60%)
            prob_center = 0.5
            prob_distance = abs(bet['model_probability'] - prob_center)
            score -= prob_distance * 20
            
            # Bonus per bookmaker affidabili
            bookmaker_rating = self.bookmaker_ratings.get(bet['bookmaker'], 0.7)
            score *= bookmaker_rating
            
            # Limita a 0-100
            bet['rating'] = max(0, min(100, score))
            
            # Aggiungi motivazione testuale
            if bet['rating'] > 80:
                confidence = "Eccellente"
            elif bet['rating'] > 60:
                confidence = "Molto buona"
            elif bet['rating'] > 40:
                confidence = "Buona"
            elif bet['rating'] > 20:
                confidence = "Discreta"
            else:
                confidence = "Bassa"
            
            bet['confidence'] = confidence
            bet['description'] = self._generate_bet_description(bet, match_data)
        
        # Ordina per rating
        value_bets = sorted(value_bets, key=lambda x: x['rating'], reverse=True)
        
        # Aggiungi ranking
        for i, bet in enumerate(value_bets):
            bet['rank'] = i + 1
        
        return value_bets
    
    def _generate_bet_description(self, bet: Dict[str, Any], 
                                 match_data: Dict[str, Any]) -> str:
        """
        Genera una descrizione testuale della value bet.
        
        Args:
            bet: Dati della value bet
            match_data: Dati della partita
            
        Returns:
            Descrizione testuale
        """
        home_team = match_data.get('home_team', 'Squadra casa')
        away_team = match_data.get('away_team', 'Squadra trasferta')
        
        # Descrizione per 1X2
        if bet['market'] == 'match_winner':
            if bet['selection'] == '1':
                return f"Vittoria {home_team} a quota {bet['odds']:.2f} con valore atteso {bet['value']:.2f} e edge {bet['edge']*100:.1f}%"
            elif bet['selection'] == 'X':
                return f"Pareggio a quota {bet['odds']:.2f} con valore atteso {bet['value']:.2f} e edge {bet['edge']*100:.1f}%"
            else:  # '2'
                return f"Vittoria {away_team} a quota {bet['odds']:.2f} con valore atteso {bet['value']:.2f} e edge {bet['edge']*100:.1f}%"
        
        # Descrizione per BTTS
        elif bet['market'] == 'btts':
            if bet['selection'] == 'Yes':
                return f"BTTS Si a quota {bet['odds']:.2f} con valore atteso {bet['value']:.2f} e edge {bet['edge']*100:.1f}%"
            else:
                return f"BTTS No a quota {bet['odds']:.2f} con valore atteso {bet['value']:.2f} e edge {bet['edge']*100:.1f}%"
        
        # Descrizione per Over/Under
        elif bet['market'] == 'over_under':
            if bet['selection'] == 'Over':
                return f"Over {bet['line']} a quota {bet['odds']:.2f} con valore atteso {bet['value']:.2f} e edge {bet['edge']*100:.1f}%"
            else:
                return f"Under {bet['line']} a quota {bet['odds']:.2f} con valore atteso {bet['value']:.2f} e edge {bet['edge']*100:.1f}%"
        
        # Descrizione per Asian Handicap
        elif bet['market'] == 'asian_handicap':
            if bet['selection'] == 'home':
                return f"{home_team} {bet['line']} a quota {bet['odds']:.2f} con valore atteso {bet['value']:.2f} e edge {bet['edge']*100:.1f}%"
            else:
                return f"{away_team} {bet['line']} a quota {bet['odds']:.2f} con valore atteso {bet['value']:.2f} e edge {bet['edge']*100:.1f}%"
        
        # Default
        return f"Quota {bet['odds']:.2f} con valore atteso {bet['value']:.2f} e edge {bet['edge']*100:.1f}%"
    
    def _get_analyzed_markets(self, odds_data: Dict[str, Any]) -> List[str]:
        """
        Ottiene la lista dei mercati analizzati.
        
        Args:
            odds_data: Quote di mercato
            
        Returns:
            Lista dei mercati analizzati
        """
        return list(odds_data.keys())
    
    def _get_analyzed_bookmakers(self, odds_data: Dict[str, Any]) -> List[str]:
        """
        Ottiene la lista dei bookmaker analizzati.
        
        Args:
            odds_data: Quote di mercato
            
        Returns:
            Lista dei bookmaker analizzati
        """
        bookmakers = set()
        for market, market_data in odds_data.items():
            if isinstance(market_data, dict):
                for bookie in market_data.keys():
                    bookmakers.add(bookie)
        return list(bookmakers)
    
    def _create_empty_value_bets(self, match_id: str) -> Dict[str, Any]:
        """
        Crea un oggetto value bet vuoto.
        
        Args:
            match_id: ID della partita
            
        Returns:
            Oggetto value bet vuoto
        """
        return {
            'match_id': match_id,
            'value_bets': [],
            'has_value_bets': False,
            'models_used': [],
            'market_types_analyzed': [],
            'bookmakers_analyzed': [],
            'analysis_time': datetime.now().isoformat()
        }

    @cached(ttl=3600*6)  # Cache di 6 ore
    def find_daily_value_bets(self, league_ids: Optional[List[str]] = None, 
                             limit: int = 10) -> Dict[str, Any]:
        """
        Identifica le migliori value bet per le partite della giornata.
        
        Args:
            league_ids: Lista di ID di campionati da considerare (opzionale)
            limit: Numero massimo di value bet da restituire
            
        Returns:
            Dizionario con le migliori value bet
        """
        logger.info(f"Cercando value bet giornaliere (limit={limit})")
        
        try:
            # Ottieni le partite di oggi e domani
            now = datetime.now()
            tomorrow = now + timedelta(days=1)
            
            matches_ref = self.db.get_reference("data/matches")
            
            # Filtra per data
            today_str = now.strftime("%Y-%m-%d")
            tomorrow_str = tomorrow.strftime("%Y-%m-%d")
            
            # Query per ottenere le partite con quote
            # In un'implementazione reale, questo sarebbe più efficiente con indici appropriati
            matches = matches_ref.get()
            
            if not matches:
                logger.warning("Nessuna partita trovata")
                return {'value_bets': [], 'has_value_bets': False}
            
            # Filtra manualmente perché Firebase Realtime Database ha limitazioni nelle query
            upcoming_matches = {}
            for match_id, match in matches.items():
                # Verifica data
                match_date = match.get('datetime', '')
                if not match_date or not (today_str in match_date or tomorrow_str in match_date):
                    continue
                
                # Verifica stato
                if match.get('status') == 'FINISHED':
                    continue
                
                # Verifica campionato
                if league_ids and match.get('league_id') not in league_ids:
                    continue
                
                # Verifica quote
                if 'odds' not in match or not match['odds']:
                    continue
                
                upcoming_matches[match_id] = match
            
            if not upcoming_matches:
                logger.warning("Nessuna partita imminente trovata con quote")
                return {'value_bets': [], 'has_value_bets': False}
            
            # Cerca value bet per ogni partita
            all_value_bets = []
            for match_id in upcoming_matches:
                match_value_bets = self.find_value_bets(match_id)
                
                if match_value_bets and match_value_bets.get('has_value_bets', False):
                    # Estrai i dati principali e aggiungi info sulla partita
                    for bet in match_value_bets['value_bets']:
                        bet['match_id'] = match_id
                        bet['home_team'] = match_value_bets['home_team']
                        bet['away_team'] = match_value_bets['away_team']
                        bet['league_id'] = match_value_bets['league_id']
                        bet['match_datetime'] = match_value_bets['match_datetime']
                        all_value_bets.append(bet)
            
            # Ordina per rating
            all_value_bets = sorted(all_value_bets, key=lambda x: x.get('rating', 0), reverse=True)
            
            # Limita al numero richiesto
            best_value_bets = all_value_bets[:limit]
            
            # Raggruppa per partita
            value_bets_by_match = {}
            for bet in best_value_bets:
                match_id = bet['match_id']
                if match_id not in value_bets_by_match:
                    value_bets_by_match[match_id] = {
                        'match_id': match_id,
                        'home_team': bet['home_team'],
                        'away_team': bet['away_team'],
                        'league_id': bet['league_id'],
                        'match_datetime': bet['match_datetime'],
                        'value_bets': []
                    }
                value_bets_by_match[match_id]['value_bets'].append(bet)
            
            result = {
                'value_bets': best_value_bets,
                'value_bets_by_match': list(value_bets_by_match.values()),
                'has_value_bets': len(best_value_bets) > 0,
                'total_value_bets_found': len(all_value_bets),
                'matches_analyzed': len(upcoming_matches),
                'analysis_time': datetime.now().isoformat()
