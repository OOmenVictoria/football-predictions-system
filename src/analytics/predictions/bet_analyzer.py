"""
Modulo per l'analisi e la valutazione delle quote di scommessa.

Questo modulo fornisce funzionalità per analizzare le quote di scommessa, 
identificare value bet e valutare il valore atteso delle scommesse.
"""
import logging
import math
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple, Union

from src.utils.cache import cached
from src.utils.database import FirebaseManager
from src.config.settings import get_setting
from src.analytics.predictions.match_predictor import predict_match

logger = logging.getLogger(__name__)


class BetAnalyzer:
    """
    Analizzatore di quote e value bet.
    
    Analizza le quote di scommessa confrontandole con le probabilità stimate
    per identificare opportunità di valore.
    """
    
    def __init__(self):
        """Inizializza l'analizzatore di scommesse."""
        self.db = FirebaseManager()
        self.margin_threshold = get_setting('analytics.betting.margin_threshold', 0.1)
        self.value_threshold = get_setting('analytics.betting.value_threshold', 0.05)
        self.confidence_threshold = get_setting('analytics.betting.confidence_threshold', 0.6)
        self.bookmakers = get_setting('analytics.betting.bookmakers', 
                                      ['average', 'bet365', 'williamhill', '1xbet', 'unibet'])
        
        logger.info(f"BetAnalyzer inizializzato con threshold={self.value_threshold}")
    
    @cached(ttl=1800)  # Cache di 30 minuti
    def analyze_match_odds(self, match_id: str) -> Dict[str, Any]:
        """
        Analizza le quote di una partita per identificare value bet.
        
        Args:
            match_id: ID della partita
            
        Returns:
            Analisi completa delle quote con value bet
        """
        logger.info(f"Analizzando quote per match_id={match_id}")
        
        try:
            # Ottieni i dati della partita
            match_ref = self.db.get_reference(f"data/matches/{match_id}")
            match_data = match_ref.get()
            
            if not match_data:
                logger.warning(f"Nessun dato trovato per match_id={match_id}")
                return self._create_empty_analysis(match_id)
            
            # Verifica che ci siano quote disponibili
            if 'odds' not in match_data or not match_data['odds']:
                logger.warning(f"Nessuna quota disponibile per match_id={match_id}")
                return self._create_empty_analysis(match_id)
            
            # Ottieni la previsione della partita
            prediction = predict_match(match_id)
            
            if not prediction.get('has_data', False):
                logger.warning(f"Nessuna previsione disponibile per match_id={match_id}")
                return self._create_empty_analysis(match_id)
            
            # Analizza le quote di ogni bookmaker
            bookmaker_analysis = {}
            for bookmaker in self.bookmakers:
                if bookmaker in match_data['odds']:
                    bookmaker_analysis[bookmaker] = self._analyze_bookmaker_odds(
                        match_data['odds'][bookmaker], prediction
                    )
            
            # Trova le migliori value bet
            value_bets = self._find_value_bets(bookmaker_analysis, prediction)
            
            # Calcola le quote comparate tra bookmaker
            odds_comparison = self._compare_bookmaker_odds(match_data['odds'])
            
            # Crea l'analisi completa
            analysis = {
                'match_id': match_id,
                'home_team': match_data.get('home_team', ''),
                'away_team': match_data.get('away_team', ''),
                'match_date': match_data.get('datetime', ''),
                'league_id': match_data.get('league_id', ''),
                'bookmaker_analysis': bookmaker_analysis,
                'value_bets': value_bets,
                'odds_comparison': odds_comparison,
                'analysis_date': datetime.now().isoformat(),
                'has_data': True
            }
            
            # Salva l'analisi nel database
            self._save_analysis(match_id, analysis)
            
            return analysis
            
        except Exception as e:
            logger.error(f"Errore nell'analisi delle quote per match_id={match_id}: {e}")
            return self._create_empty_analysis(match_id)
    
    def _analyze_bookmaker_odds(self, odds: Dict[str, Any], 
                              prediction: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analizza le quote di un bookmaker e le confronta con le probabilità previste.
        
        Args:
            odds: Quote del bookmaker
            prediction: Previsione della partita
            
        Returns:
            Analisi delle quote
        """
        analysis = {
            'markets': {},
            'margin': 0,
            'overall_value': 0
        }
        
        # Analizza le quote 1X2
        if 'standard' in odds and len(odds['standard']) >= 3:
            home_odds = odds['standard'].get('1', 0)
            draw_odds = odds['standard'].get('X', 0)
            away_odds = odds['standard'].get('2', 0)
            
            if home_odds > 0 and draw_odds > 0 and away_odds > 0:
                # Calcola il margine del bookmaker
                implied_prob_home = 1 / home_odds
                implied_prob_draw = 1 / draw_odds
                implied_prob_away = 1 / away_odds
                total_implied_prob = implied_prob_home + implied_prob_draw + implied_prob_away
                margin = total_implied_prob - 1
                
                # Salva il margine
                analysis['margin'] = margin
                
                # Calcola le probabilità "vere" (corrette per il margine)
                true_prob_home = implied_prob_home / total_implied_prob
                true_prob_draw = implied_prob_draw / total_implied_prob
                true_prob_away = implied_prob_away / total_implied_prob
                
                # Confronta con le probabilità previste
                predicted_prob_home = prediction['probabilities'].get('1', 0)
                predicted_prob_draw = prediction['probabilities'].get('X', 0)
                predicted_prob_away = prediction['probabilities'].get('2', 0)
                
                # Calcola il valore per ogni esito
                value_home = (predicted_prob_home * home_odds) - 1
                value_draw = (predicted_prob_draw * draw_odds) - 1
                value_away = (predicted_prob_away * away_odds) - 1
                
                # Aggiungi l'analisi 1X2
                analysis['markets']['1X2'] = {
                    'odds': {
                        '1': home_odds,
                        'X': draw_odds,
                        '2': away_odds
                    },
                    'implied_probabilities': {
                        '1': implied_prob_home,
                        'X': implied_prob_draw,
                        '2': implied_prob_away
                    },
                    'true_probabilities': {
                        '1': true_prob_home,
                        'X': true_prob_draw,
                        '2': true_prob_away
                    },
                    'predicted_probabilities': {
                        '1': predicted_prob_home,
                        'X': predicted_prob_draw,
                        '2': predicted_prob_away
                    },
                    'value': {
                        '1': value_home,
                        'X': value_draw,
                        '2': value_away
                    },
                    'highest_value': max(['1', 'X', '2'], key=lambda k: {'1': value_home, 'X': value_draw, '2': value_away}[k])
                }
                
                # Calcola il valore complessivo del mercato 1X2
                analysis['markets']['1X2']['overall_value'] = max(value_home, value_draw, value_away)
        
        # Analizza le quote BTTS
        if 'btts' in odds and len(odds['btts']) >= 2:
            yes_odds = odds['btts'].get('Yes', 0)
            no_odds = odds['btts'].get('No', 0)
            
            if yes_odds > 0 and no_odds > 0:
                # Calcola il margine del bookmaker
                implied_prob_yes = 1 / yes_odds
                implied_prob_no = 1 / no_odds
                total_implied_prob = implied_prob_yes + implied_prob_no
                btts_margin = total_implied_prob - 1
                
                # Calcola le probabilità "vere" (corrette per il margine)
                true_prob_yes = implied_prob_yes / total_implied_prob
                true_prob_no = implied_prob_no / total_implied_prob
                
                # Confronta con le probabilità previste
                predicted_prob_yes = prediction.get('btts', {}).get('Yes', 0)
                predicted_prob_no = 1 - predicted_prob_yes
                
                # Calcola il valore per ogni esito
                value_yes = (predicted_prob_yes * yes_odds) - 1
                value_no = (predicted_prob_no * no_odds) - 1
                
                # Aggiungi l'analisi BTTS
                analysis['markets']['BTTS'] = {
                    'odds': {
                        'Yes': yes_odds,
                        'No': no_odds
                    },
                    'implied_probabilities': {
                        'Yes': implied_prob_yes,
                        'No': implied_prob_no
                    },
                    'true_probabilities': {
                        'Yes': true_prob_yes,
                        'No': true_prob_no
                    },
                    'predicted_probabilities': {
                        'Yes': predicted_prob_yes,
                        'No': predicted_prob_no
                    },
                    'value': {
                        'Yes': value_yes,
                        'No': value_no
                    },
                    'highest_value': 'Yes' if value_yes > value_no else 'No',
                    'margin': btts_margin
                }
                
                # Calcola il valore complessivo del mercato BTTS
                analysis['markets']['BTTS']['overall_value'] = max(value_yes, value_no)
        
        # Analizza le quote Over/Under
        if 'over_under' in odds:
            for line, line_odds in odds['over_under'].items():
                if 'Over' in line_odds and 'Under' in line_odds:
                    over_odds = line_odds['Over']
                    under_odds = line_odds['Under']
                    
                    if over_odds > 0 and under_odds > 0:
                        # Calcola il margine del bookmaker
                        implied_prob_over = 1 / over_odds
                        implied_prob_under = 1 / under_odds
                        total_implied_prob = implied_prob_over + implied_prob_under
                        ou_margin = total_implied_prob - 1
                        
                        # Calcola le probabilità "vere" (corrette per il margine)
                        true_prob_over = implied_prob_over / total_implied_prob
                        true_prob_under = implied_prob_under / total_implied_prob
                        
                        # Confronta con le probabilità previste
                        predicted_prob_over = prediction.get('over_under', {}).get(line, {}).get('Over', 0)
                        predicted_prob_under = prediction.get('over_under', {}).get(line, {}).get('Under', 0)
                        
                        # Se non abbiamo la previsione per questa linea specifica, saltiamo
                        if predicted_prob_over == 0 and predicted_prob_under == 0:
                            continue
                        
                        # Calcola il valore per ogni esito
                        value_over = (predicted_prob_over * over_odds) - 1
                        value_under = (predicted_prob_under * under_odds) - 1
                        
                        # Aggiungi l'analisi Over/Under
                        market_key = f"Over/Under {line}"
                        analysis['markets'][market_key] = {
                            'odds': {
                                'Over': over_odds,
                                'Under': under_odds
                            },
                            'implied_probabilities': {
                                'Over': implied_prob_over,
                                'Under': implied_prob_under
                            },
                            'true_probabilities': {
                                'Over': true_prob_over,
                                'Under': true_prob_under
                            },
                            'predicted_probabilities': {
                                'Over': predicted_prob_over,
                                'Under': predicted_prob_under
                            },
                            'value': {
                                'Over': value_over,
                                'Under': value_under
                            },
                            'highest_value': 'Over' if value_over > value_under else 'Under',
                            'margin': ou_margin
                        }
                        
                        # Calcola il valore complessivo del mercato Over/Under
                        analysis['markets'][market_key]['overall_value'] = max(value_over, value_under)
        
        # Calcola il valore complessivo per tutti i mercati
        if analysis['markets']:
            market_values = [market['overall_value'] for market in analysis['markets'].values()]
            analysis['overall_value'] = max(market_values)
        
        return analysis
    
    def _find_value_bets(self, bookmaker_analysis: Dict[str, Dict[str, Any]], 
                       prediction: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Identifica le migliori value bet tra tutti i bookmaker.
        
        Args:
            bookmaker_analysis: Analisi delle quote per bookmaker
            prediction: Previsione della partita
            
        Returns:
            Lista di value bet
        """
        value_bets = []
        
        # Per ogni bookmaker
        for bookmaker, analysis in bookmaker_analysis.items():
            # Verifica se il margine è accettabile
            if analysis.get('margin', 0) > self.margin_threshold:
                # Bookmaker con margine troppo alto, saltiamo
                continue
            
            # Per ogni mercato
            for market_name, market_data in analysis.get('markets', {}).items():
                # Per ogni selezione nel mercato
                for selection, value in market_data.get('value', {}).items():
                    # La probabilità prevista
                    predicted_prob = market_data.get('predicted_probabilities', {}).get(selection, 0)
                    
                    # Verifica se è una value bet
                    if value > self.value_threshold and predicted_prob > self.confidence_threshold:
                        odds = market_data.get('odds', {}).get(selection, 0)
                        
                        value_bet = {
                            'bookmaker': bookmaker,
                            'market': market_name,
                            'selection': selection,
                            'odds': odds,
                            'value': value,
                            'predicted_probability': predicted_prob,
                            'expected_value': odds * predicted_prob,
                            'confidence': 'high' if predicted_prob > 0.75 else 'medium'
                        }
                        
                        value_bets.append(value_bet)
        
        # Ordina per valore decrescente
        value_bets.sort(key=lambda x: x['value'], reverse=True)
        
        # Limita a massimo 5 value bet
        return value_bets[:5]
    
    def _compare_bookmaker_odds(self, all_odds: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """
        Confronta le quote tra diversi bookmaker per trovare le migliori.
        
        Args:
            all_odds: Quote da tutti i bookmaker
            
        Returns:
            Confronto delle quote
        """
        comparison = {
            '1X2': {
                'best_odds': {'1': 0, 'X': 0, '2': 0},
                'best_bookmakers': {'1': '', 'X': '', '2': ''},
                'odds_range': {'1': [0, 0], 'X': [0, 0], '2': [0, 0]}
            },
            'BTTS': {
                'best_odds': {'Yes': 0, 'No': 0},
                'best_bookmakers': {'Yes': '', 'No': ''},
                'odds_range': {'Yes': [0, 0], 'No': [0, 0]}
            },
            'Over/Under': {}
        }
        
        # Confronta le quote 1X2
        for outcome in ['1', 'X', '2']:
            best_odds = 0
            best_bookmaker = ''
            all_values = []
            
            for bookmaker, odds in all_odds.items():
                if 'standard' in odds and outcome in odds['standard']:
                    odds_value = odds['standard'][outcome]
                    all_values.append(odds_value)
                    
                    if odds_value > best_odds:
                        best_odds = odds_value
                        best_bookmaker = bookmaker
            
            if all_values:
                comparison['1X2']['best_odds'][outcome] = best_odds
                comparison['1X2']['best_bookmakers'][outcome] = best_bookmaker
                comparison['1X2']['odds_range'][outcome] = [min(all_values), max(all_values)]
        
        # Confronta le quote BTTS
        for outcome in ['Yes', 'No']:
            best_odds = 0
            best_bookmaker = ''
            all_values = []
            
            for bookmaker, odds in all_odds.items():
                if 'btts' in odds and outcome in odds['btts']:
                    odds_value = odds['btts'][outcome]
                    all_values.append(odds_value)
                    
                    if odds_value > best_odds:
                        best_odds = odds_value
                        best_bookmaker = bookmaker
            
            if all_values:
                comparison['BTTS']['best_odds'][outcome] = best_odds
                comparison['BTTS']['best_bookmakers'][outcome] = best_bookmaker
                comparison['BTTS']['odds_range'][outcome] = [min(all_values), max(all_values)]
        
        # Confronta le quote Over/Under
        ou_lines = set()
        for bookmaker, odds in all_odds.items():
            if 'over_under' in odds:
                ou_lines.update(odds['over_under'].keys())
        
        for line in ou_lines:
            comparison['Over/Under'][line] = {
                'best_odds': {'Over': 0, 'Under': 0},
                'best_bookmakers': {'Over': '', 'Under': ''},
                'odds_range': {'Over': [0, 0], 'Under': [0, 0]}
            }
            
            for outcome in ['Over', 'Under']:
                best_odds = 0
                best_bookmaker = ''
                all_values = []
                
                for bookmaker, odds in all_odds.items():
                    if ('over_under' in odds and 
                        line in odds['over_under'] and 
                        outcome in odds['over_under'][line]):
                        
                        odds_value = odds['over_under'][line][outcome]
                        all_values.append(odds_value)
                        
                        if odds_value > best_odds:
                            best_odds = odds_value
                            best_bookmaker = bookmaker
                
                if all_values:
                    comparison['Over/Under'][line]['best_odds'][outcome] = best_odds
                    comparison['Over/Under'][line]['best_bookmakers'][outcome] = best_bookmaker
                    comparison['Over/Under'][line]['odds_range'][outcome] = [min(all_values), max(all_values)]
        
        return comparison
    
    def _create_empty_analysis(self, match_id: str) -> Dict[str, Any]:
        """
        Crea un'analisi vuota quando non ci sono dati sufficienti.
        
        Args:
            match_id: ID della partita
            
        Returns:
            Analisi vuota
        """
        return {
            'match_id': match_id,
            'home_team': '',
            'away_team': '',
            'match_date': '',
            'league_id': '',
            'bookmaker_analysis': {},
            'value_bets': [],
            'odds_comparison': {},
            'analysis_date': datetime.now().isoformat(),
            'has_data': False
        }
    
    def _save_analysis(self, match_id: str, analysis: Dict[str, Any]) -> None:
        """
        Salva l'analisi nel database.
        
        Args:
            match_id: ID della partita
            analysis: Analisi completa
        """
        try:
            analysis_ref = self.db.get_reference(f"bet_analysis/{match_id}")
            analysis_ref.set(analysis)
            logger.info(f"Analisi quote salvata con successo per match_id={match_id}")
        except Exception as e:
            logger.error(f"Errore nel salvare l'analisi quote per match_id={match_id}: {e}")
    
    @cached(ttl=3600 * 12)  # Cache di 12 ore
    def find_value_bets(self, league_id: Optional[str] = None, 
                       min_value: float = 0.1, 
                       max_matches: int = 20) -> List[Dict[str, Any]]:
        """
        Trova value bet in partite future.
        
        Args:
            league_id: ID del campionato (opzionale, per filtrare)
            min_value: Valore minimo richiesto
            max_matches: Numero massimo di partite da analizzare
            
        Returns:
            Lista di value bet
        """
        logger.info(f"Cercando value bet con min_value={min_value}")
        
        try:
            # Ottieni le partite future
            now = datetime.now()
            end_time = now + timedelta(days=3)  # Consideriamo i prossimi 3 giorni
            
            # Converti in formato ISO
            end_time_iso = end_time.isoformat() + 'Z'
            
            matches_ref = self.db.get_reference("data/matches")
            matches_query = matches_ref.order_by_child("datetime").end_at(end_time_iso)
            upcoming_matches = matches_query.get()
            
            if not upcoming_matches:
                logger.info(f"Nessuna partita trovata nei prossimi 3 giorni")
                return []
            
            # Filtra per partite future e, se specificato, per campionato
            value_bets = []
            matches_processed = 0
            
            for match_id, match in upcoming_matches.items():
                # Verifica se è una partita futura
                match_time = datetime.fromisoformat(match['datetime'].replace('Z', '+00:00'))
                
                if match_time <= now or match.get('status') == 'FINISHED':
                    continue
                
                # Verifica il campionato
                if league_id and match.get('league_id') != league_id:
                    continue
                
                # Analizza le quote
                try:
                    analysis = self.analyze_match_odds(match_id)
                    
                    if analysis.get('has_data', False) and analysis.get('value_bets'):
                        # Filtra per valore minimo
                        match_value_bets = [
                            bet for bet in analysis['value_bets'] 
                            if bet.get('value', 0) >= min_value
                        ]
                        
                        # Aggiungi dati partita
                        for bet in match_value_bets:
                            bet['match_id'] = match_id
                            bet['home_team'] = match.get('home_team', '')
                            bet['away_team'] = match.get('away_team', '')
                            bet['match_date'] = match['datetime']
                            bet['league_id'] = match.get('league_id', '')
                        
                        value_bets.extend(match_value_bets)
                    
                    matches_processed += 1
                    
                    # Controlla se abbiamo raggiunto il limite
                    if matches_processed >= max_matches:
                        break
                    
                    # Breve pausa per evitare sovraccarichi
                    time.sleep(0.5)
                except Exception as e:
                    logger.error(f"Errore nell'analisi delle quote per match_id={match_id}: {e}")
            
            # Ordina le value bet per valore decrescente
            value_bets.sort(key=lambda x: x['value'], reverse=True)
            
            logger.info(f"Trovate {len(value_bets)} value bet in {matches_processed} partite")
            return value_bets
            
        except Exception as e:
            logger.error(f"Errore nella ricerca di value bet: {e}")
            return []


# Funzioni di utilità per accesso globale
def analyze_match_odds(match_id: str) -> Dict[str, Any]:
    """
    Analizza le quote di una partita per identificare value bet.
    
    Args:
        match_id: ID della partita
        
    Returns:
        Analisi completa delle quote con value bet
    """
    analyzer = BetAnalyzer()
    return analyzer.analyze_match_odds(match_id)


def find_value_bets(league_id: Optional[str] = None, 
                  min_value: float = 0.1, 
                  max_matches: int = 20) -> List[Dict[str, Any]]:
    """
    Trova value bet in partite future.
    
    Args:
        league_id: ID del campionato (opzionale, per filtrare)
        min_value: Valore minimo richiesto
        max_matches: Numero massimo di partite da analizzare
        
    Returns:
        Lista di value bet
    """
    analyzer = BetAnalyzer()
    return analyzer.find_value_bets(league_id, min_value, max_matches)
