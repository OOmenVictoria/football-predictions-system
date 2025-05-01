# Funzioni di utilità per accesso globale
def get_team_xg_profile(team_id: str, matches_limit: int = 10) -> Dict[str, Any]:
    """
    Ottiene il profilo xG di una squadra.
    
    Args:
        team_id: ID della squadra
        matches_limit: Numero massimo di partite da considerare
        
    Returns:
        Profilo xG della squadra
    """
    analyzer = XGAnalyzer()
    return analyzer.get_team_xg_profile(team_id, matches_limit)


def analyze_match_xg(match_id: str) -> Dict[str, Any]:
    """
    Analizza i dati xG di una partita.
    
    Args:
        match_id: ID della partita
        
    Returns:
        Analisi xG della partita
    """
    analyzer = XGAnalyzer()
    return analyzer.analyze_match_xg(match_id)
"""
Modulo per l'analisi degli Expected Goals (xG).

Questo modulo fornisce funzionalità per analizzare i dati Expected Goals (xG)
delle squadre e delle partite, calcolando tendenze, confronti e predizioni basate su xG.
"""
import logging
import math
import json
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple, Union

from src.utils.cache import cached
from src.utils.database import FirebaseManager
from src.utils.time_utils import parse_date, date_to_str
from src.config.settings import get_setting

logger = logging.getLogger(__name__)


class XGAnalyzer:
    """
    Analizzatore di dati Expected Goals (xG).
    
    Fornisce strumenti per analizzare xG a livello di squadra e partita,
    calcolando tendenze, rilevando sovra/sotto-performance e generando insight.
    """
    
    def __init__(self):
        """Inizializza l'analizzatore xG."""
        self.db = FirebaseManager()
        self.min_matches = get_setting('analytics.xg.min_matches', 5)
        self.xg_sources = get_setting('analytics.xg.sources', 
                                    ['understat', 'fbref', 'sofascore', 'whoscored'])
        self.recency_factor = get_setting('analytics.xg.recency_factor', 0.9)
        logger.info(f"XGAnalyzer inizializzato con {self.min_matches} partite minime")
    
    @cached(ttl=3600)
    def get_team_xg_profile(self, team_id: str, matches_limit: int = 10) -> Dict[str, Any]:
        """
        Ottiene il profilo xG completo di una squadra.
        
        Args:
            team_id: ID della squadra
            matches_limit: Numero massimo di partite da considerare
            
        Returns:
            Profilo xG della squadra
        """
        logger.info(f"Calcolando profilo xG per team_id={team_id}")
        
        try:
            # Ottieni le ultime partite della squadra
            matches_ref = self.db.get_reference(f"data/matches")
            query_ref = matches_ref.order_by_child("datetime").limit_to_last(matches_limit * 2)
            all_matches = query_ref.get()
            
            if not all_matches:
                logger.warning(f"Nessuna partita trovata per team_id={team_id}")
                return self._create_empty_xg_profile(team_id)
            
            # Filtra le partite della squadra con dati xG
            team_matches = []
            for match_id, match in all_matches.items():
                if 'home_team_id' not in match or 'away_team_id' not in match:
                    continue
                    
                if match['home_team_id'] == team_id or match['away_team_id'] == team_id:
                    # Considera solo partite con dati xG
                    if 'xg' not in match or not match['xg']:
                        continue
                        
                    # Considera solo partite già giocate
                    if match.get('status') != 'FINISHED':
                        continue
                        
                    team_matches.append({
                        'match_id': match_id,
                        'date': match['datetime'],
                        'home_team_id': match['home_team_id'],
                        'away_team_id': match['away_team_id'],
                        'home_score': match.get('home_score', 0),
                        'away_score': match.get('away_score', 0),
                        'home_team': match.get('home_team', ''),
                        'away_team': match.get('away_team', ''),
                        'league_id': match.get('league_id', ''),
                        'xg': match['xg'],
                        'xg_sources': match.get('xg_sources', [])
                    })
            
            # Ordina le partite per data (dalla più recente alla più vecchia)
            team_matches.sort(key=lambda x: x['date'], reverse=True)
            
            # Limita alle ultime N partite
            team_matches = team_matches[:matches_limit]
            
            if len(team_matches) < self.min_matches:
                logger.warning(f"Dati xG insufficienti per team_id={team_id}: "
                             f"{len(team_matches)}/{self.min_matches}")
                return self._create_empty_xg_profile(team_id)
                
            # Analizza i dati xG
            return self._analyze_team_xg(team_id, team_matches)
            
        except Exception as e:
            logger.error(f"Errore nell'analisi xG per team_id={team_id}: {e}")
            return self._create_empty_xg_profile(team_id)
    
    def _analyze_team_xg(self, team_id: str, matches: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analizza i dati xG di una squadra dalle partite fornite.
        
        Args:
            team_id: ID della squadra
            matches: Lista di partite con dati xG
            
        Returns:
            Profilo xG completo della squadra
        """
        # Inizializza accumulatori
        home_xg = []
        away_xg = []
        home_xg_against = []
        away_xg_against = []
        home_goals = []
        away_goals = []
        home_goals_against = []
        away_goals_against = []
        match_details = []
        
        now = datetime.now()
        
        # Analizza ogni partita
        for match in matches:
            # Determina se la squadra era in casa o in trasferta
            is_home = match['home_team_id'] == team_id
            
            # Ottieni xG e gol
            team_xg = match['xg']['home'] if is_home else match['xg']['away']
            opponent_xg = match['xg']['away'] if is_home else match['xg']['home']
            team_goals = match['home_score'] if is_home else match['away_score']
            opponent_goals = match['away_score'] if is_home else match['home_score']
            
            # Calcola il peso in base alla recency
            match_date = parse_date(match['date'])
            days_ago = (now - match_date).days if match_date else 30  # Default 30 giorni se manca la data
            recency_weight = self.recency_factor ** (days_ago / 30)  # decadimento mensile
            
            # Aggiungi ai dati separati per casa/trasferta
            if is_home:
                home_xg.append((team_xg, recency_weight))
                home_xg_against.append((opponent_xg, recency_weight))
                home_goals.append((team_goals, recency_weight))
                home_goals_against.append((opponent_goals, recency_weight))
            else:
                away_xg.append((team_xg, recency_weight))
                away_xg_against.append((opponent_xg, recency_weight))
                away_goals.append((team_goals, recency_weight))
                away_goals_against.append((opponent_goals, recency_weight))
            
            # Aggiungi dettagli per questa partita
            opponent_id = match['away_team_id'] if is_home else match['home_team_id']
            opponent_name = match['away_team'] if is_home else match['home_team']
            
            # Calcola performance rispetto a xG
            xg_performance = team_goals - team_xg
            xg_defense_performance = opponent_xg - opponent_goals
            
            match_details.append({
                'match_id': match['match_id'],
                'date': match['date'],
                'opponent_id': opponent_id,
                'opponent_name': opponent_name,
                'home_away': 'home' if is_home else 'away',
                'team_xg': team_xg,
                'opponent_xg': opponent_xg,
                'team_goals': team_goals,
                'opponent_goals': opponent_goals,
                'xg_performance': xg_performance,
                'xg_defense_performance': xg_defense_performance,
                'xg_sources': match.get('xg_sources', []),
                'weight': recency_weight
            })
        
        # Calcola medie ponderate
        home_xg_avg = self._weighted_average(home_xg)
        away_xg_avg = self._weighted_average(away_xg)
        home_xg_against_avg = self._weighted_average(home_xg_against)
        away_xg_against_avg = self._weighted_average(away_xg_against)
        home_goals_avg = self._weighted_average(home_goals)
        away_goals_avg = self._weighted_average(away_goals)
        home_goals_against_avg = self._weighted_average(home_goals_against)
        away_goals_against_avg = self._weighted_average(away_goals_against)
        
        # Calcola medie complessive
        all_xg = home_xg + away_xg
        all_xg_against = home_xg_against + away_xg_against
        all_goals = home_goals + away_goals
        all_goals_against = home_goals_against + away_goals_against
        
        overall_xg_avg = self._weighted_average(all_xg)
        overall_xg_against_avg = self._weighted_average(all_xg_against)
        overall_goals_avg = self._weighted_average(all_goals)
        overall_goals_against_avg = self._weighted_average(all_goals_against)
        
        # Calcola performance complessiva rispetto a xG
        attack_performance = overall_goals_avg - overall_xg_avg
        defense_performance = overall_xg_against_avg - overall_goals_against_avg
        
        # Calcola le fonti xG utilizzate
        xg_sources_used = set()
        for match in match_details:
            for source in match.get('xg_sources', []):
                xg_sources_used.add(source)
        
        # Costruisci il profilo
        return {
            'team_id': team_id,
            'matches_analyzed': len(matches),
            'home_matches': len(home_xg),
            'away_matches': len(away_xg),
            'home': {
                'xg': home_xg_avg,
                'xg_against': home_xg_against_avg,
                'goals': home_goals_avg,
                'goals_against': home_goals_against_avg,
                'attack_performance': home_goals_avg - home_xg_avg,
                'defense_performance': home_xg_against_avg - home_goals_against_avg
            },
            'away': {
                'xg': away_xg_avg,
                'xg_against': away_xg_against_avg,
                'goals': away_goals_avg,
                'goals_against': away_goals_against_avg,
                'attack_performance': away_goals_avg - away_xg_avg,
                'defense_performance': away_xg_against_avg - away_goals_against_avg
            },
            'overall': {
                'xg': overall_xg_avg,
                'xg_against': overall_xg_against_avg,
                'goals': overall_goals_avg,
                'goals_against': overall_goals_against_avg,
                'attack_performance': attack_performance,
                'defense_performance': defense_performance,
                'xg_difference': overall_xg_avg - overall_xg_against_avg,
                'goal_difference': overall_goals_avg - overall_goals_against_avg
            },
            'performance_rating': self._calculate_performance_rating(
                attack_performance, defense_performance, overall_xg_avg, overall_xg_against_avg
            ),
            'match_details': match_details,
            'xg_sources_used': list(xg_sources_used),
            'analysis_date': datetime.now().isoformat(),
            'has_data': True
        }
    
    def _weighted_average(self, values: List[Tuple[float, float]]) -> float:
        """
        Calcola la media ponderata di una lista di valori.
        
        Args:
            values: Lista di tuple (valore, peso)
            
        Returns:
            Media ponderata
        """
        if not values:
            return 0.0
        return sum(v * w for v, w in values) / sum(w for _, w in values)
    
    def _calculate_performance_rating(self, attack_performance: float, defense_performance: float,
                                    xg_for: float, xg_against: float) -> Dict[str, Any]:
        """
        Calcola un rating di performance xG complessivo.
        
        Args:
            attack_performance: Performance offensiva (gol - xG)
            defense_performance: Performance difensiva (xG subiti - gol subiti)
            xg_for: Media xG creati
            xg_against: Media xG concessi
            
        Returns:
            Rating di performance con punteggio e interpretazione
        """
        # Calcola un punteggio di performance (0-100)
        # Combina performance offensiva e difensiva, pesate per il volume di xG
        
        # Se entrambi i valori xG sono pari a 0, restituisci un valore predefinito
        if xg_for == 0 and xg_against == 0:
            return {
                'score': 50,
                'attack_rating': 'neutrale',
                'defense_rating': 'neutrale',
                'overall_rating': 'neutrale',
                'description': "Dati insufficienti per un'analisi accurata."
            }
        
        # Normalizza i valori di performance in base al volume di xG
        if xg_for > 0:
            normalized_attack = attack_performance / xg_for
        else:
            normalized_attack = 0
        
        if xg_against > 0:
            normalized_defense = defense_performance / xg_against
        else:
            normalized_defense = 0
        
        # Limita a intervalli ragionevoli (-1 a +1)
        normalized_attack = max(-1, min(1, normalized_attack))
        normalized_defense = max(-1, min(1, normalized_defense))
        
        # Converti a punteggio 0-100
        attack_score = (normalized_attack + 1) * 50
        defense_score = (normalized_defense + 1) * 50
        
        # Combina i punteggi (60% attacco, 40% difesa)
        overall_score = 0.6 * attack_score + 0.4 * defense_score
        
        # Determina rating testuale
        attack_rating = self._get_performance_text(normalized_attack)
        defense_rating = self._get_performance_text(normalized_defense)
        
        # Rating complessivo
        overall_rating = self._get_overall_rating(overall_score)
        
        # Descrizione in base ai punteggi
        description = self._generate_performance_description(
            normalized_attack, normalized_defense, xg_for, xg_against
        )
        
        return {
            'score': overall_score,
            'attack_score': attack_score,
            'defense_score': defense_score,
            'attack_rating': attack_rating,
            'defense_rating': defense_rating,
            'overall_rating': overall_rating,
            'description': description
        }
    
    def _get_performance_text(self, normalized_performance: float) -> str:
        """
        Converte un valore di performance normalizzato in un rating testuale.
        
        Args:
            normalized_performance: Performance normalizzata (-1 a +1)
            
        Returns:
            Rating testuale
        """
        if normalized_performance > 0.5:
            return "eccellente"
        elif normalized_performance > 0.25:
            return "molto buona"
        elif normalized_performance > 0.1:
            return "buona"
        elif normalized_performance > -0.1:
            return "neutrale"
        elif normalized_performance > -0.25:
            return "sottoperformante"
        elif normalized_performance > -0.5:
            return "scarsa"
        else:
            return "molto scarsa"
    
    def _get_overall_rating(self, score: float) -> str:
        """
        Converte un punteggio complessivo in un rating testuale.
        
        Args:
            score: Punteggio 0-100
            
        Returns:
            Rating testuale complessivo
        """
        if score > 75:
            return "elite"
        elif score > 65:
            return "eccellente"
        elif score > 55:
            return "buona"
        elif score > 45:
            return "media"
        elif score > 35:
            return "sotto media"
        elif score > 25:
            return "scarsa"
        else:
            return "molto scarsa"
    
    def _generate_performance_description(self, attack_perf: float, defense_perf: float,
                                        xg_for: float, xg_against: float) -> str:
        """
        Genera una descrizione testuale della performance.
        
        Args:
            attack_perf: Performance offensiva normalizzata
            defense_perf: Performance difensiva normalizzata
            xg_for: Media xG creati
            xg_against: Media xG concessi
            
        Returns:
            Descrizione testuale
        """
        descriptions = []
        
        # Descrizione attacco
        if attack_perf > 0.25:
            descriptions.append(f"Eccellente finalizzazione: la squadra segna {attack_perf * 100:.1f}% più gol di quanto previsto dagli xG.")
        elif attack_perf > 0.1:
            descriptions.append(f"Buona finalizzazione: la squadra sovraperforma leggermente i propri xG.")
        elif attack_perf < -0.25:
            descriptions.append(f"Gravi problemi di finalizzazione: la squadra segna {-attack_perf * 100:.1f}% meno gol di quanto previsto dagli xG.")
        elif attack_perf < -0.1:
            descriptions.append("Finalizzazione inefficace: la squadra non concretizza a sufficienza le proprie occasioni.")
        
        # Descrizione difesa
        if defense_perf > 0.25:
            descriptions.append(f"Difesa eccellente: la squadra concede {defense_perf * 100:.1f}% meno gol di quanto previsto dagli xG.")
        elif defense_perf > 0.1:
            descriptions.append("Buone prestazioni difensive: portiere e difesa riducono i gol subiti rispetto agli xG concessi.")
        elif defense_perf < -0.25:
            descriptions.append(f"Gravi problemi difensivi: la squadra subisce {-defense_perf * 100:.1f}% più gol di quanto previsto dagli xG.")
        elif defense_perf < -0.1:
            descriptions.append("Difesa vulnerabile: la squadra tende a subire più gol di quanto suggerito dagli xG.")
        
        # Volume xG
        if xg_for > 2.0:
            descriptions.append(f"Alto volume offensivo: crea {xg_for:.2f} xG per partita.")
        elif xg_for < 1.0:
            descriptions.append(f"Basso volume offensivo: crea solo {xg_for:.2f} xG per partita.")
            
        if xg_against > 2.0:
            descriptions.append(f"Difesa porosa: concede {xg_against:.2f} xG per partita.")
        elif xg_against < 1.0:
            descriptions.append(f"Difesa solida: concede solo {xg_against:.2f} xG per partita.")
        
        # Se non ci sono descrizioni specifiche
        if not descriptions:
            descriptions.append("Performance complessivamente nella media rispetto alle aspettative xG.")
        
        return " ".join(descriptions)
    
    def _create_empty_xg_profile(self, team_id: str) -> Dict[str, Any]:
        """
        Crea un profilo xG vuoto quando non ci sono dati sufficienti.
        
        Args:
            team_id: ID della squadra
            
        Returns:
            Profilo xG vuoto
        """
        return {
            'team_id': team_id,
            'matches_analyzed': 0,
            'home_matches': 0,
            'away_matches': 0,
            'home': {
                'xg': 0.0,
                'xg_against': 0.0,
                'goals': 0.0,
                'goals_against': 0.0,
                'attack_performance': 0.0,
                'defense_performance': 0.0
            },
            'away': {
                'xg': 0.0,
                'xg_against': 0.0,
                'goals': 0.0,
                'goals_against': 0.0,
                'attack_performance': 0.0,
                'defense_performance': 0.0
            },
            'overall': {
                'xg': 0.0,
                'xg_against': 0.0,
                'goals': 0.0,
                'goals_against': 0.0,
                'attack_performance': 0.0,
                'defense_performance': 0.0,
                'xg_difference': 0.0,
                'goal_difference': 0.0
            },
            'performance_rating': {
                'score': 50.0,
                'attack_rating': 'neutrale',
                'defense_rating': 'neutrale',
                'overall_rating': 'neutrale',
                'description': "Dati insufficienti per l'analisi."
            },
            'match_details': [],
            'xg_sources_used': [],
            'analysis_date': datetime.now().isoformat(),
            'has_data': False
        }
    
    @cached(ttl=3600 * 6)  # 6 ore
    def analyze_match_xg(self, match_id: str) -> Dict[str, Any]:
        """
        Analizza i dati xG di una partita specifica.
        
        Args:
            match_id: ID della partita
            
        Returns:
            Analisi xG della partita
        """
        logger.info(f"Analizzando xG per match_id={match_id}")
        
        try:
            # Ottieni i dati della partita
            match_ref = self.db.get_reference(f"data/matches/{match_id}")
            match_data = match_ref.get()
            
            if not match_data:
                logger.warning(f"Nessun dato trovato per match_id={match_id}")
                return self._create_empty_match_analysis(match_id)
            
            # Verifica che ci siano dati xG
            if 'xg' not in match_data or not match_data['xg']:
                logger.warning(f"Nessun dato xG per match_id={match_id}")
                return self._create_empty_match_analysis(match_id)
            
            # Se la partita è stata giocata, analizza i risultati reali vs xG
            if match_data.get('status') == 'FINISHED':
                return self._analyze_completed_match(match_data)
            else:
                # Per partite future, fai una previsione basata su xG storici
                return self._analyze_upcoming_match(match_data)
                
        except Exception as e:
            logger.error(f"Errore nell'analisi xG per match_id={match_id}: {e}")
            return self._create_empty_match_analysis(match_id)
    
    def _analyze_completed_match(self, match_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analizza una partita completata confrontando i risultati reali con xG.
        
        Args:
            match_data: Dati della partita
            
        Returns:
            Analisi xG della partita completata
        """
        match_id = match_data.get('match_id', '')
        home_team_id = match_data.get('home_team_id', '')
        away_team_id = match_data.get('away_team_id', '')
        home_team = match_data.get('home_team', '')
        away_team = match_data.get('away_team', '')
        home_xg = match_data['xg'].get('home', 0.0)
        away_xg = match_data['xg'].get('away', 0.0)
        home_score = match_data.get('home_score', 0)
        away_score = match_data.get('away_score', 0)
        
        # Calcola le performance rispetto a xG
        home_performance = home_score - home_xg
        away_performance = away_score - away_xg
        
        # Calcola risultato xG vs risultato reale
        xg_result = 'draw'
        if home_xg > away_xg:
            xg_result = 'home'
        elif away_xg > home_xg:
            xg_result = 'away'
            
        actual_result = 'draw'
        if home_score > away_score:
            actual_result = 'home'
        elif away_score > home_score:
            actual_result = 'away'
        
        # Determina se il risultato xG coincide con quello reale
        result_matched = xg_result == actual_result
        
        # Calcola la differenza di xG
        xg_difference = abs(home_xg - away_xg)
        
        # Determina quanto il risultato era "atteso" in base alla differenza xG
        if xg_difference < 0.3:
            expectedness = "equilibrato"
        elif xg_difference < 1.0:
            expectedness = "leggero favorito"
        elif xg_difference < 2.0:
            expectedness = "favorito"
        else:
            expectedness = "forte favorito"
        
        # Genera una descrizione dell'analisi
        description = []
        
        # Descrivi il risultato previsto vs reale
        if result_matched:
            if xg_result == 'home':
                description.append(f"{home_team} ha vinto sia per xG ({home_xg:.2f} vs {away_xg:.2f}) che nel risultato finale ({home_score}-{away_score}).")
            elif xg_result == 'away':
                description.append(f"{away_team} ha vinto sia per xG ({away_xg:.2f} vs {home_xg:.2f}) che nel risultato finale ({home_score}-{away_score}).")
            else:
                description.append(f"Pareggio sia per xG ({home_xg:.2f} vs {away_xg:.2f}) che nel risultato finale ({home_score}-{away_score}).")
        else:
            if xg_result == 'home' and actual_result == 'away':
                description.append(f"{home_team} ha dominato per xG ({home_xg:.2f} vs {away_xg:.2f}) ma {away_team} ha vinto {home_score}-{away_score}.")
            elif xg_result == 'away' and actual_result == 'home':
                description.append(f"{away_team} ha dominato per xG ({away_xg:.2f} vs {home_xg:.2f}) ma {home_team} ha vinto {home_score}-{away_score}.")
            elif xg_result == 'home' and actual_result == 'draw':
                description.append(f"{home_team} ha dominato per xG ({home_xg:.2f} vs {away_xg:.2f}) ma la partita è finita in pareggio {home_score}-{away_score}.")
            elif xg_result == 'away' and actual_result == 'draw':
                description.append(f"{away_team} ha dominato per xG ({away_xg:.2f} vs {home_xg:.2f}) ma la partita è finita in pareggio {home_score}-{away_score}.")
            elif xg_result == 'draw' and actual_result == 'home':
                description.append(f"Gli xG erano equilibrati ({home_xg:.2f} vs {away_xg:.2f}) ma {home_team} ha vinto {home_score}-{away_score}.")
            elif xg_result == 'draw' and actual_result == 'away':
                description.append(f"Gli xG erano equilibrati ({home_xg:.2f} vs {away_xg:.2f}) ma {away_team} ha vinto {home_score}-{away_score}.")
        
        # Aggiungi dettagli sulla performance
        if home_performance > 0.5:
            description.append(f"{home_team} ha sovraperformato notevolmente gli xG (+{home_performance:.2f}).")
        elif home_performance < -0.5:
            description.append(f"{home_team} ha sottoperformato notevolmente gli xG ({home_performance:.2f}).")
            
        if away_performance > 0.5:
            description.append(f"{away_team} ha sovraperformato notevolmente gli xG (+{away_performance:.2f}).")
        elif away_performance < -0.5:
            description.append(f"{away_team} ha sottoperformato notevolmente gli xG ({away_performance:.2f}).")
        
        # Aggiungi dettagli sulla fortuna/sfortuna
        if not result_matched:
            if (xg_result == 'home' and actual_result == 'away') or (xg_result == 'away' and actual_result == 'home'):
                description.append(f"Risultato contrario alle aspettative xG: possibile indicazione di efficienza clinica o fortuna.")
            
        # Converti la lista in un testo
        analysis_description = " ".join(description)
        
        # Risultato dell'analisi
        return {
            'match_id': match_id,
            'home_team_id': home_team_id,
            'away_team_id': away_team_id,
            'home_team': home_team,
            'away_team': away_team,
            'match_date': match_data.get('datetime', ''),
            'xg': {
                'home': home_xg,
                'away': away_xg,
                'difference': home_xg - away_xg,
                'total': home_xg + away_xg,
                'result': xg_result
            },
            'actual': {
                'home_score': home_score,
                'away_score': away_score,
                'result': actual_result
            },
            'performance': {
                'home': home_performance,
                'away': away_performance,
                'result_matched': result_matched,
                'expectedness': expectedness,
                'xg_difference': xg_difference
            },
            'analysis': {
                'description': analysis_description,
                'luck_factor': 'lucky_winner' if not result_matched else 'expected',
                'efficiency': {
                    'home': home_performance / home_xg if home_xg > 0 else 0,
                    'away': away_performance / away_xg if away_xg > 0 else 0
                }
            },
            'xg_sources': match_data.get('xg_sources', []),
            'analysis_date': datetime.now().isoformat(),
            'has_data': True
        }
