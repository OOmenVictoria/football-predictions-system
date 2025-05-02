"""
Test per i moduli processori dei dati calcistici.
Questo modulo contiene test per verificare il corretto funzionamento
dei processori che elaborano e normalizzano i dati calcistici.
"""
import os
import sys
import unittest
from unittest.mock import patch, MagicMock
import json
from datetime import datetime

# Aggiungi la directory radice al path di Python per permettere import relativi
test_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(test_dir))
sys.path.insert(0, root_dir)

from src.data.processors.matches import MatchProcessor
from src.data.processors.teams import TeamProcessor
from src.data.processors.head_to_head import HeadToHeadProcessor
from src.data.processors.xg_processor import XGProcessor
from src.data.processors.standings import StandingsProcessor

class TestMatchProcessor(unittest.TestCase):
    """Test per il processore di partite."""
    
    def setUp(self):
        """Setup per i test."""
        self.processor = MatchProcessor()
        
        # Carica dati di test
        with open(os.path.join(test_dir, 'fixtures', 'match_data.json'), 'r', encoding='utf-8') as f:
            self.test_match = json.load(f)
    
    def test_process_match(self):
        """Test elaborazione di una partita."""
        # Processa i dati di test
        processed = self.processor.process_match(self.test_match, "football_data")
        
        # Verifiche
        self.assertIsInstance(processed, dict)
        self.assertIn('home_xg', processed)
        self.assertIn('away_xg', processed)
        self.assertIn('xg_source', processed)
        
        # Verifica valori specifici
        self.assertTrue(0 <= processed['home_xg'] <= 5)  # Range ragionevole per xG
        self.assertTrue(0 <= processed['away_xg'] <= 5)  # Range ragionevole per xG
    
    def test_process_team_xg_history(self):
        """Test elaborazione storico xG di una squadra."""
        # Estrai dati di test
        team_matches = self.test_data['team_matches']
        
        # Processa i dati
        processed = self.processor.process_team_xg_history("100", team_matches)
        
        # Verifiche
        self.assertIsInstance(processed, dict)
        self.assertIn('team_id', processed)
        self.assertIn('matches_count', processed)
        self.assertIn('average_xg', processed)
        self.assertIn('average_xg_against', processed)
        self.assertIn('home_xg', processed)
        self.assertIn('away_xg', processed)
        
        # Verifica che la media totale sia ragionevole
        self.assertTrue(0 <= processed['average_xg'] <= 5)
    
    def test_combine_xg_sources(self):
        """Test combinazione di dati xG da diverse fonti."""
        # Estrai il metodo privato per il test
        combine_method = self.processor._combine_xg_sources
        
        # Crea dati xG di test da diverse fonti
        xg_data = {
            "understat": {"home": 1.5, "away": 1.2},
            "fbref": {"home": 1.7, "away": 1.0},
            "sofascore": {"home": 1.4, "away": 1.3}
        }
        
        # Combina le fonti
        combined = combine_method(xg_data, weights={"understat": 0.5, "fbref": 0.3, "sofascore": 0.2})
        
        # Verifiche
        self.assertIsInstance(combined, dict)
        self.assertIn('home_xg', combined)
        self.assertIn('away_xg', combined)
        self.assertIn('sources', combined)
        
        # Verifica che la combinazione sia corretta (media ponderata)
        expected_home = 1.5 * 0.5 + 1.7 * 0.3 + 1.4 * 0.2
        expected_away = 1.2 * 0.5 + 1.0 * 0.3 + 1.3 * 0.2
        self.assertAlmostEqual(combined['home_xg'], expected_home, places=2)
        self.assertAlmostEqual(combined['away_xg'], expected_away, places=2)

class TestStandingsProcessor(unittest.TestCase):
    """Test per il processore di classifiche."""
    
    def setUp(self):
        """Setup per i test."""
        self.processor = StandingsProcessor()
        
        # Carica dati di test
        with open(os.path.join(test_dir, 'fixtures', 'standings_data.json'), 'r', encoding='utf-8') as f:
            self.test_data = json.load(f)
    
    def test_process_league_standings(self):
        """Test elaborazione classifica di un campionato."""
        # Estrai dati di test
        standings = self.test_data['standings']
        
        # Processa i dati
        processed = self.processor.process_league_standings(standings, "football_data")
        
        # Verifiche
        self.assertIsInstance(processed, dict)
        self.assertIn('league_id', processed)
        self.assertIn('season', processed)
        self.assertIn('standings', processed)
        self.assertIsInstance(processed['standings'], list)
        
        # Verifica struttura di una entry della classifica
        if processed['standings']:
            team = processed['standings'][0]
            self.assertIn('position', team)
            self.assertIn('team_id', team)
            self.assertIn('team_name', team)
            self.assertIn('played', team)
            self.assertIn('points', team)
    
    def test_normalize_football_data_standings(self):
        """Test normalizzazione classifica da Football-Data."""
        # Estrai il metodo privato per il test
        normalize_method = self.processor._normalize_football_data
        
        # Crea una classifica di test
        standings_data = {
            "competition": {"id": 2019, "name": "Serie A"},
            "season": {"id": 2023, "currentMatchday": 30},
            "standings": [{
                "stage": "REGULAR_SEASON",
                "type": "TOTAL",
                "table": [
                    {
                        "position": 1,
                        "team": {"id": 100, "name": "AC Milan"},
                        "playedGames": 30,
                        "won": 20,
                        "draw": 5,
                        "lost": 5,
                        "points": 65,
                        "goalsFor": 50,
                        "goalsAgainst": 25
                    },
                    {
                        "position": 2,
                        "team": {"id": 200, "name": "Inter"},
                        "playedGames": 30,
                        "won": 19,
                        "draw": 5,
                        "lost": 6,
                        "points": 62,
                        "goalsFor": 48,
                        "goalsAgainst": 30
                    }
                ]
            }]
        }
        
        # Normalizza i dati
        normalized = normalize_method(standings_data)
        
        # Verifiche
        self.assertIsInstance(normalized, dict)
        self.assertEqual(normalized['league_id'], "2019")
        self.assertEqual(normalized['season'], "2023")
        self.assertIsInstance(normalized['standings'], list)
        self.assertEqual(len(normalized['standings']), 2)
        
        # Verifica struttura delle entries
        team = normalized['standings'][0]
        self.assertEqual(team['position'], 1)
        self.assertEqual(team['team_id'], "100")
        self.assertEqual(team['team_name'], "AC Milan")
        self.assertEqual(team['played'], 30)
        self.assertEqual(team['points'], 65)
    
    def test_enrich_standings(self):
        """Test arricchimento classifica."""
        # Crea una classifica di test
        standings = {
            "league_id": "2019",
            "season": "2023",
            "standings": [
                {
                    "position": 1,
                    "team_id": "100",
                    "team_name": "AC Milan",
                    "played": 30,
                    "won": 20,
                    "draw": 5,
                    "lost": 5,
                    "points": 65,
                    "goals_for": 50,
                    "goals_against": 25
                },
                {
                    "position": 2,
                    "team_id": "200",
                    "team_name": "Inter",
                    "played": 30,
                    "won": 19,
                    "draw": 5,
                    "lost": 6,
                    "points": 62,
                    "goals_for": 48,
                    "goals_against": 30
                }
            ]
        }
        
        # Arricchisci i dati
        enriched = self.processor.enrich_standings(standings)
        
        # Verifiche
        self.assertIsInstance(enriched, dict)
        self.assertIn('league_id', enriched)
        self.assertIn('season', enriched)
        self.assertIn('standings', enriched)
        
        # Verifica campi aggiunti
        for team in enriched['standings']:
            self.assertIn('form', team)
            self.assertIn('goal_difference', team)
            self.assertIn('win_percentage', team)
            self.assertIn('points_per_game', team)
    
    def test_get_team_position(self):
        """Test ottenimento posizione in classifica."""
        # Crea una classifica di test
        standings = {
            "league_id": "2019",
            "season": "2023",
            "standings": [
                {
                    "position": 1,
                    "team_id": "100",
                    "team_name": "AC Milan",
                    "points": 65
                },
                {
                    "position": 2,
                    "team_id": "200",
                    "team_name": "Inter",
                    "points": 62
                },
                {
                    "position": 3,
                    "team_id": "300",
                    "team_name": "Juventus",
                    "points": 60
                }
            ]
        }
        
        # Ottieni la posizione
        position = self.processor.get_team_position("200", standings)
        
        # Verifiche
        self.assertEqual(position, 2)
        
        # Verifica con ID non presente
        position_not_found = self.processor.get_team_position("999", standings)
        self.assertEqual(position_not_found, -1)

if __name__ == "__main__":
    unittest.main()self.assertIsInstance(processed, dict)
        
        # Verifica campi obbligatori
        required_fields = ['match_id', 'home_team', 'away_team', 'datetime', 'status']
        for field in required_fields:
            self.assertIn(field, processed)
        
        # Verifica normalizzazione
        self.assertIn('source_ids', processed)
        self.assertIn('football_data', processed['source_ids'])
    
    def test_normalize_football_data(self):
        """Test normalizzazione dati da Football-Data."""
        # Estrai il metodo privato per il test
        normalize_method = self.processor._normalize_football_data
        
        # Crea un match di test
        match = {
            "id": 1234,
            "homeTeam": {"id": 100, "name": "AC Milan"},
            "awayTeam": {"id": 200, "name": "Inter"},
            "utcDate": "2023-05-20T20:45:00Z",
            "status": "SCHEDULED",
            "matchday": 30
        }
        
        # Normalizza i dati
        normalized = {}
        normalize_method(match, normalized)
        
        # Verifiche
        self.assertEqual(normalized['match_id'], "1234")
        self.assertEqual(normalized['home_team'], "AC Milan")
        self.assertEqual(normalized['away_team'], "Inter")
        self.assertEqual(normalized['datetime'], "2023-05-20T20:45:00Z")
        self.assertEqual(normalized['status'], "SCHEDULED")
    
    def test_normalize_api_football(self):
        """Test normalizzazione dati da API-Football."""
        # Estrai il metodo privato per il test
        normalize_method = self.processor._normalize_api_football
        
        # Crea un match di test
        match = {
            "fixture": {
                "id": 1234,
                "date": "2023-05-20T20:45:00Z",
                "status": {"short": "NS"}
            },
            "teams": {
                "home": {"id": 100, "name": "AC Milan"},
                "away": {"id": 200, "name": "Inter"}
            },
            "league": {"round": "Regular Season - 30"}
        }
        
        # Normalizza i dati
        normalized = {}
        normalize_method(match, normalized)
        
        # Verifiche
        self.assertEqual(normalized['match_id'], "1234")
        self.assertEqual(normalized['home_team'], "AC Milan")
        self.assertEqual(normalized['away_team'], "Inter")
        self.assertEqual(normalized['datetime'], "2023-05-20T20:45:00Z")
        self.assertEqual(normalized['status'], "SCHEDULED")
    
    def test_enrich_match_data(self):
        """Test arricchimento dati partita."""
        # Mock per i dati di arricchimento
        self.processor.get_team_info = MagicMock(return_value={"name": "Team", "logo": "http://example.com/logo.png"})
        self.processor.get_match_stats = MagicMock(return_value={"possession": {"home": 60, "away": 40}})
        
        # Crea un match di test
        match = {
            "match_id": "1234",
            "home_team": "AC Milan",
            "home_team_id": "100",
            "away_team": "Inter",
            "away_team_id": "200",
            "datetime": "2023-05-20T20:45:00Z",
            "status": "SCHEDULED"
        }
        
        # Arricchisci i dati
        enriched = self.processor.enrich_match_data(match)
        
        # Verifiche
        self.assertIn('home_team_info', enriched)
        self.assertIn('away_team_info', enriched)
        self.assertIn('stats', enriched)

class TestTeamProcessor(unittest.TestCase):
    """Test per il processore di squadre."""
    
    def setUp(self):
        """Setup per i test."""
        self.processor = TeamProcessor()
        
        # Carica dati di test
        with open(os.path.join(test_dir, 'fixtures', 'team_data.json'), 'r', encoding='utf-8') as f:
            self.test_team = json.load(f)
    
    def test_process_team(self):
        """Test elaborazione di una squadra."""
        # Processa i dati di test
        processed = self.processor.process_team(self.test_team, "football_data")
        
        # Verifiche
        self.assertIsInstance(processed, dict)
        
        # Verifica campi obbligatori
        required_fields = ['team_id', 'name', 'country', 'founded']
        for field in required_fields:
            self.assertIn(field, processed)
        
        # Verifica normalizzazione
        self.assertIn('source_ids', processed)
        self.assertIn('football_data', processed['source_ids'])
    
    def test_normalize_football_data(self):
        """Test normalizzazione dati squadra da Football-Data."""
        # Estrai il metodo privato per il test
        normalize_method = self.processor._normalize_football_data
        
        # Crea una squadra di test
        team = {
            "id": 100,
            "name": "AC Milan",
            "shortName": "Milan",
            "tla": "MIL",
            "crest": "http://example.com/logo.png",
            "address": "Via Aldo Rossi, 8, Milan",
            "founded": 1899,
            "clubColors": "Red / Black",
            "venue": "San Siro"
        }
        
        # Normalizza i dati
        normalized = {}
        normalize_method(team, normalized)
        
        # Verifiche
        self.assertEqual(normalized['team_id'], "100")
        self.assertEqual(normalized['name'], "AC Milan")
        self.assertEqual(normalized['short_name'], "Milan")
        self.assertEqual(normalized['founded'], 1899)
        self.assertEqual(normalized['stadium'], "San Siro")
    
    def test_normalize_transfermarkt(self):
        """Test normalizzazione dati squadra da Transfermarkt."""
        # Estrai il metodo privato per il test
        normalize_method = self.processor._normalize_transfermarkt
        
        # Crea una squadra di test
        team = {
            "id": "AC-Milan",
            "name": "AC Milan",
            "country": "Italy",
            "founded": 1899,
            "stadium": "San Siro",
            "capacity": 80018,
            "market_value": "€500.00m",
            "squad_size": 28,
            "average_age": 26.5
        }
        
        # Normalizza i dati
        normalized = {}
        normalize_method(team, normalized)
        
        # Verifiche
        self.assertEqual(normalized['team_id'], "AC-Milan")
        self.assertEqual(normalized['name'], "AC Milan")
        self.assertEqual(normalized['country'], "Italy")
        self.assertEqual(normalized['founded'], 1899)
        self.assertEqual(normalized['stadium'], "San Siro")
        self.assertEqual(normalized['market_value'], 500000000)  # Convertito in intero
    
    def test_enrich_team_data(self):
        """Test arricchimento dati squadra."""
        # Mock per i dati di arricchimento
        self.processor.get_team_stats = MagicMock(return_value={
            "matches_played": 30,
            "wins": 20,
            "draws": 5,
            "losses": 5
        })
        
        # Crea una squadra di test
        team = {
            "team_id": "100",
            "name": "AC Milan",
            "country": "Italy",
            "founded": 1899
        }
        
        # Arricchisci i dati
        enriched = self.processor.enrich_team_data(team)
        
        # Verifiche
        self.assertIn('stats', enriched)
        self.assertEqual(enriched['stats']['matches_played'], 30)
        self.assertEqual(enriched['stats']['wins'], 20)

class TestHeadToHeadProcessor(unittest.TestCase):
    """Test per il processore di head-to-head."""
    
    def setUp(self):
        """Setup per i test."""
        self.processor = HeadToHeadProcessor()
        
        # Carica dati di test
        with open(os.path.join(test_dir, 'fixtures', 'h2h_data.json'), 'r', encoding='utf-8') as f:
            self.test_matches = json.load(f)
    
    def test_process_head_to_head(self):
        """Test elaborazione head-to-head."""
        # Processa i dati di test
        processed = self.processor.process_head_to_head(
            team1_id="100",
            team2_id="200",
            matches=self.test_matches
        )
        
        # Verifiche
        self.assertIsInstance(processed, dict)
        
        # Verifica campi obbligatori
        required_fields = ['team1_id', 'team2_id', 'matches', 'stats']
        for field in required_fields:
            self.assertIn(field, processed)
        
        # Verifica statistiche
        stats = processed['stats']
        self.assertIn('total_matches', stats)
        self.assertIn('team1_wins', stats)
        self.assertIn('team2_wins', stats)
        self.assertIn('draws', stats)
    
    def test_calculate_h2h_stats(self):
        """Test calcolo statistiche head-to-head."""
        # Estrai il metodo privato per il test
        calculate_method = self.processor._calculate_h2h_stats
        
        # Crea partite di test
        matches = [
            {
                "match_id": "1",
                "home_team_id": "100",
                "away_team_id": "200",
                "home_score": 2,
                "away_score": 1
            },
            {
                "match_id": "2",
                "home_team_id": "200",
                "away_team_id": "100",
                "home_score": 0,
                "away_score": 0
            },
            {
                "match_id": "3",
                "home_team_id": "100",
                "away_team_id": "200",
                "home_score": 1,
                "away_score": 3
            }
        ]
        
        # Calcola statistiche
        stats = calculate_method("100", "200", matches)
        
        # Verifiche
        self.assertEqual(stats['total_matches'], 3)
        self.assertEqual(stats['team1_wins'], 1)
        self.assertEqual(stats['team2_wins'], 1)
        self.assertEqual(stats['draws'], 1)
        self.assertEqual(stats['team1_goals'], 3)
        self.assertEqual(stats['team2_goals'], 4)

class TestXGProcessor(unittest.TestCase):
    """Test per il processore di dati Expected Goals (xG)."""
    
    def setUp(self):
        """Setup per i test."""
        self.processor = XGProcessor()
        
        # Carica dati di test
        with open(os.path.join(test_dir, 'fixtures', 'xg_data.json'), 'r', encoding='utf-8') as f:
            self.test_data = json.load(f)
    
    def test_process_match_xg(self):
        """Test elaborazione xG di una partita."""
        # Estrai dati di test
        match_xg = self.test_data['match']
        
        # Processa i dati
        processed = self.processor.process_match_xg(match_xg)
        
        # Verifiche
