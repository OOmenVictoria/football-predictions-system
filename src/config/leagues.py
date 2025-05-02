""" Configurazione dei campionati di calcio.
Contiene informazioni sui principali campionati di calcio, inclusi
codici API, URL per le fonti dati, e altre proprietà specifiche.
"""

import os
import json
import logging
from typing import Dict, List, Any, Optional, Union
import firebase_admin
from firebase_admin import db, credentials

from src.config.settings import initialize_firebase

# Logger
logger = logging.getLogger(__name__)

# Definizione delle priorità dei campionati (più basso = più importante)
LEAGUE_PRIORITIES = {
    "premier_league": 10,
    "serie_a": 20,
    "la_liga": 30,
    "bundesliga": 40,
    "ligue_1": 50,
    "champions_league": 5,
    "europa_league": 15,
    "conference_league": 25,
    "world_cup": 1,
    "euro": 2,
    "copa_america": 3
}

# Definizione dei campionati supportati con tutte le fonti dati
# Questa è la definizione di base, può essere sovrascritta da Firebase
LEAGUES = {
    "premier_league": {
        "name": "Premier League",
        "country": "England",
        "country_code": "GB",
        "seasons": ["2023-2024"],
        "current_season": "2023-2024",
        "api_codes": {
            "football_data": "PL",
            "rapidapi_football": "39",
            "sofascore": "17",
            "fbref": "9",
            "understat": "1",
            "footystats": "1"
        },
        "urls": {
            "official": "https://www.premierleague.com/",
            "fbref": "https://fbref.com/en/comps/9/Premier-League-Stats",
            "understat": "https://understat.com/league/EPL",
            "sofascore": "https://www.sofascore.com/tournament/football/england/premier-league/17",
            "soccerway": "https://uk.soccerway.com/national/england/premier-league/",
            "transfermarkt": "https://www.transfermarkt.com/premier-league/startseite/wettbewerb/GB1",
            "wikipedia": "https://en.wikipedia.org/wiki/Premier_League"
        },
        "active": True,
        "priority": 10,
        "color": "#3D185C"  # Colore ufficiale della Premier League
    },
    "serie_a": {
        "name": "Serie A",
        "country": "Italy",
        "country_code": "IT",
        "seasons": ["2023-2024"],
        "current_season": "2023-2024",
        "api_codes": {
            "football_data": "SA",
            "rapidapi_football": "135",
            "sofascore": "23",
            "fbref": "11",
            "understat": "2",
            "footystats": "3"
        },
        "urls": {
            "official": "https://www.legaseriea.it/",
            "fbref": "https://fbref.com/en/comps/11/Serie-A-Stats",
            "understat": "https://understat.com/league/Serie_A",
            "sofascore": "https://www.sofascore.com/tournament/football/italy/serie-a/23",
            "soccerway": "https://uk.soccerway.com/national/italy/serie-a/",
            "transfermarkt": "https://www.transfermarkt.com/serie-a/startseite/wettbewerb/IT1",
            "wikipedia": "https://en.wikipedia.org/wiki/Serie_A"
        },
        "active": True,
        "priority": 20,
        "color": "#008FD7"  # Colore ufficiale della Serie A
    },
    "la_liga": {
        "name": "La Liga",
        "country": "Spain",
        "country_code": "ES",
        "seasons": ["2023-2024"],
        "current_season": "2023-2024",
        "api_codes": {
            "football_data": "PD",
            "rapidapi_football": "140",
            "sofascore": "8",
            "fbref": "12",
            "understat": "3",
            "footystats": "2"
        },
        "urls": {
            "official": "https://www.laliga.com/",
            "fbref": "https://fbref.com/en/comps/12/La-Liga-Stats",
            "understat": "https://understat.com/league/La_liga",
            "sofascore": "https://www.sofascore.com/tournament/football/spain/laliga/8",
            "soccerway": "https://uk.soccerway.com/national/spain/primera-division/",
            "transfermarkt": "https://www.transfermarkt.com/laliga/startseite/wettbewerb/ES1",
            "wikipedia": "https://en.wikipedia.org/wiki/La_Liga"
        },
        "active": True,
        "priority": 30,
        "color": "#FF4B34"  # Colore ufficiale de La Liga
    },
    "bundesliga": {
        "name": "Bundesliga",
        "country": "Germany",
        "country_code": "DE",
        "seasons": ["2023-2024"],
        "current_season": "2023-2024",
        "api_codes": {
            "football_data": "BL1",
            "rapidapi_football": "78",
            "sofascore": "35",
            "fbref": "20",
            "understat": "4",
            "footystats": "4"
        },
        "urls": {
            "official": "https://www.bundesliga.com/",
            "fbref": "https://fbref.com/en/comps/20/Bundesliga-Stats",
            "understat": "https://understat.com/league/Bundesliga",
            "sofascore": "https://www.sofascore.com/tournament/football/germany/bundesliga/35",
            "soccerway": "https://uk.soccerway.com/national/germany/bundesliga/",
            "transfermarkt": "https://www.transfermarkt.com/bundesliga/startseite/wettbewerb/L1",
            "wikipedia": "https://en.wikipedia.org/wiki/Bundesliga"
        },
        "active": True,
        "priority": 40,
        "color": "#D3010C"  # Colore ufficiale della Bundesliga
    },
    "ligue_1": {
        "name": "Ligue 1",
        "country": "France",
        "country_code": "FR",
        "seasons": ["2023-2024"],
        "current_season": "2023-2024",
        "api_codes": {
            "football_data": "FL1",
            "rapidapi_football": "61",
            "sofascore": "34",
            "fbref": "13",
            "understat": "5",
            "footystats": "5"
        },
        "urls": {
            "official": "https://www.ligue1.com/",
            "fbref": "https://fbref.com/en/comps/13/Ligue-1-Stats",
            "understat": "https://understat.com/league/Ligue_1",
            "sofascore": "https://www.sofascore.com/tournament/football/france/ligue-1/34",
            "soccerway": "https://uk.soccerway.com/national/france/ligue-1/",
            "transfermarkt": "https://www.transfermarkt.com/ligue-1/startseite/wettbewerb/FR1",
            "wikipedia": "https://en.wikipedia.org/wiki/Ligue_1"
        },
        "active": True,
        "priority": 50,
        "color": "#001489"  # Colore ufficiale della Ligue 1
    },
    "champions_league": {
        "name": "UEFA Champions League",
        "country": "Europe",
        "country_code": "EU",
        "seasons": ["2023-2024"],
        "current_season": "2023-2024",
        "api_codes": {
            "football_data": "CL",
            "rapidapi_football": "2",
            "sofascore": "7",
            "fbref": "8",
            "footystats": "7"
        },
        "urls": {
            "official": "https://www.uefa.com/uefachampionsleague/",
            "fbref": "https://fbref.com/en/comps/8/Champions-League-Stats",
            "sofascore": "https://www.sofascore.com/tournament/football/europe/uefa-champions-league/7",
            "soccerway": "https://uk.soccerway.com/international/europe/uefa-champions-league/",
            "transfermarkt": "https://www.transfermarkt.com/uefa-champions-league/startseite/pokalwettbewerb/CL",
            "wikipedia": "https://en.wikipedia.org/wiki/UEFA_Champions_League"
        },
        "active": True,
        "priority": 5,
        "color": "#0F1D5F"  # Colore ufficiale della Champions League
    },
    "europa_league": {
        "name": "UEFA Europa League",
        "country": "Europe",
        "country_code": "EU",
        "seasons": ["2023-2024"],
        "current_season": "2023-2024",
        "api_codes": {
            "football_data": "EL",
            "rapidapi_football": "3",
            "sofascore": "679",
            "fbref": "19",
            "footystats": "12"
        },
        "urls": {
            "official": "https://www.uefa.com/uefaeuropaleague/",
            "fbref": "https://fbref.com/en/comps/19/Europa-League-Stats",
            "sofascore": "https://www.sofascore.com/tournament/football/europe/uefa-europa-league/679",
            "soccerway": "https://uk.soccerway.com/international/europe/uefa-cup/",
            "transfermarkt": "https://www.transfermarkt.com/uefa-europa-league/startseite/pokalwettbewerb/EL",
            "wikipedia": "https://en.wikipedia.org/wiki/UEFA_Europa_League"
        },
        "active": True,
        "priority": 15,
        "color": "#F58723"  # Colore ufficiale della Europa League
    },
    "copa_libertadores": {
        "name": "Copa Libertadores",
        "country": "South America",
        "country_code": "SA",
        "seasons": ["2023", "2024"],  # Diverso formato annuale
        "current_season": "2024",
        "api_codes": {
            "football_data": None,  # Non disponibile in alcune API
            "rapidapi_football": "13",
            "sofascore": "384",
            "fbref": "15"
        },
        "urls": {
            "official": "https://www.conmebol.com/libertadores/",
            "fbref": "https://fbref.com/en/comps/15/Copa-Libertadores-Stats",
            "sofascore": "https://www.sofascore.com/tournament/football/south-america/copa-libertadores/384",
            "soccerway": "https://uk.soccerway.com/international/south-america/copa-libertadores/",
            "transfermarkt": "https://www.transfermarkt.com/copa-libertadores/startseite/pokalwettbewerb/CLI",
            "wikipedia": "https://en.wikipedia.org/wiki/Copa_Libertadores"
        },
        "active": True,
        "priority": 45,
        "color": "#FDBE00"  # Colore associato al torneo
    },
    "saudi_pro_league": {
        "name": "Saudi Pro League",
        "country": "Saudi Arabia",
        "country_code": "SA",
        "seasons": ["2023-2024"],
        "current_season": "2023-2024",
        "api_codes": {
            "football_data": None,  # Non disponibile in alcune API
            "rapidapi_football": "307",
            "sofascore": "955",
            "fbref": "415"
        },
        "urls": {
            "official": "https://spl.com.sa/en/",
            "fbref": "https://fbref.com/en/comps/415/Saudi-Pro-League-Stats",
            "sofascore": "https://www.sofascore.com/tournament/football/saudi-arabia/saudi-professional-league/955",
            "soccerway": "https://uk.soccerway.com/national/saudi-arabia/saudi-professional-league/",
            "transfermarkt": "https://www.transfermarkt.com/saudi-professional-league/startseite/wettbewerb/SA1",
            "wikipedia": "https://en.wikipedia.org/wiki/Saudi_Professional_League"
        },
        "active": True,
        "priority": 60,
        "color": "#00A457"  # Colore associato al torneo
    },
    "serie_b": {
        "name": "Serie B",
        "country": "Italy",
        "country_code": "IT",
        "seasons": ["2023-2024"],
        "current_season": "2023-2024",
        "api_codes": {
            "football_data": None,
            "rapidapi_football": "136",
            "sofascore": "75",
            "fbref": "18",
            "footystats": "18"
        },
        "urls": {
            "official": "https://www.legab.it/",
            "fbref": "https://fbref.com/en/comps/18/Serie-B-Stats",
            "sofascore": "https://www.sofascore.com/tournament/football/italy/serie-b/75",
            "soccerway": "https://uk.soccerway.com/national/italy/serie-b/",
            "transfermarkt": "https://www.transfermarkt.com/serie-b/startseite/wettbewerb/IT2",
            "wikipedia": "https://en.wikipedia.org/wiki/Serie_B"
        },
        "active": True,
        "priority": 65,
        "color": "#009CDE"  # Colore associato al torneo
    },
    "la_liga_2": {
        "name": "La Liga 2",
        "country": "Spain",
        "country_code": "ES",
        "seasons": ["2023-2024"],
        "current_season": "2023-2024",
        "api_codes": {
            "football_data": "SD",
            "rapidapi_football": "141",
            "sofascore": "54",
            "fbref": "17",
            "footystats": "17"
        },
        "urls": {
            "official": "https://www.laliga.com/laliga-smartbank",
            "fbref": "https://fbref.com/en/comps/17/Segunda-Division-Stats",
            "sofascore": "https://www.sofascore.com/tournament/football/spain/laliga-2/54",
            "soccerway": "https://uk.soccerway.com/national/spain/segunda-division/",
            "transfermarkt": "https://www.transfermarkt.com/laliga2/startseite/wettbewerb/ES2",
            "wikipedia": "https://en.wikipedia.org/wiki/Segunda_Divisi%C3%B3n"
        },
        "active": True,
        "priority": 70,
        "color": "#828A8F"  # Colore associato al torneo
    },
    "ligue_2": {
        "name": "Ligue 2",
        "country": "France",
        "country_code": "FR",
        "seasons": ["2023-2024"],
        "current_season": "2023-2024",
        "api_codes": {
            "football_data": "FL2",
            "rapidapi_football": "62",
            "sofascore": "42",
            "fbref": "32",
            "footystats": "22"
        },
        "urls": {
            "official": "https://www.ligue2.fr/",
            "fbref": "https://fbref.com/en/comps/32/Ligue-2-Stats",
            "sofascore": "https://www.sofascore.com/tournament/football/france/ligue-2/42",
            "soccerway": "https://uk.soccerway.com/national/france/ligue-2/",
            "transfermarkt": "https://www.transfermarkt.com/ligue-2/startseite/wettbewerb/FR2",
            "wikipedia": "https://en.wikipedia.org/wiki/Ligue_2"
        },
        "active": True,
        "priority": 75,
        "color": "#00387B"  # Colore associato al torneo
    },
    "bundesliga_2": {
        "name": "2. Bundesliga",
        "country": "Germany",
        "country_code": "DE",
        "seasons": ["2023-2024"],
        "current_season": "2023-2024",
        "api_codes": {
            "football_data": "BL2",
            "rapidapi_football": "79",
            "sofascore": "36",
            "fbref": "33",
            "footystats": "21"
        },
        "urls": {
            "official": "https://www.bundesliga.com/de/2bundesliga",
            "fbref": "https://fbref.com/en/comps/33/2-Bundesliga-Stats",
            "sofascore": "https://www.sofascore.com/tournament/football/germany/2-bundesliga/36",
            "soccerway": "https://uk.soccerway.com/national/germany/2-bundesliga/",
            "transfermarkt": "https://www.transfermarkt.com/2-bundesliga/startseite/wettbewerb/L2",
            "wikipedia": "https://en.wikipedia.org/wiki/2._Bundesliga"
        },
        "active": True,
        "priority": 80,
        "color": "#999999"  # Colore associato al torneo
    },
    "championship": {
        "name": "EFL Championship",
        "country": "England",
        "country_code": "GB",
        "seasons": ["2023-2024"],
        "current_season": "2023-2024",
        "api_codes": {
            "football_data": "ELC",
            "rapidapi_football": "40",
            "sofascore": "18",
            "fbref": "10",
            "footystats": "10"
        },
        "urls": {
            "official": "https://www.efl.com/competitions/sky-bet-championship/",
            "fbref": "https://fbref.com/en/comps/10/Championship-Stats",
            "sofascore": "https://www.sofascore.com/tournament/football/england/championship/18",
            "soccerway": "https://uk.soccerway.com/national/england/championship/",
            "transfermarkt": "https://www.transfermarkt.com/championship/startseite/wettbewerb/GB2",
            "wikipedia": "https://en.wikipedia.org/wiki/EFL_Championship"
        },
        "active": True,
        "priority": 55,
        "color": "#EF3E33"  # Colore ufficiale dell'EFL Championship
    },
    "primeira_liga": {
        "name": "Primeira Liga",
        "country": "Portugal",
        "country_code": "PT",
        "seasons": ["2023-2024"],
        "current_season": "2023-2024",
        "api_codes": {
            "football_data": "PPL",
            "rapidapi_football": "94",
            "sofascore": "238",
            "fbref": "32",
            "footystats": "25"
        },
        "urls": {
            "official": "https://www.ligaportugal.pt/",
            "fbref": "https://fbref.com/en/comps/32/Primeira-Liga-Stats",
            "sofascore": "https://www.sofascore.com/tournament/football/portugal/liga-portugal/238",
            "soccerway": "https://uk.soccerway.com/national/portugal/portuguese-liga-/",
            "transfermarkt": "https://www.transfermarkt.com/primeira-liga/startseite/wettbewerb/PO1",
            "wikipedia": "https://en.wikipedia.org/wiki/Primeira_Liga"
        },
        "active": True,
        "priority": 52,
        "color": "#009245"  # Colore associato al torneo
    },
    "super_lig": {
        "name": "Süper Lig",
        "country": "Turkey",
        "country_code": "TR",
        "seasons": ["2023-2024"],
        "current_season": "2023-2024",
        "api_codes": {
            "football_data": None,
            "rapidapi_football": "203",
            "sofascore": "52",
            "fbref": "26",
            "footystats": "28"
        },
        "urls": {
            "official": "https://www.tff.org/default.aspx?pageID=198",
            "fbref": "https://fbref.com/en/comps/26/Super-Lig-Stats",
            "sofascore": "https://www.sofascore.com/tournament/football/turkey/super-lig/52",
            "soccerway": "https://uk.soccerway.com/national/turkey/super-lig/",
            "transfermarkt": "https://www.transfermarkt.com/super-lig/startseite/wettbewerb/TR1",
            "wikipedia": "https://en.wikipedia.org/wiki/S%C3%BCper_Lig"
        },
        "active": True,
        "priority": 58,
        "color": "#E30A17"  # Colore della bandiera turca
    },
    "j1_league": {
        "name": "J1 League",
        "country": "Japan",
        "country_code": "JP",
        "seasons": ["2024"],  # In Giappone segue anno solare
        "current_season": "2024",
        "api_codes": {
            "football_data": None,
            "rapidapi_football": "98",
            "sofascore": "271",
            "fbref": "25",
            "footystats": "31"
        },
        "urls": {
            "official": "https://www.jleague.jp/",
            "fbref": "https://fbref.com/en/comps/25/J1-League-Stats",
            "sofascore": "https://www.sofascore.com/tournament/football/japan/j1-league/271",
            "soccerway": "https://uk.soccerway.com/national/japan/j1-league/",
            "transfermarkt": "https://www.transfermarkt.com/j1-league/startseite/wettbewerb/JAP1",
            "wikipedia": "https://en.wikipedia.org/wiki/J1_League"
        },
        "active": True,
        "priority": 85,
        "color": "#FF5A5F"  # Colore associato al torneo
    },
    "primera_division_argentina": {
        "name": "Liga Profesional de Fútbol",
        "country": "Argentina",
        "country_code": "AR",
        "seasons": ["2024"],  # Calendario anno solare
        "current_season": "2024",
        "api_codes": {
            "football_data": None,
            "rapidapi_football": "128",
            "sofascore": "155",
            "fbref": "21",
            "footystats": "23"
        },
        "urls": {
            "official": "https://www.ligaprofesional.ar/",
            "fbref": "https://fbref.com/en/comps/21/Primera-Division-Stats",
            "sofascore": "https://www.sofascore.com/tournament/football/argentina/liga-profesional/155",
            "soccerway": "https://uk.soccerway.com/national/argentina/primera-division/",
            "transfermarkt": "https://www.transfermarkt.com/superliga/startseite/wettbewerb/ARG1",
            "wikipedia": "https://en.wikipedia.org/wiki/Argentine_Primera_Divisi%C3%B3n"
        },
        "active": True,
        "priority": 57,
        "color": "#75AADB"  # Colore della bandiera argentina
    },
    "brasileirao": {
        "name": "Brasileirão",
        "country": "Brazil",
        "country_code": "BR",
        "seasons": ["2024"],  # Calendario anno solare
        "current_season": "2024",
        "api_codes": {
            "football_data": "BSA",
            "rapidapi_football": "71",
            "sofascore": "325",
            "fbref": "24",
            "footystats": "24"
        },
        "urls": {
            "official": "https://www.cbf.com.br/futebol-brasileiro/competicoes/campeonato-brasileiro-serie-a",
            "fbref": "https://fbref.com/en/comps/24/Serie-A-Stats",
            "sofascore": "https://www.sofascore.com/tournament/football/brazil/brasileiro-serie-a/325",
            "soccerway": "https://uk.soccerway.com/national/brazil/serie-a/",
            "transfermarkt": "https://www.transfermarkt.com/campeonato-brasileiro-serie-a/startseite/wettbewerb/BRA1",
            "wikipedia": "https://en.wikipedia.org/wiki/Campeonato_Brasileiro_S%C3%A9rie_A"
        },
        "active": True,
        "priority": 56,
        "color": "#009C3B"  # Colore della bandiera brasiliana
    },
    "world_cup": {
        "name": "FIFA World Cup",
        "country": "International",
        "country_code": "INT",
        "seasons": ["2022"],
        "current_season": "2022",
        "api_codes": {
            "football_data": "WC",
            "rapidapi_football": "1",
            "sofascore": "16",
            "fbref": "1",
            "footystats": "1"
        },
        "urls": {
            "official": "https://www.fifa.com/fifaplus/en/tournaments/mens/worldcup",
            "fbref": "https://fbref.com/en/comps/1/World-Cup-Stats",
            "sofascore": "https://www.sofascore.com/tournament/football/world/world-cup/16",
            "soccerway": "https://uk.soccerway.com/international/world/world-cup/",
            "transfermarkt": "https://www.transfermarkt.com/weltmeisterschaft/startseite/pokalwettbewerb/WM",
            "wikipedia": "https://en.wikipedia.org/wiki/FIFA_World_Cup"
        },
        "active": False,  # Non attivo al momento (tra un mondiale e l'altro)
        "priority": 1,  # Massima priorità
        "color": "#49BCE3"  # Colore associato al torneo
    },
    "world_cup_qualification_uefa": {
        "name": "World Cup Qualification UEFA",
        "country": "Europe",
        "country_code": "EU",
        "seasons": ["2024-2025"],
        "current_season": "2024-2025",
        "api_codes": {
            "football_data": "WCQEU",
            "rapidapi_football": "5",
            "sofascore": "188",
            "fbref": "61",
            "footystats": "61"
        },
        "urls": {
            "official": "https://www.uefa.com/european-qualifiers/",
            "fbref": "https://fbref.com/en/comps/61/World-Cup-Qualifying-UEFA-Stats",
            "sofascore": "https://www.sofascore.com/tournament/football/world/world-cup-qualification-uefa/188",
            "soccerway": "https://uk.soccerway.com/international/world/world-cup/2026-north-central-america-and-caribbean/",
            "transfermarkt": "https://www.transfermarkt.co.uk/uefa-wm-qualifikation/startseite/pokalwettbewerb/WCQE",
            "wikipedia": "https://en.wikipedia.org/wiki/FIFA_World_Cup_qualification"
        },
        "active": True,
        "priority": 35,
        "color": "#49BCE3"  # Colore simile al World Cup
    },
    "world_cup_qualification_conmebol": {
        "name": "World Cup Qualification CONMEBOL",
        "country": "South America",
        "country_code": "SA",
        "seasons": ["2024-2025"],
        "current_season": "2024-2025",
        "api_codes": {
            "football_data": None,
            "rapidapi_football": "28",
            "sofascore": "190",
            "fbref": "68",
            "footystats": "68"
        },
        "urls": {
            "official": "https://www.conmebol.com/",
            "fbref": "https://fbref.com/en/comps/68/World-Cup-Qualifying-CONMEBOL-Stats",
            "sofascore": "https://www.sofascore.com/tournament/football/world/world-cup-qualification-conmebol/190",
            "soccerway": "https://uk.soccerway.com/international/world/world-cup/2026-north-central-america-and-caribbean/",
            "transfermarkt": "https://www.transfermarkt.com/wm-qualifikation-sudamerika/startseite/pokalwettbewerb/WCQS",
            "wikipedia": "https://en.wikipedia.org/wiki/FIFA_World_Cup_qualification_(CONMEBOL)"
                    },
                    "active": True,
                    "priority": 36,
                    "color": "#49BCE3"  # Colore simile al World Cup
                 }
                 # Aggiungi altri campionati secondo necessità
            }

            #Mappa dei nomi alle leghe per facilitare la ricerca per nome
            LEAGUE_NAME_MAP = {
                # Nomi ufficiali
                "Premier League": "premier_league",
                "Serie A": "serie_a",
                "La Liga": "la_liga",
                "Bundesliga": "bundesliga",
                "Ligue 1": "ligue_1",
                "UEFA Champions League": "champions_league",
                "Champions League": "champions_league",
                "UEFA Europa League": "europa_league",
                "Europa League": "europa_league",
                "Copa Libertadores": "copa_libertadores",
                "Saudi Pro League": "saudi_pro_league",
                "Serie B": "serie_b",
                "La Liga 2": "la_liga_2",
                "Ligue 2": "ligue_2",
                "2. Bundesliga": "bundesliga_2",
                "EFL Championship": "championship",
                "Championship": "championship",
                "Primeira Liga": "primeira_liga",
                "Süper Lig": "super_lig",
                "J1 League": "j1_league",
                "Liga Profesional de Fútbol": "primera_division_argentina",
                "Brasileirão": "brasileirao",
                "FIFA World Cup": "world_cup",
                "World Cup": "world_cup",
            
                # Nomi alternativi/non ufficiali
                "EPL": "premier_league",
                "English Premier League": "premier_league",
                "Primera Division": "la_liga",
                "LaLiga": "la_liga",
                "LaLiga Santander": "la_liga",
                "Serie A TIM": "serie_a",
                "Ligue 1 Uber Eats": "ligue_1",
                "Bundesliga 1": "bundesliga",
                "UCL": "champions_league",
                "UEL": "europa_league",
                "Copa": "copa_libertadores",
                "SPL": "saudi_pro_league",
                "Segunda Division": "la_liga_2",
                "Bundesliga 2": "bundesliga_2",
                "Portuguese Liga": "primeira_liga",
                "Super Lig": "super_lig",
                "Argentina Primera Division": "primera_division_argentina",
                "Brazil Serie A": "brasileirao",
                "Brazilian Serie A": "brasileirao"
            }

# Funzione per ottenere i dati dei campionati da Firebase
def get_leagues_from_firebase() -> Dict[str, Any]:
    """
    Ottiene i dati dei campionati da Firebase.
    
    Returns:
        Dizionario con i dati dei campionati
    """
    try:
        initialize_firebase()
        leagues_ref = db.reference('config/leagues')
        leagues_data = leagues_ref.get()
        
        if leagues_data:
            return leagues_data
        return {}
    except Exception as e:
        logger.warning(f"Impossibile ottenere i dati dei campionati da Firebase: {e}")
        return {}

# Funzione per salvare i dati dei campionati su Firebase
def save_leagues_to_firebase(leagues_data: Dict[str, Any]) -> bool:
    """
    Salva i dati dei campionati su Firebase.
    
    Args:
        leagues_data: Dizionario con i dati dei campionati
        
    Returns:
        True se l'operazione è riuscita, False altrimenti
    """
    try:
        initialize_firebase()
        leagues_ref = db.reference('config/leagues')
        leagues_ref.set(leagues_data)
        logger.info("Dati dei campionati salvati su Firebase con successo")
        return True
    except Exception as e:
        logger.error(f"Impossibile salvare i dati dei campionati su Firebase: {e}")
        return False

# Funzione per ottenere i campionati attivi
def get_active_leagues() -> Dict[str, Dict[str, Any]]:
    """
    Ottiene i campionati attivi, combinando dati locali e Firebase.
    
    Returns:
        Dizionario con i campionati attivi
    """
    # Ottieni dati da Firebase
    firebase_leagues = get_leagues_from_firebase()
    
    # Combina con dati locali
    combined_leagues = LEAGUES.copy()
    
    # Aggiorna con dati da Firebase
    for league_id, league_data in firebase_leagues.items():
        if league_id in combined_leagues:
            # Aggiorna campi esistenti
            combined_leagues[league_id].update(league_data)
        else:
            # Aggiungi nuovo campionato
            combined_leagues[league_id] = league_data
    
    # Filtra per campionati attivi
    active_leagues = {
        league_id: league_data 
        for league_id, league_data in combined_leagues.items() 
        if league_data.get('active', True)
    }
    
    return active_leagues

# Funzione per ottenere un campionato specifico
def get_league(league_id: str) -> Optional[Dict[str, Any]]:
    """
    Ottiene i dati di un campionato specifico.
    
    Args:
        league_id: ID del campionato
        
    Returns:
        Dati del campionato o None se non trovato
    """
    # Ottieni tutti i campionati
    all_leagues = get_active_leagues()
    
    # Restituisci il campionato richiesto se esiste
    return all_leagues.get(league_id)

# Funzione per ottenere un campionato tramite nome
def get_league_by_name(name: str) -> Optional[Dict[str, Any]]:
    """
    Ottiene informazioni su una lega dal nome.
    
    Args:
        name: Nome della lega (ufficiale o alternativo)
        
    Returns:
        Dizionario con informazioni sulla lega o None se non trovata
    """
    # Normalizzazione del nome (rimuove spazi extra e converte in lowercase)
    name = name.strip().lower()
    
    # Verifica diretta nel dizionario dei nomi
    for league_name, league_key in LEAGUE_NAME_MAP.items():
        if name == league_name.lower():
            return get_league(league_key)
    
    # Prova corrispondenze parziali
    for league_name, league_key in LEAGUE_NAME_MAP.items():
        if name in league_name.lower() or league_name.lower() in name:
            return get_league(league_key)
    
    # Cerca nei dati delle leghe
    active_leagues = get_active_leagues()
    for league_key, league_data in active_leagues.items():
        league_name = league_data.get("name", "").lower()
        league_country = league_data.get("country", "").lower()
        
        if (name in league_name or league_name in name or 
            name in league_key.lower() or
            name in league_country):
            return league_data
    
    # Nessuna corrispondenza trovata
    logger.warning(f"Lega non trovata con nome: {name}")
    return None

# Funzione per ottenere campionati ordinati per priorità
def get_leagues_by_priority() -> List[Dict[str, Any]]:
    """
    Ottiene i campionati ordinati per priorità.
    
    Returns:
        Lista di campionati ordinati per priorità
    """
    active_leagues = get_active_leagues()
    
    # Converti in lista per ordinamento
    leagues_list = [
        {"id": league_id, **league_data}
        for league_id, league_data in active_leagues.items()
    ]
    
    # Ordina per priorità (più bassa prima)
    leagues_list.sort(key=lambda x: x.get('priority', 999))
    
    return leagues_list

# Funzione per ottenere il codice API per un campionato
def get_api_code(league_id: str, api_name: str) -> Optional[str]:
    """
    Ottiene il codice API per un campionato specifico.
    
    Args:
        league_id: ID del campionato
        api_name: Nome dell'API (football_data, rapidapi_football, ecc.)
        
    Returns:
        Codice API o None se non trovato
    """
    league_data = get_league(league_id)
    
    if not league_data:
        return None
    
    api_codes = league_data.get('api_codes', {})
    return api_codes.get(api_name)

# Funzione per ottenere l'URL di una fonte per un campionato
def get_league_url(league_id: str, source: str) -> Optional[str]:
    """
    Ottiene l'URL di una fonte per un campionato specifico.
    
    Args:
        league_id: ID del campionato
        source: Nome della fonte (official, fbref, understat, ecc.)
        
    Returns:
        URL o None se non trovato
    """
    league_data = get_league(league_id)
    
    if not league_data:
        return None
    
    urls = league_data.get('urls', {})
    return urls.get(source)

# Inizializza i dati dei campionati su Firebase se necessario
def initialize_leagues():
    """Inizializza i dati dei campionati su Firebase se non esistono."""
    try:
        initialize_firebase()
        leagues_ref = db.reference('config/leagues')
        
        # Verifica se i dati esistono già
        existing_data = leagues_ref.get()
        
        if not existing_data:
            # Salva i dati iniziali
            save_leagues_to_firebase(LEAGUES)
            logger.info("Dati dei campionati inizializzati su Firebase")
    except Exception as e:
        logger.warning(f"Impossibile inizializzare i dati dei campionati su Firebase: {e}")
