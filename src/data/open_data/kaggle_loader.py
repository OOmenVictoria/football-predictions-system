""" 
Modulo per il caricamento di dataset calcistici da Kaggle.
Questo modulo fornisce funzionalità per scaricare e elaborare dataset calcistici
gratuiti disponibili su Kaggle (https://www.kaggle.com/).
"""
import os
import io
import csv
import json
import logging
import zipfile
import tempfile
import pandas as pd
from typing import Dict, List, Any, Optional, Union, Tuple

from src.utils.cache import cached
from src.utils.http import make_request, download_file
from src.config.settings import get_setting
from src.utils.database import FirebaseManager

logger = logging.getLogger(__name__)

class KaggleDataLoader:
    """
    Classe per il caricamento di dataset calcistici da Kaggle.
    
    Questa classe fornisce metodi per scaricare e elaborare dataset calcistici
    pubblici disponibili su Kaggle senza richiedere l'API ufficiale di Kaggle.
    """
    
    def __init__(self):
        """Inizializza il loader di dati Kaggle."""
        self.temp_dir = tempfile.gettempdir()
        self.cache_ttl = get_setting('open_data.kaggle.cache_ttl', 86400 * 7)  # 7 giorni
        self.db = FirebaseManager()
        
        # Definisci i dataset calcistici pubblici disponibili su Kaggle
        self.datasets = {
            "european_soccer": {
                "name": "European Soccer Database",
                "description": "25000+ partite, giocatori e squadre dal 2008 al 2016",
                "source_url": "https://www.kaggle.com/datasets/hugomathien/soccer",
                "direct_download": None,  # Nessun link diretto
                "mirror_url": get_setting('open_data.kaggle.mirror_european_soccer', None),
                "files": ["database.sqlite"],
                "format": "sqlite"
            },
            "international_matches": {
                "name": "International Football Results",
                "description": "Risultati di partite internazionali dal 1872 al 2023",
                "source_url": "https://www.kaggle.com/datasets/martj42/international-football-results-from-1872-to-2017",
                "direct_download": None,
                "mirror_url": get_setting('open_data.kaggle.mirror_international', None),
                "files": ["results.csv"],
                "format": "csv"
            },
            "world_cup": {
                "name": "FIFA World Cup",
                "description": "Dati delle Coppe del Mondo dal 1930 al 2022",
                "source_url": "https://www.kaggle.com/datasets/abecklas/fifa-world-cup",
                "direct_download": None,
                "mirror_url": get_setting('open_data.kaggle.mirror_world_cup', None),
                "files": ["WorldCupMatches.csv", "WorldCupPlayers.csv", "WorldCups.csv"],
                "format": "csv"
            },
            "premier_league": {
                "name": "Premier League Complete Dataset",
                "description": "Tutte le partite della Premier League dal 2000-2018",
                "source_url": "https://www.kaggle.com/datasets/thefc17/epl-dataset",
                "direct_download": None,
                "mirror_url": get_setting('open_data.kaggle.mirror_premier_league', None),
                "files": ["EPL_dataset.csv"],
                "format": "csv"
            }
        }
        
        logger.info(f"KaggleDataLoader inizializzato con {len(self.datasets)} dataset disponibili")
    
    def get_available_datasets(self) -> Dict[str, Dict[str, Any]]:
        """
        Restituisce l'elenco dei dataset disponibili.
        
        Returns:
            Dizionario con i dataset disponibili
        """
        # Verifica quali dataset hanno mirroring configurati
        available_datasets = {}
        for dataset_id, dataset_info in self.datasets.items():
            is_available = dataset_info.get("direct_download") or dataset_info.get("mirror_url")
            dataset_info["available"] = is_available
            available_datasets[dataset_id] = dataset_info
        
        return available_datasets
    
    def download_dataset(self, dataset_id: str, destination: Optional[str] = None) -> Optional[str]:
        """
        Scarica un dataset da Kaggle.
        
        Args:
            dataset_id: ID del dataset da scaricare
            destination: Directory di destinazione (opzionale)
            
        Returns:
            Percorso al file o directory scaricato, None in caso di errore
        """
        if dataset_id not in self.datasets:
            logger.error(f"Dataset '{dataset_id}' non trovato")
            return None
        
        dataset_info = self.datasets[dataset_id]
        if not dataset_info.get("direct_download") and not dataset_info.get("mirror_url"):
            logger.error(f"Nessun URL di download disponibile per '{dataset_id}'")
            return None
        
        # Usa la directory temporanea se non specificata
        if not destination:
            destination = os.path.join(self.temp_dir, f"kaggle_{dataset_id}")
            os.makedirs(destination, exist_ok=True)
        
        # Usa il mirror URL se disponibile, altrimenti il download diretto
        download_url = dataset_info.get("mirror_url") or dataset_info.get("direct_download")
        if not download_url:
            logger.error(f"Nessun URL di download disponibile per '{dataset_id}'")
            return None
        
        logger.info(f"Scaricamento dataset '{dataset_id}' da {download_url}")
        
        try:
            # Scarica il file
            file_path = os.path.join(destination, f"{dataset_id}.zip")
            success = download_file(download_url, file_path)
            
            if not success:
                logger.error(f"Errore nel download di '{dataset_id}'")
                return None
            
            # Estrai il file zip
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                zip_ref.extractall(destination)
            
            logger.info(f"Dataset '{dataset_id}' scaricato con successo in {destination}")
            return destination
        
        except Exception as e:
            logger.error(f"Errore nel download del dataset '{dataset_id}': {e}")
            return None
    
    @cached(ttl=86400 * 7)  # Cache di 7 giorni
    def load_international_matches(self) -> pd.DataFrame:
        """
        Carica il dataset delle partite internazionali.
        
        Returns:
            DataFrame con i dati delle partite internazionali
        """
        try:
            dataset_id = "international_matches"
            dataset_path = self.download_dataset(dataset_id)
            
            if not dataset_path:
                logger.error("Impossibile scaricare il dataset delle partite internazionali")
                return pd.DataFrame()
            
            file_path = os.path.join(dataset_path, "results.csv")
            if not os.path.exists(file_path):
                logger.error(f"File non trovato: {file_path}")
                return pd.DataFrame()
            
            df = pd.read_csv(file_path)
            logger.info(f"Dataset delle partite internazionali caricato: {len(df)} righe")
            return df
            
        except Exception as e:
            logger.error(f"Errore nel caricamento del dataset delle partite internazionali: {e}")
            return pd.DataFrame()
    
    @cached(ttl=86400 * 7)  # Cache di 7 giorni
    def load_world_cup_matches(self) -> pd.DataFrame:
        """
        Carica il dataset delle partite dei mondiali.
        
        Returns:
            DataFrame con i dati delle partite dei mondiali
        """
        try:
            dataset_id = "world_cup"
            dataset_path = self.download_dataset(dataset_id)
            
            if not dataset_path:
                logger.error("Impossibile scaricare il dataset dei mondiali")
                return pd.DataFrame()
            
            file_path = os.path.join(dataset_path, "WorldCupMatches.csv")
            if not os.path.exists(file_path):
                logger.error(f"File non trovato: {file_path}")
                return pd.DataFrame()
            
            df = pd.read_csv(file_path)
            logger.info(f"Dataset delle partite dei mondiali caricato: {len(df)} righe")
            return df
            
        except Exception as e:
            logger.error(f"Errore nel caricamento del dataset dei mondiali: {e}")
            return pd.DataFrame()
    
    @cached(ttl=86400 * 7)  # Cache di 7 giorni
    def load_premier_league_matches(self) -> pd.DataFrame:
        """
        Carica il dataset delle partite della Premier League.
        
        Returns:
            DataFrame con i dati delle partite della Premier League
        """
        try:
            dataset_id = "premier_league"
            dataset_path = self.download_dataset(dataset_id)
            
            if not dataset_path:
                logger.error("Impossibile scaricare il dataset della Premier League")
                return pd.DataFrame()
            
            file_path = os.path.join(dataset_path, "EPL_dataset.csv")
            if not os.path.exists(file_path):
                logger.error(f"File non trovato: {file_path}")
                return pd.DataFrame()
            
            df = pd.read_csv(file_path)
            logger.info(f"Dataset della Premier League caricato: {len(df)} righe")
            return df
            
        except Exception as e:
            logger.error(f"Errore nel caricamento del dataset della Premier League: {e}")
            return pd.DataFrame()
    
    def get_international_matches(self, team: Optional[str] = None, 
                                  start_year: Optional[int] = None, 
                                  end_year: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Ottiene le partite internazionali, opzionalmente filtrate.
        
        Args:
            team: Nome della squadra (opzionale)
            start_year: Anno iniziale (opzionale)
            end_year: Anno finale (opzionale)
            
        Returns:
            Lista delle partite internazionali
        """
        try:
            df = self.load_international_matches()
            if df.empty:
                return []
            
            # Filtra per squadra
            if team:
                df = df[(df['home_team'] == team) | (df['away_team'] == team)]
            
            # Filtra per anno
            if start_year:
                df = df[df['date'].str.split('-').str[0].astype(int) >= start_year]
            if end_year:
                df = df[df['date'].str.split('-').str[0].astype(int) <= end_year]
            
            # Converti in lista di dizionari
            matches = df.to_dict('records')
            
            logger.info(f"Trovate {len(matches)} partite internazionali")
            return matches
            
        except Exception as e:
            logger.error(f"Errore nella ricerca delle partite internazionali: {e}")
            return []
    
    def get_world_cup_matches(self, team: Optional[str] = None, 
                              world_cup_year: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Ottiene le partite dei mondiali, opzionalmente filtrate.
        
        Args:
            team: Nome della squadra (opzionale)
            world_cup_year: Anno del mondiale (opzionale)
            
        Returns:
            Lista delle partite dei mondiali
        """
        try:
            df = self.load_world_cup_matches()
            if df.empty:
                return []
            
            # Filtra per squadra
            if team:
                df = df[(df['Home Team Name'] == team) | (df['Away Team Name'] == team)]
            
            # Filtra per anno del mondiale
            if world_cup_year:
                df = df[df['Year'] == world_cup_year]
            
            # Converti in lista di dizionari
            matches = df.to_dict('records')
            
            logger.info(f"Trovate {len(matches)} partite dei mondiali")
            return matches
            
        except Exception as e:
            logger.error(f"Errore nella ricerca delle partite dei mondiali: {e}")
            return []
    
    def get_premier_league_matches(self, team: Optional[str] = None,
                                   season: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Ottiene le partite della Premier League, opzionalmente filtrate.
        
        Args:
            team: Nome della squadra (opzionale)
            season: Stagione (opzionale, es. "2000-2001")
            
        Returns:
            Lista delle partite della Premier League
        """
        try:
            df = self.load_premier_league_matches()
            if df.empty:
                return []
            
            # Filtra per squadra
            if team:
                df = df[(df['HomeTeam'] == team) | (df['AwayTeam'] == team)]
            
            # Filtra per stagione
            if season:
                df = df[df['Season'] == season]
            
            # Converti in lista di dizionari
            matches = df.to_dict('records')
            
            logger.info(f"Trovate {len(matches)} partite della Premier League")
            return matches
            
        except Exception as e:
            logger.error(f"Errore nella ricerca delle partite della Premier League: {e}")
            return []
    
    def update_firebase_international(self) -> Dict[str, Any]:
        """
        Aggiorna i dati delle partite internazionali su Firebase.
        
        Returns:
            Risultato dell'operazione
        """
        result = {"success": 0, "error": None}
        
        try:
            # Carica i dati
            df = self.load_international_matches()
            if df.empty:
                result["error"] = "Nessun dato caricato"
                return result
            
            # Limita a un numero ragionevole di partite recenti
            df['year'] = df['date'].str.split('-').str[0].astype(int)
            recent_years = 10
            current_year = pd.Timestamp.now().year
            df_recent = df[df['year'] > (current_year - recent_years)]
            
            # Prepara i dati per Firebase (massimo 1000 partite)
            matches = df_recent.tail(1000).to_dict('records')
            
            # Raggruppa per anno
            matches_by_year = {}
            for match in matches:
                year = str(match['year'])
                if year not in matches_by_year:
                    matches_by_year[year] = []
                matches_by_year[year].append(match)
            
            # Salva su Firebase
            for year, year_matches in matches_by_year.items():
                matches_ref = self.db.get_reference(f"open_data/kaggle/international_matches/{year}")
                matches_ref.set({i: match for i, match in enumerate(year_matches)})
                result["success"] += len(year_matches)
            
            # Aggiorna meta-informazioni
            meta_ref = self.db.get_reference("open_data/kaggle/international_matches/meta")
            meta_ref.set({
                "last_update": pd.Timestamp.now().isoformat(),
                "source": "kaggle",
                "dataset": "international_matches",
                "years_available": list(matches_by_year.keys())
            })
            
            logger.info(f"Dati delle partite internazionali aggiornati: {result['success']} partite")
            return result
            
        except Exception as e:
            logger.error(f"Errore nell'aggiornamento delle partite internazionali: {e}")
            result["error"] = str(e)
            return result
    
    def update_firebase_world_cup(self) -> Dict[str, Any]:
        """
        Aggiorna i dati delle partite dei mondiali su Firebase.
        
        Returns:
            Risultato dell'operazione
        """
        result = {"success": 0, "error": None}
        
        try:
            # Carica i dati
            df = self.load_world_cup_matches()
            if df.empty:
                result["error"] = "Nessun dato caricato"
                return result
            
            # Prepara i dati per Firebase
            matches = df.to_dict('records')
            
            # Raggruppa per edizione del mondiale
            matches_by_year = {}
            for match in matches:
                year = str(match.get('Year', 'unknown'))
                if year not in matches_by_year:
                    matches_by_year[year] = []
                matches_by_year[year].append(match)
            
            # Salva su Firebase
            for year, year_matches in matches_by_year.items():
                matches_ref = self.db.get_reference(f"open_data/kaggle/world_cup/{year}")
                matches_ref.set({i: match for i, match in enumerate(year_matches)})
                result["success"] += len(year_matches)
            
            # Aggiorna meta-informazioni
            meta_ref = self.db.get_reference("open_data/kaggle/world_cup/meta")
            meta_ref.set({
                "last_update": pd.Timestamp.now().isoformat(),
                "source": "kaggle",
                "dataset": "world_cup",
                "editions_available": list(matches_by_year.keys())
            })
            
            logger.info(f"Dati delle partite dei mondiali aggiornati: {result['success']} partite")
            return result
            
        except Exception as e:
            logger.error(f"Errore nell'aggiornamento delle partite dei mondiali: {e}")
            result["error"] = str(e)
            return result
    
    def update_firebase_premier_league(self) -> Dict[str, Any]:
        """
        Aggiorna i dati delle partite della Premier League su Firebase.
        
        Returns:
            Risultato dell'operazione
        """
        result = {"success": 0, "error": None}
        
        try:
            # Carica i dati
            df = self.load_premier_league_matches()
            if df.empty:
                result["error"] = "Nessun dato caricato"
                return result
            
            # Prepara i dati per Firebase
            matches = df.to_dict('records')
            
            # Raggruppa per stagione
            matches_by_season = {}
            for match in matches:
                season = str(match.get('Season', 'unknown'))
                if season not in matches_by_season:
                    matches_by_season[season] = []
                matches_by_season[season].append(match)
            
            # Salva su Firebase
            for season, season_matches in matches_by_season.items():
                season_key = season.replace("-", "_")
                matches_ref = self.db.get_reference(f"open_data/kaggle/premier_league/{season_key}")
                matches_ref.set({i: match for i, match in enumerate(season_matches)})
                result["success"] += len(season_matches)
            
            # Aggiorna meta-informazioni
            meta_ref = self.db.get_reference("open_data/kaggle/premier_league/meta")
            meta_ref.set({
                "last_update": pd.Timestamp.now().isoformat(),
                "source": "kaggle",
                "dataset": "premier_league",
                "seasons_available": list(matches_by_season.keys())
            })
            
            logger.info(f"Dati delle partite della Premier League aggiornati: {result['success']} partite")
            return result
            
        except Exception as e:
            logger.error(f"Errore nell'aggiornamento delle partite della Premier League: {e}")
            result["error"] = str(e)
            return result
    
    def update_all_datasets(self) -> Dict[str, Dict[str, Any]]:
        """
        Aggiorna tutti i dataset su Firebase.
        
        Returns:
            Risultato dell'operazione per dataset
        """
        results = {}
        
        # Aggiorna le partite internazionali
        logger.info("Aggiornamento dataset partite internazionali")
        try:
            results["international_matches"] = self.update_firebase_international()
        except Exception as e:
            logger.error(f"Errore nell'aggiornamento delle partite internazionali: {e}")
            results["international_matches"] = {"success": 0, "error": str(e)}
        
        # Aggiorna le partite dei mondiali
        logger.info("Aggiornamento dataset dei mondiali")
        try:
            results["world_cup"] = self.update_firebase_world_cup()
        except Exception as e:
            logger.error(f"Errore nell'aggiornamento delle partite dei mondiali: {e}")
            results["world_cup"] = {"success": 0, "error": str(e)}
        
        # Aggiorna le partite della Premier League
        logger.info("Aggiornamento dataset della Premier League")
        try:
            results["premier_league"] = self.update_firebase_premier_league()
        except Exception as e:
            logger.error(f"Errore nell'aggiornamento delle partite della Premier League: {e}")
            results["premier_league"] = {"success": 0, "error": str(e)}
        
        return results

# Funzioni di utilità globali
def get_kaggle_loader() -> KaggleDataLoader:
    """
    Ottiene un'istanza del loader di dati Kaggle.
    
    Returns:
        Istanza di KaggleDataLoader
    """
    return KaggleDataLoader()

def get_available_datasets() -> Dict[str, Dict[str, Any]]:
    """
    Ottiene l'elenco dei dataset disponibili.
    
    Returns:
        Dizionario con i dataset disponibili
    """
    loader = get_kaggle_loader()
    return loader.get_available_datasets()

def get_international_matches(team: Optional[str] = None,
                             start_year: Optional[int] = None,
                             end_year: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Ottiene le partite internazionali, opzionalmente filtrate.
    
    Args:
        team: Nome della squadra (opzionale)
        start_year: Anno iniziale (opzionale)
        end_year: Anno finale (opzionale)
        
    Returns:
        Lista delle partite internazionali
    """
    loader = get_kaggle_loader()
    return loader.get_international_matches(team, start_year, end_year)

def get_world_cup_matches(team: Optional[str] = None,
                         world_cup_year: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Ottiene le partite dei mondiali, opzionalmente filtrate.
    
    Args:
        team: Nome della squadra (opzionale)
        world_cup_year: Anno del mondiale (opzionale)
        
    Returns:
        Lista delle partite dei mondiali
    """
    loader = get_kaggle_loader()
    return loader.get_world_cup_matches(team, world_cup_year)

def get_premier_league_matches(team: Optional[str] = None,
                              season: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Ottiene le partite della Premier League, opzionalmente filtrate.
    
    Args:
        team: Nome della squadra (opzionale)
        season: Stagione (opzionale, es. "2000-2001")
        
    Returns:
        Lista delle partite della Premier League
    """
    loader = get_kaggle_loader()
    return loader.get_premier_league_matches(team, season)

def update_dataset(dataset_id: str) -> Dict[str, Any]:
    """
    Aggiorna un dataset specifico su Firebase.
    
    Args:
        dataset_id: ID del dataset
        
    Returns:
        Risultato dell'operazione
    """
    loader = get_kaggle_loader()
    
    if dataset_id == "international_matches":
        return loader.update_firebase_international()
    elif dataset_id == "world_cup":
        return loader.update_firebase_world_cup()
    elif dataset_id == "premier_league":
        return loader.update_firebase_premier_league()
    else:
        logger.error(f"Dataset '{dataset_id}' non supportato")
        return {"success": 0, "error": f"Dataset '{dataset_id}' non supportato"}

def update_all_datasets() -> Dict[str, Dict[str, Any]]:
    """
    Aggiorna tutti i dataset su Firebase.
    
    Returns:
        Risultato dell'operazione per dataset
    """
    loader = get_kaggle_loader()
    return loader.update_all_datasets()

# Alias per retrocompatibilità
KaggleLoader = KaggleDataLoader

# Alla fine del file kaggle_loader.py

def get_loader():
    """
    Ottiene un'istanza del loader di dati Kaggle.
    
    Returns:
        Istanza di KaggleDataLoader
    """
    return KaggleDataLoader()
