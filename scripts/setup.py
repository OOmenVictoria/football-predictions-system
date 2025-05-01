#!/usr/bin/env python3
"""
Setup script for the Football Predictions System.
This script initializes the system, creates necessary directory structures,
and sets up the Firebase database with initial configuration.
"""
import os
import sys
import json
import time
import logging
import argparse
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import project modules
try:
    from src.utils.database import FirebaseManager
    from src.config.settings import get_setting
    from src.config.leagues import LEAGUES
    from src.config.sources import SOURCES
    from dotenv import load_dotenv
except ImportError as e:
    print(f"Error importing modules: {e}")
    print("Make sure to run this script from the project root directory.")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/setup.log')
    ]
)

logger = logging.getLogger(__name__)

def create_directory_structure() -> None:
    """Create the necessary directory structure for the project."""
    logger.info("Creating directory structure...")
    
    directories = [
        "logs",
        "cache",
        "backups",
        "reports"
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        logger.info(f"Created directory: {directory}")
    
    # Create .gitignore files for cache and logs directories
    with open('cache/.gitignore', 'w') as f:
        f.write("*\n!.gitignore\n")
    
    with open('logs/.gitignore', 'w') as f:
        f.write("*\n!.gitignore\n")
    
    with open('backups/.gitignore', 'w') as f:
        f.write("*\n!.gitignore\n")
    
    logger.info("Directory structure created successfully.")

def check_environment_variables() -> bool:
    """
    Check if all required environment variables are set.
    
    Returns:
        bool: True if all environment variables are set, False otherwise.
    """
    logger.info("Checking environment variables...")
    
    # Load environment variables
    load_dotenv()
    
    required_vars = [
        "FIREBASE_DB_URL",
        "FOOTBALL_API_KEY",
        "WP_URL",
        "WP_USER",
        "WP_APP_PASSWORD"
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        logger.error(f"Missing environment variables: {', '.join(missing_vars)}")
        logger.error("Please set these variables in your .env file.")
        return False
    
    logger.info("All environment variables are set.")
    return True

def check_firebase_credentials() -> bool:
    """
    Check if Firebase credentials are properly set up.
    
    Returns:
        bool: True if Firebase credentials are set up, False otherwise.
    """
    logger.info("Checking Firebase credentials...")
    
    # Check if FIREBASE_CREDENTIALS environment variable is set
    firebase_creds_path = os.getenv('FIREBASE_CREDENTIALS')
    
    if not firebase_creds_path:
        logger.error("FIREBASE_CREDENTIALS environment variable is not set.")
        logger.error("Please set this variable to the path of your Firebase credentials JSON file.")
        return False
    
    # Check if the credentials file exists
    if not os.path.exists(firebase_creds_path):
        logger.error(f"Firebase credentials file not found: {firebase_creds_path}")
        logger.error("Please make sure the file exists and the path is correct.")
        return False
    
    # Try to read the credentials file
    try:
        with open(firebase_creds_path, 'r') as f:
            creds = json.load(f)
        
        required_keys = ["type", "project_id", "private_key_id", "private_key", "client_email"]
        for key in required_keys:
            if key not in creds:
                logger.error(f"Firebase credentials file is missing required key: {key}")
                return False
        
        logger.info("Firebase credentials are valid.")
        return True
    
    except json.JSONDecodeError:
        logger.error("Firebase credentials file is not a valid JSON file.")
        return False
    except Exception as e:
        logger.error(f"Error reading Firebase credentials file: {e}")
        return False

def initialize_firebase_database() -> bool:
    """
    Initialize the Firebase database with default values.
    
    Returns:
        bool: True if initialization is successful, False otherwise.
    """
    logger.info("Initializing Firebase database...")
    
    try:
        db = FirebaseManager()
        
        # Check connection
        if not db.test_connection():
            logger.error("Failed to connect to Firebase database.")
            return False
        
        logger.info("Connected to Firebase database successfully.")
        
        # Create initial data structure
        initial_data = {
            "system": {
                "status": "initialized",
                "last_update": datetime.now().isoformat(),
                "version": "1.0.0"
            },
            "config": {
                "leagues": LEAGUES,
                "sources": SOURCES,
                "settings": {
                    "content": {
                        "preview_length": "medium",
                        "style": "formal",
                        "include_stats": True,
                        "include_value_bets": True,
                        "max_trends": 5,
                        "language": "en"
                    },
                    "publishing": {
                        "hours_before_match": 12,
                        "hours_after_match": 8,
                        "max_articles_per_run": 50
                    },
                    "scraping": {
                        "rate_limit": 1.0,  # Requests per second
                        "timeout": 10,
                        "retries": 3
                    }
                }
            },
            "health": {
                "status": "ok",
                "last_check": datetime.now().isoformat(),
                "components": {
                    "data_collection": {"status": "ok"},
                    "content_generation": {"status": "ok"},
                    "publishing": {"status": "ok"},
                    "database": {"status": "ok"}
                }
            }
        }
        
        # Write initial data to Firebase
        db.get_reference().update(initial_data)
        
        logger.info("Firebase database initialized successfully.")
        return True
    
    except Exception as e:
        logger.error(f"Error initializing Firebase database: {e}")
        return False

def test_api_access() -> Dict[str, bool]:
    """
    Test access to external APIs.
    
    Returns:
        Dict[str, bool]: Dictionary with API names as keys and access status as values.
    """
    logger.info("Testing API access...")
    
    import requests
    
    api_results = {}
    
    # Test Football-Data.org API
    try:
        football_api_key = os.getenv('FOOTBALL_API_KEY')
        if football_api_key:
            response = requests.get(
                'https://api.football-data.org/v4/competitions',
                headers={'X-Auth-Token': football_api_key}
            )
            
            if response.status_code == 200:
                api_results['football_data'] = True
                logger.info("Football-Data.org API access: Successful")
            else:
                api_results['football_data'] = False
                logger.error(f"Football-Data.org API access: Failed (Status code: {response.status_code})")
        else:
            api_results['football_data'] = False
            logger.error("Football-Data.org API access: Failed (No API key)")
    
    except Exception as e:
        api_results['football_data'] = False
        logger.error(f"Football-Data.org API access: Failed ({e})")
    
    # Test WordPress API
    try:
        wp_url = os.getenv('WP_URL')
        wp_user = os.getenv('WP_USER')
        wp_password = os.getenv('WP_APP_PASSWORD')
        
        if wp_url and wp_user and wp_password:
            # Remove '/posts' from the end if present
            base_url = wp_url.split('/posts')[0]
            
            # Try to access the WordPress API without authentication first
            response = requests.get(f"{base_url}")
            
            if response.status_code == 200:
                api_results['wordpress'] = True
                logger.info("WordPress API access: Successful")
            else:
                api_results['wordpress'] = False
                logger.error(f"WordPress API access: Failed (Status code: {response.status_code})")
        else:
            api_results['wordpress'] = False
            logger.error("WordPress API access: Failed (Missing credentials)")
    
    except Exception as e:
        api_results['wordpress'] = False
        logger.error(f"WordPress API access: Failed ({e})")
    
    return api_results

def create_sample_article() -> None:
    """Create a sample article to test the content generation."""
    logger.info("Creating sample article...")
    
    try:
        # Import content generator
        from src.content.generator import generate_match_article
        
        # Check if we have match data
        db = FirebaseManager()
        matches_ref = db.get_reference("data/matches")
        matches = matches_ref.get()
        
        if not matches:
            logger.info("No match data available for sample article.")
            logger.info("Run data collection first, then try creating a sample article.")
            return
        
        # Take the first match
        match_id = list(matches.keys())[0]
        
        # Generate article
        article = generate_match_article(match_id)
        
        if article and 'content' in article:
            # Save to a file
            with open('sample_article.md', 'w') as f:
                f.write(article['content'])
            
            logger.info(f"Sample article created: sample_article.md")
        else:
            logger.error("Failed to generate sample article.")
    
    except Exception as e:
        logger.error(f"Error creating sample article: {e}")

def setup(args: argparse.Namespace) -> None:
    """
    Main setup function.
    
    Args:
        args: Command-line arguments.
    """
    print("\n" + "="*60)
    print(" "*20 + "FOOTBALL PREDICTIONS SYSTEM")
    print(" "*25 + "SETUP WIZARD")
    print("="*60 + "\n")
    
    logger.info("Starting setup...")
    
    # Create directory structure
    create_directory_structure()
    
    # Check environment variables
    env_ok = check_environment_variables()
    if not env_ok and not args.skip_checks:
        logger.error("Environment variables check failed.")
        if not args.force:
            sys.exit(1)
    
    # Check Firebase credentials
    firebase_ok = check_firebase_credentials()
    if not firebase_ok and not args.skip_checks:
        logger.error("Firebase credentials check failed.")
        if not args.force:
            sys.exit(1)
    
    # Initialize Firebase database
    if args.initialize_db:
        db_ok = initialize_firebase_database()
        if not db_ok:
            logger.error("Firebase database initialization failed.")
            if not args.force:
                sys.exit(1)
    
    # Test API access
    if not args.skip_checks:
        api_results = test_api_access()
        if not all(api_results.values()):
            logger.warning("Some API access tests failed.")
            if not args.force:
                print("\nAPI access issues detected. Do you want to continue anyway? (y/n)")
                answer = input().lower()
                if answer != 'y':
                    sys.exit(1)
    
    # Create sample article
    if args.create_sample:
        create_sample_article()
    
    print("\n" + "="*60)
    print(" "*20 + "SETUP COMPLETED SUCCESSFULLY")
    print("="*60 + "\n")
    
    logger.info("Setup completed successfully.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Setup script for the Football Predictions System.")
    parser.add_argument('--force', action='store_true', help='Continue setup even if checks fail')
    parser.add_argument('--skip-checks', action='store_true', help='Skip environment and API checks')
    parser.add_argument('--initialize-db', action='store_true', help='Initialize Firebase database')
    parser.add_argument('--create-sample', action='store_true', help='Create a sample article')
    args = parser.parse_args()
    
    # Default behavior without arguments: perform all setup steps
    if not any([args.skip_checks, args.initialize_db, args.create_sample]):
        args.initialize_db = True
        args.create_sample = True
    
    setup(args)
