#!/usr/bin/env python3
"""
Daily coordinator script for the Football Predictions System.
This script orchestrates the daily operations: data collection,
content generation, and article publishing.
"""
import os
import sys
import time
import logging
import argparse
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import project modules
try:
    from src.utils.database import FirebaseManager
    from src.config.settings import get_setting
    from src.data.collector import collect_league_data
    from src.content.generator import generate_multiple_articles, save_article
    from src.monitoring.health_checker import check_system_health
    from src.monitoring.backup import create_backup
    from src.publishing.wordpress import publish_article, delete_expired_articles
    from dotenv import load_dotenv
except ImportError as e:
    print(f"Error importing modules: {e}")
    print("Make sure to run this script from the project root directory.")
    sys.exit(1)

# Configure logging
log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs')
os.makedirs(log_dir, exist_ok=True)

log_file = os.path.join(log_dir, f'daily_run_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_file)
    ]
)

logger = logging.getLogger(__name__)

def collect_data(args: argparse.Namespace) -> bool:
    """
    Collect data from external sources.
    
    Args:
        args: Command-line arguments
        
    Returns:
        bool: True if data collection was successful, False otherwise
    """
    logger.info("Starting data collection...")
    
    try:
        # Get leagues to collect data for
        league_ids = args.leagues.split(',') if args.leagues else None
        
        # Get the data limit
        limit = args.limit if args.limit else get_setting('data.collection.match_limit', 100)
        
        # Collect data for each league
        if league_ids:
            for league_id in league_ids:
                logger.info(f"Collecting data for league: {league_id}")
                result = collect_league_data(league_id, limit=limit)
                
                if result and 'matches_collected' in result:
                    logger.info(f"Collected {result['matches_collected']} matches for league {league_id}")
                else:
                    logger.warning(f"No data collected for league {league_id}")
        else:
            # Collect data for all active leagues
            db = FirebaseManager()
            leagues_ref = db.get_reference("config/leagues")
            leagues = leagues_ref.get()
            
            if not leagues:
                logger.error("No leagues found in database")
                return False
            
            active_leagues = []
            for league_id, league_data in leagues.items():
                if league_data.get('active', True):
                    active_leagues.append(league_id)
            
            if not active_leagues:
                logger.warning("No active leagues found")
                return False
            
            logger.info(f"Collecting data for {len(active_leagues)} active leagues")
            
            for league_id in active_leagues:
                logger.info(f"Collecting data for league: {league_id}")
                result = collect_league_data(league_id, limit=limit)
                
                if result and 'matches_collected' in result:
                    logger.info(f"Collected {result['matches_collected']} matches for league {league_id}")
                else:
                    logger.warning(f"No data collected for league {league_id}")
        
        logger.info("Data collection completed successfully")
        
        # Update system status in Firebase
        db = FirebaseManager()
        system_ref = db.get_reference("system")
        system_ref.update({
            "last_data_collection": datetime.now().isoformat(),
            "status": "data_collected"
        })
        
        return True
    
    except Exception as e:
        logger.error(f"Error during data collection: {e}")
        
        # Update system status in Firebase
        try:
            db = FirebaseManager()
            system_ref = db.get_reference("system")
            system_ref.update({
                "last_data_collection_error": datetime.now().isoformat(),
                "last_error": str(e),
                "status": "error"
            })
        except Exception as db_error:
            logger.error(f"Error updating system status: {db_error}")
        
        return False

def generate_content(args: argparse.Namespace) -> bool:
    """
    Generate content for upcoming matches.
    
    Args:
        args: Command-line arguments
        
    Returns:
        bool: True if content generation was successful, False otherwise
    """
    logger.info("Starting content generation...")
    
    try:
        # Get leagues to generate content for
        league_ids = args.leagues.split(',') if args.leagues else None
        
        # Get the content limit
        limit = args.limit if args.limit else get_setting('content.generation.article_limit', 50)
        
        # Get the content language
        language = args.language if args.language else get_setting('content.language', 'en')
        
        # Get the content format
        format_type = args.format if args.format else get_setting('content.format', 'markdown')
        
        # Generate content
        articles = generate_multiple_articles(
            league_id=league_ids[0] if league_ids else None,
            limit=limit,
            language=language,
            format_type=format_type
        )
        
        if not articles:
            logger.warning("No articles generated")
            return False
        
        logger.info(f"Generated {len(articles)} articles")
        
        # Save articles to Firebase
        saved_count = 0
        for article in articles:
            if save_article(article):
                saved_count += 1
        
        logger.info(f"Saved {saved_count}/{len(articles)} articles to Firebase")
        
        # Update system status in Firebase
        db = FirebaseManager()
        system_ref = db.get_reference("system")
        system_ref.update({
            "last_content_generation": datetime.now().isoformat(),
            "status": "content_generated"
        })
        
        return True
    
    except Exception as e:
        logger.error(f"Error during content generation: {e}")
        
        # Update system status in Firebase
        try:
            db = FirebaseManager()
            system_ref = db.get_reference("system")
            system_ref.update({
                "last_content_generation_error": datetime.now().isoformat(),
                "last_error": str(e),
                "status": "error"
            })
        except Exception as db_error:
            logger.error(f"Error updating system status: {db_error}")
        
        return False

def publish_articles(args: argparse.Namespace) -> bool:
    """
    Publish generated articles to WordPress.
    
    Args:
        args: Command-line arguments
        
    Returns:
        bool: True if article publishing was successful, False otherwise
    """
    logger.info("Starting article publishing...")
    
    try:
        # Get the publishing limit
        limit = args.limit if args.limit else get_setting('publishing.article_limit', 20)
        
        # Get hours before match for publishing
        hours_before_match = get_setting('publishing.hours_before_match', 12)
        
        # Get articles from Firebase
        db = FirebaseManager()
        articles_ref = db.get_reference("content/articles")
        all_articles = articles_ref.get()
        
        if not all_articles:
            logger.warning("No articles found in Firebase")
            return False
        
        # Filter articles by status and match time
        articles_to_publish = []
        now = datetime.now()
        
        for article_id, article in all_articles.items():
            # Check if article is already published
            if article.get('status') == 'published':
                continue
            
            # Check if article has match datetime
            match_datetime = article.get('match_datetime')
            if not match_datetime:
                continue
            
            try:
                # Convert to datetime
                match_dt = datetime.fromisoformat(match_datetime.replace('Z', '+00:00'))
                
                # Check if it's time to publish
                time_to_match = match_dt - now
                
                if time_to_match > timedelta(hours=hours_before_match):
                    # Match is too far in the future
                    continue
                
                if time_to_match < timedelta():
                    # Match has already started
                    continue
                
                # Article should be published
                article['article_id'] = article_id
                articles_to_publish.append(article)
            
            except (ValueError, TypeError):
                logger.warning(f"Invalid match datetime for article: {article_id}")
                continue
        
        # Limit the number of articles to publish
        articles_to_publish = articles_to_publish[:limit]
        
        if not articles_to_publish:
            logger.warning("No articles to publish at this time")
            return True  # Not an error
        
        logger.info(f"Found {len(articles_to_publish)} articles to publish")
        
        # Publish articles to WordPress
        published_count = 0
        for article in articles_to_publish:
            result = publish_article(article)
            
            if result and 'post_id' in result:
                # Update article status in Firebase
                article_ref = articles_ref.child(article['article_id'])
                article_ref.update({
                    "status": "published",
                    "post_id": result['post_id'],
                    "published_at": datetime.now().isoformat()
                })
                
                published_count += 1
            else:
                logger.warning(f"Failed to publish article: {article['article_id']}")
        
        logger.info(f"Published {published_count}/{len(articles_to_publish)} articles to WordPress")
        
        # Update system status in Firebase
        system_ref = db.get_reference("system")
        system_ref.update({
            "last_article_publishing": datetime.now().isoformat(),
            "status": "articles_published"
        })
        
        return True
    
    except Exception as e:
        logger.error(f"Error during article publishing: {e}")
        
        # Update system status in Firebase
        try:
            db = FirebaseManager()
            system_ref = db.get_reference("system")
            system_ref.update({
                "last_article_publishing_error": datetime.now().isoformat(),
                "last_error": str(e),
                "status": "error"
            })
        except Exception as db_error:
            logger.error(f"Error updating system status: {db_error}")
        
        return False

def cleanup_expired_articles(args: argparse.Namespace) -> bool:
    """
    Delete expired articles from WordPress.
    
    Args:
        args: Command-line arguments
        
    Returns:
        bool: True if cleanup was successful, False otherwise
    """
    logger.info("Starting cleanup of expired articles...")
    
    try:
        # Get hours after match for deletion
        hours_after_match = get_setting('publishing.hours_after_match', 8)
        
        # Delete expired articles
        result = delete_expired_articles(hours_after_match=hours_after_match)
        
        if result and 'deleted_count' in result:
            logger.info(f"Deleted {result['deleted_count']} expired articles")
        else:
            logger.warning("No expired articles deleted")
        
        # Update system status in Firebase
        db = FirebaseManager()
        system_ref = db.get_reference("system")
        system_ref.update({
            "last_article_cleanup": datetime.now().isoformat(),
            "status": "articles_cleaned_up"
        })
        
        return True
    
    except Exception as e:
        logger.error(f"Error during expired article cleanup: {e}")
        
        # Update system status in Firebase
        try:
            db = FirebaseManager()
            system_ref = db.get_reference("system")
            system_ref.update({
                "last_article_cleanup_error": datetime.now().isoformat(),
                "last_error": str(e),
                "status": "error"
            })
        except Exception as db_error:
            logger.error(f"Error updating system status: {db_error}")
        
        return False

def update_health(args: argparse.Namespace) -> bool:
    """
    Update system health status.
    
    Args:
        args: Command-line arguments
        
    Returns:
        bool: True if health update was successful, False otherwise
    """
    logger.info("Updating system health status...")
    
    try:
        # Check system health
        health_check = check_system_health()
        
        # Log health status
        if health_check:
            status = health_check.get('status', 'unknown')
            logger.info(f"System health status: {status}")
            
            # Log component statuses
            components = health_check.get('components', {})
            for component, component_data in components.items():
                component_status = component_data.get('status', 'unknown')
                logger.info(f"Component '{component}' status: {component_status}")
        else:
            logger.warning("No health check results")
        
        # Create a daily backup if requested
        if args.create_backup:
            backup_result = create_backup(backup_type='daily')
            if backup_result and backup_result.get('success', False):
                logger.info(f"Created daily backup: {backup_result.get('backup_file', 'unknown')}")
            else:
                logger.warning("Failed to create daily backup")
        
        return True
    
    except Exception as e:
        logger.error(f"Error during health update: {e}")
        return False

def run_full_process(args: argparse.Namespace) -> bool:
    """
    Run the full process: collect data, generate content, and publish articles.
    
    Args:
        args: Command-line arguments
        
    Returns:
        bool: True if all steps were successful, False otherwise
    """
    logger.info("Starting full process...")
    
    # Step 1: Collect data
    data_ok = collect_data(args)
    if not data_ok and not args.continue_on_error:
        logger.error("Data collection failed, stopping process")
        return False
    
    # Step 2: Generate content
    content_ok = generate_content(args)
    if not content_ok and not args.continue_on_error:
        logger.error("Content generation failed, stopping process")
        return False
    
    # Step 3: Publish articles
    publish_ok = publish_articles(args)
    if not publish_ok and not args.continue_on_error:
        logger.error("Article publishing failed, stopping process")
        return False
    
    # Step 4: Update health
    health_ok = update_health(args)
    if not health_ok and not args.continue_on_error:
        logger.error("Health update failed, stopping process")
        return False
    
    logger.info("Full process completed successfully")
    return True

def main() -> None:
    """Main function."""
    # Load environment variables
    load_dotenv()
    
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Daily coordinator for the Football Predictions System")
    
    # Action arguments
    parser.add_argument('--all', action='store_true', 
                      help='Run the full process: collect data, generate content, publish articles, and update health')
    parser.add_argument('--collect-data', action='store_true', 
                      help='Collect data from external sources')
    parser.add_argument('--generate-content', action='store_true', 
                      help='Generate content for upcoming matches')
    parser.add_argument('--publish-articles', action='store_true', 
                      help='Publish generated articles to WordPress')
    parser.add_argument('--cleanup-expired', action='store_true', 
                      help='Delete expired articles from WordPress')
    parser.add_argument('--update-health', action='store_true', 
                      help='Update system health status')
    
    # Optional parameters
    parser.add_argument('--leagues', type=str, 
                      help='Comma-separated list of league IDs')
    parser.add_argument('--limit', type=int, 
                      help='Limit the number of items to process')
    parser.add_argument('--language', type=str, choices=['en', 'it'], 
                      help='Content language')
    parser.add_argument('--format', type=str, choices=['markdown', 'html'], 
                      help='Content format')
    parser.add_argument('--continue-on-error', action='store_true', 
                      help='Continue with the next step even if the current one fails')
    parser.add_argument('--create-backup', action='store_true', 
                      help='Create a daily backup')
    
    args = parser.parse_args()
    
    # Check if at least one action is specified
    actions = [args.all, args.collect_data, args.generate_content, 
              args.publish_articles, args.cleanup_expired, args.update_health]
    
    if not any(actions):
        parser.print_help()
        sys.exit(1)
    
    # Run the specified actions
    if args.all:
        success = run_full_process(args)
    else:
        success = True
        
        if args.collect_data:
            data_ok = collect_data(args)
            success = success and data_ok
            if not data_ok and not args.continue_on_error:
                logger.error("Data collection failed, stopping process")
                sys.exit(1)
        
        if args.generate_content:
            content_ok = generate_content(args)
            success = success and content_ok
            if not content_ok and not args.continue_on_error:
                logger.error("Content generation failed, stopping process")
                sys.exit(1)
        
        if args.publish_articles:
            publish_ok = publish_articles(args)
            success = success and publish_ok
            if not publish_ok and not args.continue_on_error:
                logger.error("Article publishing failed, stopping process")
                sys.exit(1)
        
        if args.cleanup_expired:
            cleanup_ok = cleanup_expired_articles(args)
            success = success and cleanup_ok
            if not cleanup_ok and not args.continue_on_error:
                logger.error("Expired article cleanup failed, stopping process")
                sys.exit(1)
        
        if args.update_health:
            health_ok = update_health(args)
            success = success and health_ok
            if not health_ok and not args.continue_on_error:
                logger.error("Health update failed, stopping process")
                sys.exit(1)
    
    # Exit with appropriate status code
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
