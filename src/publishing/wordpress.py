"""
Module for publishing content to WordPress.
This module provides functionality to publish, update, and delete
articles on a WordPress site via a custom WordPress API.
"""
import os
import sys
import time
import logging
import json
import requests
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta

# Add the parent directory to sys.path
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from src.utils.database import FirebaseManager
from src.config.settings import get_setting

logger = logging.getLogger(__name__)

class WordPressPublisher:
    """
    Class for publishing content to WordPress via custom API.
    
    Handles authentication, content formatting, and interaction
    with the WordPress custom API.
    """
    
    def __init__(self):
        """Initialize the WordPress publisher."""
        # Base URL for your WordPress site
        self.wp_base_url = os.getenv('WP_URL').replace('/wp-json/wp/v2', '')
        
        # Custom API endpoint
        self.api_url = f"{self.wp_base_url}/wp-json/football-predictions/v1"
        
        # API Key
        self.api_key = os.getenv('WP_APP_PASSWORD')
        
        if not all([self.wp_base_url, self.api_key]):
            logger.error("WordPress credentials not found in environment variables.")
            raise ValueError("WordPress credentials not found in environment variables.")
        
        # Configure authentication header
        self.auth_header = {"X-API-Key": self.api_key}
        
        # Initialize database connection
        self.db = FirebaseManager()
        
        # Configure publishing settings
        self.hours_before_match = get_setting('publishing.hours_before_match', 12)
        self.hours_after_match = get_setting('publishing.hours_after_match', 8)
        
        logger.info("WordPress publisher initialized.")
    
    def test_connection(self) -> bool:
        """
        Test connection to WordPress API.
        
        Returns:
            bool: True if connection is successful, False otherwise.
        """
        try:
            # Try to access the WordPress site
            response = requests.get(self.wp_base_url)
            
            if response.status_code in [200, 201]:
                logger.info("WordPress connection test successful.")
                return True
            else:
                logger.error(f"WordPress connection test failed. Status code: {response.status_code}")
                logger.error(f"Response: {response.text}")
                return False
        
        except Exception as e:
            logger.error(f"WordPress connection test failed: {e}")
            return False
    
    def format_post_data(self, article: Dict[str, Any]) -> Dict[str, Any]:
        """
        Format article data for WordPress API.
        
        Args:
            article: Article data.
            
        Returns:
            Dict[str, Any]: Formatted post data for WordPress API.
        """
        # Get post title
        title = article.get('title', 'Match Preview')
        
        # Get post content
        content = article.get('content', '')
        
        # Get post excerpt
        excerpt = article.get('description', '')
        
        # Get match datetime for scheduling
        match_datetime = article.get('match_datetime', '')
        
        # Calculate post expiration time
        expiration_time = None
        if match_datetime:
            try:
                match_dt = datetime.fromisoformat(match_datetime.replace('Z', '+00:00'))
                expiration_time = match_dt + timedelta(hours=self.hours_after_match)
            except (ValueError, TypeError):
                logger.warning(f"Invalid match datetime: {match_datetime}")
        
        # Get category ID
        category_id = self._get_category_id(article)
        
        # Get tags
        tags = self._get_tags(article)
        
        # Format post data
        post_data = {
            "title": title,
            "content": content,
            "excerpt": excerpt,
            "category_id": category_id,
            "tags": tags,
            "metadata": {}
        }
        
        # Add match ID to metadata
        if 'match_id' in article:
            post_data['metadata']['match_id'] = article['match_id']
        
        # Add expiration time to metadata
        if expiration_time:
            post_data['metadata']['expiration_time'] = expiration_time.isoformat()
        
        # Add league ID to metadata
        if 'league_id' in article:
            post_data['metadata']['league_id'] = article['league_id']
        
        # Add teams to metadata
        if 'home_team' in article and 'away_team' in article:
            post_data['metadata']['home_team'] = article['home_team']
            post_data['metadata']['away_team'] = article['away_team']
        
        return post_data
    
    def _get_category_id(self, article: Dict[str, Any]) -> int:
        """
        Get WordPress category ID for the article.
        
        Args:
            article: Article data.
            
        Returns:
            int: Category ID.
        """
        # Default category for match previews
        default_category_id = get_setting('publishing.default_category_id', 1)
        
        # Get league-specific category ID if available
        league_id = article.get('league_id', '')
        
        if league_id:
            # Get league categories mapping from settings
            league_categories = get_setting('publishing.league_categories', {})
            
            if league_id in league_categories:
                return league_categories[league_id]
        
        return default_category_id
    
    def _get_tags(self, article: Dict[str, Any]) -> List[int]:
        """
        Get WordPress tag IDs for the article.
        
        Args:
            article: Article data.
            
        Returns:
            List[int]: List of tag IDs.
        """
        # Default tags
        tag_ids = []
        
        # Add "Match Preview" tag
        preview_tag_id = get_setting('publishing.preview_tag_id', None)
        if preview_tag_id:
            tag_ids.append(preview_tag_id)
        
        # Add team tags if available
        team_tags = get_setting('publishing.team_tags', {})
        
        home_team = article.get('home_team', '')
        away_team = article.get('away_team', '')
        
        if home_team and home_team in team_tags:
            tag_ids.append(team_tags[home_team])
        
        if away_team and away_team in team_tags:
            tag_ids.append(team_tags[away_team])
        
        return tag_ids
    
    def publish_article(self, article: Dict[str, Any]) -> Dict[str, Any]:
        """
        Publish an article to WordPress.
        
        Args:
            article: Article data.
            
        Returns:
            Dict[str, Any]: Response data with post ID.
        """
        logger.info(f"Publishing article: {article.get('title', 'Untitled')}")
        
        try:
            # Format post data
            post_data = self.format_post_data(article)
            
            # Send request to WordPress API
            response = requests.post(
                f"{self.api_url}/publish",
                headers={
                    **self.auth_header,
                    "Content-Type": "application/json"
                },
                json=post_data
            )
            
            if response.status_code in [200, 201]:
                logger.info(f"Article published successfully: {article.get('title', 'Untitled')}")
                
                # Extract post ID
                response_data = response.json()
                post_id = response_data.get('post_id', 0)
                
                # Update article status in database
                match_id = article.get('match_id', '')
                language = article.get('language', 'en')
                
                if match_id:
                    article_id = f"{match_id}_{language}"
                    article_ref = self.db.get_reference(f"content/articles/{article_id}")
                    article_ref.update({
                        "status": "published",
                        "post_id": post_id,
                        "published_at": datetime.now().isoformat()
                    })
                
                return {
                    "success": True,
                    "post_id": post_id,
                    "post_url": response_data.get('post_url', '')
                }
            else:
                logger.error(f"Failed to publish article: {response.status_code}")
                logger.error(f"Response: {response.text}")
                
                return {
                    "success": False,
                    "error": f"Failed to publish article: {response.status_code}",
                    "response": response.text
                }
        
        except Exception as e:
            logger.error(f"Error publishing article: {e}")
            
            return {
                "success": False,
                "error": f"Error publishing article: {str(e)}"
            }
    
    def delete_article(self, post_id: int) -> Dict[str, Any]:
        """
        Delete an article from WordPress.
        
        Args:
            post_id: WordPress post ID.
            
        Returns:
            Dict[str, Any]: Response data.
        """
        logger.info(f"Deleting article ID {post_id}")
        
        try:
            # Send request to WordPress API
            response = requests.delete(
                f"{self.api_url}/delete/{post_id}",
                headers=self.auth_header
            )
            
            if response.status_code in [200, 201, 204]:
                logger.info(f"Article deleted successfully: {post_id}")
                
                # Find and update article status in database
                articles_ref = self.db.get_reference("content/articles")
                articles = articles_ref.get()
                
                if articles:
                    for article_id, article_data in articles.items():
                        if article_data.get('post_id') == post_id:
                            article_ref = self.db.get_reference(f"content/articles/{article_id}")
                            article_ref.update({
                                "status": "deleted",
                                "deleted_at": datetime.now().isoformat()
                            })
                            break
                
                return {
                    "success": True,
                    "post_id": post_id
                }
            else:
                logger.error(f"Failed to delete article: {response.status_code}")
                logger.error(f"Response: {response.text}")
                
                return {
                    "success": False,
                    "error": f"Failed to delete article: {response.status_code}",
                    "response": response.text
                }
        
        except Exception as e:
            logger.error(f"Error deleting article: {e}")
            
            return {
                "success": False,
                "error": f"Error deleting article: {str(e)}"
            }
    
    def get_expired_articles(self, hours_after_match: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get articles that have expired and should be deleted.
        
        This is now handled by checking the database since we can't easily
        query WordPress for metadata with our custom API.
        
        Args:
            hours_after_match: Hours after match to consider article expired.
            
        Returns:
            List[Dict[str, Any]]: List of expired articles.
        """
        logger.info("Checking for expired articles")
        
        if hours_after_match is None:
            hours_after_match = self.hours_after_match
        
        try:
            # Get all matches from database
            matches_ref = self.db.get_reference("data/matches")
            matches = matches_ref.get() or {}
            
            # Filter matches with published articles
            published_matches = [
                match for match_id, match in matches.items()
                if match.get("article_published") and not match.get("article_removed")
            ]
            
            # Check which articles have expired
            expired_articles = []
            now = datetime.now()
            
            for match in published_matches:
                # Get match datetime
                match_datetime = parse_date(match.get("datetime", ""))
                if not match_datetime:
                    continue
                
                # Calculate expiration time
                expiration_time = match_datetime + timedelta(hours=hours_after_match)
                
                # Check if article has expired
                if now > expiration_time:
                    expired_articles.append({
                        "post_id": match.get("article_id"),
                        "title": f"{match.get('home_team', '')} vs {match.get('away_team', '')}",
                        "match_id": match.get("match_id", ""),
                        "expiration_time": expiration_time.isoformat()
                    })
            
            logger.info(f"Found {len(expired_articles)} expired articles")
            return expired_articles
            
        except Exception as e:
            logger.error(f"Error getting expired articles: {e}")
            return []
    
    def delete_expired_articles(self, hours_after_match: Optional[int] = None) -> Dict[str, Any]:
        """
        Delete articles that have expired.
        
        Args:
            hours_after_match: Hours after match to consider article expired.
            
        Returns:
            Dict[str, Any]: Response data with count of deleted articles.
        """
        logger.info("Deleting expired articles")
        
        try:
            # Get expired articles
            expired_articles = self.get_expired_articles(hours_after_match)
            
            if not expired_articles:
                logger.info("No expired articles found")
                return {"success": True, "deleted_count": 0}
            
            # Delete each expired article
            deleted_count = 0
            
            for article in expired_articles:
                result = self.delete_article(article['post_id'])
                
                if result['success']:
                    deleted_count += 1
                else:
                    logger.warning(f"Failed to delete expired article {article['post_id']}")
            
            logger.info(f"Deleted {deleted_count}/{len(expired_articles)} expired articles")
            
            return {
                "success": True,
                "deleted_count": deleted_count,
                "total_expired": len(expired_articles)
            }
        
        except Exception as e:
            logger.error(f"Error deleting expired articles: {e}")
            
            return {
                "success": False,
                "error": f"Error deleting expired articles: {str(e)}",
                "deleted_count": 0
            }
