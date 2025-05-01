"""
Module for publishing content to WordPress.
This module provides functionality to publish, update, and delete
articles on a WordPress site via the WordPress REST API.
"""
import os
import sys
import time
import logging
import json
import base64
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
    Class for publishing content to WordPress.
    
    Handles authentication, content formatting, and interaction
    with the WordPress REST API.
    """
    
    def __init__(self):
        """Initialize the WordPress publisher."""
        self.wp_url = os.getenv('WP_URL')
        self.wp_user = os.getenv('WP_USER')
        self.wp_password = os.getenv('WP_APP_PASSWORD')
        
        if not all([self.wp_url, self.wp_user, self.wp_password]):
            logger.error("WordPress credentials not found in environment variables.")
            raise ValueError("WordPress credentials not found in environment variables.")
        
        # Verify the URL format
        if not self.wp_url.endswith('/posts'):
            if not self.wp_url.endswith('/'):
                self.wp_url += '/'
            self.wp_url += 'posts'
        
        # Configure authentication
        self.auth_header = self._create_auth_header()
        
        # Initialize database connection
        self.db = FirebaseManager()
        
        # Configure publishing settings
        self.hours_before_match = get_setting('publishing.hours_before_match', 12)
        self.hours_after_match = get_setting('publishing.hours_after_match', 8)
        
        logger.info("WordPress publisher initialized.")
    
    def _create_auth_header(self) -> Dict[str, str]:
        """
        Create authentication header for WordPress API.
        
        Returns:
            Dict[str, str]: Authorization header.
        """
        auth_string = f"{self.wp_user}:{self.wp_password}"
        auth_base64 = base64.b64encode(auth_string.encode()).decode()
        return {"Authorization": f"Basic {auth_base64}"}
    
    def test_connection(self) -> bool:
        """
        Test connection to WordPress API.
        
        Returns:
            bool: True if connection is successful, False otherwise.
        """
        try:
            # Get base URL without posts
            base_url = self.wp_url.rsplit('/posts', 1)[0]
            
            # Try to access the WordPress API
            response = requests.get(base_url, headers=self.auth_header)
            
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
        
        # Format post data
        post_data = {
            "title": title,
            "content": content,
            "status": "publish",
            "categories": self._get_category_ids(article),
            "tags": self._get_tag_ids(article)
        }
        
        # Add excerpt if available
        if 'description' in article:
            post_data['excerpt'] = article['description']
        
        # Add metadata
        post_data['meta'] = {}
        
        # Add match ID to metadata
        if 'match_id' in article:
            post_data['meta']['match_id'] = article['match_id']
        
        # Add expiration time to metadata
        if expiration_time:
            post_data['meta']['expiration_time'] = expiration_time.isoformat()
        
        # Add league ID to metadata
        if 'league_id' in article:
            post_data['meta']['league_id'] = article['league_id']
        
        # Add teams to metadata
        if 'home_team' in article and 'away_team' in article:
            post_data['meta']['home_team'] = article['home_team']
            post_data['meta']['away_team'] = article['away_team']
        
        return post_data
    
    def _get_category_ids(self, article: Dict[str, Any]) -> List[int]:
        """
        Get WordPress category IDs for the article.
        
        Args:
            article: Article data.
            
        Returns:
            List[int]: List of category IDs.
        """
        # Default category for match previews
        default_category_id = get_setting('publishing.default_category_id', 1)
        
        # Get league-specific category ID if available
        league_id = article.get('league_id', '')
        
        if league_id:
            # Get league categories mapping from settings
            league_categories = get_setting('publishing.league_categories', {})
            
            if league_id in league_categories:
                return [league_categories[league_id]]
        
        return [default_category_id]
    
    def _get_tag_ids(self, article: Dict[str, Any]) -> List[int]:
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
                self.wp_url,
                headers={
                    **self.auth_header,
                    "Content-Type": "application/json"
                },
                json=post_data
            )
            
            if response.status_code in [200, 201]:
                logger.info(f"Article published successfully: {article.get('title', 'Untitled')}")
                
                # Extract post ID
                post_data = response.json()
                post_id = post_data.get('id', 0)
                
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
                    "post_url": post_data.get('link', '')
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
    
    def update_article(self, post_id: int, article: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update an existing article on WordPress.
        
        Args:
            post_id: WordPress post ID.
            article: Updated article data.
            
        Returns:
            Dict[str, Any]: Response data.
        """
        logger.info(f"Updating article ID {post_id}: {article.get('title', 'Untitled')}")
        
        try:
            # Format post data
            post_data = self.format_post_data(article)
            
            # Send request to WordPress API
            response = requests.post(
                f"{self.wp_url}/{post_id}",
                headers={
                    **self.auth_header,
                    "Content-Type": "application/json"
                },
                json=post_data
            )
            
            if response.status_code in [200, 201]:
                logger.info(f"Article updated successfully: {article.get('title', 'Untitled')}")
                
                # Update article status in database
                match_id = article.get('match_id', '')
                language = article.get('language', 'en')
                
                if match_id:
                    article_id = f"{match_id}_{language}"
                    article_ref = self.db.get_reference(f"content/articles/{article_id}")
                    article_ref.update({
                        "status": "updated",
                        "updated_at": datetime.now().isoformat()
                    })
                
                return {
                    "success": True,
                    "post_id": post_id,
                    "post_url": response.json().get('link', '')
                }
            else:
                logger.error(f"Failed to update article: {response.status_code}")
                logger.error(f"Response: {response.text}")
                
                return {
                    "success": False,
                    "error": f"Failed to update article: {response.status_code}",
                    "response": response.text
                }
        
        except Exception as e:
            logger.error(f"Error updating article: {e}")
            
            return {
                "success": False,
                "error": f"Error updating article: {str(e)}"
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
                f"{self.wp_url}/{post_id}",
                headers=self.auth_header,
                params={"force": True}
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
        
        Args:
            hours_after_match: Hours after match to consider article expired.
            
        Returns:
            List[Dict[str, Any]]: List of expired articles.
        """
        logger.info("Checking for expired articles")
        
        if hours_after_match is None:
            hours_after_match = self.hours_after_match
        
        try:
            # Get all published articles from WordPress
            response = requests.get(
                self.wp_url,
                headers=self.auth_header,
                params={
                    "per_page": 100,
                    "status": "publish"
                }
            )
            
            if response.status_code != 200:
                logger.error(f"Failed to get published articles: {response.status_code}")
                logger.error(f"Response: {response.text}")
                return []
            
            # Extract articles with expiration time
            articles = response.json()
            expired_articles = []
            
            now = datetime.now()
            
            for article in articles:
                # Check if article has match_id metadata
                if 'meta' not in article or 'match_id' not in article['meta']:
                    continue
                
                # Check if article has expiration_time metadata
                if 'expiration_time' not in article['meta']:
                    continue
                
                # Check if article has expired
                try:
                    expiration_time = datetime.fromisoformat(article['meta']['expiration_time'].replace('Z', '+00:00'))
                    
                    if now > expiration_time:
                        expired_articles.append({
                            "post_id": article['id'],
                            "title": article['title']['rendered'],
                            "match_id": article['meta']['match_id'],
                            "expiration_time": article['meta']['expiration_time']
                        })
                
                except (ValueError, TypeError) as e:
                    logger.warning(f"Invalid expiration time for article {article['id']}: {e}")
                    continue
            
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

# Global functions for simplified access
def publish_article(article: Dict[str, Any]) -> Dict[str, Any]:
    """
    Publish an article to WordPress.
    
    Args:
        article: Article data.
        
    Returns:
        Dict[str, Any]: Response data with post ID.
    """
    wp = WordPressPublisher()
    return wp.publish_article(article)

def update_article(post_id: int, article: Dict[str, Any]) -> Dict[str, Any]:
    """
    Update an existing article on WordPress.
    
    Args:
        post_id: WordPress post ID.
        article: Updated article data.
        
    Returns:
        Dict[str, Any]: Response data.
    """
    wp = WordPressPublisher()
    return wp.update_article(post_id, article)

def delete_article(post_id: int) -> Dict[str, Any]:
    """
    Delete an article from WordPress.
    
    Args:
        post_id: WordPress post ID.
        
    Returns:
        Dict[str, Any]: Response data.
    """
    wp = WordPressPublisher()
    return wp.delete_article(post_id)

def delete_expired_articles(hours_after_match: Optional[int] = None) -> Dict[str, Any]:
    """
    Delete articles that have expired.
    
    Args:
        hours_after_match: Hours after match to consider article expired.
        
    Returns:
        Dict[str, Any]: Response data with count of deleted articles.
    """
    wp = WordPressPublisher()
    return wp.delete_expired_articles(hours_after_match)
