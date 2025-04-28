#!/usr/bin/env python3
"""
Publish to WordPress - Publishes articles to WordPress
Manages publication, updates and expiration of articles
"""
import os
import sys
import logging
import time
import json
import base64
import requests
from datetime import datetime, timedelta
import firebase_admin
from firebase_admin import credentials, db

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("wordpress_publisher")

# Languages to publish
LANGUAGES = [
    'en',  # English
    'es',  # Spanish
    'it',  # Italian
    'de',  # German
    'fr',  # French
    'pt',  # Portuguese
    'nl',  # Dutch
    'pl',  # Polish
    'sv',  # Swedish
    'ja',  # Japanese
    'zh',  # Chinese
    'no',  # Norwegian
    'tr',  # Turkish
    'ar',  # Arabic
    'ru',  # Russian
]

def initialize_firebase():
    """Initialize Firebase connection"""
    try:
        firebase_admin.get_app()
    except ValueError:
        # GitHub Actions or CI environment
        if os.environ.get('GITHUB_ACTIONS') or os.environ.get('CI'):
            firebase_credentials = os.environ.get('FIREBASE_CREDENTIALS')
            if firebase_credentials:
                with open('firebase-credentials.json', 'w') as f:
                    f.write(firebase_credentials)
                cred = credentials.Certificate('firebase-credentials.json')
            else:
                raise Exception("FIREBASE_CREDENTIALS not found")
        else:
            # Local use
            cred_path = os.path.expanduser('~/football-predictions/creds/firebase-credentials.json')
            cred = credentials.Certificate(cred_path)
        
        firebase_admin.initialize_app(cred, {
            'databaseURL': os.getenv('FIREBASE_DB_URL')
        })
    return True

def get_wp_auth_header():
    """Generate WordPress authentication header"""
    wp_user = os.getenv('WP_USER')
    wp_password = os.getenv('WP_APP_PASSWORD')
    
    if not wp_user or not wp_password:
        logger.error("Missing WordPress credentials")
        return None
    
    auth_string = f"{wp_user}:{wp_password}"
    auth_base64 = base64.b64encode(auth_string.encode()).decode()
    return {'Authorization': f"Basic {auth_base64}"}

def get_wp_url():
    """Get WordPress API URL"""
    wp_url = os.getenv('WP_URL')
    
    if not wp_url:
        logger.error("Missing WordPress URL")
        return None
    
    # Make sure URL ends with /wp-json/wp/v2
    if not wp_url.endswith('/wp-json/wp/v2'):
        if wp_url.endswith('/'):
            wp_url = wp_url + 'wp-json/wp/v2'
        else:
            wp_url = wp_url + '/wp-json/wp/v2'
    
    return wp_url

def get_articles_to_publish():
    """Get articles ready for publication"""
    logger.info("Getting articles to publish...")
    
    try:
        articles_ref = db.reference('articles')
        
        # Get all articles
        articles = articles_ref.get()
        
        if not articles:
            logger.warning("No articles found in the database")
            return []
        
        articles_to_publish = []
        
        now = datetime.now()
        
        for article_id, article_data in articles.items():
            # Skip if already published
            if article_data.get('published', False):
                continue
                
            # Check if we're in the publishing window
            publish_time = article_data.get('publish_time')
            if publish_time:
                publish_datetime = datetime.fromisoformat(publish_time.replace('Z', '+00:00'))
                
                # If we haven't reached the publishing time yet, skip
                if now < publish_datetime:
                    continue
            
            # Add match_id if it doesn't exist (for compatibility)
            if 'match_id' not in article_data and 'id' in article_data:
                article_data['match_id'] = article_data['id']
            
            # Add article data to list with id
            article_data['id'] = article_id
            articles_to_publish.append(article_data)
        
        logger.info(f"Found {len(articles_to_publish)} articles to publish")
        return articles_to_publish
    
    except Exception as e:
        logger.error(f"Error getting articles to publish: {str(e)}")
        return []

def get_articles_to_expire():
    """Get articles to remove (expired)"""
    logger.info("Getting articles to expire...")
    
    try:
        articles_ref = db.reference('articles')
        
        # Get all articles
        articles = articles_ref.get()
        
        if not articles:
            logger.warning("No articles found in the database")
            return []
        
        articles_to_expire = []
        
        now = datetime.now()
        
        for article_id, article_data in articles.items():
            # Skip if not published or already expired
            if not article_data.get('published', False) or article_data.get('expired', False):
                continue
                
            # Check if we're past the expiration time
            expire_time = article_data.get('expire_time')
            if expire_time:
                expire_datetime = datetime.fromisoformat(expire_time.replace('Z', '+00:00'))
                
                # If we've passed the expiration time, add to list
                if now > expire_datetime:
                    # Add match_id if it doesn't exist (for compatibility)
                    if 'match_id' not in article_data and 'id' in article_data:
                        article_data['match_id'] = article_data['id']
                    
                    # Add article data to list with id
                    article_data['id'] = article_id
                    articles_to_expire.append(article_data)
        
        logger.info(f"Found {len(articles_to_expire)} articles to expire")
        return articles_to_expire
    
    except Exception as e:
        logger.error(f"Error getting articles to expire: {str(e)}")
        return []

def create_wp_post(article, lang='en'):
    """Create post on WordPress"""
    wp_url = get_wp_url()
    if not wp_url:
        return None
    
    # Get content in the specified language
    title = article.get('title', 'Match Preview')
    content = article.get('content', '')
    
    # If language is not English, try to get the translation
    if lang != 'en':
        # Check both data structures
        languages = article.get('languages', {})
        translations = article.get('translations', {})
        
        lang_data = None
        if lang in languages and languages[lang].get('content'):
            lang_data = languages[lang]
        elif lang in translations and translations[lang].get('content'):
            lang_data = translations[lang]
            
        if lang_data:
            title = lang_data.get('title', title)
            content = lang_data.get('content', content)
        else:
            logger.warning(f"Translation not available for article {article.get('id')} in {lang}")
            return None
    
    # Prepare team logos
    home_team = article.get('home_team', '').replace(' ', '-').lower()
    away_team = article.get('away_team', '').replace(' ', '-').lower()
    
    # Add shortcode for team logos if not already present
    if home_team and away_team and '[match_logos]' not in content:
        content = f'[match_logos home="{home_team}" away="{away_team}"]\n\n' + content
    
    # Prepare post data
    post_data = {
        'title': title,
        'content': content,
        'status': 'publish',
        'categories': [get_category_id('Football Predictions')],  # Default category
        'meta': {
            'match_id': article.get('match_id', article.get('id', '')),
            'home_team': article.get('home_team', ''),
            'away_team': article.get('away_team', ''),
            'competition': article.get('competition', ''),
            'match_time': article.get('match_time', ''),
            'expire_time': article.get('expire_time', ''),
            'article_language': lang
        }
    }
    
    # If it's a translation, add Polylang relation
    wp_post_id = article.get('wp_post_id', {})
    if lang != 'en' and wp_post_id and 'en' in wp_post_id:
        post_data['meta']['_pll_translation_of'] = wp_post_id['en']
    
    # Send request to WordPress
    headers = get_wp_auth_header()
    if not headers:
        return None
    
    try:
        logger.info(f"Creating WordPress post: {title}")
        response = requests.post(
            f"{wp_url}/posts",
            json=post_data,
            headers=headers
        )
        
        if response.status_code in [200, 201]:
            return response.json()
        else:
            logger.error(f"Publication error: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        logger.error(f"WordPress request error: {str(e)}")
        return None

def trash_wp_post(post_id):
    """Move post to trash"""
    wp_url = get_wp_url()
    if not wp_url or not post_id:
        return False
    
    headers = get_wp_auth_header()
    if not headers:
        return False
    
    try:
        logger.info(f"Trashing WordPress post {post_id}")
        # Update status to 'trash'
        response = requests.put(
            f"{wp_url}/posts/{post_id}",
            json={'status': 'trash'},
            headers=headers
        )
        
        return response.status_code in [200, 201]
    except Exception as e:
        logger.error(f"Post deletion error: {str(e)}")
        return False

def get_category_id(category_name):
    """Get category ID from name (or create if it doesn't exist)"""
    wp_url = get_wp_url()
    if not wp_url:
        return 1  # Default category ID
    
    headers = get_wp_auth_header()
    if not headers:
        return 1  # Default category ID
    
    try:
        # Search for category
        response = requests.get(
            f"{wp_url}/categories",
            params={'search': category_name},
            headers=headers
        )
        
        if response.status_code == 200:
            categories = response.json()
            
            # If found, return ID
            for cat in categories:
                if cat['name'].lower() == category_name.lower():
                    return cat['id']
            
            # Otherwise create new category
            if len(categories) == 0:
                create_response = requests.post(
                    f"{wp_url}/categories",
                    json={'name': category_name},
                    headers=headers
                )
                
                if create_response.status_code in [200, 201]:
                    return create_response.json().get('id')
        
        # Default category if all else fails
        return 1
    except Exception as e:
        logger.error(f"Error getting category: {str(e)}")
        return 1

def publish_article(article):
    """Publish article in all available languages"""
    results = {}
    article_id = article.get('id', '')
    match_id = article.get('match_id', article_id)
    
    # Get available languages from both data structures for compatibility
    languages = article.get('languages', {})
    translations = article.get('translations', {})
    
    # Combine available languages from both structures
    available_langs = set()
    for lang in LANGUAGES:
        if lang in languages and languages[lang].get('status') == 'completed':
            available_langs.add(lang)
        if lang in translations and translations[lang].get('content'):
            available_langs.add(lang)
    
    # First publish in English (primary language)
    if 'en' in available_langs:
        logger.info(f"Publishing {match_id} in English")
        en_result = create_wp_post(article, 'en')
        
        if en_result:
            wp_post_id = en_result.get('id')
            results['en'] = wp_post_id
            
            # Save WordPress ID to Firebase
            ref = db.reference(f'articles/{article_id}/wp_post_id')
            ref.update({'en': wp_post_id})
            
            logger.info(f"Published {match_id} in English: WP ID {wp_post_id}")
            
            # Wait before publishing translations
            time.sleep(2)
        else:
            logger.error(f"Failed to publish {match_id} in English")
            return False
    else:
        logger.error(f"English version not available for {match_id}")
        return False
    
    # Then publish translations
    for lang in available_langs:
        if lang == 'en':  # Skip English (already published)
            continue
            
        logger.info(f"Publishing {match_id} in {lang}")
        lang_result = create_wp_post(article, lang)
        
        if lang_result:
            wp_post_id = lang_result.get('id')
            results[lang] = wp_post_id
            
            # Save WordPress ID to Firebase
            ref = db.reference(f'articles/{article_id}/wp_post_id/{lang}')
            ref.set(wp_post_id)
            
            logger.info(f"Published {match_id} in {lang}: WP ID {wp_post_id}")
        else:
            logger.error(f"Failed to publish {match_id} in {lang}")
        
        # Wait between requests
        time.sleep(2)
    
    # Update article status to published
    ref = db.reference(f'articles/{article_id}')
    ref.update({
        'published': True,
        'wp_post_id': results,
        'updated_at': datetime.now().isoformat()
    })
    
    return len(results) > 0

def expire_article(article):
    """Remove expired article from WordPress"""
    article_id = article.get('id', '')
    wp_posts = article.get('wp_post_id', {})
    
    if not wp_posts:
        logger.warning(f"No WordPress IDs found for article {article_id}")
        return False
    
    success = True
    
    # Trash all language versions
    for lang, post_id in wp_posts.items():
        if not post_id:
            continue
            
        logger.info(f"Trashing {article_id} in {lang}: WP ID {post_id}")
        
        if trash_wp_post(post_id):
            logger.info(f"Successfully trashed {article_id} in {lang}")
        else:
            logger.error(f"Failed to trash {article_id} in {lang}")
            success = False
        
        # Wait between requests
        time.sleep(1)
    
    # Update article status in Firebase
    if success:
        ref = db.reference(f'articles/{article_id}')
        ref.update({
            'expired': True,
            'updated_at': datetime.now().isoformat()
        })
    
    return success

def update_component_status(status='success', details=None):
    """Update component status in Firebase"""
    try:
        ref = db.reference('health/publishing')
        update_data = {
            'last_run': datetime.now().isoformat(),
            'status': status
        }
        
        if details:
            update_data['details'] = details
            
        ref.update(update_data)
        logger.info(f"Updated component status: {status}")
    except Exception as e:
        logger.error(f"Error updating component status: {str(e)}")

def main():
    """Main function"""
    start_time = datetime.now()
    logger.info(f"Starting WordPress Publisher at {start_time.isoformat()}")
    
    try:
        # 1. Initialize Firebase
        initialize_firebase()
        
        # Check WordPress credentials
        if not get_wp_url() or not get_wp_auth_header():
            logger.error("WordPress credentials or URL missing")
            update_component_status('error', 'WordPress credentials or URL missing')
            return 1
        
        # 2. Get articles to publish
        articles_to_publish = get_articles_to_publish()
        
        # 3. Get articles to expire
        articles_to_expire = get_articles_to_expire()
        
        # 4. Publish new articles (max 5 per run)
        published_count = 0
        articles_to_process = articles_to_publish[:5]
        
        for article in articles_to_process:
            if publish_article(article):
                published_count += 1
                logger.info(f"Successfully published article {article.get('id', '')}")
            else:
                logger.error(f"Failed to publish article {article.get('id', '')}")
            
            # Wait between publications
            time.sleep(5)
        
        # 5. Expire old articles (max 5 per run)
        expired_count = 0
        articles_to_expire = articles_to_expire[:5]
        
        for article in articles_to_expire:
            if expire_article(article):
                expired_count += 1
                logger.info(f"Successfully expired article {article.get('id', '')}")
            else:
                logger.error(f"Failed to expire article {article.get('id', '')}")
            
            # Wait between operations
            time.sleep(2)
        
        # 6. Update health status
        if published_count > 0 or expired_count > 0:
            update_component_status('success', f"Published {published_count}, expired {expired_count}")
        else:
            update_component_status('success', "No articles to process")
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"WordPress Publisher completed in {duration} seconds")
        return 0
        
    except Exception as e:
        logger.error(f"General error: {str(e)}")
        update_component_status('error', str(e))
        return 1

if __name__ == "__main__":
    sys.exit(main())
