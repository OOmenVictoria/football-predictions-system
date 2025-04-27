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
from dotenv import load_dotenv

# Logging configuration
log_dir = os.path.expanduser('~/football-predictions/logs')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"publish_wordpress_{datetime.now().strftime('%Y%m%d')}.log")

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Global variables
load_dotenv()
WP_URL = os.getenv('WP_URL')
WP_USER = os.getenv('WP_USER')
WP_APP_PASSWORD = os.getenv('WP_APP_PASSWORD')
LANGUAGES = os.getenv('LANGUAGES', 'en,it,es,fr,de,pt,ar,ru,ja,pl,zh,tr,nl,sv,el').split(',')

def initialize_firebase():
    """Initialize Firebase connection"""
    try:
        firebase_admin.get_app()
    except ValueError:
        cred_path = os.path.expanduser('~/football-predictions/creds/firebase-credentials.json')
        cred = credentials.Certificate(cred_path)
        firebase_admin.initialize_app(cred, {
            'databaseURL': os.getenv('FIREBASE_DB_URL')
        })
    return True

def get_wp_auth_header():
    """Generate WordPress authentication header"""
    if not WP_USER or not WP_APP_PASSWORD:
        logging.error("Missing WordPress credentials")
        return None
    
    auth_string = f"{WP_USER}:{WP_APP_PASSWORD}"
    auth_base64 = base64.b64encode(auth_string.encode()).decode()
    return {'Authorization': f"Basic {auth_base64}"}

def get_articles_to_publish():
    """Get articles ready for publication"""
    articles_to_publish = []
    
    # Articles reference
    articles_ref = db.reference('articles')
    articles = articles_ref.get() or {}
    
    now = datetime.now()
    
    for article_id, article in articles.items():
        # Skip if already published
        if article.get('published', False):
            continue
        
        # Check if it's time to publish (based on publish_time)
        publish_time = datetime.fromisoformat(article['publish_time'].replace('Z', '+00:00'))
        
        if now >= publish_time:
            # Check that all required translations are complete (or at least English)
            languages = article.get('languages', {})
            english_ready = 'en' in languages and languages['en'].get('status') == 'completed'
            
            if english_ready:
                articles_to_publish.append(article)
    
    logging.info(f"Found {len(articles_to_publish)} articles to publish")
    return articles_to_publish

def get_articles_to_expire():
    """Get articles to remove (expired)"""
    articles_to_expire = []
    
    # Articles reference
    articles_ref = db.reference('articles')
    articles = articles_ref.get() or {}
    
    now = datetime.now()
    
    for article_id, article in articles.items():
        # Skip if not published
        if not article.get('published', False):
            continue
        
        # Check if expired (based on expire_time)
        expire_time = datetime.fromisoformat(article['expire_time'].replace('Z', '+00:00'))
        
        if now >= expire_time:
            articles_to_expire.append({
                'id': article_id,
                'wp_post_id': article.get('wp_post_id', {}),
                'title': article['title']
            })
    
    logging.info(f"Found {len(articles_to_expire)} articles to remove")
    return articles_to_expire

def create_wp_post(article, lang='en'):
    """Create post on WordPress"""
    if not WP_URL:
        logging.error("Missing WordPress URL")
        return None
    
    # Get content in the specified language
    languages = article.get('languages', {})
    
    if lang not in languages:
        logging.error(f"Language {lang} not available for article {article['match_id']}")
        return None
    
    lang_data = languages[lang]
    title = lang_data.get('title') if lang != 'en' else article['title']
    content = lang_data.get('content') if lang != 'en' else article['content']
    
    # Prepare team logos
    home_team = article['home_team'].replace(' ', '-').lower()
    away_team = article['away_team'].replace(' ', '-').lower()
    
    # Add shortcode for team logos
    if '[match_logos]' not in content:
        content = f'[match_logos home="{home_team}" away="{away_team}"]\n\n' + content
    
    # Prepare post data
    post_data = {
        'title': title,
        'content': content,
        'status': 'publish',
        'categories': [get_category_id('Football Predictions')],  # Default category
        'meta': {
            'match_id': article['match_id'],
            'home_team': article['home_team'],
            'away_team': article['away_team'],
            'competition': article['competition'],
            'match_time': article['match_time'],
            'expire_time': article['expire_time'],
            'article_language': lang
        }
    }
    
    # If it's a translation, add Polylang relation
    if lang != 'en' and 'wp_post_id' in article and 'en' in article['wp_post_id']:
        post_data['meta']['_pll_translation_of'] = article['wp_post_id']['en']
    
    # Send request to WordPress
    headers = get_wp_auth_header()
    if not headers:
        return None
    
    try:
        response = requests.post(
            f"{WP_URL}/posts",
            json=post_data,
            headers=headers
        )
        
        if response.status_code in [200, 201]:
            return response.json()
        else:
            logging.error(f"Publication error: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        logging.error(f"WordPress request error: {str(e)}")
        return None

def trash_wp_post(post_id):
    """Move post to trash"""
    if not WP_URL or not post_id:
        return False
    
    headers = get_wp_auth_header()
    if not headers:
        return False
    
    try:
        # Update status to 'trash'
        response = requests.put(
            f"{WP_URL}/posts/{post_id}",
            json={'status': 'trash'},
            headers=headers
        )
        
        return response.status_code in [200, 201]
    except Exception as e:
        logging.error(f"Post deletion error: {str(e)}")
        return False

def get_category_id(category_name):
    """Get category ID from name (or create if it doesn't exist)"""
    if not WP_URL:
        return None
    
    headers = get_wp_auth_header()
    if not headers:
        return None
    
    try:
        # Search for category
        response = requests.get(
            f"{WP_URL}/categories",
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
                    f"{WP_URL}/categories",
                    json={'name': category_name},
                    headers=headers
                )
                
                if create_response.status_code in [200, 201]:
                    return create_response.json().get('id')
        
        # Default category if all else fails
        return 1
    except Exception as e:
        logging.error(f"Error getting category: {str(e)}")
        return 1

def publish_article(article):
    """Publish article in all available languages"""
    results = {}
    match_id = article['match_id']
    
    # Get available languages
    languages = article.get('languages', {})
    available_langs = [lang for lang in LANGUAGES if lang in languages and languages[lang].get('status') == 'completed']
    
    # First publish in English (primary language)
    if 'en' in available_langs:
        logging.info(f"Publishing {match_id} in English")
        en_result = create_wp_post(article, 'en')
        
        if en_result:
            wp_post_id = en_result.get('id')
            results['en'] = wp_post_id
            
            # Save WordPress ID to Firebase
            ref = db.reference(f'articles/{match_id}/wp_post_id')
            ref.update({'en': wp_post_id})
            
            logging.info(f"Published {match_id} in English: WP ID {wp_post_id}")
            
            # Wait before publishing translations
            time.sleep(2)
    else:
        logging.error(f"English version not available for {match_id}")
        return False
    
    # Then publish translations
    for lang in available_langs:
        if lang == 'en':  # Skip English (already published)
            continue
            
        logging.info(f"Publishing {match_id} in {lang}")
        lang_result = create_wp_post(article, lang)
        
        if lang_result:
            wp_post_id = lang_result.get('id')
            results[lang] = wp_post_id
            
            # Save WordPress ID to Firebase
            ref = db.reference(f'articles/{match_id}/wp_post_id/{lang}')
            ref.set(wp_post_id)
            
            logging.info(f"Published {match_id} in {lang}: WP ID {wp_post_id}")
        else:
            logging.error(f"Failed to publish {match_id} in {lang}")
        
        # Wait between requests
        time.sleep(2)
    
    # Update article status to published
    ref = db.reference(f'articles/{match_id}')
    ref.update({
        'published': True,
        'wp_post_id': results,
        'updated_at': datetime.now().isoformat()
    })
    
    return len(results) > 0

def expire_article(article):
    """Remove expired article from WordPress"""
    article_id = article['id']
    wp_posts = article.get('wp_post_id', {})
    
    if not wp_posts:
        logging.warning(f"No WordPress IDs found for article {article_id}")
        return False
    
    success = True
    
    # Trash all language versions
    for lang, post_id in wp_posts.items():
        if not post_id:
            continue
            
        logging.info(f"Trashing {article_id} in {lang}: WP ID {post_id}")
        
        if trash_wp_post(post_id):
            logging.info(f"Successfully trashed {article_id} in {lang}")
        else:
            logging.error(f"Failed to trash {article_id} in {lang}")
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

def main():
    """Main function"""
    start_time = datetime.now()
    logging.info(f"Starting WordPress Publisher - {start_time.isoformat()}")
    
    try:
        # 1. Initialize Firebase
        initialize_firebase()
        
        # 2. Get articles to publish
        articles_to_publish = get_articles_to_publish()
        
        # 3. Get articles to expire
        articles_to_expire = get_articles_to_expire()
        
        # 4. Publish new articles
        published_count = 0
        for article in articles_to_publish:
            if publish_article(article):
                published_count += 1
                logging.info(f"Successfully published article {article['match_id']}")
            else:
                logging.error(f"Failed to publish article {article['match_id']}")
            
            # Wait between publications
            time.sleep(5)
        
        # 5. Expire old articles
        expired_count = 0
        for article in articles_to_expire:
            if expire_article(article):
                expired_count += 1
                logging.info(f"Successfully expired article {article['id']}")
            else:
                logging.error(f"Failed to expire article {article['id']}")
            
            # Wait between operations
            time.sleep(2)
        
        # 6. Update health status
        health_ref = db.reference('health/publish_wordpress')
        health_ref.set({
            'last_run': datetime.now().isoformat(),
            'articles_published': published_count,
            'articles_expired': expired_count,
            'status': 'success'
        })
        
    except Exception as e:
        logging.error(f"General error: {str(e)}")
        
        # Update health status with error
        try:
            health_ref = db.reference('health/publish_wordpress')
            health_ref.set({
                'last_run': datetime.now().isoformat(),
                'status': 'error',
                'error_message': str(e)
            })
        except:
            pass
            
        return 1
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    logging.info(f"WordPress Publisher completed in {duration} seconds")
    return 0

if __name__ == "__main__":
    sys.exit(main())
