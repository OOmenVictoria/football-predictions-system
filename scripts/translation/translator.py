#!/usr/bin/env python3
"""
Translate Articles - Translates articles into configured languages
Uses free translation services with fallback and caching
"""
import os
import sys
import logging
import time
import random
import json
import hashlib
import requests
from datetime import datetime
import firebase_admin
from firebase_admin import credentials, db
from dotenv import load_dotenv

# Logging configuration
log_dir = os.path.expanduser('~/football-predictions/logs')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"translate_articles_{datetime.now().strftime('%Y%m%d')}.log")

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Global variables
load_dotenv()
LANGUAGES = os.getenv('LANGUAGES', 'en,it,es,fr,de,pt,ar,ru,ja,pl,zh,tr,nl,sv,el').split(',')

class TranslationCache:
    """Class to manage translation cache"""
    def __init__(self):
        self.cache = {}
    
    def get_key(self, text, source_lang, target_lang):
        """Generate cache key based on text and languages"""
        # Use MD5 for fixed-length key
        text_hash = hashlib.md5(text.encode('utf-8')).hexdigest()
        return f"{source_lang}_{target_lang}_{text_hash}"
    
    def get(self, text, source_lang, target_lang):
        """Get translation from cache if available"""
        key = self.get_key(text, source_lang, target_lang)
        return self.cache.get(key)
    
    def set(self, text, source_lang, target_lang, translation):
        """Save translation in cache"""
        key = self.get_key(text, source_lang, target_lang)
        self.cache[key] = translation

class TranslationService:
    """Base class for translation services"""
    def translate(self, text, source_lang, target_lang):
        """Abstract method to be implemented in specific services"""
        raise NotImplementedError()

class LibreTranslateService(TranslationService):
    """LibreTranslate service"""
    def __init__(self):
        self.endpoints = [
            "https://libretranslate.de",
            "https://translate.argosopentech.com",
            "https://libretranslate.com"
        ]
    
    def translate(self, text, source_lang, target_lang):
        """Translate text using LibreTranslate"""
        # Select random endpoint
        endpoint = random.choice(self.endpoints)
        url = f"{endpoint}/translate"
        
        try:
            data = {
                "q": text,
                "source": source_lang,
                "target": target_lang,
                "format": "text"
            }
            
            headers = {
                "Content-Type": "application/json",
                "User-Agent": get_random_user_agent()
            }
            
            response = requests.post(url, json=data, headers=headers, timeout=30)
            
            if response.status_code == 200:
                result = response.json()
                return result.get("translatedText")
            else:
                logging.error(f"LibreTranslate error: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            logging.error(f"LibreTranslate error: {str(e)}")
            return None

class LingvaTranslateService(TranslationService):
    """Lingva Translate service"""
    def __init__(self):
        self.endpoints = [
            "https://lingva.ml",
            "https://lingva.pussthecat.org",
            "https://lingva.garudalinux.org"
        ]
    
    def translate(self, text, source_lang, target_lang):
        """Translate text using Lingva Translate"""
        # Select random endpoint
        endpoint = random.choice(self.endpoints)
        url = f"{endpoint}/api/v1/{source_lang}/{target_lang}/{text}"
        
        try:
            headers = {
                "User-Agent": get_random_user_agent()
            }
            
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                result = response.json()
                return result.get("translation")
            else:
                logging.error(f"Lingva error: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            logging.error(f"Lingva error: {str(e)}")
            return None

class MultiTranslator:
    """Manages multiple translation services with fallback"""
    def __init__(self):
        self.services = [
            LibreTranslateService(),
            LingvaTranslateService()
        ]
        self.cache = TranslationCache()
    
    def translate(self, text, source_lang, target_lang):
        """Translate text using the first available service"""
        # Check cache
        cached = self.cache.get(text, source_lang, target_lang)
        if cached:
            logging.info(f"Using translation from cache ({len(text)} characters)")
            return cached
        
        # Try each service
        for service in self.services:
            try:
                translated = service.translate(text, source_lang, target_lang)
                if translated:
                    # Save in cache
                    self.cache.set(text, source_lang, target_lang, translated)
                    return translated
            except Exception as e:
                logging.error(f"Translation service error: {str(e)}")
                continue
        
        return None

def get_random_user_agent():
    """Return a random User-Agent to avoid blocks"""
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1"
    ]
    return random.choice(user_agents)

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

def get_articles_to_translate():
    """Get articles that need translation"""
    articles_to_translate = []
    
    # Articles reference
    articles_ref = db.reference('articles')
    articles = articles_ref.get() or {}
    
    for article_id, article in articles.items():
        # Check if there are languages to translate
        languages = article.get('languages', {})
        
        for lang in LANGUAGES:
            # Skip English (original)
            if lang == 'en':
                continue
            
            # If language not yet translated
            if lang not in languages or languages[lang].get('status') != 'completed':
                articles_to_translate.append({
                    'id': article_id,
                    'article': article,
                    'target_lang': lang
                })
    
    logging.info(f"Found {len(articles_to_translate)} articles/languages to translate")
    return articles_to_translate

def translate_article(article_data, translator):
    """Translate an article to a specific language"""
    article = article_data['article']
    target_lang = article_data['target_lang']
    
    # Title
    title_orig = article['title']
    title_trans = translator.translate(title_orig, 'en', target_lang)
    
    if not title_trans:
        logging.error(f"Error translating title to {target_lang}")
        return None
    
    # Split content into blocks for efficient translation
    content_orig = article['content']
    
    # Split by paragraphs (for more efficient translations)
    paragraphs = content_orig.split('\n\n')
    translated_paragraphs = []
    
    for para in paragraphs:
        # Skip empty paragraphs
        if not para.strip():
            translated_paragraphs.append('')
            continue
        
        # Translate paragraph
        trans_para = translator.translate(para, 'en', target_lang)
        
        if not trans_para:
            logging.error(f"Error translating paragraph to {target_lang}")
            trans_para = para  # Use original in case of error
        
        translated_paragraphs.append(trans_para)
        
        # Pause between translations to respect API limits
        time.sleep(1)
    
    # Reassemble content
    content_trans = '\n\n'.join(translated_paragraphs)
    
    # Create translated version
    translated = {
        'title': title_trans,
        'content': content_trans,
        'language': target_lang,
        'status': 'completed',
        'created_at': datetime.now().isoformat(),
        'source_language': 'en'
    }
    
    return translated

def save_translation(article_id, lang, translation):
    """Save translation to Firebase"""
    ref = db.reference(f'articles/{article_id}/languages/{lang}')
    ref.set(translation)
    
    # Update timestamp
    update_ref = db.reference(f'articles/{article_id}')
    update_ref.update({
        'updated_at': datetime.now().isoformat()
    })
    
    return True

def main():
    """Main function"""
    start_time = datetime.now()
    logging.info(f"Starting Translation Service - {start_time.isoformat()}")
    
    try:
        # 1. Initialize Firebase
        initialize_firebase()
        
        # 2. Create translator
        translator = MultiTranslator()
        
        # 3. Get articles to translate
        all_to_translate = get_articles_to_translate()
        
        # 4. Limit per execution (max 10 translations per run)
        to_translate = all_to_translate[:10]
        
        # 5. Translate and save
        translated_count = 0
        for item in to_translate:
            article_id = item['id']
            target_lang = item['target_lang']
            
            logging.info(f"Translating article {article_id} to {target_lang}")
            
            # Update status to "in progress"
            ref = db.reference(f'articles/{article_id}/languages/{target_lang}')
            ref.set({
                'status': 'translating',
                'started_at': datetime.now().isoformat()
            })
            
            # Translate
            translation = translate_article(item, translator)
            
            if translation:
                # Save translation
                save_translation(article_id, target_lang, translation)
                translated_count += 1
                logging.info(f"Translation completed for {article_id} to {target_lang}")
            else:
                # Update status to "failed"
                ref.update({
                    'status': 'failed',
                    'failed_at': datetime.now().isoformat()
                })
                logging.error(f"Translation failed for {article_id} to {target_lang}")
            
            # Pause between articles
            time.sleep(2)
        
        # 6. Update health status
        health_ref = db.reference('health/translate_articles')
        health_ref.set({
            'last_run': datetime.now().isoformat(),
            'articles_translated': translated_count,
            'pending_translations': len(all_to_translate) - translated_count,
            'status': 'success'
        })
        
    except Exception as e:
        logging.error(f"General error: {str(e)}")
        
        # Update health status with error
        try:
            health_ref = db.reference('health/translate_articles')
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
    logging.info(f"Translation Service completed in {duration} seconds")
    return 0

if __name__ == "__main__":
    sys.exit(main())
