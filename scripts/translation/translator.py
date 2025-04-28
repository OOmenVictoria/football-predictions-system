#!/usr/bin/env python3
"""
Translator - Processes articles in English and translates them to multiple languages.
Uses free translation services with rotation, fallbacks and caching.
"""
import os
import sys
import json
import time
import logging
import random
import re
import requests
import hashlib
from datetime import datetime
import firebase_admin
from firebase_admin import credentials, db

# Languages to translate to (ISO codes)
TARGET_LANGUAGES = [
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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("translator")

# Translation cache
TRANSLATION_CACHE = {}

class TranslationCache:
    """Class to manage translation cache"""
    def __init__(self):
        self.cache = TRANSLATION_CACHE
    
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
            "https://translate.terraprint.co",
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
            
            # Add API key if available
            api_key = os.getenv('LIBRE_TRANSLATE_API_KEY')
            if api_key:
                data["api_key"] = api_key
            
            response = requests.post(url, json=data, headers=headers, timeout=30)
            
            if response.status_code == 200:
                result = response.json()
                return result.get("translatedText")
            else:
                logger.warning(f"LibreTranslate error: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            logger.error(f"LibreTranslate error: {str(e)}")
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
                logger.warning(f"Lingva error: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            logger.error(f"Lingva error: {str(e)}")
            return None

class GoogleTranslateFreeService(TranslationService):
    """Free Google Translate service (unofficial)"""
    def translate(self, text, source_lang, target_lang):
        """Use a scrappy free alternative - web-based translation"""
        try:
            url = "https://translate.googleapis.com/translate_a/single"
            
            params = {
                "client": "gtx",
                "sl": source_lang,
                "tl": target_lang,
                "dt": "t",
                "q": text
            }
            
            headers = {
                "User-Agent": get_random_user_agent()
            }
            
            response = requests.get(url, params=params, headers=headers, timeout=30)
            
            if response.status_code == 200:
                try:
                    result = response.json()
                    translated_text = ""
                    
                    # Extract translation from the nested JSON structure
                    for part in result[0]:
                        if part[0]:
                            translated_text += part[0]
                    
                    return translated_text
                except Exception as e:
                    logger.error(f"Error parsing free translation response: {str(e)}")
                    return None
            else:
                logger.warning(f"Free translation API error: {response.status_code}")
                return None
        except Exception as e:
            logger.error(f"Error using free translation: {str(e)}")
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

def get_articles_to_translate():
    """Get articles that need translation from Firebase"""
    logger.info("Getting articles to translate...")
    
    try:
        articles_ref = db.reference('articles')
        
        # Query for articles that are in English (source) and need translation
        articles = articles_ref.get()
        
        if not articles:
            logger.warning("No articles found in the database")
            return []
        
        articles_to_translate = []
        
        for article_id, article_data in articles.items():
            # Skip non-English articles
            if article_data.get('language', 'en') != 'en':
                continue
                
            # Check if article already has all translations
            translations = article_data.get('translations', {})
            languages = article_data.get('languages', {})
            
            # Check both translations and languages keys for backward compatibility
            missing_languages = []
            for lang in TARGET_LANGUAGES:
                # Skip English
                if lang == 'en':
                    continue
                    
                # Check if translation is missing in either structure
                translation_missing = lang not in translations or not translations[lang].get('content')
                language_missing = lang not in languages or languages[lang].get('status') != 'completed'
                
                if translation_missing and language_missing:
                    missing_languages.append(lang)
            
            if missing_languages:
                # Add article data to list with id and missing languages
                article_data['id'] = article_id
                article_data['missing_languages'] = missing_languages
                articles_to_translate.append(article_data)
        
        logger.info(f"Found {len(articles_to_translate)} articles to translate")
        return articles_to_translate
    
    except Exception as e:
        logger.error(f"Error getting articles to translate: {str(e)}")
        return []

def translate_text(text, source_lang="en", target_lang="es"):
    """Translate text using multiple services with fallback and caching"""
    # Initialize services and cache
    cache = TranslationCache()
    
    # Check cache first
    cached_translation = cache.get(text, source_lang, target_lang)
    if cached_translation:
        logger.info(f"Using cached translation ({len(text)} chars)")
        return cached_translation
    
    # Initialize translation services
    services = [
        LibreTranslateService(),
        LingvaTranslateService(),
        GoogleTranslateFreeService()
    ]
    
    # Try each service until successful
    for service in services:
        try:
            service_name = service.__class__.__name__
            translated = service.translate(text, source_lang, target_lang)
            
            if translated:
                logger.info(f"Translation successful using {service_name}")
                # Save to cache
                cache.set(text, source_lang, target_lang, translated)
                return translated
        except Exception as e:
            logger.error(f"Error with {service.__class__.__name__}: {str(e)}")
            continue
    
    # If all methods fail
    logger.error(f"All translation methods failed for language: {target_lang}")
    return None

def translate_chunks(text, max_chunk_size=1000, source_lang="en", target_lang="es"):
    """Split text into chunks and translate each chunk"""
    # Split text into paragraphs
    paragraphs = text.split('\n\n')
    
    # Translate each paragraph
    translated_paragraphs = []
    
    for paragraph in paragraphs:
        # Skip empty paragraphs
        if not paragraph.strip():
            translated_paragraphs.append('')
            continue
            
        # For long paragraphs, split further
        if len(paragraph) > max_chunk_size:
            # Split by sentences
            sentences = re.split(r'(?<=[.!?])\s+', paragraph)
            chunk = ""
            chunks = []
            
            for sentence in sentences:
                if len(chunk) + len(sentence) > max_chunk_size:
                    if chunk:
                        chunks.append(chunk)
                    chunk = sentence
                else:
                    if chunk:
                        chunk += " " + sentence
                    else:
                        chunk = sentence
            
            if chunk:
                chunks.append(chunk)
                
            # Translate each chunk
            translated_chunks = []
            for chunk in chunks:
                # Add some delay between chunks to avoid rate limits
                time.sleep(random.uniform(1, 2))
                translated = translate_text(chunk, source_lang, target_lang)
                if translated:
                    translated_chunks.append(translated)
                else:
                    # If translation fails, use original
                    translated_chunks.append(chunk)
            
            translated_paragraph = " ".join(translated_chunks)
        else:
            # For short paragraphs, translate directly
            translated_paragraph = translate_text(paragraph, source_lang, target_lang) or paragraph
        
        translated_paragraphs.append(translated_paragraph)
        # Add delay between paragraphs
        time.sleep(random.uniform(1, 3))
    
    # Join translated paragraphs
    return '\n\n'.join(translated_paragraphs)

def translate_article(article, lang):
    """Translate an article to a target language"""
    try:
        logger.info(f"Translating article {article['id']} to {lang}")
        
        # Update status to "in progress"
        try:
            status_ref = db.reference(f'articles/{article["id"]}/languages/{lang}')
            status_ref.set({
                'status': 'translating',
                'started_at': datetime.now().isoformat()
            })
        except Exception as e:
            logger.warning(f"Could not update translation status: {str(e)}")
        
        # Get original content
        original_content = article.get('content', '')
        
        if not original_content:
            logger.warning(f"Article {article['id']} has no content to translate")
            return None
        
        # Translate title (limited to 200 chars to avoid rate limits)
        original_title = article.get('title', '')
        if len(original_title) > 200:
            original_title = original_title[:197] + "..."
        
        translated_title = translate_text(original_title, "en", lang)
        
        # Translate content in chunks to avoid length limits
        translated_content = translate_chunks(original_content, 1000, "en", lang)
        
        # Create translation object
        translation = {
            'title': translated_title or original_title,
            'content': translated_content or original_content,
            'translated_at': datetime.now().isoformat(),
            'language': lang,
            'status': 'completed'
        }
        
        # Save translations in both formats for compatibility
        # Format 1: languages/{lang}
        lang_ref = db.reference(f'articles/{article["id"]}/languages/{lang}')
        lang_ref.set(translation)
        
        # Format 2: translations/{lang}
        trans_ref = db.reference(f'articles/{article["id"]}/translations/{lang}')
        trans_ref.set(translation)
        
        logger.info(f"Saved translation for article {article['id']} to {lang}")
        return translation
    except Exception as e:
        logger.error(f"Error translating article {article['id']} to {lang}: {str(e)}")
        # Update status to failed
        try:
            status_ref = db.reference(f'articles/{article["id"]}/languages/{lang}')
            status_ref.update({
                'status': 'failed',
                'error': str(e),
                'failed_at': datetime.now().isoformat()
            })
        except Exception as inner_e:
            logger.error(f"Could not update failure status: {str(inner_e)}")
        return None

def update_component_status(status='success', details=None):
    """Update component status in Firebase"""
    try:
        ref = db.reference('health/translation')
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
    try:
        logger.info(f"Starting translation process at {start_time.isoformat()}")
        
        # Initialize Firebase
        initialize_firebase()
        
        # Get articles that need translation
        articles = get_articles_to_translate()
        
        if not articles:
            logger.warning("No articles to translate")
            update_component_status('success', 'No articles to translate')
            return 0
        
        # Process at most 5 articles per run to avoid rate limits
        articles_to_process = articles[:5]
        
        success_count = 0
        failure_count = 0
        
        for article in articles_to_process:
            # Process up to 3 languages per article
            languages_to_process = article.get('missing_languages', [])[:3]
            
            for lang in languages_to_process:
                try:
                    # Skip English (original)
                    if lang == 'en':
                        continue
                        
                    # Translate article
                    translation = translate_article(article, lang)
                    
                    if translation:
                        success_count += 1
                    else:
                        failure_count += 1
                        
                    # Add a small delay to avoid rate limits
                    time.sleep(random.uniform(1, 3))
                except Exception as e:
                    logger.error(f"Error processing language {lang} for article {article['id']}: {str(e)}")
                    failure_count += 1
        
        # Update component status
        if failure_count == 0:
            update_component_status('success', f"Translated {success_count} articles/languages")
        elif success_count > 0:
            update_component_status('warning', f"Completed {success_count} translations with {failure_count} failures")
        else:
            update_component_status('error', f"All {failure_count} translations failed")
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"Translation process completed in {duration} seconds. Success: {success_count}, Failures: {failure_count}")
        
        if failure_count > 0 and success_count == 0:
            return 1
        return 0
    
    except Exception as e:
        logger.error(f"Error in translation process: {str(e)}")
        update_component_status('error', str(e))
        return 1

if __name__ == "__main__":
    sys.exit(main())
