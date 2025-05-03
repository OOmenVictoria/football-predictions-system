""" Utilità per la manipolazione e formattazione del testo.
Fornisce funzioni per pulizia, normalizzazione e formattazione dei testi
utilizzati nel sistema di pronostici calcistici.
"""

import re
import unicodedata
import logging
from typing import Dict, List, Any, Optional, Union
import html
import markdown
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

def clean_text(text: str) -> str:
    """
    Rimuove spazi extra, caratteri speciali e formattazione non necessaria.
    
    Args:
        text: Testo da pulire
        
    Returns:
        Testo pulito
    """
    if not text or not isinstance(text, str):
        return ""
    
    # Rimuove HTML
    text = html.unescape(text)
    text = re.sub(r'<[^>]+>', ' ', text)
    
    # Normalizza spazi
    text = re.sub(r'\s+', ' ', text)
    
    # Rimuove spazi iniziali e finali
    return text.strip()

def normalize_team_name(name: str) -> str:
    """
    Normalizza i nomi delle squadre per facilitare il confronto.
    Rimuove prefissi/suffissi comuni (FC, United, etc) e normalizza caratteri.
    
    Args:
        name: Nome della squadra da normalizzare
        
    Returns:
        Nome normalizzato
    """
    if not name:
        return ""
    
    # Conversione a lowercase
    name = name.lower()
    
    # Rimuove prefissi/suffissi comuni
    prefixes = ['fc ', 'afc ', 'ssc ', 'ac ', 'ss ', 'as ']
    for prefix in prefixes:
        if name.startswith(prefix):
            name = name[len(prefix):]
    
    suffixes = [' fc', ' cf', ' afc', ' united', ' utd']
    for suffix in suffixes:
        if name.endswith(suffix):
            name = name[:-len(suffix)]
    
    # Normalizza accenti e caratteri speciali
    name = ''.join(c for c in unicodedata.normalize('NFKD', name)
                   if not unicodedata.combining(c))
    
    # Rimuove caratteri non alfanumerici
    name = re.sub(r'[^a-z0-9\s]', '', name)
    
    # Normalizza spazi
    name = re.sub(r'\s+', ' ', name)
    
    return name.strip()

def normalize_player_name(name: str) -> str:
    """
    Normalizza i nomi dei giocatori per facilitare il confronto.
    
    Args:
        name: Nome del giocatore da normalizzare
        
    Returns:
        Nome normalizzato
    """
    if not name:
        return ""
    
    # Conversione a lowercase
    name = name.lower()
    
    # Normalizza accenti e caratteri speciali
    name = ''.join(c for c in unicodedata.normalize('NFKD', name)
                   if not unicodedata.combining(c))
    
    # Rimuove caratteri non alfanumerici tranne spazi
    name = re.sub(r'[^a-z\s]', '', name)
    
    # Normalizza spazi
    name = re.sub(r'\s+', ' ', name)
    
    return name.strip()

def format_score(home_score: Union[int, str], away_score: Union[int, str]) -> str:
    """
    Formatta il punteggio di una partita.
    
    Args:
        home_score: Gol della squadra di casa
        away_score: Gol della squadra ospite
        
    Returns:
        Punteggio formattato (es. "2-1")
    """
    try:
        return f"{int(home_score)}-{int(away_score)}"
    except (ValueError, TypeError):
        return "? - ?"

def format_team_form(form_sequence: List[str]) -> str:
    """
    Formatta la sequenza di form di una squadra (W, D, L).
    
    Args:
        form_sequence: Lista di risultati (es. ["W", "L", "D", "W", "W"])
        
    Returns:
        Form formattata (es. "W-W-D-L-W")
    """
    if not form_sequence:
        return "?"
    
    valid_results = []
    for result in form_sequence:
        if isinstance(result, str) and result.upper() in ["W", "D", "L"]:
            valid_results.append(result.upper())
    
    return "-".join(valid_results) if valid_results else "?"

def format_odds(odds: float) -> str:
    """
    Formatta le quote in formato leggibile.
    
    Args:
        odds: Valore della quota
        
    Returns:
        Quota formattata (es. "2.10")
    """
    try:
        odd_value = float(odds)
        return f"{odd_value:.2f}"
    except (ValueError, TypeError):
        return "?.??"

def format_percentage(value: float) -> str:
    """
    Formatta un valore in percentuale.
    
    Args:
        value: Valore da formattare (0-1)
        
    Returns:
        Percentuale formattata (es. "65%")
    """
    try:
        percentage = float(value) * 100
        return f"{percentage:.0f}%"
    except (ValueError, TypeError):
        return "?%"

def truncate_text(text: str, max_length: int = 100, add_ellipsis: bool = True) -> str:
    """
    Tronca un testo alla lunghezza specificata.
    
    Args:
        text: Testo da troncare
        max_length: Lunghezza massima
        add_ellipsis: Aggiunge "..." se il testo viene troncato
        
    Returns:
        Testo troncato
    """
    if not text or len(text) <= max_length:
        return text or ""
    
    truncated = text[:max_length].rsplit(' ', 1)[0]
    if add_ellipsis:
        truncated += "..."
    
    return truncated

def html_to_markdown(html_content: str) -> str:
    """
    Converte HTML in Markdown.
    
    Args:
        html_content: Contenuto HTML
        
    Returns:
        Testo in formato Markdown
    """
    if not html_content:
        return ""
    
    # Pulisce l'HTML con BeautifulSoup
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Rimuove script e stili
    for script in soup(["script", "style"]):
        script.extract()
    
    # Processa i tag HTML comuni
    for a in soup.find_all('a'):
        if a.get('href'):
            a.replace_with(f"[{a.get_text()}]({a.get('href')})")
    
    for img in soup.find_all('img'):
        if img.get('src'):
            alt_text = img.get('alt', 'image')
            img.replace_with(f"![{alt_text}]({img.get('src')})")
    
    for tag in soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
        level = int(tag.name[1])
        tag.replace_with(f"{'#' * level} {tag.get_text()}\n\n")
    
    for ul in soup.find_all('ul'):
        items = []
        for li in ul.find_all('li'):
            items.append(f"* {li.get_text()}")
        ul.replace_with("\n".join(items) + "\n\n")
    
    for ol in soup.find_all('ol'):
        items = []
        for i, li in enumerate(ol.find_all('li')):
            items.append(f"{i+1}. {li.get_text()}")
        ol.replace_with("\n".join(items) + "\n\n")
    
    # Ottieni il testo e normalizza
    text = soup.get_text()
    lines = (line.strip() for line in text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    text = '\n'.join(chunk for chunk in chunks if chunk)
    
    return text

def markdown_to_html(markdown_content: str) -> str:
    """
    Converte Markdown in HTML.
    
    Args:
        markdown_content: Contenuto Markdown
        
    Returns:
        Testo in formato HTML
    """
    if not markdown_content:
        return ""
    
    try:
        # Utilizza il modulo markdown per la conversione
        html_content = markdown.markdown(
            markdown_content,
            extensions=['tables', 'fenced_code', 'codehilite']
        )
        return html_content
    except Exception as e:
        logger.error(f"Errore nella conversione da Markdown a HTML: {e}")
        return markdown_content

def generate_slug(text: str) -> str:
    """
    Genera uno slug SEO-friendly da un testo.
    
    Args:
        text: Testo da convertire in slug
        
    Returns:
        Slug (es. "inter-vs-milan-serie-a")
    """
    if not text:
        return ""
    
    # Normalizza accenti e caratteri speciali
    text = ''.join(c for c in unicodedata.normalize('NFKD', text)
                  if not unicodedata.combining(c))
    
    # Converte in lowercase
    text = text.lower()
    
    # Rimuove caratteri speciali e sostituisce spazi con trattini
    text = re.sub(r'[^a-z0-9\s-]', '', text)
    text = re.sub(r'[\s-]+', '-', text)
    
    # Rimuove trattini all'inizio e alla fine
    return text.strip('-')

# Aggiungiamo un alias per slugify che richiama generate_slug
def slugify(text: str) -> str:
    """
    Alias per generate_slug. Genera uno slug SEO-friendly da un testo.
    
    Args:
        text: Testo da convertire in slug
        
    Returns:
        Slug (es. "inter-vs-milan-serie-a")
    """
    return generate_slug(text)

def extract_keywords(text: str, max_keywords: int = 5) -> List[str]:
    """
    Estrae parole chiave da un testo.
    
    Args:
        text: Testo da cui estrarre le parole chiave
        max_keywords: Numero massimo di parole chiave da estrarre
        
    Returns:
        Lista di parole chiave
    """
    if not text:
        return []
    
    # Rimuove punteggiatura e converte in lowercase
    text = re.sub(r'[^\w\s]', '', text.lower())
    
    # Lista di stopwords in inglese
    stopwords = {
        'a', 'an', 'the', 'and', 'or', 'but', 'if', 'because', 'as', 'what',
        'when', 'where', 'how', 'who', 'which', 'this', 'that', 'these', 'those',
        'then', 'just', 'so', 'than', 'such', 'both', 'through', 'about', 'for',
        'is', 'of', 'while', 'during', 'to', 'from', 'in', 'on', 'by', 'with',
        'at', 'into'
    }
    
    # Suddivide in parole e filtra stopwords
    words = [word for word in text.split() if word not in stopwords and len(word) > 3]
    
    # Conta frequenza delle parole
    word_freq = {}
    for word in words:
        word_freq[word] = word_freq.get(word, 0) + 1
    
    # Ordina per frequenza e prende le top max_keywords
    sorted_words = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)
    return [word for word, freq in sorted_words[:max_keywords]]

def find_team_mentions(text: str, team_names: List[str]) -> Dict[str, int]:
    """
    Trova menzioni delle squadre in un testo.
    
    Args:
        text: Testo in cui cercare
        team_names: Lista di nomi di squadre da cercare
        
    Returns:
        Dizionario con conteggio menzioni per ogni squadra
    """
    if not text or not team_names:
        return {}
    
    text = text.lower()
    mentions = {}
    
    for team in team_names:
        norm_team = normalize_team_name(team)
        count = len(re.findall(r'\b' + re.escape(norm_team) + r'\b', text.lower()))
        if count > 0:
            mentions[team] = count
    
    return mentions
