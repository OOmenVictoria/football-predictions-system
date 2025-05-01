"""
Formattatore per contenuti in HTML.
Questo modulo contiene funzioni per convertire e formattare
testo in HTML per la pubblicazione sul web.
"""
import logging
import re
import markdown
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

def format_as_html(content: str, add_styles: bool = True) -> str:
    """
    Converte il contenuto da Markdown a HTML.
    
    Args:
        content: Contenuto Markdown da convertire
        add_styles: Se aggiungere stili CSS inline
        
    Returns:
        Contenuto formattato in HTML
    """
    logger.debug("Formattando contenuto in HTML")
    
    try:
        # Usa la libreria Python Markdown per la conversione
        html_content = markdown.markdown(
            content,
            extensions=[
                'markdown.extensions.tables',
                'markdown.extensions.fenced_code',
                'markdown.extensions.codehilite',
                'markdown.extensions.nl2br',
                'markdown.extensions.sane_lists'
            ]
        )
        
        # Aggiungi stili CSS di base se richiesto
        if add_styles:
            html_content = _add_default_styles(html_content)
        
        # Ottimizza la formattazione HTML
        html_content = _optimize_html_formatting(html_content)
        
        # Migliora le tabelle
        html_content = _enhance_tables(html_content)
        
        # Migliora i link esterni
        html_content = _enhance_external_links(html_content)
        
        # Aggiungi attributi alt alle immagini che non li hanno
        html_content = _add_missing_alt_attributes(html_content)
        
        return html_content
    
    except Exception as e:
        logger.error(f"Errore durante la formattazione HTML: {e}")
        
        # In caso di errore, fai una conversione di base
        return f"<div>{content.replace('\\n', '<br>')}</div>"

def _add_default_styles(html_content: str) -> str:
    """
    Aggiunge stili CSS di base al contenuto HTML.
    
    Args:
        html_content: Contenuto HTML
        
    Returns:
        Contenuto HTML con stili CSS
    """
    # Stili CSS di base
    css = """
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 800px;
            margin: 0 auto;
            padding: 20px;
        }
        h1, h2, h3, h4, h5, h6 {
            margin-top: 1.5em;
            margin-bottom: 0.5em;
            font-weight: 600;
            color: #222;
        }
        h1 { font-size: 2em; border-bottom: 1px solid #eee; padding-bottom: 0.3em; }
        h2 { font-size: 1.6em; border-bottom: 1px solid #eee; padding-bottom: 0.3em; }
        h3 { font-size: 1.3em; }
        h4 { font-size: 1.1em; }
        p, ul, ol, table { margin-bottom: 1em; }
        ul, ol { padding-left: 2em; }
        a { color: #0366d6; text-decoration: none; }
        a:hover { text-decoration: underline; }
        code {
            font-family: Consolas, Monaco, 'Andale Mono', monospace;
            background-color: #f6f8fa;
            padding: 0.2em 0.4em;
            border-radius: 3px;
            font-size: 0.9em;
        }
        pre {
            background-color: #f6f8fa;
            border-radius: 3px;
            padding: 16px;
            overflow: auto;
            line-height: 1.45;
        }
        pre code {
            background-color: transparent;
            padding: 0;
            font-size: 0.9em;
        }
        table {
            border-collapse: collapse;
            width: 100%;
            margin-bottom: 1em;
        }
        table th, table td {
            padding: 8px;
            border: 1px solid #dfe2e5;
            text-align: left;
        }
        table th {
            background-color: #f6f8fa;
            font-weight: 600;
        }
        table tr:nth-child(even) {
            background-color: #f8f8f8;
        }
        img {
            max-width: 100%;
            height: auto;
        }
        blockquote {
            margin: 0;
            padding: 0 1em;
            color: #6a737d;
            border-left: 0.25em solid #dfe2e5;
        }
        .match-header {
            display: flex;
            align-items: center;
            justify-content: space-between;
            margin-bottom: 20px;
        }
        .match-date {
            font-style: italic;
            color: #666;
        }
        .team-logo {
            max-height: 50px;
            width: auto;
        }
        .prediction-box {
            background-color: #f0f8ff;
            border-left: 4px solid #0366d6;
            padding: 15px;
            margin-bottom: 20px;
            border-radius: 3px;
        }
        .value-bet {
            background-color: #f0fff0;
            border-left: 4px solid #28a745;
            padding: 15px;
            margin-bottom: 20px;
            border-radius: 3px;
        }
        .stats-comparison {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
            margin-bottom: 20px;
        }
        .team-stats {
            background-color: #f8f9fa;
            padding: 15px;
            border-radius: 3px;
        }
        .disclaimer {
            font-size: 0.9em;
            font-style: italic;
            color: #666;
            padding: 10px;
            background-color: #f8f9fa;
            border-radius: 3px;
            margin-top: 20px;
        }
    </style>
    """
    
    # Verifica se esiste già un tag style
    if "<style>" in html_content:
        return html_content
    
    # Aggiungi gli stili all'inizio del documento
    return css + html_content

def _optimize_html_formatting(html_content: str) -> str:
    """
    Ottimizza la formattazione HTML per una migliore leggibilità.
    
    Args:
        html_content: Contenuto HTML
        
    Returns:
        Contenuto HTML ottimizzato
    """
    # Rimuovi righe vuote multiple
    html_content = re.sub(r'\n{3,}', '\n\n', html_content)
    
    # Aggiungi classi a elementi specifici per migliorare lo stile
    
    # 1. Aggiungi la classe 'prediction-box' ai div che contengono previsioni
    prediction_patterns = [
        r'<h3[^>]*>.*?Top Prediction.*?</h3>',
        r'<h3[^>]*>.*?Il Nostro Pronostico.*?</h3>'
    ]
    
    for pattern in prediction_patterns:
        # Trova la sezione di previsione
        match = re.search(pattern, html_content)
        if match:
            # Trova l'inizio del paragrafo che segue
            start_pos = match.end()
            # Inserisci un div con la classe prediction-box
            html_before = html_content[:start_pos]
            html_after = html_content[start_pos:]
            html_content = f"{html_before}<div class='prediction-box'>{html_after}"
            
            # Trova dove chiudere il div
            end_patterns = [
                r'<h3[^>]*>',
                r'<h2[^>]*>'
            ]
            
            for end_pattern in end_patterns:
                end_match = re.search(end_pattern, html_content[start_pos:])
                if end_match:
                    end_pos = start_pos + end_match.start()
                    html_content = f"{html_content[:end_pos]}</div>{html_content[end_pos:]}"
                    break
    
    # 2. Aggiungi la classe 'team-stats' ai div che contengono statistiche
    stats_patterns = [
        r'<h3[^>]*>.*?Team Performance.*?</h3>',
        r'<h3[^>]*>.*?Confronto Prestazioni.*?</h3>'
    ]
    
    for pattern in stats_patterns:
        # Procedimento simile a quello per prediction-box
        match = re.search(pattern, html_content)
        if match:
            start_pos = match.end()
            html_content = f"{html_content[:start_pos]}<div class='team-stats'>{html_content[start_pos:]}"
            
            end_patterns = [
                r'<h3[^>]*>',
                r'<h2[^>]*>'
            ]
            
            for end_pattern in end_patterns:
                end_match = re.search(end_pattern, html_content[start_pos:])
                if end_match:
                    end_pos = start_pos + end_match.start()
                    html_content = f"{html_content[:end_pos]}</div>{html_content[end_pos:]}"
                    break
    
    # 3. Aggiungi la classe 'value-bet' ai div che contengono value bet
    value_patterns = [
        r'<h3[^>]*>.*?Value Bet.*?</h3>'
    ]
    
    for pattern in value_patterns:
        match = re.search(pattern, html_content)
        if match:
            start_pos = match.end()
            html_content = f"{html_content[:start_pos]}<div class='value-bet'>{html_content[start_pos:]}"
            
            end_patterns = [
                r'<h3[^>]*>',
                r'<h2[^>]*>'
            ]
            
            for end_pattern in end_patterns:
                end_match = re.search(end_pattern, html_content[start_pos:])
                if end_match:
                    end_pos = start_pos + end_match.start()
                    html_content = f"{html_content[:end_pos]}</div>{html_content[end_pos:]}"
                    break
    
    # 4. Aggiungi la classe 'disclaimer' al div del disclaimer
    disclaimer_patterns = [
        r'<h3[^>]*>.*?Betting Advice.*?</h3>',
        r'<h3[^>]*>.*?Consigli per le Scommesse.*?</h3>'
    ]
    
    for pattern in disclaimer_patterns:
        match = re.search(pattern, html_content)
        if match:
            start_pos = match.end()
            html_content = f"{html_content[:start_pos]}<div class='disclaimer'>{html_content[start_pos:]}"
            
            end_patterns = [
                r'<h[23][^>]*>',
                r'</body>',
                r'$'
            ]
            
            end_found = False
            for end_pattern in end_patterns:
                end_match = re.search(end_pattern, html_content[start_pos:])
                if end_match:
                    end_pos = start_pos + end_match.start()
                    html_content = f"{html_content[:end_pos]}</div>{html_content[end_pos:]}"
                    end_found = True
                    break
            
            if not end_found:
                # Se non troviamo una fine esplicita, chiudi il div alla fine
                html_content = f"{html_content}</div>"
    
    return html_content

def _enhance_tables(html_content: str) -> str:
    """
    Migliora l'aspetto delle tabelle HTML.
    
    Args:
        html_content: Contenuto HTML
        
    Returns:
        Contenuto HTML con tabelle migliorate
    """
    # Aggiungi classi alle tabelle per stili migliori
    html_content = re.sub(r'<table>', r'<table class="data-table">', html_content)
    
    # Aggiungi header responsive
    html_content = re.sub(
        r'<table class="data-table">',
        r'<div class="table-responsive"><table class="data-table">',
        html_content
    )
    
    html_content = re.sub(r'</table>', r'</table></div>', html_content)
    
    return html_content

def _enhance_external_links(html_content: str) -> str:
    """
    Migliora i link esterni con target blank e rel noopener.
    
    Args:
        html_content: Contenuto HTML
        
    Returns:
        Contenuto HTML con link migliorati
    """
    # Aggiungi target="_blank" e rel="noopener noreferrer" ai link esterni
    external_link_pattern = r'<a href="(https?://[^"]+)"([^>]*)>'
    
    # Funzione per gestire la sostituzione
    def enhance_link(match):
        url = match.group(1)
        attrs = match.group(2)
        
        if 'target=' not in attrs:
            attrs += ' target="_blank"'
        
        if 'rel=' not in attrs:
            attrs += ' rel="noopener noreferrer"'
        
        return f'<a href="{url}"{attrs}>'
    
    # Applica la sostituzione
    html_content = re.sub(external_link_pattern, enhance_link, html_content)
    
    return html_content

def _add_missing_alt_attributes(html_content: str) -> str:
    """
    Aggiunge attributi alt alle immagini che ne sono prive.
    
    Args:
        html_content: Contenuto HTML
        
    Returns:
        Contenuto HTML con attributi alt aggiunti
    """
    # Trova immagini senza attributo alt
    img_pattern = r'<img([^>]*?)src="([^"]+)"([^>]*?)>'
    
    # Funzione per gestire la sostituzione
    def add_alt(match):
        attrs_before = match.group(1) or ''
        src = match.group(2)
        attrs_after = match.group(3) or ''
        
        # Verifica se l'attributo alt è già presente
        if 'alt=' not in attrs_before and 'alt=' not in attrs_after:
            # Estrai il nome del file dall'URL
            file_name = src.split('/')[-1].split('.')[0]
            # Converti in formato leggibile
            alt_text = file_name.replace('-', ' ').replace('_', ' ').title()
            # Aggiungi l'attributo alt
            return f'<img{attrs_before}src="{src}"{attrs_after} alt="{alt_text}">'
        
        return match.group(0)
    
    # Applica la sostituzione
    html_content = re.sub(img_pattern, add_alt, html_content)
    
    return html_content

def add_metadata_to_html(html_content: str, metadata: Dict[str, Any]) -> str:
    """
    Aggiunge metadati al contenuto HTML.
    
    Args:
        html_content: Contenuto HTML
        metadata: Dizionario con metadati
        
    Returns:
        Contenuto HTML con metadati
    """
    # Crea meta tag per ogni metadato
    meta_tags = ""
    
    # Titolo e descrizione per SEO
    if 'title' in metadata:
        meta_tags += f'<title>{metadata["title"]}</title>\n'
        meta_tags += f'<meta name="title" content="{metadata["title"]}">\n'
    
    if 'description' in metadata:
        meta_tags += f'<meta name="description" content="{metadata["description"]}">\n'
    
    # Aggiungi metadati Open Graph
    if 'title' in metadata:
        meta_tags += f'<meta property="og:title" content="{metadata["title"]}">\n'
    
    if 'description' in metadata:
        meta_tags += f'<meta property="og:description" content="{metadata["description"]}">\n'
    
    # Tipo di contenuto
    meta_tags += '<meta property="og:type" content="article">\n'
    
    # URL canonico se presente
    if 'url' in metadata:
        meta_tags += f'<meta property="og:url" content="{metadata["url"]}">\n'
    
    # Immagine se presente
    if 'image' in metadata:
        meta_tags += f'<meta property="og:image" content="{metadata["image"]}">\n'
    
    # Autore
    if 'author' in metadata:
        meta_tags += f'<meta name="author" content="{metadata["author"]}">\n'
    
    # Data di pubblicazione
    if 'published_time' in metadata:
        meta_tags += f'<meta property="article:published_time" content="{metadata["published_time"]}">\n'
    
    # Keywords
    if 'keywords' in metadata and isinstance(metadata['keywords'], list):
        keywords = ', '.join(metadata['keywords'])
        meta_tags += f'<meta name="keywords" content="{keywords}">\n'
    
    # Verifica se l'HTML ha già un tag <head>
    if '<head>' in html_content:
        # Inserisci i meta tag nel <head> esistente
        html_content = html_content.replace('<head>', f'<head>\n{meta_tags}')
    else:
        # Crea un documento HTML completo
        full_html = f"""<!DOCTYPE html>
<html lang="{metadata.get('language', 'en')}">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
{meta_tags}
</head>
<body>
{html_content}
</body>
</html>
"""
        html_content = full_html
    
    return html_content

def create_responsive_html_template(title: str, content: str, metadata: Optional[Dict[str, Any]] = None) -> str:
    """
    Crea un template HTML responsive con il contenuto fornito.
    
    Args:
        title: Titolo della pagina
        content: Contenuto HTML
        metadata: Metadati opzionali
        
    Returns:
        Template HTML completo
    """
    # Crea metadati di base se non forniti
    if metadata is None:
        metadata = {}
    
    if 'title' not in metadata:
        metadata['title'] = title
    
    # Crea la struttura HTML di base
    html_template = f"""<!DOCTYPE html>
<html lang="{metadata.get('language', 'en')}">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{metadata.get('title', title)}</title>
"""
    
    # Aggiungi metadati
    if 'description' in metadata:
        html_template += f'<meta name="description" content="{metadata["description"]}">\n'
    
    if 'keywords' in metadata and isinstance(metadata['keywords'], list):
        keywords = ', '.join(metadata['keywords'])
        html_template += f'<meta name="keywords" content="{keywords}">\n'
    
    # Aggiungi stili CSS responsive
    html_template += """<style>
        /* Stili di base */
        * {
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            line-height: 1.6;
            color: #333;
            background-color: #f9f9f9;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
            background-color: #fff;
            box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
        }
        /* Header */
        header {
            text-align: center;
            margin-bottom: 30px;
            padding-bottom: 20px;
            border-bottom: 1px solid #eee;
        }
        h1 {
            font-size: 2em;
            margin-bottom: 10px;
            color: #222;
        }
        /* Content */
        .content {
            margin-bottom: 30px;
        }
        /* Stili per testo */
        h2 {
            font-size: 1.8em;
            margin: 25px 0 15px;
            padding-bottom: 10px;
            border-bottom: 1px solid #eee;
            color: #222;
        }
        h3 {
            font-size: 1.5em;
            margin: 20px 0 10px;
            color: #333;
        }
        p {
            margin-bottom: 15px;
        }
        ul, ol {
            margin: 15px 0;
            padding-left: 30px;
        }
        /* Tabelle responsive */
        .table-responsive {
            overflow-x: auto;
            margin-bottom: 20px;
        }
        table {
            width: 100%;
            border-collapse: collapse;
        }
        th, td {
            padding: 10px;
            border: 1px solid #ddd;
            text-align: left;
        }
        th {
            background-color: #f2f2f2;
            font-weight: bold;
        }
        tr:nth-child(even) {
            background-color: #f9f9f9;
        }
        /* Footer */
        footer {
            margin-top: 30px;
            padding-top: 20px;
            border-top: 1px solid #eee;
            text-align: center;
            font-size: 0.9em;
            color: #666;
        }
        /* Classi specifiche */
        .prediction-box {
            background-color: #f0f8ff;
            border-left: 4px solid #0366d6;
            padding: 15px;
            margin-bottom: 20px;
            border-radius: 3px;
        }
        .value-bet {
            background-color: #f0fff0;
            border-left: 4px solid #28a745;
            padding: 15px;
            margin-bottom: 20px;
            border-radius: 3px;
        }
        .team-stats {
            background-color: #f8f9fa;
            padding: 15px;
            border-radius: 3px;
            margin-bottom: 20px;
        }
        .disclaimer {
            font-size: 0.9em;
            font-style: italic;
            color: #666;
            padding: 10px;
            background-color: #f8f9fa;
            border-radius: 3px;
            margin-top: 20px;
        }
        /* Media queries per responsività */
        @media (max-width: 768px) {
            h1 {
                font-size: 1.8em;
            }
            h2 {
                font-size: 1.5em;
            }
            h3 {
                font-size: 1.3em;
            }
            .container {
                padding: 15px;
            }
        }
        @media (max-width: 480px) {
            h1 {
                font-size: 1.6em;
            }
            h2 {
                font-size: 1.3em;
            }
            h3 {
                font-size: 1.1em;
            }
            .container {
                padding: 10px;
            }
            th, td {
                padding: 8px;
                font-size: 0.9em;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>""" + title + """</h1>
"""
    
    # Aggiungi sottotitolo se presente
    if 'subtitle' in metadata:
        html_template += f'<p class="subtitle">{metadata["subtitle"]}</p>\n'
    
    html_template += """
        </header>
        <div class="content">
""" + content + """
        </div>
        <footer>
"""
    
    # Aggiungi data di pubblicazione se presente
    if 'published_time' in metadata:
        try:
            # Formatta la data in modo leggibile
            from datetime import datetime
            pub_date = datetime.fromisoformat(metadata['published_time'].replace('Z', '+00:00'))
            formatted_date = pub_date.strftime("%B %d, %Y")
            html_template += f'<p>Published on {formatted_date}</p>\n'
        except:
            # In caso di errore, usa la data non formattata
            html_template += f'<p>Published on {metadata["published_time"]}</p>\n'
    
    # Aggiungi autore se presente
    if 'author' in metadata:
        html_template += f'<p>Written by {metadata["author"]}</p>\n'
    
    html_template += """
            <p>&copy; 2024 Football Predictions System. All rights reserved.</p>
        </footer>
    </div>
</body>
</html>
"""
    
    return html_template
