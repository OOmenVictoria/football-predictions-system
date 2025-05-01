"""
Formattatore per contenuti in Markdown.
Questo modulo contiene funzioni per formattare e ottimizzare
testo in formato Markdown per la pubblicazione.
"""
import logging
import re
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

def format_as_markdown(content: str) -> str:
    """
    Formatta il contenuto in Markdown.
    
    Args:
        content: Contenuto da formattare
        
    Returns:
        Contenuto formattato in Markdown
    """
    logger.debug("Formattando contenuto in Markdown")
    
    try:
        # Applica formattazione Markdown
        formatted_content = content
        
        # 1. Corregge spaziature tra sezioni
        formatted_content = _fix_section_spacing(formatted_content)
        
        # 2. Assicura che le liste siano formattate correttamente
        formatted_content = _fix_list_formatting(formatted_content)
        
        # 3. Ottimizza le tabelle
        formatted_content = _optimize_tables(formatted_content)
        
        # 4. Assicura che i link siano formattati correttamente
        formatted_content = _fix_links(formatted_content)
        
        # 5. Aggiunge spaziatura agli emoji per migliorare la leggibilità
        formatted_content = _fix_emoji_spacing(formatted_content)
        
        # 6. Assicura che il documento finisca con una riga vuota
        if not formatted_content.endswith('\n\n'):
            if formatted_content.endswith('\n'):
                formatted_content += '\n'
            else:
                formatted_content += '\n\n'
        
        return formatted_content
    
    except Exception as e:
        logger.error(f"Errore durante la formattazione Markdown: {e}")
        # In caso di errore, restituisci il contenuto originale
        return content

def _fix_section_spacing(content: str) -> str:
    """
    Corregge la spaziatura tra sezioni.
    
    Args:
        content: Contenuto da formattare
        
    Returns:
        Contenuto con spaziatura corretta tra sezioni
    """
    # Assicura che gli header siano preceduti da una riga vuota (eccetto il primo)
    content = re.sub(r'([^\n])\n(#+\s)', r'\1\n\n\2', content)
    
    # Assicura che gli header siano seguiti da una riga vuota
    content = re.sub(r'(#+\s.*)\n([^#\n])', r'\1\n\n\2', content)
    
    return content

def _fix_list_formatting(content: str) -> str:
    """
    Corregge la formattazione delle liste.
    
    Args:
        content: Contenuto da formattare
        
    Returns:
        Contenuto con liste formattate correttamente
    """
    # Assicura che le liste siano precedute da una riga vuota
    content = re.sub(r'([^\n])\n([-*+]\s)', r'\1\n\n\2', content)
    
    # Assicura che le liste numerate siano precedute da una riga vuota
    content = re.sub(r'([^\n])\n(\d+\.\s)', r'\1\n\n\2', content)
    
    # Assicura che gli elementi della lista siano seguiti da uno spazio
    content = re.sub(r'([-*+])([^\s])', r'\1 \2', content)
    
    return content

def _optimize_tables(content: str) -> str:
    """
    Ottimizza la formattazione delle tabelle.
    
    Args:
        content: Contenuto da formattare
        
    Returns:
        Contenuto con tabelle ottimizzate
    """
    # Identifica le tabelle (righe che iniziano con | e contengono almeno un altro |)
    table_pattern = r'(\|[^\n]+\|[^\n]*\n)+'
    
    # Assicura che le tabelle siano precedute e seguite da una riga vuota
    content = re.sub(r'([^\n])\n(' + table_pattern + ')', r'\1\n\n\2', content)
    content = re.sub(r'(' + table_pattern + ')\n([^\n])', r'\1\n\n\2', content)
    
    return content

def _fix_links(content: str) -> str:
    """
    Corregge la formattazione dei link.
    
    Args:
        content: Contenuto da formattare
        
    Returns:
        Contenuto con link formattati correttamente
    """
    # Assicura che i link inline abbiano la sintassi corretta
    # Da: [testo](url) a: [testo](url) (se necessario)
    content = re.sub(r'\[([^\]]+)\]\(([^)]+)\)', r'[\1](\2)', content)
    
    # Assicura che i link di riferimento abbiano la sintassi corretta
    # Da: [testo][ref] a: [testo][ref] (se necessario)
    content = re.sub(r'\[([^\]]+)\]\[([^\]]+)\]', r'[\1][\2]', content)
    
    return content

def _fix_emoji_spacing(content: str) -> str:
    """
    Corregge la spaziatura attorno agli emoji.
    
    Args:
        content: Contenuto da formattare
        
    Returns:
        Contenuto con spaziatura corretta attorno agli emoji
    """
    # Pattern per identificare emoji comuni
    emoji_pattern = r'([\U0001F300-\U0001F6FF\U0001F700-\U0001F77F\U0001F780-\U0001F7FF\U0001F800-\U0001F8FF\U0001F900-\U0001F9FF\U00002600-\U000027BF])'
    
    # Aggiungi spazio dopo emoji se non seguito da spazio, punto o fine riga
    content = re.sub(emoji_pattern + r'([^\s.,!?;\n])', r'\1 \2', content)
    
    return content

def add_metadata_to_markdown(content: str, metadata: Dict[str, Any]) -> str:
    """
    Aggiunge metadati YAML al contenuto Markdown.
    
    Args:
        content: Contenuto Markdown
        metadata: Dizionario con metadati
        
    Returns:
        Contenuto Markdown con metadati in formato YAML frontmatter
    """
    # Prepara il blocco YAML frontmatter
    yaml_block = "---\n"
    
    # Aggiungi ogni metadato
    for key, value in metadata.items():
        # Gestisci tipi diversi
        if isinstance(value, str):
            # Gestisci stringhe multilinea
            if '\n' in value:
                yaml_block += f"{key}: |\n"
                for line in value.split('\n'):
                    yaml_block += f"  {line}\n"
            else:
                # Metti tra virgolette se necessario
                if any(c in value for c in ":#{}[]&*!|>'\"%@,"):
                    yaml_block += f'{key}: "{value}"\n'
                else:
                    yaml_block += f"{key}: {value}\n"
        elif isinstance(value, (int, float, bool)):
            yaml_block += f"{key}: {value}\n"
        elif isinstance(value, list):
            yaml_block += f"{key}:\n"
            for item in value:
                yaml_block += f"  - {item}\n"
        elif isinstance(value, dict):
            yaml_block += f"{key}:\n"
            for sub_key, sub_value in value.items():
                yaml_block += f"  {sub_key}: {sub_value}\n"
        else:
            # Converti in stringa per altri tipi
            yaml_block += f"{key}: {str(value)}\n"
    
    yaml_block += "---\n\n"
    
    # Verifica se il contenuto ha già un blocco YAML
    if content.startswith('---\n'):
        # Se sì, sostituiscilo
        if '---\n' in content[4:]:
            second_marker = content.find('---\n', 4) + 4
            return yaml_block + content[second_marker:]
        else:
            # Frontmatter malformato, aggiungi il nuovo
            return yaml_block + content
    else:
        # Aggiungi il blocco YAML all'inizio
        return yaml_block + content
