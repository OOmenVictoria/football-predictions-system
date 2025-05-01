"""
Template per la generazione della sezione di anteprima della partita.
Questo modulo contiene le funzioni per generare la sezione introduttiva
dell'articolo con i dettagli sulla partita e le squadre.
"""
import logging
import random
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

def generate_match_preview(match_data: Dict[str, Any], 
                          length: str = 'medium', 
                          style: str = 'formal',
                          language: str = 'en') -> str:
    """
    Genera la sezione di anteprima della partita.
    
    Args:
        match_data: Dati arricchiti della partita
        length: Lunghezza dell'anteprima ('short', 'medium', 'long')
        style: Stile della scrittura ('formal', 'casual', 'technical')
        language: Lingua dell'articolo ('en', 'it', etc.)
        
    Returns:
        Testo della sezione di anteprima
    """
    logger.info(f"Generando anteprima partita con lunghezza={length}, stile={style}, lingua={language}")
    
    try:
        # Estrai i dati principali
        home_team = match_data.get('home_team', 'Home Team')
        away_team = match_data.get('away_team', 'Away Team')
        match_datetime = match_data.get('datetime', '')
        league_id = match_data.get('league_id', '')
        league_name = match_data.get('league_name', 'League')
        stadium = match_data.get('venue', {}).get('name', '')
        
        # Formatta la data della partita
        match_date_formatted = _format_match_date(match_datetime, language)
        
        # Genera la sezione in base alla lingua
        if language == 'en':
            return _generate_preview_en(
                home_team, away_team, match_date_formatted, 
                league_name, stadium, match_data, length, style
            )
        elif language == 'it':
            return _generate_preview_it(
                home_team, away_team, match_date_formatted, 
                league_name, stadium, match_data, length, style
            )
        else:
            # Default a inglese per altre lingue
            return _generate_preview_en(
                home_team, away_team, match_date_formatted, 
                league_name, stadium, match_data, length, style
            )
    
    except Exception as e:
        logger.error(f"Errore nella generazione dell'anteprima partita: {e}")
        
        # In caso di errore, restituisci un'anteprima minima
        if language == 'en':
            return f"# {match_data.get('home_team', 'Home')} vs {match_data.get('away_team', 'Away')} Preview\n\nMatch preview coming soon."
        elif language == 'it':
            return f"# Anteprima {match_data.get('home_team', 'Casa')} vs {match_data.get('away_team', 'Trasferta')}\n\nAnteprima partita in arrivo."
        else:
            return f"# {match_data.get('home_team', 'Home')} vs {match_data.get('away_team', 'Away')} Preview\n\nMatch preview coming soon."

def _format_match_date(match_datetime: str, language: str) -> str:
    """
    Formatta la data della partita in modo leggibile.
    
    Args:
        match_datetime: Data e ora della partita (formato ISO)
        language: Lingua desiderata
        
    Returns:
        Data formattata
    """
    if not match_datetime:
        return "TBD"
    
    try:
        dt = datetime.fromisoformat(match_datetime.replace('Z', '+00:00'))
        
        if language == 'en':
            # Formato: "Sunday, May 15, 2022 - 20:45 CET"
            return dt.strftime("%A, %B %d, %Y - %H:%M %Z")
        elif language == 'it':
            # Formato: "Domenica 15 Maggio 2022 - 20:45 CET"
            # Mapping dei nomi dei giorni e mesi in italiano
            weekdays_it = {
                "Monday": "Lunedì",
                "Tuesday": "Martedì",
                "Wednesday": "Mercoledì",
                "Thursday": "Giovedì",
                "Friday": "Venerdì",
                "Saturday": "Sabato",
                "Sunday": "Domenica"
            }
            months_it = {
                "January": "Gennaio",
                "February": "Febbraio",
                "March": "Marzo",
                "April": "Aprile",
                "May": "Maggio",
                "June": "Giugno",
                "July": "Luglio",
                "August": "Agosto",
                "September": "Settembre",
                "October": "Ottobre",
                "November": "Novembre",
                "December": "Dicembre"
            }
            
            weekday = weekdays_it.get(dt.strftime("%A"), dt.strftime("%A"))
            month = months_it.get(dt.strftime("%B"), dt.strftime("%B"))
            
            return f"{weekday} {dt.day} {month} {dt.year} - {dt.strftime('%H:%M')} {dt.strftime('%Z')}"
        else:
            # Formato internazionale
            return dt.strftime("%Y-%m-%d %H:%M %Z")
    
    except (ValueError, TypeError) as e:
        logger.warning(f"Errore nel formato della data: {e}")
        return "TBD"

def _generate_preview_en(home_team: str, away_team: str, 
                        match_date: str, league_name: str, 
                        stadium: str, match_data: Dict[str, Any],
                        length: str, style: str) -> str:
    """
    Genera la sezione di anteprima in inglese.
    
    Args:
        home_team: Nome squadra casa
        away_team: Nome squadra trasferta
        match_date: Data formattata
        league_name: Nome del campionato
        stadium: Nome dello stadio
        match_data: Dati completi della partita
        length: Lunghezza dell'anteprima
        style: Stile della scrittura
        
    Returns:
        Testo dell'anteprima in inglese
    """
    # Titolo
    preview = f"# {home_team} vs {away_team} Preview\n\n"
    
    # Informazioni base
    preview += f"## Match Information\n\n"
    preview += f"**📅 Date:** {match_date}\n"
    preview += f"**🏆 Competition:** {league_name}\n"
    
    if stadium:
        preview += f"**🏟️ Venue:** {stadium}\n"
    
    preview += "\n"
    
    # Introduzione
    intro_sentences = [
        f"{home_team} will face {away_team} in what promises to be an exciting {league_name} match.",
        f"The {league_name} fixture between {home_team} and {away_team} is set to take place on {match_date}.",
        f"{home_team} host {away_team} in this important {league_name} clash.",
        f"Football fans are in for a treat as {home_team} take on {away_team} in {league_name} action."
    ]
    
    preview += "## Match Preview\n\n"
    preview += random.choice(intro_sentences) + " "
    
    # Aggiungi contesto in base ai dati disponibili
    if 'h2h' in match_data and match_data['h2h'] and match_data['h2h'].get('has_data', False):
        h2h = match_data['h2h']
        total_matches = h2h.get('total_matches', 0)
        
        if total_matches > 0:
            # Statistiche head-to-head
            team1_wins = h2h.get('team1_wins', 0)
            team2_wins = h2h.get('team2_wins', 0)
            draws = h2h.get('draws', 0)
            
            preview += f"These teams have met {total_matches} times before, with {home_team} winning {team1_wins} matches, {away_team} winning {team2_wins}, and {draws} draws. "
    
    # Aggiungi tendenze recenti
    if 'trends' in match_data and match_data['trends'] and match_data['trends'].get('has_data', False):
        trends = match_data['trends']
        relevant_trends = trends.get('relevant_trends', [])
        
        if relevant_trends:
            preview += "\n\n### Recent Form and Trends\n\n"
            
            # Limita il numero di tendenze in base alla lunghezza richiesta
            trend_limit = 3 if length == 'short' else (5 if length == 'medium' else 8)
            shown_trends = relevant_trends[:trend_limit]
            
            for trend in shown_trends:
                preview += f"- {trend}\n"
    
    # Per lunghezza 'medium' e 'long', aggiungi più dettagli
    if length in ['medium', 'long']:
        # Aggiungi dettagli sulle squadre
        preview += "\n\n### Team Analysis\n\n"
        
        # Per stile tecnico, usa più statistiche
        if style == 'technical':
            # Aggiungi statistiche di forma
            if 'prediction' in match_data and 'team_comparison' in match_data['prediction']:
                comparison = match_data['prediction']['team_comparison']
                
                preview += "#### Statistical Comparison\n\n"
                preview += "| Metric | " + home_team + " | " + away_team + " |\n"
                preview += "|--------|------|------|\n"
                
                # Aggiungi metriche di confronto
                if 'form' in comparison:
                    form = comparison['form']
                    home_form = form.get('team1', 0)
                    away_form = form.get('team2', 0)
                    preview += f"| Form Rating | {home_form:.1f} | {away_form:.1f} |\n"
                
                if 'attack' in comparison:
                    attack = comparison['attack']
                    home_attack = attack.get('team1', 0)
                    away_attack = attack.get('team2', 0)
                    preview += f"| Attack Rating | {home_attack:.1f} | {away_attack:.1f} |\n"
                
                if 'defense' in comparison:
                    defense = comparison['defense']
                    home_defense = defense.get('team1', 0)
                    away_defense = defense.get('team2', 0)
                    preview += f"| Defense Rating | {home_defense:.1f} | {away_defense:.1f} |\n"
        else:
            # Stile più narrativo per formal e casual
            preview += f"**{home_team}** will be looking to leverage their home advantage. "
            preview += f"Meanwhile, **{away_team}** will be aiming to secure a positive result on the road."
    
    # Per lunghezza 'long', aggiungi ancora più contesto
    if length == 'long':
        # Aggiungi informazioni sulla situazione in classifica
        preview += "\n\n### League Context\n\n"
        
        # Se abbiamo dati di classifica, li utilizziamo
        # Altrimenti, usiamo testo generico
        preview += f"This match is important for both teams as they look to improve their position in the {league_name} standings. "
        preview += "A win could significantly boost their prospects for the remainder of the season."
        
        # Aggiungi dettagli sui giocatori chiave
        preview += "\n\n### Key Players to Watch\n\n"
        
        # Se abbiamo informazioni sui giocatori, le utilizziamo
        # Altrimenti, testo generico
        preview += f"Both teams have talented players who could make the difference in this fixture. "
        preview += f"Fans will be eager to see which stars step up on the big occasion."
    
    return preview

def _generate_preview_it(home_team: str, away_team: str, 
                        match_date: str, league_name: str, 
                        stadium: str, match_data: Dict[str, Any],
                        length: str, style: str) -> str:
    """
    Genera la sezione di anteprima in italiano.
    
    Args:
        home_team: Nome squadra casa
        away_team: Nome squadra trasferta
        match_date: Data formattata
        league_name: Nome del campionato
        stadium: Nome dello stadio
        match_data: Dati completi della partita
        length: Lunghezza dell'anteprima
        style: Stile della scrittura
        
    Returns:
        Testo dell'anteprima in italiano
    """
    # Titolo
    preview = f"# Anteprima {home_team} vs {away_team}\n\n"
    
    # Informazioni base
    preview += f"## Informazioni Partita\n\n"
    preview += f"**📅 Data:** {match_date}\n"
    preview += f"**🏆 Competizione:** {league_name}\n"
    
    if stadium:
        preview += f"**🏟️ Stadio:** {stadium}\n"
    
    preview += "\n"
    
    # Introduzione
    intro_sentences = [
        f"Il {home_team} affronterà il {away_team} in quello che promette di essere un emozionante match di {league_name}.",
        f"La sfida di {league_name} tra {home_team} e {away_team} è in programma per {match_date}.",
        f"Il {home_team} ospita il {away_team} in questo importante incontro di {league_name}.",
        f"Gli appassionati di calcio possono godersi la sfida tra {home_team} e {away_team} nel quadro della {league_name}."
    ]
    
    preview += "## Anteprima della Partita\n\n"
    preview += random.choice(intro_sentences) + " "
    
    # Aggiungi contesto in base ai dati disponibili
    if 'h2h' in match_data and match_data['h2h'] and match_data['h2h'].get('has_data', False):
        h2h = match_data['h2h']
        total_matches = h2h.get('total_matches', 0)
        
        if total_matches > 0:
            # Statistiche head-to-head
            team1_wins = h2h.get('team1_wins', 0)
            team2_wins = h2h.get('team2_wins', 0)
            draws = h2h.get('draws', 0)
            
            preview += f"Queste squadre si sono affrontate {total_matches} volte in precedenza, con {team1_wins} vittorie per il {home_team}, {team2_wins} per il {away_team} e {draws} pareggi. "
    
    # Aggiungi tendenze recenti
    if 'trends' in match_data and match_data['trends'] and match_data['trends'].get('has_data', False):
        trends = match_data['trends']
        relevant_trends = trends.get('relevant_trends', [])
        
        if relevant_trends:
            preview += "\n\n### Forma Recente e Tendenze\n\n"
            
            # Limita il numero di tendenze in base alla lunghezza richiesta
            trend_limit = 3 if length == 'short' else (5 if length == 'medium' else 8)
            shown_trends = relevant_trends[:trend_limit]
            
            for trend in shown_trends:
                preview += f"- {trend}\n"
    
    # Per lunghezza 'medium' e 'long', aggiungi più dettagli
    if length in ['medium', 'long']:
        # Aggiungi dettagli sulle squadre
        preview += "\n\n### Analisi delle Squadre\n\n"
        
        # Per stile tecnico, usa più statistiche
        if style == 'technical':
            # Aggiungi statistiche di forma
            if 'prediction' in match_data and 'team_comparison' in match_data['prediction']:
                comparison = match_data['prediction']['team_comparison']
                
                preview += "#### Confronto Statistico\n\n"
                preview += "| Metrica | " + home_team + " | " + away_team + " |\n"
                preview += "|--------|------|------|\n"
                
                # Aggiungi metriche di confronto
                if 'form' in comparison:
                    form = comparison['form']
                    home_form = form.get('team1', 0)
                    away_form = form.get('team2', 0)
                    preview += f"| Rating Forma | {home_form:.1f} | {away_form:.1f} |\n"
                
                if 'attack' in comparison:
                    attack = comparison['attack']
                    home_attack = attack.get('team1', 0)
                    away_attack = attack.get('team2', 0)
                    preview += f"| Rating Attacco | {home_attack:.1f} | {away_attack:.1f} |\n"
                
                if 'defense' in comparison:
                    defense = comparison['defense']
                    home_defense = defense.get('team1', 0)
                    away_defense = defense.get('team2', 0)
                    preview += f"| Rating Difesa | {home_defense:.1f} | {away_defense:.1f} |\n"
        else:
            # Stile più narrativo per formal e casual
            preview += f"Il **{home_team}** cercherà di sfruttare il vantaggio di giocare in casa. "
            preview += f"Nel frattempo, il **{away_team}** punterà a ottenere un risultato positivo in trasferta."
    
    # Per lunghezza 'long', aggiungi ancora più contesto
    if length == 'long':
        # Aggiungi informazioni sulla situazione in classifica
        preview += "\n\n### Contesto di Campionato\n\n"
        
        # Se abbiamo dati di classifica, li utilizziamo
        # Altrimenti, usiamo testo generico
        preview += f"Questa partita è importante per entrambe le squadre che cercano di migliorare la loro posizione nella classifica di {league_name}. "
        preview += "Una vittoria potrebbe dare una spinta significativa alle loro prospettive per il resto della stagione."
        
        # Aggiungi dettagli sui giocatori chiave
        preview += "\n\n### Giocatori Chiave da Seguire\n\n"
        
        # Se abbiamo informazioni sui giocatori, le utilizziamo
        # Altrimenti, testo generico
        preview += f"Entrambe le squadre hanno giocatori di talento che potrebbero fare la differenza in questa partita. "
        preview += f"I tifosi saranno ansiosi di vedere quali stelle si distingueranno in questa importante occasione."
    
    return preview
