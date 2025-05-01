"""
Template per la generazione della sezione di analisi statistica.
Questo modulo contiene le funzioni per generare la sezione di analisi
statistica dell'articolo con grafici, tabelle e confronti dettagliati.
"""
import logging
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

def generate_stats_analysis(match_data: Dict[str, Any], language: str = 'en') -> str:
    """
    Genera la sezione di analisi statistica.
    
    Args:
        match_data: Dati arricchiti della partita
        language: Lingua dell'articolo ('en', 'it', etc.)
        
    Returns:
        Testo della sezione di analisi statistica
    """
    logger.info(f"Generando analisi statistica in lingua={language}")
    
    try:
        # Estrai i dati principali
        home_team = match_data.get('home_team', 'Home Team')
        away_team = match_data.get('away_team', 'Away Team')
        
        # Verifica la presenza di dati statistici
        has_stats = (
            'prediction' in match_data and 
            'team_comparison' in match_data['prediction'] and
            match_data['prediction']['team_comparison']
        )
        
        has_h2h = (
            'h2h' in match_data and 
            match_data['h2h'] and 
            match_data['h2h'].get('has_data', False)
        )
        
        # Se non ci sono dati sufficienti, genera una sezione minima
        if not has_stats and not has_h2h:
            return _generate_minimal_stats(home_team, away_team, language)
        
        # Genera la sezione in base alla lingua
        if language == 'en':
            return _generate_stats_en(home_team, away_team, match_data, has_stats, has_h2h)
        elif language == 'it':
            return _generate_stats_it(home_team, away_team, match_data, has_stats, has_h2h)
        else:
            # Default a inglese per altre lingue
            return _generate_stats_en(home_team, away_team, match_data, has_stats, has_h2h)
    
    except Exception as e:
        logger.error(f"Errore nella generazione dell'analisi statistica: {e}")
        
        # In caso di errore, restituisci una sezione minima
        if language == 'en':
            return "## Statistical Analysis\n\nDetailed statistics will be available soon."
        elif language == 'it':
            return "## Analisi Statistica\n\nStatistiche dettagliate saranno disponibili a breve."
        else:
            return "## Statistical Analysis\n\nDetailed statistics will be available soon."

def _generate_minimal_stats(home_team: str, away_team: str, language: str) -> str:
    """
    Genera una sezione di statistiche minima quando non ci sono dati disponibili.
    
    Args:
        home_team: Nome squadra casa
        away_team: Nome squadra trasferta
        language: Lingua dell'articolo
        
    Returns:
        Testo della sezione di statistiche minima
    """
    if language == 'en':
        content = "## Statistical Analysis\n\n"
        content += f"For this match between {home_team} and {away_team}, comprehensive statistical data is currently limited. "
        content += "Stay tuned for updates closer to match day, when more detailed performance metrics and head-to-head statistics will be available."
    elif language == 'it':
        content = "## Analisi Statistica\n\n"
        content += f"Per questa partita tra {home_team} e {away_team}, i dati statistici completi sono attualmente limitati. "
        content += "Restate sintonizzati per aggiornamenti più vicini al giorno della partita, quando saranno disponibili metriche di prestazione più dettagliate e statistiche degli scontri diretti."
    else:
        content = "## Statistical Analysis\n\n"
        content += f"For this match between {home_team} and {away_team}, comprehensive statistical data is currently limited. "
        content += "Stay tuned for updates closer to match day."
    
    return content

def _generate_stats_en(home_team: str, away_team: str, match_data: Dict[str, Any],
                      has_stats: bool, has_h2h: bool) -> str:
    """
    Genera la sezione di analisi statistica in inglese.
    
    Args:
        home_team: Nome squadra casa
        away_team: Nome squadra trasferta
        match_data: Dati completi della partita
        has_stats: Se ci sono dati statistici
        has_h2h: Se ci sono dati head-to-head
        
    Returns:
        Testo dell'analisi statistica in inglese
    """
    content = "## Statistical Analysis\n\n"
    
    # Confronto statistico tra le squadre
    if has_stats:
        content += "### Team Performance Comparison\n\n"
        comparison = match_data['prediction']['team_comparison']
        
        # Crea tabella con confronto statistico
        content += "| Metric | " + home_team + " | " + away_team + " |\n"
        content += "|--------|------|------|\n"
        
        # Aggiungi metriche principali alla tabella
        metrics_to_include = [
            ('overall', 'Overall Rating'),
            ('form', 'Recent Form'),
            ('attack', 'Attack Strength'),
            ('defense', 'Defense Strength'),
            ('home_advantage', 'Home/Away Performance'),
            ('goals_scored', 'Goals Scored (avg)'),
            ('goals_conceded', 'Goals Conceded (avg)'),
            ('clean_sheets', 'Clean Sheets %'),
            ('btts', 'Both Teams Scored %')
        ]
        
        for metric_key, metric_name in metrics_to_include:
            if metric_key in comparison:
                metric = comparison[metric_key]
                home_value = metric.get('team1', 0)
                away_value = metric.get('team2', 0)
                
                # Formatta i valori in base al tipo
                if isinstance(home_value, float) and isinstance(away_value, float):
                    if metric_key in ['goals_scored', 'goals_conceded']:
                        content += f"| {metric_name} | {home_value:.2f} | {away_value:.2f} |\n"
                    elif metric_key in ['clean_sheets', 'btts']:
                        content += f"| {metric_name} | {home_value:.0%} | {away_value:.0%} |\n"
                    else:
                        content += f"| {metric_name} | {home_value:.1f} | {away_value:.1f} |\n"
                else:
                    content += f"| {metric_name} | {home_value} | {away_value} |\n"
        
        content += "\n"
        
        # Aggiungi analisi testuale del confronto
        if 'advantage' in comparison:
            advantage = comparison['advantage']
            advantage_team = advantage.get('team', '')
            advantage_score = advantage.get('score', 0)
            
            if advantage_team and advantage_score:
                if advantage_team == 'team1':
                    if advantage_score > 15:
                        content += f"The statistical analysis shows a significant advantage for **{home_team}**. "
                    else:
                        content += f"The statistics indicate a slight advantage for **{home_team}**. "
                elif advantage_team == 'team2':
                    if advantage_score > 15:
                        content += f"The statistical analysis shows a significant advantage for **{away_team}**. "
                    else:
                        content += f"The statistics indicate a slight advantage for **{away_team}**. "
                else:  # 'equal'
                    content += f"Statistically, this match appears to be evenly balanced between **{home_team}** and **{away_team}**. "
        
        # Includi conclusioni dalle statistiche
        if 'stat_conclusions' in match_data['prediction']:
            conclusions = match_data['prediction']['stat_conclusions']
            if conclusions and isinstance(conclusions, list):
                content += "\n\n**Key Statistical Insights:**\n\n"
                for conclusion in conclusions[:3]:  # Limita a 3 conclusioni
                    content += f"- {conclusion}\n"
        
        content += "\n"
    
    # Head-to-Head
    if has_h2h:
        content += "### Head-to-Head Record\n\n"
        h2h = match_data['h2h']
        
        total_matches = h2h.get('total_matches', 0)
        team1_wins = h2h.get('team1_wins', 0)
        team2_wins = h2h.get('team2_wins', 0)
        draws = h2h.get('draws', 0)
        
        # Tabella riassuntiva H2H
        content += "| Result | Count | Percentage |\n"
        content += "|--------|-------|------------|\n"
        content += f"| {home_team} Wins | {team1_wins} | {team1_wins/total_matches:.0%} |\n" if total_matches > 0 else f"| {home_team} Wins | 0 | 0% |\n"
        content += f"| Draws | {draws} | {draws/total_matches:.0%} |\n" if total_matches > 0 else "| Draws | 0 | 0% |\n"
        content += f"| {away_team} Wins | {team2_wins} | {team2_wins/total_matches:.0%} |\n" if total_matches > 0 else f"| {away_team} Wins | 0 | 0% |\n"
        
        content += "\n"
        
        # Aggiungi statistiche goal
        if 'goals' in h2h:
            goals = h2h['goals']
            team1_goals = goals.get('team1_goals', 0)
            team2_goals = goals.get('team2_goals', 0)
            avg_match_goals = goals.get('avg_goals_per_match', 0)
            btts_pct = goals.get('btts_percentage', 0)
            
            content += "**Goal Statistics in Head-to-Head Matches:**\n\n"
            content += f"- Average goals per match: **{avg_match_goals:.2f}**\n"
            content += f"- Both teams scored: **{btts_pct:.0%}** of matches\n"
            content += f"- {home_team} scored a total of **{team1_goals}** goals (avg {team1_goals/total_matches:.2f} per game)\n" if total_matches > 0 else ""
            content += f"- {away_team} scored a total of **{team2_goals}** goals (avg {team2_goals/total_matches:.2f} per game)\n" if total_matches > 0 else ""
            
            content += "\n"
        
        # Aggiungi ultime partite
        if 'recent_matches' in h2h and h2h['recent_matches']:
            recent = h2h['recent_matches']
            content += "**Recent Head-to-Head Matches:**\n\n"
            
            for i, match in enumerate(recent[:5]):  # Mostra al massimo 5 partite recenti
                date = match.get('date', '')
                home = match.get('home_team', '')
                away = match.get('away_team', '')
                score = match.get('score', '')
                competition = match.get('competition', '')
                
                content += f"- {date}: **{home}** {score} **{away}**"
                if competition:
                    content += f" ({competition})"
                content += "\n"
            
            content += "\n"
    
    # Sezione di tendenze statistiche
    if 'trends' in match_data and match_data['trends'] and match_data['trends'].get('has_data', False):
        trends = match_data['trends']
        relevant_trends = trends.get('relevant_trends', [])
        
        if relevant_trends:
            content += "### Statistical Trends\n\n"
            
            for trend in relevant_trends[:5]:  # Limita a 5 tendenze principali
                content += f"- {trend}\n"
            
            content += "\n"
    
    return content

def _generate_stats_it(home_team: str, away_team: str, match_data: Dict[str, Any],
                      has_stats: bool, has_h2h: bool) -> str:
    """
    Genera la sezione di analisi statistica in italiano.
    
    Args:
        home_team: Nome squadra casa
        away_team: Nome squadra trasferta
        match_data: Dati completi della partita
        has_stats: Se ci sono dati statistici
        has_h2h: Se ci sono dati head-to-head
        
    Returns:
        Testo dell'analisi statistica in italiano
    """
    content = "## Analisi Statistica\n\n"
    
    # Confronto statistico tra le squadre
    if has_stats:
        content += "### Confronto Prestazioni Squadre\n\n"
        comparison = match_data['prediction']['team_comparison']
        
        # Crea tabella con confronto statistico
        content += "| Metrica | " + home_team + " | " + away_team + " |\n"
        content += "|---------|------|------|\n"
        
        # Aggiungi metriche principali alla tabella
        metrics_to_include = [
            ('overall', 'Valutazione Complessiva'),
            ('form', 'Forma Recente'),
            ('attack', 'Forza Offensiva'),
            ('defense', 'Solidità Difensiva'),
            ('home_advantage', 'Rendimento Casa/Trasferta'),
            ('goals_scored', 'Goal Segnati (media)'),
            ('goals_conceded', 'Goal Subiti (media)'),
            ('clean_sheets', 'Clean Sheet %'),
            ('btts', 'Goal Entrambe %')
        ]
        
        for metric_key, metric_name in metrics_to_include:
            if metric_key in comparison:
                metric = comparison[metric_key]
                home_value = metric.get('team1', 0)
                away_value = metric.get('team2', 0)
                
                # Formatta i valori in base al tipo
                if isinstance(home_value, float) and isinstance(away_value, float):
                    if metric_key in ['goals_scored', 'goals_conceded']:
                        content += f"| {metric_name} | {home_value:.2f} | {away_value:.2f} |\n"
                    elif metric_key in ['clean_sheets', 'btts']:
                        content += f"| {metric_name} | {home_value:.0%} | {away_value:.0%} |\n"
                    else:
                        content += f"| {metric_name} | {home_value:.1f} | {away_value:.1f} |\n"
                else:
                    content += f"| {metric_name} | {home_value} | {away_value} |\n"
        
        content += "\n"
        
        # Aggiungi analisi testuale del confronto
        if 'advantage' in comparison:
            advantage = comparison['advantage']
            advantage_team = advantage.get('team', '')
            advantage_score = advantage.get('score', 0)
            
            if advantage_team and advantage_score:
                if advantage_team == 'team1':
                    if advantage_score > 15:
                        content += f"L'analisi statistica mostra un vantaggio significativo per il **{home_team}**. "
                    else:
                        content += f"Le statistiche indicano un leggero vantaggio per il **{home_team}**. "
                elif advantage_team == 'team2':
                    if advantage_score > 15:
                        content += f"L'analisi statistica mostra un vantaggio significativo per il **{away_team}**. "
                    else:
                        content += f"Le statistiche indicano un leggero vantaggio per il **{away_team}**. "
                else:  # 'equal'
                    content += f"Statisticamente, questa partita appare equilibrata tra **{home_team}** e **{away_team}**. "
        
        # Includi conclusioni dalle statistiche
        if 'stat_conclusions' in match_data['prediction']:
            conclusions = match_data['prediction']['stat_conclusions']
            if conclusions and isinstance(conclusions, list):
                content += "\n\n**Principali Evidenze Statistiche:**\n\n"
                for conclusion in conclusions[:3]:  # Limita a 3 conclusioni
                    content += f"- {conclusion}\n"
        
        content += "\n"
    
    # Head-to-Head
    if has_h2h:
        content += "### Storico Scontri Diretti\n\n"
        h2h = match_data['h2h']
        
        total_matches = h2h.get('total_matches', 0)
        team1_wins = h2h.get('team1_wins', 0)
        team2_wins = h2h.get('team2_wins', 0)
        draws = h2h.get('draws', 0)
        
        # Tabella riassuntiva H2H
        content += "| Risultato | Numero | Percentuale |\n"
        content += "|-----------|--------|-------------|\n"
        content += f"| Vittorie {home_team} | {team1_wins} | {team1_wins/total_matches:.0%} |\n" if total_matches > 0 else f"| Vittorie {home_team} | 0 | 0% |\n"
        content += f"| Pareggi | {draws} | {draws/total_matches:.0%} |\n" if total_matches > 0 else "| Pareggi | 0 | 0% |\n"
        content += f"| Vittorie {away_team} | {team2_wins} | {team2_wins/total_matches:.0%} |\n" if total_matches > 0 else f"| Vittorie {away_team} | 0 | 0% |\n"
        
        content += "\n"
        
        # Aggiungi statistiche goal
        if 'goals' in h2h:
            goals = h2h['goals']
            team1_goals = goals.get('team1_goals', 0)
            team2_goals = goals.get('team2_goals', 0)
            avg_match_goals = goals.get('avg_goals_per_match', 0)
            btts_pct = goals.get('btts_percentage', 0)
            
            content += "**Statistiche Goal negli Scontri Diretti:**\n\n"
            content += f"- Media goal per partita: **{avg_match_goals:.2f}**\n"
            content += f"- Entrambe le squadre a segno: **{btts_pct:.0%}** delle partite\n"
            content += f"- Il {home_team} ha segnato un totale di **{team1_goals}** goal (media {team1_goals/total_matches:.2f} a partita)\n" if total_matches > 0 else ""
            content += f"- Il {away_team} ha segnato un totale di **{team2_goals}** goal (media {team2_goals/total_matches:.2f} a partita)\n" if total_matches > 0 else ""
            
            content += "\n"
        
        # Aggiungi ultime partite
        if 'recent_matches' in h2h and h2h['recent_matches']:
            recent = h2h['recent_matches']
            content += "**Scontri Diretti Recenti:**\n\n"
            
            for i, match in enumerate(recent[:5]):  # Mostra al massimo 5 partite recenti
                date = match.get('date', '')
                home = match.get('home_team', '')
                away = match.get('away_team', '')
                score = match.get('score', '')
                competition = match.get('competition', '')
                
                content += f"- {date}: **{home}** {score} **{away}**"
                if competition:
                    content += f" ({competition})"
                content += "\n"
            
            content += "\n"
    
    # Sezione di tendenze statistiche
    if 'trends' in match_data and match_data['trends'] and match_data['trends'].get('has_data', False):
        trends = match_data['trends']
        relevant_trends = trends.get('relevant_trends', [])
        
        if relevant_trends:
            content += "### Tendenze Statistiche\n\n"
            
            for trend in relevant_trends[:5]:  # Limita a 5 tendenze principali
                content += f"- {trend}\n"
            
            content += "\n"
    
    return content
