"""
Template per la generazione della sezione di previsione e pronostici.
Questo modulo contiene le funzioni per generare la sezione di previsione
dell'articolo con pronostici basati su dati statistici.
"""
import logging
import random
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

def generate_prediction(match_data: Dict[str, Any],
                       include_value_bets: bool = True,
                       language: str = 'en') -> str:
    """
    Genera la sezione di previsione e pronostici.
    
    Args:
        match_data: Dati arricchiti della partita
        include_value_bets: Se includere le value bet nel pronostico
        language: Lingua dell'articolo ('en', 'it', etc.)
        
    Returns:
        Testo della sezione di previsione
    """
    logger.info(f"Generando previsione con value_bets={include_value_bets}, lingua={language}")
    
    try:
        # Estrai i dati principali
        home_team = match_data.get('home_team', 'Home Team')
        away_team = match_data.get('away_team', 'Away Team')
        
        # Verifica la presenza di dati di previsione
        if 'prediction' not in match_data or not match_data['prediction']:
            logger.warning("Dati di previsione mancanti, generando contenuto generico")
            return _generate_generic_prediction(home_team, away_team, language)
        
        # Genera la sezione in base alla lingua
        if language == 'en':
            return _generate_prediction_en(
                home_team, away_team, match_data, include_value_bets
            )
        elif language == 'it':
            return _generate_prediction_it(
                home_team, away_team, match_data, include_value_bets
            )
        else:
            # Default a inglese per altre lingue
            return _generate_prediction_en(
                home_team, away_team, match_data, include_value_bets
            )
    
    except Exception as e:
        logger.error(f"Errore nella generazione della sezione previsione: {e}")
        
        # In caso di errore, restituisci una previsione minima
        if language == 'en':
            return "## Match Prediction\n\nPrediction coming soon."
        elif language == 'it':
            return "## Pronostico Partita\n\nPronostico in arrivo."
        else:
            return "## Match Prediction\n\nPrediction coming soon."

def _generate_generic_prediction(home_team: str, away_team: str, language: str) -> str:
    """
    Genera una previsione generica quando non ci sono dati disponibili.
    
    Args:
        home_team: Nome squadra casa
        away_team: Nome squadra trasferta
        language: Lingua dell'articolo
        
    Returns:
        Testo della previsione generica
    """
    if language == 'en':
        content = f"## Match Prediction\n\n"
        content += f"The match between {home_team} and {away_team} is expected to be competitive. "
        content += "Both teams have their strengths and will be looking to secure a positive result. "
        content += "With limited data available for this specific fixture, we recommend caution when placing bets on this match.\n"
    elif language == 'it':
        content = f"## Pronostico Partita\n\n"
        content += f"La partita tra {home_team} e {away_team} si preannuncia equilibrata. "
        content += "Entrambe le squadre hanno i loro punti di forza e cercheranno di ottenere un risultato positivo. "
        content += "Con dati limitati disponibili per questa sfida, consigliamo prudenza nelle scommesse su questa partita.\n"
    else:
        content = f"## Match Prediction\n\n"
        content += f"The match between {home_team} and {away_team} is expected to be competitive. "
        content += "Both teams have their strengths and will be looking to secure a positive result.\n"
    
    return content

def _generate_prediction_en(home_team: str, away_team: str, 
                           match_data: Dict[str, Any],
                           include_value_bets: bool) -> str:
    """
    Genera la sezione di previsione in inglese.
    
    Args:
        home_team: Nome squadra casa
        away_team: Nome squadra trasferta
        match_data: Dati completi della partita
        include_value_bets: Se includere le value bet
        
    Returns:
        Testo della previsione in inglese
    """
    prediction = match_data['prediction']
    
    # Titolo
    content = "## Match Prediction\n\n"
    
    # Previsione principale
    if 'main_prediction' in prediction and prediction['main_prediction']:
        main_pred = prediction['main_prediction']
        
        content += "### Our Top Prediction\n\n"
        
        # Estrai i dettagli
        market = main_pred.get('market', '')
        selection = main_pred.get('selection', '')
        odds = main_pred.get('odds', '')
        confidence = main_pred.get('confidence', '')
        description = main_pred.get('description', '')
        
        # Formatta la previsione principale
        if market == 'match_winner':
            if selection == '1':
                content += f"**{home_team} to Win** "
            elif selection == 'X':
                content += "**Draw** "
            elif selection == '2':
                content += f"**{away_team} to Win** "
        elif market == 'btts':
            if selection == 'Yes':
                content += "**Both Teams to Score - Yes** "
            else:
                content += "**Both Teams to Score - No** "
        elif market == 'over_under':
            if 'Over' in selection:
                line = main_pred.get('line', '2.5')
                content += f"**Over {line} Goals** "
            else:
                line = main_pred.get('line', '2.5')
                content += f"**Under {line} Goals** "
        elif market == 'double_chance':
            if selection == '1X':
                content += f"**Double Chance: {home_team} or Draw** "
            elif selection == 'X2':
                content += f"**Double Chance: Draw or {away_team}** "
            elif selection == '12':
                content += f"**Double Chance: {home_team} or {away_team}** "
        else:
            # Formato generico per altri tipi di mercato
            content += f"**{market}: {selection}** "
        
        # Aggiungi quota se disponibile
        if odds:
            content += f"@ **{odds}**"
        
        content += "\n\n"
        
        # Aggiungi livello di confidenza
        if confidence:
            content += f"**Confidence Level:** {confidence}\n\n"
        
        # Aggiungi descrizione
        if description:
            content += f"{description}\n\n"
    
    # Altre previsioni
    if 'other_predictions' in prediction and prediction['other_predictions']:
        content += "### Other Predictions\n\n"
        
        for pred in prediction['other_predictions'][:3]:  # Limita a 3 previsioni aggiuntive
            market = pred.get('market', '')
            selection = pred.get('selection', '')
            odds = pred.get('odds', '')
            
            # Formatta il mercato in modo leggibile
            if market == 'btts':
                market_str = "Both Teams to Score"
            elif market == 'over_under':
                line = pred.get('line', '2.5')
                market_str = f"Over/Under {line} Goals"
            elif market == 'asian_handicap':
                line = pred.get('line', '0')
                market_str = f"Asian Handicap {line}"
            elif market == 'double_chance':
                market_str = "Double Chance"
            else:
                market_str = market.replace('_', ' ').title()
            
            content += f"- **{market_str}:** {selection}"
            
            if odds:
                content += f" @ {odds}"
            
            content += "\n"
        
        content += "\n"
    
    # Motivazioni
    if 'reasoning' in prediction and prediction['reasoning']:
        content += "### Prediction Reasoning\n\n"
        
        for reason in prediction['reasoning'][:5]:  # Limita a 5 motivazioni
            content += f"- {reason}\n"
        
        content += "\n"
    
    # Value Bet
    if include_value_bets and 'value_bets' in match_data and match_data['value_bets']:
        value_bets = match_data['value_bets']
        
        if value_bets.get('has_value_bets', False) and value_bets.get('value_bets', []):
            content += "### Value Bets\n\n"
            
            for bet in value_bets['value_bets'][:2]:  # Limita a 2 value bet
                market = bet.get('market', '')
                selection = bet.get('selection', '')
                odds = bet.get('odds', '')
                edge = bet.get('edge', 0)
                bookmaker = bet.get('bookmaker', '')
                description = bet.get('description', '')
                
                if market and selection and odds:
                    content += f"- **{description}**"
                    
                    if bookmaker:
                        content += f" (via {bookmaker})"
                    
                    content += "\n"
            
            content += "\n"
    
    # Avvertenza
    content += "### Betting Advice\n\n"
    content += "*Remember that all predictions are based on statistical analysis and there is always inherent uncertainty in sports. "
    content += "Always bet responsibly and within your means.*\n"
    
    return content

def _generate_prediction_it(home_team: str, away_team: str, 
                           match_data: Dict[str, Any],
                           include_value_bets: bool) -> str:
    """
    Genera la sezione di previsione in italiano.
    
    Args:
        home_team: Nome squadra casa
        away_team: Nome squadra trasferta
        match_data: Dati completi della partita
        include_value_bets: Se includere le value bet
        
    Returns:
        Testo della previsione in italiano
    """
    prediction = match_data['prediction']
    
    # Titolo
    content = "## Pronostico Partita\n\n"
    
    # Previsione principale
    if 'main_prediction' in prediction and prediction['main_prediction']:
        main_pred = prediction['main_prediction']
        
        content += "### Il Nostro Pronostico Principale\n\n"
        
        # Estrai i dettagli
        market = main_pred.get('market', '')
        selection = main_pred.get('selection', '')
        odds = main_pred.get('odds', '')
        confidence = main_pred.get('confidence', '')
        description = main_pred.get('description', '')
        
        # Mappatura di confidence in italiano
        confidence_map = {
            'Very High': 'Molto Alta',
            'High': 'Alta',
            'Medium': 'Media',
            'Low': 'Bassa',
            'Very Low': 'Molto Bassa'
        }
        
        confidence_it = confidence_map.get(confidence, confidence)
        
        # Formatta la previsione principale
        if market == 'match_winner':
            if selection == '1':
                content += f"**Vittoria {home_team}** "
            elif selection == 'X':
                content += "**Pareggio** "
            elif selection == '2':
                content += f"**Vittoria {away_team}** "
        elif market == 'btts':
            if selection == 'Yes':
                content += "**Goal di Entrambe le Squadre - Si** "
            else:
                content += "**Goal di Entrambe le Squadre - No** "
        elif market == 'over_under':
            if 'Over' in selection:
                line = main_pred.get('line', '2.5')
                content += f"**Over {line} Goal** "
            else:
                line = main_pred.get('line', '2.5')
                content += f"**Under {line} Goal** "
        elif market == 'double_chance':
            if selection == '1X':
                content += f"**Doppia Chance: {home_team} o Pareggio** "
            elif selection == 'X2':
                content += f"**Doppia Chance: Pareggio o {away_team}** "
            elif selection == '12':
                content += f"**Doppia Chance: {home_team} o {away_team}** "
        else:
            # Formato generico per altri tipi di mercato
            content += f"**{market}: {selection}** "
        
        # Aggiungi quota se disponibile
        if odds:
            content += f"@ **{odds}**"
        
        content += "\n\n"
        
        # Aggiungi livello di confidenza
        if confidence:
            content += f"**Livello di Confidenza:** {confidence_it}\n\n"
        
        # Aggiungi descrizione
        if description:
            content += f"{description}\n\n"
    
    # Altre previsioni
    if 'other_predictions' in prediction and prediction['other_predictions']:
        content += "### Altri Pronostici\n\n"
        
        for pred in prediction['other_predictions'][:3]:  # Limita a 3 previsioni aggiuntive
            market = pred.get('market', '')
            selection = pred.get('selection', '')
            odds = pred.get('odds', '')
            
            # Formatta il mercato in modo leggibile in italiano
            if market == 'btts':
                market_str = "Goal di Entrambe le Squadre"
            elif market == 'over_under':
                line = pred.get('line', '2.5')
                market_str = f"Over/Under {line} Goal"
            elif market == 'asian_handicap':
                line = pred.get('line', '0')
                market_str = f"Handicap Asiatico {line}"
            elif market == 'double_chance':
                market_str = "Doppia Chance"
            elif market == 'match_winner':
                market_str = "Risultato Finale 1X2"
            else:
                market_str = market.replace('_', ' ').title()
            
            content += f"- **{market_str}:** {selection}"
            
            if odds:
                content += f" @ {odds}"
            
            content += "\n"
        
        content += "\n"
    
    # Motivazioni
    if 'reasoning' in prediction and prediction['reasoning']:
        content += "### Motivazioni del Pronostico\n\n"
        
        for reason in prediction['reasoning'][:5]:  # Limita a 5 motivazioni
            content += f"- {reason}\n"
        
        content += "\n"
    
    # Value Bet
    if include_value_bets and 'value_bets' in match_data and match_data['value_bets']:
        value_bets = match_data['value_bets']
        
        if value_bets.get('has_value_bets', False) and value_bets.get('value_bets', []):
            content += "### Value Bet\n\n"
            
            for bet in value_bets['value_bets'][:2]:  # Limita a 2 value bet
                market = bet.get('market', '')
                selection = bet.get('selection', '')
                odds = bet.get('odds', '')
                edge = bet.get('edge', 0)
                bookmaker = bet.get('bookmaker', '')
                description = bet.get('description', '')
                
                if market and selection and odds:
                    content += f"- **{description}**"
                    
                    if bookmaker:
                        content += f" (su {bookmaker})"
                    
                    content += "\n"
            
            content += "\n"
    
    # Avvertenza
    content += "### Consigli per le Scommesse\n\n"
    content += "*Ricorda che tutti i pronostici sono basati su analisi statistiche e c'è sempre un'incertezza intrinseca negli sport. "
    content += "Scommetti sempre responsabilmente e nei limiti delle tue possibilità.*\n"
    
    return content
