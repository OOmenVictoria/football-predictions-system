"""
Modulo principale per la generazione di contenuti.
Questo modulo coordina la creazione di articoli di pronostico calcistico
utilizzando i dati raccolti e le analisi generate.
"""
import logging
import random
from typing import Dict, List, Any, Optional, Tuple, Union
from datetime import datetime, timedelta

from src.utils.cache import cached
from src.utils.database import FirebaseManager
from src.config.settings import get_setting
from src.content.templates.match_preview import generate_match_preview
from src.content.templates.prediction import generate_prediction
from src.content.templates.stats_analysis import generate_stats_analysis
from src.content.formatters.markdown import format_as_markdown
from src.content.formatters.html import format_as_html

logger = logging.getLogger(__name__)

class ContentGenerator:
    """
    Generatore di contenuti per articoli di pronostico calcistico.
    
    Coordina la raccolta di dati, l'analisi e la formattazione per generare
    articoli completi pronti per la pubblicazione.
    """
    
    def __init__(self):
        """Inizializza il generatore di contenuti."""
        self.db = FirebaseManager()
        
        # Carica configurazione
        self.preview_length = get_setting('content.preview_length', 'medium')  # 'short', 'medium', 'long'
        self.content_style = get_setting('content.style', 'formal')  # 'formal', 'casual', 'technical'
        self.include_stats = get_setting('content.include_stats', True)
        self.include_value_bets = get_setting('content.include_value_bets', True)
        self.max_trends = get_setting('content.max_trends', 5)
        self.article_language = get_setting('content.language', 'en')  # Lingua predefinita (en, it, es, etc.)
        
        # Aggiunge un po' di variabilità ai contenuti
        self.intro_templates = [
            "Una partita che promette emozioni",
            "Un incontro che attira l'attenzione",
            "Una sfida da non perdere",
            "Un match che si preannuncia avvincente",
            "Un confronto che potrebbe riservare sorprese"
        ]
        
        logger.info("ContentGenerator inizializzato con stile %s e lunghezza %s", 
                   self.content_style, self.preview_length)
    
    @cached(ttl=3600)  # Cache di 1 ora
    def generate_match_article(self, match_id: str, 
                              language: Optional[str] = None, 
                              format_type: str = 'markdown') -> Dict[str, Any]:
        """
        Genera un articolo completo per una partita specifica.
        
        Args:
            match_id: ID della partita
            language: Lingua dell'articolo (opzionale, usa il valore predefinito se non specificato)
            format_type: Formato dell'articolo ('markdown' o 'html')
            
        Returns:
            Dizionario con articolo generato e metadati
        """
        logger.info(f"Generando articolo per match_id={match_id}, lingua={language or self.article_language}")
        
        try:
            # Usa la lingua specificata o quella predefinita
            target_language = language or self.article_language
            
            # Ottieni dati partita
            match_ref = self.db.get_reference(f"data/matches/{match_id}")
            match_data = match_ref.get()
            
            if not match_data:
                logger.warning(f"Nessun dato trovato per match_id={match_id}")
                return self._create_empty_article(match_id, target_language)
            
            # Verifica che la partita non sia già conclusa
            if match_data.get('status') == 'FINISHED':
                logger.info(f"La partita {match_id} è già conclusa, generando articolo post-partita")
                return self.generate_post_match_article(match_id, language, format_type)
            
            # Arricchisci i dati della partita con analisi e previsioni
            enriched_data = self._enrich_match_data(match_data)
            
            # Genera le diverse sezioni dell'articolo
            preview_content = generate_match_preview(
                enriched_data, 
                length=self.preview_length, 
                style=self.content_style,
                language=target_language
            )
            
            prediction_content = generate_prediction(
                enriched_data, 
                include_value_bets=self.include_value_bets,
                language=target_language
            )
            
            stats_content = ""
            if self.include_stats:
                stats_content = generate_stats_analysis(
                    enriched_data, 
                    language=target_language
                )
            
            # Combina le sezioni
            content_sections = [
                preview_content,
                prediction_content
            ]
            
            if stats_content:
                content_sections.append(stats_content)
            
            # Aggiungi una conclusione
            conclusion = self._generate_conclusion(enriched_data, target_language)
            if conclusion:
                content_sections.append(conclusion)
            
            # Unisci le sezioni in un unico articolo
            raw_content = "\n\n".join(content_sections)
            
            # Formatta il contenuto nel formato richiesto
            if format_type.lower() == 'html':
                formatted_content = format_as_html(raw_content)
            else:  # markdown è il default
                formatted_content = format_as_markdown(raw_content)
            
            # Genera titolo e descrizione
            title = self._generate_title(enriched_data, target_language)
            description = self._generate_description(enriched_data, target_language)
            
            # Crea il risultato
            article = {
                'match_id': match_id,
                'title': title,
                'description': description,
                'content': formatted_content,
                'raw_content': raw_content,
                'format': format_type,
                'language': target_language,
                'home_team': match_data.get('home_team', ''),
                'away_team': match_data.get('away_team', ''),
                'league_id': match_data.get('league_id', ''),
                'match_datetime': match_data.get('datetime', ''),
                'generation_time': datetime.now().isoformat(),
                'status': 'generated'
            }
            
            logger.info(f"Articolo generato con successo per {match_id} ({len(formatted_content)} caratteri)")
            return article
            
        except Exception as e:
            logger.error(f"Errore nella generazione dell'articolo per match_id={match_id}: {e}")
            return self._create_empty_article(match_id, language or self.article_language)
    
    def _enrich_match_data(self, match_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Arricchisce i dati della partita con analisi e previsioni.
        
        Args:
            match_data: Dati della partita
            
        Returns:
            Dati arricchiti con analisi e previsioni
        """
        match_id = match_data.get('match_id', '')
        
        # Per evitare import circolari, importa qui
        from src.analytics.predictions.match_predictor import predict_match
        from src.analytics.predictions.value_finder import find_value_bets
        from src.analytics.predictions.trend_analyzer import analyze_match_trends
        from src.data.processors.head_to_head import get_head_to_head
        
        # Aggiungi previsioni
        try:
            prediction = predict_match(match_id)
            match_data['prediction'] = prediction
        except Exception as e:
            logger.warning(f"Errore nel recupero delle previsioni per {match_id}: {e}")
            match_data['prediction'] = {}
        
        # Aggiungi value bet
        try:
            value_bets = find_value_bets(match_id)
            match_data['value_bets'] = value_bets
        except Exception as e:
            logger.warning(f"Errore nel recupero delle value bet per {match_id}: {e}")
            match_data['value_bets'] = {}
        
        # Aggiungi tendenze
        try:
            trends = analyze_match_trends(match_id)
            match_data['trends'] = trends
        except Exception as e:
            logger.warning(f"Errore nel recupero delle tendenze per {match_id}: {e}")
            match_data['trends'] = {}
        
        # Aggiungi head-to-head
        try:
            home_team_id = match_data.get('home_team_id', '')
            away_team_id = match_data.get('away_team_id', '')
            if home_team_id and away_team_id:
                h2h = get_head_to_head(home_team_id, away_team_id)
                match_data['h2h'] = h2h
            else:
                match_data['h2h'] = {}
        except Exception as e:
            logger.warning(f"Errore nel recupero dei dati head-to-head per {match_id}: {e}")
            match_data['h2h'] = {}
        
        return match_data
    
    def _generate_title(self, match_data: Dict[str, Any], language: str) -> str:
        """
        Genera un titolo accattivante per l'articolo.
        
        Args:
            match_data: Dati arricchiti della partita
            language: Lingua dell'articolo
            
        Returns:
            Titolo dell'articolo
        """
        home_team = match_data.get('home_team', 'Home')
        away_team = match_data.get('away_team', 'Away')
        league_name = match_data.get('league_name', '')
        
        # Aggiunge il nome del campionato se disponibile
        league_suffix = f" - {league_name}" if league_name else ""
        
        # Titolo base
        base_title = f"{home_team} vs {away_team}{league_suffix}"
        
        # Se abbiamo una previsione, rendiamola più interessante
        if 'prediction' in match_data and match_data['prediction']:
            prediction = match_data['prediction']
            
            # Usa il pronostico principale se disponibile
            if 'main_prediction' in prediction and prediction['main_prediction']:
                main_pred = prediction['main_prediction']
                confidence = main_pred.get('confidence', '').lower()
                
                if language == 'it':
                    if confidence in ['alta', 'molto alta']:
                        return f"{home_team} vs {away_team}: analisi e pronostico{league_suffix}"
                    elif 'value bet' in main_pred.get('description', '').lower():
                        return f"{home_team} vs {away_team}: dove trovare valore nelle scommesse{league_suffix}"
                elif language == 'en':
                    if confidence in ['high', 'very high']:
                        return f"{home_team} vs {away_team}: Preview and Prediction{league_suffix}"
                    elif 'value bet' in main_pred.get('description', '').lower():
                        return f"{home_team} vs {away_team}: Finding Betting Value{league_suffix}"
                else:
                    # Per altre lingue, usa il titolo base
                    return base_title
        
        # Se non abbiamo dati di previsione specifici, usa un titolo generico
        if language == 'it':
            return f"{home_team} vs {away_team}: preview e pronostico{league_suffix}"
        elif language == 'en':
            return f"{home_team} vs {away_team}: Match Preview and Prediction{league_suffix}"
        else:
            # Per altre lingue, usa il titolo base
            return base_title
    
    def _generate_description(self, match_data: Dict[str, Any], language: str) -> str:
        """
        Genera una breve descrizione per l'articolo.
        
        Args:
            match_data: Dati arricchiti della partita
            language: Lingua dell'articolo
            
        Returns:
            Descrizione dell'articolo
        """
        home_team = match_data.get('home_team', 'Home')
        away_team = match_data.get('away_team', 'Away')
        
        # Usa una delle frasi introduttive casuali
        intro = random.choice(self.intro_templates)
        
        # Se abbiamo una previsione, usala nella descrizione
        if 'prediction' in match_data and match_data['prediction']:
            prediction = match_data['prediction']
            
            if 'main_prediction' in prediction and prediction['main_prediction']:
                main_pred = prediction['main_prediction']
                selection = main_pred.get('selection', '')
                market = main_pred.get('market', '')
                
                if language == 'it':
                    if market == 'match_winner':
                        if selection == '1':
                            return f"{intro} tra {home_team} e {away_team}. Analizziamo statistiche, quote e pronostichiamo la vittoria della squadra di casa."
                        elif selection == 'X':
                            return f"{intro} tra {home_team} e {away_team}. Analizziamo statistiche, quote e pronostichiamo un pareggio."
                        elif selection == '2':
                            return f"{intro} tra {home_team} e {away_team}. Analizziamo statistiche, quote e pronostichiamo la vittoria della squadra ospite."
                    elif market == 'btts':
                        if selection == 'Yes':
                            return f"{intro} tra {home_team} e {away_team}. Analizziamo statistiche, quote e ci aspettiamo che entrambe le squadre segnino."
                        else:
                            return f"{intro} tra {home_team} e {away_team}. Analizziamo statistiche, quote e pronostichiamo che almeno una squadra non segnerà."
                    elif market == 'over_under':
                        if 'Over' in selection:
                            return f"{intro} tra {home_team} e {away_team}. Analizziamo statistiche, quote e pronostichiamo una partita con molti gol."
                        else:
                            return f"{intro} tra {home_team} e {away_team}. Analizziamo statistiche, quote e pronostichiamo una partita con pochi gol."
                elif language == 'en':
                    if market == 'match_winner':
                        if selection == '1':
                            return f"{intro} between {home_team} and {away_team}. We analyze stats, odds and predict a home win."
                        elif selection == 'X':
                            return f"{intro} between {home_team} and {away_team}. We analyze stats, odds and predict a draw."
                        elif selection == '2':
                            return f"{intro} between {home_team} and {away_team}. We analyze stats, odds and predict an away win."
                    elif market == 'btts':
                        if selection == 'Yes':
                            return f"{intro} between {home_team} and {away_team}. We analyze stats, odds and expect both teams to score."
                        else:
                            return f"{intro} between {home_team} and {away_team}. We analyze stats, odds and predict at least one team will keep a clean sheet."
                    elif market == 'over_under':
                        if 'Over' in selection:
                            return f"{intro} between {home_team} and {away_team}. We analyze stats, odds and predict a high-scoring match."
                        else:
                            return f"{intro} between {home_team} and {away_team}. We analyze stats, odds and predict a low-scoring match."
        
        # Descrizione generica se non abbiamo previsioni specifiche
        if language == 'it':
            return f"Anteprima e pronostico per la partita {home_team} vs {away_team}. Analizziamo statistiche, tendenze recenti e quote per aiutarti a scommettere."
        elif language == 'en':
            return f"Preview and prediction for {home_team} vs {away_team}. We analyze stats, recent trends and odds to help you bet."
        else:
            # Per altre lingue, usa una descrizione generica
            return f"{home_team} vs {away_team} - Match Preview"
    
    def _generate_conclusion(self, match_data: Dict[str, Any], language: str) -> str:
        """
        Genera una conclusione per l'articolo.
        
        Args:
            match_data: Dati arricchiti della partita
            language: Lingua dell'articolo
            
        Returns:
            Conclusione dell'articolo
        """
        home_team = match_data.get('home_team', 'Home')
        away_team = match_data.get('away_team', 'Away')
        
        # Ottieni il pronostico principale
        main_prediction = None
        if 'prediction' in match_data and match_data['prediction']:
            prediction = match_data['prediction']
            if 'main_prediction' in prediction:
                main_prediction = prediction['main_prediction']
        
        # Genera conclusione in base alla lingua
        if language == 'it':
            conclusion = "## Conclusione\n\n"
            
            if main_prediction:
                selection = main_prediction.get('selection', '')
                market = main_prediction.get('market', '')
                description = main_prediction.get('description', '')
                
                if market == 'match_winner':
                    if selection == '1':
                        conclusion += f"Basandoci sui dati analizzati, riteniamo che **{home_team}** abbia buone possibilità di vincere questa partita. "
                    elif selection == 'X':
                        conclusion += f"La nostra analisi suggerisce che la partita tra **{home_team}** e **{away_team}** potrebbe concludersi con un pareggio. "
                    elif selection == '2':
                        conclusion += f"I dati indicano che **{away_team}** ha buone possibilità di ottenere i tre punti in questa trasferta. "
                elif market == 'btts':
                    if selection == 'Yes':
                        conclusion += f"La nostra analisi suggerisce che sia **{home_team}** che **{away_team}** hanno buone probabilità di segnare in questa partita. "
                    else:
                        conclusion += f"Basandoci sui dati, prevediamo che almeno una tra **{home_team}** e **{away_team}** manterrà la porta inviolata. "
                elif market == 'over_under':
                    line = main_prediction.get('line', '2.5')
                    if 'Over' in selection:
                        conclusion += f"Le statistiche suggeriscono che ci saranno più di {line} gol in questa partita tra **{home_team}** e **{away_team}**. "
                    else:
                        conclusion += f"I dati indicano che ci saranno meno di {line} gol in questa partita tra **{home_team}** e **{away_team}**. "
                
                if description:
                    conclusion += f"{description} "
            else:
                conclusion += f"La partita tra **{home_team}** e **{away_team}** si preannuncia interessante e combattuta. "
            
            conclusion += "Ricordiamo che il calcio è imprevedibile e che i pronostici vanno presi come suggerimenti basati su dati statistici. "
            conclusion += "Buona visione e buona fortuna con le vostre scommesse!\n"
            
        elif language == 'en':
            conclusion = "## Conclusion\n\n"
            
            if main_prediction:
                selection = main_prediction.get('selection', '')
                market = main_prediction.get('market', '')
                description = main_prediction.get('description', '')
                
                if market == 'match_winner':
                    if selection == '1':
                        conclusion += f"Based on our analysis, we believe **{home_team}** has a good chance of winning this match. "
                    elif selection == 'X':
                        conclusion += f"Our analysis suggests that the match between **{home_team}** and **{away_team}** could end in a draw. "
                    elif selection == '2':
                        conclusion += f"The data indicates that **{away_team}** has a good chance of securing all three points in this away fixture. "
                elif market == 'btts':
                    if selection == 'Yes':
                        conclusion += f"Our analysis suggests that both **{home_team}** and **{away_team}** have a good chance of scoring in this match. "
                    else:
                        conclusion += f"Based on the data, we predict that at least one of **{home_team}** or **{away_team}** will keep a clean sheet. "
                elif market == 'over_under':
                    line = main_prediction.get('line', '2.5')
                    if 'Over' in selection:
                        conclusion += f"The statistics suggest there will be more than {line} goals in this match between **{home_team}** and **{away_team}**. "
                    else:
                        conclusion += f"The data indicates there will be fewer than {line} goals in this match between **{home_team}** and **{away_team}**. "
                
                if description:
                    conclusion += f"{description} "
            else:
                conclusion += f"The match between **{home_team}** and **{away_team}** promises to be interesting and competitive. "
            
            conclusion += "Remember that football is unpredictable and predictions should be taken as suggestions based on statistical data. "
            conclusion += "Enjoy the match and good luck with your bets!\n"
        
        else:
            # Per altre lingue, non genera conclusione
            conclusion = ""
        
        return conclusion
    
    def _create_empty_article(self, match_id: str, language: str) -> Dict[str, Any]:
        """
        Crea un articolo vuoto quando non ci sono dati sufficienti.
        
        Args:
            match_id: ID della partita
            language: Lingua dell'articolo
            
        Returns:
            Articolo vuoto
        """
        if language == 'it':
            content = "Dati insufficienti per generare un articolo completo. Si prega di riprovare più tardi."
            title = "Anteprima partita non disponibile"
            description = "Dati insufficienti per generare l'anteprima della partita"
        elif language == 'en':
            content = "Insufficient data to generate a complete article. Please try again later."
            title = "Match preview not available"
            description = "Insufficient data to generate the match preview"
        else:
            content = "Insufficient data to generate a complete article. Please try again later."
            title = "Match preview not available"
            description = "Insufficient data to generate the match preview"
        
        return {
            'match_id': match_id,
            'title': title,
            'description': description,
            'content': content,
            'raw_content': content,
            'format': 'markdown',
            'language': language,
            'home_team': '',
            'away_team': '',
            'league_id': '',
            'match_datetime': '',
            'generation_time': datetime.now().isoformat(),
            'status': 'error'
        }
    
    def generate_post_match_article(self, match_id: str, 
                                  language: Optional[str] = None, 
                                  format_type: str = 'markdown') -> Dict[str, Any]:
        """
        Genera un articolo post-partita.
        
        Args:
            match_id: ID della partita
            language: Lingua dell'articolo (opzionale, usa il valore predefinito se non specificato)
            format_type: Formato dell'articolo ('markdown' o 'html')
            
        Returns:
            Dizionario con articolo post-partita generato e metadati
        """
        logger.info(f"Generando articolo post-partita per match_id={match_id}, lingua={language or self.article_language}")
        
        try:
            # Usa la lingua specificata o quella predefinita
            target_language = language or self.article_language
            
            # Ottieni dati partita
            match_ref = self.db.get_reference(f"data/matches/{match_id}")
            match_data = match_ref.get()
            
            if not match_data:
                logger.warning(f"Nessun dato trovato per match_id={match_id}")
                return self._create_empty_article(match_id, target_language)
            
            # Verifica che la partita sia conclusa
            if match_data.get('status') != 'FINISHED':
                logger.warning(f"La partita {match_id} non è ancora conclusa")
                return self._create_empty_article(match_id, target_language)
            
            # Implementazione di base per articolo post-partita
            # In una versione più completa, si potrebbe generare un'analisi dettagliata della partita
            
            home_team = match_data.get('home_team', 'Home')
            away_team = match_data.get('away_team', 'Away')
            home_score = match_data.get('home_score', 0)
            away_score = match_data.get('away_score', 0)
            
            # Genera un titolo basato sul risultato
            if target_language == 'it':
                if home_score > away_score:
                    title = f"{home_team} batte {away_team} {home_score}-{away_score}: analisi post-partita"
                elif away_score > home_score:
                    title = f"{away_team} batte {home_team} {away_score}-{home_score}: analisi post-partita"
                else:
                    title = f"{home_team} e {away_team} pareggiano {home_score}-{away_score}: analisi post-partita"
            elif target_language == 'en':
                if home_score > away_score:
                    title = f"{home_team} beats {away_team} {home_score}-{away_score}: Post-Match Analysis"
                elif away_score > home_score:
                    title = f"{away_team} beats {home_team} {away_score}-{home_score}: Post-Match Analysis"
                else:
                    title = f"{home_team} and {away_team} draw {home_score}-{away_score}: Post-Match Analysis"
            else:
                title = f"{home_team} {home_score}-{away_score} {away_team}: Post-Match Analysis"
            
            # Genera un contenuto semplice
            if target_language == 'it':
                content = f"""
# {title}

## Risultato Finale: {home_team} {home_score}-{away_score} {away_team}

La partita tra {home_team} e {away_team} si è conclusa con il risultato di {home_score}-{away_score}.

### Analisi Risultato
"""
                if home_score > away_score:
                    content += f"\n{home_team} ha ottenuto una vittoria meritata contro {away_team}."
                elif away_score > home_score:
                    content += f"\n{away_team} è riuscito a conquistare i tre punti in trasferta contro {home_team}."
                else:
                    content += f"\nLa partita tra {home_team} e {away_team} si è conclusa in parità."
                
                content += "\n\nMaggiori dettagli sulle statistiche della partita saranno disponibili a breve."
                
            elif target_language == 'en':
                content = f"""
# {title}

## Final Score: {home_team} {home_score}-{away_score} {away_team}

The match between {home_team} and {away_team} ended with a score of {home_score}-{away_score}.

### Result Analysis
"""
                if home_score > away_score:
                    content += f"\n{home_team} earned a deserved victory against {away_team}."
                elif away_score > home_score:
                    content += f"\n{away_team} managed to secure all three points away against {home_team}."
                else:
                    content += f"\nThe match between {home_team} and {away_team} ended in a draw."
                
                content += "\n\nMore details on the match statistics will be available soon."
            else:
                content = f"""
# {home_team} {home_score}-{away_score} {away_team}: Post-Match Analysis

The match has ended. Full analysis will be available soon.
"""
            
            # Formatta il contenuto nel formato richiesto
            if format_type.lower() == 'html':
                formatted_content = format_as_html(content)
            else:  # markdown è il default
                formatted_content = format_as_markdown(content)
            
            # Genera descrizione
            if target_language == 'it':
                description = f"Analisi post-partita di {home_team} vs {away_team}, conclusasi {home_score}-{away_score}."
            elif target_language == 'en':
                description = f"Post-match analysis of {home_team} vs {away_team}, which ended {home_score}-{away_score}."
            else:
                description = f"{home_team} {home_score}-{away_score} {away_team}: Post-Match Analysis"
            
            # Crea il risultato
            article = {
                'match_id': match_id,
                'title': title,
                'description': description,
                'content': formatted_content,
                'raw_content': content,
                'format': format_type,
                'language': target_language,
                'home_team': home_team,
                'away_team': away_team,
                'home_score': home_score,
                'away_score': away_score,
                'league_id': match_data.get('league_id', ''),
                'match_datetime': match_data.get('datetime', ''),
                'generation_time': datetime.now().isoformat(),
                'status': 'generated',
                'type': 'post_match'
            }
            
            logger.info(f"Articolo post-partita generato con successo per {match_id}")
            return article
            
        except Exception as e:
            logger.error(f"Errore nella generazione dell'articolo post-partita per match_id={match_id}: {e}")
            return self._create_empty_article(match_id, language or self.article_language)
    
    @cached(ttl=3600*6)  # Cache di 6 ore
    def generate_multiple_articles(self, league_id: Optional[str] = None,
                                 limit: int = 10,
                                 language: Optional[str] = None,
                                 format_type: str = 'markdown') -> List[Dict[str, Any]]:
        """
        Genera articoli per più partite di un campionato.
        
        Args:
            league_id: ID del campionato (opzionale, se None genera per tutti i campionati)
            limit: Numero massimo di articoli da generare
            language: Lingua degli articoli (opzionale, usa il valore predefinito se non specificato)
            format_type: Formato degli articoli ('markdown' o 'html')
            
        Returns:
            Lista di articoli generati
        """
        logger.info(f"Generando {limit} articoli per league_id={league_id}, lingua={language or self.article_language}")
        
        try:
            # Ottieni le partite in programma
            matches_ref = self.db.get_reference("data/matches")
            
            # In una implementazione più avanzata, useremmo un indice per filtrare per data
            # Per ora, recuperiamo tutte le partite e filtriamo lato client
            all_matches = matches_ref.get()
            
            if not all_matches:
                logger.warning("Nessuna partita trovata")
                return []
            
            # Filtra le partite
            upcoming_matches = []
            now = datetime.now()
            cutoff_time = now + timedelta(hours=12)  # Partite nelle prossime 12 ore
            
            for match_id, match in all_matches.items():
                # Filtra per campionato se specificato
                if league_id and match.get('league_id') != league_id:
                    continue
                
                # Considera solo partite non ancora iniziate
                if match.get('status') == 'FINISHED':
                    continue
                
                # Verifica che la partita sia imminente
                match_time = match.get('datetime', '')
                if not match_time:
                    continue
                
                try:
                    match_dt = datetime.fromisoformat(match_time.replace('Z', '+00:00'))
                    if match_dt > cutoff_time:
                        continue
                except (ValueError, TypeError):
                    continue
                
                # Aggiungi alla lista
                match['match_id'] = match_id
                upcoming_matches.append(match)
            
            # Ordina per data (prima le più imminenti)
            upcoming_matches.sort(key=lambda x: x.get('datetime', ''))
            
            # Limita al numero richiesto
            upcoming_matches = upcoming_matches[:limit]
            
            if not upcoming_matches:
                logger.warning("Nessuna partita imminente trovata")
                return []
            
            # Genera articoli per ogni partita
            articles = []
            for match in upcoming_matches:
                try:
                    article = self.generate_match_article(
                        match['match_id'],
                        language=language,
                        format_type=format_type
                    )
                    articles.append(article)
                except Exception as e:
                    logger.error(f"Errore nella generazione dell'articolo per {match['match_id']}: {e}")
            
            logger.info(f"Generati {len(articles)} articoli su {len(upcoming_matches)} partite")
            return articles
            
        except Exception as e:
            logger.error(f"Errore nella generazione di articoli multipli: {e}")
            return []
    
    def save_article(self, article: Dict[str, Any]) -> bool:
        """
        Salva un articolo generato nel database.
        
        Args:
            article: Articolo da salvare
            
        Returns:
            True se salvato con successo, False altrimenti
        """
        try:
            match_id = article.get('match_id', '')
            if not match_id:
                logger.error("Impossibile salvare articolo senza match_id")
                return False
            
            article_id = f"{match_id}_{article.get('language', 'en')}"
            
            articles_ref = self.db.get_reference(f"content/articles/{article_id}")
            articles_ref.set(article)
            
            logger.info(f"Articolo salvato con successo: {article_id}")
            return True
        except Exception as e:
            logger.error(f"Errore nel salvataggio dell'articolo: {e}")
            return False

# Funzioni di utilità per accesso globale
def generate_match_article(match_id: str, language: Optional[str] = None, format_type: str = 'markdown') -> Dict[str, Any]:
    """
    Genera un articolo completo per una partita specifica.
    
    Args:
        match_id: ID della partita
        language: Lingua dell'articolo (opzionale)
        format_type: Formato dell'articolo ('markdown' o 'html')
        
    Returns:
        Dizionario con articolo generato e metadati
    """
    generator = ContentGenerator()
    return generator.generate_match_article(match_id, language, format_type)

def generate_multiple_articles(league_id: Optional[str] = None, limit: int = 10,
                              language: Optional[str] = None, format_type: str = 'markdown') -> List[Dict[str, Any]]:
    """
    Genera articoli per più partite di un campionato.
    
    Args:
        league_id: ID del campionato (opzionale)
        limit: Numero massimo di articoli da generare
        language: Lingua degli articoli (opzionale)
        format_type: Formato degli articoli ('markdown' o 'html')
        
    Returns:
        Lista di articoli generati
    """
    generator = ContentGenerator()
    return generator.generate_multiple_articles(league_id, limit, language, format_type)

def save_article(article: Dict[str, Any]) -> bool:
    """
    Salva un articolo generato nel database.
    
    Args:
        article: Articolo da salvare
        
    Returns:
        True se salvato con successo, False altrimenti
    """
    generator = ContentGenerator()
    return generator.save_article(article)
