#!/usr/bin/env python3
"""
Generate Articles - Creates prediction articles for upcoming matches
Uses collected data to generate detailed analysis and forecasts in English
"""
import os
import sys
import json
import logging
import random
import math
from datetime import datetime, timedelta
import firebase_admin
from firebase_admin import credentials, db
from dotenv import load_dotenv

# Logging configuration
log_dir = os.path.expanduser('~/football-predictions/logs')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"generate_articles_{datetime.now().strftime('%Y%m%d')}.log")

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Global variables
load_dotenv()

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

def get_matches_to_generate():
    """Get matches for which articles need to be generated"""
    matches_to_process = []
    
    # Database reference
    matches_ref = db.reference('matches')
    articles_ref = db.reference('articles')
    
    # Get matches for the next 3 days
    today = datetime.now().date()
    now = datetime.now()
    
    for i in range(3):
        date_str = (today + timedelta(days=i)).strftime('%Y-%m-%d')
        daily_matches = matches_ref.child(date_str).get() or {}
        
        for match_id, match_data in daily_matches.items():
            # Skip if article already generated
            if articles_ref.child(match_id).get():
                continue
            
            # Check publishing window (8-6 hours before)
            match_time = datetime.fromisoformat(match_data['utc_date'].replace('Z', '+00:00'))
            publish_window_start = match_time - timedelta(hours=int(os.getenv('PUBLISH_WINDOW_START', 8)))
            publish_window_end = match_time - timedelta(hours=int(os.getenv('PUBLISH_WINDOW_END', 6)))
            
            # If we're in the publishing window or approaching it (1 hour advance)
            if now >= (publish_window_start - timedelta(hours=1)) and now <= publish_window_end:
                matches_to_process.append(match_data)
    
    # Sort by date (closest first)
    matches_to_process.sort(key=lambda x: x['utc_date'])
    
    logging.info(f"Found {len(matches_to_process)} matches to process for articles")
    return matches_to_process

def get_team_stats(team_id):
    """Get team statistics"""
    stats_ref = db.reference(f'team_stats/{team_id}')
    stats = stats_ref.get()
    
    # If no statistics, generate simulated data
    if not stats:
        team_ref = db.reference(f'teams/{team_id}')
        team_data = team_ref.get() or {'name': 'Unknown Team'}
        
        # Simulated data
        stats = {
            'id': team_id,
            'name': team_data.get('name', 'Unknown'),
            'form': ''.join(random.choices(['W', 'D', 'L'], k=5)),
            'form_stats': {
                'wins': random.randint(1, 3),
                'draws': random.randint(0, 2),
                'losses': random.randint(0, 2)
            },
            'goals_stats': {
                'scored': random.randint(5, 10),
                'conceded': random.randint(3, 8),
                'per_match_scored': round(random.uniform(1.0, 2.0), 1),
                'per_match_conceded': round(random.uniform(0.8, 1.5), 1)
            },
            'advanced_stats': {
                'possession': random.randint(40, 60),
                'shots_per_game': round(random.uniform(8, 16), 1),
                'shots_on_target': round(random.uniform(3, 7), 1),
                'xG': round(random.uniform(1.0, 2.2), 2),
                'corners_per_game': round(random.uniform(4, 8), 1),
                'fouls_per_game': round(random.uniform(8, 14), 1)
            },
            'league_position': {
                'position': random.randint(1, 20),
                'played': random.randint(10, 30),
                'points': random.randint(10, 80)
            }
        }
    
    return stats

def get_h2h_data(match_id):
    """Get head-to-head data"""
    h2h_ref = db.reference(f'h2h/{match_id}')
    h2h_data = h2h_ref.get()
    
    # If no data, return an empty object
    if not h2h_data:
        return {
            'total_matches': 0,
            'home_wins': 0,
            'away_wins': 0,
            'draws': 0,
            'matches': []
        }
    
    return h2h_data

def generate_match_context(match_data, home_stats, away_stats):
    """Generate paragraph about the match context"""
    home_team = match_data['home_team']
    away_team = match_data['away_team']
    competition = match_data['competition']
    
    # Get league positions if available
    home_position = home_stats.get('league_position', {}).get('position', random.randint(1, 20))
    away_position = away_stats.get('league_position', {}).get('position', random.randint(1, 20))
    
    # Introduction templates
    intros = [
        f"This {competition} match sees {home_team} ({home_position}th in the table) hosting {away_team} ({away_position}th in the table) in a game that could have significant implications for both teams.",
        
        f"{home_team} welcomes {away_team} in this anticipated {competition} match, with both teams seeking important points in their quest to achieve their respective seasonal goals.",
        
        f"The clash between {home_team} and {away_team} promises to be exciting, with the two teams separated by {abs(home_position - away_position)} points in the standings. Expectations are high for this {competition} encounter."
    ]
    
    # Add match significance
    importance_text = ""
    position_diff = abs(home_position - away_position)
    
    if position_diff <= 3:
        importance_text = f"This direct confrontation is crucial for the standings, with only {position_diff} points separating the two teams. A win could overturn the balance in this area of the table."
    elif position_diff <= 8:
        importance_text = f"Despite the difference of {position_diff} positions in the table, both teams have much to gain from this clash, which could significantly influence their respective seasons."
    else:
        importance_text = f"Despite the gap of {position_diff} positions in the rankings, this match represents an important opportunity for both sides: {home_team} seeks to defend their home advantage, while {away_team} wants to upset the odds."
    
    # Compose the full paragraph
    context = random.choice(intros) + " " + importance_text
    
    return context

def generate_form_paragraph(home_stats, away_stats):
    """Generate paragraph about the recent form of both teams"""
    home_team = home_stats['name']
    away_team = away_stats['name']
    
    # Form analysis
    home_form = home_stats.get('form', 'DDDDD')
    away_form = away_stats.get('form', 'DDDDD')
    
    home_wins = home_stats.get('form_stats', {}).get('wins', 0)
    home_draws = home_stats.get('form_stats', {}).get('draws', 0)
    home_losses = home_stats.get('form_stats', {}).get('losses', 0)
    
    away_wins = away_stats.get('form_stats', {}).get('wins', 0)
    away_draws = away_stats.get('form_stats', {}).get('draws', 0)
    away_losses = away_stats.get('form_stats', {}).get('losses', 0)
    
    # Describe each team's form
    if home_wins >= 3:
        home_form_desc = f"{home_team} is going through an excellent period with {home_wins} wins in their last 5 matches"
    elif home_wins >= 2 and home_draws >= 2:
        home_form_desc = f"{home_team} comes to this match in positive form but with a few too many draws ({home_draws} in the last 5)"
    elif home_losses >= 3:
        home_form_desc = f"{home_team} arrives at this match in a difficult moment, with {home_losses} defeats in their last 5 games"
    else:
        home_form_desc = f"{home_team} shows inconsistent form with {home_wins} wins, {home_draws} draws and {home_losses} losses recently"
    
    if away_wins >= 3:
        away_form_desc = f"{away_team} is in great form with {away_wins} successes in their last 5 matches"
    elif away_wins >= 2 and away_draws >= 2:
        away_form_desc = f"{away_team} is showing solidity with {away_wins} wins and {away_draws} recent draws"
    elif away_losses >= 3:
        away_form_desc = f"{away_team} is not going through a good moment, having lost {away_losses} of their last 5 matches"
    else:
        away_form_desc = f"{away_team} presents mixed results: {away_wins} wins, {away_draws} draws and {away_losses} losses in their recent outings"
    
    # Add goal details
    home_form_desc += f". The team scores an average of {home_stats.get('goals_stats', {}).get('per_match_scored', 1.0)} goals and concedes {home_stats.get('goals_stats', {}).get('per_match_conceded', 1.0)} per match."
    away_form_desc += f". Their offensive performance is {away_stats.get('goals_stats', {}).get('per_match_scored', 1.0)} goals scored per match, with {away_stats.get('goals_stats', {}).get('per_match_conceded', 1.0)} conceded."
    
    # Compose full paragraph
    form_paragraph = f"{home_form_desc} {away_form_desc}"
    
    return form_paragraph

def generate_h2h_paragraph(match_data, h2h_data):
    """Generate paragraph about head-to-head statistics"""
    home_team = match_data['home_team']
    away_team = match_data['away_team']
    
    total_matches = h2h_data.get('total_matches', 0)
    home_wins = h2h_data.get('home_wins', 0)
    away_wins = h2h_data.get('away_wins', 0)
    draws = h2h_data.get('draws', 0)
    
    if total_matches == 0:
        return f"This will be the first official match between {home_team} and {away_team}. Both teams will look to establish supremacy in this new confrontation."
    
    # General h2h description
    h2h_desc = f"In the last {total_matches} head-to-head encounters, {home_team} has won {home_wins} times, while {away_team} has prevailed on {away_wins} occasions, with {draws} draws."
    
    # If there are recent matches, details on the latest
    if 'matches' in h2h_data and len(h2h_data['matches']) > 0:
        last_match = h2h_data['matches'][0]
        date = datetime.fromisoformat(last_match['utc_date'].replace('Z', '+00:00')).strftime('%d/%m/%Y')
        
        if last_match['home_team'] == home_team:
            last_score = f"{last_match['score']['home']}-{last_match['score']['away']}"
            if last_match['winner'] == 'HOME_TEAM':
                last_result = f"a win for {home_team}"
            elif last_match['winner'] == 'AWAY_TEAM':
                last_result = f"a win for {away_team}"
            else:
                last_result = "a draw"
        else:
            last_score = f"{last_match['score']['away']}-{last_match['score']['home']}"
            if last_match['winner'] == 'HOME_TEAM':
                last_result = f"a win for {away_team}"
            elif last_match['winner'] == 'AWAY_TEAM':
                last_result = f"a win for {home_team}"
            else:
                last_result = "a draw"
        
        h2h_desc += f" The last encounter on {date} ended with {last_result} ({last_score})."
    
    # Add trend
    if home_wins > away_wins + 2:
        h2h_desc += f" Recent history favors {home_team}, who has dominated this matchup."
    elif away_wins > home_wins + 2:
        h2h_desc += f" {away_team} has shown clear superiority in this confrontation in recent meetings."
    elif draws >= max(home_wins, away_wins):
        h2h_desc += " The two teams tend to neutralize each other, as evidenced by the numerous draws."
    else:
        h2h_desc += " Balance characterizes this confrontation, with both teams capable of imposing themselves."
    
    return h2h_desc

def generate_key_players(match_data, home_stats, away_stats):
    """Generate paragraph about key players (simulated)"""
    home_team = match_data['home_team']
    away_team = match_data['away_team']
    
    # Name dictionaries for simulation
    first_names = ["Marco", "Luca", "Andrea", "Alessandro", "Roberto", "James", "Kevin", "Mohamed", "David", "Cristiano", "Lionel", "Kylian", "Robert", "Karim"]
    last_names = ["Rossi", "Bianchi", "Ferrari", "Smith", "Johnson", "Williams", "Martinez", "Davies", "Garcia", "Rodriguez", "Müller", "Hernandez", "Kovalenko"]
    
    # Generate key players
    home_players = []
    away_players = []
    
    # Generate 2 players per team with seed based on team name for consistency
    random.seed(hash(home_team))
    for _ in range(2):
        home_player = {
            'name': f"{random.choice(first_names)} {random.choice(last_names)}",
            'goals': random.randint(3, 15),
            'assists': random.randint(1, 8),
            'key_stat': random.choice([
                "key passes", 
                "shots on target", 
                "successful tackles",
                "completed dribbles",
                "interceptions"
            ])
        }
        home_players.append(home_player)
    
    random.seed(hash(away_team))
    for _ in range(2):
        away_player = {
            'name': f"{random.choice(first_names)} {random.choice(last_names)}",
            'goals': random.randint(3, 15),
            'assists': random.randint(1, 8),
            'key_stat': random.choice([
                "key passes", 
                "shots on target", 
                "successful tackles",
                "completed dribbles",
                "interceptions"
            ])
        }
        away_players.append(away_player)
    
    # Generate paragraph
    players_text = f"For {home_team}, attention should be paid to {home_players[0]['name']} ({home_players[0]['goals']} goals, {home_players[0]['assists']} assists), who has distinguished himself for the number of {home_players[0]['key_stat']} this season. Also {home_players[1]['name']} ({home_players[1]['goals']} goals, {home_players[1]['assists']} assists) could be decisive.\n\n"
    
    players_text += f"On {away_team}'s side, {away_players[0]['name']} ({away_players[0]['goals']} goals, {away_players[0]['assists']} assists) represents the main offensive threat, supported by {away_players[1]['name']} ({away_players[1]['goals']} goals, {away_players[1]['assists']} assists) who excels in {away_players[1]['key_stat']}."
    
    return players_text

def generate_prediction(home_stats, away_stats, h2h_data):
    """Generate prediction based on statistics"""
    # Calculate prediction factors
    home_attack = home_stats.get('goals_stats', {}).get('per_match_scored', 1.0) * 0.8 + home_stats.get('advanced_stats', {}).get('xG', 1.5) * 1.2
    home_defense = (2.5 - home_stats.get('goals_stats', {}).get('per_match_conceded', 1.0)) * 0.7
    
    away_attack = away_stats.get('goals_stats', {}).get('per_match_scored', 1.0) * 0.7 + away_stats.get('advanced_stats', {}).get('xG', 1.5) * 1.1
    away_defense = (2.5 - away_stats.get('goals_stats', {}).get('per_match_conceded', 1.0)) * 0.6
    
    # Consider recent form
    home_form_factor = (home_stats.get('form_stats', {}).get('wins', 2) * 0.2) - (home_stats.get('form_stats', {}).get('losses', 1) * 0.1)
    away_form_factor = (away_stats.get('form_stats', {}).get('wins', 2) * 0.15) - (away_stats.get('form_stats', {}).get('losses', 1) * 0.1)
    
    # Home field advantage
    home_advantage = 0.3
    
    # Consider h2h (if available)
    h2h_factor = 0
    if h2h_data.get('total_matches', 0) > 0:
        h2h_home_ratio = h2h_data.get('home_wins', 0) / h2h_data.get('total_matches', 1)
        h2h_away_ratio = h2h_data.get('away_wins', 0) / h2h_data.get('total_matches', 1)
        h2h_factor = (h2h_home_ratio - h2h_away_ratio) * 0.2
    
    # Calculate overall strength
    home_strength = home_attack + home_defense + home_form_factor + home_advantage + h2h_factor
    away_strength = away_attack + away_defense + away_form_factor
    
    total_strength = home_strength + away_strength
    
    # Calculate probabilities
    home_win_prob = round((home_strength / total_strength) * 0.8, 2)
    away_win_prob = round((away_strength / total_strength) * 0.8, 2)
    draw_prob = round(1 - home_win_prob - away_win_prob, 2)
    
    # Ensure sum is 1
    total_prob = home_win_prob + draw_prob + away_win_prob
    if total_prob != 1:
        correction = (1 - total_prob) / 3
        home_win_prob = round(home_win_prob + correction, 2)
        away_win_prob = round(away_win_prob + correction, 2)
        draw_prob = round(1 - home_win_prob - away_win_prob, 2)
    
    # Calculate odds (1/prob)
    home_odds = round(1 / max(home_win_prob, 0.05), 2)
    draw_odds = round(1 / max(draw_prob, 0.05), 2)
    away_odds = round(1 / max(away_win_prob, 0.05), 2)
    
    # Predict number of goals
    expected_goals = home_stats.get('advanced_stats', {}).get('xG', 1.5) + away_stats.get('advanced_stats', {}).get('xG', 1.5)
    over_2_5_prob = round(0.5 + (expected_goals - 2.5) * 0.15, 2)
    over_2_5_prob = max(0.1, min(0.9, over_2_5_prob))
    under_2_5_prob = round(1 - over_2_5_prob, 2)
    
    over_odds = round(1 / over_2_5_prob, 2)
    under_odds = round(1 / under_2_5_prob, 2)
    
    # Specials
    btts_prob = round(0.5 + (home_stats.get('advanced_stats', {}).get('xG', 1.5) * 0.1) + (away_stats.get('advanced_stats', {}).get('xG', 1.5) * 0.1), 2)
    btts_prob = max(0.1, min(0.9, btts_prob))
    btts_odds = round(1 / btts_prob, 2)
    
    # Final prediction
    predictions = {
        'match_result': {
            '1': {'probability': home_win_prob, 'odds': home_odds},
            'X': {'probability': draw_prob, 'odds': draw_odds},
            '2': {'probability': away_win_prob, 'odds': away_odds}
        },
        'goals': {
            'over_2_5': {'probability': over_2_5_prob, 'odds': over_odds},
            'under_2_5': {'probability': under_2_5_prob, 'odds': under_odds}
        },
        'specials': {
            'btts': {'probability': btts_prob, 'odds': btts_odds}
        },
        'recommended_bets': []
    }
    
    # Determine recommended bets
    if home_win_prob > 0.5 and home_odds >= 1.5:
        predictions['recommended_bets'].append({'type': '1', 'odds': home_odds, 'confidence': 'high'})
    elif away_win_prob > 0.45 and away_odds >= 1.8:
        predictions['recommended_bets'].append({'type': '2', 'odds': away_odds, 'confidence': 'high'})
    elif draw_prob > 0.3 and draw_odds >= 2.8:
        predictions['recommended_bets'].append({'type': 'X', 'odds': draw_odds, 'confidence': 'medium'})
    else:
        # Find highest probability
        max_prob = max(home_win_prob, draw_prob, away_win_prob)
        if max_prob == home_win_prob:
            predictions['recommended_bets'].append({'type': '1', 'odds': home_odds, 'confidence': 'medium'})
        elif max_prob == away_win_prob:
            predictions['recommended_bets'].append({'type': '2', 'odds': away_odds, 'confidence': 'medium'})
        else:
            predictions['recommended_bets'].append({'type': 'X', 'odds': draw_odds, 'confidence': 'medium'})
    
    # Add over/under predictions
    if over_2_5_prob > 0.6:
        predictions['recommended_bets'].append({'type': 'Over 2.5', 'odds': over_odds, 'confidence': 'high'})
    elif under_2_5_prob > 0.6:
        predictions['recommended_bets'].append({'type': 'Under 2.5', 'odds': under_odds, 'confidence': 'high'})
    
    # Add BTTS if high probability
    if btts_prob > 0.65:
        predictions['recommended_bets'].append({'type': 'BTTS', 'odds': btts_odds, 'confidence': 'high'})
    
    return predictions

def format_prediction_text(predictions, home_team, away_team):
    """Format prediction text"""
    # Introductory paragraph
    probs = predictions['match_result']
    highest_prob = max(probs, key=lambda k: probs[k]['probability'])
    
    if highest_prob == '1':
        pred_intro = f"Statistical analysis indicates that {home_team} is favored in this match."
    elif highest_prob == '2':
        pred_intro = f"Statistics suggest that {away_team} has good chances of earning points away from home."
    else:
        pred_intro = f"This match looks balanced, with concrete possibilities for a draw."
    
    # Probability details
    home_prob = f"{int(probs['1']['probability'] * 100)}%"
    draw_prob = f"{int(probs['X']['probability'] * 100)}%"
    away_prob = f"{int(probs['2']['probability'] * 100)}%"
    
    prob_detail = f"The calculated probabilities are: {home_team} win {home_prob} (odds {probs['1']['odds']}), draw {draw_prob} (odds {probs['X']['odds']}), {away_team} win {away_prob} (odds {probs['2']['odds']})."
    
    # Goals details
    goals = predictions['goals']
    over_prob = f"{int(goals['over_2_5']['probability'] * 100)}%"
    under_prob = f"{int(goals['under_2_5']['probability'] * 100)}%"
    
    goals_detail = f"Regarding goals, the probability of Over 2.5 is {over_prob} (odds {goals['over_2_5']['odds']}), while Under 2.5 is {under_prob} (odds {goals['under_2_5']['odds']})."
    
    # Recommended bets
    rec_bets = predictions['recommended_bets']
    if len(rec_bets) > 0:
        rec_text = "Recommended bets:\n"
        for bet in rec_bets:
            confidence = "⭐⭐⭐" if bet['confidence'] == 'high' else "⭐⭐"
            rec_text += f"- {bet['type']} (odds {bet['odds']}) - Confidence: {confidence}\n"
    else:
        rec_text = "There are no predictions with sufficient confidence for this match."
    
    # Compose full text
    prediction_text = f"{pred_intro}\n\n{prob_detail}\n\n{goals_detail}\n\n{rec_text}"
    
    return prediction_text

def create_article(match_data):
    """Create complete article for a match"""
    # Get necessary data
    home_team_id = match_data['home_team_id']
    away_team_id = match_data['away_team_id']
    match_id = match_data['id']
    
    # Get team statistics
    home_stats = get_team_stats(home_team_id)
    away_stats = get_team_stats(away_team_id)
    
    # Get h2h data
    h2h_data = get_h2h_data(match_id)
    
    # Generate paragraphs
    context_paragraph = generate_match_context(match_data, home_stats, away_stats)
    form_paragraph = generate_form_paragraph(home_stats, away_stats)
    h2h_paragraph = generate_h2h_paragraph(match_data, h2h_data)
    key_players = generate_key_players(match_data, home_stats, away_stats)
    
    # Generate prediction
    prediction = generate_prediction(home_stats, away_stats, h2h_data)
    prediction_text = format_prediction_text(prediction, match_data['home_team'], match_data['away_team'])
    
    # Formatted dates
    match_time = datetime.fromisoformat(match_data['utc_date'].replace('Z', '+00:00'))
    formatted_date = match_time.strftime("%A, %d %B %Y - %H:%M")
    
    # Compose article
    title = f"{match_data['home_team']} vs {match_data['away_team']} - {match_data['competition']} Preview"
    
    content = f"""
# {match_data['home_team']} vs {match_data['away_team']} - {match_data['competition']} Preview

<span class="match-utc-time" data-utc="{match_data['utc_date']}">Date: {formatted_date}</span>

## Pre-Match Analysis
{context_paragraph}

{form_paragraph}

{h2h_paragraph}

## Team Condition
- **{match_data['home_team']}**: Form {home_stats.get('form', 'DDDDD')} | xG: {home_stats.get('advanced_stats', {}).get('xG', 1.5)} per match | Possession: {home_stats.get('advanced_stats', {}).get('possession', 50)}%
- **{match_data['away_team']}**: Form {away_stats.get('form', 'DDDDD')} | xG: {away_stats.get('advanced_stats', {}).get('xG', 1.5)} per match | Possession: {away_stats.get('advanced_stats', {}).get('possession', 50)}%

## Key Players to Watch
{key_players}

## Key Statistics
| Statistic | {match_data['home_team']} | {match_data['away_team']} |
|------------|-------------|-------------|
| xG | {home_stats.get('advanced_stats', {}).get('xG', 1.5)} | {away_stats.get('advanced_stats', {}).get('xG', 1.5)} |
| Shots per game | {home_stats.get('advanced_stats', {}).get('shots_per_game', 10.0)} | {away_stats.get('advanced_stats', {}).get('shots_per_game', 10.0)} |
| Shots on target | {home_stats.get('advanced_stats', {}).get('shots_on_target', 4.0)} | {away_stats.get('advanced_stats', {}).get('shots_on_target', 4.0)} |
| Possession | {home_stats.get('advanced_stats', {}).get('possession', 50)}% | {away_stats.get('advanced_stats', {}).get('possession', 50)}% |
| Corners | {home_stats.get('advanced_stats', {}).get('corners_per_game', 5.0)} | {away_stats.get('advanced_stats', {}).get('corners_per_game', 5.0)} |
| Fouls | {home_stats.get('advanced_stats', {}).get('fouls_per_game', 10.0)} | {away_stats.get('advanced_stats', {}).get('fouls_per_game', 10.0)} |

## Prediction
{prediction_text}

*Automatically generated - Updated: {datetime.now().strftime("%d/%m/%Y %H:%M")}*
"""
    
    # Create article object
    article = {
        'match_id': match_id,
        'title': title,
        'content': content,
        'home_team': match_data['home_team'],
        'away_team': match_data['away_team'],
        'competition': match_data['competition'],
        'match_time': match_data['utc_date'],
        'publish_time': match_data['publish_time'],
        'expire_time': match_data['expire_time'],
        'predictions': prediction,
        'languages': {'en': {'status': 'completed', 'created_at': datetime.now().isoformat()}},
        'published': False,
        'created_at': datetime.now().isoformat(),
        'updated_at': datetime.now().isoformat()
    }
    
    return article

def save_article(article):
    """Save article to Firebase"""
    match_id = article['match_id']
    ref = db.reference(f'articles/{match_id}')
    ref.set(article)
    
    # Update flags in the match
    match_date = datetime.fromisoformat(article['match_time'].replace('Z', '+00:00')).strftime('%Y-%m-%d')
    match_ref = db.reference(f'matches/{match_date}/{match_id}')
    match_ref.update({
        'article_generated': True,
        'last_updated': datetime.now().isoformat()
    })
    
    return True

def main():
    """Main function"""
    start_time = datetime.now()
    logging.info(f"Starting Article Generator - {start_time.isoformat()}")
    
    try:
        # 1. Initialize Firebase
        initialize_firebase()
        
        # 2. Get matches to process
        matches = get_matches_to_generate()
        
        # 3. Limit number of matches per execution
        matches_to_process = matches[:5]  # Max 5 matches per execution
        
        # 4. Generate and save articles
        generated_count = 0
        for match in matches_to_process:
            logging.info(f"Generating article for: {match['home_team']} vs {match['away_team']}")
            
            # Create and save article
            article = create_article(match)
            save_article(article)
            
            generated_count += 1
            logging.info(f"Article generated for match ID {match['id']}")
        
        # 5. Update health status
        health_ref = db.reference('health/generate_articles')
        health_ref.set({
            'last_run': datetime.now().isoformat(),
            'articles_generated': generated_count,
            'pending_matches': len(matches) - generated_count,
            'status': 'success'
        })
        
    except Exception as e:
        logging.error(f"General error: {str(e)}")
        
        # Update health status with error
        try:
            health_ref = db.reference('health/generate_articles')
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
    logging.info(f"Article Generator completed in {duration} seconds")
    return 0

if __name__ == "__main__":
    sys.exit(main())
