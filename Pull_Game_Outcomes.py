# -*- coding: utf-8 -*-
"""
Created on Tue Sep 24 11:01:21 2024

@author: ZCHODANIECKY
"""



import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
import numpy as np
import os
import socket
import requests

def Update_Win_History(winFilePath,dataFilePath):

    if socket.gethostname() == 'zchodani-p-l01':
        file_directory = r"C:\Users\zchodaniecky\OneDrive - Franklin Templeton\Documents\Python\NHL_data"
    else:
        file_directory = r"C:\Users\zanec\OneDrive\Documents\Python\NHL_data"
             
    os.chdir(file_directory)
    
    # Open win history sheet and find most recent game
    df_Win_History = pd.read_csv(winFilePath)
    latest_GameID = df_Win_History['gameId'].max()
    
    # Open game data sheet and find all games after the most recent game in Win History sheet
    df_Game_History = pd.read_csv(dataFilePath)
    df_Game_History = df_Game_History.query("situation == 'all' & playoffGame == 0")
    df_Newest_GameIds = df_Game_History[df_Game_History['gameId'] > latest_GameID]
    
    # Convert gameIds to a list and remove duplicates
    unique_GameIds = df_Newest_GameIds['gameId'].unique().tolist()
    
    
    
    nhl_URL = 'https://api-web.nhle.com/v1/wsc/game-story/'
    
    outcomes = []
    for _ in unique_GameIds:    
        game_id = _
        url = f'{nhl_URL}{game_id}'
        result = requests.get(url)  
        
        if result.status_code == 200:
            # Parse the JSON response
            data = result.json()
            
            # Assuming the JSON has an abbreviation and score under certain keys
            # You will need to adapt this based on the actual JSON structure
            away_abbrev = data.get('awayTeam', {}).get('abbrev', 'N/A') 
            away_score = data.get('awayTeam', {}).get('score', 'N/A')  
            home_abbrev = data.get('homeTeam', {}).get('abbrev', 'N/A')  
            home_score = data.get('homeTeam', {}).get('score', 'N/A')  
            
            print(f'{away_abbrev} {away_score} : {home_abbrev} {home_score}')
            new_row = [game_id,away_abbrev,away_score,home_abbrev,home_score]
            outcomes.append(new_row)
        else:
            print(f"Failed to retrieve data. Status code: {result.status_code}")

    
    df_Outcomes = pd.DataFrame(outcomes, columns=['gameId','away_team','away_score','home_team','home_score'])
    
    df_Outcomes.head()
    
    df_Outcomes.loc[:,'home_win'] = np.where(df_Outcomes['home_score'] >= df_Outcomes['away_score'], 1, 0) 
    
    df_Combined = pd.concat([df_Win_History,df_Outcomes], ignore_index=True)
    
    df_Combined.to_csv(winFilePath,index=False)
    
    gameCount = len(df_Outcomes['gameId'])
    print(f"{gameCount} games added to Win_History" )
    
    
def Update_Goalie_GAA(shotsHistoryPath,shotsCurrentYearPath):

    if socket.gethostname() == 'zchodani-p-l01':
        file_directory = r"C:\Users\zchodaniecky\OneDrive - Franklin Templeton\Documents\Python\NHL_data\Shots Model"
    elif socket.gethostname() == 'FTILC3VBil7BwCe':
        file_directory = r"C:\Users\zchodan\OneDrive - Franklin Templeton\Documents\Python\NHL_data\Shots Model"
    else:
        file_directory = r"C:\Users\zanec\OneDrive\Documents\Python\NHL_data\Shots Model"
             
    os.chdir(file_directory)
    
    shotsHistoryPath = 'shots_2015-2023.csv'
    shotsCurrentYearPath = 'shots_2024.csv'
    
    df_shots_history = pd.read_csv(shotsHistoryPath)
    df_shots_current_year = pd.read_csv(shotsCurrentYearPath)
    
     
    # Filter for columns that we want
    keep_columns = ['season','isPlayoffGame','game_id','team','homeTeamCode','awayTeamCode','isHomeTeam','goalieIdForShot','goalieNameForShot',
                    'goal','shotWasOnGoal'
                    ]
    
    # Clean up dataframe
    df_shots_history = df_shots_history[keep_columns].copy()  
    df_shots_history = df_shots_history.dropna(subset=['goalieIdForShot'])
    df_shots_history['goalieIdForShot'] = df_shots_history['goalieIdForShot'].astype(str).str.replace('.0','',regex=False)
    df_shots_history['isHomeTeam'] = df_shots_history['isHomeTeam'].astype(str).str.replace('.0','',regex=False)
    
    # Clean up datafram
    df_shots_current_year = df_shots_current_year[keep_columns].copy() 
    df_shots_current_year = df_shots_current_year.dropna(subset=['goalieIdForShot']) 
    df_shots_current_year['goalieIdForShot'] = df_shots_current_year['goalieIdForShot'].astype(str).str.replace('.0','',regex=False)
    df_shots_current_year['isHomeTeam'] = df_shots_current_year['isHomeTeam'].astype(str).str.replace('.0','',regex=False)
    
    df_Combined = pd.concat([df_shots_history,df_shots_current_year], ignore_index=True)
    
    # Create fullGameId
    df_Combined['fullGameId'] = df_Combined['season'].astype(str) + df_Combined['isPlayoffGame'].astype(str) + df_Combined['game_id'].astype(str)
    
    ##### CALCULATE GAA #####
    
    # Remove playoffs and empty net goals since we're calculating GAA
    df_filtered = df_Combined.query("isPlayoffGame == 0 & goalieIdForShot != '0' & goalieIdForShot.notna() & goal == 1 & season >= 2018")
           
    
    # Group by 'game_id' and 'goalie_id' then sum the 'goal' column to get total goals for each game/goalie
    df_goals_per_game = df_filtered.groupby(['fullGameId','goalieIdForShot','season','isHomeTeam'])['goal'].sum().reset_index()
    
    # Rename the column to make it clear that it's the total number of goals
    df_goals_per_game.rename(columns={'goal': 'totalGameGoals'}, inplace=True)
    
      
    # Step 1: Create a cumulative count of games for each goalie in each season
    df_goals_per_game['cumulativeGames'] = (
    df_goals_per_game
    .groupby(['season', 'goalieIdForShot'])
    .cumcount() + 1  # +1 to start count from 1 instead of 0
    )

    # Step 2: Calculate the rolling average with a variable window based on cumulative games
    df_goals_per_game['goalieIdSeasonGAA'] = (
    df_goals_per_game
    .groupby(['season', 'goalieIdForShot'])
    ['totalGameGoals']
    .transform(lambda x: x.rolling(window=len(x), min_periods=1).mean())
    )
    
    # Step 3: Calculate the rolling average with a variable window based on cumulative games
    df_goals_per_game['goalieIdSeasonGAA'] = (
    df_goals_per_game
    .groupby(['season', 'goalieIdForShot'])
    ['totalGameGoals']
    .transform(lambda x: x.rolling(window=len(x), min_periods=1).mean())
    )
    
    
    ##### CALCULATE SV% #####
    
    # Using goals table, Create a cumulative count of games for each goalie in each season
    df_goals_per_game['cumulativeGoals'] = (
    df_goals_per_game
    .groupby(['season', 'goalieIdForShot'])['totalGameGoals']
    .cumsum()
    )
    
     
    # Remove playoffs and empty net goals since we're calculating GAA
    df_filtered = df_Combined.query("isPlayoffGame == 0 & goalieIdForShot != '0' & goalieIdForShot.notna() & shotWasOnGoal == 1 & season >= 2018")
        
   
    # Group by 'game_id' and 'goalie_id' then sum the 'goal' column to get total goals for each goalie each game
    df_shots_on_goal_per_game = df_filtered.groupby(['fullGameId','goalieIdForShot','season','isHomeTeam'])['shotWasOnGoal'].sum().reset_index()
    
    # Rename the column to make it clear that it's the total number of goals
    df_shots_on_goal_per_game.rename(columns={'shotWasOnGoal': 'totalShotsOnGoal'}, inplace=True)
    
    # Create a cumulative shots on goal for each goalie in each season
    df_shots_on_goal_per_game['cumulativeShotsOnGoal'] = (
    df_shots_on_goal_per_game
    .groupby(['season', 'goalieIdForShot'])['totalShotsOnGoal']
    .cumsum()
    )
    
    
    
    
    df_merged = pd.merge(df_goals_per_game, df_shots_on_goal_per_game,
                     on=['fullGameId', 'goalieIdForShot','season','isHomeTeam'], how='left')
    

    df_merged.info()
    
    df_merged['goalieIdSeasonSavePct'] = ((df_merged['cumulativeShotsOnGoal'] - df_merged['cumulativeGoals']) / df_merged['cumulativeShotsOnGoal'])
    
    
    df_merged.to_csv('hehe.csv')
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    # Open win history sheet and find most recent game
    df_Win_History = pd.read_csv(winFilePath)
    latest_GameID = df_Win_History['gameId'].max()
    
    # Open game data sheet and find all games after the most recent game in Win History sheet
    df_Game_History = pd.read_csv(dataFilePath)
    df_Game_History = df_Game_History.query("situation == 'all' & playoffGame == 0")
    df_Newest_GameIds = df_Game_History[df_Game_History['gameId'] > latest_GameID]
    
    # Convert gameIds to a list and remove duplicates
    unique_GameIds = df_Newest_GameIds['gameId'].unique().tolist()
    
    
    
    nhl_URL = 'https://api-web.nhle.com/v1/wsc/game-story/'
    
    outcomes = []
    for _ in unique_GameIds:    
        game_id = _
        url = f'{nhl_URL}{game_id}'
        result = requests.get(url)  
        
        if result.status_code == 200:
            # Parse the JSON response
            data = result.json()
            
            # Assuming the JSON has an abbreviation and score under certain keys
            # You will need to adapt this based on the actual JSON structure
            away_abbrev = data.get('awayTeam', {}).get('abbrev', 'N/A') 
            away_score = data.get('awayTeam', {}).get('score', 'N/A')  
            home_abbrev = data.get('homeTeam', {}).get('abbrev', 'N/A')  
            home_score = data.get('homeTeam', {}).get('score', 'N/A')  
            
            print(f'{away_abbrev} {away_score} : {home_abbrev} {home_score}')
            new_row = [game_id,away_abbrev,away_score,home_abbrev,home_score]
            outcomes.append(new_row)
        else:
            print(f"Failed to retrieve data. Status code: {result.status_code}")

    
    df_Outcomes = pd.DataFrame(outcomes, columns=['gameId','away_team','away_score','home_team','home_score'])
    
    df_Outcomes.head()
    
    df_Outcomes.loc[:,'home_win'] = np.where(df_Outcomes['home_score'] >= df_Outcomes['away_score'], 1, 0) 
    
    df_Combined = pd.concat([df_Win_History,df_Outcomes], ignore_index=True)
    
    df_Combined.to_csv(winFilePath,index=False)
    
    gameCount = len(df_Outcomes['gameId'])
    print(f"{gameCount} games added to Win_History" )