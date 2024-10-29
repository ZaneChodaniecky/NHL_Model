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
    
    
def Update_Goalie_Stats(goalieHistoryPath,shotsCurrentYearPath):

    goalieHistoryPath = 'Goalie_History.csv'
    shotsCurrentYearPath = 'shots_2024.csv'


    if socket.gethostname() == 'zchodani-p-l01':
        file_directory = r"C:\Users\zchodaniecky\OneDrive - Franklin Templeton\Documents\Python\NHL_data"
    elif socket.gethostname() == 'FTILC3VBil7BwCe':
        file_directory = r"C:\Users\zchodan\OneDrive - Franklin Templeton\Documents\Python\NHL_data"
    else:
        file_directory = r"C:\Users\zanec\OneDrive\Documents\Python\NHL_data"
             
    os.chdir(file_directory)
    
    
    # Open win history sheet and find most recent gam
    df_goalie_history = pd.read_csv(goalieHistoryPath)
  
    latest_GameID = df_goalie_history['gameId'].max()
    
    
    df_shots_current_year = pd.read_csv(shotsCurrentYearPath)
    
     
    # Filter for columns that we want
    keep_columns = ['season','isPlayoffGame','game_id','team','homeTeamCode','awayTeamCode','isHomeTeam','goalieIdForShot','goalieNameForShot',
                    'goal','shotWasOnGoal','time','shotOnEmptyNet'
                    ]
    
    
    # Clean up datafram
    df_shots_current_year = df_shots_current_year[keep_columns].copy() 
    df_shots_current_year = df_shots_current_year.dropna(subset=['goalieIdForShot'])  # Drop empty net goals
    df_shots_current_year['goalieIdForShot'] = df_shots_current_year['goalieIdForShot'].astype(str).str.replace('.0','',regex=False) # Convert goalie ID to str
    df_shots_current_year['isHomeTeam'] = df_shots_current_year['isHomeTeam'].astype(str).str.replace('.0','',regex=False) # Convert isHomeTeam to str
    df_shots_current_year['shotWasOnGoal'] = df_shots_current_year['shotWasOnGoal'].astype(int) # Convert shotWasOnGoal to int
    
    
    # Create gameId
    df_shots_current_year['gameId'] = df_shots_current_year['season'].astype(str) + df_shots_current_year['isPlayoffGame'].astype(str) + df_shots_current_year['game_id'].astype(str)
    df_shots_current_year['gameId'] = df_shots_current_year['gameId'].astype(int)
    
    
    # Rename fields to make data less confusing
    # This is shot data, but we are looking from the goalies perspective
    df_shots_current_year.rename(columns={'goalieIdForShot':'goalieId'}, inplace=True)
    df_shots_current_year.rename(columns={'goalieNameForShot':'goalieName'}, inplace=True)
    
    
    # Remove missed shots playoff games to redce dataframe workload
    df_shots_current_year = df_shots_current_year.query("isPlayoffGame == 0 & shotWasOnGoal == 1")  
    
    # Create goalie team column based on is shooter home team
    df_shots_current_year['isGoalieTeamHome'] = np.where(df_shots_current_year['isHomeTeam'] == '1', '0','1')
    df_shots_current_year.drop(columns=['isGoalieTeamHome'])

    df_shots_current_year['goalieTeam'] = np.where(df_shots_current_year['isGoalieTeamHome'] == '1', df_shots_current_year['homeTeamCode'],df_shots_current_year['awayTeamCode'])
   
    
    # Find which goalie was in net at the end of the game, in case a goalie got pulled during the game
    # Group by 'GameId' and 'Team', then find the index of the row with the max 'Time' in each group
    df_last_in_net = df_shots_current_year.query("shotOnEmptyNet == 0 & goalieName.notna() & season >= 2015").copy() 
    # Ensure the index is preserved/reset if necessary
    df_last_in_net = df_last_in_net.reset_index(drop=True)
    max_time_indices = df_last_in_net.groupby(['gameId', 'goalieTeam'])['time'].idxmax()
    
    # Create a new DataFrame from the rows with the max 'Time' in each group
    df_max_time_goalies = df_last_in_net.loc[max_time_indices, ['gameId', 'goalieTeam', 'goalieId']]

    # Merge original DataFrame with max_time_goalies on 'GameId', 'Team', and 'GoalieId'
    df_shots_current_year = df_shots_current_year.merge(df_max_time_goalies.assign(lastGoalieInNet=1), on=['gameId', 'goalieTeam', 'goalieId'], how='left')

    # Fill NaN values in 'lastGoalieInNet' with 0 for non-matching rows
    df_shots_current_year['lastGoalieInNet'] = df_shots_current_year['lastGoalieInNet'].fillna(0).astype(int) 
   
    
    ##################################### CALCULATE GAA #################################
    
    # Remove playoffs and empty net goals since we're calculating GAA
    df_filtered = df_shots_current_year.query(f"gameId > {latest_GameID} & shotOnEmptyNet == 0 & goalieId != 0 & goalieName.notna()")
            
    # Group by 'game_id' and 'goalie_id' then sum the 'goal' column to get total goals for each game/goalie
    df_goals_per_game = df_filtered.groupby(['gameId','goalieId','goalieName','season','goalieTeam','isGoalieTeamHome','lastGoalieInNet'])['goal'].sum().reset_index()
    
    # Rename the column to make it clear that it's the total number of goals
    df_goals_per_game.rename(columns={'goal': 'totalGameGoals'}, inplace=True)
    
      
    # Step 1: Create a cumulative count of games for each goalie in each season
    df_goals_per_game['cumulativeGames'] = (
    df_goals_per_game
    .groupby(['season', 'goalieId'])
    .cumcount() + 1  # +1 to start count from 1 instead of 0
    )

    # Step 2: Calculate the rolling average with a variable window based on cumulative games
    df_goals_per_game['goalieIdSeasonGAA'] = (
    df_goals_per_game
    .groupby(['season', 'goalieId'])
    ['totalGameGoals']
    .transform(lambda x: x.rolling(window=len(x), min_periods=1).mean())
    )
    
    # Step 3: Calculate the rolling average with a variable window based on cumulative games
    df_goals_per_game['goalieIdSeasonGAA'] = (
    df_goals_per_game
    .groupby(['season', 'goalieId'])
    ['totalGameGoals']
    .transform(lambda x: x.rolling(window=len(x), min_periods=1).mean())
    )
    
    
    ###################################### CALCULATE SV% ##########################################
    
    # Using goals table, Create a cumulative count of games for each goalie in each season
    df_goals_per_game['cumulativeGoals'] = (
    df_goals_per_game
    .groupby(['season', 'goalieId'])['totalGameGoals']
    .cumsum()
    )
      
    # Remove playoffs and empty net goals since we're calculating GAA
    df_filtered = df_shots_current_year.query(f"gameId > {latest_GameID} & shotOnEmptyNet == 0 & goalieId != 0 & goalieName.notna()")
        
    # Group by 'game_id' and 'goalie_id' then sum the 'goal' column to get total goals for each goalie each game
    df_shots_on_goal_per_game = df_filtered.groupby(['gameId','goalieId','goalieName','season','goalieTeam','isGoalieTeamHome','lastGoalieInNet'])['shotWasOnGoal'].sum().reset_index()
    
    # Rename the column to make it clear that it's the total number of goals
    df_shots_on_goal_per_game.rename(columns={'shotWasOnGoal':'totalShotsOnGoal'}, inplace=True)
    
    # Create a cumulative shots on goal for each goalie in each season
    df_shots_on_goal_per_game['cumulativeShotsOnGoal'] = (
    df_shots_on_goal_per_game
    .groupby(['season', 'goalieId'])['totalShotsOnGoal']
    .cumsum()
    )
    
  
    df_goalie_update = pd.merge(df_goals_per_game, df_shots_on_goal_per_game,
                     on=['gameId', 'goalieId','goalieName','season','goalieTeam','isGoalieTeamHome','lastGoalieInNet'], how='inner')
    
    # Calculate save percentage for goalie so far for season
    df_goalie_update['goalieIdSeasonSavePct'] = ((df_goalie_update['cumulativeShotsOnGoal'] - df_goalie_update['cumulativeGoals']) / df_goalie_update['cumulativeShotsOnGoal'])
    
    
    # Sort by 'season', 'goalie', and 'game' to ensure order
    df_goalie_update = df_goalie_update.sort_values(by=['season', 'goalieId', 'gameId']).reset_index(drop=True)
    # Create a column for the start of game values for goalie statistics
    df_goalie_update['beforeGameSesaonSavePct'] = df_goalie_update.groupby(['season', 'goalieId'])['goalieIdSeasonSavePct'].shift(1).fillna(df_goalie_history['goalieIdSeasonSavePct'].mean()).astype(float)
    df_goalie_update['beforeGameSeasonGAA'] = df_goalie_update.groupby(['season', 'goalieId'])['goalieIdSeasonGAA'].shift(1).fillna(df_goalie_history['goalieIdSeasonGAA'].mean()).astype(float)
    
    # NOT SURE IF THIS NEXT SECTION IS LOGICAL?
    # Some games have 2 goalies per team because the goalie was pulled. Create a field for avg GAA and Save %
    #df_goalie_update['seasonSavePctAvgForGame'] = df_goalie_update.groupby(['gameId','goalieTeam'])['goalieIdSeasonSavePct'].transform('mean')
    #df_goalie_update['seasonGAAAvgForGame'] = df_goalie_update.groupby(['gameId','goalieTeam'])['goalieIdSeasonGAA'].transform('mean')
       
    # Rename fields to match with other data files
    df_goalie_update.rename(columns={'gameId':'gameId'}, inplace=True)
    df_goalie_update.rename(columns={'goalieTeam':'team'}, inplace=True)
    
    # Drop fields used to calc goalie averages
    df_goalie_update = df_goalie_update.drop(['totalGameGoals','cumulativeGames','cumulativeGoals','totalShotsOnGoal','cumulativeShotsOnGoal'],axis=1)
    
    
    # Append new data to the history file
    df_new_goalie_history = pd.concat([df_goalie_history,df_goalie_update], ignore_index=True)
        
    df_new_goalie_history.to_csv('Goalie_History.csv',index=False)
    
    gameCount = len(df_goalie_update['gameId'])
    print(f"{gameCount} performances added to Goalie_History" )