# -*- coding: utf-8 -*-
"""
Created on Tue Oct 22 10:51:59 2024

@author: zchodan
"""

import pandas as pd
import numpy as np
pd.options.mode.chained_assignment = None  # default='warn'
import os
import socket


if socket.gethostname() == 'zchodani-p-l01':
    file_directory = r"C:\Users\zchodaniecky\OneDrive - Franklin Templeton\Documents\Python\NHL_data"
elif socket.gethostname() == 'FTILC3VBil7BwCe':
    file_directory = r"C:\Users\zchodan\OneDrive - Franklin Templeton\Documents\Python\NHL_data"
else:
    file_directory = r"C:\Users\zanec\OneDrive\Documents\Python\NHL_data"
         
os.chdir(file_directory)

shotsHistoryPath = 'shots_2015-2023.csv'
shotsCurrentYearPath = 'shots_2024.csv'

df_shots_history = pd.read_csv(shotsHistoryPath)
df_shots_current_year = pd.read_csv(shotsCurrentYearPath)

 
# Filter for columns that we want
keep_columns = ['season','isPlayoffGame','game_id','team','homeTeamCode','awayTeamCode','isHomeTeam','goalieIdForShot','goalieNameForShot',
                'goal','shotWasOnGoal','time','shotOnEmptyNet'
                ]

# Clean up dataframe
df_shots_history = df_shots_history[keep_columns].copy()  
df_shots_history = df_shots_history.dropna(subset=['goalieIdForShot']) # Drop empty net goals
df_shots_history['goalieIdForShot'] = df_shots_history['goalieIdForShot'].astype(str).str.replace('.0','',regex=False) # Convert goalie ID to str
df_shots_history['isHomeTeam'] = df_shots_history['isHomeTeam'].astype(str).str.replace('.0','',regex=False) # Convert isHomeTeam to str
df_shots_history['shotWasOnGoal'] = df_shots_history['shotWasOnGoal'].astype(int) # Convert shotWasOnGoal to int

# Clean up datafram
df_shots_current_year = df_shots_current_year[keep_columns].copy() 
df_shots_current_year = df_shots_current_year.dropna(subset=['goalieIdForShot'])  # Drop empty net goals
df_shots_current_year['goalieIdForShot'] = df_shots_current_year['goalieIdForShot'].astype(str).str.replace('.0','',regex=False) # Convert goalie ID to str
df_shots_current_year['isHomeTeam'] = df_shots_current_year['isHomeTeam'].astype(str).str.replace('.0','',regex=False) # Convert isHomeTeam to str
df_shots_current_year['shotWasOnGoal'] = df_shots_current_year['shotWasOnGoal'].astype(int) # Convert shotWasOnGoal to int

df_Combined = pd.concat([df_shots_history,df_shots_current_year], ignore_index=True)


# Remove missed shots playoff games to redce dataframe workload
df_Combined = df_Combined.query("isPlayoffGame == 0 & shotWasOnGoal == 1 & season >= 2015")  

# Create fullGameId
df_Combined['fullGameId'] = df_Combined['season'].astype(str) + df_Combined['isPlayoffGame'].astype(str) + df_Combined['game_id'].astype(str)


# Create goalie team column
df_Combined['goalieTeam'] = np.where(df_Combined['isHomeTeam'] == '1', df_Combined['awayTeamCode'],df_Combined['homeTeamCode'])


# Find which goalie was in net at the end of the game, in case a goalie got pulled during the game
# Group by 'GameId' and 'Team', then find the index of the row with the max 'Time' in each group
df_last_in_net = df_Combined.query("shotOnEmptyNet == 0 & goalieNameForShot.notna()").copy() 
# Ensure the index is preserved/reset if necessary
df_last_in_net = df_last_in_net.reset_index(drop=True)
max_time_indices = df_last_in_net.groupby(['fullGameId', 'goalieTeam'])['time'].idxmax()

df_last_in_net.to_csv('hehe.csv', index=False)

# Create a new DataFrame from the rows with the max 'Time' in each group
df_max_time_goalies = df_last_in_net.loc[max_time_indices, ['fullGameId', 'goalieTeam', 'goalieIdForShot']]


# Merge original DataFrame with max_time_goalies on 'GameId', 'Team', and 'GoalieId'
df_Combined = df_Combined.merge(df_max_time_goalies.assign(lastGoalieInNet=1), on=['fullGameId', 'goalieTeam', 'goalieIdForShot'], how='left')

# Fill NaN values in 'Winner' with 0 for non-matching rows
df_Combined['lastGoalieInNet'] = df_Combined['lastGoalieInNet'].fillna(0).astype(int)


##### CALCULATE GAA #####

# Remove empty net goals since we're calculating GAA
df_no_empty_net = df_Combined.query("shotOnEmptyNet == 0 & goalieNameForShot.notna() & goalieIdForShot != 0")   

# Group by 'game_id' and 'goalie_id' then sum the 'goal' column to get total goals for each game/goalie
df_goals_per_game = df_no_empty_net.groupby(['fullGameId','goalieIdForShot','goalieNameForShot','season','goalieTeam','isHomeTeam','lastGoalieInNet'])['goal'].sum().reset_index()

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


# Group by 'game_id' and 'goalie_id' then sum the 'goal' column to get total goals for each goalie each game
df_shots_on_goal_per_game = df_no_empty_net.groupby(['fullGameId','goalieIdForShot','goalieNameForShot','season','goalieTeam','isHomeTeam','lastGoalieInNet'])['shotWasOnGoal'].sum().reset_index()

# Rename the column to make it clear that it's the total number of goals
df_shots_on_goal_per_game.rename(columns={'shotWasOnGoal': 'totalShotsOnGoal'}, inplace=True)

# Create a cumulative shots on goal for each goalie in each season
df_shots_on_goal_per_game['cumulativeShotsOnGoal'] = (
df_shots_on_goal_per_game
.groupby(['season', 'goalieIdForShot'])['totalShotsOnGoal']
.cumsum()
)


df_merged = pd.merge(df_goals_per_game, df_shots_on_goal_per_game,
                 on=['fullGameId', 'goalieIdForShot','goalieNameForShot','season','goalieTeam','isHomeTeam','lastGoalieInNet'], how='inner')


df_merged['goalieIdSeasonSavePct'] = ((df_merged['cumulativeShotsOnGoal'] - df_merged['cumulativeGoals']) / df_merged['cumulativeShotsOnGoal'])


# Some games have 2 goalies per team because the goalie was pulled. Create a field for avg GAA and Save %
df_merged['seasonSavePctAvgForGame'] = df_merged.groupby(['fullGameId','goalieTeam'])['goalieIdSeasonSavePct'].transform('mean')
df_merged['seasonGAAAvgForGame'] = df_merged.groupby(['fullGameId','goalieTeam'])['goalieIdSeasonGAA'].transform('mean')




# Rename fields to match with other data files
df_merged.rename(columns={'fullGameId':'gameId'}, inplace=True)
df_merged.rename(columns={'goalieTeam':'team'}, inplace=True)


df_merged.to_csv('Goalie_History.csv', index=False)
