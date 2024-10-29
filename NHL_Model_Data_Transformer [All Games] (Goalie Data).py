# -*- coding: utf-8 -*-
"""
Created on Wed Oct 23 13:02:08 2024

@author: ZCHODANIECKY
"""

import sys
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
import numpy as np
import os
import itertools
import socket
from datetime import datetime
import Pull_Game_Outcomes 



    
# Game data comes from https://moneypuck.com/data.htm
# Schedule data comes from https://media.nhl.com/public/news/18238


 # Define constants
AVERAGE_GAMES = 6 # Number of games used in moving average
START_YEAR = 2020

if socket.gethostname() == 'zchodani-p-l01':
    file_directory = r"C:\Users\zchodaniecky\OneDrive - Franklin Templeton\Documents\Python\NHL_data"
else:
    file_directory = r"C:\Users\zanec\OneDrive\Documents\Python\NHL_data"
         
os.chdir(file_directory)


# Update win history file
Pull_Game_Outcomes.Update_Win_History('Win_History.csv','all_teams.csv')
Pull_Game_Outcomes.Update_Goalie_Stats('Goalie_History.csv','shots_2024.csv')


df_original = pd.read_csv('all_teams.csv')
df_win_history = pd.read_csv('Win_History.csv')

 # Filter for only relevant columns
keep_columns = ['team','season','gameId','playoffGame','opposingTeam','home_or_away','gameDate','situation',
               'goalsFor','goalsAgainst',
               'penaltiesFor','penaltiesAgainst',
               'hitsFor','hitsAgainst',
               #'takeawaysFor','giveawaysFor',          
               #'faceOffsWonFor','faceOffsWonAgainst',         
               #'shotsOnGoalFor','shotsOnGoalAgainst',
               #'savedShotsOnGoalFor','savedShotsOnGoalAgainst',                           
               #'corsiPercentage',
               #'xGoalsPercentage',
               'fenwickPercentage',
               #'missedShotsFor',
               #'blockedShotAttemptsFor','blockedShotAttemptsAgainst',
               'reboundsFor',#'reboundsAgainst',       
               #'mediumDangerShotsFor',
               #'highDangerShotsFor',
               #'dZoneGiveawaysFor',
               #'scoreFlurryAdjustedTotalShotCreditFor'
            ]

# Keep regular season data starting in given year for all play situations
df_filtered = df_original[keep_columns].copy()
df_filtered = df_filtered.query(f"situation == 'all' & playoffGame == 0 & season >= {START_YEAR}")

# Sort by date so next step calcs correct
df_filtered.sort_values(by=['gameDate','team'], ascending= [True, True], inplace=True) 
# Drop rows on dates that team did not play
df_filtered.drop_duplicates(subset=['gameDate','team'], keep='first', inplace=True)

# Merge win history file with game data
df_initial = pd.merge(df_filtered,df_win_history[['gameId','home_win']], on='gameId', how='left')


# Create column to determine in the team won or not
outcomeConditions = [
    ((df_initial['home_or_away'] == 'HOME') & (df_initial['home_win'] == 1)),
    ((df_initial['home_or_away'] == 'HOME') & (df_initial['home_win'] == 0)),
    ((df_initial['home_or_away'] == 'AWAY') & (df_initial['home_win'] == 1)),
    ((df_initial['home_or_away'] == 'AWAY') & (df_initial['home_win'] == 0))
    ]
outcomeValues = ['1','0','0','1']
df_initial.loc[:,'win_or_lose'] = np.select(outcomeConditions,outcomeValues)   

# Tie means the game went to overtime
outcomeConditions2 = [
    (df_initial['goalsFor'] > df_initial['goalsAgainst']),
    (df_initial['goalsAgainst'] > df_initial['goalsFor']),
    (df_initial['goalsAgainst'] == df_initial['goalsFor'])
    ]
outcomeValues2 = ['WIN','LOSS','TIE']
df_initial.loc[:,'win_lose_tie'] = np.select(outcomeConditions2,outcomeValues2)  

# Create custom columns
df_initial.loc[:,'win'] = np.where(df_initial['win_lose_tie'] == 'WIN', 1, 0) 
df_initial.loc[:,'seasonWin'] = df_initial.groupby(['season','team'])['win'].cumsum(axis=0)
df_initial.loc[:,'tie'] = np.where(df_initial['win_lose_tie'] == 'TIE', 1, 0)
df_initial.loc[:,'seasonTie'] = df_initial.groupby(['season','team'])['tie'].cumsum(axis=0)
df_initial.loc[:,'pointsFromGame'] = np.where(df_initial['win_lose_tie'] == 'WIN', 2,(np.where(df_initial['win_lose_tie'] == 'TIE', 1, 0)))
df_initial.loc[:,'seasonPointTotal'] = (df_initial['seasonWin'] * 2) + (df_initial['seasonTie'])
df_initial.loc[:, 'gamesPlayed'] = df_initial.groupby(['season','team']).cumcount() +1


# Create custom columns to get Averages for
#df_initial.loc[:,'shotsOnGoalDiff'] = df_initial['shotsOnGoalFor'] - df_initial['shotsOnGoalAgainst']
#df_initial.loc[:,'goalDiff'] = df_initial['goalsFor'] - df_initial['goalsAgainst']
df_initial.loc[:,'seasonPointsPerGame'] = df_initial['seasonPointTotal'] / df_initial['gamesPlayed']
#df_initial.loc[:,'faceOffsWonPct'] = df_initial['faceOffsWonFor'] / (df_initial['faceOffsWonFor'] + df_initial['faceOffsWonAgainst'])
df_initial.loc[:,'hitsDiff'] = df_initial['hitsFor'] - df_initial['hitsAgainst']# Current
#df_initial.loc[:,'SP_Total'] = np.where(df_initial['goalsFor'] == '0', 0, (df_initial['goalsFor'] / df_initial['shotsOnGoalFor']))
#df_initial.loc[:,'SV_Total'] = (df_initial['shotsOnGoalAgainst'] - df_initial['goalsAgainst']) / df_initial['savedShotsOnGoalAgainst']
#df_initial.loc[:,'PDO'] = df_initial['SP_Total'] + df_initial['SV_Total']

# Drop columns not needed after intial filtering and transforming
df_initial = df_initial.drop(columns=['home_win','win_lose_tie',
                                      'playoffGame', 'win','seasonWin','tie','seasonTie','situation','season',
                                      #'faceOffsWonFor','faceOffsWonAgainst',
                                      #'goalsFor','goalsAgainst',
                                      'hitsFor','hitsAgainst',
                                      #'shotsOnGoalFor','shotsOnGoalAgainst',
                                      #'savedShotsOnGoalFor','savedShotsOnGoalAgainst',                                       
                                      #'SP_Total','SV_Total'
                                      ])


# Function: Average stat values for the prior {AVERAGE_GAMES} number of games. Shift to use prior game values.
def calculate_avg_stats_per_game(df_use, used_col_name, moving_avg_len,shift,EMA):   
    
    if shift == True and EMA == True:
        df_use.loc[:, used_col_name + 'Avg'] = round(df_use.groupby('team')[used_col_name].transform(lambda x: x.ewm(span=AVERAGE_GAMES,adjust=False).mean(AVERAGE_GAMES).shift().bfill()), 2)
    elif shift == True and EMA == False:
        df_use.loc[:, used_col_name + 'Avg'] = round(df_use.groupby('team')[used_col_name].transform(lambda x: x.rolling(AVERAGE_GAMES,1).mean(AVERAGE_GAMES).shift().bfill()), 2)
    elif shift == False and EMA == True:
        df_use.loc[:, used_col_name + 'Avg'] = round(df_use.groupby('team')[used_col_name].transform(lambda x: x.ewm(span=AVERAGE_GAMES,adjust=False).mean(AVERAGE_GAMES)), 2)
    elif shift == False and EMA == False:
        df_use.loc[:, used_col_name + 'Avg'] = round(df_use.groupby('team')[used_col_name].transform(lambda x: x.rolling(AVERAGE_GAMES,1).mean(AVERAGE_GAMES)), 2)
    
    
# These are the columns that will be used to calculate their moving averages then dropped after
customize_columns = [#'corsiPercentage',
               'penaltiesFor','penaltiesAgainst',
               #'takeawaysFor',
               #'giveawaysFor',      
               'goalsFor','goalsAgainst',
               #'goalDiff',
               #'faceOffsWonPct',
               #'hitsFor','hitsAgainst',
               'hitsDiff',
               #'shotsOnGoalFor','shotsOnGoalAgainst',
               #'shotsOnGoalDiff', 
               'pointsFromGame',
               #'xGoalsPercentage',
               #'PDO',
               'fenwickPercentage',
               #'missedShotsFor',
               #'blockedShotAttemptsFor',
               #'blockedShotAttemptsAgainst',
               'reboundsFor',
               #'reboundsAgainst',
               #'mediumDangerShotsFor',
               #'highDangerShotsFor',
               #'dZoneGiveawaysFor',
               #'scoreFlurryAdjustedTotalShotCreditFor'
               ]


# Create the Average columns and create list for the Prediction sheet

df_train_data = df_initial.copy()

new_col_list = [] 
for index, item in enumerate(customize_columns):
    calculate_avg_stats_per_game(df_train_data,item,AVERAGE_GAMES,shift=True,EMA=True)
    new_col_list.append(item + 'Avg')
    
# Filter out early season games less than moving average
df_train_data = df_train_data.query("gamesPlayed > @AVERAGE_GAMES")

# Remove columns not needed after filtering
df_train_data = df_train_data.drop(columns=['seasonPointTotal','gamesPlayed'])

   
# Split into Home and Away tables  
df_home = df_train_data.query("home_or_away == 'HOME'")
df_home = df_home.drop(columns=customize_columns, axis=1)
df_away = df_train_data.query("home_or_away == 'AWAY'")
df_away = df_away.drop(columns=customize_columns, axis=1)
 



########### Merge goalie stats ####################
df_goalie_stats = pd.read_csv('Goalie_History.csv')

df_home_goalie_stats = df_goalie_stats.query(f"lastGoalieInNet == 1 & isGoalieTeamHome == 1 & season >= {START_YEAR}")
df_away_goalie_stats = df_goalie_stats.query(f"lastGoalieInNet == 1 & isGoalieTeamHome == 0 & season >= {START_YEAR}")

homeGameCount = df_home_goalie_stats['gameId'].nunique()
awayGameCount = df_away_goalie_stats['gameId'].nunique()

# Check if home/away goalie stats was split properly
if homeGameCount != awayGameCount:
    print(f'**** Issue with goalie data. Home Games = {homeGameCount} & Away games = {homeGameCount} **** ')


# Merge home game data and home goalie data
df_home_combined = pd.merge(df_home, df_home_goalie_stats,
                 on=['gameId','team'], how='left')
# Merge away game data and home goalie data
df_away_combined = pd.merge(df_away, df_away_goalie_stats,
                 on=['gameId','team'], how='left')

# Check if the game data has more games than the goalie data
homeShotDataMissing = df_home_combined['goalieId'].isna().sum()
awayShotDataMissing = df_away_combined['goalieId'].isna().sum()

# Check how many days of shot data are missing compated to Game data.
# We are expecting 1 game of shot data to be missing from the MoneyPuck data. gameId 2021021028 (BUF/WSH)
if homeShotDataMissing > 1 or awayShotDataMissing > 1:
    print(f'Missing {homeShotDataMissing} home games shot data and {awayShotDataMissing} away games shot data.'
          'You may be missing the most recent shots data file. *** Data will be dropped for these dates ***')
    
df_home_combined.to_csv('Home.csv',index=False)
df_away_combined.to_csv('Away.csv',index=False)
    
# Drop rows if game data has more games than the goalie data
df_home_combined.dropna(subset=['goalieId'],inplace=True)
df_away_combined.dropna(subset=['goalieId'],inplace=True)

df_merged = pd.merge(
    df_home_combined,
    df_away_combined,
    how='inner',
    on="gameId",
    left_on=None,
    right_on=None,
    left_index=False,
    right_index=False,
    sort=True,
    suffixes=("_Home", "_Away"),
    copy=True,
    indicator=False,
    validate=None,
)


############## Goalie Stats Section End ###################



# Calculate fields based on averages that were calculated above
df_merged.loc[:,'penaltiesForTotal'] = round(df_merged['penaltiesForAvg_Home'] + df_merged['penaltiesAgainstAvg_Away'],2) # Penalties served by Home team
df_merged.loc[:,'penaltiesAgainstTotal'] = round(df_merged['penaltiesAgainstAvg_Home'] + df_merged['penaltiesForAvg_Away'],2) # Power Play Home Team
#df_merged.loc[:,'turnoversFor'] = round(df_merged['takeawaysForAvg_Home'] + df_merged['giveawaysForAvg_Away'],2)  
#df_merged.loc[:,'turnoversAgainst'] = round(df_merged['giveawaysForAvg_Home'] + df_merged['takeawaysForAvg_Away'],2) 
#df_merged.loc[:,'faceOffsWonPctDiff'] = round((df_merged['faceOffsWonPctAvg_Home'] - df_merged['faceOffsWonPctAvg_Away'])*100,2)



# Discard un-needed fields from training data
discard_fields_train = ['opposingTeam_Home','opposingTeam_Away',
                      #'takeawaysForAvg_Home','takeawaysForAvg_Away',
                      #'giveawaysForAvg_Away','giveawaysForAvg_Home',
                      #'faceOffsWonPctAvg_Home','faceOffsWonPctAvg_Away',                                    
                      'penaltiesForAvg_Home','penaltiesAgainstAvg_Away','penaltiesAgainstAvg_Home','penaltiesForAvg_Away',
                      'gameDate_Home','gameDate_Away'
                      ]

# Discard un-needed goalie stat fields from training data
discard_fields_train.extend(['season_Home','goalieId_Home','goalieName_Home','isGoalieTeamHome_Home','lastGoalieInNet_Home','goalieIdSeasonGAA_Home','goalieIdSeasonSavePct_Home',
                             'season_Away','goalieId_Away','goalieName_Away','isGoalieTeamHome_Away','lastGoalieInNet_Away','goalieIdSeasonGAA_Away','goalieIdSeasonSavePct_Away'])

df_train_data_final = df_merged.drop(columns= discard_fields_train, axis=1)

# Output transformed training data
df_train_data_final.to_csv('NHL_Data_All_Games_Goalie_Transformed.csv',index=False)



#############################################################################################
#                                                                                           #
#               Create Input file to feed into model for current day games                  #
#                                                                                           #
#############################################################################################

# Find the most recent game for each team
df_most_recent_game = df_initial.groupby('team')['gameId'].last() 


df_predict_data = df_initial.copy()



new_col_list = [] 
for index, item in enumerate(customize_columns):
    calculate_avg_stats_per_game(df_predict_data,item,AVERAGE_GAMES,shift=False,EMA=False)
    new_col_list.append(item + 'Avg')


    
# Filter out early season games less than moving average
df_predict_data = df_predict_data.query("gamesPlayed > @AVERAGE_GAMES")

# Remove columns not needed after filtering
df_predict_data = df_predict_data.drop(columns=['seasonPointTotal','gamesPlayed'])



# Create list of the most recent data for each team
df_merged2 = pd.merge(
    df_most_recent_game,
    df_predict_data,
    how='inner',
    on=['gameId','team'],
    left_on=None,
    right_on=None,
    left_index=False,
    right_index=False,
    sort=True,
    suffixes=("_Home", "_Away"),
    copy=True,
    indicator=False,
    validate=None,
)


new_col_list.insert(0,'team')
new_col_list.insert(1,'gameId')
new_col_list.insert(2, 'seasonPointsPerGame')

# Take only the input columns needed from data  
df_most_recent_game_trimmed = df_merged2.reindex(new_col_list, axis='columns').copy()
  
# List of all correct team acronyms
team_acronym = ['ANA','BOS','BUF','CAR','CBJ','CGY','CHI','COL','DAL','DET','EDM','FLA','LAK','MIN','MTL',
               'NJD','NSH','NYI','NYR','OTT','PHI','PIT','SEA','SJS','STL','TBL','TOR','UTA','VAN','VGK','WPG','WSH']

# Current day games   [AWAY, HOME]
#current_slate =  [('MTL','DET'),('BUF','TBL'),('NYI','NJD'),('NSH','PIT')]

# Import the 2024 season schedule
df_schedule = pd.read_csv('NHL_Schedule_2024.csv')   
# There is a mis-match in team labeling for Utah. Schdeule uses UTH but nhl API uses UTA
df_schedule[['AWAY', 'HOME']] = df_schedule[['AWAY', 'HOME']].replace('UTH', 'UTA')

today = datetime.today().strftime('%#m/%#d/%Y')  
df_schedule_today = df_schedule.query(f"DATE == '{today}'") 

current_slate = list(zip(df_schedule_today['AWAY'],df_schedule_today['HOME']))

# Check that no teams were input incorrectly
merged_list = list(itertools.chain(*current_slate))
check_list = list(np.setdiff1d(merged_list,team_acronym))
if check_list:
    print(f'Team names are incorrect: {check_list}')
    sys.exit()

# Create list for home and away teams
current_slate_away =  [x[0] for x in current_slate]
current_slate_home =  [x[1] for x in current_slate]


# Create dataframe with Away team data
find_team = df_most_recent_game_trimmed['team'].isin(current_slate_away)
df_away_slate = df_most_recent_game_trimmed[find_team]
df_away_slate = df_away_slate.add_suffix('_Away')


# Create dataframe with Home team data
find_team = df_most_recent_game_trimmed['team'].isin(current_slate_home)
df_homeSlate = df_most_recent_game_trimmed[find_team]
df_homeSlate = df_homeSlate.add_suffix('_Home')

# Create dataframe containing home and away team matchup
df_current_slate = pd.DataFrame(current_slate,columns=['team_Away','team_Home'])

# Merge Home team data into dataframe
df_merged3 = pd.merge(
        df_current_slate,
        df_homeSlate,
        how='inner',
        on=['team_Home'],
        left_on=None,
        right_on=None,
        left_index=False,
        right_index=False,
        sort=True,
        suffixes=("_Home", "_Away"),
        copy=True,
        indicator=False,
        validate=None,
)


df_recent_home_goalie = df_home_goalie_stats.loc[df_home_goalie_stats.groupby('team')['gameId'].idxmax()]
df_recent_home_goalie = df_recent_home_goalie.add_suffix('_Home')

# Merge Home goalie data into dataframe
df_merged4 = pd.merge(
        df_merged3,
        df_recent_home_goalie,
        how='inner',
        on=['team_Home'],
        left_on=None,
        right_on=None,
        left_index=False,
        right_index=False,
        sort=True,
        suffixes=("_Home", "_Away"),
        copy=True,
        indicator=False,
        validate=None,
)


  
# Merge Away team data into dataframe
df_merged5 = pd.merge(
        df_merged4,
        df_away_slate,
        how='inner',
        on=['team_Away'],
        left_on=None,
        right_on=None,
        left_index=False,
        right_index=False,
        sort=True,
        suffixes=("_Home", "_Away"),
        copy=True,
        indicator=False,
        validate=None,
)
  

df_recent_away_goalie = df_away_goalie_stats.loc[df_away_goalie_stats.groupby('team')['gameId'].idxmax()]
df_recent_away_goalie = df_recent_away_goalie.add_suffix('_Away')

# Merge Away goalie data into dataframe
df_merged6 = pd.merge(
        df_merged5,
        df_recent_away_goalie,
        how='inner',
        on=['team_Away'],
        left_on=None,
        right_on=None,
        left_index=False,
        right_index=False,
        sort=True,
        suffixes=("_Home", "_Away"),
        copy=True,
        indicator=False,
        validate=None,
)




# Calculate some new fields
df_merged6.loc[:,'penaltiesForTotal'] = round(df_merged6['penaltiesForAvg_Home'] + df_merged6['penaltiesAgainstAvg_Away'],2)     
df_merged6.loc[:,'penaltiesAgainstTotal'] = round(df_merged6['penaltiesAgainstAvg_Home'] + df_merged6['penaltiesForAvg_Away'],2)   
#df_merged4.loc[:,'turnoversFor'] = round(df_merged4['takeawaysForAvg_Home'] + df_merged4['giveawaysForAvg_Away'],2)  
#df_merged4.loc[:,'turnoversAgainst'] = round(df_merged4['giveawaysForAvg_Home'] + df_merged4['takeawaysForAvg_Away'],2) 
#df_merged4.loc[:,'faceOffsWonPctDiff'] = round((df_merged4['faceOffsWonPctAvg_Home'] - df_merged4['faceOffsWonPctAvg_Away'])*100,2)


# Discared un-needed fields from Prediction data dataframe
discard_fields_predict = [
                 #'takeawaysForAvg_Home','takeawaysForAvg_Away',
                 #'giveawaysForAvg_Home', 'giveawaysForAvg_Away',      
                 #'faceOffsWonPctAvg_Home','faceOffsWonPctAvg_Away',           
                 'penaltiesForAvg_Home','penaltiesForAvg_Away','penaltiesAgainstAvg_Home','penaltiesAgainstAvg_Away',
                 'gameId_Home_Home','gameId_Home_Away','gameId_Away_Home','gameId_Away_Away', 
                 ]

# Discard un-needed goalie stat fields from training data
discard_fields_predict.extend(['season_Home','isGoalieTeamHome_Home','lastGoalieInNet_Home','beforeGameSeasonGAA_Home','beforeGameSesaonSavePct_Home',
                             'season_Away','isGoalieTeamHome_Away','lastGoalieInNet_Away','beforeGameSeasonGAA_Away','beforeGameSesaonSavePct_Away'])


df_predict_data = df_merged6.drop(columns=discard_fields_predict, axis=1)

# Output transformed prediction data
df_predict_data.to_csv('NHL_Data_All_Games_Goalie_Predict.csv',index=False)

