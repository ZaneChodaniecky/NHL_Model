# -*- coding: utf-8 -*-
"""
Created on Fri Oct 11 12:02:15 2024

@author: ZaneC
"""

import sys
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
import numpy as np
import os
import itertools
import socket
from datetime import datetime


def transform_data(fileName):
    
    # Shots data comes from https://moneypuck.com/data.htm
    # Schedule data comes from https://media.nhl.com/public/news/18238


    # Define constants
    AVERAGE_GAMES = 6 # Number of games used in moving average
    
    if socket.gethostname() == 'zchodani-p-l01':
        file_directory = r"C:\Users\zchodaniecky\OneDrive - Franklin Templeton\Documents\Python\NHL_data\Shots Model"
    else:
        file_directory = r"C:\Users\zanec\OneDrive\Documents\Python\NHL_data\Shots Model"
             
    os.chdir(file_directory)
    
    
    # Combined shots history file pre-2024 with 2024 current data
    df_shots_2015_2023 = pd.read_csv('shots_2015-2023.csv')    
    df_shots_2024 = pd.read_csv('shots_2024.csv')
    
    
    
        # Filter for columns that we want
    keep_columns = ['season','isPlayoffGame','game_id','team','homeTeamCode','awayTeamCode','isHomeTeam','playerPositionThatDidEvent','shooterName',
                    'shooterPlayerId','shotWasOnGoal'
                    ]
    
    df_shots_2015_2023 = df_shots_2015_2023[keep_columns].copy()
    df_shots_2024 = df_shots_2024[keep_columns].copy()
    
    df_Combined = pd.concat([df_shots_2015_2023,df_shots_2024], ignore_index=True)
    
    # Concat fields to create full gameId
    df_Combined['fullGameId'] = df_Combined['season'].astype(str) + df_Combined['isPlayoffGame'].astype(str) + df_Combined['game_id'].astype(str)
    
  
    
    df_filtered = df_Combined.query("isPlayoffGame == 0 & shotWasOnGoal == 1 & shooterPlayerId != 0 & shooterPlayerId.notna() & season >= 2018")
    
    df_filtered.sort_values(by=['shooterPlayerId','fullGameId'], ascending= [True, True], inplace=True) # Sort by date so next step calcs correct
    

    df_shot_counts = df_filtered.groupby(['fullGameId','shooterPlayerId']).size().reset_index(name='shotCount')


    df_merged_1 = pd.merge(df_filtered,df_shot_counts, on=['fullGameId','shooterPlayerId'], how='left')
    
    
    df_merged_1.drop_duplicates(subset=['fullGameId','shooterPlayerId'], keep='first', inplace=True) # Drop rows on dates that team did not play
    
    df_merged_1['teamCode'] = np.where(df_merged_1['team'] == 'HOME', df_merged_1['homeTeamCode'], df_merged_1['awayTeamCode'])
    
    
    df_merged_1['shotCount_SMA'] = round(df_merged_1.groupby('shooterPlayerId')['shotCount'].rolling(window=5, min_periods=1).mean().reset_index(0, drop=True),2) #SMA
    df_merged_1['shotCount_EMA'] = round(df_merged_1.groupby('shooterPlayerId')['shotCount'].ewm(span=5, adjust=False).mean().reset_index(0, drop=True),2) #EMA
    
    
    df_merged_1.to_csv('Check_Player_Data.csv',index=False)
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    # Create custom columns
    outcomeConditions = [
        ((df_trimmed['team'] == 'HOME') & (df_trimmed['home_win'] == 1)),
        ((df_trimmed['home_or_away'] == 'HOME') & (df_trimmed['home_win'] == 0)),
        ((df_trimmed['home_or_away'] == 'AWAY') & (df_trimmed['home_win'] == 1)),
        ((df_trimmed['home_or_away'] == 'AWAY') & (df_trimmed['home_win'] == 0))
        ]
    # Did the home team win or not
    outcomeValues = ['1','0','0','1']
    

    outcomeConditions2 = [
        (df_trimmed['goalsFor'] > df_trimmed['goalsAgainst']),
        (df_trimmed['goalsAgainst'] > df_trimmed['goalsFor']),
        (df_trimmed['goalsAgainst'] == df_trimmed['goalsFor'])
        ]
    # Win and Loss do not count shootout, all shootouts are considered a tie
    outcomeValues2 = ['WIN','LOSS','TIE']
    
    
    df_trimmed.loc[:,'win_or_lose'] = np.select(outcomeConditions,outcomeValues)   
    df_trimmed.loc[:,'win_lose_tie'] = np.select(outcomeConditions2,outcomeValues2)   
    df_trimmed.loc[:,'win'] = np.where(df_trimmed['win_lose_tie'] == 'WIN', 1, 0) 
    df_trimmed.loc[:,'seasonWin'] = df_trimmed.groupby(['season','team'])['win'].cumsum(axis=0)
    df_trimmed.loc[:,'tie'] = np.where(df_trimmed['win_lose_tie'] == 'TIE', 1, 0)
    df_trimmed.loc[:,'seasonTie'] = df_trimmed.groupby(['season','team'])['tie'].cumsum(axis=0)
    df_trimmed.loc[:,'pointsFromGame'] = np.where(df_trimmed['win_lose_tie'] == 'WIN', 2,(np.where(df_trimmed['win_lose_tie'] == 'TIE', 1, 0)))
    df_trimmed.loc[:,'seasonPointTotal'] = (df_trimmed['seasonWin'] * 2) + (df_trimmed['seasonTie'])
    df_trimmed.loc[:, 'gamesPlayed'] = df_trimmed.groupby(['season','team']).cumcount() +1
    
    
    # Create custom columns to get Averages for
    #df_trimmed.loc[:,'shotsOnGoalDiff'] = df_trimmed['shotsOnGoalFor'] - df_trimmed['shotsOnGoalAgainst']
    df_trimmed.loc[:,'goalDiff'] = df_trimmed['goalsFor'] - df_trimmed['goalsAgainst']
    df_trimmed.loc[:,'seasonPointsPerGame'] = df_trimmed['seasonPointTotal'] / df_trimmed['gamesPlayed']
    #df_trimmed.loc[:,'faceOffsWonPct'] = df_trimmed['faceOffsWonFor'] / (df_trimmed['faceOffsWonFor'] + df_trimmed['faceOffsWonAgainst'])
    df_trimmed.loc[:,'hitsDiff'] = df_trimmed['hitsFor'] - df_trimmed['hitsAgainst']# Current
    #df_trimmed.loc[:,'SP_Total'] = np.where(df_trimmed['goalsFor'] == '0', 0, (df_trimmed['goalsFor'] / df_trimmed['shotsOnGoalFor']))
    #df_trimmed.loc[:,'SV_Total'] = (df_trimmed['shotsOnGoalAgainst'] - df_trimmed['goalsAgainst']) / df_trimmed['savedShotsOnGoalAgainst']
    #df_trimmed.loc[:,'PDO'] = df_trimmed['SP_Total'] + df_trimmed['SV_Total']
    
    # Drop columns not needed after intial filtering and transforming
    df_trimmed = df_trimmed.drop(columns=['home_win','win_lose_tie',
                                          'playoffGame', 'win','seasonWin','tie','seasonTie','situation','season',
                                          #'faceOffsWonFor','faceOffsWonAgainst',
                                          'goalsFor','goalsAgainst',
                                          'hitsFor','hitsAgainst',
                                          #'shotsOnGoalFor','shotsOnGoalAgainst',
                                          'savedShotsOnGoalFor','savedShotsOnGoalAgainst',                                       
                                          #'SP_Total','SV_Total'
                                          ])
    
        
    # Function: Average stat values for the prior {AVERAGE_GAMES} number of games. Shift to use prior game values.
    def calculate_avg_stats_per_game(df_use, used_col_name, moving_avg_len):   
        #df_use.loc[:, used_col_name + 'Avg'] = round(df_use.groupby('team')[used_col_name].transform(lambda x: x.rolling(AVERAGE_GAMES,1).mean(AVERAGE_GAMES).shift().bfill()), 2) #SMA
        df_use.loc[:, used_col_name + 'Avg'] = round(df_use.groupby('team')[used_col_name].transform(lambda x: x.ewm(span=AVERAGE_GAMES,adjust=False).mean(AVERAGE_GAMES).shift().bfill()), 2) #EMA                                                               
        
        
    # These are the columns that will be used to calculate their moving averages then dropped after
    customize_columns = [#'corsiPercentage',
                   'penaltiesFor','penaltiesAgainst',
                   #'takeawaysFor',
                   #'giveawaysFor',      
                   'goalDiff',
                   #'faceOffsWonPct',
                   'hitsDiff',
                   #'shotsOnGoalDiff', 
                   'pointsFromGame',
                   #'xGoalsPercentage',
                   #'PDO',
                   'fenwickPercentage',
                   #'missedShotsFor',
                   #'blockedShotAttemptsFor',
                   #'blockedShotAttemptsAgainst',
                   'reboundsFor',
                   #'mediumDangerShotsFor',
                   #'highDangerShotsFor',
                   #'dZoneGiveawaysFor',
                   #'scoreFlurryAdjustedTotalShotCreditFor'
                   ]
    
    
    # Create the Average columns and create list for the Prediction sheet
    new_col_list = [] 
    for index, item in enumerate(customize_columns):
        calculate_avg_stats_per_game(df_trimmed,item,AVERAGE_GAMES)
        new_col_list.append(item + 'Avg')
        
    # Filter out Ties and early season games less than moving average
    df_trimmed = df_trimmed.query("gamesPlayed > @AVERAGE_GAMES")
    
    # Remove columns not needed after filtering
    df_trimmed = df_trimmed.drop(columns=['seasonPointTotal','gamesPlayed'])
    
   
    df_most_recent_game = df_trimmed.groupby('team')['gameDate'].last() # Find the most recent game for each team

     
    # Split into Home and Away tables  
    df_home = df_trimmed.query("home_or_away == 'HOME'")
    df_home = df_home.drop(columns=customize_columns, axis=1)
    df_away = df_trimmed.query("home_or_away == 'AWAY'")
    df_away = df_away.drop(columns=customize_columns, axis=1)
 
    #df_home.to_csv('Home.csv',index=False)
    #df_away.to_csv('Away.csv',index=False)
    
    df_merged = pd.merge(
        df_home,
        df_away,
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
    
    
    df_merged.loc[:,'penaltiesForTotal'] = round(df_merged['penaltiesForAvg_Home'] + df_merged['penaltiesAgainstAvg_Away'],2) # Penalties served by Home team
    df_merged.loc[:,'penaltiesAgainstTotal'] = round(df_merged['penaltiesAgainstAvg_Home'] + df_merged['penaltiesForAvg_Away'],2) # Power Play Home Team
    #df_merged.loc[:,'turnoversFor'] = round(df_merged['takeawaysForAvg_Home'] + df_merged['giveawaysForAvg_Away'],2)  
    #df_merged.loc[:,'turnoversAgainst'] = round(df_merged['giveawaysForAvg_Home'] + df_merged['takeawaysForAvg_Away'],2) 
    #df_merged.loc[:,'faceOffsWonPctDiff'] = round((df_merged['faceOffsWonPctAvg_Home'] - df_merged['faceOffsWonPctAvg_Away'])*100,2)
    
    


    ### Create Input file to feed into model for current day games
    
    
    new_col_list.insert(0,'team')
    new_col_list.insert(1,'gameDate')
    new_col_list.insert(2, 'seasonPointsPerGame')
    
    
    # Create list of the most recent data for each team
    df_merged2 = pd.merge(
        df_most_recent_game,
        df_trimmed,
        how='inner',
        on=['gameDate','team'],
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
    
    # Take only the input columns needed from data  
    df_most_recent_game_trimmed = df_merged2.reindex(new_col_list, axis='columns').copy()
  
    # List of all correct team acronyms
    team_acronym = ['ANA','BOS','BUF','CAR','CBJ','CGY','CHI','COL','DAL','DET','EDM','FLA','LAK','MIN','MTL',
                   'NJD','NSH','NYI','NYR','OTT','PHI','PIT','SEA','SJS','STL','TBL','TOR','UTH','VAN','VGK','WPG','WSH']
    
    # Current day games   [AWAY, HOME]
    #current_slate =  [('MTL','DET'),('BUF','TBL'),('NYI','NJD'),('NSH','PIT')]
    
    # Import the 2024 season schedule
    df_schedule = pd.read_csv('NHL_Schedule_2024.csv')
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

    # Merge Away team data into dataframe
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
    
    # Merge Home team data into dataframe
    df_merged4 = pd.merge(
            df_merged3,
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



    # Calculate some new fields
    df_merged4.loc[:,'penaltiesForTotal'] = round(df_merged4['penaltiesForAvg_Home'] + df_merged4['penaltiesAgainstAvg_Away'],2)     
    df_merged4.loc[:,'penaltiesAgainstTotal'] = round(df_merged4['penaltiesAgainstAvg_Home'] + df_merged4['penaltiesForAvg_Away'],2)   
    #df_merged4.loc[:,'turnoversFor'] = round(df_merged4['takeawaysForAvg_Home'] + df_merged4['giveawaysForAvg_Away'],2)  
    #df_merged4.loc[:,'turnoversAgainst'] = round(df_merged4['giveawaysForAvg_Home'] + df_merged4['takeawaysForAvg_Away'],2) 
    #df_merged4.loc[:,'faceOffsWonPctDiff'] = round((df_merged4['faceOffsWonPctAvg_Home'] - df_merged4['faceOffsWonPctAvg_Away'])*100,2)


    # Discard un-needed fields from training data dataframe
    discard_fields_train = ['opposingTeam_Home','opposingTeam_Away',
                          #'takeawaysForAvg_Home','takeawaysForAvg_Away',
                          #'giveawaysForAvg_Away','giveawaysForAvg_Home',
                          #'faceOffsWonPctAvg_Home','faceOffsWonPctAvg_Away',
                          'gameDate_Home','gameDate_Away',             
                          'penaltiesForAvg_Home','penaltiesAgainstAvg_Away','penaltiesAgainstAvg_Home','penaltiesForAvg_Away'
                          ]
    
    
    df_train_data = df_merged.drop(columns= discard_fields_train, axis=1)
    
    df_train_data.to_csv('NHL_Data_All_Games_Transformed.csv',index=False)
    
    # Discared un-needed fields from Prediction data dataframe
    discard_fields_predict = [
                     #'takeawaysForAvg_Home','takeawaysForAvg_Away',
                     #'giveawaysForAvg_Home', 'giveawaysForAvg_Away',      
                     #'faceOffsWonPctAvg_Home','faceOffsWonPctAvg_Away',
                     'gameDate_Home','gameDate_Away',                  
                     'penaltiesForAvg_Home','penaltiesForAvg_Away','penaltiesAgainstAvg_Home','penaltiesAgainstAvg_Away'
                     ]
    
    df_predict_data = df_merged4.drop(columns= discard_fields_predict, axis=1)
    
    df_predict_data.to_csv('NHL_Data_All_Games_Predict.csv',index=False)

    




transform_data('all_teams.csv')

