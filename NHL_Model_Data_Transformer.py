# -*- coding: utf-8 -*-
"""
Created on Wed Aug 21 12:45:46 2024

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


def transform_data(fileName):
    
    # Game data comes from https://moneypuck.com/data.htm
    # Schedule data comes from https://media.nhl.com/public/news/18238


    # Define constants
    AVERAGE_GAMES = 6 # Number of games used in moving average
    
    if socket.gethostname() == 'zchodani-p-l01':
        file_directory = r"C:\Users\zchodaniecky\OneDrive - Franklin Templeton\Documents\Python\NHL_data"
    else:
        file_directory = r"C:\Users\zanec\OneDrive\Documents\Python\NHL_data"
             
    os.chdir(file_directory)
    
    
    df_original = pd.read_csv('all_teams.csv')
    
        # Filter for columns that we want
    keep_columns = ['team','season','gameId','playoffGame','opposingTeam','home_or_away','gameDate','situation',
                   'goalsFor',
                   'penaltiesFor','penaltiesAgainst',
                   'hitsFor','hitsAgainst',
                   #'takeawaysFor','giveawaysFor',
                   'goalsAgainst',
                   #'faceOffsWonFor','faceOffsWonAgainst',         
                   #'shotsOnGoalFor','shotsOnGoalAgainst',
                   'savedShotsOnGoalFor','savedShotsOnGoalAgainst',                           
                   #'corsiPercentage',
                   #'xGoalsPercentage',
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
    
    df_trimmed = df_original[keep_columns].copy()
    df_trimmed = df_trimmed.query("situation == 'all' & playoffGame == 0 & season >= 2020")
    
    
    df_trimmed.sort_values(by=['gameDate','team'], ascending= [True, True], inplace=True) # Sort by date so next step calcs correct
    df_trimmed.drop_duplicates(subset=['gameDate','team'], keep='first', inplace=True) # Drop rows on dates that team did not play
    
    # Create custom columns
    outcomeConditions = [
        (df_trimmed['goalsFor'] > df_trimmed['goalsAgainst']),
        (df_trimmed['goalsAgainst'] > df_trimmed['goalsFor']),
        (df_trimmed['goalsAgainst'] == df_trimmed['goalsFor'])
        ]
    # Win and Loss do not count shootout, all shootouts are considered a tie
    outcomeValues = ['WIN','LOSS','TIE']
    
    # Create custom columns
    df_trimmed.loc[:,'win_or_lose'] = np.select(outcomeConditions,outcomeValues)
    df_trimmed.loc[:,'win'] = np.where(df_trimmed['win_or_lose'] == 'WIN', 1, 0) 
    df_trimmed.loc[:,'seasonWin'] = df_trimmed.groupby(['season','team'])['win'].cumsum(axis=0)
    df_trimmed.loc[:,'tie'] = np.where(df_trimmed['win_or_lose'] == 'TIE', 1, 0)
    df_trimmed.loc[:,'seasonTie'] = df_trimmed.groupby(['season','team'])['tie'].cumsum(axis=0)
    df_trimmed.loc[:,'pointsFromGame'] = np.where(df_trimmed['win_or_lose'] == 'WIN', 2,(np.where(df_trimmed['win_or_lose'] == 'TIE', 1, 0)))
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
    df_trimmed = df_trimmed.drop(columns=['playoffGame', 'win','seasonWin','tie','seasonTie','situation','season',
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
    df_trimmed = df_trimmed.query("win_or_lose != 'TIE' & gamesPlayed > @AVERAGE_GAMES")
    
    # Remove columns not needed after filtering
    df_trimmed = df_trimmed.drop(columns=['seasonPointTotal','gamesPlayed'])
    
    # Convert win_or_lose column to binary
    df_trimmed.loc[:,'win_or_lose'] = np.where(df_trimmed['win_or_lose'] == 'WIN', 1, 0) # Replace withOneHotEncode

    
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
    current_slate =  [('MTL','DET'),('BUF','TBL'),('NYI','NJD'),('NSH','PIT')]
    
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
    
    df_train_data.to_csv('NHL_Data_Transformed.csv',index=False)
    
    # Discared un-needed fields from Prediction data dataframe
    discard_fields_predict = [
                     #'takeawaysForAvg_Home','takeawaysForAvg_Away',
                     #'giveawaysForAvg_Home', 'giveawaysForAvg_Away',      
                     #'faceOffsWonPctAvg_Home','faceOffsWonPctAvg_Away',
                     'gameDate_Home','gameDate_Away',                  
                     'penaltiesForAvg_Home','penaltiesForAvg_Away','penaltiesAgainstAvg_Home','penaltiesAgainstAvg_Away'
                     ]
    
    df_predict_data = df_merged4.drop(columns= discard_fields_predict, axis=1)
    
    df_predict_data.to_csv('NHL_Data_Predict.csv',index=False)

    




transform_data('all_teams.csv')

