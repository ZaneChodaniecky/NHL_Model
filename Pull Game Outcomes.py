# -*- coding: utf-8 -*-
"""
Created on Tue Sep 24 11:01:21 2024

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
import json
import requests


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
df_trimmed = df_original.query("situation == 'all' & playoffGame == 0 & season >= 2020")

df_gameID = df_trimmed['gameId']

unique_gameID = df_gameID.unique()


nhl_URL = 'https://api-web.nhle.com/v1/wsc/game-story/'

listy = ['2020020008','2020020164','2020020342']

outcomes = []
for _ in unique_gameID:    
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

df_outcomes = pd.DataFrame(outcomes, columns=['gameId','away_team','away_score','home_team','home_score'])

df_outcomes.head()

df_outcomes.loc[:,'home_win'] = np.where(df_outcomes['home_score'] >= df_outcomes['away_score'], 1, 0) 

df_outcomes.to_csv('Win_History.csv',index=False)
