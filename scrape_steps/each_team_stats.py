import requests
import json
import csv
import pandas as pd
import os
from html.parser import HTMLParser
#import fsspec

#df = pd.read_csv("C://Users/pgrts/Desktop/python/scrape_steps/ari_skaters.csv")
#roster = df.to_dict()
#print(df.to_dict())
nhl_team_array2 = ["ANA", "ARI", "BOS", "BUF", "CGY", "CAR", "CHI", "COL", "CBJ",
    "DAL", "DET", "EDM", "FLA", "LAK", "MIN", "MTL", "NSH", "NJD", "NYI", "NYR",
    "OTT", "PHI", "PIT", "SJS", "SEA", "STL", "TBL", "TOR", "VAN", "VGK", "WPG", "WSH"]
names = ['firstName', 'lastName']

for team in nhl_team_array2:
    API_players = "https://api-web.nhle.com/v1/club-stats/" + team + "/20232024/2"
    response = json.loads(requests.get(API_players).text)
    df = pd.DataFrame.from_dict(response["skaters"])
    
    for name in names:
        new_df = pd.DataFrame(list(df[name]))
        df[name] = new_df['default']
        
    df.to_csv(team + "_stats.csv")
