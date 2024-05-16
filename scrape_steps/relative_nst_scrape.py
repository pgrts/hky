import requests
import pandas as pd
import time
from bs4 import BeautifulSoup
from html.parser import HTMLParser

csv_filename = "C:/Users/pgrts/Desktop/python/scrape_steps/full_team_rosters.csv"
three_5v5sva_on_ice_relative_teammates = \
"https://www.naturalstattrick.com/playerreport.php?fromseason=20212022&thruseason=20232024&stype=2&sit=sva&stdoi=oi&rate=r&v=t&playerid="


df=pd.read_csv(csv_filename, encoding = 'cp1252',usecols=['playerId'])
x = 0
    for ind in df.index:
        while x < 5:
            player = str(df['playerId'][ind])
            filename = player + "_teammates.csv"
            full_url = three_5v5sva_on_ice_relative_teammates + player
            df2 = pd.read_html(full_url, header=0, index_col = 0, na_values=[""])[0]
            df2.to_csv(filename)
        x+=1
'''


current_season_start = 2023
num_seasons = 24 #length of seasons list
seasons = [""] * num_seasons #2000 thru 2024
start = current_season_start - num_seasons + 1 #2000

while start < current_season_start:
    for i in range(num_seasons):
        seasons[i] = str(start) + str(start+1)
        start += 1

#set season range
num_seasons_to_fetch = 2 #set to 1 less than number of seasons you want
second_season = seasons[num_seasons - 1] #current season end
first_season = seasons[num_seasons - num_seasons_to_fetch - 1] 

#url
first_part = first_season + "&thruseason=" + second_season

#url
regular_season = "2"
playoffs = "3"
second_part = "&stype=" + regular_season

    

#url
third_part = "&sit="
third_array = ["all", "5v5", "sva", "pp", "5v4", "pk", "4v5", "3v3", "enf", "ena"]

#url
fourth_part = "&stdoi="
fourth_array = ['oi', 'std'] #on ice, indiv

fifth_part = "&rate="
fifth_array = ['r', 'y', 'n'] #relative, rates, counts


sixth_part = "&v="
sixth_array = ['t','o','p','s'] #team,opposit,playersummary,scoring


URL = "https://www.naturalstattrick.com/playerreport.php?fromseason="

one_url = URL + first_part + second_part + third_part + "sva" \
+ fourth_part + "oi" + fifth_part + "r" + sixth_part + "t" + "&playerid="
    
print(one_url)

three_5v5sva_on_ice_relative_teammates = \
"https://www.naturalstattrick.com/playerreport.php?fromseason=20212022&thruseason=20232024&stype=2&sit=sva&stdoi=oi&rate=r&v=t&playerid="

two_5v5sva_on_ice_relative_teammates = \
"https://www.naturalstattrick.com/playerreport.php?fromseason=20222023&thruseason=20232024&stype=2&sit=sva&stdoi=oi&rate=r&v=t&playerid="

two_5v4_on_ice_relative_opposition = \
"https://www.naturalstattrick.com/playerreport.php?fromseason=20222023&thruseason=20232024&stype=2&sit=5v4&stdoi=oi&rate=r&v=t&playerid="

two_4v5_on_ice_relative_teammates = \
"https://www.naturalstattrick.com/playerreport.php?fromseason=20222023&thruseason=20232024&stype=2&sit=4v5&stdoi=oi&rate=r&v=t&playerid="


'''

