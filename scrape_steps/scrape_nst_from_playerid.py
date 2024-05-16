import requests
import pandas as pd
import time
from bs4 import BeautifulSoup
from html.parser import HTMLParser
from io import StringIO
#csv_filename = "C:/Users/pgrts/Desktop/python/scrape_steps/full_team_rosters.csv"
teammates5v5 = \
"https://www.naturalstattrick.com/playerreport.php?fromseason=20232024&thruseason=20232024&stype=2&sit=sva&stdoi=oi&rate=r&v=t&playerid="
teammatesPK = \
"https://www.naturalstattrick.com/playerreport.php?fromseason=20232024&thruseason=20232024&stype=2&sit=4v5&stdoi=oi&rate=r&v=t&playerid="
teammatesPP = \
"https://www.naturalstattrick.com/playerreport.php?fromseason=20232024&thruseason=20232024&stype=2&sit=5v4&stdoi=oi&rate=r&v=t&playerid="                                
#url_list = [teammates5v5, teammatesPK, teammatesPP]
#csv_list = ["_teammates5v5", "_teammatesPK", "teammatesPP"]
test_dict = {
    teammates5v5: "_teammates_5v5.csv",
    teammatesPK: "_teammatesPK.csv",
    teammatesPP: "_teammatesPP.csv"}


    
#df=pd.read_csv(csv_filename, encoding = 'cp1252',usecols=['playerId'])
#player_list = [8484145, 8481528, 8476878, 8478413, 8481522, 8479999, 8473449, 8478109, 8482175, 8482097, 8480762, 8475784, 8479420, 8477949, 8480196, 8479348, 8477365, 8480839, 8473446, 8481564, 8480035, 8482671, 8480807, 8477480, 8480045, 8476981, 8476469, 8481540, 8481523, 8477989, 8478133, 8475848, 8481093, 8481618, 8476871, 8479543, 8482749, 8483515, 8480018, 8478400, 8481058, 8482087, 8480887, 8480192, 8476875, 8475233, 8481593, 8482964, 8474596, 8478470, 8480051, 8474715, 8480220, 8476461, 8475235, 8480797, 8482159, 8480028, 8477903, 8478439, 8476872, 8480245, 8480068, 8480015, 8482142, 8475176, 8477499, 8477948, 8476372, 8471686, 8480336, 8481546, 8481178, 8481035, 8477361, 8479315, 8476960, 8478463, 8484144, 8476278, 8477450, 8477987, 8480025, 8473422, 8480252, 8475791, 8481147, 8474870, 8479383, 8480798, 8478224, 8479390, 8477482, 8481806, 8477495, 8483466, 8477034, 8476473, 8482192, 8475797, 8481568, 8479458, 8475852, 8482821, 8480205, 8482475, 8479941, 8484166, 8476346, 8476432, 8482660, 8476374, 8480893, 8479671, 8478458, 8482705, 8480074, 8481716, 8479402, 8480871, 8475790, 8479369, 8478500, 8476923, 8478460, 8478007, 8480193, 8484153, 8475842, 8480870, 8474641, 8477527, 8479368, 8473986, 8481517, 8480806, 8476934, 8482745, 8475164, 8476458, 8478873, 8478366, 8481533, 8475764, 8475462, 8481605, 8480184, 8480950, 8483490, 8480001, 8480843, 8476434, 8477479, 8477503, 8479393, 8480995, 8475714, 8480144, 8482720, 8478483, 8479318, 8482259, 8477939, 8471817, 8481582, 8475166, 8481122, 8474673, 8475906, 8478021, 8479320, 8480043, 8476931, 8474162, 8476853, 8483546, 8479982, 8474889, 8478492, 8479361, 8475760, 8476329, 8479619, 8483431, 8478474, 8482699, 8480849, 8479343, 8477021, 8481711, 8480855, 8477070, 8480008, 8477951, 8475722, 8477384, 8478408, 8476856, 8480434, 8480891, 8482655, 8479442, 8479976, 8478971, 8478872, 8480003, 8479638, 8475745, 8478498, 8479365, 8479987, 8478046, 8480880, 8473473, 8473419, 8477956, 8483505, 8478409, 8474037, 8478401, 8478443, 8475762, 8476891, 8476854, 8482511, 8479325, 8474031, 8478450, 8480280, 8476999, 8477015, 8477934, 8477998, 8474040, 8482077, 8475786, 8477406, 8475169, 8478402, 8480802, 8476454, 8470621, 8478585, 8480803, 8476879, 8479576, 8475218, 8476967, 8477498, 8475717, 8479973]

#function1(playe
def function1(player_list):
    for player in player_list:
        for key,value in test_dict.items():
            print(str((key + str(player))))
            
            df = pd.read_html(str((key + str(player))),header=0, index_col = 0, na_values=[""])[0]
            df.to_csv(str(player) + str(value))
            #commit to sql table df.to_sql(player+value)
            
        #player = str(df['playerId'][ind])
        #for url in url_list:
            #url = url + player
        #teammates_filename = player + "_5v5sva_teammates.csv"
        #df2 = pd.read_html(url+player, header=0, index_col = 0, na_values=[""])[0]
        #df2.to_csv(player + "_5v5sva_teammates.csv")

            time.sleep(2)
        time.sleep(5)
        
        #teammates_pk_url = one_PK_on_ice_relative_teammates + player
        #teammates_pk_filename = player + "_PKsva_teammates.csv"
        #df3 = pd.read_html(teammates_pk_url, header=0, index_col = 0, na_values=[""])[0]
        #df3.to_csv(teammates_pk_filename)
        #teammates_pp_url = one_PP_on_ice_relative_teammates + player
        #teammates_pp_filename = player + "_PPsva_teammates.csv"
        #df4 = pd.read_html(teammates_pp_url, header=0, index_col = 0, na_values=[""])[0]
        #df4.to_csv(teammates_pp_filename)

#scrape F lines
#scrape D pairs
#def function2(team_list):
for team in team_list:
    #team_lowercase = team.lower()
    #csv_filename = team_lowercase  + '_line_stats_sva.csv'
    #the_table = team_lowercase + '_lines_sva'
    url = "https://www.naturalstattrick.com/teamreport.php?season=20232024&team=" + str(team) + "&stype=2"
    req = requests.get(url)
    soup=BeautifulSoup(req.content, 'html.parser')
    tables = soup.find_all("table")

    df = pd.read_html(StringIO(str(tables[20])), header=0, index_col = 0, na_values=[""])[0]
    df.to_csv(csv_filename)
    #df.to_sql(the_table, engine, if_exists='replace')

