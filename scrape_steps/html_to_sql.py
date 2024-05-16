import pandas as pd
import psycopg2
from bs4 import BeautifulSoup
from html.parser import HTMLParser
import sqlalchemy as db
import sqlalchemy.orm
from sqlalchemy import URL, create_engine, select
from psycopg2 import sql
import psycopg2.extras as extras

team = 'BOS'
team_lowercase = 'bos'
filename = 'soupy_' + team + '.html'
csv_filename = team_lowercase  + '_line_stats_sva.csv'
the_table = team_lowercase + '_lines_sva'

url_hky = URL.create(
    "postgresql+psycopg2",
    username="postgres",
    password="p33Gritz!!",  
    host="localhost",
    port="5432",
    database="hockey",
)
try:
    engine = db.create_engine(url_hky)
except:
    print("I am unable to connect to the engine")
    
try:
    conn = psycopg2.connect(
        host="localhost",
        database="hockey",
        user="postgres",
        password="p33Gritz!!",
        port="5432"
    )
except:
    print("I am unable to connect to the psycopg2")
    
with open(filename) as fp:
    bs=BeautifulSoup(fp, 'html.parser')
    columns = ["Player_1",
	"Player_2",
	"Player_3",
	"GP",
	"TOI",
	"CF",
	"CA",
	"CF_Percent",
	"CF_Percent_Rel",
	"FF",
	"FA",
	"FF_Percent",
	"FF_Percent_Rel",
	"SF",
	"SA",
	"SF_Percent",
	"SF_Percent_Rel",
	"GF",
	"GA",
	"GF_Percent",
	"GF_Percent_Rel",
	"xGF",
	"xGA",
	"xGF_Percent",
	"xGF_Percent_Rel",
	"SCF",
	"SCA",
	"SCF_Percent",
	"SCF_Percent_Rel",
	"HDCF",
	"HDCA",
	"HDCF_Percent",
	"HDCF_Percent_Rel",
	"Rush_Attempts_For",
	"Rush_Attempts_Against",
	"Rush_Attempt_Percent",
	"Rush_Attempt_Percent_Rel",
	"Rebound_Attempts_For",
	"Rebound_Attempts_Against",
	"Rebound_Attempt_Percent",
	"Rebound_Attempt_Percent_Rel",
	"Off_Zone_Faceoffs",
	"Neu_Zone_Faceoffs",
	"Def_Zone_Faceoffs",
	"Off_Zone_Faceoff_Percent"]
    
    data = [e.text for e in bs.find_all('td')]
    start = 0
    table= []
    #loop through entire data
    while start+len(columns) <= len(data):
        player = []
        #use length of columns as iteration stop point to get list of info for 1 player 
        for i in range(start,start+len(columns)):
            player.append(data[i])
        #add player row to list
        table.append(player)
        #start at next player
        start += len(columns)
    df = pd.DataFrame(table, columns = columns)

#df.to_csv(csv_filename, header=True)
#df2 = pd.read_csv(csv_filename, skiprows=1, header=None)
#print(df2)
df2 = pd.read_html(url, header=0, index_col = 0, na_values=[""])[0]
#df.to_sql(the_table, engine, if_exists='replace')
    
