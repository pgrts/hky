import pandas as pd
import requests
import psycopg2
from bs4 import BeautifulSoup
from html.parser import HTMLParser
import sqlalchemy as db
import sqlalchemy.orm
from sqlalchemy import URL, create_engine, select
from psycopg2 import sql
import psycopg2.extras as extras
from io import StringIO

team = 'COL'
team_lowercase = 'col'
filename = 'soupy_' + team + '.html'
csv_filename = team_lowercase  + '_line_stats_sva.csv'
the_table = team_lowercase + '_lines_sva'
url = "https://www.naturalstattrick.com/teamreport.php?team=" + team

#tables[19] = lines5v5
#sva = tables[20]

req = requests.get(url)
soup=BeautifulSoup(req.content, 'html.parser')
tables = soup.find_all("table")

df = pd.read_html(StringIO(str(tables[20])), header=0, index_col = 0, na_values=[""])[0]
df.to_csv(csv_filename)

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
try:
    df.to_sql(the_table, engine, if_exists='replace')
    print("SQL SUCCESS")
except:
    print("SQL FAILURE")
    
'''
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

#df.to_csv(csv_filename, header=True)
#df2 = pd.read_csv(csv_filename, skiprows=1, header=None)
#print(df2)
df2 = pd.read_html(url, header=0, index_col = 0, na_values=[""])[0]
df2.to_sql(the_table, engine, if_exists='replace')
   ''' 
