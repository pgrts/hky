import requests
from bs4 import BeautifulSoup
import pandas as pd
from html.parser import HTMLParser
import sqlalchemy as db
import sqlalchemy.orm
import psycopg2
from sqlalchemy import URL, create_engine, select
from psycopg2 import sql
import psycopg2.extras as extras


url = "https://www.naturalstattrick.com/pairings.php?fromseason=20232024&thruseason=20232024&stype=2&sit=sva&score=w1&rate=r&team=ALL&loc=B&toi=15&gpfilt=none&fd=&td=&tgp=410"
req = requests.get(url)
soup = BeautifulSoup(req.content, 'lxml')
csv_filename = "relative_d_pairs.csv"
the_table = "relative_d_pairs"

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

    
df2 = pd.read_html(url, header=0, index_col = 0, na_values=[""])[0]
df2.to_csv(csv_filename)
df2.to_sql(the_table, engine, if_exists='replace')
