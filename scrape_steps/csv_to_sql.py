import requests
import pandas as pd
import sqlalchemy as db
import sqlalchemy.orm
import psycopg2
from sqlalchemy import URL, create_engine, select
from psycopg2 import sql
import psycopg2.extras as extras


csv_filename = "C:/Users/pgrts/Desktop/python/scrape_steps/" + "full_team_rosters.csv"
the_table = "full_team_rosters"

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

df = pd.read_csv(csv_filename, encoding = 'cp1252')
df.to_sql(the_table, engine, if_exists='replace')
