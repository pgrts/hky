from datetime import date, timedelta, datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
from html.parser import HTMLParser
import sqlalchemy as db
import sqlalchemy.orm
import psycopg2
from sqlalchemy import URL, create_engine, select, text
from psycopg2 import sql
import matplotlib.pyplot as plt
import numpy as np

url_hky = URL.create(
    "postgresql+psycopg2",
    username="postgres",
    password="p33Gritz!!",  
    host="localhost",
    port="5432",
    database="playbyplay",
)
try:
    engine = db.create_engine(url_hky)
except:
    print("I am unable to connect to the engine")

try:
    conn = psycopg2.connect(
        host="localhost",
        database="playbyplay",
        user="postgres",
        password="p33Gritz!!",
        port="5432"
    )
except:
    print("I am unable to connect to the psycopg2")
    
cur = conn.cursor()
query = '''select * from "2024-03-052";'''
#cur.execute('select * from "2024-03-052";')
#cur.execute(query)
    
yesterday  = date.today() - timedelta(days = 1)
values = {'table' : yesterday.strftime('%B' '%d' '%y'), 'game' : '2023020982'}

shots = '''
select * from "March0524"
where "game_id" = %(game)s and 
"event_type" = 'SHOT' or
"event_type" = 'BLOCK' or 
"event_type" = 'MISS' or 
"event_type" = 'GOAL';'''


cur.execute(shots, values)



'''
#df = pd.DataFrame(_sql_to_data(shots, date['date']))#, game['game']))
#df.to_csv('shotx.csv')
#date = {'date' : yesterday}
#game = {'game' : '2023020982'}
def _sql_to_data(sql, values):
    cur.execute(sql, values)
'''
