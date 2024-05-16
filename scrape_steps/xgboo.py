from bs4 import BeautifulSoup as bs
import requests
import json
import pandas as pd
import os
import sqlalchemy as db
import sqlalchemy.orm
import psycopg2
from sqlalchemy import URL, create_engine, select, text
from psycopg2 import sql
from datetime import date, datetime, timedelta
from io import StringIO
import matplotlib.pyplot as plt
import numpy as np
import time
from time import perf_counter
import math
import seaborn as sns

import xgboost as xgb
from xgboost import XGBRegressor
from sklearn.metrics import mean_squared_error

color_pal = sns.color_palette()
plt.style.use('fivethirtyeight')



url_hky = URL.create(
    "postgresql+psycopg2",
    username="postgres",
    password="p33Gritz!!",  
    host="localhost",
    port="5432",
    database="pbp5",
)
try:
    engine = db.create_engine(url_hky)
except:
    print("I am unable to connect to the engine")

try:
    conn = psycopg2.connect(
        host="localhost",
        database="pbp5",
        user="postgres",
        password="p33Gritz!!",
        port="5432"
    )
except:
    print("I am unable to connect to the psycopg2")


def create_features(df):
    df = df.copy()
    df['timeSinceLastEvent'] = df.index.timeSinceLastEvent
    df['shotDistance'] = df.index.arenaAdjustedShotDistance
    df['restDifference'] = df.index.averageRestDifference
    df['offWing'] = df.index.offWing
    #df['reboundAngleChange'] = df.index.shotAnglePlusRebound
    #df['y_coord'] = df.index.arenaAdjustedYCordAbs
    #df['x_coord'] = df.index.arenaAdjustedXCordABS
    #df['shotRush'] = df.index.shotRush
    #df['shotAnglePlusReboundSpeed'] = df.index.shotAnglePlusReboundSpeed
    
    #df['shotAngleReboundRoyalRoad'] = df.index.shotAngleReboundRoyalRoad
    #df['speedFromLastEvent'] = df.index.speedFromLastEvent

EVQuery = '''select * from shots_recent
where 5 in ("homeSkatersOnIce","awaySkatersOnIce");'''

EVRebQuery = '''select * from shots_recent
where 5 in ("homeSkatersOnIce","awaySkatersOnIce")
and "shotRebound" = 1;'''

EVnoRebQuery = '''select * from shots_recent
where 5 in ("homeSkatersOnIce","awaySkatersOnIce")
and "shotRebound" = 0;'''

EVRebShotQuery = '''select * from shots_recent
where 5 in ("homeSkatersOnIce","awaySkatersOnIce")
and "shotRebound" = 1;'''

query_2022 = '''select * from "shots_2022" where 5 in ("homeSkatersOnIce","awaySkatersOnIce");'''
query_early = '''select * from "shots_recent" where 5 in ("homeSkatersOnIce","awaySkatersOnIce")
and "season" in (2018,2019,2020,2021);'''

#df = pd.read_pickle('ev.pkl')

df = pd.read_sql_query(query_2022,engine)
df2 = pd.read_sql_query(query_early,engine)


#rebound
#FEATURES = ['arenaAdjustedShotDistance', 'arenaAdjustedYCordAbs',  'timeSinceLastEvent', 'shotAngleReboundRoyalRoad']

#norebound
FEATURES = ['timeSinceLastEvent','arenaAdjustedShotDistance','arenaAdjustedYCordAbs','offWing']

#allshots
#FEATURES = ['arenaAdjustedYCordAbs', 'timeSinceLastEvent', 'distanceFromLastEvent', 'arenaAdjustedShotDistance', 'offWing']

TARGET = 'goal'

#train = df.loc[df['shotID'] > 122026]
#test = df.loc[df['shotID'] < 122027]

train = df2
test = df

X_train = train[FEATURES]
y_train = train[TARGET]

X_test = test[FEATURES]
y_test = test[TARGET]

reg = xgb.XGBRegressor(base_score=0.5, booster='gbtree',    
                       n_estimators=10000,
                       early_stopping_rounds=50,
                       objective='reg:squarederror',
                       max_depth=3,
                       learning_rate=0.01)
reg.fit(X_train, y_train,
        eval_set=[(X_train, y_train), (X_test, y_test)],
        verbose=100)

fi = pd.DataFrame(data=reg.feature_importances_,
             index=reg.feature_names_in_,
             columns=['importance'])
fi.sort_values('importance').plot(kind='barh', title='Feature Importance')
plt.show()
'''
test['prediction'] = reg.predict(X_test)
df['xGpredict'] = test['prediction']
df['xGDiff'] = (df['xGpredict'] - df['xGoal']).abs()
print(df['xGDiff'].mean(skipna=True))
df[['xGoal','xGpredict','xGDiff']].to_csv('xGnorebTest.csv')


x = df['shotDistance']

y = df['xGoal']
y3 = df[TARGET]
y2 = df['xGpredict']

plt.scatter(x, y,color='blue')
plt.scatter(x, y2,color='red')
plt.scatter(x, y3,color='yellow')

plt.show()


df = df.merge(test[['prediction']], how='left', left_index=True, right_index=True)
ax = df[['goal']].plot(figsize=(15, 5))
df['prediction'].plot(ax=ax, style='.')
plt.legend(['Truth Data', 'Predictions'])
ax.set_title('Raw Data and Prediction')
plt.show()


score = np.sqrt(mean_squared_error(test[TARGET], test['prediction']))
print(f'RMSE Score on Test set: {score:0.2f}')

test['error'] = np.abs(test[TARGET] - test['prediction'])
test.groupby(['shotID'])['error'].mean().sort_values(ascending=False).head(10)
'''
'''
fi = pd.DataFrame(data=reg.feature_importances_,
             index=reg.feature_names_in_,
             columns=['importance'])
fi.sort_values('importance').plot(kind='barh', title='Feature Importance')
plt.show()
'''

