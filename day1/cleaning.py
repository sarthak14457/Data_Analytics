import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import statistics
import mysql.connector 

def checkGross(x):
    x=str(x).replace('$','').replace(',','').strip()
    if 'M' in x.upper():
       return float(x.upper().replace('M',''))*1_000_000
    if 'K' in x.upper():
        return float(x.upper().replace('K',''))*1_000
    else:
        try:
            return float(x)
        except:
            return 0
def firstFunc():
    df=pd.read_csv("/Users/sarthak/Documents/all/protfolio/Herald frontend project/practice/data analytics/day1/movies 2.csv",thousands=',',dtype={'Gross':str,'VOTES':float})
    # df.dropna()
    df.dropna(subset=['Gross'],inplace=True)
    df['VOTES']=df['VOTES'].fillna(0)
    df['GROSS']=df['Gross'].fillna(0)
    df['GENRE']=df['GENRE'].apply(lambda x: x.center(50) if isinstance(x,str) else x)
    # df['YEAR']=pd.to_numeric(df['YEAR'],errors='coerce')

    # df['MOVIES']=df['MOVIES'].to_string()
    df.dropna(subset=['GENRE','VOTES',"Gross",'RunTime','ONE-LINE','RATING'],inplace=True)
    df['VOTES']=df['VOTES'].fillna(0)
    df['Gross']=df['Gross'].apply(checkGross).astype(int)
    return df
firstFunc()

def db_connect(df):
    conn=None
    cursor=None
    try:
        conn=mysql.connector.connect(host='localhost',user='root',password='',port=3307)
        cursor=conn.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS cleaned_data")
        # print('sql created')
        cursor.execute("USE cleaned_data")
       
        cursor.execute("""CREATE TABLE IF NOT EXISTS data(SN INT PRIMARY KEY AUTO_INCREMENT,MOVIES VARCHAR(150) NOT NULL UNIQUE,VOTES FLOAT NOT NULL,RATING FLOAT NOT NULL,GROSS INT NOT NULL )""")
        cursor.execute("SELECT MOVIES FROM data")
        # print("Table created")
        """We cannot directly send dataframe data to database so we used list method"""
        data_to_insert=list(df[['MOVIES','VOTES','RATING','Gross']].itertuples(index=False,name=None))
        '''Since we should not repeat cleaned data which should be stored in database i simply used this method'''
        existing_movies=set(row[0] for row in cursor.fetchall())
        fetched_movie=[row for row in data_to_insert if row[0] not in existing_movies]
        """If new row is spotted the data is only added after that"""
        if fetched_movie:
            cursor.executemany("""INSERT IGNORE INTO data(MOVIES,VOTES,RATING,GROSS) VALUES(%s,%s,%s,%s)""",fetched_movie)
            conn.commit()
            print("Data Added")
        else:
            print("not added")
    except mysql.connector.Error as e:
        print(e) 
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()
df=firstFunc()
db_connect(df)