# -*- coding: utf-8 -*-
import sqlite3 as sq
import pandas as pd
import numpy as np
import matplotlib as plt
from datetime import datetime
import scipy as sc
import pprint as pp


connection=sq.connect("C:/Users/user/Downloads/trade_db.db") #création de la connection
cursor=connection.cursor()  #création du curseur 
Tr=[] #Création du dataframe contenant les trades 


cursor.execute('SELECT * FROM Bitfinex_candle_EOSBTC_12h ORDER BY date ASC')
rows1 = cursor.fetchall()
EOSBTC12H= pd.DataFrame(rows1,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(EOSBTC12H)

cursor.execute('SELECT * FROM Bitfinex_candle_EOSBTC_14D ORDER BY date ASC')
rows2 = cursor.fetchall()
EOSBTC14D=pd.DataFrame(rows2,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(EOSBTC14D)

cursor.execute('SELECT * FROM Bitfinex_candle_EOSBTC_15m ORDER BY date ASC')
rows3 = cursor.fetchall()
EOSBTC15M=pd.DataFrame(rows3,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(EOSBTC15M)

cursor.execute('SELECT * FROM Bitfinex_candle_EOSBTC_1D ORDER BY date ASC')
rows4 = cursor.fetchall()
EOSBTC1D=pd.DataFrame(rows4,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(EOSBTC1D)

cursor.execute('SELECT * FROM Bitfinex_candle_EOSBTC_1H ORDER BY date ASC')
rows5 = cursor.fetchall()
EOSBTC1H=pd.DataFrame(rows5,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(EOSBTC1H)

cursor.execute('SELECT * FROM Bitfinex_candle_EOSBTC_1m ORDER BY date ASC')
rows6 = cursor.fetchall()
EOSBTC1M=pd.DataFrame(rows6,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(EOSBTC1M)

cursor.execute('SELECT * FROM Bitfinex_candle_EOSBTC_30m ORDER BY date ASC')
rows7 = cursor.fetchall()
EOSBTC30M=pd.DataFrame(rows7,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(EOSBTC30M)

cursor.execute('SELECT * FROM Bitfinex_candle_EOSBTC_3h ORDER BY date ASC')
rows8 = cursor.fetchall()
EOSBTC3H=pd.DataFrame(rows8,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(EOSBTC3H)

cursor.execute('SELECT * FROM Bitfinex_candle_EOSBTC_5m ORDER BY date ASC')
rows9 = cursor.fetchall()
EOSBTC5M=pd.DataFrame(rows9,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(EOSBTC5M)

cursor.execute('SELECT * FROM Bitfinex_candle_EOSBTC_6h ORDER BY date ASC')
rows10 = cursor.fetchall()
EOSBTC6H=pd.DataFrame(rows10,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(EOSBTC6H)

cursor.execute('SELECT * FROM Bitfinex_candle_EOSBTC_7D ORDER BY date ASC')
rows11 = cursor.fetchall()
EOSBTC7D=pd.DataFrame(rows11,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(EOSBTC7D)

cursor.execute('SELECT * FROM Bitfinex_candle_EOSETH_12h ORDER BY date ASC')
rows12 = cursor.fetchall()
EOSETH12H=pd.DataFrame(rows12,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(EOSETH12H)

cursor.execute('SELECT * FROM Bitfinex_candle_EOSETH_14D ORDER BY date ASC')
rows13 = cursor.fetchall()
EOSETH14D=pd.DataFrame(rows13,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(EOSETH14D)

cursor.execute('SELECT * FROM Bitfinex_candle_EOSETH_15m ORDER BY date ASC')
rows14 = cursor.fetchall()
EOSETH15M=pd.DataFrame(rows14,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(EOSETH15M)

cursor.execute('SELECT * FROM Bitfinex_candle_EOSETH_1D ORDER BY date ASC')
rows15 = cursor.fetchall()
EOSETH1D=pd.DataFrame(rows15,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(EOSETH1D)

cursor.execute('SELECT * FROM Bitfinex_candle_EOSETH_1h ORDER BY date ASC')
rows16 = cursor.fetchall()
EOSETH1H=pd.DataFrame(rows16,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(EOSETH1H)

cursor.execute('SELECT * FROM Bitfinex_candle_EOSETH_1m ORDER BY date ASC')
rows17 = cursor.fetchall()
EOSETH1M=pd.DataFrame(rows17,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(EOSETH1M)

cursor.execute('SELECT * FROM Bitfinex_candle_EOSETH_30m ORDER BY date ASC')
rows18 = cursor.fetchall()
EOSETH30M=pd.DataFrame(rows18,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(EOSETH30M)

cursor.execute('SELECT * FROM Bitfinex_candle_EOSETH_3h ORDER BY date ASC')
rows19 = cursor.fetchall()
EOSETH3H=pd.DataFrame(rows19,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(EOSETH3H)

cursor.execute('SELECT * FROM Bitfinex_candle_EOSETH_5m ORDER BY date ASC')
rows20 = cursor.fetchall()
EOSETH5M=pd.DataFrame(rows20,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(EOSETH5M)

cursor.execute('SELECT * FROM Bitfinex_candle_EOSETH_6h ORDER BY date ASC')
rows21 = cursor.fetchall()
EOSETH6H=pd.DataFrame(rows21,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(EOSETH6H)

cursor.execute('SELECT * FROM Bitfinex_candle_EOSETH_7D ORDER BY date ASC')
rows22 = cursor.fetchall()
EOSETH7D=pd.DataFrame(rows22,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(EOSETH7D)

cursor.execute('SELECT * FROM Bitfinex_candle_ETHBTC_12h ORDER BY date ASC')
rows23 = cursor.fetchall()
ETHBTC12H=pd.DataFrame(rows23,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(ETHBTC12H)

cursor.execute('SELECT * FROM Bitfinex_candle_ETHBTC_14D ORDER BY date ASC')
rows24 = cursor.fetchall()
ETHBTC14D=pd.DataFrame(rows24,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(ETHBTC14D)

cursor.execute('SELECT * FROM Bitfinex_candle_ETHBTC_15m ORDER BY date ASC')
rows25 = cursor.fetchall()
ETHBTC15M=pd.DataFrame(rows25,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(ETHBTC15M)

cursor.execute('SELECT * FROM Bitfinex_candle_ETHBTC_1D ORDER BY date ASC')
rows26 = cursor.fetchall()
ETHBTC1D=pd.DataFrame(rows26,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(ETHBTC1D)

cursor.execute('SELECT * FROM Bitfinex_candle_ETHBTC_1h ORDER BY date ASC')
rows27 = cursor.fetchall()
ETHBTC1H=pd.DataFrame(rows27,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(ETHBTC1H)

cursor.execute('SELECT * FROM Bitfinex_candle_ETHBTC_1m ORDER BY date ASC')
rows28 = cursor.fetchall()
ETHBTC1M=pd.DataFrame(rows28,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(ETHBTC1M)

cursor.execute('SELECT * FROM Bitfinex_candle_ETHBTC_30m ORDER BY date ASC')
rows29 = cursor.fetchall()
ETHBTC30M=pd.DataFrame(rows29,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(ETHBTC30M)

cursor.execute('SELECT * FROM Bitfinex_candle_ETHBTC_3h ORDER BY date ASC')
rows30 = cursor.fetchall()
ETHBTC3H=pd.DataFrame(rows30,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(ETHBTC3H)

cursor.execute('SELECT * FROM Bitfinex_candle_ETHBTC_5m ORDER BY date ASC')
rows31 = cursor.fetchall()
ETHBTC5M=pd.DataFrame(rows31,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(ETHBTC5M)

cursor.execute('SELECT * FROM Bitfinex_candle_ETHBTC_6h ORDER BY date ASC')
rows32 = cursor.fetchall()
ETHBTC6H=pd.DataFrame(rows32,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(ETHBTC6H)

cursor.execute('SELECT * FROM Bitfinex_candle_ETHBTC_7D ORDER BY date ASC')
rows33 = cursor.fetchall()
ETHBTC7D=pd.DataFrame(rows33,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(ETHBTC7D)

cursor.execute('SELECT * FROM Bitfinex_candle_LTCBTC_12h ORDER BY date ASC')
rows34 = cursor.fetchall()
LTCBTC12H=pd.DataFrame(rows34,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(LTCBTC12H)

cursor.execute('SELECT * FROM Bitfinex_candle_LTCBTC_14D ORDER BY date ASC')
rows35 = cursor.fetchall()
LTCBTC14D=pd.DataFrame(rows35,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(LTCBTC14D)

cursor.execute('SELECT * FROM Bitfinex_candle_LTCBTC_15m ORDER BY date ASC')
rows36 = cursor.fetchall()
LTCBTC15M=pd.DataFrame(rows36,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(LTCBTC15M)

cursor.execute('SELECT * FROM Bitfinex_candle_LTCBTC_1D ORDER BY date ASC')
rows37 = cursor.fetchall()
LTCBTC1D=pd.DataFrame(rows37,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(LTCBTC1D)

cursor.execute('SELECT * FROM Bitfinex_candle_LTCBTC_1h ORDER BY date ASC')
rows38 = cursor.fetchall()
LTCBTC1H=pd.DataFrame(rows38,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(LTCBTC1H)

cursor.execute('SELECT * FROM Bitfinex_candle_LTCBTC_1m ORDER BY date ASC')
rows39 = cursor.fetchall()
LTCBTC1M=pd.DataFrame(rows39,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(LTCBTC1M)

cursor.execute('SELECT * FROM Bitfinex_candle_LTCBTC_30m ORDER BY date ASC')
rows40 = cursor.fetchall()
LTCBTC30M=pd.DataFrame(rows40,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(LTCBTC30M)

cursor.execute('SELECT * FROM Bitfinex_candle_LTCBTC_3h ORDER BY date ASC')
rows41 = cursor.fetchall()
LTCBTC3H=pd.DataFrame(rows41,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(LTCBTC3H)

cursor.execute('SELECT * FROM Bitfinex_candle_LTCBTC_5m ORDER BY date ASC')
rows42 = cursor.fetchall()
LTCBTC5M=pd.DataFrame(rows42,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(LTCBTC5M)

cursor.execute('SELECT * FROM Bitfinex_candle_LTCBTC_6h ORDER BY date ASC')
rows43 = cursor.fetchall()
LTCBTC6H=pd.DataFrame(rows43,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(LTCBTC6H)

cursor.execute('SELECT * FROM Bitfinex_candle_LTCBTC_7D ORDER BY date ASC')
rows44 = cursor.fetchall()
LTCBTC7D=pd.DataFrame(rows44,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(LTCBTC7D)

cursor.execute('SELECT * FROM Bitfinex_candle_XRPBTC_12h ORDER BY date ASC')
rows45 = cursor.fetchall()
XRPBTC12H=pd.DataFrame(rows45,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(XRPBTC12H)

cursor.execute('SELECT * FROM Bitfinex_candle_XRPBTC_14D ORDER BY date ASC')
rows46 = cursor.fetchall()
XRPBTC14D=pd.DataFrame(rows46,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(XRPBTC14D)

cursor.execute('SELECT * FROM Bitfinex_candle_XRPBTC_15m ORDER BY date ASC')
rows47 = cursor.fetchall()
XRPBTC15M=pd.DataFrame(rows47,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(XRPBTC15M)

cursor.execute('SELECT * FROM Bitfinex_candle_XRPBTC_1D ORDER BY date ASC')
rows48 = cursor.fetchall()
XRPBTC1D=pd.DataFrame(rows48,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(XRPBTC1D)

cursor.execute('SELECT * FROM Bitfinex_candle_XRPBTC_1h ORDER BY date ASC')
rows49 = cursor.fetchall()
XRPBTC1H=pd.DataFrame(rows49,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(XRPBTC1H)

cursor.execute('SELECT * FROM Bitfinex_candle_XRPBTC_1m ORDER BY date ASC')
rows50 = cursor.fetchall()
XRPBTC1M=pd.DataFrame(rows50,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(XRPBTC1M)

cursor.execute('SELECT * FROM Bitfinex_candle_XRPBTC_30m ORDER BY date ASC')
rows51 = cursor.fetchall()
XRPBTC30M=pd.DataFrame(rows51,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(XRPBTC30M)

cursor.execute('SELECT * FROM Bitfinex_candle_XRPBTC_3h ORDER BY date ASC')
rows52 = cursor.fetchall()
XRPBTC3H=pd.DataFrame(rows52,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(XRPBTC3H)

cursor.execute('SELECT * FROM Bitfinex_candle_XRPBTC_5m ORDER BY date ASC')
rows53 = cursor.fetchall()
XRPBTC5M=pd.DataFrame(rows53,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(XRPBTC5M)

cursor.execute('SELECT * FROM Bitfinex_candle_XRPBTC_6h ORDER BY date ASC')
rows54 = cursor.fetchall()
XRPBTC6H=pd.DataFrame(rows54,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(XRPBTC6H)

cursor.execute('SELECT * FROM Bitfinex_candle_XRPBTC_7D ORDER BY date ASC')
rows55 = cursor.fetchall()
XRPBTC7D=pd.DataFrame(rows55,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
Tr.append(XRPBTC7D)


connection.close()

Tr[0]['close'].plot(figsize=(11,8)) #test pour afficher l'évolution du prix pour EOSBTC 12h avec une certaine taille
Tr[1]['close'].plot(figsize=(11,7))

print(Tr[0].head()) #affiche les premières lignes du dataset EOSBTC12H
print(Tr[1].head())
   
for j in range ( len(Tr[0])):
   Tr[0].loc[j,'date']= datetime.fromtimestamp(Tr[0].loc[j,'date'])
   print(Tr[0].loc[j,'date'])
   
def transfo_timestamp_datetime(i): #transforme les dates en timestamps
    for j in range ( len(Tr[i])):
        Tr[i].loc[j,'date']= datetime.fromtimestamp(Tr[i].loc[j,'date'])
        print(Tr[i].loc[j,'date'])
   
Tr[0].index_col='date'
Tr[0].parse_dates=True
EOSBTC12H.head()  
Tr[0].loc[len(Tr[0]),'close']
EOSBTC12H.loc[:len(Tr[0]),['date','close']].rolling(window=20).mean().plot()  
T1=EOSBTC12H.loc[:len(Tr[0]),['date','close']].rolling(window=2).mean()
print(EOSBTC12H.loc[:len(Tr[0]),['date','close']].rolling(window=2).mean()  )

def calcul_sma(Trades,indice_dt,taille_fenêtre): #sma pour un certain dataframe
    sma=Trades[indice_dt].loc[:len(Trades[indice_dt]),['date','close']].rolling(window=taille_fenêtre).mean()
    return sma

def calcul_ema(Trades,indice_dt,taille_fenêtre): #ema pour un certai dataframe
    ema=Trades[indice_dt].loc[:len(Trades[indice_dt]),['date','close']].ewm(window=taille_fenêtre).mean()
    return ema

def RSIfun(price, n=14):
    delta = price['Close'].diff()
    dUp, dDown = delta.copy(), delta.copy()
    dUp[dUp < 0] = 0
    dDown[dDown > 0] = 0
    RolUp = pd.rolling_mean(dUp, n)
    RolDown = pd.rolling_mean(dDown, n).abs()
    RS = RolUp / RolDown
    rsi= 100.0 - (100.0 / (1.0 + RS))
    return rsi

def exportation_dt_indicateur(Trades,indice_dt,nom_indic, indic):
    for i in range(len(Trades[indice_dt])):
        Trades[indice_dt].loc[i,nom_indic]=indic.loc[i,'close']
 
def exportation_database(Trades,indice_dt,table,nom_indic):
    connection=sq.connect("C:/Users/user/Downloads/trade_db.db")
    print(table)
    cursor=connection.cursor()
    i=0
    dataframe=Trades[indice_dt]
    for i in range(len(Trades[indice_dt])):
        data={"nomtable":table, "nom_indic":nom_indic, "val":dataframe.loc[i,nom_indic],"i":i+1}
        query="UPDATE %(nomtable)s SET %(nom_indic)s=:val WHERE Id=:i"
        cursor.execute(query,data)
    connection.commit()
    connection.close()

def test_exp(Trades,indice_dt,table,nom_indic):
    dataframe=Trades[indice_dt]
    data={"nomtable":table, "nom_indic":nom_indic, "val":dataframe.loc[1,nom_indic]}
    query=("UPDATE %s SET %s=:val WHERE Id=:i",table,nom_indic)
    print(table)
    print(data)
    print((query,data))
    

def main():
    transfo_timestamp_datetime(6)
    sma_30=calcul_sma(Tr,6,30)
    exportation_dt_indicateur(Tr,6,'sma_30',sma_30)
    connection=sq.connect("C:/Users/user/Downloads/trade_db.db")
    cursor=connection.cursor() 
    
    data={"val":Tr[0].loc[36,'sma_30'],"val2":36}
    query="UPDATE Bitfinex_candle_EOSBTC_12h SET sma_30=:val WHERE Id=:val2"      
    cursor.execute(query,data)    
    connection.commit()
    connection.close() 
    
    cursor.execute("""UPDATE Bitfinex_candle_EOSBTC_12h
                   SET sma_30=10 
                   WHERE Id=31""" )
    
    
    
    exportation_database(Tr,0,'Bitfinex_candle_EOSBTC_12h',"sma_30")
    
    test_exp(Tr,0,'Bitfinex_candle_EOSBTC_12h',"sma_30")

    print(Tr[0].loc[345,'sma_30'])
    Tr[0].head(1)




















