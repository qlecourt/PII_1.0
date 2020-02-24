# -*- coding: utf-8 -*-
import sqlite3 as sq
import pandas as pd
import numpy as np
import matplotlib as plt
from datetime import datetime
import scipy as sc
import pprint as pp

def importation_database():
    connection=sq.connect("C:/Users/user/Downloads/trade_db.db") #création de la connection
    cursor=connection.cursor()  #création du curseur 
    Tr=[] #Création du dataframe contenant les trades 

    cursor.execute('SELECT * FROM Bitfinex_candle_EOSBTC_12h ORDER BY date ASC')
    rows0 = cursor.fetchall()
    EOSBTC12H= pd.DataFrame(rows0,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(EOSBTC12H)
    Tr[0].name='EOSBTC12H'
    
    cursor.execute('SELECT * FROM Bitfinex_candle_EOSBTC_14D ORDER BY date ASC')
    rows1 = cursor.fetchall()
    EOSBTC14D=pd.DataFrame(rows1,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(EOSBTC14D)
    Tr[1].name='EOSBTC14D'
    
    cursor.execute('SELECT * FROM Bitfinex_candle_EOSBTC_15m ORDER BY date ASC')
    rows2 = cursor.fetchall()
    EOSBTC15M=pd.DataFrame(rows2,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(EOSBTC15M)
    Tr[2].name='EOSBTC15M'

    cursor.execute('SELECT * FROM Bitfinex_candle_EOSBTC_1D ORDER BY date ASC')
    rows3 = cursor.fetchall()
    EOSBTC1D=pd.DataFrame(rows3,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(EOSBTC1D)
    Tr[3].name='EOSBTC1D'

    cursor.execute('SELECT * FROM Bitfinex_candle_EOSBTC_1H ORDER BY date ASC')
    rows4 = cursor.fetchall()
    EOSBTC1H=pd.DataFrame(rows4,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(EOSBTC1H)
    Tr[4].name='EOSBTC1H'

    cursor.execute('SELECT * FROM Bitfinex_candle_EOSBTC_1m ORDER BY date ASC')
    rows5 = cursor.fetchall()
    EOSBTC1M=pd.DataFrame(rows5,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(EOSBTC1M)
    Tr[5].name='EOSBTC1M'

    cursor.execute('SELECT * FROM Bitfinex_candle_EOSBTC_30m ORDER BY date ASC')
    rows6 = cursor.fetchall()
    EOSBTC30M=pd.DataFrame(rows6,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(EOSBTC30M)
    Tr[6].name='EOSBTC30M'

    cursor.execute('SELECT * FROM Bitfinex_candle_EOSBTC_3h ORDER BY date ASC')
    rows7 = cursor.fetchall()
    EOSBTC3H=pd.DataFrame(rows7,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(EOSBTC3H)
    Tr[7].name='EOSBTC3H'

    cursor.execute('SELECT * FROM Bitfinex_candle_EOSBTC_5m ORDER BY date ASC')
    rows8 = cursor.fetchall()
    EOSBTC5M=pd.DataFrame(rows8,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(EOSBTC5M)
    Tr[8].name='EOSBTC5M'

    cursor.execute('SELECT * FROM Bitfinex_candle_EOSBTC_6h ORDER BY date ASC')
    rows9 = cursor.fetchall()
    EOSBTC6H=pd.DataFrame(rows9,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(EOSBTC6H)
    Tr[9].name='EOSBTC6H'

    cursor.execute('SELECT * FROM Bitfinex_candle_EOSBTC_7D ORDER BY date ASC')
    rows10 = cursor.fetchall()
    EOSBTC7D=pd.DataFrame(rows10,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(EOSBTC7D)
    Tr[10].name='EOSBTC7D'

    cursor.execute('SELECT * FROM Bitfinex_candle_EOSETH_12h ORDER BY date ASC')
    rows11 = cursor.fetchall()
    EOSETH12H=pd.DataFrame(rows11,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(EOSETH12H)
    Tr[11].name='EOSETH12H'

    cursor.execute('SELECT * FROM Bitfinex_candle_EOSETH_14D ORDER BY date ASC')
    rows12 = cursor.fetchall()
    EOSETH14D=pd.DataFrame(rows12,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(EOSETH14D)
    Tr[12].name='EOSETH14D'

    cursor.execute('SELECT * FROM Bitfinex_candle_EOSETH_15m ORDER BY date ASC')
    rows13 = cursor.fetchall()
    EOSETH15M=pd.DataFrame(rows13,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(EOSETH15M)
    Tr[13].name='EOSETH15M'

    cursor.execute('SELECT * FROM Bitfinex_candle_EOSETH_1D ORDER BY date ASC')
    rows14 = cursor.fetchall()
    EOSETH1D=pd.DataFrame(rows14,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(EOSETH1D)
    Tr[14].name='EOSETH1D'

    cursor.execute('SELECT * FROM Bitfinex_candle_EOSETH_1h ORDER BY date ASC')
    rows15 = cursor.fetchall()
    EOSETH1H=pd.DataFrame(rows15,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(EOSETH1H)
    Tr[15].name='EOSETH1H'

    cursor.execute('SELECT * FROM Bitfinex_candle_EOSETH_1m ORDER BY date ASC')
    rows16 = cursor.fetchall()
    EOSETH1M=pd.DataFrame(rows16,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(EOSETH1M)
    Tr[16].name='EOSETH1M'

    cursor.execute('SELECT * FROM Bitfinex_candle_EOSETH_30m ORDER BY date ASC')
    rows17 = cursor.fetchall()
    EOSETH30M=pd.DataFrame(rows17,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(EOSETH30M)
    Tr[17].name='EOSETH30M'

    cursor.execute('SELECT * FROM Bitfinex_candle_EOSETH_3h ORDER BY date ASC')
    rows18 = cursor.fetchall()
    EOSETH3H=pd.DataFrame(rows18,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(EOSETH3H)
    Tr[18].name='EOSETH3H'

    cursor.execute('SELECT * FROM Bitfinex_candle_EOSETH_5m ORDER BY date ASC')
    rows19 = cursor.fetchall()
    EOSETH5M=pd.DataFrame(rows19,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(EOSETH5M)
    Tr[19].name='EOSETH5M'

    cursor.execute('SELECT * FROM Bitfinex_candle_EOSETH_6h ORDER BY date ASC')
    rows20 = cursor.fetchall()
    EOSETH6H=pd.DataFrame(rows20,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(EOSETH6H)
    Tr[20].name='EOSETH6H'

    cursor.execute('SELECT * FROM Bitfinex_candle_EOSETH_7D ORDER BY date ASC')
    rows21 = cursor.fetchall()
    EOSETH7D=pd.DataFrame(rows21,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(EOSETH7D)
    Tr[21].name='EOSETH7D'

    cursor.execute('SELECT * FROM Bitfinex_candle_ETHBTC_12h ORDER BY date ASC')
    rows22 = cursor.fetchall()
    ETHBTC12H=pd.DataFrame(rows22,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(ETHBTC12H)
    Tr[22].name='ETHBTC12H'

    cursor.execute('SELECT * FROM Bitfinex_candle_ETHBTC_14D ORDER BY date ASC')
    rows23 = cursor.fetchall()
    ETHBTC14D=pd.DataFrame(rows23,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(ETHBTC14D)
    Tr[23].name='ETHBTC14D'

    cursor.execute('SELECT * FROM Bitfinex_candle_ETHBTC_15m ORDER BY date ASC')
    rows24 = cursor.fetchall()
    ETHBTC15M=pd.DataFrame(rows24,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(ETHBTC15M)
    Tr[24].name='ETHBTC15M'

    cursor.execute('SELECT * FROM Bitfinex_candle_ETHBTC_1D ORDER BY date ASC')
    rows25 = cursor.fetchall()
    ETHBTC1D=pd.DataFrame(rows25,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(ETHBTC1D)
    Tr[25].name='ETHBTC1D'

    cursor.execute('SELECT * FROM Bitfinex_candle_ETHBTC_1h ORDER BY date ASC')
    rows26 = cursor.fetchall()
    ETHBTC1H=pd.DataFrame(rows26,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(ETHBTC1H)
    Tr[26].name='ETHBTC1H'

    cursor.execute('SELECT * FROM Bitfinex_candle_ETHBTC_1m ORDER BY date ASC')
    rows27 = cursor.fetchall()
    ETHBTC1M=pd.DataFrame(rows27,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(ETHBTC1M)
    Tr[27].name='ETHBTC1M'

    cursor.execute('SELECT * FROM Bitfinex_candle_ETHBTC_30m ORDER BY date ASC')
    rows28 = cursor.fetchall()
    ETHBTC30M=pd.DataFrame(rows28,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(ETHBTC30M)
    Tr[28].name='ETHBTC30M'

    cursor.execute('SELECT * FROM Bitfinex_candle_ETHBTC_3h ORDER BY date ASC')
    rows29 = cursor.fetchall()
    ETHBTC3H=pd.DataFrame(rows29,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(ETHBTC3H)
    Tr[29].name='ETHBTC3H'

    cursor.execute('SELECT * FROM Bitfinex_candle_ETHBTC_5m ORDER BY date ASC')
    rows30 = cursor.fetchall()
    ETHBTC5M=pd.DataFrame(rows30,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(ETHBTC5M)
    Tr[30].name='ETHBTC5M'

    cursor.execute('SELECT * FROM Bitfinex_candle_ETHBTC_6h ORDER BY date ASC')
    rows31 = cursor.fetchall()
    ETHBTC6H=pd.DataFrame(rows31,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(ETHBTC6H)
    Tr[31].name='ETHBTC6H'

    cursor.execute('SELECT * FROM Bitfinex_candle_ETHBTC_7D ORDER BY date ASC')
    rows32 = cursor.fetchall()
    ETHBTC7D=pd.DataFrame(rows32,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(ETHBTC7D)
    Tr[32].name='ETHBTC7D'

    cursor.execute('SELECT * FROM Bitfinex_candle_LTCBTC_12h ORDER BY date ASC')
    rows33 = cursor.fetchall()
    LTCBTC12H=pd.DataFrame(rows33,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(LTCBTC12H)
    Tr[33].name='LTCBTC12H'
    
    cursor.execute('SELECT * FROM Bitfinex_candle_LTCBTC_14D ORDER BY date ASC')
    rows34 = cursor.fetchall()
    LTCBTC14D=pd.DataFrame(rows34,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(LTCBTC14D)
    Tr[34].name='LTCBTC14D'

    cursor.execute('SELECT * FROM Bitfinex_candle_LTCBTC_15m ORDER BY date ASC')
    rows35 = cursor.fetchall()
    LTCBTC15M=pd.DataFrame(rows35,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(LTCBTC15M)
    Tr[35].name='LTCBTC15M'

    cursor.execute('SELECT * FROM Bitfinex_candle_LTCBTC_1D ORDER BY date ASC')
    rows36 = cursor.fetchall()
    LTCBTC1D=pd.DataFrame(rows36,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(LTCBTC1D)
    Tr[36].name='LTCBTC1D'

    cursor.execute('SELECT * FROM Bitfinex_candle_LTCBTC_1h ORDER BY date ASC')
    rows37 = cursor.fetchall()
    LTCBTC1H=pd.DataFrame(rows37,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(LTCBTC1H)
    Tr[37].name='LTCBTC1H'

    cursor.execute('SELECT * FROM Bitfinex_candle_LTCBTC_1m ORDER BY date ASC')
    rows38 = cursor.fetchall()
    LTCBTC1M=pd.DataFrame(rows38,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(LTCBTC1M)
    Tr[38].name='LTCBTC1M'

    cursor.execute('SELECT * FROM Bitfinex_candle_LTCBTC_30m ORDER BY date ASC')
    rows39 = cursor.fetchall()
    LTCBTC30M=pd.DataFrame(rows39,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(LTCBTC30M)
    Tr[39].name='LTCBTC30M'

    cursor.execute('SELECT * FROM Bitfinex_candle_LTCBTC_3h ORDER BY date ASC')
    rows40 = cursor.fetchall()
    LTCBTC3H=pd.DataFrame(rows40,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(LTCBTC3H)
    Tr[40].name='LTCBTC3H'

    cursor.execute('SELECT * FROM Bitfinex_candle_LTCBTC_5m ORDER BY date ASC')
    rows41 = cursor.fetchall()
    LTCBTC5M=pd.DataFrame(rows41,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(LTCBTC5M)
    Tr[41].name='LTCBTC5M'

    cursor.execute('SELECT * FROM Bitfinex_candle_LTCBTC_6h ORDER BY date ASC')
    rows42 = cursor.fetchall()
    LTCBTC6H=pd.DataFrame(rows42,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(LTCBTC6H)
    Tr[42].name='LTCBTC6H'

    cursor.execute('SELECT * FROM Bitfinex_candle_LTCBTC_7D ORDER BY date ASC')
    rows43 = cursor.fetchall()
    LTCBTC7D=pd.DataFrame(rows43,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(LTCBTC7D)
    Tr[43].name='LTCBTC7D'

    cursor.execute('SELECT * FROM Bitfinex_candle_XRPBTC_12h ORDER BY date ASC')
    rows44 = cursor.fetchall()
    XRPBTC12H=pd.DataFrame(rows44,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(XRPBTC12H)
    Tr[44].name='XPRBTC12H'

    cursor.execute('SELECT * FROM Bitfinex_candle_XRPBTC_14D ORDER BY date ASC')
    rows45 = cursor.fetchall()
    XRPBTC14D=pd.DataFrame(rows45,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(XRPBTC14D)
    Tr[45].name='XPRBTC14D'

    cursor.execute('SELECT * FROM Bitfinex_candle_XRPBTC_15m ORDER BY date ASC')
    rows46 = cursor.fetchall()
    XRPBTC15M=pd.DataFrame(rows46,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(XRPBTC15M)
    Tr[46].name='XPRBTC15M'

    cursor.execute('SELECT * FROM Bitfinex_candle_XRPBTC_1D ORDER BY date ASC')
    rows47 = cursor.fetchall()
    XRPBTC1D=pd.DataFrame(rows47,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(XRPBTC1D)
    Tr[47].name='XPRBTC1D'

    cursor.execute('SELECT * FROM Bitfinex_candle_XRPBTC_1h ORDER BY date ASC')
    rows48 = cursor.fetchall()
    XRPBTC1H=pd.DataFrame(rows48,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(XRPBTC1H)
    Tr[48].name='XPRBTC1H'

    cursor.execute('SELECT * FROM Bitfinex_candle_XRPBTC_1m ORDER BY date ASC')
    rows49 = cursor.fetchall()
    XRPBTC1M=pd.DataFrame(rows49,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(XRPBTC1M)
    Tr[49].name='XPRBTC1M'

    cursor.execute('SELECT * FROM Bitfinex_candle_XRPBTC_30m ORDER BY date ASC')
    rows50 = cursor.fetchall()
    XRPBTC30M=pd.DataFrame(rows50,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(XRPBTC30M)
    Tr[50].name='XPRBTC30M'

    cursor.execute('SELECT * FROM Bitfinex_candle_XRPBTC_3h ORDER BY date ASC')
    rows51 = cursor.fetchall()
    XRPBTC3H=pd.DataFrame(rows51,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(XRPBTC3H)
    Tr[51].name='XPRBTC3H'
    
    cursor.execute('SELECT * FROM Bitfinex_candle_XRPBTC_5m ORDER BY date ASC')
    rows52 = cursor.fetchall()
    XRPBTC5M=pd.DataFrame(rows52,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(XRPBTC5M)
    Tr[52].name='XPRBTC5M'

    cursor.execute('SELECT * FROM Bitfinex_candle_XRPBTC_6h ORDER BY date ASC')
    rows53 = cursor.fetchall()
    XRPBTC6H=pd.DataFrame(rows53,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(XRPBTC6H)
    Tr[53].name='XPRBTC6H'

    cursor.execute('SELECT * FROM Bitfinex_candle_XRPBTC_7D ORDER BY date ASC')
    rows54 = cursor.fetchall()
    XRPBTC7D=pd.DataFrame(rows54,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'] )
    Tr.append(XRPBTC7D)
    Tr[54].name='XPRBTC7D'

    connection.close()




Tr[0]['close'].plot(figsize=(11,8)) #test pour afficher l'évolution du prix pour EOSBTC 12h avec une certaine taille
Tr[33]['close'].plot(figsize=(11,7))
Tr[33]['date'].plot(figsize=(11,7))

print(Tr[0].head()) #affiche les premières lignes du dataset EOSBTC12H
print(Tr[1].head())
   
for j in range ( len(Tr[0])):
   Tr[0].loc[j,'date']= datetime.fromtimestamp(Tr[0].loc[j,'date'])
   print(Tr[0].loc[j,'date'])
   
def transfo_timestamp_datetime(i): #transforme les dates en timestamps
    for j in range ( len(Tr[i])):
        Tr[i].loc[j,'date']= datetime.fromtimestamp(Tr[i].loc[j,'date'])
    
   
Tr[33].index_col='date'
Tr[33].parse_dates=True
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
    
def BubbleSort(tab):
    
    n = len(tab[0])
    print(n)
    # Traverse through all array elements
    for i in range(n):
 
        # Last i elements are already in place
        for j in range(0, n-i-1):
 
            # traverse the array from 0 to n-i-1
            # Swap if the element found is greater
            # than the next element
            if tab[0][j] > tab[0][j+1] :
                tab[0][j], tab[0][j+1] = tab[0][j+1], tab[0][j]
                tab[1][j], tab[1][j+1] = tab[1][j+1], tab[1][j]
    return tab
                
    
def momentum(Tr,i, RW): #RW stands for Rolling Window
    Sort_tab=[]
    for j in range(RW+1,len(Tr[i]),1):
        if Tr[i].loc[j,'date']==Tr[i+11].loc[j,'date'] and Tr[i].loc[j,'date']==Tr[i+22].loc[j,'date'] and Tr[i].loc[j,'date']==Tr[i+33].loc[j,'date'] and Tr[i].loc[j,'date']==Tr[i+44].loc[j,'date']:
            moment_tab=[]
            for k in range(2):
                moment_tab.append([0] * 5)
            moment_tab[0][0]=100*Tr[i].loc[j,'close']/Tr[i].loc[j-RW,'close']-100
            moment_tab[1][0]=Tr[i].name
            moment_tab[0][1]=100*Tr[i+11].loc[j,'close']/Tr[i+11].loc[j-RW,'close']-100
            moment_tab[1][1]=Tr[i+11].name
            moment_tab[0][2]=100*Tr[i+22].loc[j,'close']/Tr[i+22].loc[j-RW,'close']-100
            moment_tab[1][2]=Tr[i+22].name
            moment_tab[0][3]=100*Tr[i+33].loc[j,'close']/Tr[i+33].loc[j-RW,'close']-100
            moment_tab[1][3]=Tr[i+33].name
            moment_tab[0][4]=100*Tr[i+44].loc[j,'close']/Tr[i+44].loc[j-RW,'close']-100
            moment_tab[1][4]=Tr[i+44].name
            pd.DataFrame(moment_tab)
            Sort_tab.append(BubbleSort(moment_tab))
    return Sort_tab

    
#impossible de parcourir l'ensemble des dataframes pour faire des tes momentum sur les mêmes dates
#solution: couper les dataframes trop long sur lesquelles on a pas les dates communes aux paires
#table EOSBBTC12H, date la plus ancienne: 2017-07-01 14:05:00, nombre de tuples: 1647
#IPour les tables 12H, on prendra comme date de début celle de EOSBTC12H et comme longueur celle de EOSBTC12H
#table EOSBBTC14D, date la plus ancienne:2017-06-29 02:05:00, nombre de tuples: 59
#IPour les tables 14D, on prendra comme date de début celle de EOSBTC14D et comme longueur celle de EOSBTC14D
#On fera de même pour le reste des tables, la table EOSBTC12H étant la plus récente


def testmomentum():
     Sort_tab=[]
     j=2
     for j in range(len(EOSBTC12H)):   
         if EOSBTC12H.loc[j,'date']==EOSETH12H.loc[j,'date'] and EOSBTC12H.loc[j,'date']==ETHBTC12H.loc[j,'date'] and EOSBTC12H.loc[j,'date']==LTCBTC12H.loc[j,'date'] and EOSBTC12H.loc[j,'date']==XRPBTC12H.loc[j,'date']:
            print(EOSBTC12H.loc[j,'date'],EOSETH12H.loc[j,'date'],ETHBTC12H.loc[j,'date'],LTCBTC12H.loc[j,'date'],XRPBTC12H.loc[j,'date'])
            moment_tab=[]
            for k in range(0,2):
                moment_tab.append([0] * 5)
            print(moment_tab)
            moment_tab[0][0]=100*EOSBTC12H.loc[j,'close']/EOSBTC12H.loc[j-2,'close']-100
            print(moment_tab[0][0])
            moment_tab[1][0]='EOSBTCH12H'
            moment_tab[0][1]=100*EOSETH12H.loc[j,'close']/EOSETH12H.loc[j-2,'close']-100
            moment_tab[1][1]='EOSETH12H'
            moment_tab[0][2]=100*ETHBTC12H.loc[j,'close']/ETHBTC12H.loc[j-2,'close']-100
            moment_tab[1][2]='ETHBTC12H'
            moment_tab[0][3]=100*LTCBTC12H.loc[j,'close']/LTCBTC12H.loc[j-2,'close']-100
            moment_tab[1][3]='LTCBTC12H'
            moment_tab[0][4]=100*XRPBTC12H.loc[j,'close']/XRPBTC12H.loc[j-2,'close']-100
            moment_tab[1][4]='XRPBTC12H'
            print(moment_tab)
            pd.DataFrame(moment_tab)
            Sort_tab.append(BubbleSort(moment_tab))
            pd.DataFrame(Sort_tab)
            print(Sort_tab)
     return Sort_tab
    

LTCBTC12Hb=[]
for i in range(0,len(Tr[33])):
    if Tr[33].loc[i,'date']==Tr[0].loc[0,'date']:
       for j in range(i,len(LTCBTC12H)):  
           LTCBTC12Hb.append(rows33[j])
LTCBTC12Hb=pd.DataFrame(LTCBTC12Hb,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'])
print(LTCBTC12Hb.loc[10,'close'])

def dataframe_cut(indice_dt_ref,indice_dt,new_dt,row_name):
   # new_dt=[]
    for i in range(0,len(Tr[indice_dt])):
        if Tr[indice_dt].loc[i,'date']==Tr[indice_dt_ref].loc[0,'date']:
            for j in range(i,len(Tr[indice_dt])):  
                new_dt.append(row_name[j])
  

def momentum12H(RW):
     Sort_tab=[]
     for j in range(RW,len(EOSBTC12H)):   
            print(EOSETH12H.loc[j,'date'])
            print(j)
            moment_tab=[]
            for k in range(0,2):
                moment_tab.append([0] * 5)
            moment_tab[0][0]=100*EOSBTC12H.loc[j,'close']/EOSBTC12H.loc[j-RW,'close']-100
            moment_tab[1][0]='EOSBTCH12H'
            moment_tab[0][1]=100*EOSETH12H.loc[j,'close']/EOSETH12H.loc[j-RW,'close']-100
            moment_tab[1][1]='EOSETH12H'
            moment_tab[0][2]=100*ETHBTC12Hb.loc[j,'close']/ETHBTC12Hb.loc[j-RW,'close']-100
            moment_tab[1][2]='ETHBTC12H'
            moment_tab[0][3]=100*LTCBTC12Hb.loc[j,'close']/LTCBTC12Hb.loc[j-RW,'close']-100
            moment_tab[1][3]='LTCBTC12H'
            moment_tab[0][4]=100*XRPBTC12Hb.loc[j,'close']/XRPBTC12Hb.loc[j-RW,'close']-100
            moment_tab[1][4]='XRPBTC12H'                   
            Sort_tab.append(BubbleSort(moment_tab))
            pd.DataFrame(Sort_tab)
     print(Sort_tab)
     return Sort_tab

def generating_P(date):
    index_date=0
    for i in range(0,len(EOSBTC12H)):
        if EOSBTC12H.loc[i,'date']==date:
            index_date=i
    P=[]
    for k in range(0,2):
        P.append([0] * 4)
    moment_tab[0][0]=EOSBTC12H.loc[index_date,'close']
    moment_tab[1][0]='EOSBTCH12H'
    moment_tab[0][1]=EOSETH12H.loc[index_date,'close']
    moment_tab[1][1]='EOSETH12H'
    moment_tab[0][2]=ETHBTC12Hb.loc[index_date,'close']
    moment_tab[1][2]='ETHBTC12H'
    moment_tab[0][3]=LTCBTC12Hb.loc[index_date,'close']
    moment_tab[1][3]='LTCBTC12H'
    moment_tab[0][4]=XRPBTC12Hb.loc[index_date,'close']
    moment_tab[1][4]='XRPBTC12H'          
    
def P_momentum(Tr,ancien_P,today_P, today_date,ancient_date):
    new_P=[]
    k=0
    j=0
    for i in range(len(Tr[0])):
        if Tr[0].loc
    moment_tab=momentum12H(k-j)
    for k in range(0,2):
        new_P.append([0] * 4)
    new_P[0][0]=today_P[0][0]/ancient_P[0][0]*100-100
    new_P[0][1]=today_P[0][1]/ancient_P[0][1]*100-100
    new_P[0][2]=today_P[0][2]/ancient_P[0][2]*100-100
    new_P[0][3]=today_P[0][3]/ancient_P[0][3]*100-100
    new_P[1][0]=today_P[1][0]
    new_P[1][1]=today_P[1][1]
    new_P[1][2]=today_P[1][2]
    new_P[1][3]=today_P[1][3]
    #new_P=pd.DataFrame(new_P,columns=['price_evolution','crypto'])
        
        
        
def rebalancing_asset(Tr,name,date1,date2):
    idate1=0
    idate2=0
    moment=0
    for i in range(0,len(Tr[])
        if(name.loc[i,'date']==date1)
            idate1=i
        if(name.loc[i,'date']==date2)
            idate2=i 
    moment=100*name.loc[idate2,'close']/name.loc[idate1,'close']-100
    
def rebalancing_P(step,P):
    
def percent_return_P(beg,end):
    return end/beg*100-100

def momentum_strat(RW,RF): #RF and RW stand for rebalancing frequency and rolling window
    
def plot_strat():
    
def main():
    
   
    importation_database()
    

    #transfo date 12H
    transfo_timestamp_datetime(0)
    transfo_timestamp_datetime(11)
    transfo_timestamp_datetime(22)
    transfo_timestamp_datetime(33)
    transfo_timestamp_datetime(44)
    #transfo date 14D
    transfo_timestamp_datetime(1)
    transfo_timestamp_datetime(12)
    transfo_timestamp_datetime(23)
    transfo_timestamp_datetime(34)
    transfo_timestamp_datetime(45)
    #transfo date 1D
    transfo_timestamp_datetime(3)
    transfo_timestamp_datetime(14)
    transfo_timestamp_datetime(25)
    transfo_timestamp_datetime(36)
    transfo_timestamp_datetime(47)
    #transfo date 1H
    transfo_timestamp_datetime(4)
    transfo_timestamp_datetime(15)
    transfo_timestamp_datetime(26)
    transfo_timestamp_datetime(37)
    transfo_timestamp_datetime(48)
     #transfo date 3H
    transfo_timestamp_datetime(7)
    transfo_timestamp_datetime(18)
    transfo_timestamp_datetime(29)
    transfo_timestamp_datetime(40)
    transfo_timestamp_datetime(51)
    #transfo date 6H
    transfo_timestamp_datetime(9)
    transfo_timestamp_datetime(20)
    transfo_timestamp_datetime(31)
    transfo_timestamp_datetime(42)
    transfo_timestamp_datetime(53)
    #transfo date 7D
    transfo_timestamp_datetime(10)
    transfo_timestamp_datetime(21)
    transfo_timestamp_datetime(32)
    transfo_timestamp_datetime(43)
    transfo_timestamp_datetime(54)
    
    ETHBTC12Hb=[]
    dataframe_cut(0,22,ETHBTC12Hb,rows22)
    ETHBTC12Hb=pd.DataFrame(ETHBTC12Hb,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'])
    LTCBTC12Hb=[]
    dataframe_cut(0,33,LTCBTC12Hb,rows33)
    LTCBTC12Hb=pd.DataFrame(LTCBTC12Hb,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'])
    XRPBTC12Hb=[]
    dataframe_cut(0,44,XRPBTC12Hb,rows44)
    XRPBTC12Hb=pd.DataFrame(XRPBTC12Hb,columns=['Id','date','high','low','open','close','volume','quotevolume','weightedaverage','sma_7','ema_7','sma_30','ema_30','sma_200','ema_20'])
    
    Tr_b=[]
    Tr_b.append(EOSBTC12H)
    Tr_b.append(EOSETH12H)
    Tr_b.append(ETHBTC12Hb)
    Tr_b.append(LTCBTC12Hb)
    Tr_b.append(XRPBTC12Hb)
    
    M=momentum12H(6)
    M=pd.DataFrame(M,columns=['price','crypto'])
    
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
   
    
    Sort_tab=[]
    for j in range(2,len(EOSBTC12H)):   
            print(EOSETH12H.loc[j,'date'])
            print(j)
            moment_tab=[]
            for k in range(0,2):
                moment_tab.append([0] * 5)
            moment_tab[0][0]=100*EOSBTC12H.loc[j,'close']/EOSBTC12H.loc[j-2,'close']-100
            moment_tab[1][0]='EOSBTCH12H'
            moment_tab[0][1]=100*EOSETH12H.loc[j,'close']/EOSETH12H.loc[j-2,'close']-100
            moment_tab[1][1]='EOSETH12H'
            moment_tab[0][2]=100*ETHBTC12Hb.loc[j,'close']/ETHBTC12Hb.loc[j-2,'close']-100
            moment_tab[1][2]='ETHBTC12H'
            moment_tab[0][3]=100*LTCBTC12Hb.loc[j,'close']/LTCBTC12Hb.loc[j-2,'close']-100
            moment_tab[1][3]='LTCBTC12H'
            moment_tab[0][4]=100*XRPBTC12Hb.loc[j,'close']/XRPBTC12Hb.loc[j-2,'close']-100
            moment_tab[1][4]='XRPBTC12H'          
            pd.DataFrame(moment_tab)
            Sort_tab.append(BubbleSort(moment_tab))
            pd.DataFrame(Sort_tab)
    print(Sort_tab)
    
    M=momentum12H(4)
    
   
    
















