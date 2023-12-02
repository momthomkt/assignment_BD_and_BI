from polygon import RESTClient
import datetime as dt
import pandas as pd
from IPython.display import display
import mysql.connector
import envVariables
# import datetime
from datetime import datetime, timedelta
import time

isFirstAction = True

def action():
    global isFirstAction
    currentDate = datetime.now()

    def getCurrentDate():
        return datetime.now().strftime('%Y-%m-%d')
    def getPrevDate():
        # currentDate = datetime.now()
        prevDate = currentDate - timedelta(days=round(365 * envVariables.period))
        return prevDate.strftime('%Y-%m-%d')

    # Setting up polygon api (for apply ticker)
    client = RESTClient(envVariables.polygonKey)
    stockTicker = 'AAPL'

    # Setting up mysql cursor (write)
    connection = mysql.connector.connect(
        user     = envVariables.user,
        password = envVariables.password,
        host     = envVariables.host,
        database = 'stockanalysis'                                 
    )
    cursor = connection.cursor()

    # Loop setup from date to date
    # dayStop = datetime.date(2023,11,30)
    # dayCount = datetime.date(2023,11,20) 
    # dayCount = datetime.date(2020,1,1)
    indexCount = 0

    # Getting data from polygon API
    # dayCountStr = dayCount.strftime('%Y-%m-%d')
    # dayStopStr = dayStop.strftime('%Y-%m-%d')
    dayCountStr = ''
    dayStopStr=''
    if isFirstAction:
        dayCountStr = getPrevDate()
        isFirstAction = False
    else:
        dayCountStr = getCurrentDate()-timedelta(days=2)
    
    dayStopStr = (currentDate-timedelta(days=1)).strftime('%Y-%m-%d')
    dataReq = client.get_aggs(ticker=stockTicker, multiplier=1,timespan='day',from_=dayCountStr,to=dayStopStr)
    priceData = pd.DataFrame(dataReq)

    # Indexing (might be important later on)
    length = len(priceData.index)
    indexCol = list(range(indexCount,indexCount+length))
    priceData['index_'] = indexCol
    indexCount = indexCount + length

    # Adding human friendly date value
    priceData['Date'] = priceData['timestamp'].apply(lambda x:pd.to_datetime(x*1000000))
    priceData['DateStr'] = priceData['Date'].dt.strftime('%d/%m/%y %H:%M:%S')

    priceData = priceData[['index_','open','high','low','close','timestamp','DateStr']]

    # Changing column labels for insertion
    priceData.rename(columns = {'open':'openStat'}, inplace = True)
    priceData.rename(columns = {'high':'highStat'}, inplace = True)
    priceData.rename(columns = {'low':'lowStat'}, inplace = True)
    priceData.rename(columns = {'close':'closeStat'}, inplace = True)
    priceData.rename(columns = {'timestamp':'timeStamp_'}, inplace = True)
    priceData.rename(columns = {'DateStr':'datetime_'}, inplace = True)
    display(priceData)

    # Creating column list for insertion
    cols = ",".join([str(i) for i in priceData.columns.tolist()])

    # Inserting DataFrame records one by one. (database name changable)
    for i,row in priceData.iterrows():
        sql = "INSERT INTO stockEntriesDaily (" +cols + ") VALUES (" + "%s,"*(len(row)-1) + "%s)"
        cursor.execute(sql, tuple(row))
        connection.commit()

    connection.close()

action()

while True:
    time.sleep(86400)
    action()
    