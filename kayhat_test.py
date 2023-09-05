# -*- coding: utf-8 -*-
"""
Created on Tue Sep  5 17:31:34 2023

@author: Raam
"""
import pandas as pd
import requests
#loadind and cleaning the data
################################################
def cleaning_concat(data):#cleans the data for specific stock
    dic_fields={'date':0, 'open':1, 'high':2, 'low':3, 'close':4, 'stock':5, 'adjclose':6}
    lst_pre_df=[]
    for row in data:#O(n)
        flag=True
        lst_row_pre_df=[0,0,0,0,0,0,0]
        for field in  dic_fields:#O(1)
            if field in row:
                if  not row[field]:
                    flag= False
                    break
                else:
                    lst_row_pre_df[dic_fields[field]]=round(row[field],2)
        if flag==True:
            lst_pre_df.append(lst_row_pre_df)
    df=pd.DataFrame(lst_pre_df)
    df=df.rename(columns={0:"date",1:"open",2:"high",3:"low",4:"close",5:"stock",6:"adjclose"})
    return df

def data():#cleans an concatenates the data from all the stocks (O(n)) 
    df_total=pd.DataFrame({"date":[],"open":[],"high":[],"low":[],"close":[],"stock":[],"adjclose":[]})
    lst_stocks= ["AAPL", "GOOG", "MSFT", "GOOG", "AMZN", "NVDA", "BTC", "MNDY", "INTC", "UNH", "META", "JNJ", "MA"]
    headers={"X-RapidAPI-Key": "cd0b5c6c49msha361c35820e253fp13a50djsnf12a87445224",
    "X-RapidAPI-Host": "apidojo-yahoo-finance-v1.p.rapidapi.com"}
    for stock_name in lst_stocks:
        params={"symbol": stock_name, "region": "US"}
        response=requests.get("https://apidojo-yahoo-finance-v1.p.rapidapi.com/stock/v3/get-historical-data",headers=headers,params=params)
        df_temp=cleaning_concat(response.json()["prices"])
        df_total=pd.concat([df_total,df_temp],ignore_index=True)
    return df_total

############################################################
#feature_engineering
def gap_categories(row):
    if row["gap"]<4:
        return "low"
    elif row["gap"]>=4 and row["gap"]<=8:
        return "medium"
    return "high"

def date_changing(row):
    return pd.to_datetime(int(row["date"])).strftime("%Y-%m-%d")

def feature_engineering(df_total):#like I mantioned I had problems with the date
    df_total["gap"]=df_total["high"]-df_total["low"]
    df_total["gap_categories"]=df_total.apply(gap_categories,axis=1)
    df_total["date"]=df_total.apply(date_changing,axis=1)
    #I didnt really filter on the last 3 days beacuse they are all in the same day by my mistake
    #the only change i need to do is in the condition in the row below 
    df_total2=df_total[df_total["date"]=="1970-01-01"][["date","gap"]]
    df_total_mean=round(list(df_total2.groupby(['date']).mean()["gap"])[0],2)
    df_total_dev=round(list(df_total2.groupby(['date']).std()["gap"])[0],2)
    df_total2["avg"]=df_total_mean
    df_total2["dev"]=df_total_dev
    df_total=df_total.merge(df_total2,how="left")
    return df_total
############################################################
#push to s3
#push to aws, it doesnt work beacuse it my first time with it
# from awsauth import S3Auth


# ACCESS_KEY = "AKIATNJ7RZCPNC5GZQH6"
# SECRET_KEY = "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"

# url = "https://finance-stock-interview.s3.ca-central-1.amazonaws.com"
# s = 'test'
# files = { 'file': open("test_5.9.csv", 'rb') }
# # Creating a file
# r = requests.put(url, data=s, auth=S3Auth(ACCESS_KEY, SECRET_KEY))
##############################################################
#operation
df_total=data()
df_total=feature_engineering(df_total)
df_total.to_csv("2023~01~01.csv")
