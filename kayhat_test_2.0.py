# -*- coding: utf-8 -*-
"""

@author: Raam
"""

import pandas as pd
import requests
from datetime import datetime
import boto3
import os
##############################################################################################
class my_aws:
    def __init__(self,bucket,aws_access_key_id,aws_secret_access_key):
        self.bucket=bucket
        self.client_s3=boto3.client('s3',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key)
        
    def download(self,server_dir,local_dir):
        try:
            self.client_s3.download_file(self.bucket, server_dir, local_dir)
        except:
            print("doesnt exist or error")
    # download('/my-new-directory/test_aws2.xlsx', 'local_file_600.xlsx') 
    
    def is_object(self,key):
        try:
            self.client_s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except:
            return False
    # is_object('/my-new-directory/test_aws2.xlsx')
    
    def make_folder(self,key):
        try:
            self.client_s3.put_object(Bucket=self.bucket, Key=key)
        except:
            print("error")
    # make_folder("test")
    

    def upload(self,file_name,directory):
        # try:
        self.client_s3.upload_file(file_name,self.bucket,directory)
        # except:
            # print("bad")
    # upload("test_aws2.xlsx","/my-new-directory/test_aws2.xlsx")
    
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
    df_total=pd.DataFrame({"stock_name":[],"date":[],"open":[],"high":[],"low":[],"close":[],"stock":[],"adjclose":[]})
    lst_stocks= ["AAPL", "GOOG", "MSFT", "GOOG", "AMZN", "NVDA", "BTC", "MNDY", "INTC", "UNH", "META", "JNJ", "MA"]
    headers={"X-RapidAPI-Key": "cd0b5c6c49msha361c35820e253fp13a50djsnf12a87445224",
    "X-RapidAPI-Host": "apidojo-yahoo-finance-v1.p.rapidapi.com"}
    for stock_name in lst_stocks:
        params={"symbol": stock_name, "region": "US"}
        response=requests.get("https://apidojo-yahoo-finance-v1.p.rapidapi.com/stock/v3/get-historical-data",headers=headers,params=params)
        df_temp=cleaning_concat(response.json()["prices"])
        df_temp["stock_name"]=stock_name
        df_temp=df_temp.drop(df_temp[(df_temp["open"]==0) &(df_temp["high"]==0)&(df_temp["low"]==0)&(df_temp["close"]==0)&(df_temp["stock"]==0)&(df_temp["adjclose"]==0)].index)
        df_total=pd.concat([df_total,df_temp],ignore_index=True)
    df_total=df_total.drop_duplicates()
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
    return datetime.strptime(pd.to_datetime(int(row["date"]),unit='s').strftime("%Y-%m-%d"),'%Y-%m-%d')
def feature_engineering(df_total):#like I mantioned I had problems with the date
    df_total["gap"]=df_total["high"]-df_total["low"]
    df_total["gap_categories"]=df_total.apply(gap_categories,axis=1)
    df_total["date"]=df_total.apply(date_changing,axis=1)
    df_total=df_total.sort_values(by="date",ignore_index=True)
    #################################################aggergate 3 close days and claulate their avg and std
    df_total2=df_total[["date","gap"]]
    df_total2=df_total2.groupby(pd.Grouper(key='date',axis=0,freq='3D',sort=True)).std()
    df_total2=df_total2.reset_index().rename(columns={"gap":"gap_std"})
    df_total=pd.merge_asof(df_total,df_total2,on="date",tolerance=pd.Timedelta(3,"d"))
    df_total2=df_total[["date","gap"]]
    df_total2=df_total2.groupby(pd.Grouper(key='date',axis=0,freq='3D',sort=True)).mean()
    df_total2=df_total2.reset_index().rename(columns={"gap":"gap_avg"})
    df_total=pd.merge_asof(df_total,df_total2,on="date",tolerance=pd.Timedelta(3,"d"))
    #################################################
    return df_total

############################################################
def push_to_s3(df_total,bucket_name,aws_access_key_id,aws_secret_access_key):
    aws=my_aws(bucket_name,aws_access_key_id,aws_secret_access_key)
    for day in set(list(df_total["date"])):
        os.mkdir("upload")
        for stock in ["AAPL", "GOOG", "MSFT", "GOOG", "AMZN", "NVDA", "BTC", "MNDY", "INTC", "UNH", "META", "JNJ", "MA"]:
            df_temp=df_total[(df_total["date"]==day) & (df_total["stock_name"]==stock)]
            df_temp.to_pickle("upload/"+stock+".pkl")#I chosed this format beacause its light  (its better to use parquet that also  load alot faster, I didnt use it only because I alredy upload to the s3 the pickels) 
            aws.upload("upload/"+stock+".pkl","/a"+str(day).split(" ")[0]+"/"+stock+".pkl")        
        for file in os.listdir("upload"):
            os.remove("upload/"+file)
        os.rmdir("upload")

################################
################################
################################
################################
################################
##############################################################
#operations
df_total=data()
df_total=feature_engineering(df_total)
bucket_name="finance-stock-interview"
aws_access_key_id="AKIATNJ7RZCPNC5GZQH6"
aws_secret_access_key="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"
push_to_s3(df_total,bucket_name,aws_access_key_id,aws_secret_access_key)
