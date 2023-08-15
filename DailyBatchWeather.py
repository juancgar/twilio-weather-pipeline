#!/usr/bin/env python
# coding: utf-8

# In[1]:


#!pip install twilio,python-dotenv


# In[161]:


import pandas as pd
import os

from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects

from datetime import date
from datetime import timedelta

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from io import BytesIO

import boto3, os, io

from datetime import datetime

from twilio.rest import Client
import time


# In[162]:


from dotenv import load_dotenv
from pathlib import Path

root = str(Path.cwd())
ConfigPath = root + "\Config.env"
print("path: ",ConfigPath)

load_dotenv(dotenv_path=ConfigPath)

twilio_acc = os.getenv('TWILIO_ACC')
twilio_secret = os.getenv('TWILIO_SECRET')
twilio_phone = os.getenv('TWILIO_PHONE')
WEATHER_API = os.getenv('WEATHER_API')

my_key= os.getenv('AWS_KEY') 
my_secret= os.getenv('AWS_SECRET')


# # Api call 

# In[163]:


def GetResponse(city,dateQuery,WEATHER_API):
    url = "http://api.weatherapi.com/v1/history.json?key=" + WEATHER_API + "&q=" + city + "&dt=" + dateQuery
    return requests.get(url).json()


# In[164]:


query = 'Poza Rica'
dateToSearch = date.today()
dateQuery = dateToSearch.strftime("%Y-%m-%d")
dateQuery


# In[165]:


responseApi = GetResponse(query,dateQuery,WEATHER_API)


# In[166]:


def get_forecast(response,i):
    general = response['forecast']['forecastday'][0]['hour'][i]
    fecha= general['time'].split()[0]
    hour = general['time'].split()[1].split(":")[0]
    condition = general["condition"]['text']
    temp_C = general["temp_c"]
    will_rain = general["will_it_rain"]
    chance_of_rain = general['chance_of_rain']
    
    return fecha,hour,condition,temp_C,will_rain,chance_of_rain
def getTimeForecastData(response):
    data = []
    for x in tqdm(range(len(response['forecast']['forecastday'][0]['hour'])),colour="green"):
        data.append(get_forecast(response,x))
    return data


# In[167]:


data = getTimeForecastData(responseApi)


# In[168]:


col = ["fecha","hour","condition","temp_C","will_rain","chance_of_rain"]
df = pd.DataFrame(data,columns=col)


# In[169]:


bucket_name = 'weather-innova-proto'


def UploadToS3(query,dateQuery,bucket_name,my_key,my_secret):
    filename = query +"-"+dateQuery
    filename = filename.replace(" ","") + '.csv'
    s3 = boto3.resource('s3',aws_access_key_id=my_key,aws_secret_access_key=my_secret) 
    csv_buf = BytesIO()
    df.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    try:
        s3.Object(bucket_name, filename).put(Body=csv_buf)
        return "succesful upload"
    except(e):
        return e
    


# In[170]:


print(UploadToS3(query,dateQuery,bucket_name,my_key,my_secret))


# In[ ]:




