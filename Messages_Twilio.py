#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import os
from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from datetime import datetime
from twilio.rest import Client
import time

from utilis import request_wapi,get_forecast,create_df,send_message,get_date
from dotenv import load_dotenv
from pathlib import Path


root = str(Path.cwd())
ConfigPath = root + "\Config.env"
print("path: ",ConfigPath)

load_dotenv(dotenv_path=ConfigPath)

twilio_acc = os.getenv('TWILIO_ACC')
twilio_secret = os.getenv('TWILIO_SECRET')
WEATHER_API = os.getenv('WEATHER_API')
query = 'Tokyo'

input_date = get_date()
print(WEATHER_API)
response = request_wapi(WEATHER_API,query)

datos = []

for i in tqdm(range(24),colour = 'green'):

    datos.append(get_forecast(response,i))


df_rain = create_df(datos)

# Send Message
message_id = send_message(twilio_acc,twilio_secret,input_date,df_rain,query)


print('Mensaje Enviado con exito ' + message_id)





