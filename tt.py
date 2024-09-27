import math,os
from datetime import datetime,date, timedelta
import numpy as np
import pandas_ta as taa
from requests import Session
import pyotp
import datetime,psutil
import enum
import json
import logging
import random
import re
import string
import pandas as pd
import websocket
import requests
from io import StringIO
import time,threading,sys
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import pandas_market_calendars as mcal
#from tvDatafeed import TvDatafeedLive, Interval

import pytz
import kj
from kj import KiteApp,order_place,order_modify,check_order_history,order_cancel_place,order_modify_multiple,order_multiple_place,order_place_sqr_complete,order_modify_complete,order_place_manage
# from kk import order_place
import streamlit as st
# from mm import pykite
from threading import Thread
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import certifi
ca = certifi.where()
file_list = ""
uri = "mongodb+srv://praju:praju@cluster0.0lfbb.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(uri, server_api=ServerApi('1'), tlsCAFile=ca)
cluster = MongoClient(uri, tlsCAFile=ca)
db = cluster["tt"]
temp = pd.DataFrame()
