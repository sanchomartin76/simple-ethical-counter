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
class CustomThread(Thread):
    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs={}, Verbose=None):
        Thread.__init__(self, group, target, name, args, kwargs)
        self._return = None
 
    def run(self):
        if self._target is not None:
            self._return = self._target(*self._args, **self._kwargs)
             
    def join(self, *args):
        Thread.join(self, *args)
        return self._return

instru_name = "BANKNIFTY"
if instru_name == "NIFTY":
    qty_plc = 25
elif instru_name == "BANKNIFTY":
    qty_plc = 15
df99=pd.DataFrame()
df79=pd.DataFrame()
kite = ""
logger = logging.getLogger(__name__)
# gauth = GoogleAuth()
# # Try to load saved client credentials
# gauth.LoadCredentialsFile("mycreds.txt")
# if gauth.credentials is None:
#     # Authenticate if they're not there
#     gauth.LocalWebserverAuth()
# elif gauth.access_token_expired:
#     # Refresh them if expired
#     gauth.Refresh()
# else:
#     # Initialize the saved creds
#     gauth.Authorize()
# # Save the current credentials to a file
# gauth.SaveCredentialsFile("mycreds.txt")

# drive = GoogleDrive(gauth)
drive = ""
# folder = '1Lb55YMwFWDqXibD3U2Z3ShZtCM9dLg9i'

# # Upload files
# directory = "E:/g/"
# for f in os.listdir(directory):
#     filename = os.path.join(directory, f)
#     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : f})
#     gfile.SetContentFile(filename)
#     gfile.Upload()
# Download files
# counter=0
# while counter<10:
#     try:
#         file_list = drive.ListFile({'q' : f"'{folder}' in parents and trashed=false"}).GetList()
#         counter=10
#     except Exception as e:
#         logger.error(e)
#         counter=counter+1
TOKEN = "5955602844:AAFfwmGzOaZoOClIKPtOSLkBjjbVBnpXuGY"
CHAT_ID = '6093993760'
SEND_URL = f'https://api.telegram.org/bot{TOKEN}/sendMessage'
kf_session =""
public_token = ""
enc_token = ""
enctoken = ""


def get_enctoken(userid, password, twofa):
    session = requests.Session()
    response = session.post('https://kite.zerodha.com/api/login', data={
        "user_id": userid,
        "password": password
    })
    response = session.post('https://kite.zerodha.com/api/twofa', data={
        "request_id": response.json()['data']['request_id'],
        "twofa_value": twofa,
        "user_id": response.json()['data']['user_id']
    })
    enctoken = response.cookies.get('enctoken')
    if enctoken:
        return enctoken
    else:
        raise Exception("Enter valid details !!!!")

def get_data(n, fromm, to, interval,s):
    ID = n
    user_id = "YZ7782"
    t1 = fromm
    t2 = to
    interv = interval
    parameters = {
        'user_id': user_id,
        'oi': "1",
        'from': t1,
        'to': t2,
        'kf_session': kf_session,
        'public_token': public_token,
        'user_id': user_id,
        'enctoken': enctoken
    }
    while (t1 <= t2):
        try:
            stock_url = "https://kite.zerodha.com/oms/instruments/historical/" + str(
                ID) + "/" + interv
            print(stock_url," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
            response = s.get(stock_url, params=parameters)
            json_data = response.content.decode('utf-8', 'replace')
            d = json.loads(json_data)
            if (d['status'] == "error"):
                print("mmmmm" )
                continue

            if len(d['data']['candles']) > 0:
                
                #df9.columns = ["TIME", "Open", "High", "Low",'CLOSE','VOLUME']
                df9 = pd.DataFrame(
                    json.loads(response.text)['data']['candles'])
                df9.columns = ["TIME", "open", "high", "low", 'close', 'VOLUME', 'OI']
                df9['TIME'] = df9['TIME'].str.replace('\\+0530','',regex=True)
                df9['TIME'] = df9['TIME'].astype('datetime64[ns]')
                #df9['TIME'] =  df9['TIME'] + pd.Timedelta(minutes = 330)
                #df9['SYMBOL'] = symbol_name
                df9['TIME1'] = df9['TIME'].dt.time
                #print(df9,file=sys.stderr)
                return df9
            break
        except requests.exceptions.RequestException as e:

            print(e," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)  #should I also sys.exit(1) after this?

class Interval(enum.Enum):
    in_1_minute = "1"
    in_3_minute = "3"
    in_5_minute = "5"
    in_15_minute = "15"
    in_30_minute = "30"
    in_45_minute = "45"
    in_1_hour = "1H"
    in_2_hour = "2H"
    in_3_hour = "3H"
    in_4_hour = "4H"
    in_daily = "1D"
    in_weekly = "1W"
    in_monthly = "1M"

class TvDatafeed:
    __sign_in_url = 'https://www.tradingview.com/accounts/signin/'
    __search_url = 'https://symbol-search.tradingview.com/symbol_search/?text={}&hl=1&exchange={}&lang=en&type=&domain=production'
    __ws_headers = json.dumps({"Origin": "https://data.tradingview.com"})
    __signin_headers = {'Referer': 'https://www.tradingview.com'}
    __ws_timeout = 5

    def __init__(
        self,
        username: str = None,
        password: str = None,
    ) -> None:
        """Create TvDatafeed object

        Args:
            username (str, optional): tradingview username. Defaults to None.
            password (str, optional): tradingview password. Defaults to None.
        """

        self.ws_debug = False

        self.token = self.__auth(username, password)

        if self.token is None:
            self.token = "unauthorized_user_token"
            logger.warning("you are using nologin method, data you access may be limited")

        self.ws = None
        self.session = self.__generate_session()
        self.chart_session = self.__generate_chart_session()

    def __auth(self, username, password):

        if (username is None or password is None):
            token = None

        else:
            data = {"username": username,
                    "password": password,
                    "remember": "on"}
            try:
                response = requests.post(
                    url=self.__sign_in_url, data=data, headers=self.__signin_headers)
                token = response.json()['user']['auth_token']
            except Exception as e:
                logger.error('error while signin')
                token = None

        return token

    def __create_connection(self):
        logging.debug("creating websocket connection")
        self.ws = websocket.WebSocket()
        self.ws.connect(
            "wss://data.tradingview.com/socket.io/websocket", headers=self.__ws_headers, timeout=self.__ws_timeout
        )

    @staticmethod
    def __filter_raw_message(text):
        try:
            found = re.search('"m":"(.+?)",', text).group(1)
            found2 = re.search('"p":(.+?"}"])}', text).group(1)

            return found, found2
        except AttributeError:
            logger.error("error in filter_raw_message")

    @staticmethod
    def __generate_session():
        stringLength = 12
        letters = string.ascii_lowercase
        random_string = "".join(random.choice(letters)
                                for i in range(stringLength))
        return "qs_" + random_string

    @staticmethod
    def __generate_chart_session():
        stringLength = 12
        letters = string.ascii_lowercase
        random_string = "".join(random.choice(letters)
                                for i in range(stringLength))
        return "cs_" + random_string

    @staticmethod
    def __prepend_header(st):
        return "~m~" + str(len(st)) + "~m~" + st

    @staticmethod
    def __construct_message(func, param_list):
        return json.dumps({"m": func, "p": param_list}, separators=(",", ":"))

    def __create_message(self, func, paramList):
        return self.__prepend_header(self.__construct_message(func, paramList))

    def __send_message(self, func, args):
        m = self.__create_message(func, args)
        if self.ws_debug:
            print(m)
        self.ws.send(m)

    @staticmethod
    def __create_df(raw_data, symbol):
        try:
            out = re.search(r'"s":\[(.+?)\}\]', raw_data).group(1)
            x = out.split(',{"')
            data = list()
            volume_data = True

            for xi in x:
                xi = re.split(r"\[|:|,|\]", xi)
                ts = datetime.datetime.fromtimestamp(float(xi[4]))

                row = [ts]

                for i in range(5, 10):

                    # skip converting volume data if does not exists
                    if not volume_data and i == 9:
                        row.append(0.0)
                        continue
                    try:
                        row.append(float(xi[i]))

                    except ValueError:
                        volume_data = False
                        row.append(0.0)
                        logger.debug('no volume data')

                data.append(row)

            data = pd.DataFrame(
                data, columns=["datetime", "open",
                               "high", "low", "close", "volume"]
            ).set_index("datetime")
            data.insert(0, "symbol", value=symbol)
            return data
        except AttributeError:
            logger.error("no data, please check the exchange and symbol")

    @staticmethod
    def __format_symbol(symbol, exchange, contract: int = None):

        if ":" in symbol:
            pass
        elif contract is None:
            symbol = f"{exchange}:{symbol}"

        elif isinstance(contract, int):
            symbol = f"{exchange}:{symbol}{contract}!"

        else:
            raise ValueError("not a valid contract")

        return symbol

    def get_hist(
        self,
        symbol: str,
        exchange: str = "NSE",
        interval: Interval = Interval.in_daily,
        n_bars: int = 10,
        fut_contract: int = None,
        extended_session: bool = False,
    ) -> pd.DataFrame:
        """get historical data

        Args:
            symbol (str): symbol name
            exchange (str, optional): exchange, not required if symbol is in format EXCHANGE:SYMBOL. Defaults to None.
            interval (str, optional): chart interval. Defaults to 'D'.
            n_bars (int, optional): no of bars to download, max 5000. Defaults to 10.
            fut_contract (int, optional): None for cash, 1 for continuous current contract in front, 2 for continuous next contract in front . Defaults to None.
            extended_session (bool, optional): regular session if False, extended session if True, Defaults to False.

        Returns:
            pd.Dataframe: dataframe with sohlcv as columns
        """
        symbol = self.__format_symbol(
            symbol=symbol, exchange=exchange, contract=fut_contract
        )

        interval = interval.value

        self.__create_connection()

        self.__send_message("set_auth_token", [self.token])
        self.__send_message("chart_create_session", [self.chart_session, ""])
        self.__send_message("quote_create_session", [self.session])
        self.__send_message(
            "quote_set_fields",
            [
                self.session,
                "ch",
                "chp",
                "current_session",
                "description",
                "local_description",
                "language",
                "exchange",
                "fractional",
                "is_tradable",
                "lp",
                "lp_time",
                "minmov",
                "minmove2",
                "original_name",
                "pricescale",
                "pro_name",
                "short_name",
                "type",
                "update_mode",
                "volume",
                "currency_code",
                "rchp",
                "rtc",
            ],
        )

        self.__send_message(
            "quote_add_symbols", [self.session, symbol,
                                  {"flags": ["force_permission"]}]
        )
        # self.__send_message("quote_fast_symbols", [self.session, symbol])

        self.__send_message(
            "resolve_symbol",
            [
                self.chart_session,
                "symbol_1",
                '={"symbol":"'
                + symbol
                + '","adjustment":"splits","session":'
                + ('"regular"' if not extended_session else '"extended"')
                + "}",
            ],
        )
        self.__send_message(
            "create_series",
            [self.chart_session, "s1", "s1", "symbol_1", interval, n_bars],
        )
        self.__send_message("switch_timezone", [
                            self.chart_session, "exchange"])

        raw_data = ""

        logger.debug(f"getting data for {symbol}...")
        while True:
            counter=0
            while counter<40:
                try:
                    result = self.ws.recv()
                    raw_data = raw_data + result + "\n"
                    counter=50
                except Exception as e:
                    logger.error(e)
                    counter=counter+1
                    #break

            if "series_completed" in result:
                break

        return self.__create_df(raw_data, symbol)

def rma(s: pd.Series, period: int) -> pd.Series:
    return s.ewm(alpha=1 / period).mean()

def atr(df: pd.DataFrame, length: int = 14) -> pd.Series:
    # Ref: https://stackoverflow.com/a/74282809/
    high, low, prev_close = df['high'], df['low'], df['close'].shift()
    tr_all = [high - low, high - prev_close, low - prev_close]
    tr_all = [tr.abs() for tr in tr_all]
    tr = pd.concat(tr_all, axis=1).max(axis=1)
    atr_ = rma(tr, length)
    return atr_

def get_pause():
    now = datetime.now()
    next_min = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
    pause = math.ceil((next_min - now).seconds)
    print(f"Sleep for {pause}")
    return pause

#drive.put("hh.txt", data="currently_buy_holding,currently_buy_holding\nFalse,False")
#response = drive.get("hh.txt")
#content = response.read().decode('utf-8')
#print(content)
#df =""
# for index, file in enumerate(file_list):
#     if(file['title'] =="hh.txt"):
#         counter=0
#         while counter<10:
#             try:
#                 df1 = file.GetContentFile(file['title'])
#                 df = pd.read_csv(file['title'])
#                 counter=10
#             except Exception as e:
#                 logger.error(e)
#                 counter=counter+1
df = pd.DataFrame()
hh_count = db.list_collection_names().count("hh")
if(hh_count > 0):
    collection = db["hh"]
    x = collection.find()
    for y in x:
        jj = pd.DataFrame([y])
        df = pd.concat([temp,jj])
        temp = df
# df = hh
temp = pd.DataFrame()
print(df)
print("Check for hh.txt file"," ", df, datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
currently_holding = False
currently_buy_holding = bool(df['currently_buy_holding'].values[0])
currently_sell_holding = bool(df['currently_sell_holding'].values[0])

contract_size = 15
TOKEN = "5955602844:AAFfwmGzOaZoOClIKPtOSLkBjjbVBnpXuGY"
CHAT_ID = '6093993760'
SEND_URL = f'https://api.telegram.org/bot{TOKEN}/sendMessage'

# start_date = (datetime.now().strftime('%Y-%m-%d'))
# print(start_date)
nyse = mcal.get_calendar('NSE')
#eastern_tz = pytz.timezone('Asia/Kolkata')
d = datetime.datetime.now(pytz.timezone('Asia/Kolkata'))
#tz_NY = pytz.timezone('Asia/Kolkata')   
#utc_offset = datetime.datetime.now(pytz.timezone('Asia/Kolkata'))
today = date.today()
today1 = datetime.datetime.now(pytz.timezone('Asia/Kolkata')).date()
print(datetime.datetime.now(pytz.timezone('Asia/Kolkata')).time(),d.minute,datetime.datetime.now(pytz.timezone('Asia/Kolkata')).date(),file=sys.stderr)
start_date = (datetime.datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d'))
if (datetime.datetime.now(pytz.timezone('Asia/Kolkata')).time() > datetime.time(0, 0,0)) and (datetime.datetime.now(pytz.timezone('Asia/Kolkata')).time() < datetime.time(9, 17, 0)):
    start_date = (datetime.datetime.now(pytz.timezone('Asia/Kolkata'))- timedelta(days=1)).strftime('%Y-%m-%d')
# else
end_date = start_date
#print(start_date)
#d = (datetime.datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
#print(d)
# Show available calendars
#print(mcal.get_calendar_names())
i = 1
x = 1
while i:
    early = nyse.schedule(start_date=start_date, end_date=end_date)
    start_date = (datetime.datetime.now(pytz.timezone('Asia/Kolkata')) - timedelta(days=x)).strftime('%Y-%m-%d')
    x = x + 1
    if len(early)>0:
        i = 0
        print(early.index.strftime('%Y-%m-%d').values[0])
fromm = early.index.strftime('%Y-%m-%d').values[0]
#fromm = "2024-03-28"

def cancel_pending(s,order_pending_tobe_cancel,order_cancel):
    global file_list
    order_pending_tobe_cancel = order_pending_tobe_cancel
    order_cancel = order_cancel
    err_sqr_1 = 0
    kl_del = []
    print("cancelling the pending orders ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
    for kl in range(0,len(order_pending_tobe_cancel)):
        status4 = order_cancel_place(s,order_pending_tobe_cancel['order_id'][kl],"pending")
        if status4 == "cancelled":
            order_pending_tobe_cancel.at[kl,"status"] = "cancelled"
            order_cancel.loc[len(order_cancel)] = order_pending_tobe_cancel.iloc[kl]
            kl_del.append(kl)
    order_pending_tobe_cancel = order_pending_tobe_cancel.drop(order_pending_tobe_cancel.index[kl_del])
    # mm = 0
    # nn = 0
    # for index, file in enumerate(file_list):
    #     if(file['title'] =="order_cancel.txt"):
    #         mm = file['id']
    #     if(file['title'] =="order_pending_tobe_cancel.txt"):
    #         nn = file['id']
    # if bool(mm):
    #     order_cancel.to_csv("order_cancel.txt", index=False)
    #     update_file = drive.CreateFile({'id': mm})
    #     update_file.SetContentFile("order_cancel.txt")
    #     update_file.Upload()
    # else:
    #     order_cancel.to_csv("order_cancel.txt", index=False)
    #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_cancel.txt"})
    #     gfile.SetContentFile("order_cancel.txt")
    #     gfile.Upload()
    # if bool(nn):
    #     order_pending_tobe_cancel.to_csv("order_pending_tobe_cancel.txt", index=False)
    #     update_file = drive.CreateFile({'id': nn})
    #     update_file.SetContentFile("order_pending_tobe_cancel.txt")
    #     update_file.Upload()
    # else:
    #     order_pending_tobe_cancel.to_csv("order_pending_tobe_cancel.txt", index=False)
    #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_pending_tobe_cancel.txt"})
    #     gfile.SetContentFile("order_pending_tobe_cancel.txt")
    #     gfile.Upload()
    order_cancel_count = db.list_collection_names().count("order_cancel")
    if(order_cancel_count < 1):
        db.create_collection("order_cancel")
    else:
        db.order_cancel.delete_many({})
    collection = db["order_cancel"]
    dict1 = order_cancel.to_dict('records')
    for x in range(0,len(dict1)):
        collection.insert_one(dict1[x])
    order_pending_tobe_cancel_count = db.list_collection_names().count("order_pending_tobe_cancel")
    if(order_pending_tobe_cancel_count < 1):
        db.create_collection("order_pending_tobe_cancel")
    else:
        db.order_pending_tobe_cancel.delete_many({})
    collection = db["order_pending_tobe_cancel"]
    dict1 = order_pending_tobe_cancel.to_dict('records')
    for x in range(0,len(dict1)):
        collection.insert_one(dict1[x])

    return order_pending_tobe_cancel

def cancel_pending_complete(s,order_pending_complete_tobe_cancel,order_cancel_complete):
    global file_list
    order_pending_complete_tobe_cancel = order_pending_complete_tobe_cancel
    order_cancel_complete = order_cancel_complete
    err_sqr_1 = 0
    kl_del = []
    print("cancelling the pending orders ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
    for kl in range(0,len(order_pending_complete_tobe_cancel)):
        status4 = order_cancel_place(s,order_pending_complete_tobe_cancel['order_id'][kl],"pending")
        if status4 == "cancelled":
            order_pending_complete_tobe_cancel.at[kl,"status"] = "cancelled"
            order_cancel_complete.loc[len(order_cancel_complete)] = order_pending_complete_tobe_cancel.iloc[kl]
            kl_del.append(kl)
    order_pending_complete_tobe_cancel = order_pending_complete_tobe_cancel.drop(order_pending_complete_tobe_cancel.index[kl_del])
    # mm = 0
    # nn = 0
    # for index, file in enumerate(file_list):
    #     if(file['title'] =="order_cancel_complete.txt"):
    #         mm = file['id']
    #     if(file['title'] =="order_pending_complete_tobe_cancel.txt"):
    #         nn = file['id']
    # if bool(mm):
    #     order_cancel_complete.to_csv("order_cancel_complete.txt", index=False)
    #     update_file = drive.CreateFile({'id': mm})
    #     update_file.SetContentFile("order_cancel_complete.txt")
    #     update_file.Upload()
    # else:
    #     order_cancel_complete.to_csv("order_cancel_complete.txt", index=False)
    #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_cancel_complete.txt"})
    #     gfile.SetContentFile("order_cancel_complete.txt")
    #     gfile.Upload()
    # if bool(nn):
    #     order_pending_complete_tobe_cancel.to_csv("order_pending_complete_tobe_cancel.txt", index=False)
    #     update_file = drive.CreateFile({'id': nn})
    #     update_file.SetContentFile("order_pending_complete_tobe_cancel.txt")
    #     update_file.Upload()
    # else:
    #     order_pending_complete_tobe_cancel.to_csv("order_pending_complete_tobe_cancel.txt", index=False)
    #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_pending_complete_tobe_cancel.txt"})
    #     gfile.SetContentFile("order_pending_complete_tobe_cancel.txt")
    #     gfile.Upload()
    order_cancel_complete_count = db.list_collection_names().count("order_cancel_complete")
    if(order_cancel_complete_count < 1):
        db.create_collection("order_cancel_complete")
    else:
        db.order_cancel_complete.delete_many({})
    collection = db["order_cancel_complete"]
    dict1 = order_cancel_complete.to_dict('records')
    for x in range(0,len(dict1)):
        collection.insert_one(dict1[x])
    order_pending_complete_tobe_cancel_count = db.list_collection_names().count("order_pending_complete_tobe_cancel")
    if(order_pending_complete_tobe_cancel_count < 1):
        db.create_collection("order_pending_complete_tobe_cancel")
    else:
        db.order_pending_complete_tobe_cancel.delete_many({})
    collection = db["order_pending_complete_tobe_cancel"]
    dict1 = order_pending_complete_tobe_cancel.to_dict('records')
    for x in range(0,len(dict1)):
        collection.insert_one(dict1[x])

    return order_pending_complete_tobe_cancel

def square_off_sell(order_manage,order_tobe_sqr_complete,s):
    global file_list,kite
    order_manage = order_manage
    order_tobe_sqr_complete = order_tobe_sqr_complete
    err_sqr = 0
    shift_data = 0
    skip_leg1 = 0
    print("squaring off sell position","klkklklk"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
    retry_order = 0
    for mj in range(0,len(order_tobe_sqr_complete)):
        print("Sqr "+str(order_tobe_sqr_complete['leg'][mj])+str(order_tobe_sqr_complete['strike'][mj])+str(order_tobe_sqr_complete['contract'][mj])," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
        if order_tobe_sqr_complete['buy_sell'][mj] == "SELL":
            buy_sell="BUY"
        else:
            buy_sell="SELL"
        # df_opt = get_data(order_tobe_sqr_complete['instrument_id'][mj],fromm, fromm, "minute",s)
        df_opt = pd.DataFrame(kite.historical_data(order_tobe_sqr_complete['instrument_id'][mj], fromm, fromm, "minute", continuous=False, oi=True))
        s = kite
        retry_order = 0
        order = order_place_sqr_complete(s,file_list,drive,order_tobe_sqr_complete['current_signal'][mj],order_tobe_sqr_complete['instrument_id'][mj],order_tobe_sqr_complete['trading_symbol'][mj],df_opt,order_tobe_sqr_complete['qty'][mj],order_tobe_sqr_complete['instru'][mj],0,order_tobe_sqr_complete['leg'][mj],order_tobe_sqr_complete['strike'][mj],order_tobe_sqr_complete['contract'][mj],order_tobe_sqr_complete['expiry'][mj],0,1,buy_sell,retry_order,order_tobe_sqr_complete['status'][mj],order_tobe_sqr_complete['reject_count'][mj],order_tobe_sqr_complete['cancel_count'][mj],0)
        if order == "close":
            skip_leg1 = 1
            requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': "Sqrd "+str(order_tobe_sqr_complete['leg'][mj])})
            print("Sqr "+str(order_tobe_sqr_complete['leg'][mj])+str(order_tobe_sqr_complete['strike'][mj])+str(order_tobe_sqr_complete['contract'][mj])," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
            print("sell position square off"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
        else:
            requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': "Fail to Sqr "+str(order_tobe_sqr_complete['leg'][mj])})
            err_sqr = err_sqr + 1 
    return err_sqr

def square_off_sell_exp_1(order_manage,order_tobe_sqr_complete,s):
    global file_list,kite
    order_manage = order_manage
    order_tobe_sqr_complete = order_tobe_sqr_complete
    err_sqr = 0
    shift_data = 0
    skip_leg1 = 0
    print("squaring off sell position","klkklklk"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
    retry_order = 0
    for mj in range(0,len(order_tobe_sqr_complete)):
        print("Sqr "+str(order_tobe_sqr_complete['leg'][mj])+str(order_tobe_sqr_complete['strike'][mj])+str(order_tobe_sqr_complete['contract'][mj])," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
        if order_tobe_sqr_complete['buy_sell'][mj] == "SELL":
            buy_sell="BUY"
        else:
            buy_sell="SELL"
        # df_opt = get_data(order_tobe_sqr_complete['instrument_id'][mj],fromm, fromm, "minute",s)
        df_opt = pd.DataFrame(kite.historical_data(order_tobe_sqr_complete['instrument_id'][mj], fromm, fromm, "minute", continuous=False, oi=True))
        s = kite
        retry_order = 0
        order = order_place_sqr_complete(s,file_list,drive,order_tobe_sqr_complete['current_signal'][mj],order_tobe_sqr_complete['instrument_id'][mj],order_tobe_sqr_complete['trading_symbol'][mj],df_opt,order_tobe_sqr_complete['qty'][mj],order_tobe_sqr_complete['instru'][mj],0,order_tobe_sqr_complete['leg'][mj],order_tobe_sqr_complete['strike'][mj],order_tobe_sqr_complete['contract'][mj],order_tobe_sqr_complete['expiry'][mj],0,1,buy_sell,retry_order,order_tobe_sqr_complete['status'][mj],order_tobe_sqr_complete['reject_count'][mj],order_tobe_sqr_complete['cancel_count'][mj],0)
        if order == "close":
            skip_leg1 = 1
            requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': "Sqrd "+str(order_tobe_sqr_complete['leg'][mj])})
            print("Sqr "+str(order_tobe_sqr_complete['leg'][mj])+str(order_tobe_sqr_complete['strike'][mj])+str(order_tobe_sqr_complete['contract'][mj])," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
            print("Sell position square off"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
        else:
            requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': "Fail to Sqr "+str(order_tobe_sqr_complete['leg'][mj])})
            err_sqr = err_sqr + 1 
    return err_sqr
    
def square_off_buy(order_manage,order_tobe_sqr_complete,s):
    global file_list, kite
    order_manage = order_manage
    order_tobe_sqr_complete = order_tobe_sqr_complete
    err_sqr = 0
    shift_data = 0
    skip_leg1 = 0
    print("squaring off buy position","klkklklk"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
    for mj in range(0,len(order_tobe_sqr_complete)):
        print("Sqr "+str(order_tobe_sqr_complete['leg'][mj])+str(order_tobe_sqr_complete['strike'][mj])+str(order_tobe_sqr_complete['contract'][mj])," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
        if order_tobe_sqr_complete['buy_sell'][mj] == "SELL":
            buy_sell="BUY"
        else:
            buy_sell="SELL"
        # df_opt = get_data(order_tobe_sqr_complete['instrument_id'][mj],fromm, fromm, "minute",s)
        df_opt = pd.DataFrame(kite.historical_data(order_tobe_sqr_complete['instrument_id'][mj], fromm, fromm, "minute", continuous=False, oi=True))
        s = kite
        retry_order = 0
        order = order_place_sqr_complete(s,file_list,drive,order_tobe_sqr_complete['current_signal'][mj],order_tobe_sqr_complete['instrument_id'][mj],order_tobe_sqr_complete['trading_symbol'][mj],df_opt,order_tobe_sqr_complete['qty'][mj],order_tobe_sqr_complete['instru'][mj],0,order_tobe_sqr_complete['leg'][mj],order_tobe_sqr_complete['strike'][mj],order_tobe_sqr_complete['contract'][mj],order_tobe_sqr_complete['expiry'][mj],0,1,buy_sell,retry_order,order_tobe_sqr_complete['status'][mj],order_tobe_sqr_complete['reject_count'][mj],order_tobe_sqr_complete['cancel_count'][mj],0)
        if order == "close":
            skip_leg1 = 1
            requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': "Sqrd "+str(order_tobe_sqr_complete['leg'][mj])})
            print("Sqr "+str(order_tobe_sqr_complete['leg'][mj])+str(order_tobe_sqr_complete['strike'][mj])+str(order_tobe_sqr_complete['contract'][mj])," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
            print("buy position square off"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
        else:
            requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': "Fail to Sqr "+str(order_tobe_sqr_complete['leg'][mj])})
            err_sqr = err_sqr + 1 
    return err_sqr

def square_off_buy_exp_1(order_manage,order_tobe_sqr_complete,s):
    global file_list, kite
    order_manage = order_manage
    order_tobe_sqr_complete = order_tobe_sqr_complete
    err_sqr = 0
    shift_data = 0
    skip_leg1 = 0
    print("squaring off buy position","klkklklk", file=sys.stderr)
    for mj in range(0,len(order_tobe_sqr_complete)):
        print("Sqr "+str(order_tobe_sqr_complete['leg'][mj])+str(order_tobe_sqr_complete['strike'][mj])+str(order_tobe_sqr_complete['contract'][mj])," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
        if order_tobe_sqr_complete['buy_sell'][mj] == "SELL":
            buy_sell="BUY"
        else:
            buy_sell="SELL"
        # df_opt = get_data(order_tobe_sqr_complete['instrument_id'][mj],fromm, fromm, "minute",s)
        df_opt = pd.DataFrame(kite.historical_data(order_tobe_sqr_complete['instrument_id'][mj], fromm, fromm, "minute", continuous=False, oi=True))
        s = kite
        retry_order = 0
        order = order_place_sqr_complete(s,file_list,drive,order_tobe_sqr_complete['current_signal'][mj],order_tobe_sqr_complete['instrument_id'][mj],order_tobe_sqr_complete['trading_symbol'][mj],df_opt,order_tobe_sqr_complete['qty'][mj],order_tobe_sqr_complete['instru'][mj],0,order_tobe_sqr_complete['leg'][mj],order_tobe_sqr_complete['strike'][mj],order_tobe_sqr_complete['contract'][mj],order_tobe_sqr_complete['expiry'][mj],0,1,buy_sell,retry_order,order_tobe_sqr_complete['status'][mj],order_tobe_sqr_complete['reject_count'][mj],order_tobe_sqr_complete['cancel_count'][mj],0)
        if order == "close":
            skip_leg1 = 1
            requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': "Sqrd "+str(order_tobe_sqr_complete['leg'][mj])})
            print("Sqr "+str(order_tobe_sqr_complete['leg'][mj])+str(order_tobe_sqr_complete['strike'][mj])+str(order_tobe_sqr_complete['contract'][mj])," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
            print("buy position square off"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
        else:
            requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': "Fail to Sqr "+str(order_tobe_sqr_complete['leg'][mj])})
            err_sqr = err_sqr + 1 
    return err_sqr

def buy_pos(x,s,df25,df,exp_1,exp_2,weekly_rollover,monthly_rollover,square_off,df54):
    current_time = datetime.datetime.now(pytz.timezone('Asia/Kolkata')).time()
    global  currently_sell_holding,currently_buy_holding,df79,df99,file_list,instru_name,qty_plc
    df79 = x
    sess = s
    retry_order = 0
    weekly_rollover = weekly_rollover
    monthly_rollover = monthly_rollover
    square_off = square_off
    print("initiate buy position"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
    today = datetime.datetime.now(pytz.timezone('Asia/Kolkata')).date()
# Roll - monthly exp & time >= 3:20PM or monthly expiry & buy signal generated when sell position held & time >= 1:00PM
# Get Fut ID
    if exp_2 and (datetime.datetime.strptime(str(exp_2), '%Y-%m-%d').date()==datetime.datetime.now(pytz.timezone('Asia/Kolkata')).date()) and ((current_time>=datetime.time(15,20,0)) or ((current_time>=datetime.time(13,0,0)) and (currently_sell_holding))):
        print("future rollover"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
        fut_id = df25.loc[(df25['name'] == instru_name) & ((pd.to_datetime(df25.expiry).dt.date -
                    datetime.datetime.now(pytz.timezone('Asia/Kolkata')).date()) > timedelta(days=0)) & ((pd.to_datetime(df25.expiry).dt.date -
                    datetime.datetime.now(pytz.timezone('Asia/Kolkata')).date()) < timedelta(days=36))& (df25['instrument_type'] == "FUT")]['instrument_token'].values[0]
    else:
        fut_id = df25.loc[(df25['name'] == instru_name) & ((pd.to_datetime(df25.expiry).dt.date -
            datetime.datetime.now(pytz.timezone('Asia/Kolkata')).date()) >= timedelta(days=0)) & ((pd.to_datetime(df25.expiry).dt.date -
            datetime.datetime.now(pytz.timezone('Asia/Kolkata')).date()) < timedelta(days=36))& (df25['instrument_type'] == "FUT")]['instrument_token'].values[0]
    df25['expiry'] = pd.to_datetime(df25.expiry, format='%Y-%m-%d').dt.date

# Filter instrument.csv to next week data if weekly exp & time >= 3:20PM or weekly expiry & buy signal generated when sell position held & time >= 1:00PM
# else select same week data from instrument.csv
    if (exp_1 or exp_2) and ((datetime.datetime.strptime(str(exp_1), '%Y-%m-%d').date()==today) or (datetime.datetime.strptime(str(exp_2), '%Y-%m-%d').date()==today)) and ((current_time>=datetime.time(15,20,0)) or ((current_time>=datetime.time(13,0,0)) and (currently_buy_holding))):
        df25 = df25[(df25['name'] == instru_name) & (df25['expiry'] > today)]
    else:
        df25 = df25[(df25['name'] == instru_name) & (df25['expiry'] >= today)]
    df25 = df25.sort_values(by=['expiry'])
    
    stk1 = ((int(math.floor(df54['close'].iloc[-1] / 100.0)) * 100)+100)
    opt_id_1 = fut_id

    expiry_opt_1 = df25.loc[(df25['instrument_token']==opt_id_1)]['expiry'].values[0]

    #get tradingsymbol from instrument.csv
    symbol_opt_1 = df25.loc[(df25['instrument_token']==fut_id)]['tradingsymbol'].values[0]
    current_signal = "BUY"
    instru = "Spread Up 1"
    quantity = qty_plc
    leg1="leg1"
    contract1 = "F"
    fresh_position = 1
    first_order = 0
    second_order = 0
    if(df79.empty):
        trade_id = 1
        print("trade id set "," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
    else:
        trade_id = df79['trade_id'].max() + 1
    # df_opt_1 = get_data(opt_id_1,fromm, fromm, "minute",s)
    df_opt_1 = pd.DataFrame(kite.historical_data(opt_id_1, fromm, fromm, "minute", continuous=False, oi=True))
    price_opt_1 = df_opt_1['close'].iloc[-1]
    s= kite
    price_opt_1_ideal = df_opt_1['close'].iloc[-1]
    instru = "Spread Up 1"
    quantity = qty_plc
    fresh_position = 1
    trade_id = trade_id + 1
    print("order for Leg2 being placed"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
    first_order = order_place(s,file_list,drive,current_signal,opt_id_1,symbol_opt_1,price_opt_1,quantity,instru,fresh_position,leg1,stk1,contract1,expiry_opt_1,trade_id,0,"BUY",retry_order,"",0,0,0)
    if first_order:
        requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': "Spread 1 S Leg 1 executed"})
        print("Spread 1 S "+str(int(stk1))+"F Pc "+str(df_opt_1['close'].iloc[-1])+" No "+str(quantity)," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
    else:
        requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': "Spread 1 S Leg 1 failed to execute"})
        print("error in executing Leg 1"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
    currently_buy_holding = True
    currently_sell_holding = False
    hh = pd.DataFrame()
    hh.at[0,'currently_buy_holding'] = currently_buy_holding
    hh.at[0,'currently_sell_holding'] = currently_sell_holding
    # hh = "currently_buy_holding,currently_sell_holding\nTrue,False"
    # with open("hh.txt", "w") as f:
    #     f.write(hh)
    # bb = ""
    # nn = ""
    # for index, file in enumerate(file_list):
    #     if(file['title'] =="hh.txt"):
    #         hh = file['id']
    # if hh:
    #     update_file = drive.CreateFile({'id': hh})
    #     update_file.SetContentFile("hh.txt")
    #     update_file.Upload()
    hh_count = db.list_collection_names().count("hh")
    if(hh_count < 1):
        db.create_collection("hh")
    else:
        db.hh.delete_many({})
    collection = db["hh"]
    dict1 = hh.to_dict('records')
    for x in range(0,len(dict1)):
        collection.insert_one(dict1[x])
    print("buy position status saved to files"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
    return 0

def sell_pos(x,s,df25,df,exp_1,exp_2,weekly_rollover,monthly_rollover,square_off,df54):
    current_time = datetime.datetime.now(pytz.timezone('Asia/Kolkata')).time()
    global  currently_sell_holding,currently_buy_holding,df79,df99,file_list,instru_name,qty_plc, kite
    df79 = x
    retry_order = 0
    weekly_rollover = weekly_rollover
    monthly_rollover = monthly_rollover
    square_off = square_off
    print("initiate sell position"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
    today = datetime.datetime.now(pytz.timezone('Asia/Kolkata')).date()
    if exp_2 and (datetime.datetime.strptime(str(exp_2), '%Y-%m-%d').date()==today) and ((current_time>=datetime.time(15,20,0)) or ((current_time>=datetime.time(13,0,0)) and (currently_buy_holding))):
        print("future rollover"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
        fut_id = df25.loc[(df25['name'] == instru_name) & ((pd.to_datetime(df25.expiry).dt.date -
                    datetime.datetime.now(pytz.timezone('Asia/Kolkata')).date()) > timedelta(days=0)) & ((pd.to_datetime(df25.expiry).dt.date -
                    datetime.datetime.now(pytz.timezone('Asia/Kolkata')).date()) < timedelta(days=36))& (df25['instrument_type'] == "FUT")]['instrument_token'].values[0]
    else:
        fut_id = df25.loc[(df25['name'] == instru_name) & ((pd.to_datetime(df25.expiry).dt.date -
            datetime.datetime.now(pytz.timezone('Asia/Kolkata')).date()) >= timedelta(days=0)) & ((pd.to_datetime(df25.expiry).dt.date -
            datetime.datetime.now(pytz.timezone('Asia/Kolkata')).date()) < timedelta(days=36))& (df25['instrument_type'] == "FUT")]['instrument_token'].values[0]

    df25['expiry'] = pd.to_datetime(df25.expiry, format='%Y-%m-%d').dt.date
    if (exp_1 or exp_2) and ((datetime.datetime.strptime(str(exp_1), '%Y-%m-%d').date()==today) or (datetime.datetime.strptime(str(exp_2), '%Y-%m-%d').date()==today)) and ((current_time>=datetime.time(15,20,0)) or ((current_time>=datetime.time(13,0,0)) and (currently_buy_holding))):
        df25 = df25[(df25['name'] == instru_name) & (df25['expiry'] > today)]
    else:
        df25 = df25[(df25['name'] == instru_name) & (df25['expiry'] >= today)]
    df25 = df25.sort_values(by=['expiry'])

    stk1 = ((int(math.floor(df54['close'].iloc[-1] / 100.0)) * 100)+100)
    opt_id_1 = fut_id

    expiry_opt_1 = df25.loc[(df25['instrument_token']==opt_id_1)]['expiry'].values[0]

    #get tradingsymbol from instrument.csv
    symbol_opt_1 = df25.loc[(df25['instrument_token']==fut_id)]['tradingsymbol'].values[0]
    current_signal = "SELL"
    instru = "Spread Down 1"
    quantity = qty_plc
    leg1="leg1"
    contract1 = "F"
    fresh_position = 1
    first_order = 0
    second_order = 0
    if(df79.empty):
        trade_id = 1
        print("trade id set "," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
    else:
        trade_id = df79['trade_id'].max() + 1
    # df_opt_1 = get_data(opt_id_1,fromm, fromm, "minute",s)
    df_opt_1 = pd.DataFrame(kite.historical_data(opt_id_1, fromm, fromm, "minute", continuous=False, oi=True))
    s = kite
    price_opt_1 = df_opt_1['close'].iloc[-1]
    price_opt_1_ideal = df_opt_1['close'].iloc[-1]
    instru = "Spread Down 1"
    quantity = qty_plc
    fresh_position = 1
    trade_id = trade_id + 1
    print("order for Leg2 being placed"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
    first_order = order_place(s,file_list,drive,current_signal,opt_id_1,symbol_opt_1,price_opt_1,quantity,instru,fresh_position,leg1,stk1,contract1,expiry_opt_1,trade_id,0,"SELL",retry_order,"",0,0,0)
    if first_order:
        requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': "Spread 1 S Leg 1 executed"})
        print("Spread 1 S "+str(int(stk1))+"F Pc "+str(df_opt_1['close'].iloc[-1])+" No "+str(quantity)," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
    else:
        requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': "Spread 1 S Leg 1 failed to execute"})
        print("error in executing Leg 1"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
    currently_buy_holding = False
    currently_sell_holding = True
    hh = "currently_buy_holding,currently_sell_holding\nFalse,True"
    with open("hh.txt", "w") as f:
        f.write(hh)
    bb = ""
    nn = ""
    for index, file in enumerate(file_list):
        if(file['title'] =="hh.txt"):
            hh = file['id']
    if hh:
        update_file = drive.CreateFile({'id': hh})
        update_file.SetContentFile("hh.txt")
        update_file.Upload()
    print("sell position status saved to files"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
    return 0

def save_pos_buy(s,df25,df,exp_1,exp_2,weekly_rollover,monthly_rollover,df54):
    global  currently_sell_holding,currently_buy_holding,file_list,instru_name,qty_plc,kite
    current_time = datetime.datetime.now(pytz.timezone('Asia/Kolkata')).time()
    weekly_rollover = weekly_rollover
    monthly_rollover = monthly_rollover
    df = df
    today = datetime.datetime.now(pytz.timezone('Asia/Kolkata')).date()
    if exp_2 and (datetime.datetime.strptime(str(exp_2), '%Y-%m-%d').date()==datetime.datetime.now(pytz.timezone('Asia/Kolkata')).date()) and ((current_time>=datetime.time(15,20,0)) or ((current_time>=datetime.time(13,0,0)) and (currently_sell_holding))):
        fut_id = df25.loc[(df25['name'] == instru_name) & ((pd.to_datetime(df25.expiry).dt.date -
                    datetime.datetime.now(pytz.timezone('Asia/Kolkata')).date()) > timedelta(days=0)) & ((pd.to_datetime(df25.expiry).dt.date -
                    datetime.datetime.now(pytz.timezone('Asia/Kolkata')).date()) < timedelta(days=36))& (df25['instrument_type'] == "FUT")]['instrument_token'].values[0]
    else:
        fut_id = df25.loc[(df25['name'] == instru_name) & ((pd.to_datetime(df25.expiry).dt.date -
            datetime.datetime.now(pytz.timezone('Asia/Kolkata')).date()) >= timedelta(days=0)) & ((pd.to_datetime(df25.expiry).dt.date -
            datetime.datetime.now(pytz.timezone('Asia/Kolkata')).date()) < timedelta(days=36))& (df25['instrument_type'] == "FUT")]['instrument_token'].values[0]
    df25['expiry'] = pd.to_datetime(df25.expiry, format='%Y-%m-%d').dt.date
    if exp_1 and (datetime.datetime.strptime(str(exp_1), '%Y-%m-%d').date()==datetime.datetime.now(pytz.timezone('Asia/Kolkata')).date()) and ((current_time>=datetime.time(15,20,0)) or ((current_time>=datetime.time(13,0,0)) and (currently_sell_holding))):
        df25 = df25[(df25['name'] == instru_name) & (df25['expiry'] > datetime.datetime.now(pytz.timezone('Asia/Kolkata')).date())]
    else:
        df25 = df25[(df25['name'] == instru_name) & (df25['expiry'] >= datetime.datetime.now(pytz.timezone('Asia/Kolkata')).date())]
    df25 = df25.sort_values(by=['expiry'])
    stk1 = ((int(math.floor(df54['close'].iloc[-1] / 100.0)) * 100)+100)
    opt_id_1 = fut_id
    expiry_opt_1 = df25.loc[(df25['instrument_token']==opt_id_1)]['expiry'].values[0]

    #get tradingsymbol from instrument.csv
    symbol_opt_1 = df25.loc[(df25['instrument_token']==fut_id)]['tradingsymbol'].values[0]
    current_signal = "BUY"
    instru = "Spread Up 1"
    quantity = qty_plc
    leg1="leg1"
    contract1 = "F"
    fresh_position = 1
    first_order = 0
    # df_opt_1 = get_data(opt_id_1,fromm, fromm, "minute",s)
    df_opt_1 = pd.DataFrame(kite.historical_data(opt_id_1, fromm, fromm, "minute", continuous=False, oi=True))
    s = kite
    price_opt_1 = df_opt_1['close'].iloc[-1]
    order_tobe_exec_log = []
    order_tobe_exec_log.append({'current_signal':current_signal,'entry_time':datetime.datetime.now(pytz.timezone('Asia/Kolkata')),'exit_time':'','leg':"leg1",'instru':"Spread Up 1",'order_id': 0,'qty':quantity,'price_when_order_placed': price_opt_1,'fresh_position': fresh_position,'buy_sell': "BUY",'instrument_id':opt_id_1,'trading_symbol':symbol_opt_1,'modification_error':0,'cancellation_error':0,'rejection_error':0,'multiple_sqr_off_error':0,'order_pending_error':0,'multi_sqr_off_error':0,'executed':0,'all_api_failed':0,'leg1_sqr_off_error':0,'leg1_fail_leg2_not_placed':0,'entry_price': 0,'strike': stk1,'contract': contract1,'status':"",'exit_price':'','expiry':expiry_opt_1,'trade_id':0,'sqr_off_order':0,'reject_count':0,'cancel_count':0,'sqr_order_id':0})
    df07 = pd.DataFrame.from_dict(order_tobe_exec_log)
    # kk = 0
    # for index, file in enumerate(file_list):
    #     if(file['title'] =="order_tobe_exec_log.txt"):
    #         kk = file['id']
    #     counter=0
    #     while counter<10:
    #         try:
    #             df29 = file.GetContentFile(file['title'])
    #             counter=10
    #         except Exception as e:
    #             logger.error(e)
    #             counter=counter+1
    # if bool(kk):
    #     df07.to_csv("order_tobe_exec_log.txt", index=False)
    #     df08 = pd.concat([df29,df08])
    #     df08.to_csv("order_tobe_exec_log.txt", index=False)
    #     update_file = drive.CreateFile({'id': kk})
    #     update_file.SetContentFile("order_tobe_exec_log.txt")
    #     update_file.Upload()
    # else:
    #     df07.to_csv("order_tobe_exec_log.txt", index=False)
    #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_tobe_exec_log.txt"})
    #     gfile.SetContentFile("order_tobe_exec_log.txt")
    #     gfile.Upload()
    
    order_tobe_exec_log_count = db.list_collection_names().count("order_tobe_exec_log")
    if(order_tobe_exec_log_count < 1):
        db.create_collection("order_tobe_exec_log")
        df08 = df07
    else:
        collection = db["order_tobe_exec_log"]
        x = collection.find()
        df29 = pd.DataFrame()
        for y in x:
            df29 = pd.DataFrame([y])
            df08 = pd.concat([df29,df08])
            # df29 = df08
        # df29 = pd.DataFrame([x])
        df08 = pd.concat([df07,df08])
        db.order_tobe_exec_log.delete_many({})
    collection = db["order_tobe_exec_log"]
    dict1 = df08.to_dict('records')
    for x in range(0,len(dict1)):
        collection.insert_one(dict1[x])

    print("buy position status saved to files"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
    return 0

def save_pos_sell(s,df25,df,exp_1,exp_2,weekly_rollover,monthly_rollover,df54):
    global  currently_sell_holding,currently_buy_holding,file_list,instru_name,qty_plc,kite
    current_time = datetime.datetime.now(pytz.timezone('Asia/Kolkata')).time()
    weekly_rollover = weekly_rollover
    monthly_rollover = monthly_rollover
    df = df
    today = datetime.datetime.now(pytz.timezone('Asia/Kolkata')).date()
    if exp_2 and (datetime.datetime.strptime(str(exp_2), '%Y-%m-%d').date()==datetime.datetime.now(pytz.timezone('Asia/Kolkata')).date()) and ((current_time>=datetime.time(15,20,0)) or ((current_time>=datetime.time(13,0,0)) and (currently_sell_holding))):
        fut_id = df25.loc[(df25['name'] == instru_name) & ((pd.to_datetime(df25.expiry).dt.date -
                    datetime.datetime.now(pytz.timezone('Asia/Kolkata')).date()) > timedelta(days=0)) & ((pd.to_datetime(df25.expiry).dt.date -
                    datetime.datetime.now(pytz.timezone('Asia/Kolkata')).date()) < timedelta(days=36))& (df25['instrument_type'] == "FUT")]['instrument_token'].values[0]
    else:
        fut_id = df25.loc[(df25['name'] == instru_name) & ((pd.to_datetime(df25.expiry).dt.date -
            datetime.datetime.now(pytz.timezone('Asia/Kolkata')).date()) >= timedelta(days=0)) & ((pd.to_datetime(df25.expiry).dt.date -
            datetime.datetime.now(pytz.timezone('Asia/Kolkata')).date()) < timedelta(days=36))& (df25['instrument_type'] == "FUT")]['instrument_token'].values[0]
    df25['expiry'] = pd.to_datetime(df25.expiry, format='%Y-%m-%d').dt.date
    if exp_1 and (datetime.datetime.strptime(str(exp_1), '%Y-%m-%d').date()==datetime.datetime.now(pytz.timezone('Asia/Kolkata')).date()) and ((current_time>=datetime.time(15,20,0)) or ((current_time>=datetime.time(13,0,0)) and (currently_sell_holding))):
        df25 = df25[(df25['name'] == instru_name) & (df25['expiry'] > datetime.datetime.now(pytz.timezone('Asia/Kolkata')).date())]
    else:
        df25 = df25[(df25['name'] == instru_name) & (df25['expiry'] >= datetime.datetime.now(pytz.timezone('Asia/Kolkata')).date())]
    df25 = df25.sort_values(by=['expiry'])
    stk1 = ((int(math.floor(df54['close'].iloc[-1] / 100.0)) * 100)+100)
    opt_id_1 = fut_id
    expiry_opt_1 = df25.loc[(df25['instrument_token']==opt_id_1)]['expiry'].values[0]

    symbol_opt_1 = df25.loc[(df25['instrument_token']==fut_id)]['tradingsymbol'].values[0]
    current_signal = "SELL"
    instru = "Spread Down 1"
    quantity = qty_plc
    leg1="leg1"
    contract1 = "F"
    fresh_position = 1
    first_order = 0
    # df_opt_1 = get_data(opt_id_1,fromm, fromm, "minute",s)
    df_opt_1 = pd.DataFrame(kite.historical_data(opt_id_1, fromm, fromm, "minute", continuous=False, oi=True))
    s = kite
    price_opt_1 = df_opt_1['close'].iloc[-1]
    order_tobe_exec_log = []
    order_tobe_exec_log.append({'current_signal':current_signal,'entry_time':datetime.datetime.now(pytz.timezone('Asia/Kolkata')),'exit_time':'','leg':"leg1",'instru':"Spread Down 1",'order_id': 0,'qty':quantity,'price_when_order_placed': price_opt_1,'fresh_position': fresh_position,'buy_sell': "SELL",'instrument_id':opt_id_1,'trading_symbol':symbol_opt_1,'modification_error':0,'cancellation_error':0,'rejection_error':0,'multiple_sqr_off_error':0,'order_pending_error':0,'multi_sqr_off_error':0,'executed':0,'all_api_failed':0,'leg1_sqr_off_error':0,'leg1_fail_leg2_not_placed':0,'entry_price': 0,'strike': stk1,'contract': contract1,'status':"",'exit_price':'','expiry':expiry_opt_1,'trade_id':0,'sqr_off_order':0,'reject_count':0,'cancel_count':0,'sqr_order_id':0})
    df07 = pd.DataFrame.from_dict(order_tobe_exec_log)
    # kk = 0
    # for index, file in enumerate(file_list):
    #     if(file['title'] =="order_tobe_exec_log.txt"):
    #         kk = file['id']
    #     counter=0
    #     while counter<10:
    #         try:
    #             df29 = file.GetContentFile(file['title'])
    #             counter=10
    #         except Exception as e:
    #             logger.error(e)
    #             counter=counter+1
    # if bool(kk):
    #     df07.to_csv("order_tobe_exec_log.txt", index=False)
    #     df08 = pd.concat([df29,df08])
    #     df08.to_csv("order_tobe_exec_log.txt", index=False)
    #     update_file = drive.CreateFile({'id': kk})
    #     update_file.SetContentFile("order_tobe_exec_log.txt")
    #     update_file.Upload()
    # else:
    #     df07.to_csv("order_tobe_exec_log.txt", index=False)
    #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_tobe_exec_log.txt"})
    #     gfile.SetContentFile("order_tobe_exec_log.txt")
    #     gfile.Upload()
    order_tobe_exec_log_count = db.list_collection_names().count("order_tobe_exec_log")
    if(order_tobe_exec_log_count < 1):
        db.create_collection("order_tobe_exec_log")
        df08 = df07
    else:
        collection = db["order_tobe_exec_log"]
        x = collection.find()
        df29 = pd.DataFrame()
        for y in x:
            df29 = pd.DataFrame([y])
            df08 = pd.concat([df29,df08])
            # df29 = df08
        df08 = pd.concat([df07,df08])
        db.order_tobe_exec_log.delete_many({})
    collection = db["order_tobe_exec_log"]
    dict1 = df08.to_dict('records')
    for x in range(0,len(dict1)):
        collection.insert_one(dict1[x])

    print("sell position status saved to files"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
    return 0
   
def hello_world():
    #logging.basicConfig(level=logging.DEBUG)
    global  currently_sell_holding,TOKEN, CHAT_ID, SEND_URL,file_list,df79,df99,file_list,drive
    global currently_buy_holding,kite
    s = Session()
    user_id = "YZ7782"
    password = "NewYork123#"
    m = pyotp.TOTP("NOB3B7VSDMP5MIRKMXC5NQ5WLXEX7EO6")
    twofa = m.now()
    enctoken = get_enctoken(user_id, password, twofa)
    kite = KiteApp(enctoken=enctoken)
    # login_url = "https://kite.zerodha.com/api/login"
    # r = s.post(login_url,
    #            data={
    #                "user_id": user_id,
    #                "password": password
    #            })
    # j = json.loads(r.text)
    # twofa_url = "https://kite.zerodha.com/api/twofa"
    # data = {
    #     "user_id": user_id,
    #     "request_id": j['data']["request_id"],
    #     "twofa_value": twofa
    # }
    # r = s.post(twofa_url, data=data)
    # j = json.loads(r.text)
    # enctoken = "enctoken " + s.cookies["enctoken"]
    # print(enctoken," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
    # kf_session = s.cookies["kf_session"]
    # public_token = s.cookies["public_token"]
    # enc_token = s.cookies['enctoken']
    # h = {}
    # h['authorization'] = "enctoken {}".format(enc_token)
    # h['x-kite-version'] = '2.4.0'
    # h['sec-fetch-site'] = 'same-origin'
    # h['sec-fetch-mode'] = 'cors'
    # h['sec-fetch-dest'] = 'empty'
    # h['User-Agent'] = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5)\AppleWebKit/537.36 (KHTML, like Gecko) Cafari/537.36'
    # s.headers.update(h)
#     kite = Kite(enctoken=enc_token)
    #response = pd.read_csv("instruments.csv")
    #content = response.read().decode('utf-8')
    #print(content)
    response =""
    response1 = ""
    instruments1 = pd.DataFrame()
    COLUMN_NAMES=['current_signal','entry_time','exit_time','leg','instru','order_id','qty','price_when_order_placed','fresh_position','buy_sell','instrument_id','trading_symbol','modification_error','cancellation_error','rejection_error','multiple_sqr_off_error','order_pending_error','multi_sqr_off_error','executed','all_api_failed','leg1_sqr_off_error','leg1_fail_leg2_not_placed','entry_price','strike','contract','status','exit_price','expiry','trade_id','sqr_off_order','reject_count','cancel_count','sqr_order_id']
    order_manage = pd.DataFrame(columns=COLUMN_NAMES) 
    order_tobe_exec_log = pd.DataFrame(columns=COLUMN_NAMES) 
    order_cancel = pd.DataFrame(columns=COLUMN_NAMES) 
    order_pending_complete = pd.DataFrame(columns=COLUMN_NAMES) 
    order_reject_complete = pd.DataFrame(columns=COLUMN_NAMES) 
    order_complete = pd.DataFrame(columns=COLUMN_NAMES) 
    order_pending = pd.DataFrame(columns=COLUMN_NAMES) 
    order_reject = pd.DataFrame(columns=COLUMN_NAMES)
    order_manage= pd.DataFrame(columns=COLUMN_NAMES)
    order_sqr_complete = pd.DataFrame(columns=COLUMN_NAMES)
    order_sqr_complete_cons = pd.DataFrame(columns=COLUMN_NAMES)
    order_sqr_multiple = pd.DataFrame(columns=COLUMN_NAMES)
    order_cancel_multiple = pd.DataFrame(columns=COLUMN_NAMES)
    order_pending_multiple= pd.DataFrame(columns=COLUMN_NAMES)
    order_reject_multiple = pd.DataFrame(columns=COLUMN_NAMES)
    order_multiple = pd.DataFrame(columns=COLUMN_NAMES)
    order_tobe_exec_log = pd.DataFrame(columns=COLUMN_NAMES)
    order_tobe_sqr_complete = pd.DataFrame(columns=COLUMN_NAMES)
    order_cancel_complete = pd.DataFrame(columns=COLUMN_NAMES)
    order_tobe_cancel = pd.DataFrame(columns=COLUMN_NAMES)
    order_tobe_cancel_complete = pd.DataFrame(columns=COLUMN_NAMES)
    order_tobe_cancel_multiple = pd.DataFrame(columns=COLUMN_NAMES)
    order_pending_tobe_cancel = pd.DataFrame(columns=COLUMN_NAMES)
    order_pending_complete_tobe_cancel = pd.DataFrame(columns=COLUMN_NAMES)
    err_sqr_1 = pd.DataFrame(columns=COLUMN_NAMES)
    #df25 = pd.read_csv(StringIO(content), sep=',')
#     counter = 0
#     while counter<10:
#         try:
#             file_list = drive.ListFile({'q' : f"'{folder}' in parents and trashed=false"}).GetList()
#             counter=10
#         except Exception as e:
#             logger.error(e)
#             counter=counter+1

#     for index, file in enumerate(file_list):
#         #print(index+1, 'file downloaded : ', file['title'])
#         counter=0
#         while counter<10:
#             try:
#                 df29 = file.GetContentFile(file['title'])
#                 counter=10
#             except Exception as e:
#                 logger.error(e)
#                 counter=counter+1
#         #csv_raw = StringIO(file.text)
#         if(file['title'] =="instruments.csv"):
#             df25 = pd.read_csv(file['title'])
    # instruments_count = db.list_collection_names().count("instruments")
    # if(instruments_count > 0):
    #     collection = db["instruments"]
    #     x = collection.find()
    #     # order_tobe_cancel = pd.read_csv(file['title'])
    #     instruments = pd.DataFrame([x])

    order_tobe_cancel_count = db.list_collection_names().count("order_tobe_cancel")
    if(order_tobe_cancel_count > 0) and (db.order_tobe_cancel.count_documents({}) > 0):
        collection = db["order_tobe_cancel"]
        x = collection.find()
        instruments1 = pd.DataFrame()
        temp = pd.DataFrame()
        for y in x:
            jj = pd.DataFrame([y])
            instruments1 = pd.concat([temp,jj])
            temp = instruments1
        # order_tobe_cancel = pd.read_csv(file['title'])
        order_tobe_cancel = instruments1
    order_tobe_cancel_complete_count = db.list_collection_names().count("order_tobe_cancel_complete")
    if(order_tobe_cancel_complete_count > 0) and (db.order_tobe_cancel_complete.count_documents({}) > 0):
        collection = db["order_tobe_cancel_complete"]
        x = collection.find()
        instruments1 = pd.DataFrame()
        temp = pd.DataFrame()
        for y in x:
            jj = pd.DataFrame([y])
            instruments1 = pd.concat([temp,jj])
            temp = instruments1
        # order_tobe_cancel = pd.read_csv(file['title'])
        order_tobe_cancel_complete = instruments1
    order_manage_count = db.list_collection_names().count("order_manage")
    if(order_manage_count > 0) and (db.order_manage.count_documents({}) > 0):
        collection = db["order_manage"]
        x = collection.find()
        instruments1 = pd.DataFrame()
        temp = pd.DataFrame()
        for y in x:
            jj = pd.DataFrame([y])
            instruments1 = pd.concat([temp,jj])
            temp = instruments1
        # order_tobe_cancel = pd.read_csv(file['title'])
        order_manage = instruments1
    order_reject_count = db.list_collection_names().count("order_reject")
    if(order_reject_count > 0) and (db.order_reject.count_documents({}) > 0):
        collection = db["order_reject"]
        x = collection.find()
        instruments1 = pd.DataFrame()
        temp = pd.DataFrame()
        # order_tobe_cancel = pd.read_csv(file['title'])
        for y in x:
            jj = pd.DataFrame([y])
            instruments1 = pd.concat([temp,jj])
            temp = instruments1
        order_reject = instruments1
    order_cancel_count = db.list_collection_names().count("order_cancel")
    if(order_cancel_count > 0) and (db.order_cancel.count_documents({}) > 0):
        collection = db["order_cancel"]
        x = collection.find()
        instruments1 = pd.DataFrame()
        temp = pd.DataFrame()
        # order_tobe_cancel = pd.read_csv(file['title'])
        for y in x:
            jj = pd.DataFrame([y])
            instruments1 = pd.concat([temp,jj])
            temp = instruments1
        order_cancel = instruments1
    order_pending_count = db.list_collection_names().count("order_pending")
    if(order_pending_count > 0) and (db.order_pending.count_documents({}) > 0):
        collection = db["order_pending"]
        x = collection.find()
        instruments1 = pd.DataFrame()
        temp = pd.DataFrame()
        # order_tobe_cancel = pd.read_csv(file['title'])
        for y in x:
            jj = pd.DataFrame([y])
            instruments1 = pd.concat([temp,jj])
            temp = instruments1
        order_pending = instruments1
    order_complete_count = db.list_collection_names().count("order_complete")
    print(order_complete)
    if(order_complete_count > 0) and (db.order_complete.count_documents({}) > 0):
        collection = db["order_complete"]
        x = collection.find()
        temp = pd.DataFrame()
        # order_tobe_cancel = pd.read_csv(file['title'])
        for y in x:
            jj = pd.DataFrame([y])
            instruments1 = pd.concat([temp,jj])
            temp = instruments1
        order_complete = instruments1
    print(order_complete)
    order_sqr_complete_count = db.list_collection_names().count("order_sqr_complete")
    if(order_sqr_complete_count > 0) and (db.order_sqr_complete.count_documents({}) > 0):
        collection = db["order_sqr_complete"]
        x = collection.find()
        temp = pd.DataFrame()
        # order_tobe_cancel = pd.read_csv(file['title'])
        for y in x:
            jj = pd.DataFrame([y])
            instruments1 = pd.concat([temp,jj])
            temp = instruments1
        order_sqr_complete = instruments1
    order_reject_complete_count = db.list_collection_names().count("order_reject_complete")
    if(order_reject_complete_count > 0) and (db.order_reject_complete.count_documents({}) > 0):
        collection = db["order_reject_complete"]
        x = collection.find()
        temp = pd.DataFrame()
        # order_tobe_cancel = pd.read_csv(file['title'])
        for y in x:
            jj = pd.DataFrame([y])
            instruments1 = pd.concat([temp,jj])
            temp = instruments1
        order_reject_complete = instruments1
    order_cancel_complete_count = db.list_collection_names().count("order_cancel_complete")
    if(order_cancel_complete_count > 0) and (db.order_cancel_complete.count_documents({}) > 0):
        collection = db["order_cancel_complete"]
        x = collection.find()
        temp = pd.DataFrame()
        # order_tobe_cancel = pd.read_csv(file['title'])
        for y in x:
            jj = pd.DataFrame([y])
            instruments1 = pd.concat([temp,jj])
            temp = instruments1
        order_cancel_complete = instruments1
    order_pending_complete_count = db.list_collection_names().count("order_pending_complete")
    if(order_pending_complete_count > 0) and (db.order_pending_complete.count_documents({}) > 0):
        collection = db["order_pending_complete"]
        x = collection.find()
        temp = pd.DataFrame()
        # order_tobe_cancel = pd.read_csv(file['title'])
        for y in x:
            jj = pd.DataFrame([y])
            instruments1 = pd.concat([temp,jj])
            temp = instruments1
        order_pending_complete = instruments1
    order_multiple_count = db.list_collection_names().count("order_multiple")
    if(order_multiple_count > 0) and (db.order_multiple.count_documents({}) > 0):
        collection = db["order_multiple"]
        x = collection.find()
        temp = pd.DataFrame()
        # order_tobe_cancel = pd.read_csv(file['title'])
        for y in x:
            jj = pd.DataFrame([y])
            instruments1 = pd.concat([temp,jj])
            temp = instruments1
        order_multiple = instruments1
    order_sqr_multiple_count = db.list_collection_names().count("order_sqr_multiple")
    if(order_sqr_multiple_count > 0) and (db.order_sqr_multiple.count_documents({}) > 0):
        collection = db["order_sqr_multiple"]
        x = collection.find()
        temp = pd.DataFrame()
        # order_tobe_cancel = pd.read_csv(file['title'])
        for y in x:
            jj = pd.DataFrame([y])
            instruments1 = pd.concat([temp,jj])
            temp = instruments1
        order_sqr_multiple = instruments1
    order_reject_multiple_count = db.list_collection_names().count("order_reject_multiple")
    if(order_reject_multiple_count > 0) and (db.order_reject_multiple.count_documents({}) > 0):
        collection = db["order_reject_multiple"]
        x = collection.find()
        temp = pd.DataFrame()
        # order_tobe_cancel = pd.read_csv(file['title'])
        for y in x:
            jj = pd.DataFrame([y])
            instruments1 = pd.concat([temp,jj])
            temp = instruments1
        order_reject_multiple = instruments1
    order_cancel_multiple_count = db.list_collection_names().count("order_cancel_multiple")
    if(order_cancel_multiple_count > 0) and (db.order_cancel_multiple.count_documents({}) > 0):
        collection = db["order_cancel_multiple"]
        x = collection.find()
        temp = pd.DataFrame()
        # order_tobe_cancel = pd.read_csv(file['title'])
        for y in x:
            jj = pd.DataFrame([y])
            instruments1 = pd.concat([temp,jj])
            temp = instruments1
        order_cancel_multiple = instruments1
    order_pending_multiple_count = db.list_collection_names().count("order_pending_multiple")
    if(order_pending_multiple_count > 0) and (db.order_pending_multiple.count_documents({}) > 0):
        collection = db["order_pending_multiple"]
        x = collection.find()
        temp = pd.DataFrame()
        # order_tobe_cancel = pd.read_csv(file['title'])
        for y in x:
            jj = pd.DataFrame([y])
            instruments1 = pd.concat([temp,jj])
            temp = instruments1
        order_pending_multiple = instruments1
    order_tobe_cancel_count = db.list_collection_names().count("order_tobe_cancel")
    if(order_tobe_cancel_count > 0) and (db.order_tobe_cancel.count_documents({}) > 0):
        collection = db["order_tobe_cancel"]
        x = collection.find()
        temp = pd.DataFrame()
        # order_tobe_cancel = pd.read_csv(file['title'])
        for y in x:
            jj = pd.DataFrame([y])
            instruments1 = pd.concat([temp,jj])
            temp = instruments1
        order_tobe_cancel = instruments1
    order_tobe_exec_log_count = db.list_collection_names().count("order_tobe_exec_log")
    if(order_tobe_exec_log_count > 0) and (db.order_tobe_exec_log.count_documents({}) > 0):
        collection = db["order_tobe_exec_log"]
        x = collection.find()
        temp = pd.DataFrame()
        # order_tobe_cancel = pd.read_csv(file['title'])
        for y in x:
            jj = pd.DataFrame([y])
            instruments1 = pd.concat([temp,jj])
            temp = instruments1
        order_tobe_exec_log = instruments1
    order_tobe_sqr_complete_count = db.list_collection_names().count("order_tobe_sqr_complete")
    if(order_tobe_sqr_complete_count > 0) and (db.order_tobe_sqr_complete.count_documents({}) > 0):
        collection = db["order_tobe_sqr_complete"]
        x = collection.find()
        temp = pd.DataFrame()
        for y in x:
            jj = pd.DataFrame([y])
            instruments1 = pd.concat([temp,jj])
            temp = instruments1
        # order_tobe_cancel = pd.read_csv(file['title'])
        order_tobe_sqr_complete = instruments1

        # if(file['title'] =="order_tobe_cancel_complete.txt"):
        #     order_tobe_cancel_complete = pd.read_csv(file['title'])
        # if(file['title'] =="order_tobe_cancel_multiple.txt"):
        #     order_tobe_cancel_multiple = pd.read_csv(file['title'])
        # if(file['title'] =="order_manage.txt"):
        #     order_manage = pd.read_csv(file['title'])
        # if(file['title'] =="order_reject.txt"):
        #     order_reject = pd.read_csv(file['title'])
        # if(file['title'] =="order_cancel.txt"):
        #     order_cancel = pd.read_csv(file['title'])
        # if(file['title'] =="order_pending.txt"):
        #     order_pending = pd.read_csv(file['title'])
        # if(file['title'] =="order_complete.txt"):
        #     order_complete = pd.read_csv(file['title'])
        # if(file['title'] =="order_sqr_complete.txt"):
        #     order_sqr_complete = pd.read_csv(file['title'])
        # if(file['title'] =="order_reject_complete.txt"):
        #     order_reject_complete = pd.read_csv(file['title'])
        # if(file['title'] =="order_cancel_complete.txt"):
        #     order_cancel_complete = pd.read_csv(file['title'])
        # if(file['title'] =="order_pending_complete.txt"):
        #     order_pending_complete = pd.read_csv(file['title'])
        # if(file['title'] =="order_multiple.txt"):
        #     order_multiple = pd.read_csv(file['title'])
        # if(file['title'] =="order_sqr_multiple.txt"):
        #     order_sqr_multiple = pd.read_csv(file['title'])
        # if(file['title'] =="order_reject_multiple.txt"):
        #     order_reject_multiple = pd.read_csv(file['title'])
        # if(file['title'] =="order_cancel_multiple.txt"):
        #     order_cancel_multiple = pd.read_csv(file['title'])
        # if(file['title'] =="order_pending_multiple.txt"):
        #     order_pending_multiple = pd.read_csv(file['title'])
        # if(file['title'] =="order_tobe_cancel.txt"):
        #     order_tobe_cancel = pd.read_csv(file['title'])
        # if(file['title'] =="order_tobe_exec_log.txt"):
        #     order_tobe_exec_log = pd.read_csv(file['title'])
        # if(file['title'] =="order_tobe_sqr_complete.txt"):
        #     order_tobe_sqr_complete = pd.read_csv(file['title'])
    df25 = pd.read_csv('https://api.kite.trade/instruments',parse_dates=True,dayfirst=True)
    tv = TvDatafeed()
   # tv = TvDatafeedLive()
    trade_id = 0
    print(len(order_manage))
    print(order_manage)
    if len(order_manage)<1:
        order_manage =pd.DataFrame(columns=COLUMN_NAMES)
    if len(order_reject)<1:
        order_reject =pd.DataFrame(columns=COLUMN_NAMES)
    if len(order_cancel)<1:
        order_cancel =pd.DataFrame(columns=COLUMN_NAMES)
    if len(order_pending)<1:
        order_pending =pd.DataFrame(columns=COLUMN_NAMES)
    if len(order_complete)<1:
        order_complete =pd.DataFrame(columns=COLUMN_NAMES)
    if len(order_sqr_complete)<1:
        order_sqr_complete =pd.DataFrame(columns=COLUMN_NAMES)
    if len(order_sqr_complete_cons)<1:
        order_sqr_complete_cons =pd.DataFrame(columns=COLUMN_NAMES)
    if len(order_reject_complete)<1:
        order_reject_complete =pd.DataFrame(columns=COLUMN_NAMES)
    if len(order_cancel_complete)<1:
        order_cancel_complete =pd.DataFrame(columns=COLUMN_NAMES)
    if len(order_pending_complete)<1:
        order_pending_complete =pd.DataFrame(columns=COLUMN_NAMES)
    if len(order_multiple)<1:
        order_multiple =pd.DataFrame(columns=COLUMN_NAMES)
    if len(order_sqr_multiple)<1:
        order_sqr_multiple =pd.DataFrame(columns=COLUMN_NAMES)
    if len(order_reject_multiple)<1:
        order_reject_multiple =pd.DataFrame(columns=COLUMN_NAMES)
    if len(order_cancel_multiple)<1:
        order_cancel_multiple =pd.DataFrame(columns=COLUMN_NAMES)
    if len(order_pending_multiple)<1:
        order_pending_multiple =pd.DataFrame(columns=COLUMN_NAMES)
    if len(order_tobe_exec_log)<1:
        order_tobe_exec_log =pd.DataFrame(columns=COLUMN_NAMES)
    if len(order_tobe_sqr_complete)<1:
        order_tobe_sqr_complete =pd.DataFrame(columns=COLUMN_NAMES)
    if len(order_tobe_cancel)<1:
        order_tobe_cancel =pd.DataFrame(columns=COLUMN_NAMES)
    if len(order_pending_tobe_cancel)<1:
        order_pending_tobe_cancel =pd.DataFrame(columns=COLUMN_NAMES)
    if len(order_pending_complete_tobe_cancel)<1:
        order_pending_complete_tobe_cancel =pd.DataFrame(columns=COLUMN_NAMES)
    df25['expiry'] = pd.to_datetime(df25.expiry, format='%Y-%m-%d').dt.date
    # df25['expiry'] = df25['expiry'].dt.strftime('%Y-%m-%d')
    df26 = df25[(df25['name'] == instru_name) & (pd.to_datetime(df25.expiry).dt.month == datetime.datetime.now().month) & (df25['expiry'] >= datetime.datetime.now(pytz.timezone('Asia/Kolkata')).date())]
    df26 = df26.sort_values(by=['expiry'])
    if len(df26) > 1:
        exp_1 = df26['expiry'].iloc[0]
            # if len(order_complete.loc[order_complete['leg']== "leg4",'expiry'])>0 and order_complete.loc[order_complete['leg']== "leg4",'expiry'].values[0]:
        exp_2 = df26['expiry'].iloc[-1]
    else:
        df26 = df25[(df25['name'] == instru_name) & ((pd.to_datetime(df25.expiry).dt.month) == (datetime.datetime.now().month+1)) & (df25['expiry'] >= datetime.datetime.now(pytz.timezone('Asia/Kolkata')).date())]
        df26 = df26.sort_values(by=['expiry'])
        exp_1 = df26['expiry'].iloc[0]
            # if len(order_complete.loc[order_complete['leg']== "leg4",'expiry'])>0 and order_complete.loc[order_complete['leg']== "leg4",'expiry'].values[0]:
        exp_2 = df26['expiry'].iloc[-1]

    while True:
        temp = pd.DataFrame()
        current_time = datetime.datetime.now(pytz.timezone('Asia/Kolkata'))
        current_time1 = datetime.datetime.now(pytz.timezone('Asia/Kolkata')).time()
        today = datetime.datetime.now(pytz.timezone('Asia/Kolkata')).date()
        exp_1 = ""
        exp_2 = ""
        kk = ""
        nn = ""
        ss = ""
        tt = ""
        dd = ""
        s = kite
        # print("lol")
        current_time3 = datetime.datetime.now(pytz.timezone('Asia/Kolkata'))
        period = pd.to_datetime(current_time3).ceil('5 min')
        next_time = period - current_time3
        # print("sleeping for seconds -"," ",(next_time-timedelta(seconds=1)).total_seconds(), datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
        # if((next_time-timedelta(seconds=1)).total_seconds()>1) and current_time.minute % 5 != 0:
        #     time.sleep((next_time-timedelta(seconds=1)).total_seconds())

#         global order_pending_complete,order_reject_complete,order_complete,order_pending,order_reject,order_manage,order_pending_multiple,order_reject_multiple,order_multiple,order_tobe_exec_log
        
        if current_time.second == 0 and current_time.minute % 5 == 0:
        # if True:
#             print(df79, file=sys.stderr) 
            print("vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv",datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
            # while counter<10:
            #     try:
            #         file_list = drive.ListFile({'q' : f"'{folder}' in parents and trashed=false"}).GetList()
            #         counter=10
            #     except Exception as e:
            #         logger.error(e)
            #         counter=counter+1

            # for index, file in enumerate(file_list):
            #     counter=0
            #     while counter<10:
            #         try:
            #             df29 = file.GetContentFile(file['title'])
            #             counter=10
            #         except Exception as e:
            #             logger.error(e)
            #             counter=counter+1
            #     if(file['title'] =="order_sqr_complete.txt"):
            #         order_sqr_complete = pd.read_csv(file['title'])
            #     if(file['title'] =="order_reject_complete.txt"):
            #         order_reject_complete = pd.read_csv(file['title'])
            #     if(file['title'] =="order_cancel_complete.txt"):
            #         order_cancel_complete = pd.read_csv(file['title'])
            #     if(file['title'] =="order_pending_complete.txt"):
            #         order_pending_complete = pd.read_csv(file['title'])
            #     if(file['title'] =="order_multiple.txt"):
            #         order_multiple = pd.read_csv(file['title'])
            #     if(file['title'] =="order_tobe_cancel_complete.txt"):
            #         order_tobe_cancel_complete = pd.read_csv(file['title'])
            #     if(file['title'] =="order_tobe_sqr_complete.txt"):
            #         order_tobe_sqr_complete = pd.read_csv(file['title'])
            #     if(file['title'] =="hh.txt"):
            #         counter=0
            #         while counter<10:
            #             try:
            #                 df29 = file.GetContentFile(file['title'])
            #                 counter=10
            #             except Exception as e:
            #                 logger.error(e)
            #                 counter=counter+1
            #         df2 = pd.read_csv(file['title'])
            order_tobe_cancel_complete_count = db.order_tobe_cancel_complete.count_documents({})
            if(order_tobe_cancel_complete_count > 0):
                collection = db["order_tobe_cancel_complete"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_tobe_cancel_complete = instruments1
            # order_manage_count = db.list_collection_names().count("order_manage")
            order_sqr_complete_count = db.order_sqr_complete.count_documents({})
            if(order_sqr_complete_count > 0):
                collection = db["order_sqr_complete"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                # order_tobe_cancel = pd.read_csv(file['title'])
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                order_sqr_complete = instruments1
            order_reject_complete_count = db.order_reject_complete.count_documents({})
            if(order_reject_complete_count > 0):
                collection = db["order_reject_complete"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                # order_tobe_cancel = pd.read_csv(file['title'])
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                order_reject_complete = instruments1
            order_cancel_complete_count = db.order_cancel_complete.count_documents({})
            if(order_cancel_complete_count > 0):
                collection = db["order_cancel_complete"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                # order_tobe_cancel = pd.read_csv(file['title'])
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                order_cancel_complete = instruments1
            order_pending_complete_count = db.order_pending_complete.count_documents({})
            if(order_pending_complete_count > 0):
                collection = db["order_pending_complete"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_pending_complete = instruments1
            order_multiple_count = db.order_multiple.count_documents({})
            if(order_multiple_count > 0):
                collection = db["order_multiple"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_multiple = instruments1
            order_tobe_sqr_complete_count = db.order_tobe_sqr_complete.count_documents({})
            if(order_tobe_sqr_complete_count > 0):
                collection = db["order_tobe_sqr_complete"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_tobe_sqr_complete = instruments1
            hh_count = db.hh.count_documents({})
            if(hh_count > 0):
                collection = db["hh"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                hh = instruments1
    
#print("Check for hh.txt file"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
#currently_holding = False
            df2 = hh
            currently_buy_holding = df2['currently_buy_holding'].values[0]
            currently_sell_holding = df2['currently_sell_holding'].values[0]
            print("order_complete is printed every 5min", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
            print(order_complete, datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)

            if len(order_pending_complete) > 0:
                print("order_pending_complete being checked"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                for kl in range(0,len(order_pending_complete)):
                    status = check_order_history(s,order_pending_complete['sqr_order_id'][kl],order_pending_complete['instrument_id'][kl],order_pending_complete['fresh_position'][kl],order_pending_complete['status'][kl])
#                     if status == "open":
#                         order_complete.loc[len(order_complete)] = order_pending_complete.iloc[kl]
#                         order_pending_complete = order_pending_complete.drop(kl)
                    if status == "close":
                        print("order_pending_complete-status-close"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                        order_sqr_complete.loc[len(order_sqr_complete)] = order_pending_complete.iloc[kl]
                        order_pending_complete = order_pending_complete.drop(kl)        
                    if status == "rejected":
                        print("order_pending_complete-status-rejected"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                        order_reject_complete.loc[len(order_reject_complete)] = order_pending_complete.iloc[kl]
                        order_pending_complete.at[kl,"status"] = "open "
                        order_tobe_sqr_complete.loc[len(order_tobe_sqr_complete)] = order_tobe_sqr_complete.iloc[kl]
                        order_pending_complete = order_pending_complete.drop(kl)        
                    if status == "cancelled":
                        print("order_pending_complete-status-cancelled"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                        order_cancel_complete.loc[len(order_cancel_complete)] = order_pending_complete.iloc[kl]
                        order_pending_complete.at[kl,"status"] = "open"
                        order_tobe_sqr_complete.loc[len(order_tobe_sqr_complete)] = order_pending_complete.iloc[kl]
                        order_pending_complete = order_pending_complete.drop(kl)        
                    if status == "pending":
                        print("order_pending_complete-status-pending"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                        status4 = ""
                        for kkl in range(0,len(order_sqr_complete)):
                            if (order_sqr_complete['instrument_id'][kkl] == order_pending_complete['instrument_id'][kl]):
                                print("order_pending_complete-trying to cancel order - as order squared off already"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                                status4 = order_cancel_place(s,order_pending_complete['sqr_order_id'][kl],"pending")
                                if status4 == "cancelled":
                                    print("order_pending_complete-order-cancelled"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                                    order_cancel_complete.loc[len(order_cancel_complete)] = order_pending_complete.iloc[kl]
                                    order_pending_complete = order_pending_complete.drop(kl)
                        if not status4:
                            retry_order = 0
                            print("order_pending_complete-modify order"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                            order_leg,price,modify_time = order_modify_complete(s,file_list,drive,order_pending_complete['current_signal'][kl],order_pending_complete['instrument_id'][kl],order_pending_complete['trading_symbol'][kl],order_pending_complete['price_when_order_placed'][kl],order_pending_complete['qty'][kl],order_pending_complete['instru'][kl],order_pending_complete['fresh_position'][kl],"leg1",order_pending_complete['strike'][kl],order_pending_complete['contract'][kl],order_pending_complete['expiry'][kl],order_pending_complete['trade_id'][kl],0,order_pending_complete['buy_sell'][kl],retry_order,order_pending_complete['status'][kl],order_pending_complete['sqr_order_id'][kl])
                            if order_leg == "modified":
                                print("order_pending_complete-order modified"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                                status1 = check_order_history(s,order_pending_complete['sqr_order_id'][kl],order_pending_complete['instrument_id'][kl],order_pending_complete['fresh_position'][kl],order_pending_complete['status'][kl])
                                if status1 == "close":
                                    print("order_pending_complete-modified order executed"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                                    order_pending_complete.at[kl,"status"] = "close"
                                    order_pending_complete.at[kl,"exit_time"] = modify_time
                                    order_pending_complete.at[kl,"exit_price"] = price
                                    order_sqr_complete.loc[len(order_sqr_complete)] = order_pending_complete.iloc[kl]
                                    order_pending_complete = order_pending_complete.drop(kl)
#                     order_pending_complete = order_pending_complete.drop(kl)
                # kk = 0
                # ll = 0
                # mm = 0
                # nn = 0
                # ss = 0
                # tt = 0
                # dd = 0
                # mk = 0
                # # file_list = drive.ListFile({'q' : f"'{folder}' in parents and trashed=false"}).GetList()
                # for index, file in enumerate(file_list):
                #     if(file['title'] =="order_pending_complete.txt"):
                #         kk = file['id']
                #     if(file['title'] =="order_cancel_complete.txt"):
                #         ll = file['id']
                #     if(file['title'] =="order_multiple.txt"):
                #         mm = file['id']
                #     if(file['title'] =="order_reject_complete.txt"):
                #         nn = file['id']
                #     if(file['title'] =="order_tobe_sqr_complete.txt"):
                #         ss = file['id']
                #     if(file['title'] =="order_pending_complete.txt"):
                #         tt = file['id']
                #     if(file['title'] =="order_sqr_complete.txt"):
                #         dd = file['id']
                # if bool(kk):
                #     order_pending_complete.to_csv("order_pending_complete.txt", index=False)
                #     update_file = drive.CreateFile({'id': kk})
                #     update_file.SetContentFile("order_pending_complete.txt")
                #     update_file.Upload()
                # if bool(ll):
                #     order_cancel_complete.to_csv("order_cancel_complete.txt", index=False)
                #     update_file = drive.CreateFile({'id': ll})
                #     update_file.SetContentFile("order_cancel_complete.txt")
                #     update_file.Upload()
                # else:
                #     order_cancel_complete.to_csv("order_cancel_complete.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_cancel_complete.txt"})
                #     gfile.SetContentFile("order_cancel_complete.txt")
                #     gfile.Upload()
                # if bool(mm):
                #     order_multiple.to_csv("order_multiple.txt", index=False)
                #     update_file = drive.CreateFile({'id': mm})
                #     update_file.SetContentFile("order_multiple.txt")
                #     update_file.Upload()
                # else:
                #     order_multiple.to_csv("order_multiple.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_multiple.txt"})
                #     gfile.SetContentFile("order_multiple.txt")
                #     gfile.Upload()
                # if bool(nn):
                #     order_reject_complete.to_csv("order_reject_complete.txt", index=False)
                #     update_file = drive.CreateFile({'id': nn})
                #     update_file.SetContentFile("order_reject_complete.txt")
                #     update_file.Upload()
                # else:
                #     order_reject_complete.to_csv("order_reject_complete.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_reject_complete.txt"})
                #     gfile.SetContentFile("order_reject_complete.txt")
                #     gfile.Upload()
                # if bool(ss):
                #     order_tobe_sqr_complete.to_csv("order_tobe_sqr_complete.txt", index=False)
                #     update_file = drive.CreateFile({'id': ss})
                #     update_file.SetContentFile("order_tobe_sqr_complete.txt")
                #     update_file.Upload()
                # else:
                #     order_tobe_sqr_complete.to_csv("order_tobe_sqr_complete.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_tobe_sqr_complete.txt"})
                #     gfile.SetContentFile("order_tobe_sqr_complete.txt")
                #     gfile.Upload()
                # if bool(tt):
                #     order_pending_complete.to_csv("order_pending_complete.txt", index=False)
                #     update_file = drive.CreateFile({'id': tt})
                #     update_file.SetContentFile("order_pending_complete.txt")
                #     update_file.Upload()
                # else:
                #     order_pending_complete.to_csv("order_pending_complete.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_pending_complete.txt"})
                #     gfile.SetContentFile("order_pending_complete.txt")
                #     gfile.Upload()
                # if bool(dd):
                #     order_sqr_complete.to_csv("order_sqr_complete.txt", index=False)
                #     update_file = drive.CreateFile({'id': dd})
                #     update_file.SetContentFile("order_sqr_complete.txt")
                #     update_file.Upload()
                # else:
                #     order_sqr_complete.to_csv("order_sqr_complete.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_sqr_complete.txt"})
                #     gfile.SetContentFile("order_sqr_complete.txt")
                #     gfile.Upload()
            # file_list = drive.ListFile({'q' : f"'{folder}' in parents and trashed=false"}).GetList()
                order_sqr_complete_count = db.list_collection_names().count("order_sqr_complete")
                if(order_sqr_complete_count < 1):
                    db.create_collection("order_sqr_complete")
                else:
                    db.order_sqr_complete.delete_many({})
                collection = db["order_sqr_complete"]
                dict1 = order_sqr_complete.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])
                order_pending_complete_count = db.list_collection_names().count("order_pending_complete")
                if(order_pending_complete_count < 1):
                    db.create_collection("order_pending_complete")
                else:
                    db.order_pending_complete.delete_many({})
                collection = db["order_pending_complete"]
                dict1 = order_pending_complete.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])
                order_cancel_complete_count = db.list_collection_names().count("order_cancel_complete")
                if(order_cancel_complete_count < 1):
                    db.create_collection("order_cancel_complete")
                else:
                    db.order_cancel_complete.delete_many({})
                collection = db["order_cancel_complete"]
                dict1 = order_cancel_complete.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])
                order_multiple_count = db.list_collection_names().count("order_multiple")
                if(order_multiple_count < 1):
                    db.create_collection("order_multiple")
                else:
                    db.order_multiple.delete_many({})
                collection = db["order_multiple"]
                dict1 = order_multiple.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])
                order_reject_complete_count = db.list_collection_names().count("order_reject_complete")
                if(order_reject_complete_count < 1):
                    db.create_collection("order_reject_complete")
                else:
                    db.order_reject_complete.delete_many({})
                collection = db["order_reject_complete"]
                dict1 = order_reject_complete.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])
                order_tobe_sqr_complete_count = db.list_collection_names().count("order_tobe_sqr_complete")
                if(order_tobe_sqr_complete_count < 1):
                    db.create_collection("order_tobe_sqr_complete")
                else:
                    db.order_tobe_sqr_complete.delete_many({})
                collection = db["order_tobe_sqr_complete"]
                dict1 = order_tobe_sqr_complete.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])

            # for index, file in enumerate(file_list):
            #     counter=0
            #     while counter<10:
            #         try:
            #             df29 = file.GetContentFile(file['title'])
            #             counter=10
            #         except Exception as e:
            #             logger.error(e)
            #             counter=counter+1
            #     if(file['title'] =="order_sqr_complete.txt"):
            #         order_sqr_complete = pd.read_csv(file['title'])
            #     if(file['title'] =="order_reject_complete.txt"):
            #         order_reject_complete = pd.read_csv(file['title'])
            #     if(file['title'] =="order_cancel_complete.txt"):
            #         order_cancel_complete = pd.read_csv(file['title'])
            #     if(file['title'] =="order_pending_complete.txt"):
            #         order_pending_complete = pd.read_csv(file['title'])
            #     if(file['title'] =="order_multiple.txt"):
            #         order_multiple = pd.read_csv(file['title'])
            #     if(file['title'] =="order_tobe_cancel_complete.txt"):
            #         order_tobe_cancel_complete = pd.read_csv(file['title'])
            #     if(file['title'] =="order_tobe_sqr_complete.txt"):
            #         order_tobe_sqr_complete = pd.read_csv(file['title'])
            order_tobe_cancel_complete_count = db.order_tobe_cancel_complete.count_documents({})
            if(order_tobe_cancel_complete_count > 0):
                collection = db["order_tobe_cancel_complete"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_tobe_cancel_complete = instruments1
            order_sqr_complete_count = db.order_sqr_complete.count_documents({})
            if(order_sqr_complete_count > 0):
                collection = db["order_sqr_complete"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_sqr_complete = instruments1
            order_reject_complete_count = db.order_reject_complete.count_documents({})
            if(order_reject_complete_count > 0):
                collection = db["order_reject_complete"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_reject_complete = instruments1
            order_cancel_complete_count = db.order_cancel_complete.count_documents({})
            if(order_cancel_complete_count > 0):
                collection = db["order_cancel_complete"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_cancel_complete = instruments1
            order_pending_complete_count = db.order_pending_complete.count_documents({})
            if(order_pending_complete_count > 0):
                collection = db["order_pending_complete"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_pending_complete = instruments1
            order_multiple_count = db.order_multiple.count_documents({})
            if(order_multiple_count > 0):
                collection = db["order_multiple"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_multiple = instruments1
            order_tobe_sqr_complete_count = db.order_tobe_sqr_complete.count_documents({})
            if(order_tobe_sqr_complete_count > 0):
                collection = db["order_tobe_sqr_complete"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_tobe_sqr_complete = instruments1

            if len(order_reject_complete) > 0:
                print("order_reject_complete being checked"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                del_rej = 0
                kl_del = []
                kl1_del = []
                for kl in range(0,len(order_reject_complete)):
                    status = check_order_history(s,order_reject_complete['sqr_order_id'][kl],order_reject_complete['instrument_id'][kl],order_reject_complete['fresh_position'][kl],order_reject_complete['status'][kl])
                    if status == "close":
                        found_complete = 0
                        for kkl in range(0,len(order_sqr_complete)):
                            if (order_sqr_complete['instrument_id'][kkl] == order_reject_complete['instrument_id'][kl]):
#                                 status1 = check_order_history(order_complete['sqr_order_id'][kkl],order_manage['instrument_id'][kkl],order_manage['fresh_position'][kkl],order_manage['status'][kkl])
#                                 rw = kkl
#                                 if status == "open" and status1 == "open":
                                found_complete = 1
                                order_reject_complete.at[kl,"status"] = "close"
                                order_multiple.loc[len(order_multiple)] = order_reject_complete.iloc[kl]
#                                 order_reject_complete = order_reject_complete.drop(kl)
                            if found_complete == 0:
                                order_reject_complete.at[kl,"status"] = "close"
                                order_sqr_complete.loc[len(order_sqr_complete)] = order_reject_complete.iloc[kl]
#                                 order_reject_complete = order_reject_complete.drop(kl)
                                if (len(order_tobe_sqr_complete)>0):
                                    order_tobe_sqr_complete = order_tobe_sqr_complete.drop(order_tobe_sqr_complete.index[(order_tobe_sqr_complete.instrument_id==order_reject_complete['instrument_id'][kl])].values[0])
                        for kkl in range(0,len(order_pending_complete)):
                            if (order_pending_complete['instrument_id'][kkl] == order_reject_complete['instrument_id'][kl]):
                                if found_complete < 1:
                                    # order_sqr_complete.loc[len(order_sqr_complete)] = order_reject_complete.iloc[kl]
                                    status2 = check_order_history(s,order_pending_complete['sqr_order_id'][kl],order_pending_complete['instrument_id'][kl],order_pending_complete['fresh_position'][kl],order_pending_complete['status'][kl])
                                    if status2 == "close":
                                        order_multiple.loc[len(order_multiple)] = order_pending_complete.iloc[kl]
                                        # order_sqr_complete.loc[len(order_sqr_complete)] = order_pending_complete.iloc[kkl]
                                    if status2 == "pending":
                                        status22 = order_cancel_place(s,order_pending_complete['sqr_order_id'][kkl],"pending")
                                        if status22 == "cancelled":
                                            order_pending_complete.at[kkl,"status"] = "cancelled"
                                            order_cancel_complete.loc[len(order_cancel_complete)] = order_pending_complete.iloc[kkl]
                                        else:
                                            order_tobe_cancel_complete.loc[len(order_tobe_cancel_complete)] = order_pending_complete.iloc[kkl]
    #                                     status2 = order_cancel(order_pending['sqr_order_id'][kl],"pending")
                                    if status2 == "cancelled":
                                        order_pending_complete.at[kkl,"status"] = "cancelled"
                                        order_cancel_complete.loc[len(order_cancel_complete)] = order_pending_complete.iloc[kkl]
#                                         order_pending_complete = order_pending_complete.drop(kkl)
                                        order_sqr_complete.loc[len(order_sqr_complete)] = order_reject_complete.iloc[kl]
                                    if status2 == "rejected":
                                        order_pending_complete.at[kkl,"status"] = "rejected"
                                        order_reject_complete.loc[len(order_reject_complete)] = order_pending_complete.iloc[kkl]
                                    kl1_del.append(kkl)
#                                         order_pending_complete = order_pending_complete.drop(kkl)
                                        # order_sqr_complete.loc[len(order_sqr_complete)] = order_reject_complete.iloc[kl]
                            # order_pending_complete = order_pending_complete.drop(kkl)
                        del_rej = 1
                        kl_del.append(kl)
                        continue
                    if status == "pending":
                        # for kkl in range(0,len(order_sqr_complete)):
                        found_complete = 0
                        for kkl in range(0,len(order_pending_complete)):
                            found_pending = 0
                            if (order_pending_complete['instrument_id'][kkl] == order_reject_complete['instrument_id'][kl]):
                                found_pending = 1
                                index_pending = kkl
                                status22 = kj.order_cancel_place(s,order_reject_complete['order_id'][kl],"pending")
                                if status22 == "cancelled":
                                    order_reject_complete.at[kl,"status"] = "cancelled"
                                    order_cancel_complete.loc[len(order_cancel_complete)] = order_reject_complete.iloc[kl]
                                else:
                                    order_tobe_cancel_complete.loc[len(order_tobe_cancel_complete)] = order_reject_complete.iloc[kl]
                        for kkl in range(0,len(order_sqr_complete)):
                            drop = 0
                            if (order_sqr_complete['instrument_id'][kkl] == order_reject_complete['instrument_id'][kl]):
                                if found_pending < 1:
                                    order_reject_complete.at[kl,"status"] = "pending"
                                    order_pending_complete.loc[len(order_pending_complete)] = order_reject_complete.iloc[kl]
                                    found_complete = 1
                                if found_pending > 1:
                                    status22 = kj.order_cancel_complete_place(s,order_pending_complete['order_id'][index_pending],"pending")
                                    if status22 == "cancelled":
                                        order_pending_complete.at[kl,"status"] = "cancelled"
                                        order_cancel_complete.loc[len(order_cancel_complete)] = order_pending_complete.iloc[index_pending]
                                    else:
                                        order_tobe_cancel_complete.loc[len(order_tobe_cancel_complete)] = order_pending_complete.iloc[index_pending]
                        if found_complete == 0 and found_pending < 1:
                            order_reject_complete.at[kl,"status"] = "pending"
                            order_pending_complete.loc[len(order_pending_complete)] = order_reject_complete.iloc[kl]
                            if (len(order_tobe_sqr_complete)>0):
                                order_tobe_sqr_complete = order_tobe_sqr_complete.drop(order_tobe_sqr_complete.index[(order_tobe_sqr_complete.instrument_id==order_reject_complete['instrument_id'][kl])].values[0])
                        kl_del.append(kl)
                        continue
                        # del_rej = 1
                    if status == "cancelled":
                        order_reject_complete.at[kl,"status"] = "cancelled"
                        order_cancel_complete.loc[len(order_cancel_complete)] = order_reject_complete.iloc[kkl]
                        kl_del.append(kl)
                        continue
                order_reject_complete = order_reject_complete.drop(order_reject_complete.index[kl_del])
                order_pending_complete = order_pending_complete.drop(order_pending_complete.index[kl1_del])
                # file_list = drive.ListFile({'q' : f"'{folder}' in parents and trashed=false"}).GetList()
                # kk = 0
                # ll = 0
                # mm = 0
                # nn = 0
                # ss = 0
                # tt = 0
                # dd = 0
                # mk = 0
                # for index, file in enumerate(file_list):
                #     if(file['title'] =="order_pending_complete.txt"):
                #         kk = file['id']
                #     if(file['title'] =="order_cancel_complete.txt"):
                #         ll = file['id']
                #     if(file['title'] =="order_multiple.txt"):
                #         mm = file['id']
                #     if(file['title'] =="order_reject_complete.txt"):
                #         nn = file['id']
                #     if(file['title'] =="order_tobe_sqr_complete.txt"):
                #         ss = file['id']
                #     if(file['title'] =="order_pending_complete.txt"):
                #         tt = file['id']
                #     if(file['title'] =="order_sqr_complete.txt"):
                #         dd = file['id']
                #     if(file['title'] =="order_tobe_cancel_complete.txt"):
                #         mk = file['id']
                # if bool(kk):
                #     order_pending_complete.to_csv("order_pending_complete.txt", index=False)
                #     update_file = drive.CreateFile({'id': kk})
                #     update_file.SetContentFile("order_pending_complete.txt")
                #     update_file.Upload()
                # if bool(ll):
                #     order_cancel_complete.to_csv("order_cancel_complete.txt", index=False)
                #     update_file = drive.CreateFile({'id': ll})
                #     update_file.SetContentFile("order_cancel_complete.txt")
                #     update_file.Upload()
                # else:
                #     order_cancel_complete.to_csv("order_cancel_complete.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_cancel_complete.txt"})
                #     gfile.SetContentFile("order_cancel_complete.txt")
                #     gfile.Upload()
                # if bool(mm):
                #     order_multiple.to_csv("order_multiple.txt", index=False)
                #     update_file = drive.CreateFile({'id': mm})
                #     update_file.SetContentFile("order_multiple.txt")
                #     update_file.Upload()
                # else:
                #     order_multiple.to_csv("order_multiple.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_multiple.txt"})
                #     gfile.SetContentFile("order_multiple.txt")
                #     gfile.Upload()
                # if bool(nn):
                #     order_reject_complete.to_csv("order_reject_complete.txt", index=False)
                #     update_file = drive.CreateFile({'id': nn})
                #     update_file.SetContentFile("order_reject_complete.txt")
                #     update_file.Upload()
                # else:
                #     order_reject_complete.to_csv("order_reject_complete.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_reject_complete.txt"})
                #     gfile.SetContentFile("order_reject_complete.txt")
                #     gfile.Upload()
                # if bool(ss):
                #     order_tobe_sqr_complete.to_csv("order_tobe_sqr_complete.txt", index=False)
                #     update_file = drive.CreateFile({'id': ss})
                #     update_file.SetContentFile("order_tobe_sqr_complete.txt")
                #     update_file.Upload()
                # else:
                #     order_tobe_sqr_complete.to_csv("order_tobe_sqr_complete.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_tobe_sqr_complete.txt"})
                #     gfile.SetContentFile("order_tobe_sqr_complete.txt")
                #     gfile.Upload()
                # if bool(tt):
                #     order_pending_complete.to_csv("order_pending_complete.txt", index=False)
                #     update_file = drive.CreateFile({'id': tt})
                #     update_file.SetContentFile("order_pending_complete.txt")
                #     update_file.Upload()
                # else:
                #     order_pending_complete.to_csv("order_pending_complete.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_pending_complete.txt"})
                #     gfile.SetContentFile("order_pending_complete.txt")
                #     gfile.Upload()
                # if bool(dd):
                #     order_sqr_complete.to_csv("order_sqr_complete.txt", index=False)
                #     update_file = drive.CreateFile({'id': dd})
                #     update_file.SetContentFile("order_sqr_complete.txt")
                #     update_file.Upload()
                # else:
                #     order_sqr_complete.to_csv("order_sqr_complete.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_sqr_complete.txt"})
                #     gfile.SetContentFile("order_sqr_complete.txt")
                #     gfile.Upload()
                # if bool(mk):
                #     order_tobe_cancel_complete.to_csv("order_tobe_cancel_complete.txt", index=False)
                #     update_file = drive.CreateFile({'id': mk})
                #     update_file.SetContentFile("order_tobe_cancel_complete.txt")
                #     update_file.Upload()
                # else:
                #     order_tobe_cancel_complete.to_csv("order_tobe_cancel_complete.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_tobe_cancel_complete.txt"})
                #     gfile.SetContentFile("order_tobe_cancel_complete.txt")
                #     gfile.Upload()
            # file_list = drive.ListFile({'q' : f"'{folder}' in parents and trashed=false"}).GetList()
                order_sqr_complete_count = db.list_collection_names().count("order_sqr_complete")
                if(order_sqr_complete_count < 1):
                    db.create_collection("order_sqr_complete")
                else:
                    db.order_sqr_complete.delete_many({})
                collection = db["order_sqr_complete"]
                dict1 = order_sqr_complete.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])
                order_pending_complete_count = db.list_collection_names().count("order_pending_complete")
                if(order_pending_complete_count < 1):
                    db.create_collection("order_pending_complete")
                else:
                    db.order_pending_complete.delete_many({})
                collection = db["order_pending_complete"]
                dict1 = order_pending_complete.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])
                order_cancel_complete_count = db.list_collection_names().count("order_cancel_complete")
                if(order_cancel_complete_count < 1):
                    db.create_collection("order_cancel_complete")
                else:
                    db.order_cancel_complete.delete_many({})
                collection = db["order_cancel_complete"]
                dict1 = order_cancel_complete.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])
                order_multiple_count = db.list_collection_names().count("order_multiple")
                if(order_multiple_count < 1):
                    db.create_collection("order_multiple")
                else:
                    db.order_multiple.delete_many({})
                collection = db["order_multiple"]
                dict1 = order_multiple.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])
                order_reject_complete_count = db.list_collection_names().count("order_reject_complete")
                if(order_reject_complete_count < 1):
                    db.create_collection("order_reject_complete")
                else:
                    db.order_reject_complete.delete_many({})
                collection = db["order_reject_complete"]
                dict1 = order_reject_complete.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])
                order_tobe_sqr_complete_count = db.list_collection_names().count("order_tobe_sqr_complete")
                if(order_tobe_sqr_complete_count < 1):
                    db.create_collection("order_tobe_sqr_complete")
                else:
                    db.order_tobe_sqr_complete.delete_many({})
                collection = db["order_tobe_sqr_complete"]
                dict1 = order_tobe_sqr_complete.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])
                order_tobe_cancel_complete_count = db.list_collection_names().count("order_tobe_cancel_complete")
                if(order_tobe_cancel_complete_count < 1):
                    db.create_collection("order_tobe_cancel_complete")
                else:
                    db.order_tobe_cancel_complete.delete_many({})
                collection = db["order_tobe_cancel_complete"]
                dict1 = order_tobe_cancel_complete.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])

            # counter=0
            # while counter<10:
            #     try:
            #         file_list = drive.ListFile({'q' : f"'{folder}' in parents and trashed=false"}).GetList()
            #         counter=10
            #     except Exception as e:
            #         logger.error(e)
            #         counter=counter+1

            # for index, file in enumerate(file_list):
            #     #print(index+1, 'file downloaded : ', file['title'])
            #     counter=0
            #     while counter<10:
            #         try:
            #             df29 = file.GetContentFile(file['title'])
            #             counter=10
            #         except Exception as e:
            #             logger.error(e)
            #             counter=counter+1
            #     if(file['title'] =="order_manage.txt"):
            #         order_manage = pd.read_csv(file['title'])
            #     if(file['title'] =="order_reject.txt"):
            #         order_reject = pd.read_csv(file['title'])
            #     if(file['title'] =="order_cancel.txt"):
            #         order_cancel = pd.read_csv(file['title'])
            #     if(file['title'] =="order_pending.txt"):
            #         order_pending = pd.read_csv(file['title'])
            #     if(file['title'] =="order_complete.txt"):
            #         order_complete = pd.read_csv(file['title'])
            #     if(file['title'] =="order_tobe_cancel.txt"):
            #         order_tobe_cancel = pd.read_csv(file['title'])
            order_manage_count = db.order_manage.count_documents({}) 
            if(order_manage_count > 0):
                collection = db["order_manage"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_manage = instruments1
            order_reject_count = db.order_reject.count_documents({})
            if(order_reject_count > 0):
                collection = db["order_reject"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_reject = instruments1
            order_cancel_count = db.order_cancel.count_documents({})
            if(order_cancel_count > 0):
                collection = db["order_cancel"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_cancel = instruments1
            order_pending_count = db.order_pending.count_documents({})
            if(order_pending_count > 0):
                collection = db["order_pending"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_pending = instruments1
            order_complete_count = db.order_complete.count_documents({})
            if(order_complete_count > 0):
                collection = db["order_complete"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_complete = instruments1
            order_tobe_cancel_count = db.order_tobe_cancel.count_documents({})
            if(order_tobe_cancel_count > 0):
                collection = db["order_tobe_cancel"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_tobe_cancel = instruments1

            print("order_complete  before order_pending being checked", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
            print(order_complete, datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)

            if len(order_pending) > 0:
                print("order_pending being checked"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                kl_del = []
                for kl in range(0,len(order_pending)):
                    status = check_order_history(s,order_pending['order_id'][kl],order_pending['instrument_id'][kl],order_pending['fresh_position'][kl],order_pending['status'][kl])
                    if status == "open":
                        order_pending.at[kl,"status"] = "open"
                        order_complete.loc[len(order_complete)] = order_pending.iloc[kl]
                        kl_del.append(kl)
                        # order_pending = order_pending.drop(kl)
                        continue
                    if status == "close":
                        order_pending.at[kl,"status"] = "close"
                        order_complete.loc[len(order_complete)] = order_pending.iloc[kl]
                        kl_del.append(kl)
                        # order_pending = order_pending.drop(kl)     
                        continue   
                    if status == "rejected":
                        order_pending.at[kl,"status"] = "rejected"
                        order_reject.loc[len(order_reject)] = order_pending.iloc[kl]
                        order_pending.at[kl,"status"] = ""
                        order_manage.loc[len(order_manage)] = order_pending.iloc[kl]
                        kl_del.append(kl)
                        # order_pending = order_pending.drop(kl)   
                        continue     
                    if status == "cancelled":
                        order_pending.at[kl,"status"] = "cancelled"
                        order_cancel.loc[len(order_cancel)] = order_pending.iloc[kl]
                        order_pending.at[kl,"status"] = ""
                        order_manage.loc[len(order_manage)] = order_pending.iloc[kl]
                        kl_del.append(kl)
                        # order_pending = order_pending.drop(kl)        
                        continue
                    if status == "pending":
                        status4 = ""
                        for kkl in range(0,len(order_complete)):
                            if (order_complete['instrument_id'][kkl] == order_pending['instrument_id'][kl]):
                                status4 = order_cancel_place(s,order_pending['order_id'][kl],"pending")
                                if status4 == "cancelled":
                                    order_pending.at[kl,"status"] = "cancelled"
                                    order_cancel.loc[len(order_cancel)] = order_pending.iloc[kl]
                                    kl_del.append(kl)
                                    # order_pending = order_pending.drop(kl)
                                else:
                                    order_tobe_cancel.loc[len(order_tobe_cancel)] = order_pending.iloc[kkl]
                        if not status4:
                            retry_order = 0
                            print(order_pending['order_id'][kl],"llllllllllllllll")
                            order_leg,price,modify_time = order_modify(s,file_list,drive,order_pending['current_signal'][kl],order_pending['instrument_id'][kl],order_pending['trading_symbol'][kl],order_pending['price_when_order_placed'][kl],order_pending['qty'][kl],order_pending['instru'][kl],order_pending['fresh_position'][kl],"leg1",order_pending['strike'][kl],order_pending['contract'][kl],order_pending['expiry'][kl],order_pending['trade_id'][kl],0,order_pending['buy_sell'][kl],retry_order,order_pending['status'][kl],order_pending['order_id'][kl])
                            if order_leg == "modified":
                                status1 = check_order_history(s,order_pending['order_id'][kl],order_pending['instrument_id'][kl],order_pending['fresh_position'][kl],order_pending['status'][kl])
                                if status1 == "open":
                                    print(order_pending)
                                    order_pending.at[kl,"status"] = "open"
                                    # order_pending.set_value(kl, 'status', "open")
                                    print(order_pending)
                                    order_pending.at[kl,"entry_time"] = modify_time
                                    order_pending.at[kl,"entry_price"] = price
                                    print(order_pending)
                                    print(order_complete)
                                    order_complete.loc[len(order_complete)] = order_pending.iloc[kl]
                                    print(order_complete)
                                    # order_pending = order_pending.drop(kl)
                                    kl_del.append(kl)
                                    continue
                                if status1 == "close":
                                    order_pending.at[kl,"status"] = "close"
                                    order_pending.at[kl,"exit_time"] = modify_time
                                    order_pending.at[kl,"exit_price"] = price
                                    order_complete.loc[len(order_complete)] = order_pending.iloc[kl]
                                    # order_pending = order_pending.drop(kl)
                                    kl_del.append(kl)
                print("order_pending  before index deletion", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                print(order_pending, datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                order_pending = order_pending.drop(order_pending.index[kl_del])
                print("order_pending  after index deletion", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                print(order_pending, datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                # file_list = drive.ListFile({'q' : f"'{folder}' in parents and trashed=false"}).GetList()
                print("order_complete  after order_pending being checked", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                print(order_complete, datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)

                # kk = 0
                # mm= 0
                # nn= 0
                # ss = 0
                # tt = 0
                # dd = 0
                # mk = 0
                # ll = 0
                # km = 0
                # for index, file in enumerate(file_list):
                #     if(file['title'] =="order_manage.txt"):
                #         kk = file['id']
                #     if(file['title'] =="order_cancel.txt"):
                #         ll = file['id']
                #     if(file['title'] =="order_reject.txt"):
                #         nn = file['id']
                #     if(file['title'] =="order_complete.txt"):
                #         ss = file['id']
                #     if(file['title'] =="order_pending.txt"):
                #         tt = file['id']
                #     if(file['title'] =="order_sqr_complete.txt"):
                #         dd = file['id']
                #     if(file['title'] =="order_sqr_complete.txt"):
                #         mk = file['id']
                #     if(file['title'] =="order_tobe_cancel.txt"):
                #         km = file['id']
                # if bool(kk):
                #     order_manage.to_csv("order_manage.txt", index=False)
                #     update_file = drive.CreateFile({'id': kk})
                #     update_file.SetContentFile("order_manage.txt")
                #     update_file.Upload()
                # if bool(ll):
                #     order_cancel.to_csv("order_cancel.txt", index=False)
                #     update_file = drive.CreateFile({'id': ll})
                #     update_file.SetContentFile("order_cancel.txt")
                #     update_file.Upload()
                # else:
                #     order_cancel.to_csv("order_cancel.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_cancel.txt"})
                #     gfile.SetContentFile("order_cancel.txt")
                #     gfile.Upload()
                # if bool(nn):
                #     order_reject.to_csv("order_reject.txt", index=False)
                #     update_file = drive.CreateFile({'id': nn})
                #     update_file.SetContentFile("order_reject.txt")
                #     update_file.Upload()
                # else:
                #     order_reject.to_csv("order_reject.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_reject.txt"})
                #     gfile.SetContentFile("order_reject.txt")
                #     gfile.Upload()
                # if bool(ss):
                #     order_complete.to_csv("order_complete.txt", index=False)
                #     update_file = drive.CreateFile({'id': ss})
                #     update_file.SetContentFile("order_complete.txt")
                #     update_file.Upload()
                # else:
                #     order_complete.to_csv("order_complete.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_complete.txt"})
                #     gfile.SetContentFile("order_complete.txt")
                #     gfile.Upload()
                # if bool(tt):
                #     order_pending.to_csv("order_pending.txt", index=False)
                #     update_file = drive.CreateFile({'id': tt})
                #     update_file.SetContentFile("order_pending.txt")
                #     update_file.Upload()
                # else:
                #     order_pending.to_csv("order_pending.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_pending.txt"})
                #     gfile.SetContentFile("order_pending.txt")
                #     gfile.Upload()
                # if bool(dd):
                #     order_sqr_complete.to_csv("order_sqr_complete.txt", index=False)
                #     update_file = drive.CreateFile({'id': dd})
                #     update_file.SetContentFile("order_sqr_complete.txt")
                #     update_file.Upload()
                # else:
                #     order_sqr_complete.to_csv("order_sqr_complete.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_sqr_complete.txt"})
                #     gfile.SetContentFile("order_sqr_complete.txt")
                #     gfile.Upload()
                # if bool(km):
                #     order_tobe_cancel.to_csv("order_tobe_cancel.txt", index=False)
                #     update_file = drive.CreateFile({'id': km})
                #     update_file.SetContentFile("order_tobe_cancel.txt")
                #     update_file.Upload()
                # else:
                #     order_tobe_cancel.to_csv("order_tobe_cancel.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_tobe_cancel.txt"})
                #     gfile.SetContentFile("order_tobe_cancel.txt")
                #     gfile.Upload()
                order_manage_count = db.list_collection_names().count("order_manage")
                if(order_manage_count < 1):
                    db.create_collection("order_manage")
                else:
                    db.order_manage.delete_many({})
                collection = db["order_manage"]
                dict1 = order_manage.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])
                order_cancel_count = db.list_collection_names().count("order_cancel")
                if(order_cancel_count < 1):
                    db.create_collection("order_cancel")
                else:
                    db.order_cancel.delete_many({})
                collection = db["order_cancel"]
                dict1 = order_cancel.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])
                order_reject_count = db.list_collection_names().count("order_reject")
                if(order_reject_count < 1):
                    db.create_collection("order_reject")
                else:
                    db.order_reject.delete_many({})
                collection = db["order_reject"]
                dict1 = order_reject.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])
                order_complete_count = db.list_collection_names().count("order_complete")
                if(order_complete_count < 1):
                    db.create_collection("order_complete")
                else:
                    db.order_complete.delete_many({})
                collection = db["order_complete"]
                dict1 = order_complete.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])
                order_pending_count = db.list_collection_names().count("order_pending")
                if(order_pending_count < 1):
                    db.create_collection("order_pending")
                else:
                    db.order_pending.delete_many({})
                collection = db["order_pending"]
                dict1 = order_pending.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])
                order_sqr_complete_count = db.list_collection_names().count("order_sqr_complete")
                if(order_sqr_complete_count < 1):
                    db.create_collection("order_sqr_complete")
                else:
                    db.order_sqr_complete.delete_many({})
                collection = db["order_sqr_complete"]
                dict1 = order_sqr_complete.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])
                order_tobe_cancel_count = db.list_collection_names().count("order_tobe_cancel")
                if(order_tobe_cancel_count < 1):
                    db.create_collection("order_tobe_cancel")
                else:
                    db.order_tobe_cancel.delete_many({})
                collection = db["order_tobe_cancel"]
                dict1 = order_tobe_cancel.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])
            # counter=0
            # while counter<10:
            #     try:
            #         file_list = drive.ListFile({'q' : f"'{folder}' in parents and trashed=false"}).GetList()
            #         counter=10
            #     except Exception as e:
            #         logger.error(e)
            #         counter=counter+1

            # # file_list = drive.ListFile({'q' : f"'{folder}' in parents and trashed=false"}).GetList()
            # for index, file in enumerate(file_list):
            #     #print(index+1, 'file downloaded : ', file['title'])
            #     counter=0
            #     while counter<10:
            #         try:
            #             df29 = file.GetContentFile(file['title'])
            #             counter=10
            #         except Exception as e:
            #             logger.error(e)
            #             counter=counter+1
            #     if(file['title'] =="order_manage.txt"):
            #         order_manage = pd.read_csv(file['title'])
            #     if(file['title'] =="order_reject.txt"):
            #         order_reject = pd.read_csv(file['title'])
            #     if(file['title'] =="order_cancel.txt"):
            #         order_cancel = pd.read_csv(file['title'])
            #     if(file['title'] =="order_pending.txt"):
            #         order_pending = pd.read_csv(file['title'])
            #     if(file['title'] =="order_complete.txt"):
            #         order_complete = pd.read_csv(file['title'])
            #     if(file['title'] =="order_tobe_cancel.txt"):
            #         order_tobe_cancel = pd.read_csv(file['title'])
            #     if(file['title'] =="order_multiple.txt"):
            #         order_multiple = pd.read_csv(file['title'])

            order_manage_count = db.order_manage.count_documents({})
            if(order_manage_count > 0):
                collection = db["order_manage"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_manage = instruments1
            order_reject_count = db.order_reject.count_documents({})
            if(order_reject_count > 0):
                collection = db["order_reject"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_reject = instruments1
            order_cancel_count = db.order_cancel.count_documents({})
            if(order_cancel_count > 0):
                collection = db["order_cancel"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                # order_tobe_cancel = pd.read_csv(file['title'])
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                order_cancel = instruments1
            order_pending_count = db.order_pending.count_documents({})
            if(order_pending_count > 0):
                collection = db["order_pending"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_pending = instruments1
            order_complete_count = db.order_complete.count_documents({})
            if(order_complete_count > 0):
                collection = db["order_complete"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_complete = instruments1
            order_tobe_cancel_count = db.order_tobe_cancel.count_documents({})
            if(order_tobe_cancel_count > 0):
                collection = db["order_tobe_cancel"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_tobe_cancel = instruments1
            order_multiple_count = db.order_multiple.count_documents({})
            if(order_multiple_count > 0):
                collection = db["order_multiple"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_multiple = instruments1

            if len(order_reject) > 0:
                print("order_reject being checked"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                kl_del = []
                kl1_del = []
                for kl in range(0,len(order_reject)):
                    print(order_reject['order_id'][kl],len(order_reject))
                    drop = 0
                    status = check_order_history(s,order_reject['order_id'][kl],order_reject['instrument_id'][kl],order_reject['fresh_position'][kl],order_reject['status'][kl])
                    if status == "open":
                        found_complete = 0
                        for kkl in range(0,len(order_complete)):
                            print(order_complete['instrument_id'][kkl], order_reject['instrument_id'][kl])
                            if (order_complete['instrument_id'][kkl] == order_reject['instrument_id'][kl]):
                                found_complete = 1
                                order_reject.at[kl,"status"] = "open"
                                order_multiple.loc[len(order_multiple)] = order_reject.iloc[kl]
#                                 order_reject = order_reject.drop(kl)
                        if found_complete == 0:
                            order_reject.at[kl,"status"] = "open"
                            order_complete.loc[len(order_complete)] = order_reject.iloc[kl]
#                                 order_reject = order_reject.drop(kl)
                            if (len(order_manage)>0):
                                order_manage = order_manage.drop(order_manage.index[(order_manage.instrument_id==order_reject['instrument_id'][kl])].values[0])
                        for kkl in range(0,len(order_pending)):
                            if (order_pending['instrument_id'][kkl] == order_reject['instrument_id'][kl]):
                                if found_complete < 1:
                                    status2 = check_order_history(s,order_pending['order_id'][kkl],order_pending['instrument_id'][kkl],order_pending['fresh_position'][kkl],order_pending['status'][kkl])
                                    if status2 == "open":
                                        order_pending.at[kkl,"status"] = "open"
                                        order_multiple.loc[len(order_multiple)] = order_pending.iloc[kkl]
                                    if status2 == "pending":
                                        status22 = order_cancel_place(s,order_pending['order_id'][kkl],"pending")
                                        if status22 == "cancelled":
                                            order_pending.at[kkl,"status"] = "cancelled"
                                            order_cancel.loc[len(order_cancel)] = order_pending.iloc[kkl]
                                        else:
                                            order_tobe_cancel.loc[len(order_tobe_cancel)] = order_pending.iloc[kkl]
                                    if status2 == "cancelled":
                                        order_pending.at[kkl,"status"] = "cancelled"
                                        order_cancel.loc[len(order_cancel)] = order_pending.iloc[kkl]
                                    if status2 == "rejected":
                                        order_pending.at[kkl,"status"] = "rejected"
                                        order_reject.loc[len(order_reject)] = order_pending.iloc[kkl]
                                    kl1_del.append(kkl)
                        kl_del.append(kl)
                        continue
                    if status == "pending":
                        found_complete = 0
                        for kkl in range(0,len(order_pending)):
                            found_pending = 0
                            if (order_pending['instrument_id'][kkl] == order_reject['instrument_id'][kl]):
                                found_pending = 1
                                index_pending = kkl
                                status22 = kj.order_cancel_place(s,order_reject['order_id'][kl],"pending")
                                if status22 == "cancelled":
                                    order_reject.at[kl,"status"] = "cancelled"
                                    order_cancel.loc[len(order_cancel)] = order_reject.iloc[kl]
                                else:
                                    order_tobe_cancel.loc[len(order_tobe_cancel)] = order_reject.iloc[kl]

                        for kkl in range(0,len(order_complete)):
                            drop = 0
                            if (order_complete['instrument_id'][kkl] == order_reject['instrument_id'][kl]):
                                if found_pending < 1:
                                    order_reject.at[kl,"status"] = "pending"
                                    order_pending.loc[len(order_pending)] = order_reject.iloc[kl]
                                    found_complete = 1
                                if found_pending > 1:
                                    status22 = kj.order_cancel_place(s,order_pending['order_id'][index_pending],"pending")
                                    if status22 == "cancelled":
                                        order_pending.at[kl,"status"] = "cancelled"
                                        order_cancel.loc[len(order_cancel)] = order_pending.iloc[index_pending]
                                    else:
                                        order_tobe_cancel.loc[len(order_tobe_cancel)] = order_pending.iloc[index_pending]
                        if found_complete == 0 and found_pending < 1:
                            order_reject.at[kl,"status"] = "pending"
                            order_pending.loc[len(order_pending)] = order_reject.iloc[kl]
#                                 order_reject = order_reject.drop(kl)
                            if (len(order_manage)>0):
                                order_manage = order_manage.drop(order_manage.index[(order_manage.instrument_id==order_reject['instrument_id'][kl])].values[0])
                                
                        kl_del.append(kl)
                        continue
                    if status == "cancelled":
                        drop = 1
                        order_reject.at[kl,"status"] = "cancelled"
                        order_cancel.loc[len(order_cancel)] = order_reject.iloc[kl]
                        kl_del.append(kl)
                        continue

#                         order_reject = order_reject.drop(kl)
                print(order_reject)
                order_reject = order_reject.drop(order_reject.index[kl_del])
                order_pending = order_pending.drop(order_pending.index[kl1_del])
                print(order_reject)

                # kk = 0
                # mm= 0
                # nn= 0
                # ss = 0
                # tt = 0
                # dd = 0
                # mk = 0
                # ll = 0
                # for index, file in enumerate(file_list):
                #     if(file['title'] =="order_manage.txt"):
                #         kk = file['id']
                #     if(file['title'] =="order_cancel.txt"):
                #         ll = file['id']
                #     if(file['title'] =="order_multiple.txt"):
                #         mm = file['id']
                #     if(file['title'] =="order_reject.txt"):
                #         nn = file['id']
                #     if(file['title'] =="order_complete.txt"):
                #         ss = file['id']
                #     if(file['title'] =="order_pending.txt"):
                #         tt = file['id']
                #     if(file['title'] =="order_sqr_complete.txt"):
                #         dd = file['id']
                #     if(file['title'] =="order_tobe_cancel.txt"):
                #         mk = file['id']
                # if bool(kk):
                #     order_manage.to_csv("order_manage.txt", index=False)
                #     update_file = drive.CreateFile({'id': kk})
                #     update_file.SetContentFile("order_manage.txt")
                #     update_file.Upload()
                # if bool(ll):
                #     order_cancel.to_csv("order_cancel.txt", index=False)
                #     update_file = drive.CreateFile({'id': ll})
                #     update_file.SetContentFile("order_cancel.txt")
                #     update_file.Upload()
                # else:
                #     order_cancel.to_csv("order_cancel.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_cancel.txt"})
                #     gfile.SetContentFile("order_cancel.txt")
                #     gfile.Upload()
                # if bool(mm):
                #     order_multiple.to_csv("order_multiple.txt", index=False)
                #     update_file = drive.CreateFile({'id': mm})
                #     update_file.SetContentFile("order_multiple.txt")
                #     update_file.Upload()
                # else:
                #     order_multiple.to_csv("order_multiple.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_multiple.txt"})
                #     gfile.SetContentFile("order_multiple.txt")
                #     gfile.Upload()
                # if bool(nn):
                #     order_reject.to_csv("order_reject.txt", index=False)
                #     update_file = drive.CreateFile({'id': nn})
                #     update_file.SetContentFile("order_reject.txt")
                #     update_file.Upload()
                # else:
                #     order_reject.to_csv("order_reject.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_reject.txt"})
                #     gfile.SetContentFile("order_reject.txt")
                #     gfile.Upload()
                # if bool(ss):
                #     order_complete.to_csv("order_complete.txt", index=False)
                #     update_file = drive.CreateFile({'id': ss})
                #     update_file.SetContentFile("order_complete.txt")
                #     update_file.Upload()
                # else:
                #     order_complete.to_csv("order_complete.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_complete.txt"})
                #     gfile.SetContentFile("order_complete.txt")
                #     gfile.Upload()
                # if bool(tt):
                #     order_pending.to_csv("order_pending.txt", index=False)
                #     update_file = drive.CreateFile({'id': tt})
                #     update_file.SetContentFile("order_pending.txt")
                #     update_file.Upload()
                # else:
                #     order_pending.to_csv("order_pending.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_pending.txt"})
                #     gfile.SetContentFile("order_pending.txt")
                #     gfile.Upload()
                # if bool(dd):
                #     order_sqr_complete.to_csv("order_sqr_complete.txt", index=False)
                #     update_file = drive.CreateFile({'id': dd})
                #     update_file.SetContentFile("order_sqr_complete.txt")
                #     update_file.Upload()
                # else:
                #     order_sqr_complete.to_csv("order_sqr_complete.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_sqr_complete.txt"})
                #     gfile.SetContentFile("order_sqr_complete.txt")
                #     gfile.Upload()
                # if bool(mk):
                #     order_tobe_cancel.to_csv("order_tobe_cancel.txt", index=False)
                #     update_file = drive.CreateFile({'id': mk})
                #     update_file.SetContentFile("order_tobe_cancel.txt")
                #     update_file.Upload()
                # else:
                #     order_tobe_cancel.to_csv("order_tobe_cancel.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_tobe_cancel.txt"})
                #     gfile.SetContentFile("order_tobe_cancel.txt")
                #     gfile.Upload()
                order_manage_count = db.list_collection_names().count("order_manage")
                if(order_manage_count < 1):
                    db.create_collection("order_manage")
                else:
                    db.order_manage.delete_many({})
                collection = db["order_manage"]
                dict1 = order_manage.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])
                order_cancel_count = db.list_collection_names().count("order_cancel")
                if(order_cancel_count < 1):
                    db.create_collection("order_cancel")
                else:
                    db.order_cancel.delete_many({})
                collection = db["order_cancel"]
                dict1 = order_cancel.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])
                order_reject_count = db.list_collection_names().count("order_reject")
                if(order_reject_count < 1):
                    db.create_collection("order_reject")
                else:
                    db.order_reject.delete_many({})
                collection = db["order_reject"]
                dict1 = order_reject.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])
                order_complete_count = db.list_collection_names().count("order_complete")
                if(order_complete_count < 1):
                    db.create_collection("order_complete")
                else:
                    db.order_complete.delete_many({})
                collection = db["order_complete"]
                dict1 = order_complete.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])
                order_pending_count = db.list_collection_names().count("order_pending")
                if(order_pending_count < 1):
                    db.create_collection("order_pending")
                else:
                    db.order_pending.delete_many({})
                collection = db["order_pending"]
                dict1 = order_pending.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])
                order_sqr_complete_count = db.list_collection_names().count("order_sqr_complete")
                if(order_sqr_complete_count < 1):
                    db.create_collection("order_sqr_complete")
                else:
                    db.order_sqr_complete.delete_many({})
                collection = db["order_sqr_complete"]
                dict1 = order_sqr_complete.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])
                order_tobe_cancel_count = db.list_collection_names().count("order_tobe_cancel")
                if(order_tobe_cancel_count < 1):
                    db.create_collection("order_tobe_cancel")
                else:
                    db.order_tobe_cancel.delete_many({})
                collection = db["order_tobe_cancel"]
                dict1 = order_tobe_cancel.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])
                order_multiple_count = db.list_collection_names().count("order_multiple")
                if(order_multiple_count < 1):
                    db.create_collection("order_multiple")
                else:
                    db.order_multiple.delete_many({})
                collection = db["order_multiple"]
                dict1 = order_multiple.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])

            # counter=0
            # while counter<10:
            #     try:
            #         file_list = drive.ListFile({'q' : f"'{folder}' in parents and trashed=false"}).GetList()
            #         counter=10
            #     except Exception as e:
            #         logger.error(e)
            #         counter=counter+1

            # # file_list = drive.ListFile({'q' : f"'{folder}' in parents and trashed=false"}).GetList()
            # for index, file in enumerate(file_list):
            #     #print(index+1, 'file downloaded : ', file['title'])
            #     counter=0
            #     while counter<10:
            #         try:
            #             df29 = file.GetContentFile(file['title'])
            #             counter=10
            #         except Exception as e:
            #             logger.error(e)
            #             counter=counter+1
            #     if(file['title'] =="order_manage.txt"):
            #         order_manage = pd.read_csv(file['title'])
            #     if(file['title'] =="order_complete.txt"):
            #         order_complete = pd.read_csv(file['title'])
            order_manage_count = db.order_manage.count_documents({})
            if(order_manage_count > 0):
                collection = db["order_manage"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_manage = instruments1
            order_complete_count = db.order_complete.count_documents({})
            if(order_complete_count > 0):
                collection = db["order_complete"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_complete = instruments1
            if len(order_manage) > 0:
                print("order_manage being checked"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                skip_leg1 = 0
                kl_del = []
                for kl in range(0,len(order_manage)):
                    if order_manage['fresh_position'][kl] > 0 and order_manage['leg'][kl] == "leg2":
                        retry_order = 0
                        val_pending2 = 0
                        order_leg2 = 0
                        order_leg2,val_pending2 = order_place_manage(s,file_list,drive,order_manage.loc[order_manage['leg']== "leg2",'current_signal'].values[0],order_manage.loc[order_manage['leg']== "leg2",'instrument_id'].values[0],order_manage.loc[order_manage['leg']== "leg2",'trading_symbol'].values[0],order_manage.loc[order_manage['leg']== "leg2",'price_when_order_placed'].values[0],order_manage.loc[order_manage['leg']== "leg2",'qty'].values[0],order_manage.loc[order_manage['leg']== "leg2",'instru'].values[0],order_manage.loc[order_manage['leg']== "leg2",'fresh_position'].values[0],"leg2",order_manage.loc[order_manage['leg']== "leg2",'strike'].values[0],order_manage.loc[order_manage['leg']== "leg2",'contract'].values[0],order_manage.loc[order_manage['leg']== "leg2",'expiry'].values[0],order_manage.loc[order_manage['leg']== "leg2",'trade_id'].values[0],0,order_manage.loc[order_manage['leg']== "leg2",'buy_sell'].values[0],retry_order,order_manage.loc[order_manage['leg']== "leg2",'status'].values[0],0,0)
                        if (order_leg2 or val_pending2) and order_manage['leg'][kl] == "leg2":
                            # order_manage = order_manage.drop(kl)
                            kl_del.append(kl)
                            if len(order_manage.loc[order_manage['leg']== "leg1",'instrument_id'])>0:
                                order_leg1 = 0
                                val_pending1 = 0
                                # df_opt_1 = get_data(order_manage.loc[order_manage['leg']== "leg1",'instrument_id'].values[0],fromm, fromm, "minute",s)
                                df_opt_1 = pd.DataFrame(kite.historical_data(order_manage.loc[order_manage['leg']== "leg1",'instrument_id'].values[0], fromm, fromm, "minute", continuous=False, oi=True))
                                s = kite
                                price_opt_1 = df_opt_1['close'].iloc[-1]
                                price = order_manage.loc[order_manage['leg']== "leg1",'price_when_order_placed'].values[0]
                                if abs(((price - price_opt_1) / price) * 100) <= 0.1:
                                    order_leg1,val_pending1 = order_place_manage(s,file_list,drive,order_manage.loc[order_manage['leg']== "leg1",'current_signal'].values[0],order_manage.loc[order_manage['leg']== "leg1",'instrument_id'].values[0],order_manage.loc[order_manage['leg']== "leg1",'trading_symbol'].values[0],order_manage.loc[order_manage['leg']== "leg1",'price_when_order_placed'].values[0],order_manage.loc[order_manage['leg']== "leg1",'qty'].values[0],order_manage.loc[order_manage['leg']== "leg1",'instru'].values[0],order_manage.loc[order_manage['leg']== "leg1",'fresh_position'].values[0],"leg1",order_manage.loc[order_manage['leg']== "leg1",'strike'].values[0],order_manage.loc[order_manage['leg']== "leg1",'contract'].values[0],order_manage.loc[order_manage['leg']== "leg1",'expiry'].values[0],order_manage.loc[order_manage['leg']== "leg1",'trade_id'].values[0],0,order_manage.loc[order_manage['leg']== "leg1",'buy_sell'].values[0],retry_order,order_manage.loc[order_manage['leg']== "leg1",'status'].values[0],0,0)
                                if order_leg1 or val_pending1:
                                    kl_del.append(order_manage.index[(order_manage.leg=="leg1")].values[0])
                                    skip_leg1 = 1
                        continue
                    if not skip_leg1  and order_manage['fresh_position'][kl] > 0 and order_manage['leg'][kl] == "leg1":
                        for kkl in range(0,len(order_complete)):
                            if (order_complete['leg'][kkl] == "leg2"):
                                order_leg1 = 0
                                val_pending1 = 0
                                # df_opt_1 = get_data(order_manage.loc[order_manage['leg']== "leg1",'instrument_id'].values[0],fromm, fromm, "minute",s)
                                df_opt_1 = pd.DataFrame(kite.historical_data(order_manage.loc[order_manage['leg']== "leg1",'instrument_id'].values[0], fromm, fromm, "minute", continuous=False, oi=True))
                                s = kite
                                price_opt_1 = df_opt_1['close'].iloc[-1]
                                price = order_manage.loc[order_manage['leg']== "leg1",'price_when_order_placed'].values[0]
                                if abs(((price - price_opt_1) / price) * 100) <= 0.1:
                                    retry_order = 1
                                    order_leg1,val_pending1 = order_place_manage(s,file_list,drive,order_manage.loc[order_manage['leg']== "leg1",'current_signal'].values[0],order_manage.loc[order_manage['leg']== "leg1",'instrument_id'].values[0],order_manage.loc[order_manage['leg']== "leg1",'trading_symbol'].values[0],order_manage.loc[order_manage['leg']== "leg1",'price_when_order_placed'].values[0],order_manage.loc[order_manage['leg']== "leg1",'qty'].values[0],order_manage.loc[order_manage['leg']== "leg1",'instru'].values[0],order_manage.loc[order_manage['leg']== "leg1",'fresh_position'].values[0],"leg1",order_manage.loc[order_manage['leg']== "leg1",'strike'].values[0],order_manage.loc[order_manage['leg']== "leg1",'contract'].values[0],order_manage.loc[order_manage['leg']== "leg1",'expiry'].values[0],order_manage.loc[order_manage['leg']== "leg1",'trade_id'].values[0],0,order_manage.loc[order_manage['leg']== "leg1",'buy_sell'].values[0],retry_order,order_manage.loc[order_manage['leg']== "leg1",'status'].values[0],0,0,0)
                                if order_leg1 or val_pending1:
                                    kl_del.append(order_manage.index[(order_manage.leg=="leg1")].values[0])
                        continue
                    if order_manage['fresh_position'][kl] > 0 and order_manage['leg'][kl] == "leg3":
                        val_pending3 = 0
                        order_leg3 = 0
                        order_leg3,val_pending3 = order_place_manage(s,file_list,drive,order_manage.loc[order_manage['leg']== "leg3",'current_signal'].values[0],order_manage.loc[order_manage['leg']== "leg3",'instrument_id'].values[0],order_manage.loc[order_manage['leg']== "leg3",'trading_symbol'].values[0],order_manage.loc[order_manage['leg']== "leg3",'price_when_order_placed'].values[0],order_manage.loc[order_manage['leg']== "leg3",'qty'].values[0],order_manage.loc[order_manage['leg']== "leg3",'instru'].values[0],order_manage.loc[order_manage['leg']== "leg3",'fresh_position'].values[0],"leg3",order_manage.loc[order_manage['leg']== "leg3",'strike'].values[0],order_manage.loc[order_manage['leg']== "leg3",'contract'].values[0],order_manage.loc[order_manage['leg']== "leg3",'expiry'].values[0],order_manage.loc[order_manage['leg']== "leg3",'trade_id'].values[0],0,order_manage.loc[order_manage['leg']== "leg3",'buy_sell'].values[0],retry_order,order_manage.loc[order_manage['leg']== "leg3",'status'].values[0],0,0)
                        if order_leg3 or val_pending3:
                            kl_del.append(order_manage.index[(order_manage.leg=="leg3")].values[0])
                        continue
                    if order_manage['fresh_position'][kl] > 0 and order_manage['leg'][kl] == "leg4":
                        val_pending4 = 0
                        order_leg4 = 0
                        order_leg4,val_pending4 = order_place_manage(s,file_list,drive,order_manage.loc[order_manage['leg']== "leg4",'current_signal'].values[0],order_manage.loc[order_manage['leg']== "leg4",'instrument_id'].values[0],order_manage.loc[order_manage['leg']== "leg4",'trading_symbol'].values[0],order_manage.loc[order_manage['leg']== "leg4",'price_when_order_placed'].values[0],order_manage.loc[order_manage['leg']== "leg4",'qty'].values[0],order_manage.loc[order_manage['leg']== "leg4",'instru'].values[0],order_manage.loc[order_manage['leg']== "leg4",'fresh_position'].values[0],"leg4",order_manage.loc[order_manage['leg']== "leg4",'strike'].values[0],order_manage.loc[order_manage['leg']== "leg4",'contract'].values[0],order_manage.loc[order_manage['leg']== "leg4",'expiry'].values[0],order_manage.loc[order_manage['leg']== "leg4",'trade_id'].values[0],0,order_manage.loc[order_manage['leg']== "leg4",'buy_sell'].values[0],retry_order,order_manage.loc[order_manage['leg']== "leg4",'status'].values[0],0,0)
#                     if order_manage.loc[order_manage['leg']== "leg1",'executed'].values < 1:
                        if order_leg4 or val_pending4:
                            kl_del.append(order_manage.index[(order_manage.leg=="leg4")].values[0])
                            # order_manage = order_manage.drop(kl)
                        continue
                print(order_manage)
                order_manage = order_manage.drop(order_manage.index[kl_del])
                print(order_manage)
                # kk = 0
                # ll = 0
                # for index, file in enumerate(file_list):
                #     if(file['title'] =="order_manage.txt"):
                #         kk = file['id']
                #     if(file['title'] =="order_complete.txt"):
                #         kk = file['id']
                # if bool(kk):
                #     order_manage.to_csv("order_manage.txt", index=False)
                #     update_file = drive.CreateFile({'id': kk})
                #     update_file.SetContentFile("order_manage.txt")
                #     update_file.Upload()
                # if bool(ll):
                #     order_manage.to_csv("order_complete.txt", index=False)
                #     update_file = drive.CreateFile({'id': ll})
                #     update_file.SetContentFile("order_complete.txt")
                #     update_file.Upload()
                    # file_list = drive.ListFile({'q' : f"'{folder}' in parents and trashed=false"}).GetList()
                order_manage_count = db.list_collection_names().count("order_manage")
                if(order_manage_count < 1):
                    db.create_collection("order_manage")
                else:
                    db.order_manage.delete_many({})
                collection = db["order_manage"]
                dict1 = order_manage.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])
                order_complete_count = db.list_collection_names().count("order_complete")
                if(order_complete_count < 1):
                    db.create_collection("order_complete")
                else:
                    db.order_complete.delete_many({})
                collection = db["order_complete"]
                dict1 = order_complete.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])

            # kk = 0
            # mm= 0
            # nn= 0
            # ss = 0
            # tt = 0
            # dd = 0
            # mk = 0
            # ll = 0
            # for index, file in enumerate(file_list):
            #     #print(index+1, 'file downloaded : ', file['title'])
            #     counter=0
            #     while counter<10:
            #         try:
            #             df29 = file.GetContentFile(file['title'])
            #             counter=10
            #         except Exception as e:
            #             logger.error(e)
            #             counter=counter+1
            #     #csv_raw = StringIO(file.text)
            #     if(file['title'] =="order_tobe_sqr_complete.txt"):
            #         order_tobe_sqr_complete = pd.read_csv(file['title'])
            #     if(file['title'] =="order_manage.txt"):
            #         order_manage = pd.read_csv(file['title'])
            #     if(file['title'] =="order_pending.txt"):
            #         order_pending = pd.read_csv(file['title'])
            order_manage_count = db.order_manage.count_documents({})
            if(order_manage_count > 0):
                collection = db["order_manage"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_manage = instruments1
            order_tobe_sqr_complete_count = db.order_tobe_sqr_complete.count_documents({})
            if(order_tobe_sqr_complete_count > 0):
                collection = db["order_tobe_sqr_complete"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_tobe_sqr_complete = instruments1
            order_pending_count = db.order_pending.count_documents({})
            if(order_pending_count > 0):
                collection = db["order_pending"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_pending = instruments1

            if len(order_tobe_sqr_complete) > 0 and len(order_manage) <= 0 and len(order_pending) <= 0:
                skip_leg1 = 0
                print("order_tobe_sqr_complete being checked"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                for mj in range(0,len(order_tobe_sqr_complete)):
                    if order_tobe_sqr_complete['buy_sell'][mj] == "SELL":
                        buy_sell="BUY"
                    else:
                        buy_sell="SELL"
                    if order_tobe_sqr_complete['leg'][mj] == "leg1":
                        # df_opt = get_data(order_tobe_sqr_complete['instrument_id'][mj],fromm, fromm, "minute",s)
                        df_opt = pd.DataFrame(kite.historical_data(order_tobe_sqr_complete['instrument_id'][mj], fromm, fromm, "minute", continuous=False, oi=True))
                        s = kite
                        retry_order = 0
                        order = order_place_sqr_complete(s,file_list,drive,order_tobe_sqr_complete['current_signal'][mj],order_tobe_sqr_complete['instrument_id'][mj],order_tobe_sqr_complete['trading_symbol'][mj],df_opt,order_tobe_sqr_complete['qty'][mj],order_tobe_sqr_complete['instru'][mj],0,order_tobe_sqr_complete['leg'][mj],order_tobe_sqr_complete['strike'][mj],order_tobe_sqr_complete['contract'][mj],order_tobe_sqr_complete['expiry'][mj],0,1,buy_sell,retry_order,order_tobe_sqr_complete['status'][mj],order_tobe_sqr_complete['reject_count'][mj],order_tobe_sqr_complete['cancel_count'][mj],0)
                        if order == "close":
                            skip_leg1 = 1
                            requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': "Sqrd "+str(order_tobe_sqr_complete['leg'][mj])})
                            print("Sqr "+str(order_tobe_sqr_complete['leg'][mj])+str(order_tobe_sqr_complete['strike'][mj])+str(order_tobe_sqr_complete['contract'][mj])," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                            print("position square off"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                        else:
                            requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': "Fail to Sqr "+str(order_tobe_sqr_complete['leg'][mj])})
                    if (skip_leg1  and order_tobe_sqr_complete['leg'][mj] == "leg2") or ((len(order_tobe_sqr_complete.loc[order_tobe_sqr_complete['leg']== "leg1",'instrument_id'])<1) and order_tobe_sqr_complete['leg'][mj] == "leg2"):
                        # df_opt = get_data(order_tobe_sqr_complete['instrument_id'][mj],fromm, fromm, "minute",s)
                        df_opt = pd.DataFrame(kite.historical_data(order_tobe_sqr_complete['instrument_id'][mj], fromm, fromm, "minute", continuous=False, oi=True))
                        s = kite
                        retry_order = 0
                        order = order_place_sqr_complete(s,file_list,drive,order_tobe_sqr_complete['current_signal'][mj],order_tobe_sqr_complete['instrument_id'][mj],order_tobe_sqr_complete['trading_symbol'][mj],df_opt,order_tobe_sqr_complete['qty'][mj],order_tobe_sqr_complete['instru'][mj],0,order_tobe_sqr_complete['leg'][mj],order_tobe_sqr_complete['strike'][mj],order_tobe_sqr_complete['contract'][mj],order_tobe_sqr_complete['expiry'][mj],0,1,buy_sell,retry_order,order_tobe_sqr_complete['status'][mj],order_tobe_sqr_complete['reject_count'][mj],order_tobe_sqr_complete['cancel_count'][mj],0)
                        if order == "close":
                            requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': "Sqrd "+str(order_tobe_sqr_complete['leg'][mj])})
                            print("Sqr "+str(order_tobe_sqr_complete['leg'][mj])+str(order_tobe_sqr_complete['strike'][mj])+str(order_tobe_sqr_complete['contract'][mj])," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                            print("position square off"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                        else:
                            requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': "Fail to Sqr "+str(order_tobe_sqr_complete['leg'][mj])})
                # file_list = drive.ListFile({'q' : f"'{folder}' in parents and trashed=false"}).GetList()
                # ss = 0
                # for index, file in enumerate(file_list):
                #     if(file['title'] =="order_tobe_sqr_complete.txt"):
                #         ss = file['id']
                # if bool(ss):
                #     order_tobe_sqr_complete.to_csv("order_tobe_sqr_complete.txt", index=False)
                #     update_file = drive.CreateFile({'id': ss})
                #     update_file.SetContentFile("order_tobe_sqr_complete.txt")
                #     update_file.Upload()
                # else:
                #     order_tobe_sqr_complete.to_csv("order_tobe_sqr_complete.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_tobe_sqr_complete.txt"})
                #     gfile.SetContentFile("order_tobe_sqr_complete.txt")
                #     gfile.Upload()
            
                order_tobe_sqr_complete_count = db.list_collection_names().count("order_tobe_sqr_complete")
                if(order_tobe_sqr_complete_count < 1):
                    db.create_collection("order_tobe_sqr_complete")
                else:
                    db.order_tobe_sqr_complete.delete_many({})
                collection = db["order_tobe_sqr_complete"]
                dict1 = order_tobe_sqr_complete.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])
            

            # for index, file in enumerate(file_list):
            #     #print(index+1, 'file downloaded : ', file['title'])
            #     counter=0
            #     while counter<10:
            #         try:
            #             df29 = file.GetContentFile(file['title'])
            #             counter=10
            #         except Exception as e:
            #             logger.error(e)
            #             counter=counter+1
            #     if(file['title'] =="order_manage.txt"):
            #         order_manage = pd.read_csv(file['title'])
            #     if(file['title'] =="order_multiple.txt"):
            #         order_multiple = pd.read_csv(file['title'])
            #     if(file['title'] =="order_sqr_multiple.txt"):
            #         order_sqr_multiple = pd.read_csv(file['title'])
            #     if(file['title'] =="order_reject_multiple.txt"):
            #         order_reject_multiple = pd.read_csv(file['title'])
            #     if(file['title'] =="order_cancel_multiple.txt"):
            #         order_cancel_multiple = pd.read_csv(file['title'])
            #     if(file['title'] =="order_pending_multiple.txt"):
            #         order_pending_multiple = pd.read_csv(file['title'])
            order_manage_count = db.order_manage.count_documents({})
            if(order_manage_count > 0):
                collection = db["order_manage"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_manage = instruments1
            order_multiple_count = db.order_multiple.count_documents({})
            if(order_multiple_count > 0):
                collection = db["order_multiple"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_multiple = instruments1
            order_sqr_multiple_count = db.order_sqr_multiple.count_documents({})
            if(order_sqr_multiple_count > 0):
                collection = db["order_sqr_multiple"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_sqr_multiple = instruments1
            order_reject_multiple_count = db.order_reject_multiple.count_documents({})
            if(order_reject_multiple_count > 0):
                collection = db["order_reject_multiple"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_reject_multiple = instruments1
            order_cancel_multiple_count = db.order_cancel_multiple.count_documents({})
            if(order_cancel_multiple_count > 0):
                collection = db["order_cancel_multiple"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_cancel_multiple = instruments1
            order_pending_multiple_count = db.order_pending_multiple.count_documents({})
            if(order_pending_multiple_count > 0):
                collection = db["order_pending_multiple"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_pending_multiple = instruments1

            if len(order_pending_multiple) > 0:
                print("order_pending_multiple being checked"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                kl_del = []
                for kl in range(0,len(order_pending_multiple)):
                    status = check_order_history(s,order_pending_multiple['order_id'][kl],order_pending_multiple['instrument_id'][kl],order_pending_multiple['fresh_position'][kl],order_pending_multiple['status'][kl])
                    if status == "close":
                        order_pending_multiple.at[kl,"status"] = "close"
                        order_sqr_multiple.loc[len(order_sqr_multiple)] = order_pending_multiple.iloc[kl]
                        kl_del.append(kl)
#                        order_pending_multiple = order_pending_multiple.drop(kl)        
                        continue
                    if status == "rejected":
                        order_pending_multiple.at[kl,"status"] = "rejected"
                        order_reject_multiple.loc[len(order_reject_multiple)] = order_pending_multiple.iloc[kl]
                        order_pending_multiple.at[kl,"status"] = "open"
                        order_multiple.loc[len(order_multiple)] = order_pending_multiple.iloc[kl]
                        kl_del.append(kl)
#                        order_pending_multiple = order_pending_multiple.drop(kl)        
                        continue
                    if status == "cancelled":
                        order_pending_multiple.at[kl,"status"] = "cancelled"
                        order_cancel_multiple.loc[len(order_cancel_multiple)] = order_pending_multiple.iloc[kl]
                        order_pending_multiple.at[kl,"status"] = "open"
                        order_multiple.loc[len(order_multiple)] = order_pending_multiple.iloc[kl]
                        kl_del.append(kl)
#                        order_pending_multiple = order_pending_multiple.drop(kl)        
                        continue
                    if status == "pending":
                        status4 = ""
                        for kkl in range(0,len(order_sqr_multiple)):
                            if (order_sqr_multiple['instrument_id'][kkl] == order_pending_multiple['instrument_id'][kl]) and (order_sqr_multiple['buy_sell'][kkl] == order_pending_multiple['buy_sell'][kl]):
                                status4 = order_cancel_place(s,order_pending_multiple['order_id'][kl],"pending")
                                if status4 == "cancelled":
                                    order_pending_multiple.at[kl,"status"] = "cancelled"
                                    order_cancel_multiple.loc[len(order_cancel_multiple)] = order_pending_multiple.iloc[kl]
                                    kl_del.append(kl)
#                                    order_pending_multiple = order_pending_multiple.drop(kl)
                        if not status4:
                           order_leg,price,modify_time = order_modify_multiple(s,file_list,drive,order_pending_multiple['current_signal'][kl],order_pending_multiple['instrument_id'][kl],order_pending_multiple['trading_symbol'][kl],order_pending_multiple['price_when_order_placed'][kl],order_pending_multiple['qty'][kl],order_pending_multiple['instru'][kl],order_pending_multiple['fresh_position'][kl],"leg1",order_pending_multiple['strike'][kl],order_pending_multiple['contract'][kl],order_pending_multiple['expiry'][kl],order_pending_multiple['trade_id'][kl],0,order_pending_multiple['buy_sell'][kl],retry_order,order_pending_multiple['status'][kl],order_pending_multiple['order_id'][kl])
                           if order_leg == "modified":
                                status1 = check_order_history(s,order_pending_multiple['order_id'][kl],order_pending_multiple['instrument_id'][kl],order_pending_multiple['fresh_position'][kl],order_pending_multiple['status'][kl])
                                if status1 == "close":
                                    order_pending_multiple.at[kl,"status"] = "close"
                                    order_pending_multiple.at[kl,"exit_time"] = modify_time
                                    order_pending_multiple.at[kl,"exit_price"] = price
                                    order_sqr_multiple.loc[len(order_sqr_multiple)] = order_pending_multiple.iloc[kl]
#                                    order_pending_multiple = order_pending_multiple.drop(kl)
                                    kl_del.append(kl)
                print(order_pending_multiple)
                order_pending_multiple = order_pending_multiple.drop(order_pending_multiple.index[kl_del])
                print(order_pending_multiple)
                # file_list = drive.ListFile({'q' : f"'{folder}' in parents and trashed=false"}).GetList()
                # ll = 0
                # kk = 0
                # mm = 0
                # nn = 0
                # tt = 0
                # dd = 0
                # for index, file in enumerate(file_list):
                #     if(file['title'] =="order_pending_multiple.txt"):
                #         kk = file['id']
                #     if(file['title'] =="order_cancel_multiple.txt"):
                #         ll = file['id']
                #     if(file['title'] =="order_multiple.txt"):
                #         mm = file['id']
                #     if(file['title'] =="order_reject_multiple.txt"):
                #         nn = file['id']
                #     if(file['title'] =="order_sqr_multiple.txt"):
                #         dd = file['id']
                # if bool(kk):
                #     order_pending_multiple.to_csv("order_pending_multiple.txt", index=False)
                #     update_file = drive.CreateFile({'id': kk})
                #     update_file.SetContentFile("order_pending_multiple.txt")
                #     update_file.Upload()
                # if bool(ll):
                #     order_cancel_multiple.to_csv("order_cancel_multiple.txt", index=False)
                #     update_file = drive.CreateFile({'id': ll})
                #     update_file.SetContentFile("order_cancel_multiple.txt")
                #     update_file.Upload()
                # else:
                #     order_cancel_multiple.to_csv("order_cancel_multiple.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_cancel_multiple.txt"})
                #     gfile.SetContentFile("order_cancel_multiple.txt")
                #     gfile.Upload()
                # if bool(mm):
                #     order_multiple.to_csv("order_multiple.txt", index=False)
                #     update_file = drive.CreateFile({'id': mm})
                #     update_file.SetContentFile("order_multiple.txt")
                #     update_file.Upload()
                # else:
                #     order_multiple.to_csv("order_multiple.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_multiple.txt"})
                #     gfile.SetContentFile("order_multiple.txt")
                #     gfile.Upload()

                # if bool(nn):
                #     order_reject_multiple.to_csv("order_reject_multiple.txt", index=False)
                #     update_file = drive.CreateFile({'id': nn})
                #     update_file.SetContentFile("order_reject_multiple.txt")
                #     update_file.Upload()
                # else:
                #     order_reject_multiple.to_csv("order_reject_multiple.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_reject_multiple.txt"})
                #     gfile.SetContentFile("order_reject_multiple.txt")
                #     gfile.Upload()

                # if bool(dd):
                #     order_sqr_multiple.to_csv("order_sqr_multiple.txt", index=False)
                #     update_file = drive.CreateFile({'id': dd})
                #     update_file.SetContentFile("order_sqr_multiple.txt")
                #     update_file.Upload()
                # else:
                #     order_sqr_multiple.to_csv("order_sqr_multiple.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_sqr_multiple.txt"})
                #     gfile.SetContentFile("order_sqr_multiple.txt")
                #     gfile.Upload()
            # file_list = drive.ListFile({'q' : f"'{folder}' in parents and trashed=false"}).GetList()
                order_pending_multiple_count = db.list_collection_names().count("order_pending_multiple")
                if(order_pending_multiple_count < 1):
                    db.create_collection("order_pending_multiple")
                else:
                    db.order_pending_multiple.delete_many({})
                collection = db["order_pending_multiple"]
                dict1 = order_pending_multiple.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])
                order_cancel_multiple_count = db.list_collection_names().count("order_cancel_multiple")
                if(order_cancel_multiple_count < 1):
                    db.create_collection("order_cancel_multiple")
                else:
                    db.order_cancel_multiple.delete_many({})
                collection = db["order_cancel_multiple"]
                dict1 = order_cancel_multiple.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])
                order_multiple_count = db.list_collection_names().count("order_multiple")
                if(order_multiple_count < 1):
                    db.create_collection("order_multiple")
                else:
                    db.order_multiple.delete_many({})
                collection = db["order_multiple"]
                dict1 = order_multiple.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])
                order_reject_multiple_count = db.list_collection_names().count("order_reject_multiple")
                if(order_reject_multiple_count < 1):
                    db.create_collection("order_reject_multiple")
                else:
                    db.order_reject_multiple.delete_many({})
                collection = db["order_reject_multiple"]
                dict1 = order_reject_multiple.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])
                order_sqr_multiple_count = db.list_collection_names().count("order_sqr_multiple")
                if(order_sqr_multiple_count < 1):
                    db.create_collection("order_sqr_multiple")
                else:
                    db.order_sqr_multiple.delete_many({})
                collection = db["order_sqr_multiple"]
                dict1 = order_sqr_multiple.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])

            # for index, file in enumerate(file_list):
            #     #print(index+1, 'file downloaded : ', file['title'])
            #     counter=0
            #     while counter<10:
            #         try:
            #             df29 = file.GetContentFile(file['title'])
            #             counter=10
            #         except Exception as e:
            #             logger.error(e)
            #             counter=counter+1
            #     if(file['title'] =="order_multiple.txt"):
            #         order_multiple = pd.read_csv(file['title'])
            #     if(file['title'] =="order_sqr_multiple.txt"):
            #         order_sqr_multiple = pd.read_csv(file['title'])
            #     if(file['title'] =="order_reject_multiple.txt"):
            #         order_reject_multiple = pd.read_csv(file['title'])
            #     if(file['title'] =="order_cancel_multiple.txt"):
            #         order_cancel_multiple = pd.read_csv(file['title'])
            #     if(file['title'] =="order_pending_multiple.txt"):
            #         order_pending_multiple = pd.read_csv(file['title'])
            #     if(file['title'] =="order_tobe_cancel_multiple.txt"):
            #         order_tobe_cancel_multiple = pd.read_csv(file['title'])
            order_multiple_count = db.order_multiple.count_documents({})
            if(order_multiple_count > 0):
                collection = db["order_multiple"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_multiple = instruments1
            order_sqr_multiple_count = db.order_sqr_multiple.count_documents({})
            if(order_sqr_multiple_count > 0):
                collection = db["order_sqr_multiple"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_sqr_multiple = instruments1
            order_reject_multiple_count = db.order_reject_multiple.count_documents({})
            if(order_reject_multiple_count > 0):
                collection = db["order_reject_multiple"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_reject_multiple = instruments1
            order_cancel_multiple_count = db.order_cancel_multiple.count_documents({})
            if(order_cancel_multiple_count > 0):
                collection = db["order_cancel_multiple"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_cancel_multiple = instruments1
            order_pending_multiple_count = db.order_pending_multiple.count_documents({})
            if(order_pending_multiple_count > 0):
                collection = db["order_pending_multiple"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_pending_multiple = instruments1
            order_tobe_cancel_multiple_count = db.order_tobe_cancel_multiple.count_documents({})
            if(order_tobe_cancel_multiple_count > 0):
                collection = db["order_tobe_cancel_multiple"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_tobe_cancel_multiple = instruments1
                   
            if len(order_reject_multiple) > 0:
                print("order_reject_multiple being checked"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                kl_del = []
                kl1_del = []
                for kl in range(0,len(order_reject_multiple)):
                    status = check_order_history(s,order_reject_multiple['order_id'][kl],order_reject_multiple['instrument_id'][kl],order_reject_multiple['fresh_position'][kl],order_reject_multiple['status'][kl])
                    if status == "close":
                        found_complete = 0
                        for kkl in range(0,len(order_sqr_multiple)):
                            if (order_sqr_multiple['instrument_id'][kkl] == order_reject_multiple['instrument_id'][kl]) and (order_sqr_multiple['buy_sell'][kkl] == order_reject_multiple['buy_sell'][kl]):
#                                 status1 = check_order_history(order_complete['order_id'][kkl],order_manage['instrument_id'][kkl],order_manage['fresh_position'][kkl],order_manage['status'][kkl])
#                                 rw = kkl
#                                 if status == "open" and status1 == "open":
                                found_complete = 1
                                order_reject_multiple.loc["status",kl] = "close"
                                order_multiple.loc[len(order_multiple)] = order_reject_multiple.iloc[kl]
#                                 order_reject_multiple = order_reject_multiple.drop(kl)
                                # order_multiple = order_multiple.drop(order_multiple.index[(order_multiple.instrument_id==order_reject_multiple['instrument_id'][kl])].values[0])
                        if found_complete == 0:
                            order_reject_multiple.at[kl,"status"] = "close"
                            order_sqr_multiple.loc[len(order_sqr_multiple)] = order_reject_multiple.iloc[kl]
                            if (len(order_multiple)>0):
                                order_multiple = order_multiple.drop(order_multiple.index[(order_multiple.instrument_id==order_reject_multiple['instrument_id'][kl])].values[0])
                        for kkl in range(0,len(order_pending_multiple)):
                            if (order_pending_multiple['instrument_id'][kkl] == order_reject_multiple['instrument_id'][kl]) and (order_pending_multiple['buy_sell'][kkl] == order_reject_multiple['buy_sell'][kl]):
                                if found_complete < 1:
                                    # order_sqr_multiple.loc[len(order_sqr_multiple)] = order_reject_multiple.iloc[kl]
#                                 order_reject_multiple = order_reject_multiple.drop(kl)
                                    status2 = check_order_history(s,order_pending_multiple['order_id'][kl],order_pending_multiple['instrument_id'][kl],order_pending_multiple['fresh_position'][kl],order_pending_multiple['status'][kl])
                                    if status2 == "close":
                                        order_pending_multiple.at[kkl,"status"] = "close"
                                        order_multiple.loc[len(order_multiple)] = order_pending_multiple.iloc[kl]
#                                     order_reject_multiple = order_reject_multiple.drop(kl)
                                        # order_sqr_multiple.loc[len(order_sqr_multiple)] = order_pending_multiple.iloc[kkl]
#                                     order_pending_multiple = order_pending_multiple.drop(kkl)
                                    if status2 == "pending":
                                        order_pending_multiple.at[kkl,"status"] = "pending"
                                        status22 = order_cancel_place(s,order_pending_multiple['order_id'][kkl],"pending")
                                        if status22 == "cancelled":
                                            order_pending_multiple.at[kkl,"status"] = "cancelled"
                                            order_cancel_multiple.loc[len(order_cancel_multiple)] = order_pending_multiple.iloc[kkl]
#                                             order_pending_multiple = order_pending_multiple.drop(kl)
                                        else:
                                            order_tobe_cancel_multiple.loc[len(order_tobe_cancel_multiple)] = order_pending_multiple.iloc[kkl]

#                                     status2 = order_cancel(order_pending_multiple['order_id'][kl],"pending")
                                    if status2 == "cancelled":
                                        order_pending_multiple.at[kkl,"status"] = "cancelled"
                                        # order_pending_multiple.loc["status",kkl] = "cancelled"
                                        order_cancel_multiple.loc[len(order_cancel_multiple)] = order_pending_multiple.iloc[kkl]
                                    if status2 == "rejected":
                                        order_pending_multiple.at[kkl,"status"] = "rejected"
                                        # order_pending_multiple_complete.loc["status",kkl] = "rejected"
                                        order_reject_multiple.loc[len(order_reject_multiple)] = order_pending_multiple.iloc[kkl]
                                kl1_del.append(kkl)
                        kl_del.append(kl)
                        continue
                    if status == "pending":
                        found_complete = 0
                        for kkl in range(0,len(order_pending_multiple)):
                            found_pending = 0
                            if (order_pending_multiple['instrument_id'][kkl] == order_reject_multiple['instrument_id'][kl]) and (order_pending_multiple['buy_sell'][kkl] == order_reject_multiple['buy_sell'][kl]):
                                found_pending = 1
                                index_pending = kkl
                                status22 = kj.order_cancel_place(s,order_reject_multiple['order_id'][kl],"pending")
                                if status22 == "cancelled":
                                    order_reject_multiple.at[kl,"status"] = "cancelled"
                                    order_cancel_multiple.loc[len(order_cancel_multiple)] = order_reject_multiple.iloc[kl]
                                else:
                                    order_tobe_cancel_multiple.loc[len(order_tobe_cancel_multiple)] = order_reject_multiple.iloc[kl]
#                                     order_reject = order_reject.drop(kl)
                        for kkl in range(0,len(order_sqr_multiple)):
                            if (order_sqr_multiple['instrument_id'][kkl] == order_reject_multiple['instrument_id'][kl]) and (order_sqr_multiple['buy_sell'][kkl] == order_reject_multiple['buy_sell'][kl]):
                                if found_pending < 1:
                                    order_reject_multiple.at[kl,"status"] = "pending"
                                    order_pending_multiple.loc[len(order_pending_multiple)] = order_reject_multiple.iloc[kl]
                                    found_complete = 1
                                if found_pending > 1:
                                    status22 = kj.order_cancel_place(s,order_pending_multiple['order_id'][index_pending],"pending")
                                    if status22 == "cancelled":
                                        order_pending_multiple.at[kl,"status"] = "cancelled"
                                        order_cancel_multiple.loc[len(order_cancel_multiple)] = order_pending_multiple.iloc[index_pending]
                                    else:
                                        order_tobe_cancel_multiple.loc[len(order_tobe_cancel_multiple)] = order_pending_multiple.iloc[index_pending]
                        if found_complete == 0 and found_pending < 1:
                            order_reject_multiple.at[kl,"status"] = "pending"
                            order_pending_multiple.loc[len(order_pending_multiple)] = order_reject_multiple.iloc[kl]
#                                 order_reject = order_reject.drop(kl)
                            if (len(order_multiple)>0):
                                order_multiple = order_multiple.drop(order_multiple.index[(order_multiple.instrument_id==order_reject_multiple['instrument_id'][kl])].values[0])
                        kl_del.append(kl)
                        continue
                    if status == "cancelled":
                        order_reject_multiple.at[kl,"status"] = "cancelled"
                        order_cancel_multiple.loc[len(order_cancel_multiple)] = order_reject_multiple.iloc[kl]
                        kl_del.append(kl)

                print(order_reject_multiple)
                order_reject_multiple = order_reject_multiple.drop(order_reject_multiple.index[kl_del])
                order_pending_multiple = order_pending_multiple.drop(order_pending_multiple.index[kl1_del])
                print(order_reject_multiple)
                # file_list = drive.ListFile({'q' : f"'{folder}' in parents and trashed=false"}).GetList()
                # ll = 0
                # kk = 0
                # mm = 0
                # nn = 0
                # tt = 0
                # dd = 0
                # for index, file in enumerate(file_list):
                #     if(file['title'] =="order_pending_multiple.txt"):
                #         kk = file['id']
                #     if(file['title'] =="order_cancel_multiple.txt"):
                #         ll = file['id']
                #     if(file['title'] =="order_multiple.txt"):
                #         mm = file['id']
                #     if(file['title'] =="order_reject_multiple.txt"):
                #         nn = file['id']
                #     if(file['title'] =="order_tobe_cancel_multiple.txt"):
                #         tt = file['id']
                #     if(file['title'] =="order_sqr_multiple.txt"):
                #         dd = file['id']
                # if bool(kk):
                #     order_pending_multiple.to_csv("order_pending_multiple.txt", index=False)
                #     update_file = drive.CreateFile({'id': kk})
                #     update_file.SetContentFile("order_pending_multiple.txt")
                #     update_file.Upload()
                # if bool(ll):
                #     order_cancel_multiple.to_csv("order_cancel_multiple.txt", index=False)
                #     update_file = drive.CreateFile({'id': ll})
                #     update_file.SetContentFile("order_cancel_multiple.txt")
                #     update_file.Upload()
                # else:
                #     order_cancel_multiple.to_csv("order_cancel_multiple.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_cancel_multiple.txt"})
                #     gfile.SetContentFile("order_cancel_multiple.txt")
                #     gfile.Upload()
                # if bool(mm):
                #     order_multiple.to_csv("order_multiple.txt", index=False)
                #     update_file = drive.CreateFile({'id': mm})
                #     update_file.SetContentFile("order_multiple.txt")
                #     update_file.Upload()
                # else:
                #     order_multiple.to_csv("order_multiple.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_multiple.txt"})
                #     gfile.SetContentFile("order_multiple.txt")
                #     gfile.Upload()
                # if bool(nn):
                #     order_reject_multiple.to_csv("order_reject_multiple.txt", index=False)
                #     update_file = drive.CreateFile({'id': nn})
                #     update_file.SetContentFile("order_reject_multiple.txt")
                #     update_file.Upload()
                # else:
                #     order_reject_multiple.to_csv("order_reject_multiple.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_reject_multiple.txt"})
                #     gfile.SetContentFile("order_reject_multiple.txt")
                #     gfile.Upload()
                # if bool(tt):
                #     order_tobe_cancel_multiple.to_csv("order_tobe_cancel_multiple.txt", index=False)
                #     update_file = drive.CreateFile({'id': tt})
                #     update_file.SetContentFile("order_tobe_cancel_multiple.txt")
                #     update_file.Upload()
                # else:
                #     order_tobe_cancel_multiple.to_csv("order_tobe_cancel_multiple.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_tobe_cancel_multiple.txt"})
                #     gfile.SetContentFile("order_tobe_cancel_multiple.txt")
                #     gfile.Upload()
                # if bool(dd):
                #     order_sqr_multiple.to_csv("order_sqr_multiple.txt", index=False)
                #     update_file = drive.CreateFile({'id': dd})
                #     update_file.SetContentFile("order_sqr_multiple.txt")
                #     update_file.Upload()
                # else:
                #     order_sqr_multiple.to_csv("order_sqr_multiple.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_sqr_multiple.txt"})
                #     gfile.SetContentFile("order_sqr_multiple.txt")
                #     gfile.Upload()
            # file_list = drive.ListFile({'q' : f"'{folder}' in parents and trashed=false"}).GetList()
                order_pending_multiple_count = db.list_collection_names().count("order_pending_multiple")
                if(order_pending_multiple_count < 1):
                    db.create_collection("order_pending_multiple")
                else:
                    db.order_pending_multiple.delete_many({})
                collection = db["order_pending_multiple"]
                dict1 = order_pending_multiple.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])
                order_cancel_multiple_count = db.list_collection_names().count("order_cancel_multiple")
                if(order_cancel_multiple_count < 1):
                    db.create_collection("order_cancel_multiple")
                else:
                    db.order_cancel_multiple.delete_many({})
                collection = db["order_cancel_multiple"]
                dict1 = order_cancel_multiple.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])
                order_multiple_count = db.list_collection_names().count("order_multiple")
                if(order_multiple_count < 1):
                    db.create_collection("order_multiple")
                else:
                    db.order_multiple.delete_many({})
                collection = db["order_multiple"]
                dict1 = order_multiple.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])
                order_reject_multiple_count = db.list_collection_names().count("order_reject_multiple")
                if(order_reject_multiple_count < 1):
                    db.create_collection("order_reject_multiple")
                else:
                    db.order_reject_multiple.delete_many({})
                collection = db["order_reject_multiple"]
                dict1 = order_reject_multiple.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])
                order_sqr_multiple_count = db.list_collection_names().count("order_sqr_multiple")
                if(order_sqr_multiple_count < 1):
                    db.create_collection("order_sqr_multiple")
                else:
                    db.order_sqr_multiple.delete_many({})
                collection = db["order_sqr_multiple"]
                dict1 = order_sqr_multiple.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])
                order_tobe_cancel_multiple_count = db.list_collection_names().count("order_tobe_cancel_multiple")
                if(order_tobe_cancel_multiple_count < 1):
                    db.create_collection("order_tobe_cancel_multiple")
                else:
                    db.order_tobe_cancel_multiple.delete_many({})
                collection = db["order_tobe_cancel_multiple"]
                dict1 = order_tobe_cancel_multiple.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])

            # for index, file in enumerate(file_list):
            #     #print(index+1, 'file downloaded : ', file['title'])
            #     counter=0
            #     while counter<10:
            #         try:
            #             df29 = file.GetContentFile(file['title'])
            #             counter=10
            #         except Exception as e:
            #             logger.error(e)
            #             counter=counter+1
            #     #csv_raw = StringIO(file.text)
            #     if(file['title'] =="order_multiple.txt"):
            #         order_multiple = pd.read_csv(file['title'])
            #     if(file['title'] =="order_sqr_multiple.txt"):
            #         order_sqr_multiple = pd.read_csv(file['title'])
            #     if(file['title'] =="order_reject_multiple.txt"):
            #         order_reject_multiple = pd.read_csv(file['title'])
            #     if(file['title'] =="order_cancel_multiple.txt"):
            #         order_cancel_multiple = pd.read_csv(file['title'])
            #     if(file['title'] =="order_pending_multiple.txt"):
            #         order_pending_multiple = pd.read_csv(file['title'])
            order_multiple_count = db.order_multiple.count_documents({})
            if(order_multiple_count > 0):
                collection = db["order_multiple"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_multiple = instruments1
            order_sqr_multiple_count = db.order_sqr_multiple.count_documents({})
            if(order_sqr_multiple_count > 0):
                collection = db["order_sqr_multiple"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_sqr_multiple = instruments1
            order_reject_multiple_count = db.order_reject_multiple.count_documents({})
            if(order_reject_multiple_count > 0):
                collection = db["order_reject_multiple"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_reject_multiple = instruments1
            order_cancel_multiple_count = db.order_cancel_multiple.count_documents({})
            if(order_cancel_multiple_count > 0):
                collection = db["order_cancel_multiple"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_cancel_multiple = instruments1
            order_pending_multiple_count = db.order_pending_multiple.count_documents({})
            if(order_pending_multiple_count > 0):
                collection = db["order_pending_multiple"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_pending_multiple = instruments1

            if len(order_multiple) > 0:
                print("order_multiple being checked"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                kl_del = []
                for kl in range(0,len(order_multiple)):
                    if order_multiple['buy_sell'][kl] == "SELL":
                        buy_sell="BUY"
                    else:
                        buy_sell="SELL"
                    order_leg = order_multiple_place(s,file_list,drive,order_multiple['current_signal'][kl],order_multiple['instrument_id'][kl],order_multiple['trading_symbol'][kl],order_multiple['price_when_order_placed'][kl],order_multiple.loc['qty'][kl],order_multiple['instru'][kl],order_multiple['fresh_position'][kl],order_multiple['leg'][kl],order_multiple['strike'][kl],order_multiple['contract'][kl],order_multiple['expiry'][kl],order_multiple['trade_id'][kl],0,buy_sell,retry_order,order_multiple['status'][kl],order_multiple['reject_count'][kl],order_multiple['cancel_count'][kl],0)
                    if order_leg == "close":
                        order_multiple.at[kl,"status"] = "close"
                        order_sqr_multiple.loc[len(order_sqr_multiple)] = order_multiple.iloc[kl]
                        kl_del.append(kl)
                    if status == "pending":
                        order_multiple.at[kl,"status"] = "pending"
                        order_pending_multiple.loc[len(order_pending_multiple)] = order_multiple.iloc[kl]
                        kl_del.append(kl)
                    if status == "cancelled": 
                        order_multiple.at[kl,"status"] = "cancelled"
                        order_cancel_multiple.loc[len(order_cancel_multiple)] = order_multiple.iloc[kl]
                    if status == "rejected":
                        order_multiple.at[kl,"status"] = "cancelled"
                        order_reject_multiple.loc[len(order_reject_multiple)] = order_reject_multiple.iloc[kl]
                order_multiple = order_multiple.drop(order_multiple.index[kl_del])
                # file_list = drive.ListFile({'q' : f"'{folder}' in parents and trashed=false"}).GetList()
                # ll = 0
                # kk = 0
                # mm = 0
                # nn = 0
                # tt = 0
                # dd = 0
                # for index, file in enumerate(file_list):
                #     if(file['title'] =="order_pending_multiple.txt"):
                #         kk = file['id']
                #     if(file['title'] =="order_cancel_multiple.txt"):
                #         ll = file['id']
                #     if(file['title'] =="order_multiple.txt"):
                #         mm = file['id']
                #     if(file['title'] =="order_reject_multiple.txt"):
                #         nn = file['id']
                #     if(file['title'] =="order_tobe_cancel_multiple.txt"):
                #         tt = file['id']
                #     if(file['title'] =="order_sqr_multiple.txt"):
                #         dd = file['id']
                # if bool(kk):
                #     order_pending_multiple.to_csv("order_pending_multiple.txt", index=False)
                #     update_file = drive.CreateFile({'id': kk})
                #     update_file.SetContentFile("order_pending_multiple.txt")
                #     update_file.Upload()
                # if bool(ll):
                #     order_cancel_multiple.to_csv("order_cancel_multiple.txt", index=False)
                #     update_file = drive.CreateFile({'id': ll})
                #     update_file.SetContentFile("order_cancel_multiple.txt")
                #     update_file.Upload()
                # else:
                #     order_cancel_multiple.to_csv("order_cancel_multiple.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_cancel_multiple.txt"})
                #     gfile.SetContentFile("order_cancel_multiple.txt")
                #     gfile.Upload()
                # if bool(mm):
                #     order_multiple.to_csv("order_multiple.txt", index=False)
                #     update_file = drive.CreateFile({'id': mm})
                #     update_file.SetContentFile("order_multiple.txt")
                #     update_file.Upload()
                # else:
                #     order_multiple.to_csv("order_multiple.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_multiple.txt"})
                #     gfile.SetContentFile("order_multiple.txt")
                #     gfile.Upload()
                # if bool(nn):
                #     order_reject_multiple.to_csv("order_reject_multiple.txt", index=False)
                #     update_file = drive.CreateFile({'id': nn})
                #     update_file.SetContentFile("order_reject_multiple.txt")
                #     update_file.Upload()
                # else:
                #     order_reject_multiple.to_csv("order_reject_multiple.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_reject_multiple.txt"})
                #     gfile.SetContentFile("order_reject_multiple.txt")
                #     gfile.Upload()
                # if bool(tt):
                #     order_tobe_cancel_multiple.to_csv("order_tobe_cancel_multiple.txt", index=False)
                #     update_file = drive.CreateFile({'id': tt})
                #     update_file.SetContentFile("order_tobe_cancel_multiple.txt")
                #     update_file.Upload()
                # else:
                #     order_tobe_cancel_multiple.to_csv("order_tobe_cancel_multiple.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_tobe_cancel_multiple.txt"})
                #     gfile.SetContentFile("order_tobe_cancel_multiple.txt")
                #     gfile.Upload()
                # if bool(dd):
                #     order_sqr_multiple.to_csv("order_sqr_multiple.txt", index=False)
                #     update_file = drive.CreateFile({'id': dd})
                #     update_file.SetContentFile("order_sqr_multiple.txt")
                #     update_file.Upload()
                # else:
                #     order_sqr_multiple.to_csv("order_sqr_multiple.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_sqr_multiple.txt"})
                #     gfile.SetContentFile("order_sqr_multiple.txt")
                #     gfile.Upload()
                order_pending_multiple_count = db.list_collection_names().count("order_pending_multiple")
                if(order_pending_multiple_count < 1):
                    db.create_collection("order_pending_multiple")
                else:
                    db.order_pending_multiple.delete_many({})
                collection = db["order_pending_multiple"]
                dict1 = order_pending_multiple.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])
                order_cancel_multiple_count = db.list_collection_names().count("order_cancel_multiple")
                if(order_cancel_multiple_count < 1):
                    db.create_collection("order_cancel_multiple")
                else:
                    db.order_cancel_multiple.delete_many({})
                collection = db["order_cancel_multiple"]
                dict1 = order_cancel_multiple.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])
                order_multiple_count = db.list_collection_names().count("order_multiple")
                if(order_multiple_count < 1):
                    db.create_collection("order_multiple")
                else:
                    db.order_multiple.delete_many({})
                collection = db["order_multiple"]
                dict1 = order_multiple.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])
                order_reject_multiple_count = db.list_collection_names().count("order_reject_multiple")
                if(order_reject_multiple_count < 1):
                    db.create_collection("order_reject_multiple")
                else:
                    db.order_reject_multiple.delete_many({})
                collection = db["order_reject_multiple"]
                dict1 = order_reject_multiple.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])
                order_sqr_multiple_count = db.list_collection_names().count("order_sqr_multiple")
                if(order_sqr_multiple_count < 1):
                    db.create_collection("order_sqr_multiple")
                else:
                    db.order_sqr_multiple.delete_many({})
                collection = db["order_sqr_multiple"]
                dict1 = order_sqr_multiple.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])
                order_tobe_cancel_multiple_count = db.list_collection_names().count("order_tobe_cancel_multiple")
                if(order_tobe_cancel_multiple_count < 1):
                    db.create_collection("order_tobe_cancel_multiple")
                else:
                    db.order_tobe_cancel_multiple.delete_many({})
                collection = db["order_tobe_cancel_multiple"]
                dict1 = order_tobe_cancel_multiple.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])

            # counter=0
            # while counter<10:
            #     try:
            #         file_list = drive.ListFile({'q' : f"'{folder}' in parents and trashed=false"}).GetList()
            #         counter=10
            #     except Exception as e:
            #         logger.error(e)
            #         counter=counter+1

            # # file_list = drive.ListFile({'q' : f"'{folder}' in parents and trashed=false"}).GetList()
            # for index, file in enumerate(file_list):
            #     #print(index+1, 'file downloaded : ', file['title'])
            #     counter=0
            #     while counter<10:
            #         try:
            #             df29 = file.GetContentFile(file['title'])
            #             counter=10
            #         except Exception as e:
            #             logger.error(e)
            #             counter=counter+1
            #     #csv_raw = StringIO(file.text)
            #     if(file['title'] =="order_pending_tobe_cancel.txt"):
            #         order_pending_tobe_cancel = pd.read_csv(file['title'])
            #     if(file['title'] =="order_pending_complete_tobe_cancel.txt"):
            #         order_pending_complete_tobe_cancel = pd.read_csv(file['title'])
            #     if(file['title'] =="order_cancel.txt"):
            #         order_cancel = pd.read_csv(file['title'])
            #     if(file['title'] =="order_cancel_complete.txt"):
            #         order_cancel_complete = pd.read_csv(file['title'])

            order_pending_complete_tobe_cancel_count = db.order_pending_complete_tobe_cancel.count_documents({})
            if(order_pending_complete_tobe_cancel_count > 0):
                collection = db["order_pending_complete_tobe_cancel"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_pending_complete_tobe_cancel = instruments1

            order_pending_tobe_cancel_count = db.order_pending_tobe_cancel.count_documents({})
            if(order_pending_tobe_cancel_count > 0):
                collection = db["order_pending_tobe_cancel"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_pending_tobe_cancel = instruments1
            order_cancel_count = db.order_cancel.count_documents({})
            if(order_cancel_count > 0):
                collection = db["order_cancel"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_cancel = instruments1
            order_cancel_complete_count = db.order_cancel_complete.count_documents({})
            if(order_cancel_complete_count > 0):
                collection = db["order_cancel_complete"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_cancel_complete = instruments1

            if len(order_pending_tobe_cancel) > 0:
                print("order_pending_tobe_cancel being checked"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                kl_del = []
                for kl in range(0,len(order_pending_tobe_cancel)):
                    status22 = ''
                    status22 = order_cancel_place(s,order_pending_tobe_cancel['order_id'][kl],"")
                    if status22 == "cancelled":
                        order_cancel.loc[len(order_cancel)] = order_pending_tobe_cancel.iloc[kl]
                        kl_del.append(kl)
                order_pending_tobe_cancel = order_pending_tobe_cancel.drop(order_pending_tobe_cancel.index[kl_del])
                # file_list = drive.ListFile({'q' : f"'{folder}' in parents and trashed=false"}).GetList()
                # kk = 0
                # mm = 0
                # for index, file in enumerate(file_list):
                #     if(file['title'] =="order_pending_tobe_cancel.txt"):
                #         kk = file['id']
                #     if(file['title'] =="order_cancel.txt"):
                #         mm = file['id']
                # if bool(kk):
                #     order_pending_tobe_cancel.to_csv("order_pending_tobe_cancel.txt", index=False)
                #     update_file = drive.CreateFile({'id': kk})
                #     update_file.SetContentFile("order_pending_tobe_cancel.txt")
                #     update_file.Upload()
                # else:
                #     order_pending_tobe_cancel.to_csv("order_pending_tobe_cancel.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_pending_tobe_cancel.txt"})
                #     gfile.SetContentFile("order_pending_tobe_cancel.txt")
                #     gfile.Upload()
                # if bool(mm):
                #     order_cancel.to_csv("order_cancel.txt", index=False)
                #     update_file = drive.CreateFile({'id': mm})
                #     update_file.SetContentFile("order_cancel.txt")
                #     update_file.Upload()
                # else:
                #     order_cancel.to_csv("order_cancel.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_cancel.txt"})
                #     gfile.SetContentFile("order_cancel.txt")
                #     gfile.Upload()
                order_pending_tobe_cancel_count = db.list_collection_names().count("order_pending_tobe_cancel")
                if(order_pending_tobe_cancel_count < 1):
                    db.create_collection("order_pending_tobe_cancel")
                else:
                    db.order_pending_tobe_cancel.delete_many({})
                collection = db["order_pending_tobe_cancel"]
                dict1 = order_pending_tobe_cancel.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])

                order_cancel_count = db.list_collection_names().count("order_cancel")
                if(order_cancel_count < 1):
                    db.create_collection("order_cancel")
                else:
                    db.order_cancel.delete_many({})
                collection = db["order_cancel"]
                dict1 = order_cancel.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])

            if len(order_pending_complete_tobe_cancel) > 0:
                print("order_pending_tobe_cancel being checked"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                kl_del = []
                for kl in range(0,len(order_pending_complete_tobe_cancel)):
                    status22 = ''
                    status22 = order_cancel_place(s,order_pending_complete_tobe_cancel['order_id'][kl],"")
                    if status22 == "cancelled":
                        order_cancel_complete.loc[len(order_cancel_complete)] = order_pending_complete_tobe_cancel.iloc[kl]
                        kl_del.append(kl)
                order_pending_complete_tobe_cancel = order_pending_complete_tobe_cancel.drop(order_pending_complete_tobe_cancel.index[kl_del])

                # file_list = drive.ListFile({'q' : f"'{folder}' in parents and trashed=false"}).GetList()
                # ll = 0
                # nn = 0
                # for index, file in enumerate(file_list):
                #     if(file['title'] =="order_pending_complete_tobe_cancel.txt"):
                #         ll = file['id']
                #     if(file['title'] =="order_cancel_complete.txt"):
                #         nn = file['id']
                # if bool(ll):
                #     order_pending_complete_tobe_cancel.to_csv("order_pending_complete_tobe_cancel.txt", index=False)
                #     update_file = drive.CreateFile({'id': ll})
                #     update_file.SetContentFile("order_pending_complete_tobe_cancel.txt")
                #     update_file.Upload()
                # else:
                #     order_pending_complete_tobe_cancel.to_csv("order_pending_complete_tobe_cancel.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_pending_complete_tobe_cancel.txt"})
                #     gfile.SetContentFile("order_pending_complete_tobe_cancel.txt")
                #     gfile.Upload()
                # if bool(nn):
                #     order_cancel_complete.to_csv("order_cancel_complete.txt", index=False)
                #     update_file = drive.CreateFile({'id': nn})
                #     update_file.SetContentFile("order_cancel_complete.txt")
                #     update_file.Upload()
                # else:
                #     order_cancel_complete.to_csv("order_cancel_complete.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_cancel_complete.txt"})
                #     gfile.SetContentFile("order_cancel_complete.txt")
                #     gfile.Upload()
                order_pending_complete_tobe_cancel_count = db.list_collection_names().count("order_pending_complete_tobe_cancel")
                if(order_pending_complete_tobe_cancel_count < 1):
                    db.create_collection("order_pending_complete_tobe_cancel")
                else:
                    db.order_pending_complete_tobe_cancel.delete_many({})
                collection = db["order_pending_complete_tobe_cancel"]
                dict1 = order_pending_complete_tobe_cancel.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])

                order_cancel_complete_count = db.list_collection_names().count("order_cancel_complete")
                if(order_cancel_complete_count < 1):
                    db.create_collection("order_cancel_complete")
                else:
                    db.order_cancel_complete.delete_many({})
                collection = db["order_cancel_complete"]
                dict1 = order_cancel_complete.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])

            # counter=0
            # while counter<10:
            #     try:
            #         file_list = drive.ListFile({'q' : f"'{folder}' in parents and trashed=false"}).GetList()
            #         counter=10
            #     except Exception as e:
            #         logger.error(e)
            #         counter=counter+1

            # file_list = drive.ListFile({'q' : f"'{folder}' in parents and trashed=false"}).GetList()
            # for index, file in enumerate(file_list):
            #     #print(index+1, 'file downloaded : ', file['title'])
            #     counter=0
            #     while counter<10:
            #         try:
            #             df29 = file.GetContentFile(file['title'])
            #             counter=10
            #         except Exception as e:
            #             logger.error(e)
            #             counter=counter+1
            #     #csv_raw = StringIO(file.text)
            #     if(file['title'] =="order_complete.txt"):
            #         order_complete = pd.read_csv(file['title'])
            #     if(file['title'] =="order_manage.txt"):
            #         order_manage = pd.read_csv(file['title'])
            #     if(file['title'] =="order_pending.txt"):
            #         order_pending = pd.read_csv(file['title'])
            #     if(file['title'] =="order_pending_complete.txt"):
            #         order_pending_complete = pd.read_csv(file['title'])
            #     if(file['title'] =="order_tobe_exec_log.txt"):
            #         order_tobe_exec_log = pd.read_csv(file['title'])
            #     if(file['title'] =="order_tobe_sqr_complete.txt"):
            #         order_tobe_sqr_complete = pd.read_csv(file['title'])
            #     if(file['title'] =="order_tobe_cancel.txt"):
            #         order_tobe_cancel = pd.read_csv(file['title'])
            #     if(file['title'] =="order_tobe_cancel_multiple.txt"):
            #         order_tobe_cancel_multiple = pd.read_csv(file['title'])
            #     if(file['title'] =="order_tobe_cancel_complete.txt"):
            #         order_tobe_cancel_complete = pd.read_csv(file['title'])
            #     if(file['title'] =="order_pending_tobe_cancel.txt"):
            #         order_pending_tobe_cancel = pd.read_csv(file['title'])
            #     if(file['title'] =="order_pending_complete_tobe_cancel.txt"):
            #         order_pending_complete_tobe_cancel = pd.read_csv(file['title'])
            #     if(file['title'] =="order_multiple.txt"):
            #         order_multiple = pd.read_csv(file['title'])
            order_tobe_cancel_count = db.order_tobe_cancel.count_documents({})
            if(order_tobe_cancel_count > 0):
                collection = db["order_tobe_cancel"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_tobe_cancel = instruments1
            order_tobe_cancel_complete_count = db.order_tobe_cancel_complete.count_documents({})
            if(order_tobe_cancel_complete_count > 0):
                collection = db["order_tobe_cancel_complete"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_tobe_cancel_complete = instruments1
            order_manage_count = db.order_manage.count_documents({})
            if(order_manage_count > 0):
                collection = db["order_manage"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_manage = instruments1
            order_reject_count = db.order_reject.count_documents({})
            if(order_reject_count > 0):
                collection = db["order_reject"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_reject = instruments1
            order_pending_count = db.order_pending.count_documents({})
            if(order_pending_count > 0):
                collection = db["order_pending"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_pending = instruments1
            order_complete_count = db.order_complete.count_documents({})
            if(order_complete_count > 0):
                collection = db["order_complete"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_complete = instruments1
            order_pending_complete_count = db.order_pending_complete.count_documents({})
            if(order_pending_complete_count > 0):
                collection = db["order_pending_complete"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_pending_complete = instruments1
            order_multiple_count = db.order_multiple.count_documents({})
            if(order_multiple_count > 0):
                collection = db["order_multiple"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_multiple = instruments1
            order_tobe_cancel_count = db.order_tobe_cancel.count_documents({})
            if(order_tobe_cancel_count > 0):
                collection = db["order_tobe_cancel"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_tobe_cancel = instruments1
            order_tobe_exec_log_count = db.order_tobe_exec_log.count_documents({})
            if(order_tobe_exec_log_count > 0):
                collection = db["order_tobe_exec_log"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_tobe_exec_log = instruments1
            order_tobe_sqr_complete_count = db.order_tobe_sqr_complete.count_documents({})
            if(order_tobe_sqr_complete_count > 0):
                collection = db["order_tobe_sqr_complete"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_tobe_sqr_complete = instruments1
            order_tobe_cancel_multiple_count = db.order_tobe_cancel_multiple.count_documents({})
            if(order_tobe_cancel_multiple_count > 0):
                collection = db["order_tobe_cancel_multiple"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_tobe_cancel_multiple = instruments1
            order_tobe_cancel_complete_count = db.order_tobe_cancel_complete.count_documents({})
            if(order_tobe_cancel_complete_count > 0):
                collection = db["order_tobe_cancel_complete"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_tobe_cancel_complete = instruments1
            order_pending_tobe_cancel_count = db.order_pending_tobe_cancel.count_documents({})
            if(order_pending_tobe_cancel_count > 0):
                collection = db["order_pending_tobe_cancel"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_pending_tobe_cancel = instruments1
            order_pending_complete_tobe_cancel_count = db.order_pending_complete_tobe_cancel.count_documents({})
            if(order_pending_complete_tobe_cancel_count > 0):
                collection = db["order_pending_complete_tobe_cancel"]
                x = collection.find()
                instruments1 = pd.DataFrame()
                temp = pd.DataFrame()
                for y in x:
                    jj = pd.DataFrame([y])
                    instruments1 = pd.concat([temp,jj])
                    temp = instruments1
                # order_tobe_cancel = pd.read_csv(file['title'])
                order_pending_complete_tobe_cancel = instruments1

            if len(order_tobe_exec_log) > 0 and len(order_manage) < 1 and len(order_pending_complete) < 1 and len(order_pending) < 1 and len(order_tobe_sqr_complete) < 1 and len(order_pending_tobe_cancel) < 1 and len(order_pending_complete_tobe_cancel) < 1:
                kkl = 0
                print("order_tobe_exec_log being checked"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                for kl in range(0,len(order_tobe_exec_log)):
                    if order_tobe_exec_log['leg'][kl] != "leg1":
                        order_leg = order_place(s,file_list,drive,order_tobe_exec_log['current_signal'][kl],order_tobe_exec_log['instrument_id'][kl],order_tobe_exec_log['trading_symbol'][kl],order_tobe_exec_log['price_when_order_placed'][kl],order_tobe_exec_log['qty'][kl],order_tobe_exec_log['instru'][kl],order_tobe_exec_log['fresh_position'][kl],order_tobe_exec_log['leg'][kl],order_tobe_exec_log['strike'][kl],order_tobe_exec_log['contract'][kl],order_tobe_exec_log['expiry'][kl],order_tobe_exec_log['trade_id'][kl],0,order_tobe_exec_log['buy_sell'][kl],retry_order,order_tobe_exec_log['status'][kl],order_tobe_exec_log['reject_count'][kl],order_tobe_exec_log['cancel_count'][kl])
                        if order_leg and order_tobe_exec_log['leg'][kl] == "leg2":
                            kkl = order_tobe_exec_log.index[(order_tobe_exec_log.leg=="leg1")].values[0]
                            order_leg1 = order_place(s,file_list,drive,order_tobe_exec_log['current_signal'][kkl],order_tobe_exec_log['instrument_id'][kkl],order_tobe_exec_log['trading_symbol'][kkl],order_tobe_exec_log['price_when_order_placed'][kkl],order_tobe_exec_log['qty'][kkl],order_tobe_exec_log['instru'][kkl],order_tobe_exec_log['fresh_position'][kkl],"leg2",order_tobe_exec_log['strike'][kkl],order_tobe_exec_log['contract'][kkl],order_tobe_exec_log['expiry'][kkl],order_tobe_exec_log['trade_id'][kkl],0,order_tobe_exec_log['buy_sell'][kkl],retry_order,order_tobe_exec_log['status'][kkl],order_tobe_exec_log['reject_count'][kkl],order_tobe_exec_log['cancel_count'][kkl])
#                         else:
#                             # order_tobe_exec_log.loc[(order_tobe_exec_log.leg=="leg2"),kl] = ""
#                             order_manage.loc[len(order_manage)] = order_tobe_exec_log.iloc[kl]
#                             if kkl:
#                                 order_manage.loc[len(order_manage)] = order_tobe_exec_log.iloc[kkl]
#                                 kkl = 0
#                         order_tobe_exec_log = order_tobe_exec_log.drop(kl)
#                         order_m.loc[len(order_complete)] = order_tobe_exec_log.iloc[kl]
                order_tobe_exec_log = order_tobe_exec_log[0:0]
                # ll = 0
                # for index, file in enumerate(file_list):
                #     if(file['title'] =="order_tobe_exec_log.txt"):
                #         ll = file['id']
                # if bool(ll):
                #     order_tobe_exec_log.to_csv("order_tobe_exec_log.txt", index=False)
                #     update_file = drive.CreateFile({'id': ll})
                #     update_file.SetContentFile("order_tobe_exec_log.txt")
                #     update_file.Upload()
                # else:
                #     order_tobe_exec_log.to_csv("order_tobe_exec_log.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_tobe_exec_log.txt"})
                #     gfile.SetContentFile("order_tobe_exec_log.txt")
                #     gfile.Upload()
                order_tobe_exec_log_count = db.list_collection_names().count("order_tobe_exec_log")
                if(order_tobe_exec_log_count < 1):
                    db.create_collection("order_tobe_exec_log")
                else:
                    db.order_tobe_exec_log.delete_many({})
                collection = db["order_tobe_exec_log"]
                dict1 = order_tobe_exec_log.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])

            if len(order_tobe_cancel) > 0:
                print("order_tobe_cancel being checked"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                kl_del = []
                for kl in range(0,len(order_tobe_cancel)):
                    status22 = ''
                    status22 = order_cancel_place(s,order_tobe_cancel['order_id'][kl],"")
                    if status22 == "cancelled":
                        order_cancel.loc[len(order_cancel)] = order_tobe_cancel.iloc[kl]
                        kl_del.append(kl)
                order_tobe_cancel = order_tobe_cancel.drop(order_tobe_cancel.index[kl_del])
                # ll = 0
                # mm = 0
                # for index, file in enumerate(file_list):
                #     if(file['title'] =="order_tobe_cancel.txt"):
                #         ll = file['id']
                #     if(file['title'] =="order_cancel.txt"):
                #         mm = file['id']
                # if bool(ll):
                #     order_tobe_cancel.to_csv("order_tobe_cancel.txt", index=False)
                #     update_file = drive.CreateFile({'id': ll})
                #     update_file.SetContentFile("order_tobe_cancel.txt")
                #     update_file.Upload()
                # else:
                #     order_tobe_cancel.to_csv("order_tobe_cancel.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_tobe_cancel.txt"})
                #     gfile.SetContentFile("order_tobe_cancel.txt")
                #     gfile.Upload()
                # if bool(mm):
                #     order_cancel.to_csv("order_cancel.txt", index=False)
                #     update_file = drive.CreateFile({'id': ll})
                #     update_file.SetContentFile("order_cancel.txt")
                #     update_file.Upload()
                # else:
                #     order_cancel.to_csv("order_cancel.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_cancel.txt"})
                #     gfile.SetContentFile("order_cancel.txt")
                #     gfile.Upload()

                order_tobe_cancel_count = db.list_collection_names().count("order_tobe_cancel")
                if(order_tobe_cancel_count < 1):
                    db.create_collection("order_tobe_cancel")
                else:
                    db.order_tobe_cancel.delete_many({})
                collection = db["order_tobe_cancel"]
                dict1 = order_tobe_cancel.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])

                order_cancel_count = db.list_collection_names().count("order_cancel")
                if(order_cancel_count < 1):
                    db.create_collection("order_cancel")
                else:
                    db.order_cancel.delete_many({})
                collection = db["order_cancel"]
                dict1 = order_cancel.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])

            if len(order_tobe_cancel_complete) > 0:
                print("order_tobe_cancel_complete being checked"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                kl_del = []
                for kl in range(0,len(order_tobe_cancel_complete)):
                    status22 = ''
                    status22 = order_cancel_place(s,order_tobe_cancel_complete['order_id'][kl],"")
                    if status22 == "cancelled":
                        order_cancel_complete.loc[len(order_cancel_complete)] = order_tobe_cancel_complete.iloc[kl]
                        kl_del.append(kl)
                order_tobe_cancel_complete = order_tobe_cancel_complete.drop(order_tobe_cancel_complete.index[kl_del])
                # ll = 0
                # nn = 0
                # for index, file in enumerate(file_list):
                #     if(file['title'] =="order_tobe_cancel_complete.txt"):
                #         ll = file['id']
                #     if(file['title'] =="order_cancel_complete.txt"):
                #         nn = file['id']
                # if bool(ll):
                #     order_tobe_cancel_complete.to_csv("order_tobe_cancel_complete.txt", index=False)
                #     update_file = drive.CreateFile({'id': ll})
                #     update_file.SetContentFile("order_tobe_cancel_complete.txt")
                #     update_file.Upload()
                # else:
                #     order_tobe_cancel_complete.to_csv("order_tobe_cancel_complete.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_tobe_cancel_complete.txt"})
                #     gfile.SetContentFile("order_tobe_cancel_complete.txt")
                #     gfile.Upload()
                # if bool(nn):
                #     order_cancel_complete.to_csv("order_cancel_complete.txt", index=False)
                #     update_file = drive.CreateFile({'id': nn})
                #     update_file.SetContentFile("order_cancel_complete.txt")
                #     update_file.Upload()
                # else:
                #     order_cancel_complete.to_csv("order_cancel_complete.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_cancel_complete.txt"})
                #     gfile.SetContentFile("order_cancel_complete.txt")
                #     gfile.Upload()

                order_tobe_cancel_complete_count = db.list_collection_names().count("order_tobe_cancel_complete")
                if(order_tobe_cancel_complete_count < 1):
                    db.create_collection("order_tobe_cancel_complete")
                else:
                    db.order_tobe_cancel_complete.delete_many({})
                collection = db["order_tobe_cancel_complete"]
                dict1 = order_tobe_cancel_complete.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])

                order_cancel_complete_count = db.list_collection_names().count("order_cancel_complete")
                if(order_cancel_complete_count < 1):
                    db.create_collection("order_cancel_complete")
                else:
                    db.order_cancel_complete.delete_many({})
                collection = db["order_cancel_complete"]
                dict1 = order_cancel_complete.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])

            if len(order_tobe_cancel_multiple) > 0:
                print("order_tobe_cancel_multiple being checked"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                kl_del = []
                for kl in range(0,len(order_tobe_cancel_multiple)):
                    status22 = ''
                    status22 = order_cancel_place(s,order_tobe_cancel_multiple['order_id'][kl],"")
                    if status22 == "cancelled":
                        order_cancel_multiple.loc[len(order_cancel_multiple)] = order_tobe_cancel_multiple.iloc[kl]
                        kl_del.append(kl)
                order_tobe_cancel_multiple = order_tobe_cancel_multiple.drop(order_tobe_cancel_multiple.index[kl_del])
                # ll = 0
                # nn = 0
                # for index, file in enumerate(file_list):
                #     if(file['title'] =="order_tobe_cancel_multiple.txt"):
                #         ll = file['id']
                #     if(file['title'] =="order_cancel_multiple.txt"):
                #         nn = file['id']
                # if bool(ll):
                #     order_tobe_cancel_multiple.to_csv("order_tobe_cancel_multiple.txt", index=False)
                #     update_file = drive.CreateFile({'id': ll})
                #     update_file.SetContentFile("order_tobe_cancel_multiple.txt")
                #     update_file.Upload()
                # else:
                #     order_tobe_cancel_multiple.to_csv("order_tobe_cancel_multiple.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_tobe_cancel_multiple.txt"})
                #     gfile.SetContentFile("order_tobe_cancel_multiple.txt")
                #     gfile.Upload()
                # if bool(nn):
                #     order_cancel_multiple.to_csv("order_cancel_multiple.txt", index=False)
                #     update_file = drive.CreateFile({'id': nn})
                #     update_file.SetContentFile("order_cancel_multiple.txt")
                #     update_file.Upload()
                # else:
                #     order_cancel_multiple.to_csv("order_cancel_multiple.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_cancel_multiple.txt"})
                #     gfile.SetContentFile("order_cancel_multiple.txt")
                #     gfile.Upload()
                order_tobe_cancel_multiple_count = db.list_collection_names().count("order_tobe_cancel_multiple")
                if(order_tobe_cancel_multiple_count < 1):
                    db.create_collection("order_tobe_cancel_multiple")
                else:
                    db.order_tobe_cancel_multiple.delete_many({})
                collection = db["order_tobe_cancel_multiple"]
                dict1 = order_tobe_cancel_multiple.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])

                order_cancel_multiple_count = db.list_collection_names().count("order_cancel_multiple")
                if(order_cancel_multiple_count < 1):
                    db.create_collection("order_cancel_multiple")
                else:
                    db.order_cancel_multiple.delete_many({})
                collection = db["order_cancel_multiple"]
                dict1 = order_cancel_multiple.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])

            if len(order_complete) > 0:
                print("order_complete being checked for duplicate"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                df_order_complete = order_complete.loc[order_complete.duplicated('instrument_id'),:]
                if len(df_order_complete) > 0:
                    order_complete = order_complete.drop_duplicates('instrument_id')
                    for kl in range(0,len(df_order_complete)):
                        order_multiple.loc[len(order_multiple)] = df_order_complete.iloc[kl]
                # ll = 0
                # nn = 0
                # for index, file in enumerate(file_list):
                #     if(file['title'] =="order_complete.txt"):
                #         ll = file['id']
                #     if(file['title'] =="order_multiple.txt"):
                #         nn = file['id']
                # if bool(ll):
                #     order_complete.to_csv("order_complete.txt", index=False)
                #     update_file = drive.CreateFile({'id': ll})
                #     update_file.SetContentFile("order_complete.txt")
                #     update_file.Upload()
                # else:
                #     order_complete.to_csv("order_complete.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_complete.txt"})
                #     gfile.SetContentFile("order_complete.txt")
                #     gfile.Upload()
                # if bool(nn):
                #     order_multiple.to_csv("order_multiple.txt", index=False)
                #     update_file = drive.CreateFile({'id': nn})
                #     update_file.SetContentFile("order_multiple.txt")
                #     update_file.Upload()
                # else:
                #     order_multiple.to_csv("order_multiple.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_multiple.txt"})
                #     gfile.SetContentFile("order_multiple.txt")
                #     gfile.Upload()
                order_complete_count = db.list_collection_names().count("order_complete")
                if(order_complete_count < 1):
                    db.create_collection("order_complete")
                else:
                    db.order_complete.delete_many({})
                collection = db["order_complete"]
                dict1 = order_complete.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])

                order_multiple_count = db.list_collection_names().count("order_multiple")
                if(order_multiple_count < 1):
                    db.create_collection("order_multiple")
                else:
                    db.order_multiple.delete_many({})
                collection = db["order_multiple"]
                dict1 = order_multiple.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])

            if len(order_pending) > 0:
                print("order_pending being checked for duplicate"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                df_order_pending = order_pending.loc[order_pending.duplicated('instrument_id'),:]
                if len(df_order_pending) > 0:
                    order_pending = order_pending.drop_duplicates('instrument_id')
                    for kl in range(0,len(df_order_pending)):
                        order_tobe_cancel.loc[len(order_tobe_cancel)] = df_order_pending.iloc[kl]
                # ll = 0
                # nn = 0
                # for index, file in enumerate(file_list):
                #     if(file['title'] =="order_pending.txt"):
                #         ll = file['id']
                #     if(file['title'] =="order_tobe_cancel.txt"):
                #         nn = file['id']
                # if bool(ll):
                #     order_pending.to_csv("order_pending.txt", index=False)
                #     update_file = drive.CreateFile({'id': ll})
                #     update_file.SetContentFile("order_pending.txt")
                #     update_file.Upload()
                # else:
                #     order_pending.to_csv("order_pending.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_pending.txt"})
                #     gfile.SetContentFile("order_pending.txt")
                #     gfile.Upload()
                # if bool(nn):
                #     order_tobe_cancel.to_csv("order_tobe_cancel.txt", index=False)
                #     update_file = drive.CreateFile({'id': nn})
                #     update_file.SetContentFile("order_tobe_cancel.txt")
                #     update_file.Upload()
                # else:
                #     order_tobe_cancel.to_csv("order_tobe_cancel.txt", index=False)
                #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_tobe_cancel.txt"})
                #     gfile.SetContentFile("order_tobe_cancel.txt")
                #     gfile.Upload()
                order_pending_count = db.list_collection_names().count("order_pending")
                if(order_pending_count < 1):
                    db.create_collection("order_pending")
                else:
                    db.order_pending.delete_many({})
                collection = db["order_pending"]
                dict1 = order_pending.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])

                order_tobe_cancel_count = db.list_collection_names().count("order_tobe_cancel")
                if(order_tobe_cancel_count < 1):
                    db.create_collection("order_tobe_cancel")
                else:
                    db.order_tobe_cancel.delete_many({})
                collection = db["order_tobe_cancel"]
                dict1 = order_tobe_cancel.to_dict('records')
                for x in range(0,len(dict1)):
                    collection.insert_one(dict1[x])

#                         order_leg2 = order_place(s,file_list,drive,df30.loc[df30['leg']== "leg2",'current_signal'].values[0],df30.loc[df30['leg']== "leg2",'instrument_id'].values[0],df30.loc[df30['leg']== "leg2",'trading_symbol'].values[0],df30.loc[df30['leg']== "leg2",'price_when_order_placed'].values[0],df30.loc[df30['leg']== "leg2",'qty'].values[0],df30.loc[df30['leg']== "leg2",'instru'].values[0],df30.loc[df30['leg']== "leg2",'fresh_position'].values[0],"leg2",df30.loc[df30['leg']== "leg2",'strike'].values[0],df30.loc[df30['leg']== "leg2",'contract'].values[0],df30.loc[df30['leg']== "leg2",'expiry'].values[0],df30.loc[df30['leg']== "leg2",order_manage.loc[order_manage['leg']== "leg1",'trade_id'].values[0],0,'buy_sell'].values[0],retry_order,order_manage.loc[order_manage['leg']== "leg2",'status'].values[0])
#                         if not order_leg2:
#                             order_leg1 = order_place(s,file_list,drive,df30.loc[df30['leg']== "leg1",'current_signal'].values[0],df30.loc[df30['leg']== "leg1",'instrument_id'].values[0],df30.loc[df30['leg']== "leg1",'trading_symbol'].values[0],df30.loc[df30['leg']== "leg1",'price_when_order_placed'].values[0],df30.loc[df30['leg']== "leg1",'qty'].values[0],df30.loc[df30['leg']== "leg1",'instru'].values[0],df30.loc[df30['leg']== "leg1",'fresh_position'].values[0],"leg1",df30.loc[df30['leg']== "leg1",'strike'].values[0],df30.loc[df30['leg']== "leg1",'contract'].values[0],df30.loc[df30['leg']== "leg1",'expiry'].values[0],df30.loc[df30['leg']== "leg1",order_manage.loc[order_manage['leg']== "leg1",'trade_id'].values[0],0,'buy_sell'].values[0],retry_order,order_manage.loc[order_manage['leg']== "leg1",'status'].values[0])
#                     order_leg3 = order_place(s,file_list,drive,df30.loc[df30['leg']== "leg3",'current_signal'].values[0],df30.loc[df30['leg']== "leg3",'instrument_id'].values[0],df30.loc[df30['leg']== "leg3",'trading_symbol'].values[0],df30.loc[df30['leg']== "leg3",'price_when_order_placed'].values[0],df30.loc[df30['leg']== "leg3",'qty'].values[0],df30.loc[df30['leg']== "leg3",'instru'].values[0],df30.loc[df30['leg']== "leg3",'fresh_position'].values[0],"leg3",df30.loc[df30['leg']== "leg3",'strike'].values[0],df30.loc[df30['leg']== "leg3",'contract'].values[0],df30.loc[df30['leg']== "leg3",'expiry'].values[0],order_manage.loc[order_manage['leg']== "leg1",'trade_id'].values[0],0,df30.loc[df30['leg']== "leg3",'buy_sell'].values[0],retry_order,order_manage.loc[order_manage['leg']== "leg3",'status'].values[0])
#                     order_leg4 = order_place(s,file_list,drive,df30.loc[df30['leg']== "leg4",'current_signal'].values[0],df30.loc[df30['leg']== "leg4",'instrument_id'].values[0],df30.loc[df30['leg']== "leg4",'trading_symbol'].values[0],df30.loc[df30['leg']== "leg4",'price_when_order_placed'].values[0],df30.loc[df30['leg']== "leg4",'qty'].values[0],df30.loc[df30['leg']== "leg4",'instru'].values[0],df30.loc[df30['leg']== "leg4",'fresh_position'].values[0],"leg4",df30.loc[df30['leg']== "leg4",'strike'].values[0],df30.loc[df30['leg']== "leg4",'contract'].values[0],df30.loc[df30['leg']== "leg4",'expiry'].values[0],order_manage.loc[order_manage['leg']== "leg1",'trade_id'].values[0],0,df30.loc[df30['leg']== "leg4",'buy_sell'].values[0],retry_order,order_manage.loc[order_manage['leg']== "leg4",'status'].values[0])
                    
            if (not currently_buy_holding and not currently_sell_holding) or (currently_buy_holding) or (currently_sell_holding):
#             if False:
                requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': "running "+str(current_time.date())+" "+str(current_time.time())})
                df99=pd.DataFrame()
#                 df99 = response
                # file_list = drive.ListFile({'q' : f"'{folder}' in parents and trashed=false"}).GetList()
                # for index, file in enumerate(file_list):
                #     #print(index+1, 'file downloaded : ', file['title'])
                #     counter=0
                #     while counter<10:
                #         try:
                #             df29 = file.GetContentFile(file['title'])
                #             counter=10
                #         except Exception as e:
                #             logger.error(e)
                #             counter=counter+1
                #     #csv_raw = StringIO(file.text)
                #     if(file['title'] =="order_manage.txt"):
                #         order_manage = pd.read_csv(file['title'])
                #     if(file['title'] =="order_complete.txt"):
                #         order_complete = pd.read_csv(file['title'])
                #     if(file['title'] =="order_pending.txt"):
                #         order_pending = pd.read_csv(file['title'])
                #     if(file['title'] =="order_pending_complete.txt"):
                #         order_pending_complete = pd.read_csv(file['title'])
                #     if(file['title'] =="order_tobe_sqr_complete.txt"):
                #         order_tobe_sqr_complete = pd.read_csv(file['title'])
                #     if(file['title'] =="order_tobe_exec_log.txt"):
                #         order_tobe_exec_log = pd.read_csv(file['title'])
                #     if(file['title'] =="order_cancel.txt"):
                #         order_cancel = pd.read_csv(file['title'])
                #     if(file['title'] =="order_cancel_complete.txt"):
                #         order_cancel_complete = pd.read_csv(file['title'])
                order_cancel_complete_count = db.list_collection_names().count("order_cancel_complete")
                if(order_cancel_complete_count > 0):
                    collection = db["order_cancel_complete"]
                    x = collection.find()
                    instruments1 = pd.DataFrame()
                    temp = pd.DataFrame()
                    for y in x:
                        jj = pd.DataFrame([y])
                        instruments1 = pd.concat([temp,jj])
                        temp = instruments1
                    # order_tobe_cancel = pd.read_csv(file['title'])
                    order_cancel_complete = instruments1

                order_tobe_exec_log_count = db.list_collection_names().count("order_tobe_exec_log")
                if(order_tobe_exec_log_count > 0):
                    collection = db["order_tobe_exec_log"]
                    x = collection.find()
                    instruments1 = pd.DataFrame()
                    temp = pd.DataFrame()
                    for y in x:
                        jj = pd.DataFrame([y])
                        instruments1 = pd.concat([temp,jj])
                        temp = instruments1
                    # order_tobe_cancel = pd.read_csv(file['title'])
                    order_tobe_exec_log = instruments1
                order_tobe_sqr_complete_count = db.list_collection_names().count("order_tobe_sqr_complete")
                if(order_tobe_sqr_complete_count > 0):
                    collection = db["order_tobe_sqr_complete"]
                    x = collection.find()
                    instruments1 = pd.DataFrame()
                    temp = pd.DataFrame()
                    for y in x:
                        jj = pd.DataFrame([y])
                        instruments1 = pd.concat([temp,jj])
                        temp = instruments1
                    # order_tobe_cancel = pd.read_csv(file['title'])
                    order_tobe_sqr_complete = instruments1
                order_pending_complete_count = db.list_collection_names().count("order_pending_complete")
                if(order_pending_complete_count > 0):
                    collection = db["order_pending_complete"]
                    x = collection.find()
                    instruments1 = pd.DataFrame()
                    temp = pd.DataFrame()
                    for y in x:
                        jj = pd.DataFrame([y])
                        instruments1 = pd.concat([temp,jj])
                        temp = instruments1
                    # order_tobe_cancel = pd.read_csv(file['title'])
                    order_pending_complete = instruments1

                order_manage_count = db.list_collection_names().count("order_manage")
                if(order_manage_count > 0):
                    collection = db["order_manage"]
                    x = collection.find()
                    instruments1 = pd.DataFrame()
                    temp = pd.DataFrame()
                    for y in x:
                        jj = pd.DataFrame([y])
                        instruments1 = pd.concat([temp,jj])
                        temp = instruments1
                    # order_tobe_cancel = pd.read_csv(file['title'])
                    order_manage = instruments1
                order_pending_count = db.list_collection_names().count("order_pending")
                if(order_pending_count > 0):
                    collection = db["order_pending"]
                    x = collection.find()
                    instruments1 = pd.DataFrame()
                    temp = pd.DataFrame()
                    for y in x:
                        jj = pd.DataFrame([y])
                        instruments1 = pd.concat([temp,jj])
                        temp = instruments1
                    # order_tobe_cancel = pd.read_csv(file['title'])
                    order_pending = instruments1
                order_complete_count = db.list_collection_names().count("order_complete")
                if(order_complete_count > 0):
                    collection = db["order_complete"]
                    x = collection.find()
                    instruments1 = pd.DataFrame()
                    temp = pd.DataFrame()
                    for y in x:
                        jj = pd.DataFrame([y])
                        instruments1 = pd.concat([temp,jj])
                        temp = instruments1
                    # order_tobe_cancel = pd.read_csv(file['title'])
                    order_complete = instruments1
                order_cancel_count = db.list_collection_names().count("order_cancel")
                if(order_cancel_count > 0):
                    collection = db["order_cancel"]
                    x = collection.find()
                    instruments1 = pd.DataFrame()
                    instruments1 = pd.DataFrame()
                    temp = pd.DataFrame()
                    for y in x:
                        jj = pd.DataFrame([y])
                        instruments1 = pd.concat([temp,jj])
                        temp = instruments1
                    # order_tobe_cancel = pd.read_csv(file['title'])
                    order_cancel = instruments1

            #             response1 = response1.reset_index(drop=True)

                print(s, file=sys.stderr)
                # if len(order_manage) > 0:
                #     df99 = df79[df79['status']=="Open"].reset_index()
#                 if len(order_complete) > 0:
#                     print("P and L"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
#                     mesg = ''
#                     currnt_profit = [None] * 4
#                     instru = [None] * 4
#                     strike = [None] * 4
#                     contract = [None] * 4
#                     entry_price = [None] * 4
#                     current_price =[None] * 4
#                     overall_profit = 0
#                     for mj in range(0,len(order_complete)):
#                         # df_opt = get_data(order_complete['instrument_id'][mj],fromm, fromm, "minute",s)
#                         df_opt = pd.DataFrame(kite.historical_data(order_complete['instrument_id'][mj], fromm, fromm, "minute", continuous=False, oi=True))
#                         s = kite
#                         currnt_profit[mj] = round((order_complete['entry_price'][mj] - df_opt['close'].iloc[-1])*order_complete['qty'][mj],2) if order_complete['current_signal'][mj] == "SELL" else round((df_opt['close'].iloc[-1] - order_complete['entry_price'][mj])*order_complete['qty'][mj],2)
#                         instru[mj] = order_complete['instru'][mj]
#                         strike[mj] = order_complete['strike'][mj]
#                         contract[mj] = order_complete['contract'][mj]
#                         entry_price[mj] = order_complete['entry_price'][mj]
#                         current_price[mj] = df_opt['close'].iloc[-1]
#                         overall_profit = overall_profit + currnt_profit[mj]
#                         mesg = str(mesg) + str(instru[mj])+" "+str(strike[mj])+str(contract[mj])+" "+str(entry_price[mj])+" "+str(current_price[mj])+" "+str(currnt_profit[mj])+"\n"
# #                 currnt_profit_opt_2 = round((df_opt_2['close'].iloc[-1] - df99['entry price'][1])*15,2)
# #                 currnt_profit_opt_3 = round((df_opt_3['close'].iloc[-1] - df99['entry price'][2])*15,2)
# #                 print(currnt_profit_opt_1,currnt_profit_opt_2,currnt_profit_opt_3,lll)
#                     mesg = str(mesg)+str("overall  ")+str(overall_profit)
#                     requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': mesg})  

# #                 print(df99, file=sys.stderr)
# #                 print("P and L", file=sys.stderr)
                
# #                 colname = ["exit date", "entry_portfolio_value", "exit price","status","Exit Trd Value"]
# #                 print(df79.loc[(df79.trade_id==df99['trade_id'][0]), colname], file=sys.stderr)
#                 mesg = ''
#                 for mj in range(0,len(df80.loc[(df80.status=="Open") and (df80.executed > 0), 'trade_id'])):
#                     df_opt = get_data(df80['instrument_id'][mj],fromm, fromm, "minute",s)
#                     currnt_profit[mj] = round((df80['entry_price'][mj] - df_opt['close'].iloc[-1])*df80['quantity'][mj],2)
#                     instru[mj] = df80['instru'][mj]
#                     strike[mj] = df80['strike'][mj]
#                     contract[mj] = df80['contract'][mj]
#                     entry_price[mj] = df80['entry_price'][mj]
#                     current_price[mj] = df_opt['close'].iloc[-1]
#                     mesg = str(mesg) + str(instru[mj])+" "+str(strike[mj])+str(contract[mj])+" "+str(entry_price[mj])+" "+str(current_price[mj])+" "+str(currnt_profit[mj])+"\n"
# #                 currnt_profit_opt_2 = round((df_opt_2['close'].iloc[-1] - df99['entry price'][1])*15,2)
# #                 currnt_profit_opt_3 = round((df_opt_3['close'].iloc[-1] - df99['entry price'][2])*15,2)
# #                 print(currnt_profit_opt_1,currnt_profit_opt_2,currnt_profit_opt_3,lll)
                        
                
#                 df_opt_1 = get_data(df99['ID'][0],fromm, fromm, "minute",s)
#                 df_opt_2 = get_data(df99['ID'][1],fromm, fromm, "minute",s)
#                 df_opt_3 = get_data(df99['ID'][2],fromm, fromm, "minute",s)
#                 df_fut = get_data(df99['ID'][3],fromm, fromm, "minute",s)
                # if len(order_complete) > 0:
                #     if order_complete.loc[order_complete['leg']== "leg1",'expiry'].values[0]:
                # df25['expiry'] = pd.to_datetime(df25.expiry, format='%Y-%m-%d').dt.date
                # # df25['expiry'] = df25['expiry'].dt.strftime('%Y-%m-%d')
                # df26 = df25[(df25['name'] == instru_name) & (pd.to_datetime(df25.expiry).dt.month == datetime.datetime.now().month) & (df25['expiry'] >= datetime.datetime.now(pytz.timezone('Asia/Kolkata')).date())]
                # df26 = df26.sort_values(by=['expiry'])
                # if len(df26) > 1:
                #     exp_1 = df26['expiry'].iloc[0]
                #         # if len(order_complete.loc[order_complete['leg']== "leg4",'expiry'])>0 and order_complete.loc[order_complete['leg']== "leg4",'expiry'].values[0]:
                #     exp_2 = df26['expiry'].iloc[-1]
                # else:
                #     df26 = df25[(df25['name'] == instru_name) & ((pd.to_datetime(df25.expiry).dt.month) == (datetime.datetime.now().month+1)) & (df25['expiry'] >= datetime.datetime.now(pytz.timezone('Asia/Kolkata')).date())]
                #     df26 = df26.sort_values(by=['expiry'])
                #     exp_1 = df26['expiry'].iloc[0]
                #         # if len(order_complete.loc[order_complete['leg']== "leg4",'expiry'])>0 and order_complete.loc[order_complete['leg']== "leg4",'expiry'].values[0]:
                #     exp_2 = df26['expiry'].iloc[-1]

#                 exp_1 = df99['Expiry'][0]
#                 exp_2 = df99['Expiry'][3]
#                 exp_1 = datetime.datetime.now(pytz.timezone('Asia/Kolkata')).date()
#                 exp_2 = datetime.datetime.now(pytz.timezone('Asia/Kolkata')).date()
#                 current_time = datetime.datetime.now().time()
#                 print(current_time>=datetime.time(21,0,0))
#                 print(today== datetime.datetime.strptime(exp_1, '%Y-%m-%d').date()-datetime.timedelta(days=3))
#                 print(df_opt_1['close'].iloc[-1], file=sys.stderr)
#                 print((df99['entry price'][0] - df_opt_1['close'].iloc[-1]), file=sys.stderr)
                
#                 currnt_profit_opt_1 = round((df99['entry price'][0] - df_opt_1['close'].iloc[-1])*15,2)
#                 currnt_profit_opt_2 = round((df_opt_2['close'].iloc[-1] - df99['entry price'][1])*15,2)
#                 currnt_profit_opt_3 = round((df_opt_3['close'].iloc[-1] - df99['entry price'][2])*15,2)
#                 print(currnt_profit_opt_1,currnt_profit_opt_2,currnt_profit_opt_3,lll)
#                 lll = lll + 1
#                 if df99['side'][3] == "sell":
#                     currnt_profit_fut = round((df99['entry price'][3] - df_fut['close'].iloc[-1])*15,2)
#                 elif df99['side'][3] == "buy":
#                     currnt_profit_fut = round((df_fut['close'].iloc[-1] - df99['entry price'][3])*15,2)
#                 total_profit = round((currnt_profit_opt_1+currnt_profit_opt_2+currnt_profit_opt_3+currnt_profit_fut),2)
#                 requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': str(df99['instru'][0])+" "+str(int(df99['strike'][0]))+str(df99['contract'][0])+" "+str(df99['entry price'][0])+" "+str(df_opt_1['close'].iloc[-1])+" "+str(currnt_profit_opt_1)+"\n"+
#                                               str(df99['instru'][1])+" "+str(int(df99['strike'][1]))+str(df99['contract'][1])+" "+str(currnt_profit_opt_2)+ "\n"+
#                                               str(df99['instru'][2])+" "+str(int(df99['strike'][2]))+str(df99['contract'][2])+" "+str(currnt_profit_opt_3)+"\n"+
#                                               str(df99['instru'][3])+" "+str(df99['contract'][3])+" "+str(currnt_profit_fut)+"\n"+
#                                               "Overall"+" "+str(total_profit)})  
#                 requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': df99.iloc[-1].to_string()})

#             print(current_time)
        #time.sleep(1)
            df54 = pd.DataFrame()
            counter = 0
            while counter<10:
                try:
                    df = tv.get_hist(symbol=instru_name,exchange='NSE',fut_contract=1,interval=Interval.in_5_minute,n_bars=500)
                    df54 = tv.get_hist(symbol=instru_name,exchange='NSE',interval=Interval.in_5_minute,n_bars=500)
                    counter = 20
                except Exception as e:
                    logger.error(e)
                    counter = counter + 1
                    print(e," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
#             print(df)
            print(" ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
            counter = 0
            SENSITIVITY = 5
            ATR_PERIOD = 10
            #import matplotlib.dates as mpl_dates
            df65 = pd.DataFrame()
            df1 = pd.DataFrame()
            data = pd.DataFrame()
            df5 = pd.DataFrame()
            #print(nifty_index_data)
            #df = nifty_index_data
            #print(df)
#             df["5_EMA"] = taa.ema(df["close"], length=100)
       
        #     # Compute ATR And nLoss variable
        #     df["xATR"] = taa.atr(df["high"], df["low"], df["close"], timeperiod=ATR_PERIOD)
        #     df["nLoss"] = SENSITIVITY * df["xATR"]
            df["xATR"] = atr(df,   ATR_PERIOD )
            #df["xATR"] = taa.atr(df["high"], df["low"], df["close"], timeperiod=ATR_PERIOD)
            df["nLoss"] = SENSITIVITY * df["xATR"]
        #    print(df)
            #Drop all rows that have nan, X first depending on the ATR preiod for the moving average
            df = df.dropna()
            df = df.reset_index()
            # Function to compute ATRTrailingStop
            def xATRTrailingStop_func(close, prev_close, prev_atr, nloss):
                if close > prev_atr and prev_close > prev_atr:
                    return max(prev_atr, close - nloss)
                elif close < prev_atr and prev_close < prev_atr:
                    return min(prev_atr, close + nloss)
                elif close > prev_atr:
                    return close - nloss
                else:
                    return close + nloss
       
            #Filling ATRTrailingStop Variable
            df["ATRTrailingStop"] = [0.0] + [np.nan for i in range(len(df) - 1)]
       
            for i in range(1, len(df)):
                df.loc[i, "ATRTrailingStop"] = xATRTrailingStop_func(
                    df.loc[i, "close"],
                    df.loc[i - 1, "close"],
                    df.loc[i - 1, "ATRTrailingStop"],
                    df.loc[i, "nLoss"],)
            df["5_EMA"] = taa.sma(df["close"], length=100)
            df["5_EMA_1"] = df["5_EMA"].shift(1)
            df["RSI"] = taa.rsi(df.close, 3)
            df["ATRTrailingStop_1"] = df["ATRTrailingStop"].shift(1)
            df["close_1"] = df["close"].shift(1)
            df['above1'] = (df['5_EMA'] >df["ATRTrailingStop"])
            df['above2'] = (df['5_EMA_1'] <df["ATRTrailingStop_1"])
            df['above'] = (df['above1'] & df["above2"])
            df['below1'] = (df["ATRTrailingStop"]>df['5_EMA'])
            df['below2'] = (df["ATRTrailingStop_1"]<df['5_EMA_1'])
            df['below'] = df['below1'] & df['below2']
            df["Buy"] = (df["close"] > df["ATRTrailingStop"]) & (df['above']) & (df["RSI"] > 60) & (df['close'] > df['close_1'])
            df["Sell"] = (df["close"] < df["ATRTrailingStop"]) & (df['below']) & (df["RSI"] < 40) & (df['close'] < df['close_1'])
            #print(str(exp_1) == str(today + timedelta(days=1)))
            #print(exp_1 == (today + timedelta(days=1)))
            day_week = datetime.datetime.today().weekday()
            day_hr = datetime.datetime.today().hour
            day_min = datetime.datetime.today().minute
            cond1 = (day_week == 2) or (day_week == 4) or (day_week == 3 and ((day_hr == 9 and day_min == 50) or (day_hr == 10 and day_min == 10) or (day_hr == 14 and day_min == 35) or (day_hr == 15 and day_min == 0))) or (day_week == 5 and ((day_hr == 10 and day_min == 10) or (day_hr == 14 and day_min == 35))) or (day_week == 6 and ((day_hr == 9 and day_min == 50) or (day_hr == 10 and day_min == 10) or (day_hr == 15 and day_min == 0)))
            cond2 = (day_hr == 9 and day_min == 25) or (day_hr == 10 and day_min == 15) or (day_hr == 11 and day_min == 35) or (day_hr == 12 and day_min == 30) or (day_hr == 12 and day_min == 40) or (day_hr == 12 and day_min == 50) or (day_hr == 13 and day_min == 50) or (day_hr == 14 and day_min == 0)
            cond3 = (day_week == 4 and day_hr == 14 and day_min == 35) or (day_week == 5 and day_hr == 9 and day_min == 15) or (day_week == 6 and ((day_hr == 9 and day_min == 15) or (day_hr == 9 and day_min == 20)))
        #     print(df25.head(5))
            # if False:
#             print("P and L"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
            print(currently_buy_holding,currently_sell_holding," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
            df = df[(df['datetime'] <= (datetime.datetime.now()- pd.Timedelta(minutes = 5)))]
            # requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': "Buy " +str(df['Buy'].iloc[-1])+" Sell "+str(df['Sell'].iloc[-1])})  
            # requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': df.iloc[-1].to_string()})
            # if True:
            if df['Buy'].iloc[-1] and not currently_buy_holding and not cond2 and not cond3:
                print("Buy Signal Generated"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                signal_ongoing = "BUY"
#             if True:
            #*****square Future****
                square_off = 0
                err_sqr = 0
                # if False:
                if currently_sell_holding:
                    print("suqaring off sell position"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                    requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': "suqaring off sell position"})
                    print(currently_sell_holding)
                    order_complete = order_complete.sort_values(by=['leg'],ignore_index=True)
                    order_pending_tobe_cancel = order_pending
                    order_pending_complete_tobe_cancel = order_pending_complete
                    order_sqr_complete_cons = order_sqr_complete
                    order_tobe_sqr_complete = order_complete
                    order_complete = order_complete[0:0]
                    order_sqr_complete = order_sqr_complete[0:0]
                    order_manage = order_manage[0:0]
                    order_pending = order_pending[0:0]
                    order_pending_complete = order_pending_complete[0:0]
                    # kk = 0
                    # ll = 0
                    # mm = 0
                    # ss = 0
                    # tt = 0
                    # yy = 0
                    # xx = 0
                    # for index, file in enumerate(file_list):
                    #     if(file['title'] =="order_complete.txt"):
                    #         kk = file['id']
                    #     if(file['title'] =="order_manage.txt"):
                    #         ll = file['id']
                    #     if(file['title'] =="order_tobe_sqr_complete.txt"):
                    #         mm = file['id']
                    #     if(file['title'] =="order_pending.txt"):
                    #         ss = file['id']
                    #     if(file['title'] =="order_pending_complete.txt"):
                    #         tt = file['id']
                    #     if(file['title'] =="order_sqr_complete.txt"):
                    #         yy = file['id']
                    #     if(file['title'] =="order_sqr_complete_cons.txt"):
                    #         xx = file['id']
                    # if bool(kk):
                    #     order_complete.to_csv("order_complete.txt", index=False)
                    #     update_file = drive.CreateFile({'id': kk})
                    #     update_file.SetContentFile("order_complete.txt")
                    #     update_file.Upload()
                    # if bool(mm):
                    #     order_tobe_sqr_complete.to_csv("order_tobe_sqr_complete.txt", index=False)
                    #     update_file = drive.CreateFile({'id': mm})
                    #     update_file.SetContentFile("order_tobe_sqr_complete.txt")
                    #     update_file.Upload()
                    # else:
                    #     order_tobe_sqr_complete.to_csv("order_tobe_sqr_complete.txt", index=False)
                    #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_tobe_sqr_complete.txt"})
                    #     gfile.SetContentFile("order_tobe_sqr_complete.txt")
                    #     gfile.Upload()
                    # if bool(ll):
                    #     order_manage.to_csv("order_manage.txt", index=False)
                    #     update_file = drive.CreateFile({'id': ll})
                    #     update_file.SetContentFile("order_manage.txt")
                    #     update_file.Upload()
                    # else:
                    #     order_manage.to_csv("order_manage.txt", index=False)
                    #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_manage.txt"})
                    #     gfile.SetContentFile("order_manage.txt")
                    #     gfile.Upload()
                    # if bool(ss):
                    #     order_pending.to_csv("order_pending.txt", index=False)
                    #     update_file = drive.CreateFile({'id': ss})
                    #     update_file.SetContentFile("order_pending.txt")
                    #     update_file.Upload()
                    # else:
                    #     order_pending.to_csv("order_pending.txt", index=False)
                    #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_pending.txt"})
                    #     gfile.SetContentFile("order_pending.txt")
                    #     gfile.Upload()
                    # if bool(tt):
                    #     order_pending_complete.to_csv("order_pending_complete.txt", index=False)
                    #     update_file = drive.CreateFile({'id': tt})
                    #     update_file.SetContentFile("order_pending_complete.txt")
                    #     update_file.Upload()
                    # else:
                    #     order_pending_complete.to_csv("order_pending_complete.txt", index=False)
                    #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_pending_complete.txt"})
                    #     gfile.SetContentFile("order_pending_complete.txt")
                    #     gfile.Upload()
                    # if bool(yy):
                    #     order_sqr_complete.to_csv("order_sqr_complete.txt", index=False)
                    #     update_file = drive.CreateFile({'id': yy})
                    #     update_file.SetContentFile("order_sqr_complete.txt")
                    #     update_file.Upload()
                    # else:
                    #     order_sqr_complete.to_csv("order_sqr_complete.txt", index=False)
                    #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_sqr_complete.txt"})
                    #     gfile.SetContentFile("order_sqr_complete.txt")
                    #     gfile.Upload()
                    # if bool(xx):
                    #     order_sqr_complete_cons.to_csv("order_sqr_complete_cons.txt", index=False)
                    #     update_file = drive.CreateFile({'id': xx})
                    #     update_file.SetContentFile("order_sqr_complete_cons.txt")
                    #     update_file.Upload()
                    # else:
                    #     order_sqr_complete_cons.to_csv("order_sqr_complete_cons.txt", index=False)
                    #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_sqr_complete_cons.txt"})
                    #     gfile.SetContentFile("order_sqr_complete_cons.txt")
                    #     gfile.Upload()
                    order_manage_count = db.list_collection_names().count("order_manage")
                    if(order_manage_count < 1):
                        db.create_collection("order_manage")
                    else:
                        db.order_manage.delete_many({})
                    collection = db["order_manage"]
                    dict1 = order_manage.to_dict('records')
                    for x in range(0,len(dict1)):
                        collection.insert_one(dict1[x])
                    order_complete_count = db.list_collection_names().count("order_complete")
                    if(order_complete_count < 1):
                        db.create_collection("order_complete")
                    else:
                        db.order_complete.delete_many({})
                    collection = db["order_complete"]
                    dict1 = order_complete.to_dict('records')
                    for x in range(0,len(dict1)):
                        collection.insert_one(dict1[x])
                    order_pending_count = db.list_collection_names().count("order_pending")
                    if(order_pending_count < 1):
                        db.create_collection("order_pending")
                    else:
                        db.order_pending.delete_many({})
                    collection = db["order_pending"]
                    dict1 = order_pending.to_dict('records')
                    for x in range(0,len(dict1)):
                        collection.insert_one(dict1[x])
                    order_sqr_complete_count = db.list_collection_names().count("order_sqr_complete")
                    if(order_sqr_complete_count < 1):
                        db.create_collection("order_sqr_complete")
                    else:
                        db.order_sqr_complete.delete_many({})
                    collection = db["order_sqr_complete"]
                    dict1 = order_sqr_complete.to_dict('records')
                    for x in range(0,len(dict1)):
                        collection.insert_one(dict1[x])
                    order_tobe_sqr_complete_count = db.list_collection_names().count("order_tobe_sqr_complete")
                    if(order_tobe_sqr_complete_count < 1):
                        db.create_collection("order_tobe_sqr_complete")
                    else:
                        db.order_tobe_sqr_complete.delete_many({})
                    collection = db["order_tobe_sqr_complete"]
                    dict1 = order_tobe_sqr_complete.to_dict('records')
                    for x in range(0,len(dict1)):
                        collection.insert_one(dict1[x])
                    order_pending_complete_count = db.list_collection_names().count("order_pending_complete")
                    if(order_pending_complete_count < 1):
                        db.create_collection("order_pending_complete")
                    else:
                        db.order_pending_complete.delete_many({})
                    collection = db["order_pending_complete"]
                    dict1 = order_pending_complete.to_dict('records')
                    for x in range(0,len(dict1)):
                        collection.insert_one(dict1[x])
                    order_sqr_complete_cons_count = db.list_collection_names().count("order_sqr_complete_cons")
                    if(order_sqr_complete_cons_count < 1):
                        db.create_collection("order_sqr_complete_cons")
                    else:
                        db.order_sqr_complete_cons.delete_many({})
                    collection = db["order_sqr_complete_cons"]
                    dict1 = order_sqr_complete_cons.to_dict('records')
                    for x in range(0,len(dict1)):
                        collection.insert_one(dict1[x])

                    err_sqr = square_off_sell(order_manage,order_tobe_sqr_complete,s)
                    square_off = 1
                    if(len(order_pending_tobe_cancel))>0:
                        err_sqr_1 = cancel_pending(s,order_pending_tobe_cancel,order_cancel)
                        if(len(err_sqr_1))>0:
                            err_sqr = err_sqr + 1
                    if(len(order_pending_complete_tobe_cancel))>0:
                        err_sqr_1 = cancel_pending_complete(s,order_pending_complete_tobe_cancel,order_cancel_complete)
                        if(len(err_sqr_1))>0:
                            err_sqr = err_sqr + 1
                        # square_off_buy(order_manage,order_tobe_sqr_complete,s)
#                 sell_pos(df79,s,df25,df,portfolio_value_spread,portfolio_value_naked,portfolio_value_fut,exp_1,exp_2)
                weekly_rollover = 0
                monthly_rollover = 0
                if len(order_tobe_exec_log) > 0:
                    order_tobe_exec_log = order_tobe_exec_log[0:0]
                    # mm = 0
                    # for index, file in enumerate(file_list):
                    #     if(file['title'] =="order_tobe_exec_log.txt"):
                    #         mm = file['id']
                    # if bool(mm):
                    #     order_tobe_exec_log.to_csv("order_tobe_exec_log.txt", index=False)
                    #     update_file = drive.CreateFile({'id': mm})
                    #     update_file.SetContentFile("order_tobe_exec_log.txt")
                    #     update_file.Upload()
                    # else:
                    #     order_tobe_exec_log.to_csv("order_tobe_exec_log.txt", index=False)
                    #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_tobe_exec_log.txt"})
                    #     gfile.SetContentFile("order_tobe_exec_log.txt")
                    #     gfile.Upload()
                    order_tobe_exec_log_count = db.list_collection_names().count("order_tobe_exec_log")
                    if(order_tobe_exec_log_count < 1):
                        db.create_collection("order_tobe_exec_log")
                    else:
                        db.order_tobe_exec_log.delete_many({})
                    collection = db["order_tobe_exec_log"]
                    dict1 = order_tobe_exec_log.to_dict('records')
                    for x in range(0,len(dict1)):
                        collection.insert_one(dict1[x])

                if err_sqr < 1:
                    print("entering buy position"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                    buy_pos(df79,s,df25,df,exp_1,exp_2,weekly_rollover,monthly_rollover,square_off,df54)
                    requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': "Buy " +str(df['Buy'].iloc[-1])+" Sell "+str(df['Sell'].iloc[-1])})  
                    requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': df.iloc[-1].to_string()})
                    requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': "Buy Signal Generated"})
                    requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': df.iloc[-1].to_string()})
                    requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': "entering buy position"})
                else:
                    print("error in entering buy position so save"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                    if len(order_tobe_exec_log) > 0:
                        order_tobe_exec_log = order_tobe_exec_log[0:0]
                    requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': "saving buy position due to error"})
                    save_pos_buy(s,df25,df,exp_1,exp_2,weekly_rollover,monthly_rollover,df54)
    #                     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "Fullstatus.txt"})
    #                     gfile.SetContentFile("Fullstatus.txt")
    #                     gfile.Upload()                    
    #                     df99.to_csv("status.txt")
    #                     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "status.txt"})
    #                     gfile.SetContentFile("status.txt")
    #                     gfile.Upload()
    #                     #drive.put("status.txt", data=df99.to_csv(index=False, sep=','))
                        #drive.put("Fullstatus.txt", data=df79.to_csv(index=False, sep=','))
            #*****Future****
            #elif True:
            elif (df['Sell'].iloc[-1] and not currently_sell_holding and not cond2 and not cond1):
                print("Sell Signal Generated"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': "Sell Signal Generated"})  
                requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': df.iloc[-1].to_string()})
                signal_ongoing = "SELL"
#             elif True:
            #*****square Future****
                square_off = 0
                err_sqr = 0
                # if True:
                if(currently_buy_holding):
                    print("suqaring off buy position"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                    requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': "suqaring off buy position"})
                    order_complete = order_complete.sort_values(by=['leg'],ignore_index=True)
                    order_pending_tobe_cancel = order_pending
                    order_pending_complete_tobe_cancel = order_pending_complete
                    order_sqr_complete_cons = order_sqr_complete
                    order_tobe_sqr_complete = order_complete
                    order_complete = order_complete[0:0]
                    order_sqr_complete = order_sqr_complete[0:0]
                    order_manage = order_manage[0:0]
                    order_pending = order_pending[0:0]
                    order_pending_complete = order_pending_complete[0:0]
                    # kk = 0
                    # ll = 0
                    # mm = 0
                    # ss = 0
                    # tt = 0
                    # yy = 0
                    # xx = 0
                    # for index, file in enumerate(file_list):
                    #     if(file['title'] =="order_complete.txt"):
                    #         kk = file['id']
                    #     if(file['title'] =="order_manage.txt"):
                    #         ll = file['id']
                    #     if(file['title'] =="order_tobe_sqr_complete.txt"):
                    #         mm = file['id']
                    #     if(file['title'] =="order_pending.txt"):
                    #         ss = file['id']
                    #     if(file['title'] =="order_pending_complete.txt"):
                    #         tt = file['id']
                    #     if(file['title'] =="order_sqr_complete.txt"):
                    #         yy = file['id']
                    #     if(file['title'] =="order_sqr_complete_cons.txt"):
                    #         xx = file['id']
                    # if bool(kk):
                    #     order_complete.to_csv("order_complete.txt", index=False)
                    #     update_file = drive.CreateFile({'id': kk})
                    #     update_file.SetContentFile("order_complete.txt")
                    #     update_file.Upload()
                    # if bool(mm):
                    #     order_tobe_sqr_complete.to_csv("order_tobe_sqr_complete.txt", index=False)
                    #     update_file = drive.CreateFile({'id': mm})
                    #     update_file.SetContentFile("order_tobe_sqr_complete.txt")
                    #     update_file.Upload()
                    # else:
                    #     order_tobe_sqr_complete.to_csv("order_tobe_sqr_complete.txt", index=False)
                    #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_tobe_sqr_complete.txt"})
                    #     gfile.SetContentFile("order_tobe_sqr_complete.txt")
                    #     gfile.Upload()
                    # if bool(ll):
                    #     order_manage.to_csv("order_manage.txt", index=False)
                    #     update_file = drive.CreateFile({'id': ll})
                    #     update_file.SetContentFile("order_manage.txt")
                    #     update_file.Upload()
                    # else:
                    #     order_manage.to_csv("order_manage.txt", index=False)
                    #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_manage.txt"})
                    #     gfile.SetContentFile("order_manage.txt")
                    #     gfile.Upload()
                    # if bool(ss):
                    #     order_pending.to_csv("order_pending.txt", index=False)
                    #     update_file = drive.CreateFile({'id': ss})
                    #     update_file.SetContentFile("order_pending.txt")
                    #     update_file.Upload()
                    # else:
                    #     order_pending.to_csv("order_pending.txt", index=False)
                    #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_pending.txt"})
                    #     gfile.SetContentFile("order_pending.txt")
                    #     gfile.Upload()
                    # if bool(tt):
                    #     order_pending_complete.to_csv("order_pending_complete.txt", index=False)
                    #     update_file = drive.CreateFile({'id': tt})
                    #     update_file.SetContentFile("order_pending_complete.txt")
                    #     update_file.Upload()
                    # else:
                    #     order_pending_complete.to_csv("order_pending_complete.txt", index=False)
                    #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_pending_complete.txt"})
                    #     gfile.SetContentFile("order_pending_complete.txt")
                    #     gfile.Upload()
                    # if bool(yy):
                    #     order_sqr_complete.to_csv("order_sqr_complete.txt", index=False)
                    #     update_file = drive.CreateFile({'id': yy})
                    #     update_file.SetContentFile("order_sqr_complete.txt")
                    #     update_file.Upload()
                    # else:
                    #     order_sqr_complete.to_csv("order_sqr_complete.txt", index=False)
                    #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_sqr_complete.txt"})
                    #     gfile.SetContentFile("order_sqr_complete.txt")
                    #     gfile.Upload()
                    # if bool(xx):
                    #     order_sqr_complete_cons.to_csv("order_sqr_complete_cons.txt", index=False)
                    #     update_file = drive.CreateFile({'id': xx})
                    #     update_file.SetContentFile("order_sqr_complete_cons.txt")
                    #     update_file.Upload()
                    # else:
                    #     order_sqr_complete_cons.to_csv("order_sqr_complete_cons.txt", index=False)
                    #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_sqr_complete_cons.txt"})
                    #     gfile.SetContentFile("order_sqr_complete_cons.txt")
                    #     gfile.Upload()
                    order_manage_count = db.list_collection_names().count("order_manage")
                    if(order_manage_count < 1):
                        db.create_collection("order_manage")
                    else:
                        db.order_manage.delete_many({})
                    collection = db["order_manage"]
                    dict1 = order_manage.to_dict('records')
                    for x in range(0,len(dict1)):
                        collection.insert_one(dict1[x])
                    order_complete_count = db.list_collection_names().count("order_complete")
                    if(order_complete_count < 1):
                        db.create_collection("order_complete")
                    else:
                        db.order_complete.delete_many({})
                    collection = db["order_complete"]
                    dict1 = order_complete.to_dict('records')
                    for x in range(0,len(dict1)):
                        collection.insert_one(dict1[x])
                    order_pending_count = db.list_collection_names().count("order_pending")
                    if(order_pending_count < 1):
                        db.create_collection("order_pending")
                    else:
                        db.order_pending.delete_many({})
                    collection = db["order_pending"]
                    dict1 = order_pending.to_dict('records')
                    for x in range(0,len(dict1)):
                        collection.insert_one(dict1[x])
                    order_sqr_complete_count = db.list_collection_names().count("order_sqr_complete")
                    if(order_sqr_complete_count < 1):
                        db.create_collection("order_sqr_complete")
                    else:
                        db.order_sqr_complete.delete_many({})
                    collection = db["order_sqr_complete"]
                    dict1 = order_sqr_complete.to_dict('records')
                    for x in range(0,len(dict1)):
                        collection.insert_one(dict1[x])
                    order_tobe_sqr_complete_count = db.list_collection_names().count("order_tobe_sqr_complete")
                    if(order_tobe_sqr_complete_count < 1):
                        db.create_collection("order_tobe_sqr_complete")
                    else:
                        db.order_tobe_sqr_complete.delete_many({})
                    collection = db["order_tobe_sqr_complete"]
                    dict1 = order_tobe_sqr_complete.to_dict('records')
                    for x in range(0,len(dict1)):
                        collection.insert_one(dict1[x])
                    order_pending_complete_count = db.list_collection_names().count("order_pending_complete")
                    if(order_pending_complete_count < 1):
                        db.create_collection("order_pending_complete")
                    else:
                        db.order_pending_complete.delete_many({})
                    collection = db["order_pending_complete"]
                    dict1 = order_pending_complete.to_dict('records')
                    for x in range(0,len(dict1)):
                        collection.insert_one(dict1[x])
                    order_sqr_complete_cons_count = db.list_collection_names().count("order_sqr_complete_cons")
                    if(order_sqr_complete_cons_count < 1):
                        db.create_collection("order_sqr_complete_cons")
                    else:
                        db.order_sqr_complete_cons.delete_many({})
                    collection = db["order_sqr_complete_cons"]
                    dict1 = order_sqr_complete_cons.to_dict('records')
                    for x in range(0,len(dict1)):
                        collection.insert_one(dict1[x])

                    err_sqr = square_off_buy(order_manage,order_tobe_sqr_complete,s)
                    square_off = 1
                    if(len(order_pending_tobe_cancel))>0:
                        err_sqr_1 = cancel_pending(s,order_pending_tobe_cancel,order_cancel)
                        if(len(err_sqr_1))>0:
                            err_sqr = err_sqr + 1
                    if(len(order_pending_complete_tobe_cancel))>0:
                        err_sqr_1 = cancel_pending_complete(s,order_pending_complete_tobe_cancel,order_cancel_complete)
                        if(len(err_sqr_1))>0:
                            err_sqr = err_sqr + 1
                        # square_off_buy(order_manage,order_tobe_sqr_complete,s)
#                 sell_pos(df79,s,df25,df,portfolio_value_spread,portfolio_value_naked,portfolio_value_fut,exp_1,exp_2)
                weekly_rollover = 0
                monthly_rollover = 0
                if len(order_tobe_exec_log) > 0:
                    order_tobe_exec_log = order_tobe_exec_log[0:0]
                    # mm = 0
                    # for index, file in enumerate(file_list):
                    #     if(file['title'] =="order_tobe_exec_log.txt"):
                    #         mm = file['id']
                    # if bool(mm):
                    #     order_tobe_exec_log.to_csv("order_tobe_exec_log.txt", index=False)
                    #     update_file = drive.CreateFile({'id': mm})
                    #     update_file.SetContentFile("order_tobe_exec_log.txt")
                    #     update_file.Upload()
                    # else:
                    #     order_tobe_exec_log.to_csv("order_tobe_exec_log.txt", index=False)
                    #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_tobe_exec_log.txt"})
                    #     gfile.SetContentFile("order_tobe_exec_log.txt")
                    #     gfile.Upload()
                    order_tobe_exec_log_count = db.list_collection_names().count("order_tobe_exec_log")
                    if(order_tobe_exec_log_count < 1):
                        db.create_collection("order_tobe_exec_log")
                    else:
                        db.order_tobe_exec_log.delete_many({})
                    collection = db["order_tobe_exec_log"]
                    dict1 = order_tobe_exec_log.to_dict('records')
                    for x in range(0,len(dict1)):
                        collection.insert_one(dict1[x])

                if err_sqr < 1:
                    print("entering sell position"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                    requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': "entering sell position"})
                    sell_pos(df79,s,df25,df,exp_1,exp_2,weekly_rollover,monthly_rollover,square_off,df54)
                else:
                    print("error in entering sell position so save"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                    requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': "saving sell position due to error"})
                    save_pos_sell(s,df25,df,exp_1,exp_2,weekly_rollover,monthly_rollover,df54)
            elif False and str(exp_1) == str(today) and not (str(exp_2) == str(today)) and (current_time1>=datetime.time(15,20,0)) and (current_time1<datetime.time(15,25,0)):
#             elif True:
                print("weekly rollover"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
#                 print("weekly rollover")
                if(currently_buy_holding):
                    print("suqaring off buy position"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                    requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': "suqaring off buy position"})
                    square_off = 0
                    err_sqr = 0
                    requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': "weekly rollover - Buy side"})
                    order_complete = order_complete.sort_values(by=['leg'],ignore_index=True)
                    order_pending_tobe_cancel = order_pending
                    order_pending_complete_tobe_cancel = order_pending_complete
                    order_sqr_complete_cons = order_sqr_complete
                    order_tobe_sqr_complete = order_complete[order_complete['contract']!="F"]
                    order_complete = order_complete.drop(order_complete.index[order_complete['contract']!="F"])
                    order_complete = order_complete.reset_index()
#                     order_tobe_sqr_complete = order_complete
#                     order_complete = order_complete[0:0]
                    order_sqr_complete = order_sqr_complete[0:0]
                    order_manage = order_manage[0:0]
                    order_pending = order_pending[0:0]
                    order_pending_complete = order_pending_complete[0:0]
                    # kk = 0
                    # ll = 0
                    # mm = 0
                    # ss = 0
                    # tt = 0
                    # yy = 0
                    # xx = 0
                    # for index, file in enumerate(file_list):
                    #     if(file['title'] =="order_complete.txt"):
                    #         kk = file['id']
                    #     if(file['title'] =="order_manage.txt"):
                    #         ll = file['id']
                    #     if(file['title'] =="order_tobe_sqr_complete.txt"):
                    #         mm = file['id']
                    #     if(file['title'] =="order_pending.txt"):
                    #         ss = file['id']
                    #     if(file['title'] =="order_pending_complete.txt"):
                    #         tt = file['id']
                    #     if(file['title'] =="order_sqr_complete.txt"):
                    #         yy = file['id']
                    #     if(file['title'] =="order_sqr_complete_cons.txt"):
                    #         xx = file['id']
                    # if bool(kk):
                    #     order_complete.to_csv("order_complete.txt", index=False)
                    #     update_file = drive.CreateFile({'id': kk})
                    #     update_file.SetContentFile("order_complete.txt")
                    #     update_file.Upload()
                    # if bool(mm):
                    #     order_tobe_sqr_complete.to_csv("order_tobe_sqr_complete.txt", index=False)
                    #     update_file = drive.CreateFile({'id': mm})
                    #     update_file.SetContentFile("order_tobe_sqr_complete.txt")
                    #     update_file.Upload()
                    # else:
                    #     order_tobe_sqr_complete.to_csv("order_tobe_sqr_complete.txt", index=False)
                    #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_tobe_sqr_complete.txt"})
                    #     gfile.SetContentFile("order_tobe_sqr_complete.txt")
                    #     gfile.Upload()
                    # if bool(ll):
                    #     order_manage.to_csv("order_manage.txt", index=False)
                    #     update_file = drive.CreateFile({'id': ll})
                    #     update_file.SetContentFile("order_manage.txt")
                    #     update_file.Upload()
                    # else:
                    #     order_manage.to_csv("order_manage.txt", index=False)
                    #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_manage.txt"})
                    #     gfile.SetContentFile("order_manage.txt")
                    #     gfile.Upload()
                    # if bool(ss):
                    #     order_pending.to_csv("order_pending.txt", index=False)
                    #     update_file = drive.CreateFile({'id': ss})
                    #     update_file.SetContentFile("order_pending.txt")
                    #     update_file.Upload()
                    # else:
                    #     order_pending.to_csv("order_pending.txt", index=False)
                    #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_pending.txt"})
                    #     gfile.SetContentFile("order_pending.txt")
                    #     gfile.Upload()
                    # if bool(tt):
                    #     order_pending_complete.to_csv("order_pending_complete.txt", index=False)
                    #     update_file = drive.CreateFile({'id': tt})
                    #     update_file.SetContentFile("order_pending_complete.txt")
                    #     update_file.Upload()
                    # else:
                    #     order_pending_complete.to_csv("order_pending_complete.txt", index=False)
                    #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_pending_complete.txt"})
                    #     gfile.SetContentFile("order_pending_complete.txt")
                    #     gfile.Upload()
                    # if bool(yy):
                    #     order_sqr_complete.to_csv("order_sqr_complete.txt", index=False)
                    #     update_file = drive.CreateFile({'id': yy})
                    #     update_file.SetContentFile("order_sqr_complete.txt")
                    #     update_file.Upload()
                    # else:
                    #     order_sqr_complete.to_csv("order_sqr_complete.txt", index=False)
                    #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_sqr_complete.txt"})
                    #     gfile.SetContentFile("order_sqr_complete.txt")
                    #     gfile.Upload()
                    # if bool(xx):
                    #     order_sqr_complete_cons.to_csv("order_sqr_complete_cons.txt", index=False)
                    #     update_file = drive.CreateFile({'id': xx})
                    #     update_file.SetContentFile("order_sqr_complete_cons.txt")
                    #     update_file.Upload()
                    # else:
                    #     order_sqr_complete_cons.to_csv("order_sqr_complete_cons.txt", index=False)
                    #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_sqr_complete_cons.txt"})
                    #     gfile.SetContentFile("order_sqr_complete_cons.txt")
                    #     gfile.Upload()
                    order_manage_count = db.list_collection_names().count("order_manage")
                    if(order_manage_count < 1):
                        db.create_collection("order_manage")
                    else:
                        db.order_manage.delete_many({})
                    collection = db["order_manage"]
                    dict1 = order_manage.to_dict('records')
                    for x in range(0,len(dict1)):
                        collection.insert_one(dict1[x])
                    order_complete_count = db.list_collection_names().count("order_complete")
                    if(order_complete_count < 1):
                        db.create_collection("order_complete")
                    else:
                        db.order_complete.delete_many({})
                    collection = db["order_complete"]
                    dict1 = order_complete.to_dict('records')
                    for x in range(0,len(dict1)):
                        collection.insert_one(dict1[x])
                    order_pending_count = db.list_collection_names().count("order_pending")
                    if(order_pending_count < 1):
                        db.create_collection("order_pending")
                    else:
                        db.order_pending.delete_many({})
                    collection = db["order_pending"]
                    dict1 = order_pending.to_dict('records')
                    for x in range(0,len(dict1)):
                        collection.insert_one(dict1[x])
                    order_sqr_complete_count = db.list_collection_names().count("order_sqr_complete")
                    if(order_sqr_complete_count < 1):
                        db.create_collection("order_sqr_complete")
                    else:
                        db.order_sqr_complete.delete_many({})
                    collection = db["order_sqr_complete"]
                    dict1 = order_sqr_complete.to_dict('records')
                    for x in range(0,len(dict1)):
                        collection.insert_one(dict1[x])
                    order_tobe_sqr_complete_count = db.list_collection_names().count("order_tobe_sqr_complete")
                    if(order_tobe_sqr_complete_count < 1):
                        db.create_collection("order_tobe_sqr_complete")
                    else:
                        db.order_tobe_sqr_complete.delete_many({})
                    collection = db["order_tobe_sqr_complete"]
                    dict1 = order_tobe_sqr_complete.to_dict('records')
                    for x in range(0,len(dict1)):
                        collection.insert_one(dict1[x])
                    order_pending_complete_count = db.list_collection_names().count("order_pending_complete")
                    if(order_pending_complete_count < 1):
                        db.create_collection("order_pending_complete")
                    else:
                        db.order_pending_complete.delete_many({})
                    collection = db["order_pending_complete"]
                    dict1 = order_pending_complete.to_dict('records')
                    for x in range(0,len(dict1)):
                        collection.insert_one(dict1[x])
                    order_sqr_complete_cons_count = db.list_collection_names().count("order_sqr_complete_cons")
                    if(order_sqr_complete_cons_count < 1):
                        db.create_collection("order_sqr_complete_cons")
                    else:
                        db.order_sqr_complete_cons.delete_many({})
                    collection = db["order_sqr_complete_cons"]
                    dict1 = order_sqr_complete_cons.to_dict('records')
                    for x in range(0,len(dict1)):
                        collection.insert_one(dict1[x])
                    err_sqr = buy_exp_1(df79,s,df25,df,exp_1,exp_2,weekly_rollover,monthly_rollover,square_off,df54)
#                     err_sqr = square_off_buy_exp_1(order_manage,order_tobe_sqr_complete,s)
                    square_off = 1
                    if(len(order_pending_tobe_cancel))>0:
                        err_sqr_1 = cancel_pending(s,order_pending_tobe_cancel,order_cancel)
                        if(len(err_sqr_1))>0:
                            err_sqr = err_sqr + 1
                    if(len(order_pending_complete_tobe_cancel))>0:
                        err_sqr_1 = cancel_pending_complete(s,order_pending_complete_tobe_cancel,order_cancel_complete)
                        if(len(err_sqr_1))>0:
                            err_sqr = err_sqr + 1

#                     buy_pos(df79,s,df25,df,portfolio_value_spread,portfolio_value_naked,portfolio_value_fut,exp_1,exp_2)
                    weekly_rollover = 1
                    monthly_rollover = 0
                    if len(order_tobe_exec_log) > 0:
                        order_tobe_exec_log = order_tobe_exec_log[0:0]
                        # mm = 0
                        # for index, file in enumerate(file_list):
                        #     if(file['title'] =="order_tobe_exec_log.txt"):
                        #         mm = file['id']
                        # if bool(mm):
                        #     order_tobe_exec_log.to_csv("order_tobe_exec_log.txt", index=False)
                        #     update_file = drive.CreateFile({'id': mm})
                        #     update_file.SetContentFile("order_tobe_exec_log.txt")
                        #     update_file.Upload()
                        # else:
                        #     order_tobe_exec_log.to_csv("order_tobe_exec_log.txt", index=False)
                        #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_tobe_exec_log.txt"})
                        #     gfile.SetContentFile("order_tobe_exec_log.txt")
                        #     gfile.Upload()
                        order_tobe_exec_log_count = db.list_collection_names().count("order_tobe_exec_log")
                        if(order_tobe_exec_log_count < 1):
                            db.create_collection("order_tobe_exec_log")
                        else:
                            db.order_tobe_exec_log.delete_many({})
                        collection = db["order_tobe_exec_log"]
                        dict1 = order_tobe_exec_log.to_dict('records')
                        for x in range(0,len(dict1)):
                            collection.insert_one(dict1[x])

                    if err_sqr < 1:
                        print("sqr off buy position"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                        requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': "sqr off buy position"})
                        a = square_off_buy_exp_1(order_manage,order_tobe_sqr_complete,s)
#                         buy_pos(df79,s,df25,df,exp_1,exp_2,weekly_rollover,monthly_rollover,square_off,df54)
                    else:
                        print("sqr off buy position error"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
#                         if len(order_tobe_exec_log) > 0:
#                             order_tobe_exec_log = order_tobe_exec_log[0:0]
#                         requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': "saving buy position due to error"})
#                         save_pos_buy(s,df25,df,exp_1,exp_2,weekly_rollover,monthly_rollover,df54)
                if(currently_sell_holding):
                    print("suqaring off sell position"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                    requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': "suqaring off sell position"})
                    square_off = 0
                    err_sqr = 0
                    requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': "weekly rollover - Sell side"})
                    order_complete = order_complete.sort_values(by=['leg'],ignore_index=True)
                    order_pending_tobe_cancel = order_pending
                    order_pending_complete_tobe_cancel = order_pending_complete
                    order_sqr_complete_cons = order_sqr_complete
                    order_tobe_sqr_complete = order_complete[order_complete['contract']!="F"]
                    order_complete = order_complete.drop(order_complete.index[order_complete['contract']!="F"])
                    order_complete = order_complete.reset_index()
#                     order_tobe_sqr_complete = order_complete
#                     order_complete = order_complete[0:0]
                    order_sqr_complete = order_sqr_complete[0:0]
                    order_manage = order_manage[0:0]
                    order_pending = order_pending[0:0]
                    order_pending_complete = order_pending_complete[0:0]
                    # kk = 0
                    # ll = 0
                    # mm = 0
                    # ss = 0
                    # tt = 0
                    # yy = 0
                    # xx = 0
                    # for index, file in enumerate(file_list):
                    #     if(file['title'] =="order_complete.txt"):
                    #         kk = file['id']
                    #     if(file['title'] =="order_manage.txt"):
                    #         ll = file['id']
                    #     if(file['title'] =="order_tobe_sqr_complete.txt"):
                    #         mm = file['id']
                    #     if(file['title'] =="order_pending.txt"):
                    #         ss = file['id']
                    #     if(file['title'] =="order_pending_complete.txt"):
                    #         tt = file['id']
                    #     if(file['title'] =="order_sqr_complete.txt"):
                    #         yy = file['id']
                    #     if(file['title'] =="order_sqr_complete_cons.txt"):
                    #         xx = file['id']
                    # if bool(kk):
                    #     order_complete.to_csv("order_complete.txt", index=False)
                    #     update_file = drive.CreateFile({'id': kk})
                    #     update_file.SetContentFile("order_complete.txt")
                    #     update_file.Upload()
                    # if bool(mm):
                    #     order_tobe_sqr_complete.to_csv("order_tobe_sqr_complete.txt", index=False)
                    #     update_file = drive.CreateFile({'id': mm})
                    #     update_file.SetContentFile("order_tobe_sqr_complete.txt")
                    #     update_file.Upload()
                    # else:
                    #     order_tobe_sqr_complete.to_csv("order_tobe_sqr_complete.txt", index=False)
                    #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_tobe_sqr_complete.txt"})
                    #     gfile.SetContentFile("order_tobe_sqr_complete.txt")
                    #     gfile.Upload()
                    # if bool(ll):
                    #     order_manage.to_csv("order_manage.txt", index=False)
                    #     update_file = drive.CreateFile({'id': ll})
                    #     update_file.SetContentFile("order_manage.txt")
                    #     update_file.Upload()
                    # else:
                    #     order_manage.to_csv("order_manage.txt", index=False)
                    #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_manage.txt"})
                    #     gfile.SetContentFile("order_manage.txt")
                    #     gfile.Upload()
                    # if bool(ss):
                    #     order_pending.to_csv("order_pending.txt", index=False)
                    #     update_file = drive.CreateFile({'id': ss})
                    #     update_file.SetContentFile("order_pending.txt")
                    #     update_file.Upload()
                    # else:
                    #     order_pending.to_csv("order_pending.txt", index=False)
                    #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_pending.txt"})
                    #     gfile.SetContentFile("order_pending.txt")
                    #     gfile.Upload()
                    # if bool(tt):
                    #     order_pending_complete.to_csv("order_pending_complete.txt", index=False)
                    #     update_file = drive.CreateFile({'id': tt})
                    #     update_file.SetContentFile("order_pending_complete.txt")
                    #     update_file.Upload()
                    # else:
                    #     order_pending_complete.to_csv("order_pending_complete.txt", index=False)
                    #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_pending_complete.txt"})
                    #     gfile.SetContentFile("order_pending_complete.txt")
                    #     gfile.Upload()
                    # if bool(yy):
                    #     order_sqr_complete.to_csv("order_sqr_complete.txt", index=False)
                    #     update_file = drive.CreateFile({'id': yy})
                    #     update_file.SetContentFile("order_sqr_complete.txt")
                    #     update_file.Upload()
                    # else:
                    #     order_sqr_complete.to_csv("order_sqr_complete.txt", index=False)
                    #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_sqr_complete.txt"})
                    #     gfile.SetContentFile("order_sqr_complete.txt")
                    #     gfile.Upload()
                    # if bool(xx):
                    #     order_sqr_complete_cons.to_csv("order_sqr_complete_cons.txt", index=False)
                    #     update_file = drive.CreateFile({'id': xx})
                    #     update_file.SetContentFile("order_sqr_complete_cons.txt")
                    #     update_file.Upload()
                    # else:
                    #     order_sqr_complete_cons.to_csv("order_sqr_complete_cons.txt", index=False)
                    #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_sqr_complete_cons.txt"})
                    #     gfile.SetContentFile("order_sqr_complete_cons.txt")
                    #     gfile.Upload()
                    order_manage_count = db.list_collection_names().count("order_manage")
                    if(order_manage_count < 1):
                        db.create_collection("order_manage")
                    else:
                        db.order_manage.delete_many({})
                    collection = db["order_manage"]
                    dict1 = order_manage.to_dict('records')
                    for x in range(0,len(dict1)):
                        collection.insert_one(dict1[x])
                    order_complete_count = db.list_collection_names().count("order_complete")
                    if(order_complete_count < 1):
                        db.create_collection("order_complete")
                    else:
                        db.order_complete.delete_many({})
                    collection = db["order_complete"]
                    dict1 = order_complete.to_dict('records')
                    for x in range(0,len(dict1)):
                        collection.insert_one(dict1[x])
                    order_pending_count = db.list_collection_names().count("order_pending")
                    if(order_pending_count < 1):
                        db.create_collection("order_pending")
                    else:
                        db.order_pending.delete_many({})
                    collection = db["order_pending"]
                    dict1 = order_pending.to_dict('records')
                    for x in range(0,len(dict1)):
                        collection.insert_one(dict1[x])
                    order_sqr_complete_count = db.list_collection_names().count("order_sqr_complete")
                    if(order_sqr_complete_count < 1):
                        db.create_collection("order_sqr_complete")
                    else:
                        db.order_sqr_complete.delete_many({})
                    collection = db["order_sqr_complete"]
                    dict1 = order_sqr_complete.to_dict('records')
                    for x in range(0,len(dict1)):
                        collection.insert_one(dict1[x])
                    order_tobe_sqr_complete_count = db.list_collection_names().count("order_tobe_sqr_complete")
                    if(order_tobe_sqr_complete_count < 1):
                        db.create_collection("order_tobe_sqr_complete")
                    else:
                        db.order_tobe_sqr_complete.delete_many({})
                    collection = db["order_tobe_sqr_complete"]
                    dict1 = order_tobe_sqr_complete.to_dict('records')
                    for x in range(0,len(dict1)):
                        collection.insert_one(dict1[x])
                    order_pending_complete_count = db.list_collection_names().count("order_pending_complete")
                    if(order_pending_complete_count < 1):
                        db.create_collection("order_pending_complete")
                    else:
                        db.order_pending_complete.delete_many({})
                    collection = db["order_pending_complete"]
                    dict1 = order_pending_complete.to_dict('records')
                    for x in range(0,len(dict1)):
                        collection.insert_one(dict1[x])
                    order_sqr_complete_cons_count = db.list_collection_names().count("order_sqr_complete_cons")
                    if(order_sqr_complete_cons_count < 1):
                        db.create_collection("order_sqr_complete_cons")
                    else:
                        db.order_sqr_complete_cons.delete_many({})
                    collection = db["order_sqr_complete_cons"]
                    dict1 = order_sqr_complete_cons.to_dict('records')
                    for x in range(0,len(dict1)):
                        collection.insert_one(dict1[x])

                    err_sqr = square_off_sell_exp_1(order_manage,order_tobe_sqr_complete,s)
                    #err_sqr = sell_exp_1(df79,s,df25,df,exp_1,exp_2,weekly_rollover,monthly_rollover,square_off,df54)
                    square_off = 1
                    if(len(order_pending_tobe_cancel))>0:
                        err_sqr_1 = cancel_pending(s,order_pending_tobe_cancel,order_cancel)
                        if(len(err_sqr_1))>0:
                            err_sqr = err_sqr + 1
                    if(len(order_pending_complete_tobe_cancel))>0:
                        err_sqr_1 = cancel_pending_complete(s,order_pending_complete_tobe_cancel,order_cancel_complete)
                        if(len(err_sqr_1))>0:
                            err_sqr = err_sqr + 1
                    weekly_rollover = 1
                    monthly_rollover = 0
                    if len(order_tobe_exec_log) > 0:
                        order_tobe_exec_log = order_tobe_exec_log[0:0]
                        # mm = 0
                        # for index, file in enumerate(file_list):
                        #     if(file['title'] =="order_tobe_exec_log.txt"):
                        #         mm = file['id']
                        # if bool(mm):
                        #     order_tobe_exec_log.to_csv("order_tobe_exec_log.txt", index=False)
                        #     update_file = drive.CreateFile({'id': mm})
                        #     update_file.SetContentFile("order_tobe_exec_log.txt")
                        #     update_file.Upload()
                        # else:
                        #     order_tobe_exec_log.to_csv("order_tobe_exec_log.txt", index=False)
                        #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_tobe_exec_log.txt"})
                        #     gfile.SetContentFile("order_tobe_exec_log.txt")
                        #     gfile.Upload()
#                     sell_pos(df79,s,df25,df,portfolio_value_spread,portfolio_value_naked,portfolio_value_fut,exp_1,exp_2)
                        order_tobe_exec_log_count = db.list_collection_names().count("order_tobe_exec_log")
                        if(order_tobe_exec_log_count < 1):
                            db.create_collection("order_tobe_exec_log")
                        else:
                            db.order_tobe_exec_log.delete_many({})
                        collection = db["order_tobe_exec_log"]
                        dict1 = order_tobe_exec_log.to_dict('records')
                        for x in range(0,len(dict1)):
                            collection.insert_one(dict1[x])

                    if err_sqr < 1:
                        print("sqr off sell position"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                        a = square_off_sell_exp_1(order_manage,order_tobe_sqr_complete,s)
                        requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': "sqr off sell position"})
#                         sell_pos(df79,s,df25,df,exp_1,exp_2,weekly_rollover,monthly_rollover,square_off,df54)
                    else:
                        print("sqr off sell position due to error"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
#                         if len(order_tobe_exec_log) > 0:
#                             order_tobe_exec_log = order_tobe_exec_log[0:0]
#                         requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': "saving sell position due to error"})
#                         save_pos_sell(s,df25,df,exp_1,exp_2,weekly_rollover,monthly_rollover,df54)
            elif (str(exp_2) == str(today)) and (current_time1>=datetime.time(15,20,0)) and (current_time1<datetime.time(15,25,0)):
            # elif True:
                print("monthly rollover"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
#                 print("monthly rollover")
                if(currently_buy_holding):
                    print("suqaring off buy position"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                    requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': "suqaring off buy position"})
                    square_off = 0
                    err_sqr = 0
                    requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': "monthly rollover - Buy side"})
                    order_complete = order_complete.sort_values(by=['leg'],ignore_index=True)
                    order_pending_tobe_cancel = order_pending
                    order_pending_complete_tobe_cancel = order_pending_complete
                    order_sqr_complete_cons = order_sqr_complete
                    order_tobe_sqr_complete = order_complete
                    order_complete = order_complete[0:0]
                    order_sqr_complete = order_sqr_complete[0:0]
                    order_manage = order_manage[0:0]
                    order_pending = order_pending[0:0]
                    order_pending_complete = order_pending_complete[0:0]
                    # kk = 0
                    # ll = 0
                    # mm = 0
                    # ss = 0
                    # tt = 0
                    # yy = 0
                    # xx = 0
                    # for index, file in enumerate(file_list):
                    #     if(file['title'] =="order_complete.txt"):
                    #         kk = file['id']
                    #     if(file['title'] =="order_manage.txt"):
                    #         ll = file['id']
                    #     if(file['title'] =="order_tobe_sqr_complete.txt"):
                    #         mm = file['id']
                    #     if(file['title'] =="order_pending.txt"):
                    #         ss = file['id']
                    #     if(file['title'] =="order_pending_complete.txt"):
                    #         tt = file['id']
                    #     if(file['title'] =="order_sqr_complete.txt"):
                    #         yy = file['id']
                    #     if(file['title'] =="order_sqr_complete_cons.txt"):
                    #         xx = file['id']
                    # if bool(kk):
                    #     order_complete.to_csv("order_complete.txt", index=False)
                    #     update_file = drive.CreateFile({'id': kk})
                    #     update_file.SetContentFile("order_complete.txt")
                    #     update_file.Upload()
                    # if bool(mm):
                    #     order_tobe_sqr_complete.to_csv("order_tobe_sqr_complete.txt", index=False)
                    #     update_file = drive.CreateFile({'id': mm})
                    #     update_file.SetContentFile("order_tobe_sqr_complete.txt")
                    #     update_file.Upload()
                    # else:
                    #     order_tobe_sqr_complete.to_csv("order_tobe_sqr_complete.txt", index=False)
                    #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_tobe_sqr_complete.txt"})
                    #     gfile.SetContentFile("order_tobe_sqr_complete.txt")
                    #     gfile.Upload()
                    # if bool(ll):
                    #     order_manage.to_csv("order_manage.txt", index=False)
                    #     update_file = drive.CreateFile({'id': ll})
                    #     update_file.SetContentFile("order_manage.txt")
                    #     update_file.Upload()
                    # else:
                    #     order_manage.to_csv("order_manage.txt", index=False)
                    #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_manage.txt"})
                    #     gfile.SetContentFile("order_manage.txt")
                    #     gfile.Upload()
                    # if bool(ss):
                    #     order_pending.to_csv("order_pending.txt", index=False)
                    #     update_file = drive.CreateFile({'id': ss})
                    #     update_file.SetContentFile("order_pending.txt")
                    #     update_file.Upload()
                    # else:
                    #     order_pending.to_csv("order_pending.txt", index=False)
                    #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_pending.txt"})
                    #     gfile.SetContentFile("order_pending.txt")
                    #     gfile.Upload()
                    # if bool(tt):
                    #     order_pending_complete.to_csv("order_pending_complete.txt", index=False)
                    #     update_file = drive.CreateFile({'id': tt})
                    #     update_file.SetContentFile("order_pending_complete.txt")
                    #     update_file.Upload()
                    # else:
                    #     order_pending_complete.to_csv("order_pending_complete.txt", index=False)
                    #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_pending_complete.txt"})
                    #     gfile.SetContentFile("order_pending_complete.txt")
                    #     gfile.Upload()
                    # if bool(yy):
                    #     order_sqr_complete.to_csv("order_sqr_complete.txt", index=False)
                    #     update_file = drive.CreateFile({'id': yy})
                    #     update_file.SetContentFile("order_sqr_complete.txt")
                    #     update_file.Upload()
                    # else:
                    #     order_sqr_complete.to_csv("order_sqr_complete.txt", index=False)
                    #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_sqr_complete.txt"})
                    #     gfile.SetContentFile("order_sqr_complete.txt")
                    #     gfile.Upload()
                    # if bool(xx):
                    #     order_sqr_complete_cons.to_csv("order_sqr_complete_cons.txt", index=False)
                    #     update_file = drive.CreateFile({'id': xx})
                    #     update_file.SetContentFile("order_sqr_complete_cons.txt")
                    #     update_file.Upload()
                    # else:
                    #     order_sqr_complete_cons.to_csv("order_sqr_complete_cons.txt", index=False)
                    #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_sqr_complete_cons.txt"})
                    #     gfile.SetContentFile("order_sqr_complete_cons.txt")
                    #     gfile.Upload()
                    order_manage_count = db.list_collection_names().count("order_manage")
                    if(order_manage_count < 1):
                        db.create_collection("order_manage")
                    else:
                        db.order_manage.delete_many({})
                    collection = db["order_manage"]
                    dict1 = order_manage.to_dict('records')
                    for x in range(0,len(dict1)):
                        collection.insert_one(dict1[x])
                    order_complete_count = db.list_collection_names().count("order_complete")
                    if(order_complete_count < 1):
                        db.create_collection("order_complete")
                    else:
                        db.order_complete.delete_many({})
                    collection = db["order_complete"]
                    dict1 = order_complete.to_dict('records')
                    for x in range(0,len(dict1)):
                        collection.insert_one(dict1[x])
                    order_pending_count = db.list_collection_names().count("order_pending")
                    if(order_pending_count < 1):
                        db.create_collection("order_pending")
                    else:
                        db.order_pending.delete_many({})
                    collection = db["order_pending"]
                    dict1 = order_pending.to_dict('records')
                    for x in range(0,len(dict1)):
                        collection.insert_one(dict1[x])
                    order_sqr_complete_count = db.list_collection_names().count("order_sqr_complete")
                    if(order_sqr_complete_count < 1):
                        db.create_collection("order_sqr_complete")
                    else:
                        db.order_sqr_complete.delete_many({})
                    collection = db["order_sqr_complete"]
                    dict1 = order_sqr_complete.to_dict('records')
                    for x in range(0,len(dict1)):
                        collection.insert_one(dict1[x])
                    order_tobe_sqr_complete_count = db.list_collection_names().count("order_tobe_sqr_complete")
                    if(order_tobe_sqr_complete_count < 1):
                        db.create_collection("order_tobe_sqr_complete")
                    else:
                        db.order_tobe_sqr_complete.delete_many({})
                    collection = db["order_tobe_sqr_complete"]
                    dict1 = order_tobe_sqr_complete.to_dict('records')
                    for x in range(0,len(dict1)):
                        collection.insert_one(dict1[x])
                    order_pending_complete_count = db.list_collection_names().count("order_pending_complete")
                    if(order_pending_complete_count < 1):
                        db.create_collection("order_pending_complete")
                    else:
                        db.order_pending_complete.delete_many({})
                    collection = db["order_pending_complete"]
                    dict1 = order_pending_complete.to_dict('records')
                    for x in range(0,len(dict1)):
                        collection.insert_one(dict1[x])
                    order_sqr_complete_cons_count = db.list_collection_names().count("order_sqr_complete_cons")
                    if(order_sqr_complete_cons_count < 1):
                        db.create_collection("order_sqr_complete_cons")
                    else:
                        db.order_sqr_complete_cons.delete_many({})
                    collection = db["order_sqr_complete_cons"]
                    dict1 = order_sqr_complete_cons.to_dict('records')
                    for x in range(0,len(dict1)):
                        collection.insert_one(dict1[x])


                    err_sqr = square_off_buy(order_manage,order_tobe_sqr_complete,s)
                    square_off = 1
                    if(len(order_pending_tobe_cancel))>0:
                        err_sqr_1 = cancel_pending(s,order_pending_tobe_cancel,order_cancel)
                        if(len(err_sqr_1))>0:
                            err_sqr = err_sqr + 1
                    if(len(order_pending_complete_tobe_cancel))>0:
                        err_sqr_1 = cancel_pending_complete(s,order_pending_complete_tobe_cancel,order_cancel_complete)
                        if(len(err_sqr_1))>0:
                            err_sqr = err_sqr + 1
#                     buy_pos(df79,s,df25,df,portfolio_value_spread,portfolio_value_naked,portfolio_value_fut,exp_1,exp_2)
                    weekly_rollover = 0
                    monthly_rollover = 1
                    if len(order_tobe_exec_log) > 0:
                        order_tobe_exec_log = order_tobe_exec_log[0:0]
                        # mm = 0
                        # for index, file in enumerate(file_list):
                        #     if(file['title'] =="order_tobe_exec_log.txt"):
                        #         mm = file['id']
                        # if bool(mm):
                        #     order_tobe_exec_log.to_csv("order_tobe_exec_log.txt", index=False)
                        #     update_file = drive.CreateFile({'id': mm})
                        #     update_file.SetContentFile("order_tobe_exec_log.txt")
                        #     update_file.Upload()
                        # else:
                        #     order_tobe_exec_log.to_csv("order_tobe_exec_log.txt", index=False)
                        #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_tobe_exec_log.txt"})
                        #     gfile.SetContentFile("order_tobe_exec_log.txt")
                        #     gfile.Upload()
                        order_tobe_exec_log_count = db.list_collection_names().count("order_tobe_exec_log")
                        if(order_tobe_exec_log_count < 1):
                            db.create_collection("order_tobe_exec_log")
                        else:
                            db.order_tobe_exec_log.delete_many({})
                        collection = db["order_tobe_exec_log"]
                        dict1 = order_tobe_exec_log.to_dict('records')
                        for x in range(0,len(dict1)):
                            collection.insert_one(dict1[x])

                    if err_sqr < 1:
                        print("entering buy position"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                        requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': "entering buy position"})
                        buy_pos(df79,s,df25,df,exp_1,exp_2,weekly_rollover,monthly_rollover,square_off,df54)
                    else:
                        print("saving buy position due to error"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                        requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': "saving buy position due to error"})
                        save_pos_buy(s,df25,df,exp_1,exp_2,weekly_rollover,monthly_rollover,df54)
                
                if(currently_sell_holding):
                    print("suqaring off sell position"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                    requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': "suqaring off sell position"})
                    square_off = 0
                    err_sqr = 0
                    requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': "monthly rollover - Sell side"})
                    order_complete = order_complete.sort_values(by=['leg'],ignore_index=True)
                    order_pending_tobe_cancel = order_pending
                    order_pending_complete_tobe_cancel = order_pending_complete
                    order_sqr_complete_cons = order_sqr_complete
                    order_tobe_sqr_complete = order_complete
                    order_complete = order_complete[0:0]
                    order_sqr_complete = order_sqr_complete[0:0]
                    order_manage = order_manage[0:0]
                    order_pending = order_pending[0:0]
                    order_pending_complete = order_pending_complete[0:0]
                    # kk = 0
                    # ll = 0
                    # mm = 0
                    # ss = 0
                    # tt = 0
                    # yy = 0
                    # xx = 0
                    # for index, file in enumerate(file_list):
                    #     if(file['title'] =="order_complete.txt"):
                    #         kk = file['id']
                    #     if(file['title'] =="order_manage.txt"):
                    #         ll = file['id']
                    #     if(file['title'] =="order_tobe_sqr_complete.txt"):
                    #         mm = file['id']
                    #     if(file['title'] =="order_pending.txt"):
                    #         ss = file['id']
                    #     if(file['title'] =="order_pending_complete.txt"):
                    #         tt = file['id']
                    #     if(file['title'] =="order_sqr_complete.txt"):
                    #         yy = file['id']
                    #     if(file['title'] =="order_sqr_complete_cons.txt"):
                    #         xx = file['id']
                    # if bool(kk):
                    #     order_complete.to_csv("order_complete.txt", index=False)
                    #     update_file = drive.CreateFile({'id': kk})
                    #     update_file.SetContentFile("order_complete.txt")
                    #     update_file.Upload()
                    # if bool(mm):
                    #     order_tobe_sqr_complete.to_csv("order_tobe_sqr_complete.txt", index=False)
                    #     update_file = drive.CreateFile({'id': mm})
                    #     update_file.SetContentFile("order_tobe_sqr_complete.txt")
                    #     update_file.Upload()
                    # else:
                    #     order_tobe_sqr_complete.to_csv("order_tobe_sqr_complete.txt", index=False)
                    #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_tobe_sqr_complete.txt"})
                    #     gfile.SetContentFile("order_tobe_sqr_complete.txt")
                    #     gfile.Upload()
                    # if bool(ll):
                    #     order_manage.to_csv("order_manage.txt", index=False)
                    #     update_file = drive.CreateFile({'id': ll})
                    #     update_file.SetContentFile("order_manage.txt")
                    #     update_file.Upload()
                    # else:
                    #     order_manage.to_csv("order_manage.txt", index=False)
                    #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_manage.txt"})
                    #     gfile.SetContentFile("order_manage.txt")
                    #     gfile.Upload()
                    # if bool(ss):
                    #     order_pending.to_csv("order_pending.txt", index=False)
                    #     update_file = drive.CreateFile({'id': ss})
                    #     update_file.SetContentFile("order_pending.txt")
                    #     update_file.Upload()
                    # else:
                    #     order_pending.to_csv("order_pending.txt", index=False)
                    #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_pending.txt"})
                    #     gfile.SetContentFile("order_pending.txt")
                    #     gfile.Upload()
                    # if bool(tt):
                    #     order_pending_complete.to_csv("order_pending_complete.txt", index=False)
                    #     update_file = drive.CreateFile({'id': tt})
                    #     update_file.SetContentFile("order_pending_complete.txt")
                    #     update_file.Upload()
                    # else:
                    #     order_pending_complete.to_csv("order_pending_complete.txt", index=False)
                    #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_pending_complete.txt"})
                    #     gfile.SetContentFile("order_pending_complete.txt")
                    #     gfile.Upload()
                    # if bool(yy):
                    #     order_sqr_complete.to_csv("order_sqr_complete.txt", index=False)
                    #     update_file = drive.CreateFile({'id': yy})
                    #     update_file.SetContentFile("order_sqr_complete.txt")
                    #     update_file.Upload()
                    # else:
                    #     order_sqr_complete.to_csv("order_sqr_complete.txt", index=False)
                    #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_sqr_complete.txt"})
                    #     gfile.SetContentFile("order_sqr_complete.txt")
                    #     gfile.Upload()
                    # if bool(xx):
                    #     order_sqr_complete_cons.to_csv("order_sqr_complete_cons.txt", index=False)
                    #     update_file = drive.CreateFile({'id': xx})
                    #     update_file.SetContentFile("order_sqr_complete_cons.txt")
                    #     update_file.Upload()
                    # else:
                    #     order_sqr_complete_cons.to_csv("order_sqr_complete_cons.txt", index=False)
                    #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_sqr_complete_cons.txt"})
                    #     gfile.SetContentFile("order_sqr_complete_cons.txt")
                    #     gfile.Upload()
                    # err_sqr = square_off_sell(df79,s)
                    order_manage_count = db.list_collection_names().count("order_manage")
                    if(order_manage_count < 1):
                        db.create_collection("order_manage")
                    else:
                        db.order_manage.delete_many({})
                    collection = db["order_manage"]
                    dict1 = order_manage.to_dict('records')
                    for x in range(0,len(dict1)):
                        collection.insert_one(dict1[x])
                    order_complete_count = db.list_collection_names().count("order_complete")
                    if(order_complete_count < 1):
                        db.create_collection("order_complete")
                    else:
                        db.order_complete.delete_many({})
                    collection = db["order_complete"]
                    dict1 = order_complete.to_dict('records')
                    for x in range(0,len(dict1)):
                        collection.insert_one(dict1[x])
                    order_pending_count = db.list_collection_names().count("order_pending")
                    if(order_pending_count < 1):
                        db.create_collection("order_pending")
                    else:
                        db.order_pending.delete_many({})
                    collection = db["order_pending"]
                    dict1 = order_pending.to_dict('records')
                    for x in range(0,len(dict1)):
                        collection.insert_one(dict1[x])
                    order_sqr_complete_count = db.list_collection_names().count("order_sqr_complete")
                    if(order_sqr_complete_count < 1):
                        db.create_collection("order_sqr_complete")
                    else:
                        db.order_sqr_complete.delete_many({})
                    collection = db["order_sqr_complete"]
                    dict1 = order_sqr_complete.to_dict('records')
                    for x in range(0,len(dict1)):
                        collection.insert_one(dict1[x])
                    order_tobe_sqr_complete_count = db.list_collection_names().count("order_tobe_sqr_complete")
                    if(order_tobe_sqr_complete_count < 1):
                        db.create_collection("order_tobe_sqr_complete")
                    else:
                        db.order_tobe_sqr_complete.delete_many({})
                    collection = db["order_tobe_sqr_complete"]
                    dict1 = order_tobe_sqr_complete.to_dict('records')
                    for x in range(0,len(dict1)):
                        collection.insert_one(dict1[x])
                    order_pending_complete_count = db.list_collection_names().count("order_pending_complete")
                    if(order_pending_complete_count < 1):
                        db.create_collection("order_pending_complete")
                    else:
                        db.order_pending_complete.delete_many({})
                    collection = db["order_pending_complete"]
                    dict1 = order_pending_complete.to_dict('records')
                    for x in range(0,len(dict1)):
                        collection.insert_one(dict1[x])
                    order_sqr_complete_cons_count = db.list_collection_names().count("order_sqr_complete_cons")
                    if(order_sqr_complete_cons_count < 1):
                        db.create_collection("order_sqr_complete_cons")
                    else:
                        db.order_sqr_complete_cons.delete_many({})
                    collection = db["order_sqr_complete_cons"]
                    dict1 = order_sqr_complete_cons.to_dict('records')
                    for x in range(0,len(dict1)):
                        collection.insert_one(dict1[x])


                    err_sqr = square_off_sell(order_manage,order_tobe_sqr_complete,s)
                    square_off = 1
                    if(len(order_pending_tobe_cancel))>0:
                        err_sqr_1 = cancel_pending(s,order_pending_tobe_cancel,order_cancel)
                        if(len(err_sqr_1))>0:
                            err_sqr = err_sqr + 1
                    if(len(order_pending_complete_tobe_cancel))>0:
                        err_sqr_1 = cancel_pending_complete(s,order_pending_complete_tobe_cancel,order_cancel_complete)
                        if(len(err_sqr_1))>0:
                            err_sqr = err_sqr + 1
#                     sell_pos(df79,s,df25,df,portfolio_value_spread,portfolio_value_naked,portfolio_value_fut,exp_1,exp_2)
                    weekly_rollover = 0
                    monthly_rollover = 1
                    if len(order_tobe_exec_log) > 0:
                        order_tobe_exec_log = order_tobe_exec_log[0:0]
                        # mm = 0
                        # for index, file in enumerate(file_list):
                        #     if(file['title'] =="order_tobe_exec_log.txt"):
                        #         mm = file['id']
                        # if bool(mm):
                        #     order_tobe_exec_log.to_csv("order_tobe_exec_log.txt", index=False)
                        #     update_file = drive.CreateFile({'id': mm})
                        #     update_file.SetContentFile("order_tobe_exec_log.txt")
                        #     update_file.Upload()
                        # else:
                        #     order_tobe_exec_log.to_csv("order_tobe_exec_log.txt", index=False)
                        #     gfile = drive.CreateFile({'parents' : [{'id' : folder}], 'title' : "order_tobe_exec_log.txt"})
                        #     gfile.SetContentFile("order_tobe_exec_log.txt")
                        #     gfile.Upload()
                        order_tobe_exec_log_count = db.list_collection_names().count("order_tobe_exec_log")
                        if(order_tobe_exec_log_count < 1):
                            db.create_collection("order_tobe_exec_log")
                        else:
                            db.order_tobe_exec_log.delete_many({})
                        collection = db["order_tobe_exec_log"]
                        dict1 = order_tobe_exec_log.to_dict('records')
                        for x in range(0,len(dict1)):
                            collection.insert_one(dict1[x])

                    if err_sqr < 1:
                        print("entering sell position"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                        requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': "entering sell position"})
                        sell_pos(df79,s,df25,df,exp_1,exp_2,weekly_rollover,monthly_rollover,square_off,df54)
                    else:
                        print("saving sell position due to error"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                        requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': "saving sell position due to error"})
                        save_pos_sell(s,df25,df,exp_1,exp_2,weekly_rollover,monthly_rollover,df54)
#return str("hi")
#             break
            if len(order_complete) > 0:
                print("P and L"," ", datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
                mesg = ''
                currnt_profit = [None] * 4
                instru = [None] * 4
                strike = [None] * 4
                contract = [None] * 4
                entry_price = [None] * 4
                current_price =[None] * 4
                overall_profit = 0
                for mj in range(0,len(order_complete)):
                    # df_opt = get_data(order_complete['instrument_id'][mj],fromm, fromm, "minute",s)
                    df_opt = pd.DataFrame(kite.historical_data(order_complete['instrument_id'][mj], fromm, fromm, "minute", continuous=False, oi=True))
                    s = kite
                    currnt_profit[mj] = round((order_complete['entry_price'][mj] - df_opt['close'].iloc[-1])*order_complete['qty'][mj],2) if order_complete['current_signal'][mj] == "SELL" else round((df_opt['close'].iloc[-1] - order_complete['entry_price'][mj])*order_complete['qty'][mj],2)
                    instru[mj] = order_complete['instru'][mj]
                    strike[mj] = order_complete['strike'][mj]
                    contract[mj] = order_complete['contract'][mj]
                    entry_price[mj] = order_complete['entry_price'][mj]
                    current_price[mj] = df_opt['close'].iloc[-1]
                    overall_profit = overall_profit + currnt_profit[mj]
                    mesg = str(mesg) + str(instru[mj])+" "+str(strike[mj])+str(contract[mj])+" "+str(entry_price[mj])+" "+str(current_price[mj])+" "+str(currnt_profit[mj])+"\n"
#                 currnt_profit_opt_2 = round((df_opt_2['close'].iloc[-1] - df99['entry price'][1])*15,2)
#                 currnt_profit_opt_3 = round((df_opt_3['close'].iloc[-1] - df99['entry price'][2])*15,2)
#                 print(currnt_profit_opt_1,currnt_profit_opt_2,currnt_profit_opt_3,lll)
                mesg = str(mesg)+str("overall  ")+str(overall_profit)
                requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': mesg})  

#                 print(df99, file=sys.stderr)
#                 print("P and L", file=sys.stderr)
            
#                 colname = ["exit date", "entry_portfolio_value", "exit price","status","Exit Trd Value"]
#                 print(df79.loc[(df79.trade_id==df99['trade_id'][0]), colname], file=sys.stderr)
            mesg = ''

            current_time3 = datetime.datetime.now(pytz.timezone('Asia/Kolkata'))
            period = pd.to_datetime(current_time3).ceil('5 min')
            next_time = period - current_time3
            print("sleeping for seconds -"," ",(next_time-timedelta(seconds=1)).total_seconds(), datetime.datetime.now(pytz.timezone('Asia/Kolkata')), file=sys.stderr)
            time.sleep((next_time-timedelta(seconds=1)).total_seconds())
   
# if __name__ == "__main__":
#     hello_world()

    
start = st.sidebar.button("Start")
exit_app = st.sidebar.button("Shut Down")
if start:
    hello_world()
if exit_app:
    # Give a bit of delay for user experience
    #time.sleep(5)
    # Close streamlit browser tab
   # keyboard.press_and_release('ctrl+w')
    # Terminate streamlit python process
    pid = os.getpid()
    p = psutil.Process(pid)
    p.kill()
