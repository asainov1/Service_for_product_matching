
#%% Импорт бибилиотек 
import argparse
import pandas as pd
import mysql.connector
import cx_Oracle
from datetime import datetime, timedelta
import re
import tqdm 
import os
import pymorphy2
import json
import string
import time
import requests
from tqdm import tqdm
from urllib import request
from urllib.parse import quote
import logging
from logging.handlers import SMTPHandler
import sys, traceback
import warnings 
from sqlalchemy.types import Integer, Float, Date, String, VARCHAR
from sqlalchemy import create_engine
from sqlalchemy.dialects import oracle
warnings.filterwarnings('ignore')
from datetime import datetime
from pytz import timezone  # Import timezone from pytz
import pytz
#%% Коннекшны
def main ():
    myconn = mysql.connector.connect(user='sainov_58623', password='VL$%_I4', 
    port = '9030',
    host='starrocksdb')

    conn = cx_Oracle.connect(user='CRRT', password='NLP4321###', dsn='dwhnew-db:1521/node124')

    proxy_handler = request.ProxyHandler({})

    myconn_crrt = mysql.connector.connect(
        host='starrocksdb',#'dwh-dbr5-lp2',
        port='9030',
        user='crrt',
        password='4g1q6Dc=c7') 

    # Устанавливаю время
    
    iteration_date = datetime.now(timezone('Asia/Karachi')).strftime('%Y-%m-%d %H:%M:%S')

    query = f""" select *
                FROM S0271.PARSING_SKU_LIST_FOR_MAPPING_NEW orm
                """
    top_df = pd.read_sql_query(query, con=myconn_crrt)

    data = [eval(x) for x in top_df.JSON_DATA.to_list()]
    top_sku_df = pd.DataFrame.from_dict(data)
    top_sku_df_active = top_sku_df[(top_sku_df.is_active == '1')&(top_sku_df.type == 'TOP')].reset_index(drop=True)
    


    # Забираем данные появившиеся в таблице S0244.OZON_PRODUCT_PARS за последний день. 

    start_time_str = datetime.now().date().strftime(r'%Y-%m-%d') + ' 00:00:00'
    end_time_str = datetime.now().date().strftime(r'%Y-%m-%d') + ' 23:59:59'
        
    query_main_df = f"""
        SELECT KASPI_ID, ID FROM S0244.WB_PRODUCT_PARS 
        WHERE TIMESTAMP(S$KAFKA_TIMESTAMP) BETWEEN '{start_time_str}' AND '{end_time_str}' 
    """
    parsed_df = pd.read_sql_query(query_main_df, con=myconn_crrt)

    # !!! ДОБАВИТЬ ТАКУЮ ЖЕ ЛОГИКУ ВРЕМЕНИ В РЕГЛАМЕНТ

    # Формируем ID для пары
    parsed_df['COMB_ID'] = parsed_df['KASPI_ID'].astype(str) + ' | ' + parsed_df['ID'].astype(str)

    # Оставляем только нужные ID 
    parsed_df_needed  = parsed_df.loc[:, ['KASPI_ID', 'ID', 'COMB_ID']]
    parsed_df_needed.drop_duplicates(inplace=True)

    # Переименуем колонки
    parsed_df_needed.columns = ['KASPI_ID_pars', 'SKU_pars', 'COMB_ID_pars']


    # Соединяем спарсенные данные с топом
    df_all = top_sku_df_active.merge(parsed_df_needed, left_on= 'sku', right_on='KASPI_ID_pars', how = 'left')

    tz = pytz.timezone('Asia/Karachi')
    start_time_str = (datetime.now()).astimezone(tz).date().strftime(r'%Y-%m-%d') + ' 00:00:00'
    end_time_str = (datetime.now()).astimezone(tz).date().strftime(r'%Y-%m-%d') + ' 23:59:59' # to change 


    # Импорт данных, уже прогоняли
    
    query = f"""
        SELECT 
        KASPI_ID, 
        ID, 
        TO_TIMESTAMP(DBMS_LOB.SUBSTR(TIME_STAMP, 19, 1), 'YYYY-MM-DD HH24:MI:SS') AS TIME_STAMP, 
        MATCHING_RESULT
        FROM crrt.MATCHING_RESULT_CRON_WB
        WHERE TO_TIMESTAMP(DBMS_LOB.SUBSTR(TIME_STAMP, 19, 1), 'YYYY-MM-DD HH24:MI:SS')
        BETWEEN TO_TIMESTAMP('{start_time_str}', 'YYYY-MM-DD HH24:MI:SS') AND TO_TIMESTAMP('{end_time_str}', 'YYYY-MM-DD HH24:MI:SS')
    """
    res = pd.read_sql_query(query, con=conn)

 
    # Формируем ID для пары
    res['COMB_ID'] = res['KASPI_ID'].astype(str) + ' | ' + res['ID'].astype(str)
    res = res[['KASPI_ID','COMB_ID','TIME_STAMP','MATCHING_RESULT']]
    #OLD-NEW
    now = datetime.now() + timedelta(hours=5)

    res['TIME_STAMP'] = pd.to_datetime(res['TIME_STAMP'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
    res['is_runned'] = res['TIME_STAMP'].apply(lambda x: 1 if x.date() == now.date() else 0)


    # Ранее прогонялись
    res_before = res[res['is_runned']==0]
    res_needed = res_before.loc[:, ['KASPI_ID', 'COMB_ID']].drop_duplicates()
    res_needed.columns = ['KASPI_ID_pars_before', 'COMB_ID_pars_before']

    # Прогонялись сегодня
    res_after= res[res['is_runned']==1]
    res_needed_after = res_after.loc[:, ['KASPI_ID', 'COMB_ID']].drop_duplicates()
    res_needed_after.columns = ['KASPI_ID_pars_today', 'COMB_ID_pars_today']

    # Прогонялись сегодня
    res_after_match = res[(res['is_runned']==1)&(res.MATCHING_RESULT=='1')]
    res_needed_after_match = res_after_match.loc[:, ['KASPI_ID', 'COMB_ID']].drop_duplicates()
    res_needed_after_match.columns = ['KASPI_ID_pars_today_match', 'COMB_ID_pars_today_match']

    df_all = df_all.merge(res_needed, left_on = 'COMB_ID_pars' , right_on = 'COMB_ID_pars_before', how = 'left' )
    df_all = df_all.merge(res_needed_after, left_on = 'COMB_ID_pars' , right_on = 'COMB_ID_pars_today', how = 'left' )
    df_all = df_all.merge(res_needed_after_match, left_on = 'COMB_ID_pars' , right_on = 'COMB_ID_pars_today_match', how = 'left' )














    myconn = mysql.connector.connect(user='sainov_58623', password='VL$%_I4', 
    port = '9030',
    host='starrocksdb')
    query = f"""
        SELECT * FROM S0244.OZON_RESULTS_MAPPING
        
        """
    kafka = pd.read_sql_query(query, con=myconn)

    # Форматирование данных 
    kafka_data = [json.loads(x) for x in kafka.d_mes.to_list()]
    kafka_parsed = pd.DataFrame.from_dict(kafka_data)
    kafka_parsed['s$change_date'] = kafka['s$change_date'].to_list()

    kafka_parsed = kafka_parsed[['kaspi_sku','ext_sku','source', 's$change_date']]
    kafka_parsed = kafka_parsed.query("source == 'wb'")

    now = datetime.now() + timedelta(hours=5)
    # Формирование пары
    kafka_parsed['comb_id'] = kafka_parsed['kaspi_sku'].astype(str) + ' | ' + kafka_parsed['ext_sku'].astype(str)
    kafka_parsed['s$change_date'] = pd.to_datetime(kafka_parsed['s$change_date'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
    kafka_parsed['is_runned_kafka'] = kafka_parsed['s$change_date'].apply(lambda x: 1 if x.date() == now.date() else 0)

  
    # обьединение
    kafka_parsed_matches = kafka_parsed[kafka_parsed['is_runned_kafka']==1]
    kafka_parsed_matches = kafka_parsed_matches[['comb_id','is_runned_kafka','kaspi_sku']]
    kafka_parsed_matches.columns = ['comb_id_today', 'is_runned_kafka_today', 'kaspi_sku_kafka_today']

    kafka_parsed = kafka_parsed[['comb_id','is_runned_kafka','kaspi_sku']]
    kafka_parsed.columns = ['comb_id_all', 'is_runned_kafka_all', 'kaspi_sku_kafka_all']


    # Для каспи матчей
    merged_with_top = pd.merge(top_sku_df_active, kafka_parsed, how='left', left_on = 'sku', right_on = 'kaspi_sku_kafka_all')


    df_all = df_all.merge(kafka_parsed, left_on= 'COMB_ID_pars', right_on ='comb_id_all', how='left')
    df_all = df_all.merge(kafka_parsed_matches, left_on= 'COMB_ID_pars', right_on ='comb_id_today', how='left')


    top_cat_logging = df_all.sku.nunique()
    pars_logging = df_all.COMB_ID_pars.nunique()
    pars_logging_unique = df_all.KASPI_ID_pars.nunique()
    share_of_top = f'{round((pars_logging_unique/top_cat_logging) *100, 2)}%'
    new_matches_logging = df_all[~df_all.KASPI_ID_pars_today.isna()].shape[0]
    new_unique_matches_logging =  df_all[~df_all.KASPI_ID_pars_today.isna()].KASPI_ID_pars_today.nunique()
    if pars_logging != 0:
        new_matches_share_logging = f'{round((new_matches_logging / pars_logging) *100,2)}%'
    else:
        new_matches_share_logging = '0%'

    cron_logging = df_all[~df_all.COMB_ID_pars_today_match.isna()].shape[0]
    cron_logging_unique = df_all[~df_all.COMB_ID_pars_today_match.isna()].sku.nunique()
    if new_matches_logging != 0:
        cron_logging_share = f'{round((cron_logging / new_matches_logging) *100, 2)}%'
    else:
        cron_logging_share = '0%'

    kafka_logging = df_all[~df_all.is_runned_kafka_today.isna()].shape[0]
    kafka_unique_logging = df_all[~df_all.is_runned_kafka_today.isna()].KASPI_ID_pars.nunique()
    if cron_logging != 0:
        kafka_cleaned_share = f'{round((kafka_logging / cron_logging) *100, 2)}%'
    else:
        kafka_cleaned_share = '0%'

    kaspi_matches = merged_with_top[~merged_with_top.kaspi_sku_kafka_all.isna()].sku.nunique()
    share_of_new_skus_kaspi = f'{round((kafka_unique_logging / kaspi_matches) *100, 2)}%'


    logging_values = {

        "Дата_прогона": [iteration_date], 
        "Всего_топ_ску": [top_cat_logging],
        "Спарсенные_матчи": [pars_logging],
        "Уникальные_спарсенные_матчи": [pars_logging_unique],
        "Доля_уникальных_спарсенных_матчей_от_топ": [share_of_top],
        "Новые_матчи": [new_matches_logging],
        "Новые_уникальные_матчи": [new_unique_matches_logging],
        "Новые_матчи_доля_от_спарсенных": [new_matches_share_logging],
        "Матчи_алгоритма": [cron_logging],
        "Уникальные_матчи_алгоритма": [cron_logging_unique],
        "Доля_матчей_алгоритма_от_новых": [cron_logging_share],  # Example placeholder
        "Отчищенные_матчи": [kafka_logging],
        "Отчищенные_уникальные_матчи": [kafka_unique_logging],
        "Доля_отчищенных_матчей_от_матчей_алгоритма": [kafka_cleaned_share],
        "Каспи_матчи": [kaspi_matches],
        "Уникальные_каспи_матчи": [kafka_unique_logging],
        "Доля_новых_уникальных_каспи_матчей": [share_of_new_skus_kaspi]
    }
    monitoring = pd.DataFrame(logging_values)
    return monitoring, logging_values
#%%
#### логирование #######
receivers = ['Alikhan.Sainov@kaspi.kz',
            'Ilyas.Mohammad@kaspi.kz',
            'Eleonora.Baimbetova@kaspi.kz']


dtypes = {
        "Дата_прогона": Date,
        "Всего_топ_ску":Integer,
        "Спарсенные_матчи":Integer,
        "Уникальные_спарсенные_матчи":Integer,
        "Доля_уникальных_спарсенных_матчей_от_топ":VARCHAR(50),
        "Новые_матчи":Integer,
        "Новые_уникальные_матчи":Integer,
        "Новые_матчи_доля_от_спарсенных":VARCHAR(50),
        "Матчи_алгоритма":Integer,
        "Уникальные_матчи_алгоритма":Integer,
        "Доля_матчей_алгоритма_от_новых":VARCHAR(50),
        "Отчищенные_матчи":Integer,
        "Отчищенные_уникальные_матчи":Integer,
        "Доля_отчищенных_матчей_от_матчей_алгоритма":VARCHAR(50),
        "Каспи_матчи":Integer,
        "Уникальные_каспи_матчи":Integer,
        "Доля_новых_уникальных_каспи_матчей":VARCHAR(50)
    }
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from smtplib import SMTP
email_server = 'relay2.bc.kz'
email_sender = 'reglament_info@kaspi.kz'
email_subject = 'Service Matching-monitoring-WB'
email_recipient_list = receivers
try:
    monitoring, logging_values = main()
    
    db_url = 'mysql+mysqlconnector://crrt:4g1q6Dc=c7@starrocksdb:9030'
    engine = create_engine(db_url) 
    myconn=mysql.connector.connect(user='crrt',
        password='4g1q6Dc=c7',
        port='9030',
        host='starrocksdb') 

    new_row = pd.DataFrame(logging_values)

    new_row.to_sql(
        "MATCHING_RESULT_STAT_WB",
        con=engine,
        if_exists="append",
        index=False,
        schema='crrt_db',
        dtype=dtypes
    )

    time.sleep(10)
    query = """
    select *
    from crrt_db.MATCHING_RESULT_STAT_WB
    """
    stat = pd.read_sql_query(query, con=myconn)
    stat = stat.sort_values('Дата_прогона',ascending=False)
    # convert the DataFrame to an HTML table
    html_table = stat.to_html()

    # Create the email message
    msg = MIMEMultipart()
    msg['From'] = email_sender
    msg['To'] = ','.join(email_recipient_list)
    msg['Subject'] = email_subject

    # Attach the query result to the email body
    msg.attach(MIMEText(html_table, 'html'))

    # Connect to the email server and send the email
    server = SMTP(email_server)
    server.starttls()
    server.sendmail(email_sender, email_recipient_list, msg.as_string())
    server.quit()


except Exception as e:
    print (f'error: {e}')
    logging.error(traceback.format_exc())























