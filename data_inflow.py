#%% Импорт библиотек
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

#%% Подключение учеток

#%% 

logger = logging.getLogger('logger-1')

def issue_dataframe ():

    # Забираем данные появившиеся в таблице S0244.OZON_PRODUCT_PARS за последний день. 
    # previous_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    start_time_str = datetime.now().date().strftime(r'%Y-%m-%d') + ' 00:00:00'
    end_time_str = datetime.now().date().strftime(r'%Y-%m-%d') + ' 23:59:59'

    query_main_df = f"""
        SELECT * FROM S0244.OZON_PRODUCT_PARS
        WHERE S$CHANGE_DATE BETWEEN '{start_time_str}' AND '{end_time_str}' 
    """
    df = pd.read_sql_query(query_main_df, con=myconn)
    
    ############# логирование ##################
    if df.empty == False:
        logger.info('Все ок DATA_INFLOW не пустой!')
        print ('Все ок DATA_INFLOW не пустой!')
    else:
        trace = traceback.format_exc()
        logger.error('\n'.join(['ERROR - DATA_INFLOW пустой!', trace])) 


    df.KASPI_ID =df.KASPI_ID.astype(str)
    df.SKU =df.SKU.astype(str) 

    df=df.rename(columns={'FULL_NAME':'OZON_TITLE','DESCRIPTION':'descriptions','BRAND_NAME':'BRAND', 'KASPI_PRODUCT_NAME':'KASPI_TITLE', 'PRODUCT_URL':'EXT_URL'})

    def combine_chars(row):
        chars = [row[f'SHORTCHAR#{i}'] for i in range(0, 5) if pd.notna(row[f'SHORTCHAR#{i}'])]
        combined = ' | '.join(chars)
        return combined

    df['ALL_CHARS'] = df.apply(combine_chars, axis=1)
    df = df[['KASPI_ID', 'SKU', 'OZON_TITLE', 'descriptions', 'ALL_CHARS', 'BRAND', 'KASPI_TITLE', 'EXT_URL']]

    # Джойн с категориями Каспи контента
    cats = pd.read_csv('pipeline/cats.csv')
    cats  = cats.rename(columns={'SKU':'KASPI_ID'})
    cats.KASPI_ID = cats.KASPI_ID.astype(str) 
    df_c = pd.merge(df, cats, how='left', on='KASPI_ID')
    df_c = df_c.rename(columns={'CAT_NAME2':'CATEGORY_LEVEL1'})
    df_c.drop('Unnamed: 0',axis=1,inplace=True)
    df = df_c

    df['OZON_TITLE'] = df['OZON_TITLE'].astype(str)
    df['descriptions'] = df['descriptions'].astype(str)
    df['ALL_CHARS'] = df['ALL_CHARS'].astype(str)
    df['KASPI_TITLE'] = df['KASPI_TITLE'].astype(str)
    df['EXT_URL'] = df['EXT_URL'].astype(str)
    df.CATEGORY_LEVEL1 = df.CATEGORY_LEVEL1.astype(str)
    df = df.reset_index(drop=True)
    return df

def issue_cat_char_dict(df):
    
    cats_list = list(df.CATEGORY_NAME.unique())
    main_harakter = f"""

        select 
        * from EXP_USER.CATEGORY@dwhsas
        """
    formulas = pd.DataFrame()
    formulas = pd.read_sql_query(main_harakter, con=conn)
    formulas_need_cat = formulas[(formulas.NAME.isin(cats_list))&(~formulas.NAME_GENERATION_FORMULA.isna())].reset_index()

    # Проверка на категории у которых не нашли формулу
    deleted = []
    for cat in cats_list:
        if cat not in formulas_need_cat.NAME.unique():
            deleted.append(cat)

    cat_char_dict={}
    for i, row in formulas_need_cat.iterrows():
        category_name = row.NAME
        value = row.NAME_GENERATION_FORMULA	
        cat_char_dict[category_name] = value.lower() 
    cat_char_dict

    pattern = r'\$\{([^}]+)\}'
    for category, formula in cat_char_dict.items():
        if formula:
            parameters = re.findall(pattern, formula)
            cat_char_dict[category] = parameters

    key_to_remove = 'brand'
    for cat, val in cat_char_dict.items():
        if key_to_remove in val:
            cat_char_dict[cat] = [value for value in val if value != key_to_remove]
    cols = ['Рамки для выключателей и розеток','Динамометрические ключи','Инструменты для наливных полов','Краскопульты']
    
    for col in cols:
        if col in cats_list and col == 'Рамки для выключателей и розеток':
            some = 'Frames for switches*Posts'
            cat_char_dict['Рамки для выключателей и розеток'].append(some.lower())
        elif col in cats_list and col == 'Динамометрические ключи':
            some2 = 'Torque wrenches*Length'
            cat_char_dict['Динамометрические ключи'].append(some2.lower())
        elif col in cats_list and col == 'Инструменты для наливных полов':
            some3 = 'Self-leveling floor tools*Handle material'
            cat_char_dict['Инструменты для наливных полов'].append(some3.lower())
        elif col in cats_list and col == 'Краскопульты':
            some4 = 'Spray guns*Nozzle diameter'
            cat_char_dict['Краскопульты'].append(some4.lower())
        else:
            pass
    return cat_char_dict


#%%
compiled_patterns = [
    (re.compile(r'[зЗ]елёный'), ', зеленый'),
    (re.compile(r'(\d)\s+sf(\d+)'), r'\1sf\2'),  # Добавлены группы захвата
    (re.compile(r'\(|\)'), ''),  # скобки
    (re.compile(r'cm$', re.IGNORECASE), ''),
    (re.compile(r'кт-104,', re.IGNORECASE), 'kt-104'),
    ]

# Быстрые замены через str.replace
simple_replacements = {
    'темно-серый': 'серый',
    'grey': 'серый',
    'черная': 'черный',
    'white': 'белый',
    'серебристый': 'серый',
    'серый металлик': 'серый',
    'бежевый': 'белый',
    'кремовый': 'белый',
    'cалатовый': 'зеленый',
    'белая': 'белый',
    'электрический чайник': 'электрочайник',
    'чайник электрический': 'электрочайник',
}
def extract_meaningful_value(data):
    for key in data:
        value = data[key]
        if value:
            return value.lstrip('\\').strip()
    return None

def clean_title(title):
    if isinstance(title, str):
        # Быстрые замены
        for old, new in simple_replacements.items():
            title = title.replace(old, new)
        
        # Применение регулярных выражений
        for pattern, replacement in compiled_patterns:
            title = pattern.sub(replacement, title)
        
        title = title.strip()  # Удаление лишних пробелов в начале и конце
    return title





# связанные c LLAMA 
def clean_json_string(json_string):
    for key, value in json_string.items():
        if value is not None:
            value = value.replace('\"', '')  
            value = re.sub(r'\s+', ' ', value.replace('\n', '').strip())
            json_string[key] = value
        else:
            break
    return json_string

def compare_dicts(ozon_dict, kaspi_dict, sku, not_match_str):
    kaspi_value =  kaspi_dict.get(not_match_str, '')
    ozon_value = ozon_dict.get(not_match_str, '')
    kaspi_value = re.sub(r" ", "", str(kaspi_value))
    ozon_value = re.sub(r" ", "", str(ozon_value))
    
    if kaspi_value == '' and ozon_value != '':
        return True
    elif kaspi_value != '' and ozon_value == '':
        return True
    elif kaspi_value==ozon_value:
        return True
    elif kaspi_value!=ozon_value:
        return False           
    else:
        return False

def clean_response(response):
    cleaned_response_re1 = re.sub(r"(?<=[{\s])'(?=[а-я0-9a-z])", '"', response)
    cleaned_response_re2 = re.sub(r"(?<=[а-я0-9a-z\\])'(?:[:,]|$)", '"', cleaned_response_re1)
    return cleaned_response_re2
def process_response(aim, keys_dict_id, keys_dict_kaspi):
    try:
        response = llama_request(aim, keys_dict_id, keys_dict_kaspi)
        cleaned_response = clean_response(response)
        return cleaned_response
    except json.JSONDecodeError as e:
        return e

# %%
# myconn_crrt = mysql.connector.connect(
#     host='starrocksdb',#'dwh-dbr5-lp2',
#     port='9030',
#     user='crrt',
#     password='4g1q6Dc=c7')
# query = f"""
#         SELECT t1.catalog_id as SKU, t1.param as PARAM, t1.name as NAME, t1.display_name_ru as DISPLAY_NAME_RU               
#         FROM DP.SEARCH_PARAM_SHOP t1
#         WHERE t1.catalog_id IN {tuple(chunk)}
#     """

# mysql_data = pd.read_sql_query(query, con=myconn_crrt)


def read_chunk (chunk):
    # Oracle соединение к старрокс
    conn = cx_Oracle.connect(user='CRRT', password='NLP4321###', dsn='dwhnew-db:1521/node124')
    query = f"""
                SELECT t1.SKU, t1.TITLE, t1.BRAND, t3.CATEGORY_NAME,
                    t2.NAME, t2.PARAM, t2.DISPLAY_NAME_RU
                FROM sasuser.SHOP_ALL_ITEMS_CAT_REL@dwhsas t1
                LEFT JOIN sasuser.SHOP_ITEM_CAT_DICT@dwhsas t4
                    ON t4.ARTICLE_NUMBER = t1.SKU
                LEFT JOIN sasuser.SEARCH_SHOP_CATEGORY_ALL_NEW@dwhsas t3
                    ON t3.CATEGORY_ID = t1.CATEGORY_CODE
                LEFT JOIN sasuser.SEARCH_PARAM_SHOP@dwhsas t2
                    ON t2.CATALOG_ID = t1.SKU
                WHERE t1.SKU IN {tuple(chunk)}
            """

    
    subcategories_name_construct = pd.read_sql_query(query, con=conn)
    return subcategories_name_construct
#%%
def issue_kaspi_params(df):
    combined_df=[]
    kaspi_id_list = df['KASPI_ID'].unique()
    length = len(kaspi_id_list)
    portion_size = 1000 
    for i in range(0, length, portion_size):
        chunk = kaspi_id_list[i:i + portion_size]
        retries_left = 3
        for k in range (retries_left):
            try:
                subcategories_name_construct = read_chunk(chunk)
                break
            except:
                if k == retries_left - 1:
                    trace = traceback.format_exc()
                    logger.error('\n'.join(['Ошибка базы', trace])) 

                    raise Exception()
                else:
                    time.sleep(5)
                    continue
        combined_df.append(subcategories_name_construct)
    combined_df = pd.concat(combined_df, axis=0)
    return combined_df


#%%


