
#%% Импорт бибилиотек 
import argparse
import pandas as pd
import sys
import mysql.connector
import cx_Oracle
from datetime import datetime, timedelta
from data_inflow import issue_dataframe
import os
import re
import pymorphy2
import json
import string
import time
import requests
from tqdm import tqdm
from urllib import request
from urllib.parse import quote
import data_inflow
from data_inflow import issue_kaspi_params, issue_cat_char_dict, extract_meaningful_value, read_chunk
from sqlalchemy import create_engine
from sqlalchemy.dialects import oracle
import pytz
import services
from services import translit_by_letters, add_normalized_values
import warnings 
from Levenshtein import distance as lev
from sqlalchemy.types import DateTime

from kafka import KafkaProducer, KafkaConsumer
warnings.filterwarnings('ignore')
import traceback
import logging
from logging.handlers import SMTPHandler
#%%
receivers = [
    'Alikhan.Sainov@kaspi.kz'
]

handler = SMTPHandler(mailhost='relay2.bc.kz',
                        fromaddr='reglament_info@kaspi.kz',
                        toaddrs=receivers,
                        subject='Service Matching')

logger = logging.getLogger('logger-1')
logger.setLevel(logging.INFO)
logger.addHandler(handler)
try: 
    #%%
    # 1. ВЫЗОВ ФУНКЦИИ DATA INFLOW 
    engine = create_engine("oracle+cx_oracle://CRRT:NLP4321###@dwhnew-db:1521/?service_name=node124")
    myconn = mysql.connector.connect(user='sainov_58623', password='VL$%_I4', 
    port = '9030',
    host='starrocksdb')
    conn = cx_Oracle.connect(user='CRRT', password='NLP4321###', dsn='dwhnew-db:1521/node124')

    parser = argparse.ArgumentParser()
    parser.add_argument('--sos')
    args = parser.parse_args()
    sos = int(args.sos)

    df = pd.read_csv(f'df_{sos}.csv')
    combined_df = issue_kaspi_params(df)
    cat_char_dict = issue_cat_char_dict(df)

    ############# логирование ##################
    log_time_info = df.shape[0]
    log_time_info_unique = df.KASPI_ID.nunique()
    
    # Предобработка цвета
    color_dict = {
        'черный':'черный',
        'черная':'черный',
        'black':'черный',
        'черное':'черный',
        'midnight':'черный',
        'ночь':'черный',
        'серый':'серый',
        'grey':'серый',
        'gray': 'серый',
        'silver':'серый',
        'графитовый':'серый',
        'серая':'серый',
        'графит':'серый',
        'graphite':'серый',
        'titanium':'серый',
        'титан':'серый',
        'титановый':'серый',
        'серебристый':'серый',
        'стальной':'серый',
        'металлик':'серый',
        'стали':'серый',
        'синий':'синий',
        'blue':'синий',
        'голубой':'синий',
        'бирюзовый':'синий',
        'зеленый':'зеленый',
        'green':'зеленый',
        'лазурный':'зеленый',
        'мята':'зеленый',
        'mint':'зеленый',
        'белый':'белый',
        'белая':'белый',
        'white':'белый',
        'белые':'белый',
        'starlight':'белый',
        'сияющая звезда':'белый',
        'прозрачный':'белый',
        'бел':'белый',
        'розовый':'розовый',
        'pink':'розовый',
        'желтый':'желтый',
        'yellow':'желтый',
        'красный':'красный',
        'red':'красный',
        'бордовый':'красный',
        'фиолетовый':'фиолетовый',
        'лавандовый':'фиолетовый',
        'лаванда':'фиолетовый',
        'сиреневый':'фиолетовый',
        'purple':'фиолетовый',
        'пурпурный':'фиолетовый',
        'лиловый':'фиолетовый',
        'золотой':'золотой',
        'золотистый':'золотой',
        'золото':'золотой',
        'gold':''
        
    }


    # kaspi_colors = [c.lower() for c in chars.loc[chars.DISPLAY_NAME_RU=='Цвет', 'PARAM'].unique()]

    def color_preprocess(text):
        ozon_preprocess_color = ''
        if isinstance(text, str):
            text = text.lower()
            ozon_color_word_list = text.split()
            for i, word in enumerate(ozon_color_word_list):
                    if word in color_dict:
                        ozon_color_word_list[i] = color_dict[word]
            ozon_preprocess_color = ' '.join(ozon_color_word_list).strip()
        else:
            ozon_preprocess_color = text
        return ozon_preprocess_color

    def model_preproccess(text):
        if isinstance (text, str):
            text = text.lower()
            if text=='redmi 9c 3':
                text = 'redmi 9c'
            # for key in brands_list:
            #     text = text.replace(key, "")
            text = text.replace('slim box', "")
            text = text.replace('5g 6', "")
            text = text.replace('5g', "")
            text = text.replace('4g', "")
            text = text.replace('new', "")
            text = text.replace('dual sim', "")
            text = text.replace('/подарок', "")
            text = text.replace('y27s + y27s', "")
            text = text.replace('c65 + note 50 + m28', "")
            text = text.replace('white edition', "")
            text = text.replace('wwv ru', "")
            text = text.strip()
        return text
    def volume_preproccess(text):
        if isinstance (text, str):
            text = text.lower()
            text = text.replace('мл', "")
            text = text.replace('.0', "") 
            text = text.strip()
        return text


    columns = ['OZON_TITLE','descriptions','ALL_CHARS']
    df[columns] = df[columns].applymap(volume_preproccess)
    df[columns] = df[columns].applymap(model_preproccess)
    df[columns] = df[columns].applymap(color_preprocess)

    column = ['PARAM']
    combined_df[column] = combined_df[column].applymap(volume_preproccess)
    combined_df[column] = combined_df[column].applymap(model_preproccess)
    combined_df[column] = combined_df[column].applymap(color_preprocess)

    #%%
    # СХЕМА TOP 80% GMV Вертикали
    # myconn = mysql.connector.connect(
    #     host='starrocksdb',#'dwh-dbr5-lp2',
    #     port='9030',
    #     user='crrt',
    #     password='4g1q6Dc=c7') 


    # query = f"""
    #     SELECT *
    #     FROM S0271.PARSING_SKU_LIST_FOR_MAPPING 
    # """

    # top_cat = pd.read_sql_query(query, con=myconn)

    ############# логирование ##################
    # top_cat_logging = top_cat.shape[0]
    # share_of_top = round(log_time_info_unique / top_cat_logging , 2) 
    #%%



    # df_out_skus = tuple(top_cat.SKU.unique())
    # top_in_curr_df = df[df.KASPI_ID.isin(df_out_skus)]


    ########## CHECKER added by Alikhan ###########
    conn = cx_Oracle.connect(user='CRRT', password='NLP4321###', dsn='dwhnew-db:1521/node124')
    query = f"""
        SELECT *
        FROM crrt.MATCHING_RESULT_CRON
    """

    res = pd.read_sql_query(query, con=conn)
    res=res.rename(columns={'SKU':'KASPI_ID'})
    res=res.rename(columns={'OZON_SKU':'SKU'})
    res['COMB_ID'] = res['KASPI_ID'].astype(str) + ' | ' + res['SKU'].astype(str)
    res_skus = tuple(res.COMB_ID.unique())
    df['COMB_ID'] = df['KASPI_ID'].astype(str) + ' | ' + df['SKU'].astype(str)
    df_new = df[~df.COMB_ID.isin(res_skus)]
    df_old = df[df.COMB_ID.isin(res_skus)]

    ############# логирование ##################
    # df_new_logging = df_new.shape[0]
    # df_new_skus_logging = df_new.KASPI_ID.nunique()
    # df_new_share_out_all_logging = round(df_new_logging / df.shape[0], 2)






    df = df_new

    # Чистка по пар по категориям - по новой схеме

    myconn = mysql.connector.connect(user='sainov_58623', password='VL$%_I4', 
    port = '9030',
    host='starrocksdb')
    # 4. ЧИСТКА МАТЧЕЙ ПО КАТЕГОРИЯМ
    conn = cx_Oracle.connect(user='CRRT', password='NLP4321###', dsn='dwhnew-db:1521/node124')
    id_columns = ['KASPI_ID', 'SKU']
    for c in id_columns:
        df[c] = df[c].astype(str) 
    ids = tuple(df.KASPI_ID.unique())
    ids_2 = tuple(df.SKU.unique())
    length = len(ids)
    portion = 1000
    ozon_cats = pd.DataFrame()

    for i in range(0, length, portion):
        chunk = ids[i:i + portion]
        query_main_df = f"""
            SELECT * FROM S0244.OZON_PRODUCT_PARS
            WHERE KASPI_ID IN {chunk}
        """
        subcategories_name_construct = pd.read_sql_query(query_main_df, con=myconn)
        ozon_cats = pd.concat([ozon_cats, subcategories_name_construct], ignore_index=True)
    ozon_cats.SKU = ozon_cats.SKU.astype(str)
    ozon_cats.KASPI_ID = ozon_cats.KASPI_ID.astype(str)
    ozon_cats = ozon_cats[ozon_cats.SKU.isin(ids_2)]
    ozon_cats = ozon_cats[['KASPI_ID','SKU','CATEGORIES']]

    last_iter_n = pd.merge(df, ozon_cats, how='left', on =['KASPI_ID','SKU'])

    # Данные по категориям
    query = f"""

        select * from MATCHING_CATEGORY 
        """
    categories = pd.read_sql_query(query, con=conn)

    # Исправление пропущенных значений в CATEGORY_NAME Kaspi
    categories['CAT_NAME_NEW'] = categories['CAT_NAME6']
    categories.loc[categories['CAT_NAME_NEW']=='nan', 'CAT_NAME_NEW'] = categories.loc[categories['CAT_NAME_NEW']=='nan', 'CAT_NAME5']
    categories.loc[categories['CAT_NAME_NEW']=='nan', 'CAT_NAME_NEW'] = categories.loc[categories['CAT_NAME_NEW']=='nan', 'CAT_NAME4']
    categories.loc[categories['CAT_NAME_NEW']=='nan', 'CAT_NAME_NEW'] = categories.loc[categories['CAT_NAME_NEW']=='nan', 'CAT_NAME3']
    categories.loc[categories['CAT_NAME_NEW']=='nan', 'CAT_NAME_NEW'] = categories.loc[categories['CAT_NAME_NEW']=='nan', 'CAT_NAME2']

    colum_names = [f"LEVEL{n}" for n in range(1, categories.OZON_CATEGORIES.str.split(';', expand=True).shape[1]+1)]
    categories[colum_names] = categories.OZON_CATEGORIES.str.split(';', expand=True)

    categories['CATEGORY_NAME_OZON'] = categories.LEVEL6.fillna(categories.LEVEL5)
    categories['CATEGORY_NAME_OZON'] = categories.CATEGORY_NAME_OZON.fillna(categories.LEVEL4)
    categories['CATEGORY_NAME_OZON'] = categories.CATEGORY_NAME_OZON.fillna(categories.LEVEL3)
    categories['CATEGORY_NAME_OZON'] = categories.CATEGORY_NAME_OZON.fillna(categories.LEVEL2)
    categories['CATEGORY_NAME_OZON'] = categories.CATEGORY_NAME_OZON.fillna(categories.LEVEL1)

    # Фильтрация по категории
    categories_top = categories
    # Сокращения датасета
    categories_short = categories_top.loc[:, ['CATEGORY_NAME', 'OZON_CATEGORIES', 'LEVEL1', 'CATEGORY_NAME_OZON', 'MATCH', 'CAT_NAME_NEW']]
    categories_short.columns = ['CATEGORY_NAME', 'OZON_CATEGORIES', 'LEVEL1', 'CATEGORY_NAME_OZON', 'MATCH_CAT' , 'CAT_NAME_NEW']
    categories_short.drop_duplicates(inplace=True)
    #Фильтрация данных
    categories_short = categories_short[categories_short.OZON_CATEGORIES!='nan'].reset_index(drop=True)
    categories_short.loc[categories_short.MATCH_CAT=='nan', 'MATCH_CAT'] = '0'
    categories_short.CATEGORY_NAME_OZON = categories_short.CATEGORY_NAME_OZON.str.strip()
    categories_short.sort_values(by = 'MATCH_CAT' , ascending=False, inplace=True)


    ### addition by Alikhan #####
    categories_short.loc[categories_short.CATEGORY_NAME == 'nan', 'CATEGORY_NAME'] = categories_short.CAT_NAME_NEW
    last_cats = categories_short.CATEGORY_NAME_OZON.unique()

    matches_all = last_iter_n
    #  Последняя категория
    colum_names = [f"LEVEL{n}" for n in range(1, matches_all.CATEGORIES.str.split(';', expand=True).shape[1]+1)]
    matches_all[colum_names] = matches_all.CATEGORIES.str.split(';', expand=True)
    matches_all.reset_index(drop=True, inplace=True)

    brands, results, categories_ozon = [], [], []

    for i in range(len(matches_all)):
        category_ozon_full = matches_all.CATEGORIES[i]

        last_category = ''
        level_last = ''
        brand = ''
        last_n = -1
        
        for n in range(len(colum_names),0,-1):
            level = matches_all[f'LEVEL{n}'][i]
            if level==None:
                continue
            else:
                level = level.strip()
                level_preprocc = re.sub(r'[a-zA-Z]', "", level).strip()

                if level_preprocc=="":
                    brand = level.strip()
                    continue
                else:
                    if level in last_cats:
                        last_category = level
                        break
                    elif n > last_n:
                        last_category = level
                        last_n = n
                        continue             
                    else:
                        continue
        
        if last_category=="":
            last_category = level
        elif last_category == None:
            last_category = level
        else:
            last_category = last_category.strip()
        if last_category != None:
            category_ozon_full_clear = category_ozon_full[:category_ozon_full.find(last_category)+len(last_category)].strip()

        results.append(last_category)
        brands.append(brand)
        categories_ozon.append(category_ozon_full_clear)

    matches_all.loc[:, 'CATEGORY_NAME_OZON'] = results
    matches_all.loc[:, 'CATEGORY_NAME_OZON_FULL'] = categories_ozon
    matches_all.loc[:, 'OZON_BRANDS'] = brands

    matches_all_with_cat = matches_all.merge(categories_short, how= 'left', on = ['LEVEL1', 'CATEGORY_NAME_OZON', 'CATEGORY_NAME'])
    matches_all_with_cat.drop_duplicates(inplace=True)

    #%%
    cleaned_matches = matches_all_with_cat[matches_all_with_cat.MATCH_CAT != '0']
    all_other_matches = matches_all_with_cat[matches_all_with_cat.MATCH_CAT == '0']
    

    df = cleaned_matches
    necessary_columns = ['KASPI_ID', 'SKU', 'OZON_TITLE', 'descriptions', 'ALL_CHARS', 'BRAND', 'KASPI_TITLE', 'EXT_URL', 'CATEGORY_NAME', 'CATEGORY_LEVEL1', 'COMB_ID']
    df = df[necessary_columns]
    all_other_matches = all_other_matches[necessary_columns]
    

    all_other_matches =all_other_matches.rename(columns={'CATEGORY_LEVEL1':'VERTICAL', 'descriptions': 'DESCRIPTION' , 'OZON_TITLE': 'TITLE_OZON' , 'SKU':'OZON_SKU'}) 
    all_other_matches =all_other_matches.rename(columns={'KASPI_ID':'SKU'}) 
    needed_columns = ['CATEGORY_NAME', 'SKU', 'TITLE_DETAIL', 'TITLE_OZON',
       'MATCHING_RESULT', 'CONSTRUCTOR', 'LEN_OF_MATCHED', 'LEN_OF_CHAR',
       'ALL_CHARS', 'DESCRIPTION', 'VERTICAL', 'OZON_SKU','EXT_URL']
    for column in needed_columns:
        if column not in all_other_matches.columns:
            all_other_matches[column] = None

    all_other_matches = all_other_matches[['CATEGORY_NAME', 'SKU', 'TITLE_DETAIL', 'TITLE_OZON','MATCHING_RESULT', 'CONSTRUCTOR', 'LEN_OF_MATCHED', 'LEN_OF_CHAR','ALL_CHARS', 'DESCRIPTION', 'VERTICAL', 'OZON_SKU', 'EXT_URL']]
    all_other_matches['MATCHING_RESULT'] = 'definitely_not_match'
    # 3. ЗАГРУЗКА DEFINITELY NOT MATCH В ЦРРТ
    for c in all_other_matches.columns:
        all_other_matches[c] = all_other_matches[c].astype(str)

    columns = ['ALL_CHARS','DESCRIPTION', 'TITLE_OZON']
    for c in columns:
        all_other_matches[c] = all_other_matches[c].apply(lambda x: str(x)[:2000]) 
    
    all_other_matches['TIME_STAMP'] = (datetime.now() + timedelta(hours=5)).strftime('%Y-%m-%d %H:%M:%S')
    dtype_mapping = {c: oracle.VARCHAR2(2048) for c in all_other_matches.columns}
    portion = 50000
    for i in range(all_other_matches.shape[0]//portion + 1):
        temp = all_other_matches.iloc[i*portion:(i+1)*portion]
        temp.to_sql("MATCHING_RESULT_CRON", 
                    con=engine, 
                    if_exists="append",#changed 
                    index=False, 
                    schema='crrt',
                    dtype = dtype_mapping)  
    


    ###### NEW FUNCTIONS ######
    #%%
    titles = ['OZON_TITLE']
    cat_unselect = ['Детские товары','Аптека']
    found_list =[]
    found_list_2 =[]
    for title in df[titles]:
        found = df[title].apply(lambda x: re.search(f'во(сс|с)тановле(н|нн)(ый|ая|ое|ые)', str(x)) is not None)
        found_2 = df[title].apply(lambda x: re.search(f'\+', str(x)) is not None)
        found_list.append(found)
        found_list_2.append(found_2)

    df['is_second_hand_ext'] = found_list[0]
    df['is_bundle_sales'] = found_list_2[0]

    df.loc[df.query('True == is_second_hand_ext and CATEGORY_LEVEL1.isin(@cat_unselect)').index, 'is_second_hand_ext'] = False

    #df.query('True == is_second_hand_ext')
    df = df[df.is_second_hand_ext == False]
    df = df[df.is_bundle_sales == False]

    









    df['ALL_CHARS'] = df['ALL_CHARS'].fillna('').astype(str)

    df['OZON_TITLE'] = df['OZON_TITLE'].astype(str)
    df['descriptions'] = df['descriptions'].astype(str)
    df['BRAND'] = df['BRAND'].astype(str)
    df['KASPI_TITLE'] = df['KASPI_TITLE'].astype(str)
    df['EXT_URL'] = df['EXT_URL'].astype(str)
    df.BRAND =df.BRAND.fillna('None')

    logger.info('Все ок! with pre_main_script!')
except:
    trace = traceback.format_exc()
    logger.error('\n'.join(['ERROR with pre_main script!', trace])) 




















# 2. ПРОГОН ДАННЫХ ЧЕРЕЗ АЛГОРИТМ
def main (): 
    # Предотвращаем потерю данных
    global df
    df_save = df

    metchings, skus, titles_ozon, categories, titles_kaspi_detail =  [],[], [], [], []
    values, all_chars_l, category_level, descriptions, urls = [],[], [], [], []
    len_of_mathches, lenth_of_chars, ozon_sku_l = [], [], []
    result_df_detail = pd.DataFrame({"CATEGORY_NAME": [], 
                                     "SKU": [], 
                                     "TITLE_DETAIL": [],
                                     "TITLE_OZON": [],
                                     "MATCHING_RESULT": [],
                                     "CONSTRUCTOR": [],
                                     "LEN_OF_MATCHED": [],
                                     "LEN_OF_CHAR": [],
                                     "ALL_CHARS": [],
                                     "DESCRIPTION": [],
                                     "VERTICAL": [],
                                     "OZON_SKU": [],
                                     "EXT_URL": [] }) 
    dtypes = {
    "CATEGORY_NAME":object, 
    "SKU":int, 
    "TITLE_DETAIL":object,
    "TITLE_OZON":object,
    "MATCHING_RESULT":int,
    "CONSTRUCTOR":object,
    "LEN_OF_MATCHED":int,
    "LEN_OF_CHAR": int,
    "ALL_CHARS":object,
    "DESCRIPTION":object,
    "VERTICAL":object,
    "OZON_SKU":object,
    "EXT_URL":object 
    }
    for col, dtype in dtypes.items():
        result_df_detail[col] = result_df_detail[col].astype(dtype)

    
    CATEGORY_LIST  = df.CATEGORY_NAME.unique()

    for CATEGORY_NAME in tqdm(CATEGORY_LIST):
        subcategories_name_construct = combined_df[combined_df['CATEGORY_NAME'] == CATEGORY_NAME].reset_index(drop=True)
        df_tmp_ = df[df.CATEGORY_NAME == CATEGORY_NAME].reset_index(drop=True)
        if not subcategories_name_construct.empty and not df_tmp_.empty and CATEGORY_NAME in cat_char_dict.keys():
            if 'CATEGORY_NAME' in subcategories_name_construct.columns:
                for i in range(len(df_tmp_)):
                    sku_kaspi = int(df_tmp_.KASPI_ID[i])
                    title_exp_ozon = df_tmp_.OZON_TITLE[i].lower()
                    all_chars = df_tmp_.ALL_CHARS[i].lower()
                    brand_char = df_tmp_.BRAND[i].lower()
                    ext_url_ozon = df_tmp_.EXT_URL[i].lower()
                    ozon_sku = df_tmp_.SKU[i]
                    subcategories_name_construct.SKU = subcategories_name_construct.SKU.astype(int)
                    char_kaspi_exp_df = subcategories_name_construct[subcategories_name_construct.SKU==sku_kaspi]
                    char_kaspi_exp_df.NAME = char_kaspi_exp_df.NAME.str.lower()     
                    char_kaspi_exp_df= char_kaspi_exp_df.fillna('')  
                    ozon_flag_dict = {}
                    main_chars = cat_char_dict[CATEGORY_NAME]
                    main_chars = ['BRAND'] + main_chars
                    category_level1 = df_tmp_.CATEGORY_LEVEL1[i].lower()
                    description = df_tmp_.descriptions[i].lower()
                    matched_values=[]
                    unmatched_skus = []
                    df_tmp_['OZON_TITLE_NR'] = df_tmp_['OZON_TITLE'].apply(lambda x: add_normalized_values(x))
                    df_tmp_['OZON_TITLE_NR'] = df_tmp_['OZON_TITLE_NR'].astype(str)
                    normalized = df_tmp_.OZON_TITLE_NR[i]
                    df_tmp_['OZON_TITLE_TR'] = df_tmp_['OZON_TITLE'].apply(lambda x: translit_by_letters(x))
                    df_tmp_['OZON_TITLE_TR'] = df_tmp_['OZON_TITLE_TR'].astype(str)
                    translit = df_tmp_.OZON_TITLE_TR[i]
                    title_detail = ''
                    main_keys = cat_char_dict[CATEGORY_NAME]
                    main_keys = ['BRAND'] + main_keys

                    for cat in main_chars:
                        if cat != 'unit!' and cat!= 'hyphen!' and cat != 'not_present':
                            ozon_cat_flag = 0
                            cat_value = ''
                            cat_value_nr = ''
                            cat_value_tr = ''
                            param_value = ''
    

                            if char_kaspi_exp_df.empty == False:
                                if cat == 'BRAND':
                                    cat_value = char_kaspi_exp_df.BRAND.values.tolist()[0].lower()
                                    ozon_cat_flag = 1 if (str(cat_value) in title_exp_ozon or lev(str(cat_value), str(brand_char)) <= 1) else 0
                                    title_detail += cat_value + ' | '
                                                  
                                else:
                                    if (char_kaspi_exp_df[char_kaspi_exp_df.NAME == cat].empty == False) and len(char_kaspi_exp_df[char_kaspi_exp_df.NAME == cat].PARAM.values[0])!=0:
                                        param_value = char_kaspi_exp_df.loc[char_kaspi_exp_df.NAME == cat, 'PARAM'].to_list()[0].lower()
                                        cat_value_nr = add_normalized_values(param_value)
                                    
                                        cat_value_tr = translit_by_letters(param_value)
            
                                        cat_value = param_value
                                        ozon_cat_flag = 1 if (str(cat_value) in title_exp_ozon) and (cat_value != '') else 0                                    
                        
                                    else:
                                        log_param = char_kaspi_exp_df.PARAM.to_list()[0]
                                    
                                    title_detail += cat_value + ' | ' 

                                    if ozon_cat_flag == 0 and  (cat_value != '' or cat_value_nr != '' or cat_value_tr != ''):
                                        ozon_cat_flag = 1 if str(cat_value) in all_chars or ''.join(str(cat_value).strip().split()) in all_chars  else 0
                                        if ozon_cat_flag==0:
                                            ozon_cat_flag = 1 if str(cat_value) in translit or ''.join(str(cat_value).strip().split()) in translit else 0
                                            if ozon_cat_flag==0:
                                                ozon_cat_flag = 1 if str(cat_value) in normalized or ''.join(str(cat_value).strip().split()) in normalized else 0
                                                if ozon_cat_flag ==0:
                                                    ozon_cat_flag = 1 if str(cat_value_tr) in title_exp_ozon or ''.join(str(cat_value_tr).strip().split()) in title_exp_ozon else 0 
                                                    if ozon_cat_flag == 0:
                                                        ozon_cat_flag = 1 if str(cat_value) in description or ''.join(str(cat_value).strip().split()) in description else 0
                                                    
                                if ozon_cat_flag:
                                    matched_values.append(cat_value)
                                ozon_flag_dict[cat] = ozon_cat_flag
                                
                            else:
                                unmatched_skus.append(sku_kaspi)
                
                    LEN_OF_MATCHED = len([i for i in ozon_flag_dict.values() if i!= 0])
                    LEN_OF_CHAR = len(ozon_flag_dict)
                    metching_flag = 0
                    # Все характеристики нашлись в озоне - Матч
                    if ((len(ozon_flag_dict)) == len([i for i in ozon_flag_dict.values() if i!= 0]) and (title_detail != '')):
                        metching_flag = 1

                    categories.append(CATEGORY_NAME)
                    skus.append(sku_kaspi)
                    titles_kaspi_detail.append(title_detail)
                    titles_ozon.append(title_exp_ozon)
                    metchings.append(metching_flag)
                    values.append(', '.join(matched_values))
                    all_chars_l.append(all_chars)
                    len_of_mathches.append(LEN_OF_MATCHED)
                    lenth_of_chars.append(LEN_OF_CHAR)
                    category_level.append(category_level1)
                    descriptions.append(description)
                    ozon_sku_l.append(ozon_sku)
                    urls.append(ext_url_ozon)
                temp_df = pd.DataFrame({"CATEGORY_NAME":categories, 
                                        "SKU":skus, 
                                        "TITLE_DETAIL": titles_kaspi_detail, 
                                        "TITLE_OZON":titles_ozon, 
                                        "MATCHING_RESULT":metchings,
                                        "CONSTRUCTOR":values,
                                        "LEN_OF_MATCHED":len_of_mathches, 
                                        "LEN_OF_CHAR":lenth_of_chars, 
                                        "ALL_CHARS":all_chars_l,        
                                        "DESCRIPTION": descriptions,
                                        "VERTICAL":category_level,
                                        "OZON_SKU":ozon_sku_l,
                                        "EXT_URL":urls
                                        })        
                result_df_detail = pd.concat([result_df_detail, temp_df], ignore_index=True)
                result_df_detail = result_df_detail.drop_duplicates()
                result_df_detail = result_df_detail[result_df_detail.TITLE_OZON != '']
            else: 
                continue
        else:
            continue
        
    result_df_detail.SKU = result_df_detail.SKU.astype(str)
    result_df_detail['COMB_ID'] = result_df_detail['SKU'] + ' | ' + result_df_detail['OZON_SKU']

    result_skus = tuple(result_df_detail.COMB_ID.unique())
    lost_df = df_save[~df_save.COMB_ID.isin(result_skus)].reset_index(drop=True)

    


    lost_df =lost_df.rename(columns={'CATEGORY_LEVEL1':'VERTICAL', 'descriptions': 'DESCRIPTION' , 'OZON_TITLE': 'TITLE_OZON' , 'SKU':'OZON_SKU'}) 
    lost_df =lost_df.rename(columns={'KASPI_ID':'SKU'})
    needed_columns = ['CATEGORY_NAME', 'SKU', 'TITLE_DETAIL', 'TITLE_OZON',
       'MATCHING_RESULT', 'CONSTRUCTOR', 'LEN_OF_MATCHED', 'LEN_OF_CHAR',
       'ALL_CHARS', 'DESCRIPTION', 'VERTICAL', 'OZON_SKU','EXT_URL']
    for column in needed_columns:
        if column not in lost_df.columns:
            lost_df[column] = None

    lost_df = lost_df[['CATEGORY_NAME', 'SKU', 'TITLE_DETAIL', 'TITLE_OZON','MATCHING_RESULT', 'CONSTRUCTOR', 'LEN_OF_MATCHED', 'LEN_OF_CHAR','ALL_CHARS', 'DESCRIPTION', 'VERTICAL', 'OZON_SKU', 'EXT_URL']]   
    lost_df['MATCHING_RESULT'] = 'no_content'
    result_df_detail.drop('COMB_ID', axis=1, inplace=True)
    result_df_detail_a = pd.concat([result_df_detail, lost_df])
    result_df_detail = result_df_detail_a

    engine = create_engine("oracle+cx_oracle://CRRT:NLP4321###@dwhnew-db:1521/?service_name=node124")
    conn = engine.connect()


    # 3. ЗАГРУЗКА ВЫХЛОПА АЛГОРИТМА В ЦРРТ
    for c in result_df_detail.columns:
        result_df_detail[c] = result_df_detail[c].astype(str)

    columns = ['ALL_CHARS','DESCRIPTION', 'TITLE_OZON']
    for c in columns:
        result_df_detail[c] = result_df_detail[c].apply(lambda x: str(x)[:2000]) 
    
    result_df_detail['TIME_STAMP'] = (datetime.now() + timedelta(hours=5)).strftime('%Y-%m-%d %H:%M:%S')
    dtype_mapping = {c: oracle.VARCHAR2(2048) for c in result_df_detail.columns}
    portion = 50000
    for i in range(result_df_detail.shape[0]//portion + 1):
        temp = result_df_detail.iloc[i*portion:(i+1)*portion]
        temp.to_sql("MATCHING_RESULT_CRON", 
                    con=engine, 
                    if_exists="append",#changed 
                    index=False, 
                    schema='crrt',
                    dtype = dtype_mapping)


    # 4. ЧИСТКА МАТЧЕЙ ПО КАТЕГОРИЯМ
    conn = cx_Oracle.connect(user='CRRT', password='NLP4321###', dsn='dwhnew-db:1521/node124')
    result_df_detail = result_df_detail.rename(columns={'SKU':'KASPI_ID'})
    result_df_detail.KASPI_ID = result_df_detail.KASPI_ID.astype(str)
    result_df_detail.OZON_SKU = result_df_detail.OZON_SKU.astype(str)
    
    ids = tuple(result_df_detail.KASPI_ID.unique())
    ids_2 = tuple(result_df_detail.OZON_SKU.unique())
    length = len(ids)
    portion = 1000
    ozon_cats = pd.DataFrame()
    myconn = mysql.connector.connect(user='sainov_58623', password='VL$%_I4', 
    port = '9030',
    host='starrocksdb')
    for i in range(0, length, portion):
        chunk = ids[i:i + portion]
        query_main_df = f"""
            SELECT * FROM S0244.OZON_PRODUCT_PARS
            WHERE KASPI_ID IN {chunk}
        """
        subcategories_name_construct = pd.read_sql_query(query_main_df, con=myconn)
        ozon_cats = pd.concat([ozon_cats, subcategories_name_construct], ignore_index=True)
    ozon_cats['SKU'] = ozon_cats['SKU'].astype(str) 
    ozon_cats['KASPI_ID'] = ozon_cats['KASPI_ID'].astype(str)
    ozon_cats = ozon_cats[ozon_cats.SKU.isin(ids_2)]
    ozon_cats = ozon_cats[['KASPI_ID','SKU','CATEGORIES']]

    result_df_detail=result_df_detail.rename(columns={'OZON_SKU':'SKU'})
    last_iter_n = pd.merge(result_df_detail, ozon_cats, how='left', on =['KASPI_ID','SKU'])

    # Данные по категориям
    query = f"""

        select * from MATCHING_CATEGORY 
        """
    categories = pd.read_sql_query(query, con=conn)

    # Исправление пропущенных значений в CATEGORY_NAME Kaspi
    categories['CAT_NAME_NEW'] = categories['CAT_NAME6']
    categories.loc[categories['CAT_NAME_NEW']=='nan', 'CAT_NAME_NEW'] = categories.loc[categories['CAT_NAME_NEW']=='nan', 'CAT_NAME5']
    categories.loc[categories['CAT_NAME_NEW']=='nan', 'CAT_NAME_NEW'] = categories.loc[categories['CAT_NAME_NEW']=='nan', 'CAT_NAME4']
    categories.loc[categories['CAT_NAME_NEW']=='nan', 'CAT_NAME_NEW'] = categories.loc[categories['CAT_NAME_NEW']=='nan', 'CAT_NAME3']
    categories.loc[categories['CAT_NAME_NEW']=='nan', 'CAT_NAME_NEW'] = categories.loc[categories['CAT_NAME_NEW']=='nan', 'CAT_NAME2']

    colum_names = [f"LEVEL{n}" for n in range(1, categories.OZON_CATEGORIES.str.split(';', expand=True).shape[1]+1)]
    categories[colum_names] = categories.OZON_CATEGORIES.str.split(';', expand=True)

    categories['CATEGORY_NAME_OZON'] = categories.LEVEL6.fillna(categories.LEVEL5)
    categories['CATEGORY_NAME_OZON'] = categories.CATEGORY_NAME_OZON.fillna(categories.LEVEL4)
    categories['CATEGORY_NAME_OZON'] = categories.CATEGORY_NAME_OZON.fillna(categories.LEVEL3)
    categories['CATEGORY_NAME_OZON'] = categories.CATEGORY_NAME_OZON.fillna(categories.LEVEL2)
    categories['CATEGORY_NAME_OZON'] = categories.CATEGORY_NAME_OZON.fillna(categories.LEVEL1)

    # Фильтрация по категории
    categories_top = categories
    # Сокращения датасета
    categories_short = categories_top.loc[:, ['CATEGORY_NAME', 'OZON_CATEGORIES', 'LEVEL1', 'CATEGORY_NAME_OZON', 'MATCH', 'CAT_NAME_NEW']]
    categories_short.columns = ['CATEGORY_NAME', 'OZON_CATEGORIES', 'LEVEL1', 'CATEGORY_NAME_OZON', 'MATCH_CAT' , 'CAT_NAME_NEW']
    categories_short.drop_duplicates(inplace=True)
    #Фильтрация данных
    categories_short = categories_short[categories_short.OZON_CATEGORIES!='nan'].reset_index(drop=True)
    categories_short.loc[categories_short.MATCH_CAT=='nan', 'MATCH_CAT'] = '0'
    categories_short.CATEGORY_NAME_OZON = categories_short.CATEGORY_NAME_OZON.str.strip()
    categories_short.sort_values(by = 'MATCH_CAT' , ascending=False, inplace=True)


    ### addition by Alikhan #####
    categories_short.loc[categories_short.CATEGORY_NAME == 'nan', 'CATEGORY_NAME'] = categories_short.CAT_NAME_NEW
    last_cats = categories_short.CATEGORY_NAME_OZON.unique()

    matches_all = last_iter_n
    #  Последняя категория
    colum_names = [f"LEVEL{n}" for n in range(1, matches_all.CATEGORIES.str.split(';', expand=True).shape[1]+1)]
    matches_all[colum_names] = matches_all.CATEGORIES.str.split(';', expand=True)
    matches_all.reset_index(drop=True, inplace=True)

    brands, results, categories_ozon = [], [], []

    for i in range(len(matches_all)):
        category_ozon_full = matches_all.CATEGORIES[i]
        
        last_category = ''
        level_last = ''
        brand = ''
        last_n = -1
        category_ozon_full_clear = ''
        
        for n in range(len(colum_names),0,-1):
            level = matches_all[f'LEVEL{n}'][i]
            if level==None:
                continue
            else:
                level = level.strip()
                level_preprocc = re.sub(r'[a-zA-Z]', "", level).strip()

                if level_preprocc=="":
                    brand = level.strip()
                    continue
                else:
                    if level in last_cats:
                        last_category = level
                        break
                    elif n > last_n:
                        last_category = level
                        last_n = n
                        continue             
                    else:
                        continue
        
        if last_category=="":
            last_category = level
        elif last_category == None:
            last_category = level
        else:
            last_category = last_category.strip()
        if last_category != None:
            category_ozon_full_clear = category_ozon_full[:category_ozon_full.find(last_category)+len(last_category)].strip()

        results.append(last_category)
        brands.append(brand)
        categories_ozon.append(category_ozon_full_clear)

    matches_all.loc[:, 'CATEGORY_NAME_OZON'] = results
    matches_all.loc[:, 'CATEGORY_NAME_OZON_FULL'] = categories_ozon
    matches_all.loc[:, 'OZON_BRANDS'] = brands
    
    matches_all_with_cat = matches_all.merge(categories_short, how= 'left', on = ['LEVEL1', 'CATEGORY_NAME_OZON', 'CATEGORY_NAME'])
    matches_all_with_cat.drop_duplicates(inplace=True)
    matches_all_with_cat.MATCHING_RESULT =matches_all_with_cat.MATCHING_RESULT.astype(str) 
    matches_only = matches_all_with_cat[matches_all_with_cat.MATCHING_RESULT == '1']
    matches_only['is_correct'] = matches_only.MATCH_CAT
    ones = matches_only
    ones['source'] = 'ozon'
    ones=ones[['KASPI_ID','SKU','EXT_URL','TITLE_OZON','source','is_correct']]
    ones = ones.rename(columns={'KASPI_ID':'kaspi_sku', 'SKU':'ext_sku','TITLE_OZON':'ext_title','EXT_URL':'ext_url'})
    ones.is_correct=ones.is_correct.astype(str) 
    final_to_business_m = ones
    
    ids = tuple(final_to_business_m['ext_sku'].unique())



    ########### ЧИСТКА ПО ЦЕНАМ ##############
    ozon_price = pd.DataFrame()
    for i in range(0, len(ids), 1000):
        chunk = ids[i:i + 1000]
        query_main_df = f"""
        SELECT SKU, PRICE FROM S0244.OZON_PRODUCT_PARS
        WHERE SKU IN {chunk}
        """
        subcategories_name_construct = pd.read_sql_query(query_main_df, con=myconn)
        ozon_price = pd.concat([ozon_price, subcategories_name_construct], ignore_index=True)         
    ozon_price.SKU = ozon_price.SKU.astype(str)
    
    delim = ozon_price['PRICE'].str.split(' ', expand=True)
    ozon_price['PRICE'] = delim[0]
    ozon_price.PRICE =ozon_price.PRICE.astype(float) 
    ozon_prices_median=ozon_price.groupby('SKU').PRICE.median()
    ozon_prices = pd.DataFrame(ozon_prices_median).reset_index()

    skus = tuple(final_to_business_m['kaspi_sku'].unique())
    kaspi_prices = pd.DataFrame()
    for i in range(0, len(skus), 1000):
        chunk = skus[i:i + 1000]
        query = f"""
        select ARTICLE_NUMBER, PARTNER_PRICE
        from sasuser.SHOP_MERCHANT_ITEM@dwhsas
        where ARTICLE_NUMBER IN {chunk}
        """
        subcategories_name_construct = pd.read_sql_query(query, con=conn)
        kaspi_prices = pd.concat([kaspi_prices, subcategories_name_construct], ignore_index=True)        

    kaspi_prices.rename(columns = {'ARTICLE_NUMBER':'KASPI_ID'}, inplace=True)
    kaspi_prices.KASPI_ID = kaspi_prices.KASPI_ID.astype(str)

    prices_median = kaspi_prices.groupby('KASPI_ID').PARTNER_PRICE.median()
    kaspi_prices = pd.DataFrame(prices_median).reset_index()


    final_to_business_m = pd.merge(final_to_business_m, ozon_prices, how='left', left_on =['ext_sku'], right_on ='SKU')
    final_to_business_m = pd.merge(final_to_business_m, kaspi_prices, how='left', left_on =['kaspi_sku'], right_on='KASPI_ID')

    final = final_to_business_m
    final = final.rename(columns={'PRICE':'PRICE_OZON','PARTNER_PRICE':'KASPI_PRICE' })

    final['BASE'] = ''
    final['BASE'] = final.apply(
        lambda row: row['PRICE_OZON'] if (row['PRICE_OZON'] > row['KASPI_PRICE'] and pd.notna(row['PRICE_OZON']) and pd.notna(row['KASPI_PRICE'])) else row['KASPI_PRICE'],
        axis=1
    )
    final['REL_DIFF'] =  round(abs(final['KASPI_PRICE'] - final['PRICE_OZON']) / final['BASE'],2)
    final['suspicious_price'] = final.apply(lambda row: 1 if row['REL_DIFF'] >= 0.60 else 0, axis=1)
    final.suspicious_price =final.suspicious_price.astype(str) 

    final=final[['kaspi_sku','ext_sku','ext_url','ext_title','source','is_correct','suspicious_price']]
    final_to_business_m = final


    # 5. ЗАГРУЗКА ДАННЫХ ПО МАТЧАМ В KAFKA 
    myconn=mysql.connector.connect(user='crrt',
    password='4g1q6Dc=c7',
    port='9030',
    host='starrocksdb') 

    query = """
    select *
    from crrt_db.MATCHING_SOURCE_CODES
    """
    src_map = pd.read_sql_query(query, con=myconn)
    src_map = {k: v for k, v in zip(src_map['source'], src_map['code'])}

    # Функция генерации уникальных ключей
    def generate_uuid(source, kaspi_sku, ext_sku):
        return f'{src_map[source]}-{kaspi_sku}-{ext_sku}' 
    uuids = [generate_uuid(row['source'], row['kaspi_sku'], row['ext_sku']) for i, row in final_to_business_m.iterrows()]
    final_to_business_m['uuid'] = uuids

    topic_to = 'IN.S0244.OZON_RESULTS_MAPPING'
    # Функция заливки в kafka
    
    producer=KafkaProducer(
                bootstrap_servers = ['dwh-kfk5-lp2.hq.bc:9093','dwh-kfk6-lp1.hq.bc:9093','dwh-kfk7-lp2.hq.bc:9093'],  # Укажите адрес и порт вашего Kafka брокера
                value_serializer=lambda v:str(v).encode('utf-8'), # Сериализатор для значений (в данном случае, преобразуем в байты)
                sasl_plain_username='S0212.CRTR_USER',
                sasl_plain_password='gaD6eMIq,EtWIb',
                sasl_mechanism='SCRAM-SHA-256',
                security_protocol='SASL_PLAINTEXT',
                api_version=(0, 10, 1),
                linger_ms=500
    )

    # Проверка на наличие уже в кафке каспи и озон ску комбинаций
    query = f"""
    SELECT * FROM S0244.OZON_RESULTS_MAPPING
    
    """
    res = pd.read_sql_query(query, con=myconn)

    res.d_mes = res.d_mes.apply( lambda x: json.loads(x) if isinstance (x, str) else x)
    res['ext_sku'] = res.apply(lambda row : row['d_mes'].get('ext_sku'), axis=1)
    res['kaspi_sku'] = res.apply(lambda row : row['d_mes'].get('kaspi_sku'), axis=1)
    

    sel = ['ext_sku', 'kaspi_sku']
    for c in sel:
        res[c] = res[c].astype(str)

    res['comb_id'] = res['ext_sku'] + ' | ' + res['kaspi_sku']
    res_skus = tuple(res.comb_id.unique())
    final_to_business_m['comb_id'] = final_to_business_m['ext_sku'] + final_to_business_m['kaspi_sku']
    final_to_business_m = final_to_business_m[~final_to_business_m.comb_id.isin(res_skus)]
    final_to_business_m.drop('comb_id', axis=1, inplace=True)
    # Kafka topic
    for i in range(len(final_to_business_m)):
        example = final_to_business_m.iloc[i, :] 
        uuid_value = example['uuid'] 
        example_dict = example.to_dict()
        example_json = json.dumps(example_dict) 
        producer.send(topic_to, headers=None, key=uuid_value.encode('utf-8'), value=example_json)

    
    ############# логирование ##################
    handler.toaddrs = ['Alikhan.Sainov@kaspi.kz']


    all_rows = matches_all_with_cat.shape[0]
    m_rows = matches_only.shape[0]
    matches_logging = matches_only[matches_only.is_correct == '1'].shape[0]
    

    if matches_logging != 0:
        logger.info('\n'.join([f'За день мы получили: {log_time_info} пар матчей с данных парсинга',
                               f'Всего пар: {all_rows}', 
                               f'До чистки матчей: {m_rows} и после чистки матчей загружено в KAFKA: {matches_logging}']))
        print ('Все ок KAFKA обновлена данными')
    else:
        trace = traceback.format_exc()
        logger.error('\n'.join([f'За день мы получили: {log_time_info} пар матчей с данных парсинга',
                                f'После прогона не было получено новых матчей в KAFKA', 
                                f'Итого пар: {all_rows}, до чистки матчей: {m_rows} и после чистки матчей: {matches_logging}'])) 


#%%


# Launch and manage errors
if __name__ == '__main__':
    # Create logger
    receivers = [
        'Alikhan.Sainov@kaspi.kz'
    ]

    handler = SMTPHandler(mailhost='relay2.bc.kz',
                          fromaddr='reglament_info@kaspi.kz',
                          toaddrs=receivers,
                          subject='Service Matching')

    logger = logging.getLogger('logger-1')
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)

    try:
        main()
        # logger.info('Все ок!')
    except:
        trace = traceback.format_exc()
        logger.error('\n'.join(['ERROR!', trace])) 







