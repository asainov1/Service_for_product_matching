
#%% Импорт библиотек
import os
import pandas as pd
import mysql.connector
import cx_Oracle
from datetime import datetime, timedelta
import re
import pytz
import json
from urllib import request
import logging
import traceback
import polars as pl
import time
#%% Подключение учеток
global myconn, conn






#%% 

# Cron note
# ... bash -c "...; LAUNCH_NUM=first python3 main_algo.py"
"""
4 8 12 16 20 0
"""
tz = pytz.timezone('Asia/Karachi')

logger = logging.getLogger('logger-1')
def issue_dataframe (conn):
    global myconn
    cursor = conn.cursor()
    tz = pytz.timezone('Asia/Karachi')
    # if os.environ['LAUNCH_NUM'] == 'first':
    #     start_time_str = (datetime.now() - timedelta(days=2)).astimezone(tz).date().strftime(r'%Y-%m-%d') + ' 12:00:00'
    #     end_time_str = (datetime.now() - timedelta(days=2)).astimezone(tz).date().strftime(r'%Y-%m-%d') + ' 13:59:59' # to change 
    # elif os.environ['LAUNCH_NUM'] == 'second':
    #     start_time_str = (datetime.now() - timedelta(days=2)).astimezone(tz).date().strftime(r'%Y-%m-%d') + ' 18:00:00'
    #     end_time_str = (datetime.now() - timedelta(days=2)).astimezone(tz).date().strftime(r'%Y-%m-%d') + ' 23:59:59'

    start_time_str = (datetime.now() - timedelta(days=2)).astimezone(tz).date().strftime(r'%Y-%m-%d') + ' 18:00:00'
    end_time_str = (datetime.now() - timedelta(days=2)).astimezone(tz).date().strftime(r'%Y-%m-%d') + ' 23:59:59'




    # Выгрузка lazy фрэйма 
    query_main_df = f"""
        SELECT distinct
        t1.KASPI_ID, t1.ID,
        get_json_string(t1.COL_DATA, '$.title') as WB_TITLE,
        get_json_string(t1.COL_DATA, '$.description') as descriptions,
        get_json_string(t1.COL_DATA, '$.brand') as BRAND,
        get_json_string(t1.COL_DATA, '$.product_url') as PRODUCT_URL,
        get_json_string(t1.COL_DATA, '$.category.name') as `CATEGORY#NAME`
        FROM S0244.WB_PRODUCT_PARS_JSON t1
        WHERE TIMESTAMP(S$CHANGE_DATE) BETWEEN '{start_time_str}' AND '{end_time_str}' 
    """
    df = pl.read_database(query_main_df, connection=myconn).lazy()


    # логирование 
    if df.select(pl.len()).collect().item():
        logger.info('Все ок DATA_INFLOW-WB не пустой!')
        print ('Все ок DATA_INFLOW-WB не пустой!')
    else:
        trace = traceback.format_exc()
        logger.error('\n'.join(['ERROR - DATA_INFLOW-WB пустой!', trace])) 

    # Форматируем
    df = df.with_columns([
        pl.col("KASPI_ID").cast(pl.Utf8),
        pl.col("ID").cast(pl.Utf8)
    ])
    


    # Запрос в таблицу PRODUCT_PARS_CHARACT

    df_char_list = []
    skus = df.select("KASPI_ID").unique().collect()["KASPI_ID"].to_list()

    query_main_df_2_template = """
        SELECT DISTINCT KASPI_ID, ID, `CHARACTERISTICS#NAME` as name, `CHARACTERISTICS#VALUE` as value
        FROM S0244.WB_PRODUCT_PARS_CHARACT 
        WHERE KASPI_ID IN {chunk}
    """
    for i in range(0, len(skus), 500):
        chunk = tuple(skus[i : i + 500])
        query_main_df_2 = query_main_df_2_template.format(chunk=str(chunk))
        df_sub = pl.read_database(query_main_df_2, connection=myconn).lazy()
        df_char_list.append(df_sub)
    df_char = pl.concat(df_char_list)

    # Дропаем дубли
    df_char = df_char.unique()
    df_char = df_char.sql(
        """
         select * from self
         where not (
         lower(name) regexp '.*артикул.*|.*article.*|^oem.*|партномер'
        )
        """)
    # Группировка характеристик #str...
    df_char = df_char.with_columns(
        (pl.col("name") + " : " + pl.col("value")).alias("Combined_Characteristics")
    ).select(["KASPI_ID", "ID", "Combined_Characteristics"])





    # Группировка характеристик
    df_char_groupped = df_char.group_by(["KASPI_ID", "ID"]).agg(
    pl.col("Combined_Characteristics").str.concat(" | ")
    )
    # Merge функция
    df = df.join(df_char_groupped, on=["KASPI_ID", "ID"], how="left")




    if isinstance (skus, list):
        skus = [(str(sku), ) for sku in skus] 
    #skus
    cursor = conn.cursor()
    query = f"""
        create global temporary table CRRT.ALIKHAN_TEMP_TABLE_1 (
            sku varchar(512)
        )
        on commit preserve rows
    """

    try:
        cursor.execute(query)
        conn.commit()
    except:
        pass

    batch_size = 1000

    query = f"""
        insert into CRRT.ALIKHAN_TEMP_TABLE_1 (sku)
        values (:1)
    """
    for i in range(0, len(skus), batch_size):
        cursor.executemany(query, skus[i : i + batch_size])
        conn.commit()

    query = f"""
        SELECT t1.SKU, t3.CATEGORY_NAME, t3.CAT_NAME2
        FROM sasuser.SHOP_ALL_ITEMS_CAT_REL@dwhsas t1
        LEFT JOIN sasuser.SEARCH_SHOP_CATEGORY_ALL_NEW@dwhsas t3
        ON t3.CATEGORY_ID = t1.CATEGORY_CODE
        WHERE to_char(t1.SKU) in (
            select distinct sku
            from CRRT.ALIKHAN_TEMP_TABLE_1)
        
    """
    cats = pl.read_database(query, conn).lazy()

    # chars.collect().write_parquet(os.path.join(checkpt_path, 'chars.parquet'))

    cursor.close()
    conn.close()


    df = df.join(cats, left_on="KASPI_ID", right_on='SKU' ,how="left")
    # Rename columns
    df = df.rename({"PRODUCT_URL": "EXT_URL", "Combined_Characteristics": "ALL_CHARS", "CAT_NAME2": "CATEGORY_LEVEL1"})
    # Convert necessary columns to string
    df = df.with_columns([
        pl.col("WB_TITLE").cast(pl.Utf8),
        pl.col("descriptions").cast(pl.Utf8),
        pl.col("ALL_CHARS").cast(pl.Utf8),
        pl.col("EXT_URL").cast(pl.Utf8),
        pl.col("CATEGORY_LEVEL1").cast(pl.Utf8)
    ])
    
    return df




def read_chunk (chunk):
    global myconn, conn
    query_1 = f"""
        SELECT t1.catalog_id as SKU, t1.param as PARAM, t1.name as NAME, t1.display_name_ru as DISPLAY_NAME_RU               
        FROM DP.SEARCH_PARAM_SHOP t1
        WHERE t1.catalog_id IN {tuple(chunk)}
    """

    query_2 = f"""
            SELECT t1.SKU, t3.CATEGORY_NAME, t1.BRAND
            FROM sasuser.SHOP_ALL_ITEMS_CAT_REL@dwhsas t1
            LEFT JOIN sasuser.SEARCH_SHOP_CATEGORY_ALL_NEW@dwhsas t3
                ON t3.CATEGORY_ID = t1.CATEGORY_CODE
            WHERE t1.SKU IN {tuple(chunk)}
        """


    subcategories_name_construct_1 = pl.read_database(query_1, connection=myconn).lazy().with_columns(pl.col('SKU').cast(pl.Utf8))
    subcategories_name_construct_2 = pl.read_database(query_2, connection=conn).lazy().with_columns(pl.col('SKU').cast(pl.Utf8))
    
    combined_lazy = subcategories_name_construct_1.join(subcategories_name_construct_2, on='SKU', how='left')

    return combined_lazy

def issue_kaspi_params(df):
    combined_df=[]
    kaspi_id_list = df.select('KASPI_ID').unique().collect()["KASPI_ID"].to_list()
    length = len(kaspi_id_list)
    portion_size = 500
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
    combined_df = pl.concat(combined_df)
    return combined_df



