
#%%
import re
import argparse
import pandas as pd
from sqlalchemy import create_engine
import mysql.connector
import cx_Oracle
from search_normalization_shop.norm_main import normal_suggest
from search_transliteration_shop.trans_main import trnslt
import pandas as pd
from datetime import datetime, timedelta
from data_inflow_wb import issue_kaspi_params, read_chunk
from Levenshtein import distance as lev
from tqdm import tqdm
from urllib import request
from urllib.parse import quote
import base64
from sqlalchemy import create_engine
from Levenshtein import distance as lev
import warnings 
warnings.filterwarnings('ignore')
import numpy as np 
from sqlalchemy.dialects import oracle
from kafka import KafkaProducer, KafkaConsumer
import json
from pytz import timezone
import polars as pl
import traceback
import logging
from logging.handlers import SMTPHandler
from pytz import timezone  
import pytz
from sqlalchemy.types import VARCHAR, TIMESTAMP
receivers = [
    'Alikhan.Sainov@kaspi.kz'
]

handler = SMTPHandler(..)

logger = logging.getLogger('logger-1')
logger.setLevel(logging.INFO)
logger.addHandler(handler)



#%%
try:
    engine = create_engine("oracle+cx_oracle://CRRT:NLP4321###@dwhnew-db:1521/?service_name=node124")
    global myconn, conn 

    parser = argparse.ArgumentParser()
    parser.add_argument('--sos')
    args = parser.parse_args()
    sos = int(args.sos)

    df = pl.scan_csv(f'wb/multi_proc/wb_dfs_0/df_{sos}.csv', dtypes={"ID":pl.Utf8})

    # df = df.filter(pl.col("ID") != "notfound")
    df = df.with_columns(
        pl.col("KASPI_ID").cast(pl.Utf8), 
        pl.col("ID").cast(pl.Utf8)
    )

    # cat_char_dict = issue_cat_char_dict(df)
    combined_df = issue_kaspi_params(df)
    
    #%%
    
    ############### Integration of search services by Ilyas ###############
    import pymorphy2
    translit_dict = {
    'a': 'а', 'b': 'б', 'c': 'ц', 'd': 'д', 'e': 'е', 'f': 'ф', 'g': 'г', 'h': 'х',
    'i': 'и', 'j': 'й', 'k': 'к', 'l': 'л', 'm': 'м', 'n': 'н', 'o': 'о', 'p': 'п',
    'q': 'к', 'r': 'р', 's': 'с', 't': 'т', 'u': 'у', 'v': 'в', 'w': 'в', 'x': 'х',
    'y': 'й', 'z': 'з','ck':'к','sh':'ш','ch':'ч','zh':'ж','ie':'и','qu':'кв','ee':'и','oo':'у',
    'ai':'ай','oi':'ой','ei':'ей','th':'т','ph':'ф','kh':'х','ya':'я'
    }
    def translit_by_letters(word):
        transliterated = []
        i = 0
        while i < len(word):
            # Check for 2-character patterns first
            if i < len(word) - 1 and word[i:i + 2] in translit_dict:
                transliterated.append(translit_dict[word[i:i + 2]])
                i += 2
            else:
                # Single character transliteration
                transliterated.append(translit_dict.get(word[i].lower(), word[i]))
                i += 1
        return ''.join(transliterated).lower()
    morph = pymorphy2.MorphAnalyzer()

    morph_cache = {} 
    def add_normalized_values(text):
        words = text.split()
        normalized_words = []
        for word in words:
            if word not in morph_cache:  # Проверка наличия слова в кэше
                morph_cache[word] = str(morph.parse(word)[0].normal_form if morph.word_is_known(word) else word)
            normalized_words.append(morph_cache[word])
        return " ".join(normalized_words)

    #######################################################################

    #%%
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

    def color_preprocess_polars(text):
        if text is None:
            return None
        words = text.lower().split()
        words = [color_dict.get(word, word) for word in words] # Replace if in dictionary
        return " ".join(words).strip()

    # Model preprocessing function for Polars
    def model_preprocess_polars(text):
        if text is None:
            return None
        text = text.lower()
        text = text.replace('redmi 9c 3', 'redmi 9c')
        for word in ["slim box", "5g 6", "5g", "4g", "new", "dual sim", "/подарок",
        "y27s + y27s", "c65 + note 50 + m28", "white edition", "wwv ru"]:
            text = text.replace(word, "")
        return text.strip()


    def volume_preprocess_polars(text):
        if text is None:
            return None
        text = text.lower()
        text = text.replace("мл", "").replace(".0", "").strip()
        return text


    df = df.with_columns([
        pl.col("WB_TITLE").map_elements(color_preprocess_polars).alias("WB_TITLE"),
        pl.col("descriptions").map_elements(model_preprocess_polars).alias("descriptions"),
        pl.col("ALL_CHARS").map_elements(color_preprocess_polars).alias("ALL_CHARS")
        ])


    combined_df = combined_df.with_columns([
        pl.col("PARAM").map_elements(volume_preprocess_polars).alias("PARAM")
        ])

    #%%
    ######## CHECKER added by Alikhan ###########
    conn = cx_Oracle.connect(user='CRRT', password='NLP4321###', dsn='dwhnew-db:1521/node124')
    query = f"""
        SELECT KASPI_ID, ID FROM crrt.MATCHING_RESULT_CRON_WB
    """
    res = pl.read_database(query, connection=conn).lazy()
    res = res.with_columns(
        (pl.col("KASPI_ID").cast(pl.Utf8) + " | " + pl.col("ID").cast(pl.Utf8)).alias("COMB_ID")
    )
    res_skus = set(res.select("COMB_ID").unique().collect().to_series().to_list())
    df = df.with_columns(
        (pl.col("KASPI_ID").cast(pl.Utf8) + " | " + pl.col("ID").cast(pl.Utf8)).alias("COMB_ID")
    )
    df = df.filter(~pl.col("COMB_ID").is_in(res_skus))   

    del res, res_skus




    # # 4. ЧИСТКА МАТЧЕЙ ПО КАТЕГОРИЯМ
   
    df = df.with_columns(pl.col("KASPI_ID").cast(pl.Utf8), pl.col("ID").cast(pl.Utf8))
    
    
    # To correct
    query = """
        SELECT * FROM MATCHING_CATEGORY_WB
    """
    categories = pl.read_database(query, connection=conn).lazy()

    categories_short = (
        categories
        .unique(subset=None)  # Drop duplicates
        .sort(by="MATCH", descending=True)  # Sort by MATCH in descending order
    )

    
    matches_all = df.clone()
    categories_short = categories_short.with_columns(pl.col("CAT_NAME2").str.to_lowercase())
    matches_all = matches_all.with_columns(pl.col("CATEGORY_LEVEL1").str.to_lowercase())


    matches_all_with_cat = matches_all.join(
        categories_short,
        how="left",
        left_on=["CATEGORY_LEVEL1", "CATEGORY#NAME", "CATEGORY_NAME"],
        right_on=["CAT_NAME2", "CATEGORY_NAME_OZON", "CATEGORY_NAME"]
    ).unique()  # Drop duplicates

    
    cleaned_matches = matches_all_with_cat.filter(pl.col("MATCH") == "1")
    all_other_matches = matches_all_with_cat.filter(pl.col("MATCH") == "0")

    
    necessary_columns = [
        "KASPI_ID", "ID", "WB_TITLE", "descriptions", "ALL_CHARS",
        "BRAND", "EXT_URL", "CATEGORY#NAME", "CATEGORY_NAME", "CATEGORY_LEVEL1", "COMB_ID"
    ]
    df = cleaned_matches.select(necessary_columns)
    all_other_matches = all_other_matches.select(necessary_columns)

    
    all_other_matches = all_other_matches.rename({
        "CATEGORY_LEVEL1": "VERTICAL",
        "descriptions": "DESCRIPTION",
        "WB_TITLE": "TITLE_WB"
    })

  
    needed_columns = [
        "CATEGORY_NAME", "KASPI_ID", "TITLE_DETAIL", "TITLE_WB",
        "MATCHING_RESULT", "CONSTRUCTOR", "LEN_OF_MATCHED", "LEN_OF_CHAR",
        "ALL_CHARS", "DESCRIPTION", "VERTICAL", "ID", "EXT_URL"
    ]
    for column in needed_columns:
        if column not in all_other_matches.columns:
            all_other_matches = all_other_matches.with_columns(
                pl.lit(None).alias(column)
            )


    all_other_matches = all_other_matches.select([
        "CATEGORY_NAME", "KASPI_ID", "TITLE_DETAIL", "TITLE_WB",
        "MATCHING_RESULT", "CONSTRUCTOR", "LEN_OF_MATCHED", "LEN_OF_CHAR",
        "ALL_CHARS", "DESCRIPTION", "VERTICAL", "ID", "EXT_URL"
    ])
    all_other_matches = all_other_matches.with_columns(
        pl.lit("definitely_not_match").alias("MATCHING_RESULT")
    )

   
    columns = ["ALL_CHARS", "DESCRIPTION", "TITLE_WB"]
    for col in columns:
        all_other_matches = all_other_matches.with_columns(
            pl.col(col).str.slice(0, 2000).alias(col)  # Use str.slice for truncation
        )

    ### to return !!! #####

    all_other_matches = all_other_matches.with_columns(
        pl.lit((datetime.now() + timedelta(hours=5)).strftime('%Y-%m-%d %H:%M:%S')).alias("TIME_STAMP")
    )
    
    dtype_mapping = {c: oracle.VARCHAR2(2048) for c in all_other_matches.columns}
    portion = 50000

    
    for i in range(0, all_other_matches.select(pl.len()).collect().item(), portion):
        temp = all_other_matches.slice(i, portion)
        temp.collect().to_pandas().to_sql(
            "MATCHING_RESULT_CRON_WB",
            con=engine,
            if_exists="append",
            index = False,
            dtype=dtype_mapping,
            schema="crrt"
        )
    del all_other_matches
    df = df.with_columns([
        pl.col("ALL_CHARS").fill_null("").cast(pl.Utf8),
        pl.col("WB_TITLE").cast(pl.Utf8),
        pl.col("CATEGORY_LEVEL1").cast(pl.Utf8),
        pl.col("descriptions").cast(pl.Utf8),
        pl.col("EXT_URL").cast(pl.Utf8),
        pl.col("BRAND").fill_null("").cast(pl.Utf8),
        pl.col("ID").cast(pl.Utf8)
    ])


except:
    trace = traceback.format_exc()
    logger.error('\n'.join(['ERROR with pre_main script!', trace])) 
    


#%%

def main():
    logger.info('Все ок!-WB- вошли в основной скрипт')
    # Пропроцессинг для основной логики матчинга 
    global combined_df
    combined_df = combined_df.with_columns(pl.col("NAME").str.to_lowercase())
    global conn
    main_harakter = """
        SELECT NAME_GENERATION_FORMULA,NAME  FROM EXP_USER.CATEGORY@dwhsas
    """
    formulas = pl.read_database(main_harakter, connection=conn, infer_schema_length=None).lazy()
    global df
    

    cats_list = df.select("CATEGORY_NAME").unique().collect()["CATEGORY_NAME"].to_list()
    # фильтрация в lazy frame
    formulas_need_cat = formulas.filter(
        pl.col("NAME").is_in(cats_list) & pl.col("NAME_GENERATION_FORMULA").is_not_null()
        ).with_columns(pl.col("NAME_GENERATION_FORMULA").str.to_lowercase())

    expanded_df = formulas_need_cat.with_columns(
        pl.col("NAME_GENERATION_FORMULA").map_elements(lambda x: re.findall(r'\$\{([^}]+)\}', x), return_dtype=pl.List(pl.Utf8)).alias("Placeholders")).explode("Placeholders")

    expanded_df = expanded_df.drop("NAME_GENERATION_FORMULA")

    combined_df_with = combined_df.join(expanded_df, how="inner", left_on = ["CATEGORY_NAME", "NAME"], right_on = ["NAME", "Placeholders"])
    
    # Приведение в нижний регистр
    selected_col = ["PARAM", "BRAND"]
    combined_df_with = combined_df_with.with_columns([pl.col(col).str.to_lowercase().alias(col) for col in selected_col])
    

    groupped_df = (combined_df_with.group_by("SKU").agg([
        pl.col("PARAM"),
        pl.col("BRAND").first()]).with_columns(
            pl.concat_list([pl.col("PARAM"), pl.col("BRAND")]).alias("PARAM")).drop("BRAND"))
    groupped_df = groupped_df.unique()
    merged = df.join(groupped_df, how="left", left_on=["KASPI_ID"], right_on=["SKU"])

    # Приведение в нижний регистр
    selected_col = ["WB_TITLE", "descriptions", "ALL_CHARS", "BRAND"]
    merged = merged.with_columns([pl.col(col).str.to_lowercase().alias(col) for col in selected_col])

    merged = merged.with_columns(
    pl.concat_list([
        pl.col("WB_TITLE"),
        pl.col("descriptions"),
        pl.col("ALL_CHARS"),
        pl.col("BRAND")
    ]).alias("Content_WB"))
    merged = merged.drop(['descriptions', 'ALL_CHARS', 'BRAND', 'COMB_ID'])
    merged = merged.rename({"PARAM":"Content_Kaspi"})

    merged = merged.with_columns(pl.col("Content_WB").list.join(", ").alias("Content_WB"))
    

    # Нормализация и транслитерация текста контента ВБ 
    merged = merged.with_columns([
        pl.col("Content_WB").map_elements(
            lambda words: add_normalized_values(words),
            return_dtype= pl.Utf8
        ).alias("Content_WB_Normalized"),
        pl.col("Content_WB").map_elements(
            lambda words: translit_by_letters(words),
            return_dtype=pl.Utf8
        ).alias("Content_WB_Transliterized")
        ])
    

    # работа с нуллами
    selected_columns = ["Content_WB_Transliterized", "Content_WB", "Content_WB_Normalized"]

    merged = merged.with_columns([
        pl.col(col).fill_null('').alias(col) for col in selected_columns
    ])
    
    # работа с нуллами

    merged = merged.with_columns(
        pl.col("Content_Kaspi").fill_null(pl.lit([])).cast(pl.List(pl.Utf8)))


   
    # Основная логика матчинга 
    merged_lim_n = merged.with_columns(
        pl.struct(["Content_Kaspi", "Content_WB", "Content_WB_Normalized", "Content_WB_Transliterized"]).map_elements(
            lambda row: [x for x in row["Content_Kaspi"] 
            if any(x in row[col] for col in ["Content_WB", "Content_WB_Normalized", "Content_WB_Transliterized"])], 
            return_dtype=pl.List(pl.Utf8)).alias("Matched_Params"))


    # Подсчет статистики 
    merged_lim_n = merged_lim_n.with_columns([
    pl.col("Content_Kaspi").list.len()
    .alias("len_Content_Kaspi"),

    pl.col("Matched_Params").list.len()
    .alias("len_Matched_Params")
    ])

    merged_lim_n = merged_lim_n.with_columns([pl.when((pl.col("len_Content_Kaspi") > 0) & (pl.col("len_Matched_Params").is_not_null()))
        .then((pl.col("len_Matched_Params") == pl.col("len_Content_Kaspi")).cast(pl.Int8))
        .otherwise(None) 
        .alias("Matching_Result")])

    to_crrt = merged_lim_n.clone()
    to_crrt = to_crrt.unique()
    to_crrt = to_crrt.drop(["Content_WB_Normalized", "Content_WB_Transliterized"])
    

     
    # Set timezone explicitly
    tz = pytz.timezone('Asia/Almaty')

    to_crrt = to_crrt.collect().to_pandas()
    # to_crrt=to_crrt.rename(columns={'len_Content_Kaspi':'LEN_OF_CHAR', 'len_Matched_Params':'LEN_OF_MATCHED','Matching_Result':'MATCHING_RESULT','Matched_Params':'CONSTRUCTOR', 'Content_WB':'ALL_CHARS', 'WB_TITLE':'TITLE_WB','Content_Kaspi':'TITLE_DETAIL' })
    to_crrt=to_crrt.rename(columns={'Matching_Result':'MATCHING_RESULT','WB_TITLE':'TITLE_WB' })
    needed_columns = ['CATEGORY_NAME', 'KASPI_ID', 'TITLE_DETAIL', 'TITLE_WB',
        'MATCHING_RESULT', 'CONSTRUCTOR', 'LEN_OF_MATCHED', 'LEN_OF_CHAR',
        'ALL_CHARS', 'DESCRIPTION', 'VERTICAL', 'ID','EXT_URL']
    for column in needed_columns:
        if column not in to_crrt.columns:
            to_crrt[column] = None

    to_crrt = to_crrt[['CATEGORY_NAME', 'KASPI_ID', 'TITLE_DETAIL', 'TITLE_WB','MATCHING_RESULT', 'CONSTRUCTOR', 'LEN_OF_MATCHED', 'LEN_OF_CHAR','ALL_CHARS', 'DESCRIPTION', 'VERTICAL', 'ID', 'EXT_URL', 'TIME_STAMP']]   
    
    
    
    
    to_crrt['TIME_STAMP'] = (datetime.now() + timedelta(hours=5)).strftime('%Y-%m-%d %H:%M:%S')
    dtype_mapping = {c: oracle.VARCHAR2(2048) for c in to_crrt.columns}
    portion = 50000

    ####### added by Alikhan #########

    for i in range(to_crrt.shape[0]//portion + 1):
        temp = to_crrt.iloc[i*portion:(i+1)*portion]
        temp.to_sql("MATCHING_RESULT_CRON_WB", 
                    con=engine, 
                    if_exists="append",#changed 
                    index=False, 
                    schema='crrt',
                    dtype = dtype_mapping)

    
    del to_crrt


    df = merged_lim_n
    df = df.filter(pl.col("Matching_Result") == 1)
    df = df.unique()
    




    if df.select(pl.len()).collect().item():
        # logger.info('Все ок!-WB - имеет матчи')
        
        df = df.with_columns(pl.col("KASPI_ID").cast(pl.Utf8), pl.col("ID").cast(pl.Utf8))

    
        # To correct
        query = """
            SELECT * FROM MATCHING_CATEGORY_WB
        """
        categories = pl.read_database(query, connection=conn).lazy()

        categories_short = (
            categories
            .unique(subset=None)  # Drop duplicates
            .sort(by="MATCH", descending=True)  # Sort by MATCH in descending order
        )

        
        matches_all = df.clone()
        categories_short = categories_short.with_columns(pl.col("CAT_NAME2").str.to_lowercase())
        matches_all = matches_all.with_columns(pl.col("CATEGORY_LEVEL1").str.to_lowercase())


        matches_all_with_cat = matches_all.join(
            categories_short,
            how="left",
            left_on=["CATEGORY_LEVEL1", "CATEGORY#NAME", "CATEGORY_NAME"],
            right_on=["CAT_NAME2", "CATEGORY_NAME_OZON", "CATEGORY_NAME"]
        ).unique()  # Drop duplicates

        
        cleaned_matches = matches_all_with_cat.filter(pl.col("MATCH") == "1")
        all_other_matches = matches_all_with_cat.filter(pl.col("MATCH") == "0")
        df = cleaned_matches





        # Чистка по ценам 
        ids = df.select("KASPI_ID").unique().collect()["KASPI_ID"].to_list()
        ids_2 = df.select("ID").unique().collect()["ID"].to_list()
        global myconn
        ozon_price = []
        for i in range(0, len(ids_2), 1000):
            chunk = tuple(ids_2[i:i+1000])
            query_main_df = f"SELECT ID, PRICE FROM S0244.WB_PRODUCT_PARS WHERE ID IN {chunk}"
            subcategories_name_construct = pd.read_sql_query(query_main_df, con=myconn)
            ozon_price.append(subcategories_name_construct)
        ozon_price = pl.from_pandas(pd.concat(ozon_price, ignore_index=True)).lazy()
        ozon_prices = ozon_price.group_by("ID").agg(pl.median("PRICE").alias("PRICE_OZON"))
        
        
       
        # Цены Каспи
        kaspi_prices = []
        for i in range(0, len(ids), 1000):
            chunk = tuple(ids[i:i+1000])
            query = f"SELECT ARTICLE_NUMBER as KASPI_ID, PARTNER_PRICE FROM sasuser.SHOP_MERCHANT_ITEM@dwhsas WHERE ARTICLE_NUMBER IN {chunk}"
            subcategories_name_construct = pd.read_sql_query(query, con=conn)
            kaspi_prices.append(subcategories_name_construct)
        kaspi_prices = pl.from_pandas(pd.concat(kaspi_prices, ignore_index=True)).lazy()
        kaspi_prices = kaspi_prices.with_columns(pl.col("KASPI_ID").cast(pl.Utf8))
        kaspi_prices = kaspi_prices.group_by("KASPI_ID").agg(pl.median("PARTNER_PRICE").alias("PARTNER_PRICE"))


        final = df.join(ozon_prices, how="left", left_on="ID", right_on="ID")
        final = final.join(kaspi_prices, how="left", left_on="KASPI_ID", right_on="KASPI_ID")


        
        # Статистика расчет
        final_1 = final.with_columns(
            pl.when((pl.col("PRICE_OZON") > pl.col("PARTNER_PRICE")) & pl.col("PRICE_OZON").is_not_null())
            .then(pl.col("PRICE_OZON"))
            .otherwise(pl.col("PARTNER_PRICE"))
            .alias("BASE"))

        final_2 = final_1.with_columns(
            ((pl.col("PARTNER_PRICE") - pl.col("PRICE_OZON")).abs() / pl.col("BASE")).round(2).alias("REL_DIFF"),
            pl.when(pl.col("REL_DIFF") >= 0.60).then(1).otherwise(0).alias("suspicious_price")
        )

        final_2 = final_1.with_columns(
            ((pl.col("PARTNER_PRICE") - pl.col("PRICE_OZON")).abs() / pl.col("BASE")).round(2).alias("REL_DIFF")
        )
        
        final_3 = final_2.with_columns((pl.when(pl.col("REL_DIFF") >= 0.60).then(1).otherwise(0).alias("suspicious_price")))
        final = final_3

        del kaspi_prices, ozon_prices
        final = final.with_columns(pl.lit("wb").alias("source"))
        # final = final.with_columns(pl.when(pl.col("suspicious_price") == 1).then(0).otherwise(1).alias("is_correct"))

        final = final.rename({"MATCH":"is_correct","KASPI_ID":'kaspi_sku', "ID":'ext_sku',"EXT_URL":'ext_url', "WB_TITLE":'ext_title'})
        final = final.select(["kaspi_sku", "ext_sku", "ext_url", "ext_title", "source", "is_correct", "suspicious_price"])


     
        final_to_business_m = final.collect().to_pandas()
        final_to_business_m.drop_duplicates(inplace=True)

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
        # logger.info('Все ок!-WB - загрузка в кафку завершена')




import traceback
import logging
from logging.handlers import SMTPHandler

# Launch and manage errors
if __name__ == '__main__':
    # Create logger
    receivers = [
        'Alikhan.Sainov@kaspi.kz'
    ]

    handler = SMTPHandler(mailhost='relay2.bc.kz',
                          fromaddr='reglament_info@kaspi.kz',
                          toaddrs=receivers,
                          subject='Service Matching-WB')

    logger = logging.getLogger('logger-1')
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)

    try:
        main()

        logger.info('Все ок!-WB')
    except:
        trace = traceback.format_exc()
        logger.error('\n'.join(['ERROR! - WB', trace])) 

