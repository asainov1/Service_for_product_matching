
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
import base64
myconn_crrt = mysql.connector.connect(
    host='starrocksdb',#'dwh-dbr5-lp2',
    port='9030',
    user='crrt',
    password='4g1q6Dc=c7') 
myconn = mysql.connector.connect(
    user='sainov_58623', 
    password='VL$%_I4', 
    port = '9030',
    host='starrocksdb')
conn = cx_Oracle.connect(user='CRRT', password='NLP4321###', dsn='dwhnew-db:1521/node124')
# tqdm.tqdm.pandas()
proxy_handler = request.ProxyHandler({})
opener = request.build_opener(proxy_handler)

#%%
kaspi_im = pd.read_parquet('/maindir/wb/vector_folder/kaspi_images_b64.parquet')
wb_im = pd.read_parquet('/maindir/wb/vector_folder/wb_images_b64.parquet')
# comb_wb = pd.read_csv('/maindir/wb/wb_results/comb_wb.csv')


#%%
# Проверка на наличие уже в кафке каспи и озон ску комбинаций
query = f"""
SELECT * FROM S0244.OZON_RESULTS_MAPPING

"""
res = pd.read_sql_query(query, con=myconn)

res.d_mes = res.d_mes.apply( lambda x: json.loads(x) if isinstance (x, str) else x)
res['source'] = res.apply(lambda row : row['d_mes'].get('source'), axis=1)
res[res.source == 'wb']
wb = res[res.source == 'wb']

data = [eval(str(x)) for x in wb.d_mes.to_list()]
wb = pd.DataFrame.from_dict(data)



#%%



wb_matches = comb_wb[comb_wb.MATCHING_RESULT == 1]
wb_matches = wb_matches.rename(columns={'SKU':'KASPI_ID','WB_SKU':'ID'})

wb_matches.KASPI_ID = wb_matches.KASPI_ID.astype(str)
wb_matches.ID = wb_matches.ID.astype(str)

#%%

ids = tuple(wb_matches.ID.unique())

wb_price = pd.DataFrame()
for i in range(0, len(ids), 1000):
    chunk = ids[i:i + 1000]
    query_main_df = f"""
    SELECT ID, PRICE FROM S0244.WB_PRODUCT_PARS
    WHERE ID IN {chunk}
    """
    subcategories_name_construct = pd.read_sql_query(query_main_df, con=myconn)
    wb_price = pd.concat([wb_price, subcategories_name_construct], ignore_index=True)        
wb_price.ID = wb_price.ID.astype(str)

wb_prices_median=wb_price.groupby('ID').PRICE.median()
wb_prices = pd.DataFrame(wb_prices_median).reset_index()

#%%
skus = tuple(wb_matches.KASPI_ID.unique())
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
#%%
last_iter = wb_matches
last_iter.KASPI_ID =last_iter.KASPI_ID.astype(str)
last_iter.ID =last_iter.ID.astype(str)
last_iter_n = pd.merge(last_iter, wb_prices, how='left', on =['ID'])
last_iter_m = pd.merge(last_iter_n, kaspi_prices, how='left', on =['KASPI_ID'])
last_iter_m.KASPI_ID=last_iter_m.KASPI_ID.astype(str)
last_iter_m.ID=last_iter_m.ID.astype(str)

last_iter_m = last_iter_m[['KASPI_ID', 'ID','PRICE','PARTNER_PRICE','TITLE_WB']]

#%%
# wb_url = pd.DataFrame()
# for i in range(0, len(ids), 1000):
#     chunk = ids[i:i + 1000]
#     query_main_df = f"""
#     SELECT ID, PRODUCT_URL FROM S0244.WB_PRODUCT_PARS
#     WHERE ID IN {chunk}
#     """
#     subcategories_name_construct = pd.read_sql_query(query_main_df, con=myconn)
#     wb_url = pd.concat([wb_url, subcategories_name_construct], ignore_index=True)        
# wb_url.ID = wb_url.ID.astype(str)
# wb_url.drop_duplicates(inplace=True)
# last_iter_m_n = pd.merge(last_iter_m, wb_url, how='left', left_on = 'ID', right_on='ID')
#%%
last_iter_m_n = last_iter_m
kaspi_images = kaspi_im[~kaspi_im.embeddings.isna()]



#%%
final = last_iter_m
final = final.rename(columns={'PRICE':'PRICE_OZON','PARTNER_PRICE':'KASPI_PRICE' })

final['BASE'] = ''
final['BASE'] = final.apply(
    lambda row: row['PRICE_OZON'] if (row['PRICE_OZON'] > row['KASPI_PRICE'] and pd.notna(row['PRICE_OZON']) and pd.notna(row['KASPI_PRICE'])) else row['KASPI_PRICE'],
    axis=1
)
final['REL_DIFF'] =  round(abs(final['KASPI_PRICE'] - final['PRICE_OZON']) / final['BASE'],2)
final['suspicious_price'] = final.apply(lambda row: 1 if row['REL_DIFF'] >= 0.60 else 0, axis=1)
final.drop(['ID','KASPI_PRICE','PRICE_OZON','BASE','REL_DIFF'],axis=1,inplace=True)
final.suspicious_price =final.suspicious_price.astype(str) 


#%% Получение сходства картинок
from itertools import product as cartesian
import numpy as np 
image_list = []
scores = []
def get_cosine_similarity(vector_a, vector_b):
    return np.dot(vector_a, vector_b) / np.dot(np.linalg.norm(vector_a), np.linalg.norm(vector_b)) 

for idx, row in tqdm(final.iterrows(), total=final.shape[0]):
    sku_kaspi = row['kaspi_sku']
    wb_id = row['ext_sku']

    kaspi_image = kaspi_images.query("(@sku_kaspi == SKU) & (TYPE == 'KASPI')")
    kaspi_image = kaspi_image['embeddings'].tolist()

    wb_images = wb_im.query("(@wb_id == SKU) & (TYPE == 'WB')")
    wb_images = wb_images['embeddings'].tolist()
    if len(kaspi_image) != 0 and len(wb_images) != 0:
        max_sim = max([
            get_cosine_similarity(x, y) 
            for x, y in cartesian(kaspi_image, wb_images)
        ])
    else:
        max_sim = 0
    scores.append(max_sim)
    flag = int(max_sim >= 0.75)
    image_list.append(flag)

final['score'] = scores
final['SIMILAR_IMG'] = image_list
final['is_correct'] = final['SIMILAR_IMG']
#%%

from kafka import KafkaProducer, KafkaConsumer
myconn=mysql.connector.connect(
    user='crrt',
    password='4g1q6Dc=c7',
    port='9030',
    host='starrocksdb') 
topic_to = 'IN.S0244.OZON_RESULTS_MAPPING'
producer=KafkaProducer(
            bootstrap_servers = ['dwh-kfk5-lp2.hq.bc:9093','dwh-kfk6-lp1.hq.bc:9093','dwh-kfk7-lp2.hq.bc:9093'],  # Укажите адрес и порт вашего Kafka брокера
            value_serializer=lambda v:str(v).encode('utf-8'), # Сериализатор для значений (в данном случае, преобразуем в байты)
            sasl_plain_username='S0212.CRTR_USER',
            
            # sasl_plain_password='R1NmGoFV0',
            sasl_plain_password='gaD6eMIq,EtWIb',
            sasl_mechanism='SCRAM-SHA-256',
            security_protocol='SASL_PLAINTEXT',
            api_version=(0, 10, 1),
            linger_ms=500
)
for i in range(len(final)):
    example = final.iloc[i, :] 
    uuid_value = example['uuid'] 
    example_dict = example.to_dict()
    example_json = json.dumps(example_dict) 
    producer.send(topic_to, headers=None, key=uuid_value.encode('utf-8'), value=example_json)
















#%%

####################### LABEL FOR MATCHES ########################
query = f"""
SELECT * FROM CRRT.MATCHING_ANNOTATIONS_RESULT 

"""
res = pd.read_sql_query(query, con=conn)

res = res[['KASPI_SKU', 'EXT_SKU', 'FINAL_RESULT']]


# non_match = res[res.FINAL_RESULT == 0]

ext_ids = tuple(map(str, res.EXT_SKU.unique()))
kaspi_ids = tuple(map(str, res.KASPI_SKU.unique()))
chunksize = 10000
wb_images = []
images = []

#%%
# Kaspi images
for i in range(0, len(kaspi_ids), 1000):
    chunk = kaspi_ids[i : i + 1000]
    products_query = f"""
    SELECT /*+ parallel(auto) */ distinct
        p.code as sku,
        'https://resources.cdn-kaspi.kz/img/m/p/' || m.p_location || '?format=gallery-medium' as url_1,
        'https://resources.cdn-kaspi.kz/img/m/p/'|| m2.p_location || '?format=gallery-medium' as url_2,
        'https://resources.cdn-kaspi.kz/img/m/p/'|| m3.p_location || '?format=gallery-medium' as url_3,
        'https://resources.cdn-kaspi.kz/img/m/p/'|| m4.p_location || '?format=gallery-medium' as url_4,
        'https://resources.cdn-kaspi.kz/img/m/p/'|| m5.p_location || '?format=gallery-medium' as url_5,
        'https://resources.cdn-kaspi.kz/img/m/p/'|| m6.p_location || '?format=gallery-medium' as url_6
    FROM EXP_USER.PRODUCTS@dwhsas p
        JOIN EXP_USER.PRODUCTSLP@dwhsas plp ON p.PK = plp.ItemPK and plp.LANGPK = 8796093317152
        JOIN EXP_USER.ENUMERATIONVALUES@dwhsas item_t1 ON item_t1.PK = p.p_approvalstatus
        JOIN EXP_USER.CATALOGVERSIONS@dwhsas item_t2 ON item_t2.PK = p.p_catalogversion
        JOIN EXP_USER.CATALOGS@dwhsas item_t3 ON item_t3.PK = item_t2.p_catalog
        JOIN EXP_USER.MEDIAS@dwhsas m ON m.p_mediacontainer = p.p_primaryimage
        JOIN EXP_USER.GENERICITEMS@dwhsas g ON p.p_brand = g.PK
        JOIN EXP_USER.CAT2PRODREL@dwhsas ctgp on ctgp.TargetPK = p.PK
        JOIN EXP_USER.CATEGORIES@dwhsas ctg ON ctg.PK = ctgp.SourcePK
        JOIN (SELECT distinct
                    prim.code as sku,
                    REGEXP_SUBSTR(prim.p_galleryimages, '[^,]+', 1, 3) col_2,
                    REGEXP_SUBSTR(prim.p_galleryimages, '[^,]+', 1, 4) col_3,
                    REGEXP_SUBSTR(prim.p_galleryimages, '[^,]+', 1, 5) col_4,
                    REGEXP_SUBSTR(prim.p_galleryimages, '[^,]+', 1, 6) col_5,
                    REGEXP_SUBSTR(prim.p_galleryimages, '[^,]+', 1, 7) col_6
                FROM EXP_USER.PRODUCTS@dwhsas prim  
            ) ad ON ad.sku = p.code
        LEFT JOIN EXP_USER.MEDIAS@dwhsas m2 ON m2.p_mediacontainer = ad.col_2 and m2.p_mediaformat = 8796355199027
        LEFT JOIN EXP_USER.MEDIAS@dwhsas m3 ON m3.p_mediacontainer = ad.col_3 and m3.p_mediaformat = 8796355199027  
        LEFT JOIN EXP_USER.MEDIAS@dwhsas m4 ON m4.p_mediacontainer = ad.col_4 and m4.p_mediaformat = 8796355199027
        LEFT JOIN EXP_USER.MEDIAS@dwhsas m5 ON m5.p_mediacontainer = ad.col_5 and m5.p_mediaformat = 8796355199027
        LEFT JOIN EXP_USER.MEDIAS@dwhsas m6 ON m6.p_mediacontainer = ad.col_6 and m6.p_mediaformat = 8796355199027
    WHERE 1=1 AND sku in {tuple(chunk)}
    """
    kk_images = pd.read_sql_query(products_query, con=conn)
    images.append(kk_images)
images = pd.concat(images, axis=0)

url_columns = [col for col in images.columns if col.startswith('URL')]
melted_df = pd.melt(images,
                    id_vars='SKU', 
                    value_vars=url_columns, 
                    var_name='URL_Type',
                    value_name='URL')

melted_df = melted_df.rename(columns={'SKU': 'KASPI_ID'})
melted_df.KASPI_ID = melted_df.KASPI_ID.astype(str)
melted_df = melted_df[~melted_df.URL.isna()]
melted_df = melted_df[['KASPI_ID', 'URL']]
melted_df.drop_duplicates(inplace=True)
melted_df = melted_df[['p/?format' not in c for c in melted_df.URL]]

#%%
# External images
for i in range(0, len(ext_ids), chunksize):
    chunk = ext_ids[i : i + chunksize]
    query = f"""
        select t2.*
        from (
            select distinct
                get_json_string(t1.json_data, '$.s3_img_url') as img_url,
                get_json_string(t1.json_data, '$.product_article') as ext_sku
            from S0271.PARSING_IMG_LINKS_FOR_MAPPING t1
        ) as t2
        where t2.ext_sku in {tuple(chunk)}
    """
    pars_img = pd.read_sql_query(query, con=myconn_crrt)
    wb_images.append(pars_img)
wb_images = pd.concat(wb_images, axis=0)


#%%
# Если вдруг картинки отсутствуют 
wb_images_skus_absent = tuple(wb_images.ext_sku.unique())
absent = non_match[~non_match.EXT_SKU.isin(wb_images_skus_absent)]
absent=absent[['EXT_SKU']]



#%%

# Секретная инфа
key_id = 'E55Z2ELPFQBA9PWY3HF3'
secret_key = '0isXCmi3QEMk4TDCIeww61HSLRJevBB7BZ6ZpCJI'
endpoint = 'http://ses.hq.bc'


session = boto3.session.Session(aws_access_key_id=key_id,
                                aws_secret_access_key=secret_key)

s3 = session.client(service_name='s3', endpoint_url=endpoint)
based_images = []

for idx, row in tqdm(wb_images.iterrows(), total=wb_images.shape[0]):
    # Спим, чтобы не сломать S3 
    time.sleep(0.002)

    # Разбираем ссылку на части
    bucket = row['img_url'].split('/')[3]
    key = row['img_url'].split('/')[4]

    # Читаем картину
    image = s3.get_object(Bucket=bucket, Key=key)
    body = image['Body'].read()
    based_images.append(body)

wb_images['data'] = based_images
wb_images.to_parquet('/maindir/wb/vector_folder/wb_images_b64.parquet', index=False)

#%% Если лень качать S3
wb_images = pd.read_parquet('/maindir/wb/vector_folder/wb_images_b64.parquet')





#%%
melted_df = melted_df.rename(columns={'KASPI_ID': 'SKU'})
melted_df['TYPE'] = 'KASPI'
wb_images = wb_images[['ext_sku', 'data']].rename(columns={'ext_sku': 'SKU'})
wb_images['TYPE'] = 'WB'
unique_images = pd.concat([melted_df, wb_images], axis=0)

#%%
proxy_host = 'proxy-all.hq.bc:8080'
user = 'sainov_58623'
password = 'Qwerty441551@@'
proxy_host = f'http://{user}:{password}@proxy-all.hq.bc:8080'

host = 'http://ks-dedup-triton.de.bc'

def get_image_base64(url):
    response = requests.get(url, verify=False).content
    return base64.b64encode(response).decode('utf-8')

def extract(row):
    if row['TYPE'] == 'KASPI':
        image_base64 = get_image_base64(row['URL'])
    else:
        image_base64 = str(row['data'])[2:-1]

    data = {
        "request_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
        "images": [image_base64]
    }

    response = requests.post(
        f'{host}/api/v1/extract/dino/1/base64',
        data=json.dumps(data)
    )

    try:
        result = response.json()['vectors'][0]
    except:
        result = None
    return result

#%%
import warnings 
warnings.filterwarnings('ignore')
# unique_images = melted_df
embeddings = []
for _, row in tqdm(unique_images.iterrows(), total=unique_images.shape[0]):
    embeddings.append(extract(row))
    time.sleep(0.2)

unique_images['embeddings'] = embeddings
unique_images.to_parquet('/maindir/wb/vector_folder/kaspi_images_b64.parquet', index=False)






























#%%#### Проверка на факт то что в есть все картинки или нет

from multiprocessing.pool import ThreadPool
def fetch_data (chunk):
    with mysql.connector.connect(user='sainov_58623', password='VL$%_I4', port = '9030',host='starrocksdb') as conn:
        query_main_df = f"""SELECT *  FROM S0244.OZON_PRODUCT_PARS_IMAGES WHERE SKU IN {chunk}"""
        return pd.read_sql_query(query_main_df, conn)

chunks = [ids[i : i + 1000] for i in range(0, len(ids), 1000)]
with ThreadPool(4) as pool:
    df_df = pool.map(fetch_data, chunks)
df_df = pd.concat(df_df, axis=0)

###

myconn=mysql.connector.connect(
    user='crrt',
    password='4g1q6Dc=c7',
    port='9030',
    host='starrocksdb') 

# External images
wb_images = []
for i in range(0, len(ext_ids), chunksize):
    chunk = ext_ids[i : i + chunksize]
    query = f"""
        select t2.*, t2.img_url, t2.ext_sku, t2.source_image_url
        from (
            select distinct
                get_json_string(t1.json_data, '$.s3_img_url') as img_url,
                get_json_string(t1.json_data, '$.product_article') as ext_sku,
                t1.source_img_url as source_image_url
    
            from S0271.PARSING_IMG_LINKS_FOR_MAPPING t1
        ) as t2
        where t2.ext_sku in {tuple(chunk)}
    """
    pars_img = pd.read_sql_query(query, con=myconn)
    wb_images.append(pars_img)
wb_images = pd.concat(wb_images, axis=0)

###
#%%
df1 = df_df.copy()
df2 = wb_images.copy()

sku_images_map = df1.groupby('SKU')['IMAGES'].apply(set).to_dict()
parse = set(df2['source_image_url'].tolist())


flag_list = []
for i in tqdm(range(len(df1))):
    sku = df1.SKU[i]
    urls = sku_images_map.get(sku, set())
    flag = 0
    # smaller_url = df2.query("@sku == ext_sku")
    # smaller_url_list = smaller_url['source_image_url'].tolist()
    if len(urls) != 0:
        if all(url in parse for url in urls):
            flag = 1
        else:
            flag = 0
    else:
        flag=0
    flag_list.append(flag)



#%%

only_full_links = df1.query("all_links == 1")
only_full_links = only_full_links[['SKU','all_links']]
only_full_links.drop_duplicates(inplace=True)
merged = pd.merge(df2, only_full_links, how='left', left_on = 'ext_sku', right_on ='SKU')
merged_m = merged[['ext_sku','all_links']]

merged_final = pd.merge(final, merged_m, how='left', left_on='EXT_SKU', right_on='ext_sku')
merged_final.drop_duplicates(inplace=True)

all_links = merged_final[merged_final.all_links == 1]


#%% CONFIDENCE INTERVAL ANALYSIS FOR SIMILARITY


confidence_levels = [0.5, 0.51, 0.52, 0.53, 0.54, 0.55, 0.56, 0.57, 0.58, 0.59, 

 0.6, 0.61, 0.62, 0.63, 0.64, 0.65, 0.66, 0.67, 0.68, 0.69, 

 0.7, 0.71, 0.72, 0.73, 0.74, 0.75, 0.76, 0.77, 0.78, 0.79, 

 0.8, 0.81, 0.82, 0.83, 0.84, 0.85, 0.86, 0.87, 0.88, 0.89, 

 0.9, 0.91, 0.92, 0.93, 0.94, 0.95, 0.96, 0.97, 0.98, 0.99, 1.0]  # Add desired confidence levels
results = []  # To store results for each confidence level
category_results = []

for confidence in confidence_levels:
    per_word = int(confidence * 100)  # Convert confidence to percentage (e.g., 50, 60)  
    # Create a copy of the DataFrame to process for the current confidence level
    final_ = all_links.copy()
    # Set confidence level and suspicious price
    final_['confidence_level'] = confidence
    final_['is_similar'] = final_.apply(lambda row: 1 if row['score'] >= confidence else 0, axis=1)

    # Define is_valid
    def assign_is_valid(row):

        if row['is_similar'] == 1 and row['FINAL_RESULT'] == 1:
            return 'true_1'
        elif row['is_similar'] == 0 and row['FINAL_RESULT'] == 0:
            return 'true_0'
        elif row['is_similar'] == 1 and row['FINAL_RESULT'] == 0:
            return 'false_0'
        elif row['is_similar'] == 0 and row['FINAL_RESULT'] == 1:
            return 'false_1'
        else:
            return None
    # Apply is_valid

    final_['is_valid'] = final_.apply(assign_is_valid, axis=1)

    grouped = final_.groupby('CAT_NAME2')
    # Calculate metrics
    for category, group in grouped:
        metrics = group['is_valid'].value_counts()
        TP = metrics.get('true_1', 0)  # True Positives
        TN = metrics.get('true_0', 0)  # True Negatives
        FN = metrics.get('false_1', 0)  # False Negatives
        FP = metrics.get('false_0', 0)  # False Positives

        precision = TP / (TP + FP) if (TP + FP) > 0 else 0
        recall = TP / (TP + FN) if (TP + FN) > 0 else 0
        f1_score = (2 * (precision * recall)) / (precision + recall) if (precision + recall) > 0 else 0
        # Store results

        category_results.append({
            'confidence_level': confidence,
            'category':category,
            'precision': round(precision, 2),
            'recall': round(recall, 2),
            'f1_score':round(f1_score, 2),
            'TP': TP,
            'FP': FP,
            'FN': FN,
            'TN': TN
        })
#%%
# Convert results to a DataFrame for easy visualization
import pandas as pd

# results_df = pd.DataFrame(results)
category_results_df = pd.DataFrame(category_results)
category_results_df.loc[category_results_df.groupby(['category'])['precision'].idxmax()].sort_values('precision',ascending=False)




#%%
import numpy as np

import pandas as pd

# Generate multipliers dynamically (e.g., from 0.5 to 10 with steps of 0.1)

confidence_multipliers = np.arange(0.5, 10.1, 0.1).tolist()  # Multipliers: 0.5, 0.6, ..., 10.0

results = []  # To store results for each multiplier



for multiplier in confidence_multipliers:
    # Create a copy of the DataFrame to process for the current multiplier
    final_ = price_a.copy()
    # Calculate BASE

    final_['BASE'] = final_['KASPI_PRICE']

    # Convert PRICE_OZON to float

    final_['PRICE_OZON'] = final_['PRICE_OZON'].astype(float)
    # Calculate relative difference
    final_['REL_DIFF'] = abs(final_['KASPI_PRICE'] - final_['PRICE_OZON']) / final_['BASE']
    # Set confidence multiplier and suspicious price flag
    final_['confidence_multiplier'] = multiplier
    final_['suspicious_price'] = final_.apply(lambda row: 1 if row['REL_DIFF'] >= multiplier else 0, axis=1)

    # Define is_valid

    def assign_is_valid(row):

        if row['suspicious_price'] == 0 and row['FINAL_RESULT'] == 1:
            return 'true_1'
        elif row['suspicious_price'] == 1 and row['FINAL_RESULT'] == 0:
            return 'true_0'
        elif row['suspicious_price'] == 0 and row['FINAL_RESULT'] == 0:
            return 'false_0'
        elif row['suspicious_price'] == 1 and row['FINAL_RESULT'] == 1:
            return 'false_1'
        else:
            return None

    # Apply is_valid
    final_['is_valid'] = final_.apply(assign_is_valid, axis=1)

    # Calculate metrics
    metrics = final_['is_valid'].value_counts()
    TP = metrics.get('true_1', 0)  # True Positives
    TN = metrics.get('true_0', 0)  # True Negatives
    FN = metrics.get('false_1', 0)  # False Negatives
    FP = metrics.get('false_0', 0)  # False Positives

    precision = TP / (TP + FP) if (TP + FP) > 0 else 0
    recall = TP / (TP + FN) if (TP + FN) > 0 else 0
    f1_score = (2 * (precision * recall)) / (precision + recall) if (precision + recall) > 0 else 0
    # Store results
    results.append({
        'confidence_multiplier': multiplier,
        'precision': round(precision, 2),
        'recall': round(recall, 2),
        'f1_score': round(f1_score, 2),    
        'TP': TP,
        'FP': FP,
        'FN': FN,
        'TN': TN
    })

# Convert results to a DataFrame for easy visualization

results_df = pd.DataFrame(results)




#%%
ids = tuple(map(str, all_links.KASPI_SKU.unique()))
from multiprocessing.pool import ThreadPool
def fetch_data (chunk):
    with cx_Oracle.connect(user='CRRT', password='NLP4321###', dsn='dwhnew-db:1521/node124')as conn:
        query_main_df = f"""SELECT t1.SKU, t3.CAT_NAME2
                            FROM sasuser.SHOP_ALL_ITEMS_CAT_REL@dwhsas t1
                            LEFT JOIN sasuser.SHOP_ITEM_CAT_DICT@dwhsas t4
                                ON t4.ARTICLE_NUMBER = t1.SKU
                            LEFT JOIN sasuser.SEARCH_SHOP_CATEGORY_ALL_NEW@dwhsas t3
                                ON t3.CATEGORY_ID = t1.CATEGORY_CODE
                            LEFT JOIN sasuser.SEARCH_PARAM_SHOP@dwhsas t2
                                ON t2.CATALOG_ID = t1.SKU
                            WHERE t1.SKU IN {chunk}"""
        return pd.read_sql_query(query_main_df, conn)

chunks = [ids[i : i + 1000] for i in range(0, len(ids), 1000)]
with ThreadPool(4) as pool:
    df_df = pool.map(fetch_data, chunks)
df_df = pd.concat(df_df, axis=0)

#%%

all_links_p = pd.merge(merged_final, df_df, how='left', left_on='KASPI_SKU',right_on='SKU')
all_links_p.FINAL_RESULT = all_links_p.FINAL_RESULT.astype(int)












#%% Получаем ссылки спарсенных картинок сторонних маркетплейсов промежуточное хранилище hq.bc

sim = pd.read_excel('/maindir/stat/for_sim_analysis.xlsx')
wb_images = []
chunksize = 10000
# для специального случая 
sim_some = sim[(sim["FINAL_RESULT"]==1) & (sim["is_correct"]==1) & (sim["score"]>0.90)]
sim_some = sim_some.iloc[:10]
#########
sim_some.KASPI_SKU    =sim_some.KASPI_SKU.astype(str)
sim_some.EXT_SKU    =sim_some.EXT_SKU.astype(str)
kaspi_ids = tuple(map(str, sim_some.KASPI_SKU.unique()))
ext_ids = tuple(map(str, sim_some.EXT_SKU.unique()))
for i in range(0, len(ext_ids), chunksize):
    chunk = ext_ids[i : i + chunksize]
    query = f"""
        select t2.*
        from (
            select distinct
                get_json_string(t1.json_data, '$.s3_img_url') as img_url,
                get_json_string(t1.json_data, '$.product_article') as ext_sku
            from S0271.PARSING_IMG_LINKS_FOR_MAPPING t1
        ) as t2
        where t2.ext_sku in {tuple(chunk)}
    """
    pars_img = pd.read_sql_query(query, con=myconn_crrt)
    wb_images.append(pars_img)
wb_images = pd.concat(wb_images, axis=0)


kaspi_images = pd.read_parquet('/maindir/wb/vector_folder/kaspi_images_b64.parquet')

# для специального случая 
kaspi_images = kaspi_images[kaspi_images["SKU"].isin(kaspi_ids)]
#%% Получаем base 64 формат картинок Озона или ВБ на s3
import boto3
key_id = 'E55Z2ELPFQBA9PWY3HF3'
secret_key = '0isXCmi3QEMk4TDCIeww61HSLRJevBB7BZ6ZpCJI'
endpoint = 'http://ses.hq.bc'

session = boto3.session.Session(aws_access_key_id=key_id,
                                aws_secret_access_key=secret_key)

s3 = session.client(service_name='s3', endpoint_url=endpoint)




#%% Получение b64

from tqdm import tqdm
import base64
import requests
import warnings
warnings.filterwarnings("ignore")
base64_list = []
proxy_host = 'proxy-tech.hq.bc:8080'
user = 'sainov_58623'
password = 'Qwerty441551@@'
proxy_host = f'http://{user}:{password}@proxy-tech-all.hq.bc:8080'
def get_image_base64(url):
    proxies ={'http': '' , 'https': ''}
    response = requests.get(url, proxies=proxies, verify=False).content
    return base64.b64encode(response).decode('utf-8')

for _, row in tqdm(kaspi_images.iterrows(), total=kaspi_images.shape[0]):
    url = row['URL']
    base64_list.append(get_image_base64(url))
    time.sleep(0.2)

kaspi_images['kaspi_data'] = base64_list
#%% Получение b64 для озона

based_images_ozon = []

for idx, row in tqdm(wb_images.iterrows(), total=wb_images.shape[0]):
    # Спим, чтобы не сломать S3 
    time.sleep(0.002)

    # Разбираем ссылку на части
    bucket = row['img_url'].split('/')[3]
    key = row['img_url'].split('/')[4]
    # Читаем картину
    image = s3.get_object(Bucket=bucket, Key=key)
    body = image['Body'].read()
    based_images_ozon.append(body)

wb_images['data'] = based_images_ozon

#%%

wb_images.data = wb_images.apply(lambda row: str(row['data'])[2:-1] , axis=1) 








# %%
# sku_images_map = df_df.groupby('SKU')['IMAGES'].apply(set).to_dict()
# kaspi_sku_images_map = kaspi_images.groupby('SKU')['URL'].apply(set).to_dict()
from datetime import datetime
import os 
# from openai import OpenAI
import openai
import tqdm 

os.environ['HTTP_PROXY'] = f'http://:@proxy-tech.hq.bc:8080' 
os.environ['HTTPS_PROXY'] = f'http://:@proxy-tech.hq.bc:8080'
URL = "https://api.openai.com/v1/chat/completions"

OPENAI_API_KEY = 'sk-proj-UuUqhNyfxACaPrekvs8nT3BlbkFJmpFqykoB8WUynvba8QSY'
openai.api_key = OPENAI_API_KEY
# openai = OpenAI(
#     api_key=OPENAI_API_KEY,
# )

#%% GPT function 

kaspi_images.reset_index(inplace=True)
wb_images.reset_index(inplace=True)

i = 0
kaspi_sku = kaspi_images["SKU"][i]
kaspi_url = kaspi_images["URL"][i]
kaspi_b64 = kaspi_images["kaspi_data"][i]

ozon_sku = wb_images["ext_sku"][i]
ozon_url = wb_images["img_url"][i]
ozon_b64 = wb_images["data"][i]





#%%
# GPT
# sim_some.reset_index(inplace=True)
from tqdm import tqdm
results = []
for idx, row in tqdm(sim_some.iterrows(), total=len(sim_some)):
    kaspi_sku = row["KASPI_SKU"]
    ext_sku =  row["EXT_SKU"]

    kaspi_b64_list = kaspi_images.loc[kaspi_images["SKU"] == kaspi_sku, "kaspi_data"].values
    ozon_b64_list = wb_images.loc[wb_images["ext_sku"] == ext_sku, "data"].values

    for kaspi_b64 in kaspi_b64_list:
        for ozon_b64 in ozon_b64_list:
            if ((kaspi_b64!="" )&(ozon_b64!="")):

                image_base64_kaspi = "data:image/jpeg;base64," + kaspi_b64
                image_base64_ozon = "data:image/jpeg;base64," + ozon_b64

                images_prompt = '''
                Сравните две фотографии и определите, изображен ли на них один и тот же товар. Учитывайте внешний вид, надписи, серии, объемы и бренд товара. Любое различие в этих характеристиках указывает на то, что товары не являются одинаковыми.

                # Steps

                1. **Изучите изображение**: Внимательно рассмотрите каждую фотографию.
                2. **Анализируйте характеристики**:
                - **Внешний вид**: Сравните основные визуальные детали. Отенок содержимого сильно влиять не должен, так как это может быть спецификой обработки фотографии. 
                - **Надписи**: Проверьте текст на упаковке на предмет совпадений.
                - **Серии и объемы**: Убедитесь, что серии и объемы продукции совпадают.
                - **Брэнд**: Сравните бренды и логотипы.

                # Output Format

                ```json

                {
                'result': "Совпадают"/"Не совпадают", 
                'reason': причина почему не совпадает. 
                }
                ```
                # Examples

                **Пример 1:**
                - **Изображение 1**: [фото с изображением бутылки шампуня]
                - **Изображение 2**: [фото с изображением такой же бутылки шампуня, но с другим объемом]
                - **Вывод**: "Не совпадают"

                **Пример 2:**
                - **Изображение 1**: [фото коробки сока бренда A, объем 1 литр]
                - **Изображение 2**: [фото такой же коробки сока бренда A, объем 1 литр]
                - **Вывод**: "Совпадают"

                **Пример 3:**
                - **Изображение 1**: [фото крема]
                - **Изображение 2**: [фото крема но сама картинка чуть отличается в цвете]
                - **Вывод**: "Совпадают"

                # Notes

                Будьте внимательны к мелким деталям, так как небольшое различие может иметь большое значение.
                '''
                response = openai.ChatCompletion.create(
                model="gpt-4o-mini",
                messages=[
                    {
                    "role": "system",
                    "content": [
                        {
                        "type": "text",
                        "text": images_prompt
                        }
                    ]
                    },
                    {
                    "role": "user",
                    "content": [
                        {
                        "type": "image_url",
                        "image_url": {
                            "url": image_base64_kaspi
                        }
                        },
                        {
                        "type": "image_url",
                        "image_url": {
                            "url": image_base64_ozon
                        }
                        }
                    ]
                    }
                ],
                temperature=1,
                max_tokens=2048,
                top_p=1,
                frequency_penalty=0,
                presence_penalty=0,
                response_format={
                    "type": "text"
                }
                )
                response_content = response["choices"][0]["message"]["content"]
                response_cleaned = re.sub(r"^```json\n?|```$", "", response_content.strip()) 
                try:
                    response_json = json.loads(response_cleaned)
                except json.JSONDecodeError as e:
                    response_json = {}
                results.append({ "KASPI_SKU": row["KASPI_SKU"],
                                "OZON_SKU": row["EXT_SKU"], 
                                "Result": response_json.get("result","Errors parsing JSON"),
                                "Reason": response_json.get("reason","Errors parsing JSON")})

#%% Анализ ответов Чат ГПТ
results = pd.DataFrame(results)


some = pd.DataFrame(results.groupby(["KASPI_SKU", "OZON_SKU"])["Result"].agg(lambda x: x.mode())).reset_index(drop=False)


merged = pd.merge(sim_some, some, how='left', left_on =["KASPI_SKU", "EXT_SKU"], right_on=["KASPI_SKU","OZON_SKU"])

merged["Result_CHAT"] = merged["Result"].str.lower().apply(lambda x: 1 if x == "совпадают" else 0)

def map_result(row):
    if row["Result_CHAT"] == 1 and row["FINAL_RESULT"] == 1 and row["SIMILAR_IMG"] == 1:
        return "Both"
    elif row["Result_CHAT"] == 1 and row["FINAL_RESULT"] == 1 and row["SIMILAR_IMG"] == 0:
        return "Chat"
    elif row["SIMILAR_IMG"] == 1 and row["FINAL_RESULT"] == 1 and row["Result_CHAT"] == 0:
        return "CV"
    elif row["Result_CHAT"] == 0 and row["FINAL_RESULT"] == 1 and row["SIMILAR_IMG"] == 0:
        return "Neither"
    elif row["Result_CHAT"] == 0 and row["SIMILAR_IMG"] == 0 and row["FINAL_RESULT"] == 1:
        return "Neither"
    elif row["Result_CHAT"] == 0 and row["SIMILAR_IMG"] == 1 and row["FINAL_RESULT"] == 0:
        return "Chat"
    elif row["Result_CHAT"] == 1 and row["SIMILAR_IMG"] == 0 and row["FINAL_RESULT"] == 0:
        return "CV"
    else:
        return "None"

# Apply the function to each row
merged["Mapped_Result"] = merged.apply(map_result, axis=1)






#%% Анализ цены Озон


query = f"""
    SELECT SKU, OZON_SKU, MATCHING_RESULT
    FROM crrt.MATCHING_RESULT_CRON
    WHERE MATCHING_RESULT='1'
"""
res1 = pd.read_sql_query(query, con=conn)
ids = tuple(map(str, res1.OZON_SKU.unique()))
ids_2 = tuple(map(str, res1.SKU.unique()))
wb_price = pd.DataFrame()
for i in range(0, len(ids), 1000):
    chunk = ids[i:i + 1000]
    query_main_df = f"""
    SELECT SKU, PRICE FROM S0244.OZON_PRODUCT_PARS
    WHERE SKU IN {chunk}
    """
    subcategories_name_construct = pd.read_sql_query(query_main_df, con=myconn)
    wb_price = pd.concat([wb_price, subcategories_name_construct], ignore_index=True)      
delim = wb_price["PRICE"].str.split(" ", expand=True)
wb_price["PRICE"] = delim[0]


wb_prices_median=wb_price.groupby('SKU').PRICE.median()
wb_prices = pd.DataFrame(wb_prices_median).reset_index()


kaspi_prices = pd.DataFrame()
for i in range(0, len(ids_2), 1000):
    chunk = ids_2[i:i + 1000]
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

last_iter = res1
last_iter.SKU =last_iter.SKU.astype(str)
last_iter.OZON_SKU =last_iter.OZON_SKU.astype(str)
last_iter_n = pd.merge(last_iter, wb_prices, how='left', left_on =['OZON_SKU'], right_on=["SKU"]).rename(columns={"SKU_x":'KASPI_ID'})
last_iter_m = pd.merge(last_iter_n, kaspi_prices, how='left', left_on =['KASPI_ID'], right_on =["KASPI_ID"])
last_iter_m.drop(["SKU_y"],axis=1,inplace=True)


last_iter_m =last_iter_m.tail(1500)





