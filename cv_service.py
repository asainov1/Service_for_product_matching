#%%
import pandas as pd
import glob
import os
from sqlalchemy import create_engine
from sqlalchemy.dialects import oracle
from numpy.linalg import norm
import numpy as np 
import mysql.connector
import cx_Oracle
import requests
import json
import base64
from datetime import datetime
import mysql.connector
import cx_Oracle
from tqdm import tqdm
import time
import boto3
#%%

# Секретная инфа
key_id = 'E55Z2ELPFQBA9PWY3HF3'
secret_key = '0isXCmi3QEMk4TDCIeww61HSLRJevBB7BZ6ZpCJI'
endpoint = 'http://ses.hq.bc'

# Соединения с базами
engine = create_engine("oracle+cx_oracle://CRRT:NLP4321###@dwhnew-db:1521/?service_name=node124")
myconn = mysql.connector.connect(user='sainov_58623', 
                                 password='VL$%_I4', 
                                 port = '9030',
                                 host='starrocksdb')
myconn_crrt = mysql.connector.connect(host='starrocksdb',
                                      port='9030',
                                      user='crrt',
                                      password='4g1q6Dc=c7') 
conn = cx_Oracle.connect(user='CRRT', 
                         password='NLP4321###', 
                         dsn='dwhnew-db:1521/node124')
#%%
# Все остальное
query_main_df = f"""
    SELECT * FROM crrt.MATCHING_RESULT_TO_CV_WB
"""
wb_matches = pd.read_sql_query(query_main_df, con=conn)

matches = wb_matches[wb_matches.MATCHING_RESULT == '1']
ext_ids = matches.ID.unique().tolist()
kaspi_ids = matches.KASPI_ID.unique().tolist()
chunksize = 10000
wb_images = []
images = []

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
melted_df = melted_df[melted_df.URL != 'https://resources.cdn-kaspi.kz/img/m/p/?format=gall']

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

embeddings = []
for _, row in tqdm(unique_images.iterrows(), total=unique_images.shape[0]):
    embeddings.append(extract(row))
    time.sleep(0.2)

unique_images['embeddings'] = embeddings
unique_images.to_parquet('/maindir/wb/vector_folder/wb_images_b64.parquet', index=False)

# np.dot(a, b) / (norm(a) * norm(b))

# %%
