
#%%
import pandas as pd
from tqdm import tqdm
import numpy as np 
import multiprocessing
import subprocess
import numpy as np
import sys
from data_inflow_wb import issue_dataframe
import warnings 
warnings.filterwarnings('ignore')
import logging
from logging.handlers import SMTPHandler
import cx_Oracle
import polars as pl



#%%

conn = cx_Oracle.connect(user='CRRT', password='NLP4321###', dsn='dwhnew-db:1521/node124')

if __name__ == '__main__':
    # Create logger
    receivers = ['Alikhan.Sainov@kaspi.kz']

    handler = SMTPHandler(..)

    logger = logging.getLogger('logger-1')
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)

    # Connect to Oracle DB
  

    # Load lazy frame
    df_tmp = issue_dataframe(conn)

    num_proc = 9
    smaller_dfs = np.array_split(df_tmp.collect().to_pandas(),num_proc)  # Convert to Pandas for splitting

    for i, smaller_df in tqdm(enumerate(smaller_dfs), total=num_proc):
        # Convert each chunk back to LazyFrame before saving
        pl.from_pandas(smaller_df).write_csv(f"/maindir/wb/multi_proc/wb_dfs_0/df_{i}.csv")

    ######## By Ilyas ########
    python_executable = sys.executable
    file = '/maindir/wb/multi_proc/sub_algo.py'

    def run_python_file(sos):
        try:
            subprocess.run([python_executable, file, '--sos', str(sos)], check=True)
        except subprocess.CalledProcessError as e:
            print(f"Error running {file}: {e}")

    print("WB- checkpoint: 1")

    processes = []
    for sos in range(num_proc):
        process = multiprocessing.Process(target=run_python_file, args=(sos,))
        processes.append(process)
        process.start()

    for process in processes:
        process.join()
