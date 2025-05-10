#%%
import pandas as pd
from tqdm import tqdm
import numpy as np 
import multiprocessing
import subprocess
import numpy as np
import sys
from data_inflow import issue_dataframe
import warnings 
warnings.filterwarnings('ignore')
import argparse
import logging
from logging.handlers import SMTPHandler

if __name__ == '__main__':
    # Create logger
    receivers = [
        'Alikhan.Sainov@kaspi.kz',
        'Ilyas.Mohammad@kaspi.kz'
    ]

    handler = SMTPHandler(mailhost='relay2.bc.kz',
                          fromaddr='reglament_info@kaspi.kz',
                          toaddrs=receivers,
                          subject='Service Matching')

    logger = logging.getLogger('logger-1')
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    #%%
    df_tmp = issue_dataframe()

    
  
    

    #%%
    num_proc = 10

    smaller_dfs = np.array_split(df_tmp, num_proc)

    for i, smaller_df in tqdm(enumerate(smaller_dfs), total=num_proc):
        smaller_df.to_csv(f"df_{i}.csv", index=False)

    #%%
    ######## By Ilyas ########
    python_executable = sys.executable
    file = 'sub_algo.py'

    def run_python_file(sos):
        try:
            subprocess.run([python_executable, file, '--sos', str(sos)], check=True)
        except subprocess.CalledProcessError as e:
            print(f"Error running {file}:{e}")

    print("checkpoint: 1")
    processes = []
    # for file in python_files:
    for sos in range(num_proc):
        process = multiprocessing.Process(target=run_python_file, args=(sos,))
        processes.append(process)
        process.start()

    for process in processes:
        process.join()



