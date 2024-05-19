
import abc
import sys
import copy
import time
import datetime
import importlib
from abc import ABC
import os 
import pandas as pd
import multiprocessing
from pathlib import Path
from typing import Iterable
from tqdm import tqdm
CUR_DIR = Path(__file__).resolve().parent
sys.path.append(str(CUR_DIR.parent.parent))

from data_collector.yahoo.collector import YahooCollector, YahooCollectorCN1d, YahooCollectorCN1min

def get_date_interval(save_data_path, benchmark='sh000300.csv'):
    # end date is today
    end_date = datetime.datetime.now().strftime('%Y-%m-%d')
    # start date is the last update
    file_list = os.listdir(save_data_path)
    target_csv = pd.read_csv(save_data_path + '/' + benchmark)
    start_date = target_csv['date'].sort_index(ascending=True).values[-1]
    print(start_date, end_date)
    if len(start_date) > 10:
        start_date = start_date[:10]
    assert start_date < end_date, "The data is already up to date."
    
    return start_date, end_date


def merge_csv(new_data_path, old_data_path):
    new_files = os.listdir(new_data_path)
    old_files = os.listdir(old_data_path)
    for file_name in tqdm(new_files):
        if file_name not in old_files:
            # copy the file to the old_data_path
            os.system(f'cp {new_data_path}/{file_name} {old_data_path}/{file_name}')
            continue

        new_csv = pd.read_csv(f'{new_data_path}/{file_name}')
        old_csv = pd.read_csv(f'{old_data_path}/{file_name}')
        # merge the two csv files, and drop the duplicate dates
        merged_csv = pd.concat([old_csv, new_csv], axis=0)
        merged_csv = merged_csv.drop_duplicates(subset=['date'])
        merged_csv.to_csv(f'{old_data_path}/{file_name}', index=False)
    print('Merge csv files successfully.')


root_path = '/cos-data/qlib_data'
start_date, end_date = get_date_interval('/cos-data/qlib_data/cn_1d')

date_dir = f'/cos-data/qlib_data/daily_update/{start_date}-{end_date}'
if not os.path.exists(date_dir):
    os.mkdir(date_dir)

save_dir = f'/cos-data/qlib_data/daily_update/{start_date}-{end_date}/cn_1min'
if not os.path.exists(save_dir):
    os.mkdir(save_dir)

download_config = {
    'save_dir': save_dir,
    'max_workers': 2,
    'delay': 0,
    'start': start_date,
    'end': end_date,
    # 'interval': '1d',
    # 'region': 'CN',
}



if __name__=="__main__":
    collector = YahooCollectorCN1min(
        **download_config
    )
    collector.collector_data()

    # save_dir = '/cos-data/qlib_data/daily_update/2024-03-22-2024-04-19/cn_1d'
    merge_csv(save_dir, '/cos-data/qlib_data/cn_1d')