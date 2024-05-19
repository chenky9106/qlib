
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

import datetime
CUR_DIR = Path(__file__).resolve().parent
sys.path.append(str(CUR_DIR.parent.parent))

from data_collector.yahoo.collector import * # YahooCollector, YahooCollectorCN1d, YahooCollectorCN1min
from dump_bin import DumpDataUpdate, DumpDataAll

import argparse

parser = argparse.ArgumentParser(description="Yahoo data collector")
parser.add_argument("--interval", type=str, default="1d", help="The interval of the data, 1min or 1d")
parser.add_argument("--region", type=str, default="cn", help="The region of the data, cn or us")
parser.add_argument("--root_path", type=str, default="/cos-data/qlib_data", help="The start date of the data")
args = parser.parse_args()


def get_date_interval(save_data_path, benchmark='sh000300.csv'):
    # end date is today
    if args.interval == '1min':
        end_date = datetime.datetime.now().strftime('%Y-%m-%d') #  %H:%M:%S
        # start date is the last update
        file_list = os.listdir(save_data_path)
        target_csv = pd.read_csv(save_data_path + '/' + benchmark)
        start_date = target_csv['date'].sort_index(ascending=True).values[-1]
        print('start_date', start_date, type(start_date), len(start_date))
    else:
        end_date = (datetime.datetime.now() + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        # start date is the last update
        file_list = os.listdir(save_data_path)
        target_csv = pd.read_csv(save_data_path + '/' + benchmark)
        start_date = target_csv['date'].sort_index(ascending=True).values[-1]

    # if args.interval == '1min':
    #     if len(start_date) > 25:
    #         start_date = start_date[:25]
    # elif args.interval == '1d':
    if len(start_date) > 10:
        start_date = start_date[:10]
    # else:
    #     raise NotImplementedError
    print(start_date, end_date)
    assert start_date < end_date, "The data is already up to date."

    return start_date, end_date

def standard_date_format(df, interval='1d'):
    if args.interval == '1d':
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
    elif args.interval == '1min':
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d %H:%M:%S')
    df = df.drop_duplicates(subset=['date'])
    return df


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
        # apply the same date format to date column
        merged_csv = standard_date_format(merged_csv, interval=args.interval)
        merged_csv = merged_csv.drop_duplicates(subset=['date'])
        merged_csv = merged_csv.fillna(0.0)

        merged_csv.to_csv(f'{old_data_path}/{file_name}', index=False)
    
    print('Merge csv files successfully.')


benchmark_list = {
    'cn_1d': 'sh000300.csv',
    'cn_1min': 'sz000300.ss.csv',
    'us_1d': 'AAPL.csv',
    'us_1min': 'AAPL.csv'
}
benchmark = benchmark_list[f'{args.region}_{args.interval}']

start_date, end_date = get_date_interval(
    save_data_path=f'{args.root_path}/{args.region}_{args.interval}',
    benchmark=benchmark
)

date_dir = f'{args.root_path}/daily_update/{start_date}-{end_date}'
if not os.path.exists(date_dir):
    os.mkdir(date_dir)

save_dir = f'{args.root_path}/daily_update/{start_date}-{end_date}/{args.region}_{args.interval}'
if not os.path.exists(save_dir):
    os.mkdir(save_dir)

download_config = {
    'save_dir': save_dir,
    'max_workers': 2,
    'delay': 0,
    'start': start_date,
    'end': end_date,
    'interval': args.interval,
    # 'region': 'CN',
}

normalize_config = {
    'source_dir': f'{args.root_path}/{args.region}_{args.interval}',
    'target_dir' : f'{args.root_path}/{args.region}_{args.interval}_nor',
    'max_workers' : 2, 
    'region': args.region,
    'interval': args.interval,
    'qlib_data_1d_dir': f'{args.root_path}/{args.region}_1d' if args.interval == '1min' else None,
}

dump_config = {
    'csv_path': f'{args.root_path}/{args.region}_{args.interval}_nor',
    'qlib_dir': f'{args.root_path}/{args.region}_{args.interval}_bin',
    'exclude_fields': "symbol,date",
    'max_workers': 2,
}

qlib_data_1d_dir = f'{args.root_path}/{args.region}_1d'


if __name__=="__main__":
    collector_name = f"YahooCollector{args.region.upper()}{args.interval}"
    # collector = YahooCollectorCN1d(
    #     **download_config
    # )
    print(collector_name)
    collector_type = globals()[collector_name]
    collector = collector_type(
        **download_config
    )
    collector.collector_data()
    # save_dir = '/cos-data/qlib_data/daily_update/2024-03-22-2024-04-19/cn_1d'
    merge_csv(save_dir, f'{args.root_path}/{args.region}_{args.interval}')

    # normalizer_name = f"YahooNormalize{args.region.upper()}{args.interval}"
    # print(normalizer_name)
    # normalizer_type = globals()[normalizer_name]
    # normalize_config['normalize_class'] = normalizer_type
    # normalizer = Normalize(
    #     **normalize_config
    # )
    # normalizer.normalize()
    # print('Normalization successfully.')

    # if not os.path.exists(f'{args.root_path}/{args.region}_{args.interval}_bin'):
    #     os.mkdir(f'{args.root_path}/{args.region}_{args.interval}_bin')
    #     dumper = DumpDataAll(**dump_config)
    #     dumper.dump()
    # else:

    #     dumper = DumpDataUpdate(
    #         **dump_config
    #     )
    #     dumper.dump()

    # # parse index
    # _region = args.region.lower()
    # if _region not in ["cn", "us"]:
    #     logger.warning(f"Unsupported region: region={_region}, component downloads will be ignored")
    # else:
    #     index_list = ["CSI100", "CSI300"] if _region == "cn" else ["SP500", "NASDAQ100", "DJIA", "SP400"]
    #     get_instruments = getattr(
    #         importlib.import_module(f"data_collector.{_region}_index.collector"), "get_instruments"
    #     )
    #     for _index in index_list:
    #         get_instruments(str(qlib_data_1d_dir), _index, market_index=f"{_region}_index")
