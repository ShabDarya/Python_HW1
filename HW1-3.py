import asyncio
from itertools import product
import pandas as pd
import numpy as np
import statistics
import asyncio
import concurrent.futures
import time

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

time_s = time.time()


#функции для параллельного вычисления
def rolling_mean_async(data):
    series = pd.Series(data['temperature'])
    window_size = 30
    return series.rolling(window=window_size).mean()

def compute_rolling(data, r):
    ds = data[(data['city'] == r[0]) & (data['season'] == r[1])]
    mean_d = rolling_mean_async(ds)
    df_mean_v = pd.DataFrame()
    df_mean_v['temperature'] = mean_d
    df_mean_v['timestamp'] = ds['timestamp']
    df_mean_v['city'],df_mean_v['season'] = r[0], r[1]
    return df_mean_v
    
def compute_stats(data, r):
    ds = data[(data['city'] == r[0]) & (data['season'] == r[1])]
    mean_t = ds['temperature'].mean()
    d = pd.DataFrame([[r[0], r[1], mean_t, statistics.stdev(ds['temperature'], xbar=mean_t)]], columns=['city','season','mean_t','std_t'])
    return d

def anomaly_p(df_stat, x):
    stats = df_stat[(df_stat['city'] == x[1]['city']) & (df_stat['season'] == x[1]['season'])]
    a = float(stats['mean_t'] - 2 * stats['std_t'])
    b = float(stats['mean_t'] + 2 * stats['std_t'])
    if x[1]['temperature'] is not np.NaN:
        if (x[1]['temperature'] < a) | (x[1]['temperature'] > b):
            return True
    return False

#функции для обычного вычисления
def rolling_mean(data):
    series = pd.Series(data['temperature'])
    window_size = 30
    return series.rolling(window=window_size).mean()



def main():
    time_s = time.time()
    data = pd.read_csv('temperature_data.csv', sep = ',')
    citys = list(data['city'].unique())
    seasons = list(data['season'].unique())
    citys_seasons = list(product(citys, seasons))

    with concurrent.futures.ProcessPoolExecutor(max_workers=3) as executor:
        futures_stat = [executor.submit(compute_stats, data, r) for r in citys_seasons]
        futures_rolling = [executor.submit(compute_rolling, data, r) for r in citys_seasons]

    processed_stat = [future.result() for future in futures_stat]
    processed_rolling = [future.result() for future in futures_rolling]

    final_df_stat = pd.concat(processed_stat, ignore_index=True)
    final_df_rolling = pd.concat(processed_rolling, ignore_index=True)

    with concurrent.futures.ProcessPoolExecutor(max_workers=3) as executor:
        futures_anomaly = [executor.submit(anomaly_p, final_df_stat, r) for r in data.iterrows()]
    processed_anomaly = [future.result() for future in futures_anomaly]
    data['anomaly'] = processed_anomaly


    time_p = time.time() - time_s

    time_s = time.time()
    data1 = pd.read_csv('temperature_data.csv', sep = ',')
    citys = list(data1['city'].unique())
    seasons = list(data1['season'].unique())

    df_mean_rolling=pd.DataFrame()
    df_stat=pd.DataFrame(columns=['city','season','mean_t','std_t'])
    for c in citys:
        for s in seasons:
            ds = data1[(data1['city'] == c) & (data1['season'] == s)]
            mean_d = rolling_mean(ds)
            df_mean_v = pd.DataFrame()
            df_mean_v['temperature'] = mean_d
            df_mean_v['timestamp'] = ds['timestamp']
            df_mean_v['city'],df_mean_v['season'] = c,s
            
            mean_t = ds['temperature'].mean()
            df_s = pd.DataFrame([[c, s, mean_t, statistics.stdev(ds['temperature'], xbar=mean_t)]], columns=['city','season','mean_t','std_t'])
            df_stat = pd.concat([df_stat, df_s], ignore_index=True)

        df_mean_rolling = pd.concat([df_mean_rolling, df_mean_v], ignore_index=True) #скользящее среднее

    def anomaly(x):
        stats = df_stat[(df_stat['city'] == x['city']) & (df_stat['season'] == x['season'])]
        a = float(stats['mean_t'] - 2 * stats['std_t'])
        b = float(stats['mean_t'] + 2 * stats['std_t'])
        if x['temperature'] is not np.NaN:
            if (x['temperature'] < a) | (x['temperature'] > b):
                return True
        return False
    
    data1['anomaly'] = data1.apply(lambda x: anomaly(x), axis = 1)

    time_u = time.time() - time_s

    print(f'Время параллельного вычисления: {time_p}')
    print(f'Время обычного вычисления: {time_u}')


if __name__ == '__main__':
    main()