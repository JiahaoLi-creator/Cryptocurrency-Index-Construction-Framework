from datetime import datetime
from glob import glob
import pandas as pd
from joblib import Parallel, delayed
from Config import *
from Functions import *
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 5000)  # 最多显示数据的行数


def calc_factors(file_path, benchmark):
    print(file_path)
    # =读入数据
    df = pd.read_csv(file_path, encoding='gbk', skiprows=1, parse_dates=['candle_begin_time'])

    # =合并基准数据
    df = pd.merge(left=df, right=benchmark, on='candle_begin_time', how='right', sort=True, indicator=True)
    if df.empty:
        return pd.DataFrame()

    # ===处理原始数据
    fillna_list = ['open', 'high', 'low', 'close']
    df.loc[:, fillna_list] = df[fillna_list].fillna(method='ffill')
    df.sort_values(by='candle_begin_time', inplace=True)
    df.drop_duplicates(subset=['candle_begin_time'], inplace=True, keep='last')
    df['涨跌幅'] = df['close'].pct_change()
    df['开盘买入涨跌幅'] = df['close'] / df['open'] - 1
    df.reset_index(drop=True, inplace=True)

    # 拷贝一份数据，转换成日线，计算日线的因子
    df_d = df.copy()
    # ===进行周期转换，将小时数据转成日线数据，然后去计算日线的因子
    df_d.set_index('candle_begin_time', inplace=True)
    # 必备字段
    agg_dict = {
        'symbol': 'first',
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'quote_volume': 'sum',
        'volume': 'sum',
    }
    df_d = df_d.resample('1D').agg(agg_dict)
    df_d.sort_values(by='candle_begin_time', inplace=True)
    df_d.reset_index(inplace=True)

    # ===计算选币因子
    df_d, factor_column_list = calc_factors_for_filename(df_d, factor_class_list, filename='factors')
    df_d = df_d[['candle_begin_time'] + factor_column_list]
    # 将日线因子合并到小时级别数据中去
    df = pd.merge(left=df, right=df_d, on='candle_begin_time', how='left')

    # =将日线数据转化为月线或者周线
    exg_dict = {'open': 'first', 'close': 'last'}
    # 对每个因子设置转换规则
    for f in factor_column_list:
        exg_dict[f] = 'first'
    df = trans_period_for_period(df, hold_period, exg_dict)

    return df


if __name__ == '__main__':
    # 定义一个开始的基准时间
    benchmark = pd.DataFrame(pd.date_range(start='2017-01-01', end=end_date, freq='1H'))  # 创建2017-01-01至回测结束时间的1H列表
    benchmark.rename(columns={0: 'candle_begin_time'}, inplace=True)

    # 标记开始时间
    start_time = datetime.now()

    # 获取所有文件路径
    symbol_file_path = glob(kline_path + '*USDT.csv')  # 获取kline_path路径下，所有以usdt.csv结尾的文件路径

    # 并行处理
    multiply_process = True
    if multiply_process:
        df_list = Parallel(n_jobs=max(os.cpu_count() - 1, 1))(
            delayed(calc_factors)(file_path, benchmark)
            for file_path in symbol_file_path
        )
    else:
        df_list = []
        for file_path in symbol_file_path:
            data = calc_factors(file_path, benchmark)
            df_list.append(data)
    print('读入完成, 开始合并:', datetime.now() - start_time)

    # 合并为一个大的DataFrame
    all_stock_data = pd.concat(df_list, ignore_index=True)
    all_stock_data.sort_values('candle_begin_time', inplace=True)
    all_stock_data.reset_index(inplace=True, drop=True)
    all_stock_data.to_pickle(f'{root_path}/data/数据整理/all_data_{hold_period}.pkl')
    print(datetime.now() - start_time)
