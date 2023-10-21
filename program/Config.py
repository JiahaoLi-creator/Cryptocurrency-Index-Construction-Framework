import os

_ = os.path.abspath(os.path.dirname(__file__))  # 返回当前文件路径
root_path = os.path.abspath(os.path.join(_, '..'))  # 返回根目录文件夹

# k线数据路径
kline_path = root_path + '/data/k线数据/'

# 回测信息配置
start_date = '2021-01-01'  # 回测开始时间
end_date = '2023-03-10'  # 回测结束时间
hold_period = '7D'

# 参与计算的因子
factor_class_list = ['PriceMa', 'QuoteVolumeStd']
