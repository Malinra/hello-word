import tushare as ts

print(ts.get_hist_data('600848')) #一次性获取全部数据


ts.get_hist_data('600848',start='2015-01-05',end='2015-01-09')
