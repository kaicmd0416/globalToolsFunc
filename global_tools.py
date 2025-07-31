"""
global_tools.py
金融数据处理和投资组合计算工具包
提供股票、期货、期权、ETF等金融数据的处理和投资组合收益计算功能
"""
import pandas as pd
import numpy as np
import os
import shutil
import warnings
import json
import pymysql
import subprocess
import sys
from datetime import time, datetime, timedelta, date
from scipy.io import loadmat
import re
from dbutils.pooled_db import PooledDB
# 导入全局配置
from global_dic import get as glv
from portfolio_calculation import portfolio_calculation
from backtesting_tools import Back_testing_processing
from sql_saving import SqlSaving, DatabaseWriter
from sqlalchemy import text
from mktData_sql import mktData_sql
from mktData_local import mktData_local
# 忽略警告信息
warnings.filterwarnings("ignore")
from utils import *
from time_utils import *
# 全局变量
global source
source=source_getting()
# ============= 基础工具函数 =============
def sql_to_timeseries(df):
    df.columns = ['valuation_date', 'code', 'value']
    # 处理NULL值
    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    # 按valuation_date和code分组，将value列的值作为新的列
    df_pivot = df.pivot(index='valuation_date', columns='code', values='value')
    # 重置索引，使valuation_date成为列
    df_pivot = df_pivot.reset_index()
    return df_pivot
def rank_score_processing(df_score):
    """
    标准化分数生成
    
    Args:
        df_score (pandas.DataFrame): 包含valuation_date和code列的数据框
    
    Returns:
        pandas.DataFrame: 处理后的数据框
    """
    date_list = df_score['valuation_date'].unique().tolist()
    df_score = df_score.copy()  # Create a copy to avoid modifying the original
    final_list_score = []
    
    for i in date_list:
        slice_df = df_score[df_score['valuation_date'] == i]
        stock_code2 = slice_df['code'].tolist()
        list_score = np.array(range(len(stock_code2)))
        list_score = (list_score - np.mean(list_score)) / np.std(list_score)
        list_score = list(reversed(list_score))
        final_list_score.extend(list_score)
    
    df_score['final_score'] = final_list_score
    df_score.reset_index(inplace=True, drop=True)
    df_score['valuation_date'] = pd.to_datetime(df_score['valuation_date'])
    df_score['valuation_date'] = df_score['valuation_date'].astype(str)
    return df_score
def code_transfer(df):
    """
    股票代码格式转换
    
    Args:
        df (pandas.DataFrame): 包含code列的数据框
    
    Returns:
        pandas.DataFrame: 处理后的数据框
    """
    df.dropna(subset=['code'], axis=0, inplace=True)
    df['code'] = df['code'].astype(int)
    df['code'] = df['code'].apply(lambda x: '{:06d}'.format(x))
    
    def sz_sh(x):
        if str(x)[0] == '6' or str(x)[0] == '5':
            x = str(x) + '.SH'
        elif str(x)[0] == '0' or str(x)[0] == '3':
            x = str(x) + '.SZ'
        elif str(x)[:2] == '51' or str(x)[:2] == '11':
            x = str(x) + '.SH'
        elif str(x)[:2] == '15' or str(x)[:2] == '16' or str(x)[:2] == '12' or str(x)[:2] == '18':
            x = str(x) + '.SZ'
        elif str(x)[0] == '8' or str(x)[0] == '4' or str(x)[0] == '9':
            x = str(x) + '.BJ'
        else:
            print(x + '没有找到匹配规则')
            x = x
        return x
    
    df['code'] = df['code'].apply(lambda x: sz_sh(x))
    return df
# ============= 因子名称相关函数 =============
def factor_name_old():
    """
    获取旧版因子名称
    
    Returns:
        tuple: (barra_name, industry_name)
    """
    barra_name = ['country', 'size', 'beta', 'momentum', 'resvola', 'nlsize', 'btop', 
                  'liquidity', 'earningsyield', 'growth']
    industry_name = ['石油石化', '煤炭', '有色金属', '电力及公用事业', '钢铁', '基础化工', 
                    '建筑', '建材', '轻工制造', '机械', '电力设备', '国防军工', '汽车', 
                    '商贸零售', '餐饮旅游', '家电', '纺织服装', '医药', '食品饮料', 
                    '农林牧渔', '银行', '非银行金融', '房地产', '交通运输', '电子元器件', 
                    '通信', '计算机', '传媒', '综合']
    return barra_name, industry_name
def factor_name_new():
    """
    获取新版因子名称
    
    Returns:
        tuple: (barra_name, industry_name)
    """
    barra_name = ['country', 'size', 'beta', 'momentum', 'resvola', 'nlsize', 'btop', 
                  'liquidity', 'earningsyield', 'growth']
    industry_name = ['石油石化', '煤炭', '有色金属', '电力及公用事业', '钢铁', '基础化工', 
                    '建筑', '建材', '轻工制造', '机械', '电力设备及新能源', '国防军工', 
                    '汽车', '商贸零售', '消费者服务', '家电', '纺织服装', '医药', 
                    '食品饮料', '农林牧渔', '银行', '非银行金融', '房地产', '综合金融', 
                    '交通运输', '电子', '通信', '计算机', '传媒', '综合']
    return barra_name, industry_name

def factor_name(inputpath_factor):
    """
    从因子文件中提取因子名称
    
    Args:
        inputpath_factor (str): 因子文件路径
    
    Returns:
        tuple: (barra_name, industry_name)
    """
    annots = loadmat(inputpath_factor)['lnmodel_active_daily']['factornames'][0][0][0]
    annots = [np.array(i)[0] for i in annots]
    industry_name = [i for i in annots if '\u4e00' <= i <= '\u9fff']
    barra_name = [i for i in annots if '\u4e00' > i or i > '\u9fff']
    return barra_name, industry_name
# ============= 数据处理函数 =============
def weight_sum_check(df):
    """
    检查权重和
    
    Args:
        df (pandas.DataFrame): 包含weight列的数据框
    
    Returns:
        pandas.DataFrame: 处理后的数据框
    """
    weight_sum = df['weight'].sum()
    if weight_sum < 0.99:
        df['weight'] = df['weight'] / weight_sum
    return df

def weight_sum_warning(df):
    """
    权重和警告
    
    Args:
        df (pandas.DataFrame): 包含weight列的数据框
    """
    weight_sum = df['weight'].sum()
    if weight_sum < 0.99 or weight_sum > 1.01:
        print(f'权重和异常: {weight_sum}')

def stock_volatility_calculate(df, available_date):
    """
    计算股票波动率
    
    Args:
        df (pandas.DataFrame): 包含valuation_date列的数据框
        available_date (str): 日期
    
    Returns:
        pandas.DataFrame: 计算后的数据框
    """
    df = df[df['valuation_date'] <= available_date]
    df.set_index('valuation_date', inplace=True, drop=True)
    df = df.rolling(248).std()
    return df
def factor_universe_withdraw(type='new'):
    """
    获取股票池数据
    
    Args:
        type (str, optional): 类型（'new'或'old'）
    
    Returns:
        pandas.DataFrame: 股票池数据
    """
    if type == 'new':
        inputpath = glv('stock_universe_new')
        if source == 'sql':
            inputpath =str(inputpath)+ " Where type='stockuni_new'"
    elif type == 'old':
        inputpath = glv('stock_universe_old')
        if source == 'sql':
            inputpath =str(inputpath)+ " Where type='stockuni_old'"
    df_universe = data_getting_glb(inputpath)
    return df_universe
# ============= 指数数据函数 =============
def index_weight_withdraw(index_type, available_date):
    """
    提取指数权重股数据
    
    Args:
        index_type (str): 指数类型
        available_date (str): 日期
    
    Returns:
        pandas.DataFrame: 权重股数据
    """
    mkts=mktData_sql()
    mktl=mktData_local()
    if source == 'local':
        df=mktl.index_weight_withdraw_local(index_type, available_date)
    else:
        df=mkts.index_weight_withdraw_sql(index_type,available_date)
    if df.empty:
        print(f"未找到指数 {index_type} 在 {available_date} 的权重数据")
        return pd.DataFrame()
        
    if 'code' in df.columns and 'weight' in df.columns:
        df = df[['code', 'weight']]
    else:
        print(f"数据列不完整，期望的列: code, weight，实际的列: {df.columns.tolist()}")
        return pd.DataFrame()
    return df
def indexData_withdraw(index_type,start_date=None,end_date=None,columns=None,realtime=False):
    """
    提取指数收益率数据
    
    Args:
        index_type (str): 指数类型
        available_date (str): 日期
    
    Returns:
        float or None: 指数收益率
    """
    # 根据是否包含中文决定short_name的赋值
    mkts = mktData_sql()
    mktl = mktData_local()
    if realtime==False:
        if source == 'local':
            df=mktl.indexData_withdraw_local_daily(index_type,start_date,end_date,columns)
        else:
            df=mkts.indexData_withdraw_sql_daily(index_type,start_date,end_date,columns)
    else:
        if source == 'local':
            df=mktl.indexData_withdraw_local_realtime(index_type,columns)
        else:
            df=mkts.indexData_withdraw_sql_realtime(index_type,columns)
    return df
def indexFactor_withdraw(index_type,start_date=None,end_date=None):
    """
    提取指数因子暴露数据
    
    Args:
        index_type (str): 指数类型
        available_date (str): 日期
    
    Returns:
        pandas.DataFrame: 因子暴露数据
    """
    mkts = mktData_sql()
    mktl = mktData_local()
    if source=='local':
        df=mktl.indexFactor_withdraw_local(index_type,start_date,end_date)
    else:
        df=mkts.indexFactor_withdraw_sql(index_type,start_date,end_date)
    return df
# ============= 证券数据处理函数 =============
def stockData_withdraw(start_date=None,end_date=None,columns=None,realtime=False):
    mkts = mktData_sql()
    mktl = mktData_local()
    if realtime == True:
        if source == 'local':
            df=mktl.stockData_withdraw_local_realtime(columns)
        else:
            df=mkts.stockData_withdraw_sql_realtime(columns)
    else:
        if source == 'local':
            df=mktl.stockData_withdraw_local_daily(start_date,end_date,columns)
        else:
            df=mkts.stockData_withdraw_sql_daily(start_date,end_date,columns)
    return df
# ============= etf数据处理函数 =============
def etfData_withdraw(start_date=None,end_date=None,columns=None,realtime=False):
    mkts = mktData_sql()
    mktl = mktData_local()
    if realtime == True:
        if source == 'local':
            df=mktl.etfData_withdraw_local_realtime(columns)
        else:
            df=mkts.etfData_withdraw_sql_realtime(columns)
    else:
        if source == 'local':
            df=mktl.etfData_withdraw_local_daily(start_date,end_date,columns)
        else:
            df=mkts.etfData_withdraw_sql_daily(start_date,end_date,columns)
    return df

def cbdata_withdraw(available_date,realtime=False):
    """
    提取可转债数据
    
    Args:
        available_date (str): 日期
    
    Returns:
        pandas.DataFrame: 可转债数据
    """

    if realtime==False:
        yes2 = last_workday_calculate(available_date)
        int_yes2 = intdate_transfer(yes2)
        available_date2 = intdate_transfer(available_date)
    else:
        print('暂时没有realtime的可转债数据，用日频数据替代')
        available_date = date.today()
        available_date=strdate_transfer(available_date)
        yes = last_workday_calculate(available_date)
        available_date=yes
        available_date2=intdate_transfer(available_date)
        yes2 = last_workday_calculate(yes)
        int_yes2=intdate_transfer(yes2)
    inputpath_cbdata = glv('input_cbdata')
    if source == 'local':
        inputpath_cbdata1 = file_withdraw(inputpath_cbdata, available_date2)
        inputpath_cbdata2 = file_withdraw(inputpath_cbdata, int_yes2)
    else:
        inputpath_cbdata1 = inputpath_cbdata + f" WHERE valuation_date='{available_date}'"
        inputpath_cbdata2 = inputpath_cbdata + f" WHERE valuation_date='{yes2}'"
    df = data_getting_glb(inputpath_cbdata1)
    df2 = data_getting_glb(inputpath_cbdata2)
    try:
        df2 = df2[['code', 'delta']]
        df2.columns = ['code', 'delta_yes']
        df = df.merge(df2, on='code', how='left')
    except:
        df['delta_yes']=None
    return df

# ============= 期权和期货数据处理函数 =============
def get_string_before_last_dot(s):
    last_dot_index = s.rfind('.')
    if last_dot_index != -1:
        return s[:last_dot_index]
def optiondata_greeksprocessing(df):
    """
    处理期权Greeks数据，将delta_wind和implied_vol_wind的缺失值用delta和impliedvol补充，
    然后删除原始列并将wind列重命名
    
    Args:
        df (pandas.DataFrame): 包含delta, delta_wind, impliedvol, implied_vol_wind列的数据框
    
    Returns:
        pandas.DataFrame: 处理后的数据框
    """
    if df.empty:
        return df
        
    # 复制数据框以避免修改原始数据
    df = df.copy()
    # 处理delta_wind缺失值
    if 'delta_wind' in df.columns and 'delta' in df.columns:
        # 处理字符串'None'和Python None值
        mask = (df['delta_wind'] == 'None') | (df['delta_wind'].isna())
        df.loc[mask, 'delta_wind'] = df.loc[mask, 'delta']
        # 删除原始delta列
        df = df.drop(columns=['delta'])
        # 重命名delta_wind为delta
        df = df.rename(columns={'delta_wind': 'delta'})
    
    # 处理implied_vol_wind缺失值
    if 'implied_vol_wind' in df.columns and 'impliedvol' in df.columns:
        # 处理字符串'None'和Python None值
        mask = (df['implied_vol_wind'] == 'None') | (df['implied_vol_wind'].isna())
        df.loc[mask, 'implied_vol_wind'] = df.loc[mask, 'impliedvol']
        # 删除原始impliedvol列
        df = df.drop(columns=['impliedvol'])
        # 重命名implied_vol_wind为implied_vol
        df = df.rename(columns={'implied_vol_wind': 'implied_vol'})
    return df
def optiondata_withdraw(available_date,realtime=False):
    """
    提取期权数据
    
    Args:
        available_date (str): 日期
    
    Returns:
        pandas.DataFrame: 期权数据
    """
    if realtime==False:
        yes = last_workday_calculate(available_date)
        yes2 = intdate_transfer(yes)
        available_date2 = intdate_transfer(available_date)
        inputpath_optiondata = glv('input_optiondata')
        if source == 'local':
            inputpath_optiondata1 = file_withdraw(inputpath_optiondata, available_date2)
            inputpath_optiondata2 = file_withdraw(inputpath_optiondata, yes2)
        else:
            inputpath_optiondata1 = inputpath_optiondata + f" WHERE valuation_date='{available_date}'"
            inputpath_optiondata2 = inputpath_optiondata + f" WHERE valuation_date='{yes}'"
        df = data_getting_glb(inputpath_optiondata1)
        df2 = data_getting_glb(inputpath_optiondata2)
        df = optiondata_greeksprocessing(df)
        df2 = optiondata_greeksprocessing(df2)
    else:
        available_date=date.today()
        yes = last_workday_calculate(available_date)
        yes2 = intdate_transfer(yes)
        inputpath_optiondata = glv('input_optiondata')
        inputpath_optiondata_realtime=glv('input_optiondata_realtime')
        if source == 'local':
            df = data_getting_glb(inputpath_optiondata_realtime, 'option_info')
            inputpath_optiondata2 = file_withdraw(inputpath_optiondata, yes2)
        else:
            inputpath_optiondata_realtime = inputpath_optiondata_realtime + f" WHERE type='option'"
            df = data_getting_glb(inputpath_optiondata_realtime)
            inputpath_optiondata2 = inputpath_optiondata + f" WHERE valuation_date='{yes}'"
        df2 = data_getting_glb(inputpath_optiondata2)
        df2 = optiondata_greeksprocessing(df2)
    
    if realtime == True:
        if source == 'local':
            df = df[['代码', '现价', '前收盘价','前结算价', 'Delta','中价隐含波动率']]
            df.columns = ['code', 'close','pre_close', 'pre_settle', 'delta','implied_vol']
        else:
            df = df[['code', 'close','pre_close', 'pre_settle', 'delta','implied_vol']]
        df['code'] = df['code'].apply(lambda x: get_string_before_last_dot(x))
        try:
            df2 = df2[['code', 'delta']]
            df2.columns = ['code', 'delta_yes']
            df_final = df.merge(df2, on='code', how='left')
        except:
            df_final = df
    else:
        df2 = df2[['code', 'delta']]
        df2.columns = ['code', 'delta_yes']
        df_final = df.merge(df2, on='code', how='left')
    
    return df_final
def futuredata_withdraw(available_date,realtime=False):
    if realtime == False:
        available_date2 = intdate_transfer(available_date)
        inputpath_futuredata = glv('input_futuredata')
        if source == 'local':
            inputpath_futuredata = file_withdraw(inputpath_futuredata, available_date2)
        else:
            inputpath_futuredata = inputpath_futuredata + f" WHERE valuation_date='{available_date}'"
        df = data_getting_glb(inputpath_futuredata)
    else:
        inputpath_futuredata = glv('input_futuredata_realtime')
        if source == 'local':
            df = data_getting_glb(inputpath_futuredata, 'future_info')
        else:
            inputpath_futuredata = inputpath_futuredata + f" WHERE type='future'"
            df = data_getting_glb(inputpath_futuredata)
    if realtime == True:
        if source == 'local':
            df = df[['代码', '现价', '前结算价', '前收盘价','合约系数']]
        else:
            df = df[['code', 'close', 'pre_settle','pre_close', 'multiplier']]
        df.columns = ['code', 'close', 'pre_settle','pre_close', 'multiplier']
        df['code'] = df['code'].apply(lambda x: get_string_before_last_dot(x))
    else:
        df['code'] = df['code'].apply(lambda x: get_string_before_last_dot(x))
    return df
def weight_df_standardization(df):
    """
            标准化权重数据

            Args:
                df (pandas.DataFrame): 包含code列的数据框

            Returns:
                pandas.DataFrame: 标准化后的数据框
            """
    if 'code' not in df.columns:
        print("警告: DataFrame中没有code列")
        return df

    df = df.copy()
    df['code'] = df['code'].astype(str)
    df['code']=df['code'].str.upper()
    df['code'] = df['code'].str.strip()

    # 根据code长度分类
    df_security = df[df['code'].str.len() < 12].copy()
    df_option = df[df['code'].str.len() >= 12].copy()

    # 处理证券代码
    if not df_security.empty:
        # 处理带.的代码
        df_security['code'] = df_security['code'].apply(lambda x: x.split('.')[0] if '.' in x else x)

        # 分离股票和期货
        mask_alpha = df_security['code'].str.contains('[A-Za-z]', regex=True)
        df_stock = df_security[~mask_alpha].copy()
        df_future = df_security[mask_alpha].copy()

        # 处理股票代码
        if not df_stock.empty:
            # 补齐股票代码前导零
            df_stock['code'] = df_stock['code'].apply(lambda x: x.zfill(6))
            # 使用code_transfer处理股票代码
            df_stock = code_transfer(df_stock)

        # 处理期货代码
        if not df_future.empty:
            def process_future_code(code):
                patterns = [
                    r"[A-Z]{2}\d{4}",
                    r"[A-Z]{1}\d{4}",
                    r"[A-Z]{2}\d{3}"
                ]
                for pattern in patterns:
                    match = re.search(pattern, code)
                    if match:
                        return match.group(0)
                return code

            df_future['code'] = df_future['code'].apply(process_future_code)

    # 处理期权代码
    if not df_option.empty:
        def process_option_code(code):
            pattern = r"[A-Z]{2}\d{4}-[PC]-\d{4}"
            match = re.search(pattern, code)
            if match:
                return match.group(0)
            return code

        df_option['code'] = df_option['code'].apply(process_option_code)

    # 合并所有处理后的数据框
    result_dfs = []
    if 'df_stock' in locals() and not df_stock.empty:
        result_dfs.append(df_stock)
    if 'df_future' in locals() and not df_future.empty:
        result_dfs.append(df_future)
    if not df_option.empty:
        result_dfs.append(df_option)

    # 如果没有任何数据需要处理，返回原始数据框
    if not result_dfs:
        return df

    return pd.concat(result_dfs, ignore_index=True)

def portfolio_analyse(start_date=None,end_date=None,df_initial=pd.DataFrame(),df_holding=pd.DataFrame(),account_money=10000000,index_type=None,cost_stock=0.00085,cost_etf=0.0003,cost_future=0.00006,cost_option=0.01,cost_convertiblebond=0.0007,realtime=False,adj_source='wind',weight_standardize=False):
    if weight_standardize==True:
        if not df_initial.empty:
            df_initial=weight_df_standardization(df_initial)
        if not df_holding.empty:
            df_holding=weight_df_standardization(df_holding)
    df_final=pd.DataFrame()
    if realtime==True:
        today = date.today()
        today = today.strftime('%Y-%m-%d')
        day_list=[today]
    else:
        if start_date==None or end_date==None:
            print('start_date和end_date不能为None')
            raise ValueError
        else:
            day_list=working_days_list(start_date,end_date)
    i=0
    day_list2=[]
    for available_date in day_list:
         df_stock=stockdata_withdraw(available_date,realtime)
         df_future=futuredata_withdraw(available_date,realtime)
         df_etf=etfdata_withdraw(available_date,realtime)
         df_option=optiondata_withdraw(available_date,realtime)
         df_convertible_bond=cbdata_withdraw(available_date,realtime)
         df_adj_factor=stockdata_adj_withdraw(available_date,realtime,adj_source)
         if df_stock.empty:
             print(str(available_date) + 'stock_data为空,可能会导致计算结果不准')
         if df_future.empty:
             print(str(available_date) + 'future_data为空,可能会导致计算结果不准')
         if df_etf.empty:
             print(str(available_date) + 'etf_data为空,可能会导致计算结果不准')
         if df_option.empty:
             print(str(available_date) + 'option_data为空,可能会导致计算结果不准')
         if df_convertible_bond.empty:
             print(str(available_date) + 'onvertible_bond_data为空,可能会导致计算结果不准')
         if df_adj_factor.empty:
             print(str(available_date) + 'stock_adj_factor为空,可能会导致计算结果不准')
         if df_adj_factor.empty:
             print(str(available_date) + 'stock_adj_factor为空,可能会导致计算结果不准')
         if df_stock.empty and df_future.empty and df_etf.empty and df_option.empty and df_convertible_bond.empty and df_adj_factor.empty:
            print(str(available_date) + '全部数据为空无法计算')
         else:
             if len(day_list) > 1:
                 if i == 0:
                     pc = portfolio_calculation(df_initial, df_holding, df_stock, df_etf, df_option, df_future,
                                                df_convertible_bond, df_adj_factor, account_money,cost_stock,cost_etf,cost_future,cost_option,cost_convertiblebond,realtime)
                     df_portfolio = pc.portfolio_calculation_main()
                     i += 1
                 else:
                     df_initial = df_holding
                     pc = portfolio_calculation(df_initial, df_holding, df_stock, df_etf, df_option, df_future,
                                                df_convertible_bond, df_adj_factor, account_money,cost_stock,cost_etf,cost_future,cost_option,cost_convertiblebond,realtime)
                     df_portfolio = pc.portfolio_calculation_main()
             else:
                 pc = portfolio_calculation(df_initial, df_holding, df_stock, df_etf, df_option, df_future,
                                            df_convertible_bond, df_adj_factor, account_money,cost_stock,cost_etf,cost_future,cost_option,cost_convertiblebond,realtime)
                 df_portfolio = pc.portfolio_calculation_main()
             if index_type != None:
                 index_return = crossSection_index_return_withdraw(index_type, available_date, realtime)
             else:
                 index_return = 0
             df_portfolio['index_return'] = index_return
             df_portfolio['excess_return'] = df_portfolio['portfolio_return'] - df_portfolio['index_return']
             df_portfolio['excess_return_paper'] = df_portfolio['paper_return'] - df_portfolio['index_return']
             df_final = pd.concat([df_final, df_portfolio])
             day_list2.append(available_date)
    df_final['valuation_date'] = day_list2
    df_final = df_final[['valuation_date'] + df_final.columns.tolist()[:-1]]
    return df_final
def portfolio_analyse_manual(start_date=None,end_date=None,df_initial=pd.DataFrame(),df_holding=pd.DataFrame(),detail=False,df_stock=pd.DataFrame(),df_future=pd.DataFrame(),df_etf=pd.DataFrame(),df_option=pd.DataFrame(),df_convertible_bond=pd.DataFrame(),df_adj_factor=pd.DataFrame(),account_money=10000000,index_type=None,cost_stock=0.00085,cost_etf=0.0003,cost_future=0.00006,cost_option=0.01,cost_convertiblebond=0.0007,realtime=False,adj_source='wind',weight_standardize=False):
    if weight_standardize==True:
        if not df_initial.empty:
            df_initial=weight_df_standardization(df_initial)
        if not df_holding.empty:
            df_holding=weight_df_standardization(df_holding)
    df_final=pd.DataFrame()
    df_detail=pd.DataFrame()
    if realtime==True:
        today = date.today()
        today = today.strftime('%Y-%m-%d')
        day_list=[today]
    else:
        if start_date==None or end_date==None:
            print('start_date和end_date不能为None')
            raise ValueError
        else:
            day_list=working_days_list(start_date,end_date)
    i=0
    day_list2=[]
    for available_date in day_list:
        if len(day_list) > 1:
            if i == 0:
                pc = portfolio_calculation(df_initial, df_holding, df_stock, df_etf, df_option, df_future,
                                           df_convertible_bond, df_adj_factor, account_money, cost_stock, cost_etf,
                                           cost_future, cost_option, cost_convertiblebond,realtime)
                if detail==False:
                    df_portfolio = pc.portfolio_calculation_main(detail)
                else:
                    df_portfolio,df=pc.portfolio_calculation_main(detail)
                i += 1
            else:
                df_initial = df_holding
                pc = portfolio_calculation(df_initial, df_holding, df_stock, df_etf, df_option, df_future,
                                           df_convertible_bond, df_adj_factor, account_money, cost_stock, cost_etf,
                                           cost_future, cost_option, cost_convertiblebond,realtime)
                if detail == False:
                    df_portfolio = pc.portfolio_calculation_main(detail)
                else:
                    df_portfolio, df = pc.portfolio_calculation_main(detail)
        else:
            pc = portfolio_calculation(df_initial, df_holding, df_stock, df_etf, df_option, df_future,
                                       df_convertible_bond, df_adj_factor, account_money, cost_stock, cost_etf,
                                       cost_future, cost_option, cost_convertiblebond,realtime)
            if detail == False:
                df_portfolio = pc.portfolio_calculation_main(detail)
            else:
                df_portfolio, df = pc.portfolio_calculation_main(detail)
        if index_type != None:
            index_return = crossSection_index_return_withdraw(index_type, available_date, realtime)
        else:
            index_return = 0
        df_portfolio['index_return'] = index_return
        df_portfolio['excess_return'] = df_portfolio['portfolio_return'] - df_portfolio['index_return']
        df_portfolio['excess_return_paper'] = df_portfolio['paper_return'] - df_portfolio['index_return']
        df_final = pd.concat([df_final, df_portfolio])
        if detail==True:
            df_detail=pd.concat([df_detail,df])
        day_list2.append(available_date)
    df_final['valuation_date'] = day_list2
    df_final = df_final[['valuation_date'] + df_final.columns.tolist()[:-1]]
    if detail==False:
         return df_final
    else:
        return df_final,df_detail
#回测标准化模块:
def backtesting_report(df_portfolio=pd.DataFrame(),outputpath=None,index_type=None,signal_name='portfolio'):
    df_indexreturn=timeSeries_index_return_withdraw()
    BTP=Back_testing_processing(df_indexreturn)
    if df_portfolio.empty:
        print('输入的portfolio不能为空')
    if outputpath==None:
        print('输入的outputpth不能为空')
    if not df_portfolio.empty and outputpath!=None:
        if 'valuation_date' not in df_portfolio.columns.tolist():
            print('输入的portfolio必须为时序数据')
        else:
            # 根据是否包含中文决定index_short的赋值
            if contains_chinese(index_type):
                index_short = index_mapping(index_type,'code')
            else:
                index_short = index_type
            BTP.back_testing_history(df_portfolio, outputpath, index_short, signal_name)
#入库标准化模块
class sqlSaving_main:
    def __init__(self,config_path=None,parameter_name=None,delete=False):
        self.config_path=config_path
        self.parameter_name=parameter_name
        self.delete=delete
        if self.config_path == None:
            print('找不到对应配置文件')
            raise TypeError
        else:
            if self.parameter_name == None:
                print('参数名不能为空')
                raise TypeError
            else:
                self.SS = SqlSaving(self.config_path, self.parameter_name,self.delete)
    def df_to_sql(self,df=pd.DataFrame(),delete_name=None,delet_key=None):
        if df.empty:
            print('输入的文件为空，无法入库')
            pass
        else:
            self.SS.process_file(df,delete_name,delet_key)

def table_manager(config_path, table_name):
    """
    删除指定的数据库表
    
    Args:
        config_path (str): 配置文件路径
        table_name (str): 要删除的表名
    
    Returns:
        bool: 操作是否成功
    """
    try:
        # 获取数据库连接
        conn = get_db_connection(config_path)
        if conn is None:
            print("无法连接到数据库")
            return False
            
        # 创建游标
        cursor = conn.cursor()
        
        # 执行DROP TABLE语句
        drop_query = f"DROP TABLE IF EXISTS {table_name};"
        cursor.execute(drop_query)
        
        # 提交更改
        conn.commit()
        
        # 关闭游标和连接
        cursor.close()
        conn.close()
        
        print(f"成功删除表: {table_name}")
        return True
        
    except Exception as e:
        print(f"删除表时发生错误: {str(e)}")
        return False







