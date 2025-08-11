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
source = source_getting()

# ============= 基础工具函数 =============
def sql_to_timeseries(df):
    """
    将SQL查询结果转换为时间序列格式
    
    将包含valuation_date、code、value三列的DataFrame转换为宽格式时间序列
    
    Args:
        df (pandas.DataFrame): 包含valuation_date、code、value列的数据框
    
    Returns:
        pandas.DataFrame: 转换后的时间序列数据框，每行一个日期，每列一个代码
    """
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
    
    对每个日期的分数进行标准化处理，生成标准正态分布的分数
    
    Args:
        df_score (pandas.DataFrame): 包含valuation_date和code列的数据框
    
    Returns:
        pandas.DataFrame: 处理后的数据框，包含标准化后的final_score列
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
    
    将数字格式的股票代码转换为标准格式（如000001.SZ）
    
    Args:
        df (pandas.DataFrame): 包含code列的数据框
    
    Returns:
        pandas.DataFrame: 处理后的数据框，code列转换为标准格式
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
    
    返回旧版本的Barra因子名称和行业因子名称列表
    
    Returns:
        tuple: (barra_name, industry_name) - Barra因子名称列表和行业因子名称列表
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
    
    返回新版本的Barra因子名称和行业因子名称列表
    
    Returns:
        tuple: (barra_name, industry_name) - Barra因子名称列表和行业因子名称列表
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
    
    从MATLAB格式的因子文件中提取Barra因子和行业因子名称
    
    Args:
        inputpath_factor (str): 因子文件路径
    
    Returns:
        tuple: (barra_name, industry_name) - Barra因子名称列表和行业因子名称列表
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
    
    检查权重列的和是否为1，如果不是则进行标准化
    
    Args:
        df (pandas.DataFrame): 包含weight列的数据框
    
    Returns:
        pandas.DataFrame: 处理后的数据框，权重已标准化
    """
    weight_sum = df['weight'].sum()
    if weight_sum < 0.99:
        df['weight'] = df['weight'] / weight_sum
    return df

def weight_sum_warning(df):
    """
    权重和警告
    
    检查权重列的和是否在合理范围内，如果不在则发出警告
    
    Args:
        df (pandas.DataFrame): 包含weight列的数据框
    """
    weight_sum = df['weight'].sum()
    if weight_sum < 0.99 or weight_sum > 1.01:
        print(f'权重和异常: {weight_sum}')

def stock_volatility_calculate(df, available_date):
    """
    计算股票波动率
    
    计算指定日期之前的股票收益率波动率（滚动252天标准差）
    
    Args:
        df (pandas.DataFrame): 包含valuation_date列的数据框
        available_date (str): 计算截止日期
    
    Returns:
        pandas.DataFrame: 计算后的波动率数据框
    """
    df = df[df['valuation_date'] <= available_date]
    df.set_index('valuation_date', inplace=True, drop=True)
    df = df.rolling(248).std()
    return df

def factor_universe_withdraw(type='new'):
    """
    获取股票池数据
    
    根据类型获取新版本或旧版本的股票池数据
    
    Args:
        type (str, optional): 类型（'new'或'old'），默认为'new'
    
    Returns:
        pandas.DataFrame: 股票池数据
    """
    if type == 'new':
        inputpath = glv('stock_universe_new')
        if source == 'sql':
            inputpath = str(inputpath) + " Where type='stockuni_new'"
    elif type == 'old':
        inputpath = glv('stock_universe_old')
        if source == 'sql':
            inputpath = str(inputpath) + " Where type='stockuni_old'"
    df_universe = data_getting_glb(inputpath)
    return df_universe

# ============= 指数数据函数 =============
def index_weight_withdraw(index_type, available_date):
    """
    提取指数权重股数据
    
    获取指定指数在指定日期的成分股权重数据
    
    Args:
        index_type (str): 指数类型
        available_date (str): 日期
    
    Returns:
        pandas.DataFrame: 权重股数据，包含code和weight列
    """
    mkts = mktData_sql()
    mktl = mktData_local()
    if source == 'local':
        df = mktl.index_weight_withdraw_local(index_type, available_date)
    else:
        df = mkts.index_weight_withdraw_sql(index_type, available_date)
    if df.empty:
        print(f"未找到指数 {index_type} 在 {available_date} 的权重数据")
        return pd.DataFrame()
        
    if 'code' in df.columns and 'weight' in df.columns:
        df = df[['code', 'weight']]
    else:
        print(f"数据列不完整，期望的列: code, weight，实际的列: {df.columns.tolist()}")
        return pd.DataFrame()
    return df

def indexData_withdraw(index_type=None, start_date=None, end_date=None, columns=None, realtime=False):
    """
    提取指数收益率数据
    
    获取指定指数在指定时间范围内的数据
    
    Args:
        index_type (str, optional): 指数类型
        start_date (str, optional): 开始日期
        end_date (str, optional): 结束日期
        columns (list, optional): 需要的列名
        realtime (bool, optional): 是否为实时数据，默认为False
    
    Returns:
        pandas.DataFrame: 指数数据
    """
    # 根据是否包含中文决定short_name的赋值
    mkts = mktData_sql()
    mktl = mktData_local()
    if realtime == False:
        if source == 'local':
            df = mktl.indexData_withdraw_local_daily(index_type, start_date, end_date, columns)
        else:
            df = mkts.indexData_withdraw_sql_daily(index_type, start_date, end_date, columns)
    else:
        if source == 'local':
            df = mktl.indexData_withdraw_local_realtime(index_type, columns)
        else:
            df = mkts.indexData_withdraw_sql_realtime(index_type, columns)
    return df

def indexFactor_withdraw(index_type, start_date=None, end_date=None):
    """
    提取指数因子暴露数据
    
    获取指定指数在指定时间范围内的因子暴露数据
    
    Args:
        index_type (str): 指数类型
        start_date (str, optional): 开始日期
        end_date (str, optional): 结束日期
    
    Returns:
        pandas.DataFrame: 因子暴露数据
    """
    mkts = mktData_sql()
    mktl = mktData_local()
    if source == 'local':
        df = mktl.indexFactor_withdraw_local(index_type, start_date, end_date)
    else:
        df = mkts.indexFactor_withdraw_sql(index_type, start_date, end_date)
    return df

# ============= 证券数据处理函数 =============
def stockData_withdraw(start_date=None, end_date=None, columns=None, realtime=False):
    """
    提取股票数据
    
    获取指定时间范围内的股票数据
    
    Args:
        start_date (str, optional): 开始日期
        end_date (str, optional): 结束日期
        columns (list, optional): 需要的列名
        realtime (bool, optional): 是否为实时数据，默认为False
    
    Returns:
        pandas.DataFrame: 股票数据
    """
    mkts = mktData_sql()
    mktl = mktData_local()
    if realtime == True:
        if source == 'local':
            df = mktl.stockData_withdraw_local_realtime(columns)
        else:
            df = mkts.stockData_withdraw_sql_realtime(columns)
    else:
        if source == 'local':
            df = mktl.stockData_withdraw_local_daily(start_date, end_date, columns)
        else:
            df = mkts.stockData_withdraw_sql_daily(start_date, end_date, columns)
    return df

# ============= etf数据处理函数 =============
def etfData_withdraw(start_date=None, end_date=None, columns=None, realtime=False):
    """
    提取ETF数据
    
    获取指定时间范围内的ETF数据
    
    Args:
        start_date (str, optional): 开始日期
        end_date (str, optional): 结束日期
        columns (list, optional): 需要的列名
        realtime (bool, optional): 是否为实时数据，默认为False
    
    Returns:
        pandas.DataFrame: ETF数据
    """
    mkts = mktData_sql()
    mktl = mktData_local()
    if realtime == True:
        if source == 'local':
            df = mktl.etfData_withdraw_local_realtime(columns)
        else:
            df = mkts.etfData_withdraw_sql_realtime(columns)
    else:
        if source == 'local':
            df = mktl.etfData_withdraw_local_daily(start_date, end_date, columns)
        else:
            df = mkts.etfData_withdraw_sql_daily(start_date, end_date, columns)
    return df

def cbData_withdraw(start_date=None, end_date=None, columns=None, realtime=False):
    """
    提取可转债数据
    
    获取指定时间范围内的可转债数据
    
    Args:
        start_date (str, optional): 开始日期
        end_date (str, optional): 结束日期
        columns (list, optional): 需要的列名
        realtime (bool, optional): 是否为实时数据，默认为False
    
    Returns:
        pandas.DataFrame: 可转债数据
    """
    mkts = mktData_sql()
    mktl = mktData_local()
    if realtime == True:
        print('暂时没有realtime的可转债数据，用日频数据替代')
        available_date = date.today()
        available_date = strdate_transfer(available_date)
        yes = last_workday_calculate(available_date)
        start_date = yes
        end_date = yes
    if source == 'local':
        df = mktl.cbData_withdraw_local_daily(start_date, end_date, columns)
    else:
        df = mkts.cbData_withdraw_sql_daily(start_date, end_date, columns)
    return df

# ============= 期权和期货数据处理函数 =============
def optionData_withdraw(start_date=None, end_date=None, columns=None, realtime=False):
    """
    提取期权数据
    
    获取指定时间范围内的期权数据
    
    Args:
        start_date (str, optional): 开始日期
        end_date (str, optional): 结束日期
        columns (list, optional): 需要的列名
        realtime (bool, optional): 是否为实时数据，默认为False
    
    Returns:
        pandas.DataFrame: 期权数据
    """
    mkts = mktData_sql()
    mktl = mktData_local()
    if realtime == True:
        if source == 'local':
            df = mktl.optionData_withdraw_local_realtime(columns)
        else:
            df = mkts.optionData_withdraw_sql_realtime(columns)
    else:
        if source == 'local':
            df = mktl.optionData_withdraw_local_daily(start_date, end_date, columns)
        else:
            df = mkts.optionData_withdraw_sql_daily(start_date, end_date, columns)
    return df

def futureData_withdraw(start_date=None, end_date=None, columns=None, realtime=False):
    """
    提取期货数据
    
    获取指定时间范围内的期货数据
    
    Args:
        start_date (str, optional): 开始日期
        end_date (str, optional): 结束日期
        columns (list, optional): 需要的列名
        realtime (bool, optional): 是否为实时数据，默认为False
    
    Returns:
        pandas.DataFrame: 期货数据
    """
    mkts = mktData_sql()
    mktl = mktData_local()
    if realtime == False:
        if source == 'local':
            df = mktl.futureData_withdraw_local_daily(start_date, end_date, columns)
        else:
            df = mkts.futureData_withdraw_sql_daily(start_date, end_date, columns)
    else:
        if source == 'local':
            df = mktl.futureData_withdraw_local_realtime(columns)
        else:
            df = mkts.futureData_withdraw_sql_realtime(columns)
    return df

def weight_df_standardization(df):
    """
    标准化权重数据
    
    对包含code列的DataFrame进行代码标准化处理，支持股票、期货、期权等不同类型
    
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
    df['code'] = df['code'].str.upper()
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

def weight_df_datecheck(df):
    """
    检查权重数据的日期完整性
    
    检查DataFrame中的日期是否连续，如果不连续则抛出异常
    
    Args:
        df (pandas.DataFrame): 包含valuation_date列的数据框
    """
    def check(df):
        date_list_df = df['valuation_date'].unique().tolist()
        date_list_df.sort()
        start_date = date_list_df[0]
        end_date = date_list_df[-1]
        date_list = working_days_list(start_date, end_date)
        date_difference = list(set(date_list) - set(date_list_df))
        if len(date_difference) > 0:
            print(f"{date_difference}没有holding")
            raise ValueError
    if 'portfolio_name' not in df.columns.tolist():
        check(df)
    else:
        for portfolio_name in df['portfolio_name'].unique().tolist():
            slice_df = df[df['portfolio_name'] == portfolio_name]
            check(slice_df)

def portfolio_analyse(df_holding=pd.DataFrame(), account_money=10000000, cost_stock=0.00085, cost_etf=0.0003, cost_future=0.00006, cost_option=0.01, cost_convertiblebond=0.0007, realtime=False, weight_standardize=True):
    """
    投资组合分析主函数
    
    对投资组合进行完整的收益分析，包括收益计算、风险分析等
    
    Args:
        df_holding (pandas.DataFrame, optional): 持仓数据，默认为空DataFrame
        account_money (float, optional): 账户资金，默认为10000000
        cost_stock (float, optional): 股票交易成本，默认为0.00085
        cost_etf (float, optional): ETF交易成本，默认为0.0003
        cost_future (float, optional): 期货交易成本，默认为0.00006
        cost_option (float, optional): 期权交易成本，默认为0.01
        cost_convertiblebond (float, optional): 可转债交易成本，默认为0.0007
        realtime (bool, optional): 是否为实时数据，默认为False
        weight_standardize (bool, optional): 是否标准化权重，默认为True
    
    Returns:
        tuple: (df_info, df_detail) - 投资组合汇总信息和详细数据
    """
    weight_df_datecheck(df_holding)
    date_list = df_holding['valuation_date'].unique().tolist()
    date_list.sort()
    start_date = date_list[0]
    end_date = date_list[-1]
    if weight_standardize == True:
        if not df_holding.empty:
            df_holding = weight_df_standardization(df_holding)
    df_stock = stockData_withdraw(start_date, end_date, ['close', 'pre_close'], realtime)
    if realtime == True:
        df_future = futureData_withdraw(start_date, end_date, ['close', 'pre_settle', 'multiplier'],
                                        realtime)
        df_option = optionData_withdraw(start_date, end_date, ['close', 'pre_settle', 'delta', 'delta_yes'],
                                        realtime)
    else:
        df_future = futureData_withdraw(start_date, end_date, ['settle', 'pre_settle', 'multiplier'],
                                        realtime)
        df_option = optionData_withdraw(start_date, end_date, ['settle', 'pre_settle', 'delta', 'delta_yes'],
                                        realtime)
    df_etf = etfData_withdraw(start_date, end_date, ['close', 'pre_close'], realtime)
    df_convertible_bond = cbData_withdraw(start_date, end_date, ['close', 'pre_close', 'delta', 'delta_yes'],
                                          realtime)
    df_index = indexData_withdraw(None, start_date, end_date, ['pct_chg'], realtime)
    pc = portfolio_calculation(df_stock, df_etf, df_option, df_future,
                               df_convertible_bond, df_index, account_money, cost_stock, cost_etf, cost_future,
                               cost_option, cost_convertiblebond, realtime)
    if 'portfolio_name' not in df_holding.columns.tolist():
        df_info, df_detail = pc.portfolio_calculation_main(df_holding)
    else:
        df_info, df_detail = pd.DataFrame(), pd.DataFrame()
        for portfolio_name in df_holding['portfolio_name'].unique().tolist():
            df_single_holding = df_holding[df_holding['portfolio_name'] == portfolio_name]
            single_info, single_detail = pc.portfolio_calculation_main(df_single_holding)
            single_info['portfolio_name'] = portfolio_name
            single_detail['portfolio_name'] = portfolio_name
            df_info = pd.concat([df_info, single_info])
            df_detail = pd.concat([df_detail, single_detail])
    return df_info, df_detail

# 回测标准化模块:
def backtesting_report(df_portfolio=pd.DataFrame(), outputpath=None, index_type=None, signal_name='portfolio'):
    """
    生成回测报告
    
    对投资组合数据进行回测分析并生成报告
    
    Args:
        df_portfolio (pandas.DataFrame, optional): 投资组合数据，默认为空DataFrame
        outputpath (str, optional): 输出路径，默认为None
        index_type (str, optional): 基准指数类型，默认为None
        signal_name (str, optional): 信号名称，默认为'portfolio'
    """
    df_portfolio.sort_values(by='valuation_date', inplace=True)
    start_date = df_portfolio['valuation_date'].tolist()[0]
    end_date = df_portfolio['valuation_date'].tolist()[-1]
    df_indexreturn = indexData_withdraw(index_type, start_date, end_date, ['pct_chg'])
    BTP = Back_testing_processing(df_indexreturn)
    if df_portfolio.empty:
        print('输入的portfolio不能为空')
    if outputpath == None:
        print('输入的outputpth不能为空')
    if not df_portfolio.empty and outputpath != None:
        if 'valuation_date' not in df_portfolio.columns.tolist():
            print('输入的portfolio必须为时序数据')
        else:
            # 根据是否包含中文决定index_short的赋值
            if contains_chinese(index_type):
                index_short = index_mapping(index_type, 'code')
            else:
                index_short = index_type
            BTP.back_testing_history(df_portfolio, outputpath, index_short, signal_name)

# 入库标准化模块
class sqlSaving_main:
    """
    数据库保存主类
    
    提供标准化的数据库保存功能
    """
    
    def __init__(self, config_path=None, parameter_name=None, delete=False):
        """
        初始化数据库保存对象
        
        Args:
            config_path (str, optional): 配置文件路径
            parameter_name (str, optional): 参数名称
            delete (bool, optional): 是否删除，默认为False
        """
        self.config_path = config_path
        self.parameter_name = parameter_name
        self.delete = delete
        if self.config_path == None:
            print('找不到对应配置文件')
            raise TypeError
        else:
            if self.parameter_name == None:
                print('参数名不能为空')
                raise TypeError
            else:
                self.SS = SqlSaving(self.config_path, self.parameter_name, self.delete)
    
    def df_to_sql(self, df=pd.DataFrame(), delete_name=None, delet_key=None):
        """
        将DataFrame保存到数据库
        
        Args:
            df (pandas.DataFrame, optional): 要保存的数据框，默认为空DataFrame
            delete_name (str, optional): 删除名称，默认为None
            delet_key (str, optional): 删除键，默认为None
        """
        if df.empty:
            print('输入的文件为空，无法入库')
            pass
        else:
            self.SS.process_file(df, delete_name, delet_key)

def table_manager(config_path, database_name,table_name):
    """
    删除指定的数据库表
    
    删除数据库中指定的表
    
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
        drop_query = f"DROP TABLE IF EXISTS {database_name}.{table_name};"
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







