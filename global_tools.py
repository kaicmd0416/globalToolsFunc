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
# 导入全局配置
from global_dic import get as glv
from portfolio_calculation import portfolio_calculation
from backtesting_tools import Back_testing_processing
from sql_saving import SqlSaving, DatabaseWriter
from sqlalchemy import text
# 忽略警告信息
warnings.filterwarnings("ignore")

# 全局变量
global df_date, source
def source_getting():
    """
    获取数据源配置
    
    Returns:
        str: 数据源模式（'local' 或 'sql'）
    """
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(current_dir,  'tools_path_config.json')
        with open(config_path, 'r', encoding='utf-8') as f:
            config_data = json.load(f)
        source = config_data['components']['data_source']['mode']
    except Exception as e:
        print(f"获取配置出错: {str(e)}")
        source = 'local'
    return source

def get_db_connection(config_path=None,use_database2=False):
    """
    获取数据库连接
    
    Args:
        use_database2 (bool, optional): 是否使用第二个数据库。默认为False。
    
    Returns:
        pymysql.connections.Connection: 数据库连接对象
    """
    if source == 'local':
        return None
        
    try:
        if config_path==None:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            config_path = os.path.join(current_dir, 'tools_path_config.json')
        else:
            config_path=config_path
        
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        # 选择数据库配置
        db_key = 'database2' if use_database2 else 'database1'
        db_config = config['components']['database'][db_key]
        
        connection = pymysql.connect(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database'],
            charset=db_config['charset']
        )
        return connection
    except Exception as e:
        print(f"数据库连接失败: {str(e)}")
        return None

# 初始化全局变量
source = source_getting()

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
def readcsv(filepath, dtype=None, index_col=None):
    """
    读取CSV文件，支持多种编码格式
    
    Args:
        filepath (str): CSV文件路径
        dtype (dict, optional): 指定列的数据类型
        index_col (int, optional): 指定索引列
    
    Returns:
        pandas.DataFrame: 读取的数据框
    """
    for en in ['gbk', 'utf-8', 'ANSI', 'utf_8_sig']:
        try:
            df = pd.read_csv(filepath, encoding=en, dtype=dtype, index_col=index_col)
            for col in df.select_dtypes(include=['object']).columns:
                df[col] = df[col].str.strip() if df[col].dtype == 'object' else df[col]
            return df
        except Exception as e:
            continue
    return pd.DataFrame()

def move_specific_files(old_path, new_path, files_to_move=None):
    """
    移动特定文件
    
    Args:
        old_path (str): 源目录
        new_path (str): 目标目录
        files_to_move (list, optional): 要移动的文件列表
    """
    if files_to_move is None:
        filelist = os.listdir(old_path)
    else:
        filelist = files_to_move

    for file in filelist:
        src = os.path.join(old_path, file)
        if not os.path.exists(src):
            print(f"文件不存在: {src}")
            continue
        dst = os.path.join(new_path, file)
        shutil.copy(src, dst)

def move_specific_files2(old_path, new_path):
    """
    复制整个目录
    
    Args:
        old_path (str): 源目录
        new_path (str): 目标目录
    """
    shutil.copytree(old_path, new_path, dirs_exist_ok=True)

def rr_score_processing(df_score):
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

# ============= 文件操作函数 =============

def chunks(lst, n):
    """
    等分列表
    
    Args:
        lst (list): 要等分的列表
        n (int): 等分数量
    
    Returns:
        list: 等分后的列表
    """
    return [lst[i::n] for i in range(n)]

def file_withdraw(inputpath, available_date):
    """
    提取指定日期的文件
    
    Args:
        inputpath (str): 输入路径
        available_date (str): 日期
    
    Returns:
        str: 文件路径
    """
    input_list = os.listdir(inputpath)
    try:
        file_name = [file for file in input_list if available_date in file][0]
    except:
        print(f'找不到日期 {available_date} 对应的文件: {inputpath}')
        file_name = None
    
    if file_name is not None:
        return os.path.join(inputpath, file_name)
    return None
def file_withdraw2(inputpath, available_date):
    """
    提取指定日期的文件

    Args:
        inputpath (str): 输入路径
        available_date (str): 日期

    Returns:
        str: 文件路径
    """
    input_list = os.listdir(inputpath)
    try:
        file_name = [file for file in input_list if available_date in file][0]
    except:
        print(f'找不到日期 {available_date} 对应的文件: {inputpath}')
        file_name = None
    if file_name is not None:
        inputpath= os.path.join(inputpath, file_name)
        df=data_reader(inputpath)
    else:
        df=pd.DataFrame()
    return df

def folder_creator(inputpath):
    """
    创建文件夹
    
    Args:
        inputpath (str): 文件夹路径
    """
    try:
        os.mkdir(inputpath)
    except:
        pass

def folder_creator2(path):
    """
    创建多级目录
    
    Args:
        path (str): 目录路径
    """
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)

def folder_creator3(file_path):
    """
    创建文件的路径
    
    Args:
        file_path (str): 文件路径
    """
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)

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

# ============= 数据读取函数 =============

def data_reader(filepath, dtype=None, index_col=None,sheet_name=None):
        """
        读取数据文件，支持CSV、Excel和MAT格式
        
        Args:
            filepath (str): 文件路径
            dtype (dict, optional): 数据类型
            index_col (int, optional): 索引列
        
        Returns:
            pandas.DataFrame: 读取的数据框
        """
        if filepath==None:
            print(f"文件不存在: {filepath}")
            return pd.DataFrame()
            
        file_extension = os.path.splitext(filepath)[1].lower()
        
        if file_extension == '.csv':
            # 处理CSV文件
            for en in ['gbk', 'utf-8', 'ANSI', 'utf_8_sig']:
                try:
                    df = pd.read_csv(filepath, encoding=en, dtype=dtype, index_col=index_col)
                    for col in df.select_dtypes(include=['object']).columns:
                        df[col] = df[col].str.strip() if df[col].dtype == 'object' else df[col]
                    return df
                except Exception:
                    continue
            return pd.DataFrame()
            
        elif file_extension == '.xlsx':
            # 处理Excel文件
            try:
                if sheet_name!=None:
                     df = pd.read_excel(filepath, dtype=dtype, index_col=index_col,sheet_name=sheet_name)
                else:
                    df = pd.read_excel(filepath, dtype=dtype, index_col=index_col)
                for col in df.select_dtypes(include=['object']).columns:
                    df[col] = df[col].str.strip() if df[col].dtype == 'object' else df[col]
                return df
            except Exception:
                return pd.DataFrame()

        elif file_extension == '.mat':
            # 处理MAT文件
            try:
                mat_data = loadmat(filepath)
                # 获取第一个非系统变量
                data_key = None
                for key in mat_data.keys():
                    if not key.startswith('__'):
                        data_key = key
                        break
                if data_key is None:
                    return pd.DataFrame()
                df = pd.DataFrame(mat_data[data_key])
                return df
            except Exception:
                return pd.DataFrame()
                
        else:
            return pd.DataFrame()

def data_getting(path,config_path=None,sheet_name=None):
    """
    获取数据
    
    Args:
        path (str): 数据路径或SQL查询
    
    Returns:
        pandas.DataFrame: 获取的数据
    """
    df = pd.DataFrame()
    if source == 'local':
        df = data_reader(path,sheet_name=sheet_name)
    else:
        try:
            # 首先尝试主数据库
            conn = get_db_connection(config_path)
            if conn is None:
                conn = get_db_connection(config_path,use_database2=True)
            
            if conn is not None:
                try:
                    df = pd.read_sql(path, con=conn)
                    conn.close()
                    
                    # 如果主数据库没有数据，尝试第二个数据库
                    if df.empty:
                        conn2 = get_db_connection(config_path,use_database2=True)
                        if conn2 is not None:
                            df = pd.read_sql(path, con=conn2)
                            conn2.close()
                except Exception:
                    # 如果主数据库查询失败，尝试备用数据库
                    conn2 = get_db_connection(config_path,use_database2=True)
                    if conn2 is not None:
                        try:
                            df = pd.read_sql(path, con=conn2)
                            conn2.close()
                        except Exception:
                            pass
            
            # 处理数据
            if not df.empty:
                for col in df.select_dtypes(include=['object']).columns:
                    df[col] = df[col].astype(str).str.strip()
                
        except Exception as e:
            if df.empty:
                print(f"数据获取失败: {str(e)}")
            
    if df.empty:
        print(f"未找到数据: {path}")
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
    elif type == 'old':
        inputpath = glv('stock_universe_old')
    df_universe = data_getting(inputpath)
    return df_universe

# ============= 日期处理函数 =============

def Chinese_valuation_date():
    """
    获取中国交易日期数据
    
    Returns:
        pandas.DataFrame: 交易日期数据
    """
    try:
        inputpath = glv('valuation_date')
        df_date = data_getting(inputpath)
        if df_date.empty:
            return pd.DataFrame(columns=['valuation_date'])
            
        if 'valuation_date' in df_date.columns:
            df_date['valuation_date'] = df_date['valuation_date'].str.strip()
            return df_date
        else:
            return pd.DataFrame(columns=['valuation_date'])
    except Exception as e:
        return pd.DataFrame(columns=['valuation_date'])

# 初始化全局变量
df_date = Chinese_valuation_date()
def next_workday():
    """
    获取下一个工作日
    
    Returns:
        str: 下一个工作日
    """
    today = date.today()
    today = today.strftime('%Y-%m-%d')
    try:
        if df_date.empty:
            return today
        index_today = df_date[df_date['valuation_date'] == today].index.tolist()[0]
        index_tommorow = index_today + 1
        tommorow = df_date.iloc[index_tommorow].tolist()[0]
    except:
        if df_date.empty:
            return today
        index_today = df_date[df_date['valuation_date'] >= today].index.tolist()[0]
        tommorow = df_date.iloc[index_today].tolist()[0]
    return tommorow

def last_workday():
    """
    获取上一个工作日
    
    Returns:
        str: 上一个工作日
    """
    today = date.today()
    today = today.strftime('%Y-%m-%d')
    try:
        if df_date.empty:
            return today
        index_today = df_date[df_date['valuation_date'] == today].index.tolist()[0]
        index_tommorow = index_today - 1
        yesterday = df_date.iloc[index_tommorow].tolist()[0]
    except:
        if df_date.empty:
            return today
        index_today = df_date[df_date['valuation_date'] <= today].index.tolist()[-1]
        yesterday = df_date.iloc[index_today].tolist()[0]
    return yesterday

def last_workday_calculate(x):
    """
    计算指定日期的上一个工作日
    
    Args:
        x (str/datetime): 指定日期
    
    Returns:
        str: 上一个工作日
    """
    today = x
    try:
        today = today.strftime('%Y-%m-%d')
    except:
        today = today
    if df_date.empty:
        print("警告: 未找到交易日期数据")
        return today
    yesterday = df_date[df_date['valuation_date'] < today]['valuation_date'].tolist()[-1]
    return yesterday

def next_workday_calculate(x):
    """
    计算指定日期的下一个工作日
    
    Args:
        x (str/datetime): 指定日期
    
    Returns:
        str: 下一个工作日
    """
    today = x
    try:
        today = today.strftime('%Y-%m-%d')
    except:
        today = today
    if df_date.empty:
        print("警告: 未找到交易日期数据")
        return today
    try:
        index_today = df_date[df_date['valuation_date'] == today].index.tolist()[0]
        index_tommorow = index_today + 1
        tommorow = df_date.iloc[index_tommorow].tolist()[0]
    except:
        index_today = df_date[df_date['valuation_date'] >= today].index.tolist()[0]
        tommorow = df_date.iloc[index_today].tolist()[0]
    return tommorow

def last_workday_calculate2(df_score):
    """
    批量计算上一个工作日
    
    Args:
        df_score (pandas.DataFrame): 包含date列的数据框
    
    Returns:
        pandas.DataFrame: 处理后的数据框
    """
    if df_date.empty:
        print("警告: 未找到交易日期数据")
        return df_score
    df_final = pd.DataFrame()
    date_list = df_score['date'].unique().tolist()
    for date in date_list:
        slice_df = df_score[df_score['date'] == date]
        yesterday = last_workday_calculate(date)
        slice_df['date'] = yesterday
        df_final = pd.concat([df_final, slice_df])
    return df_final

def is_workday(today):
    """
    判断是否为工作日
    
    Args:
        today (str): 日期
    
    Returns:
        bool: 是否为工作日
    """
    try:
        df_date2 = df_date[df_date['valuation_date'] == today]
    except:
        df_date2 = pd.DataFrame()
    if len(df_date2) != 1:
        return False
    else:
        return True

def working_days(df):
    """
    筛选工作日数据
    
    Args:
        df (pandas.DataFrame): 包含date列的数据框
    
    Returns:
        pandas.DataFrame: 处理后的数据框
    """
    date_list = df_date['valuation_date'].tolist()
    df = df[df['date'].isin(date_list)]
    return df

def is_workday2():
    """
    判断今天是否为工作日
    
    Returns:
        bool: 是否为工作日
    """
    today = date.today()
    today = today.strftime('%Y-%m-%d')
    try:
        df_date2 = df_date[df_date['valuation_date'] == today]
    except:
        df_date2 = pd.DataFrame()
    if len(df_date2) != 1:
        return False
    else:
        return True

def intdate_transfer(date):
    """
    日期转整数格式
    
    Args:
        date (str/datetime): 日期
    
    Returns:
        str: 整数格式日期
    """
    date = pd.to_datetime(date)
    date = date.strftime('%Y%m%d')
    return date

def strdate_transfer(date):
    """
    日期转字符串格式
    
    Args:
        date (str/datetime): 日期
    
    Returns:
        str: 字符串格式日期
    """
    date = pd.to_datetime(date)
    date = date.strftime('%Y-%m-%d')
    return date

def working_days_list(start_date, end_date):
    """
    获取工作日列表
    
    Args:
        start_date (str): 开始日期
        end_date (str): 结束日期
    
    Returns:
        list: 工作日列表
    """
    global df_date
    df_date_copy = df_date.copy()
    df_date_copy.rename(columns={'valuation_date': 'date'}, inplace=True)
    df_date_copy = df_date_copy[(df_date_copy['date'] >= '2010-12-31') &
                               (df_date_copy['date'] <= '2030-01-01')]
    df_date_copy['target_date'] = df_date_copy['date']
    df_date_copy.dropna(inplace=True)
    df_date_copy = df_date_copy[(df_date_copy['date'] >= start_date) & 
                               (df_date_copy['date'] <= end_date)]
    date_list = df_date_copy['target_date'].tolist()
    return date_list

def working_day_count(start_date, end_date):
    """
    计算工作日天数
    
    Args:
        start_date (str): 开始日期
        end_date (str): 结束日期
    
    Returns:
        int: 工作日天数
    """
    global df_date
    slice_df_date = df_date[df_date['valuation_date'] > start_date]
    slice_df_date = slice_df_date[slice_df_date['valuation_date'] <= end_date]
    total_day = len(slice_df_date)
    return total_day

def month_lastday():
    """
    获取每月最后工作日
    
    Returns:
        list: 每月最后工作日列表
    """
    df_date['year_month'] = df_date['valuation_date'].apply(lambda x: str(x)[:7])
    month_lastday = df_date.groupby('year_month')['valuation_date'].tail(1).tolist()
    return month_lastday

def last_weeks_lastday():
    """
    获取上周最后工作日
    
    Returns:
        str: 上周最后工作日
    """
    today = date.today()
    today = today.strftime('%Y-%m-%d')
    inputpath = glv('weeks_lastday')
    df_lastday = data_getting(inputpath)
    if source=='sql':
        df_lastday=df_lastday[df_lastday['type']=='weeksLastDay']
    if df_lastday.empty:
        print("警告: 未找到周最后工作日数据")
        return today
    lastday = df_lastday[df_lastday['valuation_date'] < today]['valuation_date'].tolist()[-1]
    return lastday

def last_weeks_lastday2(date):
    """
    获取指定日期的上周最后工作日
    
    Args:
        date (str): 指定日期
    
    Returns:
        str: 上周最后工作日
    """
    inputpath = glv('weeks_lastday')
    df_lastday = data_getting(inputpath)
    if source=='sql':
        df_lastday=df_lastday[df_lastday['type']=='weeksLastDay']
    if df_lastday.empty:
        print("警告: 未找到周最后工作日数据")
        return date
    date = pd.to_datetime(date)
    date = date.strftime('%Y-%m-%d')
    lastday = df_lastday[df_lastday['valuation_date'] < date]['valuation_date'].tolist()[-1]
    return lastday

def weeks_firstday(date):
    """
    获取周第一个工作日
    
    Args:
        date (str): 日期
    
    Returns:
        str: 周第一个工作日
    """
    inputpath = glv('weeks_firstday')
    df_firstday = data_getting(inputpath)
    if source=='sql':
        df_firstday=df_firstday[df_firstday['type']=='weeksFirstDay']
    if df_firstday.empty:
        print("警告: 未找到周第一个工作日数据")
        return date
    firstday = df_firstday[df_firstday['valuation_date'] < date]['valuation_date'].tolist()[-1]
    return firstday

def next_weeks_lastday2(date):
    """
    获取下周最后工作日
    
    Args:
        date (str): 日期
    
    Returns:
        str: 下周最后工作日
    """
    date = pd.to_datetime(date)
    date = date.strftime('%Y-%m-%d')
    inputpath = glv('weeks_lastday')
    df_lastday = data_getting(inputpath)
    if source=='sql':
        df_lastday=df_lastday[df_lastday['type']=='weeksLastDay']
    if df_lastday.empty:
        print("警告: 未找到周最后工作日数据")
        return date
    lastday = df_lastday[df_lastday['valuation_date'] > date]['valuation_date'].tolist()[0]
    return lastday

# ============= 指数数据处理函数 =============

def index_mapping(index_name, type='shortname'):
    """
    指数名称映射
    
    Args:
        index_name (str): 指数中文名称
        type (str, optional): 返回类型
    
    Returns:
        str: 指数代码或简称
    """
    if index_name == '上证50':
        if type == 'shortname':
            return 'sz50'
        else:
            return '000016.SH'
    elif index_name == '沪深300':
        if type == 'shortname':
            return 'hs300'
        else:
            return '000300.SH'
    elif index_name == '中证500':
        if type == 'shortname':
            return 'zz500'
        else:
            return '000905.SH'
    elif index_name == '中证1000':
        if type == 'shortname':
            return 'zz1000'
        else:
            return '000852.SH'
    elif index_name == '中证2000':
        if type == 'shortname':
            return 'zz2000'
        else:
            return '932000.CSI'
    elif index_name == '国证2000':
        if type == 'shortname':
            return 'gz2000'
        else:
            return '399303.SZ'
    elif index_name == '中证A500':
        if type == 'shortname':
            return 'zzA500'
        else:
            return '000510.SH'
    else:
        print(f'{index_name} 不存在')
        return None

def index_weight_withdraw(index_type, available_date):
    """
    提取指数权重股数据
    
    Args:
        index_type (str): 指数类型
        available_date (str): 日期
    
    Returns:
        pandas.DataFrame: 权重股数据
    """
    inputpath_index = glv('input_indexcomponent')
    available_date2 = intdate_transfer(available_date)
    short_name = index_mapping(index_type)
    if source == 'local':
        inputpath_index = os.path.join(inputpath_index, short_name)
        inputpath_index = file_withdraw(inputpath_index, available_date2)
    else:
        inputpath_index = inputpath_index + f" WHERE valuation_date='{available_date}' AND organization='{short_name}'"
    
    df = data_getting(inputpath_index)
    if df.empty:
        print(f"未找到指数 {index_type} 在 {available_date} 的权重数据")
        return pd.DataFrame()
        
    if 'code' in df.columns and 'weight' in df.columns:
        df = df[['code', 'weight']]
    else:
        print(f"数据列不完整，期望的列: code, weight，实际的列: {df.columns.tolist()}")
        return pd.DataFrame()
        
    return df

def crossSection_index_return_withdraw(index_type, available_date,realtime=False):
    """
    提取指数收益率数据
    
    Args:
        index_type (str): 指数类型
        available_date (str): 日期
    
    Returns:
        float or None: 指数收益率
    """
    short_name = index_mapping(index_type,'code')
    if realtime==False:
        available_date2 = intdate_transfer(available_date)
        inputpath_indexreturn = glv('index_data')
        if source == 'local':
            inputpath_indexreturn = file_withdraw(inputpath_indexreturn, available_date2)
        else:
            inputpath_indexreturn = inputpath_indexreturn + f" WHERE valuation_date='{available_date}' AND code='{short_name}'"
        df = data_getting(inputpath_indexreturn)
        try:
            index_return=df[df['code']==short_name]['pct_chg'].tolist()[0]
            index_return=float(index_return)
        except:
            index_return=None
    else:
        inputpath_indexreturn=glv('input_indexreturn_realtime')
        if source == 'local':
            df=data_getting(inputpath_indexreturn,sheet_name='indexreturn')
            try:
                index_return=df[short_name].tolist()[0]
                index_return=float(index_return)
            except:
                index_return=None
        else:
            inputpath_indexreturn = inputpath_indexreturn + f" WHERE  type='index' AND code='{short_name}' "
            df=data_getting(inputpath_indexreturn)
            try:
                index_return=df['ret'].tolist()[0]
                index_return=float(index_return)
            except:
                index_return=None
    return index_return



def crossSection_index_factorexposure_withdraw(index_type, available_date):
    """
    提取指数因子暴露数据
    
    Args:
        index_type (str): 指数类型
        available_date (str): 日期
    
    Returns:
        pandas.DataFrame: 因子暴露数据
    """
    available_date2 = intdate_transfer(available_date)
    inputpath_indexexposure = glv('input_index_exposure')
    short_name = index_mapping(index_type)
    if source == 'local':
        inputpath_indexexposure = os.path.join(inputpath_indexexposure, short_name)
        inputpath_indexexposure = file_withdraw(inputpath_indexexposure, available_date2)
    else:
        inputpath_indexexposure = inputpath_indexexposure + f" WHERE valuation_date='{available_date}' AND organization='{short_name}'"
    df = data_getting(inputpath_indexexposure)
    if df.empty:
        df = pd.DataFrame()
    else:
        try:
            df = df.drop(columns=['organization'])
        except:
            pass
    return df

def timeSeries_index_return_withdraw():
    """
    提取时间序列指数收益率数据
    
    Returns:
        pandas.DataFrame: 时间序列指数收益率数据
    """
    inputpath_indexreturn = glv('timeseires_indexReturn')
    df = data_getting(inputpath_indexreturn)
    if source=='sql':
        df=df[['valuation_date','code','pct_chg']]
        df=sql_to_timeseries(df)
    df['valuation_date'] = pd.to_datetime(df['valuation_date'])
    df['valuation_date'] = df['valuation_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
    return df

# ============= 证券数据处理函数 =============
def crossSection_stockdata_local_withdraw(available_date):
    yes = last_workday_calculate(available_date)
    available_date2 = intdate_transfer(available_date)
    yes2 = intdate_transfer(yes)
    inputpath_stockclose = glv('input_stockdata')
    if source == 'local':
        inputpath_stockclose1 = file_withdraw(inputpath_stockclose, available_date2)
        inputpath_stockclose2=file_withdraw(inputpath_stockclose, yes2)
    else:
        inputpath_stockclose1 = inputpath_stockclose + f" WHERE valuation_date='{available_date}'"
        inputpath_stockclose2 = inputpath_stockclose + f" WHERE valuation_date='{available_date}'"
    df = data_getting(inputpath_stockclose1)
    df2 = data_getting(inputpath_stockclose2)
    df2=df2[['code','adjfactor_jy','adjfactor_wind']]
    df2.columns=['code','adjfactor_jy_yes','adjfactor_wind_yes']
    df=df.merge(df2,on='code',how='left')
    return df
def stockdata_withdraw(available_date,realtime=False):
    if realtime == True:
        inputpath_stockreturn = glv('input_stockclose_realtime')
        if source == 'local':
            df = data_getting(inputpath_stockreturn,'stockprice')
            df=df[['代码','close','pre_close','return']]
        else:
            inputpath_stockreturn = inputpath_stockreturn + f" WHERE type='stock'"
            df = data_getting(inputpath_stockreturn)
            df=df[['code','close','pre_close','ret']]
        df.columns = ['code', 'close', 'pre_close','pct_chg']
        df['pct_chg']=df['pct_chg']/100
    else:
        df=crossSection_stockdata_local_withdraw(available_date)
    return df
def stockdata_adj_withdraw(available_date,realtime,adj_source):
    df_stock=stockdata_withdraw(available_date,realtime)
    df_adjfactor=pd.DataFrame()
    df_adjfactor['code'] = df_stock['code'].tolist()
    if realtime==True:
        df_adjfactor['adjfactor']=1
        df_adjfactor['adjfactor_yes']=1
    else:
        if adj_source=='wind':
            df_adjfactor['adjfactor']=df_stock['adjfactor_wind'].tolist()
            df_adjfactor['adjfactor_yes'] = df_stock['adjfactor_wind_yes'].tolist()
        else:
            df_adjfactor['adjfactor']=df_stock['adjfactor_jy'].tolist()
            df_adjfactor['adjfactor_yes'] = df_stock['adjfactor_jy_yes'].tolist()
    return df_adjfactor
def etfdata_withdraw(available_date,realtime=False):
    """
    提取ETF数据
    
    Args:
        available_date (str): 日期
    
    Returns:
        pandas.DataFrame: ETF数据
    """
    if realtime==False:
        available_date2 = intdate_transfer(available_date)
        inputpath_etfdata = glv('input_etfdata')
        if source == 'local':
            inputpath_etfdata = file_withdraw(inputpath_etfdata, available_date2)
        else:
            inputpath_etfdata = inputpath_etfdata + f" WHERE valuation_date='{available_date}'"
        df = data_getting(inputpath_etfdata)
        try:
           df['pct_chg'] = (df['close'] - df['pre_close']) / df['pre_close']
        except:
            pass
    else:
        inputpath_etfdata = glv('input_etfdata_realtime')
        if source == 'local':
            df = data_getting(inputpath_etfdata,'etf_info')
            df = df[['代码', '现价', '前收']]
            df['pct_chg']=(df['现价']-df['前收'])/df['前收']
        else:
            inputpath_etfdata= inputpath_etfdata + f" WHERE type='etf'"
            df = data_getting(inputpath_etfdata)
            df = df[['code', 'close', 'pre_close']]
            try:
                df['pct_chg'] = (df['close'] - df['pre_close']) / df['pre_close']
            except:
                pass
        df.columns = ['code', 'close', 'pre_close', 'pct_chg']
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
    df = data_getting(inputpath_cbdata1)
    df2 = data_getting(inputpath_cbdata2)
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
        df = data_getting(inputpath_optiondata1)
        df2 = data_getting(inputpath_optiondata2)
        df = optiondata_greeksprocessing(df)
        df2 = optiondata_greeksprocessing(df2)
    else:
        available_date=date.today()
        yes = last_workday_calculate(available_date)
        yes2 = intdate_transfer(yes)
        inputpath_optiondata = glv('input_optiondata')
        inputpath_optiondata_realtime=glv('input_optiondata_realtime')
        if source == 'local':
            df = data_getting(inputpath_optiondata_realtime, 'option_info')
            inputpath_optiondata2 = file_withdraw(inputpath_optiondata, yes2)
        else:
            inputpath_optiondata_realtime = inputpath_optiondata_realtime + f" WHERE type='option'"
            df = data_getting(inputpath_optiondata_realtime)
            inputpath_optiondata2 = inputpath_optiondata + f" WHERE valuation_date='{yes}'"
        df2 = data_getting(inputpath_optiondata2)
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
        df = data_getting(inputpath_futuredata)
    else:
        inputpath_futuredata = glv('input_futuredata_realtime')
        if source == 'local':
            df = data_getting(inputpath_futuredata, 'future_info')
        else:
            inputpath_futuredata = inputpath_futuredata + f" WHERE type='future'"
            df = data_getting(inputpath_futuredata)
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

def portfolio_analyse(start_date=None,end_date=None,df_initial=pd.DataFrame(),df_holding=pd.DataFrame(),account_money=10000000,index_type=None,cost=0.00085,realtime=False,adj_source='wind',weight_standardize=False):
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
         if df_stock.empty and df_future.empty and df_etf.empty and df_option.empty and df_convertible_bond.empty and df_adj_factor.empty:
            print(str(available_date) + '全部数据为空无法计算')
         else:
             if len(day_list) > 1:
                 if i == 0:
                     pc = portfolio_calculation(df_initial, df_holding, df_stock, df_etf, df_option, df_future,
                                                df_convertible_bond, df_adj_factor, account_money, cost)
                     df_portfolio = pc.portfolio_calculation_main()
                     i += 1
                 else:
                     df_initial = df_holding
                     pc = portfolio_calculation(df_initial, df_holding, df_stock, df_etf, df_option, df_future,
                                                df_convertible_bond, df_adj_factor, account_money, cost)
                     df_portfolio = pc.portfolio_calculation_main()
             else:
                 pc = portfolio_calculation(df_initial, df_holding, df_stock, df_etf, df_option, df_future,
                                            df_convertible_bond, df_adj_factor, account_money, cost)
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
            BTP.back_testing_history(df_portfolio, outputpath, index_type, signal_name)
#入库标准化模块
class sqlSaving_main:
    def __init__(self,config_path=None,parameter_name=None):
        self.config_path=config_path
        self.parameter_name=parameter_name
        if self.config_path == None:
            print('找不到对应配置文件')
            raise TypeError
        else:
            if self.parameter_name == None:
                print('参数名不能为空')
                raise TypeError
            else:
                self.SS = SqlSaving(self.config_path, self.parameter_name)
    def df_to_sql(self,df=pd.DataFrame()):
        if df.empty:
            print('输入的文件为空，无法入库')
            pass
        else:
            self.SS.process_file(df)
            






