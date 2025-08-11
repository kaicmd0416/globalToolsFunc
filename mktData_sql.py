import pandas as pd
import numpy as np
import os
import warnings
from datetime import time, datetime, timedelta, date
# 导入全局配置
from global_dic import get as glv
from utils import index_mapping, data_getting_glb, get_string_before_last_dot, optiondata_greeksprocessing
from time_utils import strdate_transfer, last_workday_calculate
# 忽略警告信息
warnings.filterwarnings("ignore")

class mktData_sql:
    """
    SQL数据库市场数据获取类
    
    提供从SQL数据库获取各种金融数据的接口，包括股票、ETF、期权、期货、可转债等
    
    支持日频数据和实时数据的获取，数据格式统一化处理，与本地文件版本保持接口一致
    """

    # ============= 指数权重数据处理函数 =============
    def index_weight_withdraw_sql(self, index_type, available_date):
        """
        从SQL数据库获取指数权重数据
        
        根据指数类型和日期，从SQL数据库中提取指定指数的成分股权重数据
        
        Args:
            index_type (str): 指数类型，如'沪深300'、'中证500'等
            available_date (str): 查询日期，格式为'YYYY-MM-DD'
            
        Returns:
            pandas.DataFrame: 指数权重数据，包含code和weight列
        """
        inputpath_index = glv('input_indexcomponent')
        # 根据是否包含中文决定short_name的赋值
        short_name = index_mapping(index_type, 'shortname')
        inputpath_index = inputpath_index + f" WHERE valuation_date='{available_date}' AND organization='{short_name}'"
        df = data_getting_glb(inputpath_index)
        return df

    # ============= 指数收益率数据处理函数 =============
    def indexData_withdraw_sql_daily(self, index_type=None, start_date=None, end_date=None, columns=list):
        """
        从SQL数据库获取指数日频数据
        
        获取指定时间范围内的指数收益率数据，支持多个指数同时查询
        
        Args:
            index_type (str, optional): 指数类型，默认为None表示获取所有指数
            start_date (str, optional): 开始日期，格式为'YYYY-MM-DD'
            end_date (str, optional): 结束日期，格式为'YYYY-MM-DD'
            columns (list, optional): 需要的列名列表，默认为空列表表示获取所有列
            
        Returns:
            pandas.DataFrame: 指数日频数据
        """
        # 根据是否包含中文决定short_name的赋值
        code = index_mapping(index_type, 'code')
        inputpath_indexreturn = glv('index_data')
        if index_type != None:
             inputpath_indexreturn = inputpath_indexreturn + f" WHERE valuation_date Between'{start_date}' AND '{end_date}' AND code='{code}'"
             df_final = data_getting_glb(inputpath_indexreturn)
             try:
                 df_final = df_final[df_final['code'] == code]
             except:
                 code_list = df_final['code'].unique().tolist()
                 print(f"输入的{code}需要在{code_list}列里面")
                 df_final = pd.DataFrame()
        else:
            inputpath_indexreturn = inputpath_indexreturn + f" WHERE valuation_date Between'{start_date}' AND '{end_date}'"
            df_final = data_getting_glb(inputpath_indexreturn)
        if not columns:
            return df_final
        else:
            try:
                df_final = df_final[['valuation_date', 'code'] + columns]
            except:
                type_list = df_final.columns.tolist()
                print(f"输入的{columns}需要在{type_list}列里面")
                df_final = pd.DataFrame()
            return df_final

    def indexData_withdraw_sql_realtime(self, index_type=None, columns=list):
        """
        从SQL数据库获取指数实时数据
        
        获取当前交易日的指数实时收益率数据
        
        Args:
            index_type (str, optional): 指数类型，默认为None表示获取所有指数
            columns (list, optional): 需要的列名列表，默认为空列表
            
        Returns:
            pandas.DataFrame: 指数实时数据
        """
        if columns != ['pct_chg']:
            print('目前指数的realtimedata只支持pct_chg')
            return pd.DataFrame()
        # 根据是否包含中文决定short_name的赋值
        date = datetime.today()
        date = strdate_transfer(date)
        code = index_mapping(index_type, 'code')
        if code == '000510.CSI':
            code = '000510.SH'
        inputpath_indexreturn = glv('input_indexreturn_realtime')
        if index_type != None:
            inputpath_indexreturn = inputpath_indexreturn + f" WHERE  type='index' AND code='{code}' "
        else:
            inputpath_indexreturn = inputpath_indexreturn + f" WHERE  type='index'"
        df = data_getting_glb(inputpath_indexreturn)
        df.replace('000510.SH', '000510.CSI', inplace=True)
        df['valuation_date'] = date
        df = df[['valuation_date', 'code', 'ret']]
        df.columns = ['valuation_date', 'code', 'pct_chg']
        return df
    
    def indexFactor_withdraw_sql(self, index_type=None, start_date=None, end_date=None):
        """
        从SQL数据库获取指数因子暴露数据
        
        获取指定时间范围内的指数因子暴露数据，用于风险分析
        
        Args:
            index_type (str, optional): 指数类型
            start_date (str, optional): 开始日期
            end_date (str, optional): 结束日期

        Returns:
            pandas.DataFrame: 因子暴露数据
        """
        inputpath_indexexposure = glv('input_index_exposure')
        short_name = index_mapping(index_type, 'shortname')
        inputpath_indexexposure = inputpath_indexexposure + f" WHERE valuation_date Between'{start_date}' AND '{end_date}'  AND organization='{short_name}'"
        df_final = data_getting_glb(inputpath_indexexposure)
        try:
            df_final = df_final.drop(columns=['organization'])
        except:
            pass
        return df_final

    # ============= 股票数据处理函数 =============
    def stockData_withdraw_sql_daily(self, start_date=None, end_date=None, columns=list):
        """
        从SQL数据库获取股票日频数据
        
        获取指定时间范围内的股票价格和收益率数据
        
        Args:
            start_date (str, optional): 开始日期，格式为'YYYY-MM-DD'
            end_date (str, optional): 结束日期，格式为'YYYY-MM-DD'
            columns (list, optional): 需要的列名列表，默认为空列表表示获取所有列
            
        Returns:
            pandas.DataFrame: 股票日频数据
        """
        inputpath_stockclose = glv('input_stockdata')
        inputpath_stockclose = inputpath_stockclose + f" WHERE valuation_date Between'{start_date}' AND '{end_date}'"
        df_final = data_getting_glb(inputpath_stockclose)
        # 确保数据按股票代码和日期排序
        df_final = df_final.sort_values(['code', 'valuation_date'])
        if not columns:
            return df_final
        else:
            try:
                df_final = df_final[['valuation_date', 'code'] + columns]
            except:
                type_list = df_final.columns.tolist()
                print(f"输入的{columns}需要在{type_list}列里面")
                df_final = pd.DataFrame()
            return df_final
    
    def stockData_withdraw_sql_realtime(self, columns=list):
        """
        从SQL数据库获取股票实时数据
        
        获取当前交易日的股票实时价格和收益率数据
        
        Args:
            columns (list, optional): 需要的列名列表，默认为空列表
            
        Returns:
            pandas.DataFrame: 股票实时数据
        """
        inputpath_stockreturn = glv('input_stockclose_realtime')
        inputpath_stockreturn = inputpath_stockreturn + f" WHERE type='stock'"
        df = data_getting_glb(inputpath_stockreturn)
        df.rename(columns={'ret': 'pct_chg'}, inplace=True)
        df = df[['valuation_date', 'code', 'close', 'pre_close', 'pct_chg']]
        df.loc[df['close'] == 0, ['close']] = df[df['close'] == 0]['pre_close'].tolist()
        df[['adjfactor_jy', 'adjfactor_wind']] = 1
        date = datetime.today()
        date = strdate_transfer(date)
        df['valuation_date'] = date
        if not columns:
            df['pct_chg'] = df['pct_chg'] / 100
            return df
        else:
            try:
                df['pct_chg'] = df['pct_chg'] / 100
                df = df[['valuation_date', 'code'] + columns]
            except:
                type_list = df.columns.tolist()
                print(f"输入的{columns}需要在{type_list}列里面")
                df = pd.DataFrame()
            return df
    
    # ============= etf数据处理函数 =============
    def etfData_withdraw_sql_daily(self, start_date=None, end_date=None, columns=list):
        """
        从SQL数据库获取ETF日频数据
        
        获取指定时间范围内的ETF价格和收益率数据
        
        Args:
            start_date (str, optional): 开始日期，格式为'YYYY-MM-DD'
            end_date (str, optional): 结束日期，格式为'YYYY-MM-DD'
            columns (list, optional): 需要的列名列表，默认为空列表表示获取所有列
            
        Returns:
            pandas.DataFrame: ETF日频数据
        """
        inputpath_etfdata = glv('input_etfdata')
        inputpath_stockclose = inputpath_etfdata  + f" WHERE valuation_date Between'{start_date}' AND '{end_date}'"
        df_final = data_getting_glb(inputpath_stockclose)
        # 确保数据按股票代码和日期排序
        df_final = df_final.sort_values(['code', 'valuation_date'])
        # 按股票代码分组，计算每个股票前一天的复权因子
        df_final['pct_chg'] = (df_final['close'] - df_final['pre_close']) / df_final['pre_close']
        if not columns:
            return df_final
        else:
            try:
                df_final = df_final[['valuation_date', 'code'] + columns]
            except:
                type_list = df_final.columns.tolist()
                print(f"输入的{columns}需要在{type_list}列里面")
                df_final = pd.DataFrame()
            return df_final
    
    def etfData_withdraw_sql_realtime(self, columns=list):
        """
        从SQL数据库获取ETF实时数据
        
        获取当前交易日的ETF实时价格和收益率数据
        
        Args:
            columns (list, optional): 需要的列名列表，默认为空列表
            
        Returns:
            pandas.DataFrame: ETF实时数据
        """
        inputpath_etfdata = glv('input_etfdata_realtime')
        inputpath_etfdata = inputpath_etfdata+ f" WHERE type='etf'"
        df = data_getting_glb(inputpath_etfdata)
        df.rename(columns={'ret': 'pct_chg'}, inplace=True)
        df = df[['valuation_date', 'code', 'close', 'pre_close', 'pct_chg']]
        df['pct_chg'] = (df['close'] - df['pre_close']) / df['pre_close']
        df[['adjfactor', 'adjfactor_yes']] = 1
        date = datetime.today()
        date = strdate_transfer(date)
        df['valuation_date'] = date
        if not columns:
            return df
        else:
            try:
                df = df[['valuation_date', 'code'] + columns]
            except:
                type_list = df.columns.tolist()
                print(f"输入的{columns}需要在{type_list}列里面")
                df = pd.DataFrame()
            return df
    
    # ============= 转债数据处理函数 =============
    def cbData_withdraw_sql_daily(self, start_date=None, end_date=None, columns=list):
        """
        从SQL数据库获取可转债日频数据
        
        获取指定时间范围内的可转债价格、收益率和Delta数据
        
        Args:
            start_date (str, optional): 开始日期，格式为'YYYY-MM-DD'
            end_date (str, optional): 结束日期，格式为'YYYY-MM-DD'
            columns (list, optional): 需要的列名列表，默认为空列表表示获取所有列
            
        Returns:
            pandas.DataFrame: 可转债日频数据
        """
        inputpath_cbdata = glv('input_cbdata')
        start_date2 = last_workday_calculate(start_date)
        inputpath_cbdata = inputpath_cbdata  + f" WHERE valuation_date Between'{start_date2}' AND '{end_date}'"
        df_final = data_getting_glb(inputpath_cbdata)
        # 确保数据按股票代码和日期排序
        df_final = df_final.sort_values(['code', 'valuation_date'])
        # 按股票代码分组，计算每个股票前一天的复权因子
        df_final['delta_yes'] = df_final.groupby('code')['delta'].shift(1)
        df_final = df_final[~(df_final['valuation_date'] == start_date2)]
        if not columns:
            return df_final
        else:
            try:
                df_final = df_final[['valuation_date', 'code'] + columns]
            except:
                type_list = df_final.columns.tolist()
                print(f"输入的{columns}需要在{type_list}列里面")
                df_final = pd.DataFrame()
            return df_final
    
    # ============= 期权数据处理函数 =============
    def optionData_withdraw_sql_daily(self, start_date=None, end_date=None, columns=list):
        """
        从SQL数据库获取期权日频数据
        
        获取指定时间范围内的期权价格、收益率和Greeks数据
        
        Args:
            start_date (str, optional): 开始日期，格式为'YYYY-MM-DD'
            end_date (str, optional): 结束日期，格式为'YYYY-MM-DD'
            columns (list, optional): 需要的列名列表，默认为空列表表示获取所有列
            
        Returns:
            pandas.DataFrame: 期权日频数据
        """
        inputpath_optiondata = glv('input_optiondata')
        start_date2 = last_workday_calculate(start_date)
        inputpath_optiondata = inputpath_optiondata  + f" WHERE valuation_date Between'{start_date2}' AND '{end_date}'"
        df_final = data_getting_glb(inputpath_optiondata)
        df_final = optiondata_greeksprocessing(df_final)
        # 确保数据按股票代码和日期排序
        df_final = df_final.sort_values(['code', 'valuation_date'])
        # 按股票代码分组，计算每个股票前一天的复权因子
        df_final['delta_yes'] = df_final.groupby('code')['delta'].shift(1)
        df_final = df_final[~(df_final['valuation_date'] == start_date2)]
        if not columns:
            return df_final
        else:
            try:
                df_final = df_final[['valuation_date', 'code'] + columns]
            except:
                type_list = df_final.columns.tolist()
                print(f"输入的{columns}需要在{type_list}列里面")
                df_final = pd.DataFrame()
            return df_final
    
    def optionData_withdraw_sql_realtime(self, columns=list):
        """
        从SQL数据库获取期权实时数据
        
        获取当前交易日的期权实时价格和Greeks数据
        
        Args:
            columns (list, optional): 需要的列名列表，默认为空列表
            
        Returns:
            pandas.DataFrame: 期权实时数据
        """
        inputpath_optiondata = glv('input_optiondata')
        date = datetime.today()
        date = strdate_transfer(date)
        date2 = last_workday_calculate(date)
        inputpath_optiondata = inputpath_optiondata + f" WHERE valuation_date ='{date2}'"
        df2 = data_getting_glb(inputpath_optiondata)
        df2 = optiondata_greeksprocessing(df2)
        df2 = df2[['code', 'delta']]
        df2.columns = ['code', 'delta_yes']
        inputpath_optiondata_realtime = glv('input_optiondata_realtime')
        inputpath_optiondata_realtime = inputpath_optiondata_realtime+ f" WHERE type='option'"
        df = data_getting_glb(inputpath_optiondata_realtime)
        df['code'] = df['code'].apply(lambda x: get_string_before_last_dot(x))
        df.rename(columns={'ret': 'pct_chg'}, inplace=True)
        df['valuation_date'] = date
        df = df.merge(df2, on='code', how='left')
        if not columns:
            return df
        else:
            try:
                df = df[['valuation_date', 'code'] + columns]
            except:
                type_list = df.columns.tolist()
                print(f"输入的{columns}需要在{type_list}列里面")
                df = pd.DataFrame()
            return df

# ============= 期货数据处理函数 =============
    def futureData_withdraw_sql_daily(self, start_date=None, end_date=None, columns=list):
        """
        从SQL数据库获取期货日频数据
        
        获取指定时间范围内的期货价格和收益率数据
        
        Args:
            start_date (str, optional): 开始日期，格式为'YYYY-MM-DD'
            end_date (str, optional): 结束日期，格式为'YYYY-MM-DD'
            columns (list, optional): 需要的列名列表，默认为空列表表示获取所有列
            
        Returns:
            pandas.DataFrame: 期货日频数据
        """
        inputpath_futuredata = glv('input_futuredata')
        inputpath_futuredata = inputpath_futuredata  + f" WHERE valuation_date Between'{start_date}' AND '{end_date}'"
        df_final = data_getting_glb(inputpath_futuredata)
        # 按股票代码分组，计算每个股票前一天的复权因子
        df_final['code'] = df_final['code'].apply(lambda x: get_string_before_last_dot(x))
        if not columns:
            return df_final
        else:
            try:
                df_final = df_final[['valuation_date', 'code'] + columns]
            except:
                type_list = df_final.columns.tolist()
                print(f"输入的{columns}需要在{type_list}列里面")
                df_final = pd.DataFrame()
            return df_final
    
    def futureData_withdraw_sql_realtime(self, columns=list):
        """
        从SQL数据库获取期货实时数据
        
        获取当前交易日的期货实时价格和收益率数据
        
        Args:
            columns (list, optional): 需要的列名列表，默认为空列表
            
        Returns:
            pandas.DataFrame: 期货实时数据
        """
        inputpath_futuredata = glv('input_futuredata_realtime')
        inputpath_futuredata = inputpath_futuredata+ f" WHERE type='future'"
        df = data_getting_glb(inputpath_futuredata)
        df.rename(columns={'ret': 'pct_chg'}, inplace=True)
        df['code'] = df['code'].apply(lambda x: get_string_before_last_dot(x))
        date = datetime.today()
        date = strdate_transfer(date)
        df['valuation_date'] = date
        if not columns:
            return df
        else:
            try:
                df = df[['valuation_date', 'code'] + columns]
            except:
                type_list = df.columns.tolist()
                print(f"输入的{columns}需要在{type_list}列里面")
                df = pd.DataFrame()
            return df


