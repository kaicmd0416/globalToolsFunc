import pandas as pd
import numpy as np
import os
import warnings
import json
import pymysql
import subprocess
from datetime import time, datetime, timedelta, date
from scipy.io import loadmat
import re
from dbutils.pooled_db import PooledDB
# 导入全局配置
from global_dic import get as glv
from utils import index_mapping, data_getting_glb,file_withdraw2
from time_utils import intdate_transfer,working_days_list,strdate_transfer,last_workday_calculate
# 忽略警告信息
warnings.filterwarnings("ignore")
class mktData_local:
    # ============= 指数权重数据处理函数 =============
    def index_weight_withdraw_local(self,index_type, available_date):
        inputpath_index = glv('input_indexcomponent')
        available_date2 = intdate_transfer(available_date)
        # 根据是否包含中文决定short_name的赋值
        short_name = index_mapping(index_type, 'shortname')
        inputpath_index = os.path.join(inputpath_index, short_name)
        df = file_withdraw2(inputpath_index, available_date2)
        return df

    # ============= 指数数据处理函数 =============
    def indexData_withdraw_local_daily(self,index_type=None, start_date=None, end_date=None, columns=list):
        # 根据是否包含中文决定short_name的赋值
        code = index_mapping(index_type, 'code')
        df_final = pd.DataFrame()
        day_list = working_days_list(start_date, end_date)
        inputpath_indexreturn = glv('index_data')
        for date in day_list:
            date = intdate_transfer(date)
            df = file_withdraw2(inputpath_indexreturn, date)
            df_final = pd.concat([df_final, df])
        try:
            df_final = df_final[df_final['code'] == code]
        except:
            code_list=df_final['code'].unique().tolist()
            print(f"输入的{code}需要在{code_list}列里面")
            df_final=pd.DataFrame()
        
        # 如果columns为空list，返回所有列；否则只返回指定列
        if not columns:
            return df_final
        else:
            try:
                df_final = df_final[['valuation_date'] + columns]
            except:
                type_list=df_final.columns.tolist()
                print(f"输入的{columns}需要在{type_list}列里面")
                df_final = pd.DataFrame()
            return df_final

    def indexData_withdraw_local_realtime(self,index_type=None,columns=list):
        if columns!=['pct_chg']:
            print('目前指数的realtimedata只支持pct_chg')
            return pd.DataFrame()
        # 根据是否包含中文决定short_name的赋值
        date=datetime.today()
        date=strdate_transfer(date)
        code = index_mapping(index_type, 'code')
        if code == '000510.CSI':
            code = '000510.SH'
        inputpath_indexreturn = glv('input_indexreturn_realtime')
        df = data_getting_glb(inputpath_indexreturn, sheet_name='indexreturn')
        df['valuation_date']=date
        try:
            df = df[['valuation_date', code]]
            df.columns = ['valuation_date', 'pct_chg']
        except:
            code_list = df.columns.tolist()
            print(f"输入的{code}需要在{code_list}列里面")
        return df

    def indexFactor_withdraw_local(self,index_type=None,start_date=None,end_date=None):
        """
        提取指数因子暴露数据

        Args:
            index_type (str): 指数类型
            available_date (str): 日期

        Returns:
            pandas.DataFrame: 因子暴露数据
        """
        df_final = pd.DataFrame()
        day_list = working_days_list(start_date, end_date)
        inputpath_indexexposure = glv('input_index_exposure')
        short_name = index_mapping(index_type,'shortname')
        for date in day_list:
            date = intdate_transfer(date)
            inputpath_indexexposure_daily=os.path.join(inputpath_indexexposure,short_name)
            df = file_withdraw2(inputpath_indexexposure_daily, date)
            df_final = pd.concat([df_final, df])
        try:
            df_final = df_final.drop(columns=['organization'])
        except:
            pass
        return df_final
    # ============= 股票数据处理函数 =============
    def stockData_withdraw_local_daily(self,start_date=None,end_date=None,columns=list):
        inputpath_stockclose = glv('input_stockdata')
        day_list=working_days_list(start_date,end_date)
        df_final=pd.DataFrame()
        for day in day_list:
            yes = last_workday_calculate(day)
            day2 = intdate_transfer(day)
            yes2 = intdate_transfer(yes)
            df1=file_withdraw2(inputpath_stockclose, day2)
            df2 = file_withdraw2(inputpath_stockclose, yes2)
            df2 = df2[['code', 'adjfactor_jy', 'adjfactor_wind']]
            df2.columns = ['code', 'adjfactor_jy_yes', 'adjfactor_wind_yes']
            df1 = df1.merge(df2, on='code', how='left')
            df_final=pd.concat([df1,df2])
        
        # 如果columns为空list，返回所有列；否则只返回指定列
        if not columns:
            return df_final
        else:
            try:
                df_final = df_final[['valuation_date','code'] + columns]
            except:
                type_list=df_final.columns.tolist()
                print(f"输入的{columns}需要在{type_list}列里面")
                df_final = pd.DataFrame()
            return df_final

    def stockData_withdraw_local_realtime(self,columns=list):
        inputpath_stockreturn = glv('input_stockclose_realtime')
        df = data_getting_glb(inputpath_stockreturn, 'stockprice')
        df.rename(columns={'代码':'code','简称':'chi_name','return':'pct_change','日期':'valuation_date','时间':'update_time'},inplace=True)
        df[['adjfactor_jy','adjfactor_wind','adjfactor_jy_yes','adjfactor_wind_yes']]=1
        date=datetime.today()
        date=strdate_transfer(date)
        df['valuation_date']=date
        
        # 如果columns为空list，返回所有列；否则只返回指定列
        if not columns:
            df['pct_chg']=df['pct_chg']/100
            return df
        else:
            try:
                df = df[['valuation_date','code'] + columns]
                df['pct_chg'] = df['pct_chg'] / 100
            except:
                type_list=df.columns.tolist()
                print(f"输入的{columns}需要在{type_list}列里面")
                df = pd.DataFrame()
            return df

    # ============= etf数据处理函数 =============
    def etfData_withdraw_local_daily(self, start_date=None, end_date=None, columns=list):
        inputpath_stockclose = glv('input_etfdata')
        day_list = working_days_list(start_date, end_date)
        df_final = pd.DataFrame()
        for day in day_list:
            yes = last_workday_calculate(day)
            day2 = intdate_transfer(day)
            yes2 = intdate_transfer(yes)
            df1 = file_withdraw2(inputpath_stockclose, day2)
            df2 = file_withdraw2(inputpath_stockclose, yes2)
            df2 = df2[['code', 'adjfactor']]
            df2.columns = ['code', 'adjfactor_yes']
            df1 = df1.merge(df2, on='code', how='left')
            df_final = pd.concat([df1, df2])
        df_final['pct_chg'] = (df_final['close'] - df_final['pre_close']) / df_final['pre_close']
        # 如果columns为空list，返回所有列；否则只返回指定列
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
    def etfData_withdraw_local_realtime(self, columns=list):
        inputpath_etfdata = glv('input_etfdata_realtime')
        df = data_getting_glb(inputpath_etfdata, 'stockprice')
        df.rename(columns={'代码': 'code', '简称': 'chi_name', '现价': 'close','前收':'pre_close', '日期': 'valuation_date',
                           '时间': 'update_time'}, inplace=True)
        df[['adjfactor','adjfactor_yes']] = 1
        date = datetime.today()
        date = strdate_transfer(date)
        df['valuation_date'] = date

        # 如果columns为空list，返回所有列；否则只返回指定列
        if not columns:
            df['pct_chg'] =(df['close'] - df['pre_close']) / df['pre_close']
            return df
        else:
            try:
                df = df[['valuation_date', 'code'] + columns]
                df['pct_chg'] = (df['close'] - df['pre_close']) / df['pre_close']
            except:
                type_list = df.columns.tolist()
                print(f"输入的{columns}需要在{type_list}列里面")
                df = pd.DataFrame()
            return df