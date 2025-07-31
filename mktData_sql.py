import pandas as pd
import numpy as np
import os
import warnings
from datetime import time, datetime, timedelta, date
# 导入全局配置
from global_dic import get as glv
from utils import index_mapping, data_getting_glb,get_string_before_last_dot,optiondata_greeksprocessing
from time_utils import strdate_transfer,last_workday_calculate
# 忽略警告信息
warnings.filterwarnings("ignore")
class mktData_sql:

    # ============= 指数权重数据处理函数 =============
    def index_weight_withdraw_sql(self,index_type, available_date):

        inputpath_index = glv('input_indexcomponent')
        # 根据是否包含中文决定short_name的赋值
        short_name = index_mapping(index_type, 'shortname')
        inputpath_index = inputpath_index + f" WHERE valuation_date='{available_date}' AND organization='{short_name}'"
        df = data_getting_glb(inputpath_index)
        return df

    # ============= 指数收益率数据处理函数 =============
    def indexData_withdraw_sql_daily(self,index_type=None, start_date=None, end_date=None, columns=list):
        # 根据是否包含中文决定short_name的赋值
        code = index_mapping(index_type, 'code')
        inputpath_indexreturn = glv('index_data')
        inputpath_indexreturn = inputpath_indexreturn + f" WHERE valuation_date Between'{start_date}' AND '{end_date}' AND code='{code}'"
        df_final = data_getting_glb(inputpath_indexreturn)
        try:
            df_final = df_final[df_final['code'] == code]
        except:
            code_list = df_final['code'].unique().tolist()
            print(f"输入的{code}需要在{code_list}列里面")
            df_final = pd.DataFrame()
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

    def indexData_withdraw_sql_realtime(self,index_type=None,columns=list):
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
        inputpath_indexreturn = inputpath_indexreturn + f" WHERE  type='index' AND code='{code}' "
        df = data_getting_glb(inputpath_indexreturn)
        df['valuation_date']=date
        df = df[['valuation_date', 'ret']]
        df.columns = ['valuation_date', 'pct_chg']
        return df
    def indexFactor_withdraw_sql(self,index_type=None,start_date=None,end_date=None):
        """
        提取指数因子暴露数据

        Args:
            index_type (str): 指数类型
            available_date (str): 日期

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
    def stockData_withdraw_sql_daily(self,start_date=None,end_date=None,columns=list):
        inputpath_stockclose = glv('input_stockdata')
        start_date2=last_workday_calculate(start_date)
        inputpath_stockclose = inputpath_stockclose + f" WHERE valuation_date Between'{start_date2}' AND '{end_date}'"
        df_final = data_getting_glb(inputpath_stockclose)
        # 确保数据按股票代码和日期排序
        df_final = df_final.sort_values(['code', 'valuation_date'])
        # 按股票代码分组，计算每个股票前一天的复权因子
        df_final['adjfactor_jy_yes'] = df_final.groupby('code')['adjfactor_jy'].shift(1)
        df_final['adjfactor_wind_yes'] = df_final.groupby('code')['adjfactor_wind'].shift(1)
        df_final=df_final[~(df_final['valuation_date']==start_date2)]
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
    def stockData_withdraw_sql_realtime(self,columns=list):
        inputpath_stockreturn = glv('input_stockclose_realtime')
        inputpath_stockreturn = inputpath_stockreturn + f" WHERE type='stock'"
        df = data_getting_glb(inputpath_stockreturn)
        df.rename(columns={'ret':'pct_chg'},inplace=True)
        df=df[['valuation_date','code','close','pre_close','pct_chg']]
        df[['adjfactor_jy','adjfactor_wind','adjfactor_jy_yes','adjfactor_wind_yes']]=1
        date=datetime.today()
        date=strdate_transfer(date)
        df['valuation_date']=date
        if not columns:
            df['pct_chg'] = df['pct_chg'] / 100
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
    def etfData_withdraw_sql_daily(self,start_date=None,end_date=None,columns=list):
        inputpath_etfdata = glv('input_etfdata')
        start_date2=last_workday_calculate(start_date)
        inputpath_stockclose = inputpath_etfdata  + f" WHERE valuation_date Between'{start_date2}' AND '{end_date}'"
        df_final = data_getting_glb(inputpath_stockclose)
        # 确保数据按股票代码和日期排序
        df_final = df_final.sort_values(['code', 'valuation_date'])
        # 按股票代码分组，计算每个股票前一天的复权因子
        df_final['adjfactor_yes'] = df_final.groupby('code')['adjfactor'].shift(1)
        df_final=df_final[~(df_final['valuation_date']==start_date2)]
        df_final['pct_chg'] = (df_final['close'] - df_final['pre_close']) / df_final['pre_close']
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
    def etfData_withdraw_sql_realtime(self,columns=list):
        inputpath_etfdata = glv('input_etfdata_realtime')
        inputpath_etfdata = inputpath_etfdata+ f" WHERE type='etf'"
        df = data_getting_glb( inputpath_etfdata)
        df.rename(columns={'ret':'pct_chg'},inplace=True)
        df=df[['valuation_date','code','close','pre_close','pct_chg']]
        df['pct_chg'] = (df['close'] - df['pre_close']) / df['pre_close']
        df[['adjfactor','adjfactor_yes']]=1
        date=datetime.today()
        date=strdate_transfer(date)
        df['valuation_date']=date
        if not columns:
            return df
        else:
            try:
                df = df[['valuation_date','code'] + columns]
            except:
                type_list=df.columns.tolist()
                print(f"输入的{columns}需要在{type_list}列里面")
                df = pd.DataFrame()
            return df
    # ============= 转债数据处理函数 =============
    def cbData_withdraw_sql_daily(self,start_date=None,end_date=None,columns=list):
        inputpath_cbdata = glv('input_cbdata')
        start_date2=last_workday_calculate(start_date)
        inputpath_cbdata = inputpath_cbdata  + f" WHERE valuation_date Between'{start_date2}' AND '{end_date}'"
        df_final = data_getting_glb(inputpath_cbdata)
        # 确保数据按股票代码和日期排序
        df_final = df_final.sort_values(['code', 'valuation_date'])
        # 按股票代码分组，计算每个股票前一天的复权因子
        df_final['delta_yes'] = df_final.groupby('code')['delta_yes'].shift(1)
        df_final=df_final[~(df_final['valuation_date']==start_date2)]
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
    # ============= 期权数据处理函数 =============
    def optionData_withdraw_sql_daily(self,start_date=None,end_date=None,columns=list):
        inputpath_optiondata = glv('input_optiondata')
        start_date2=last_workday_calculate(start_date)
        inputpath_optiondata = inputpath_optiondata  + f" WHERE valuation_date Between'{start_date2}' AND '{end_date}'"
        df_final = data_getting_glb(inputpath_optiondata)
        df_final=optiondata_greeksprocessing(df_final)
        # 确保数据按股票代码和日期排序
        df_final = df_final.sort_values(['code', 'valuation_date'])
        # 按股票代码分组，计算每个股票前一天的复权因子
        df_final['delta_yes'] = df_final.groupby('code')['delta'].shift(1)
        df_final=df_final[~(df_final['valuation_date']==start_date2)]
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
    def optionData_withdraw_sql_realtime(self,columns=list):
        inputpath_optiondata = glv('input_optiondata')
        date = datetime.today()
        date = strdate_transfer(date)
        date2 = last_workday_calculate(date)
        inputpath_optiondata = inputpath_optiondata + f" WHERE valuation_date ='{date2}'"
        df2=data_getting_glb(inputpath_optiondata)
        df2 = optiondata_greeksprocessing(df2)
        df2=df2[['code','delta']]
        df2.columns=['code','delta_yes']
        inputpath_optiondata_realtime = glv('input_optiondata_realtime')
        inputpath_optiondata_realtime = inputpath_optiondata_realtime+ f" WHERE type='option'"
        df = data_getting_glb(inputpath_optiondata_realtime)
        df['code']=df['code'].apply(lambda x: get_string_before_last_dot(x))
        df.rename(columns={'ret': 'pct_chg'}, inplace=True)
        df['valuation_date']=date
        df=df.merge(df2,on='code',how='left')
        if not columns:
            return df
        else:
            try:
                df = df[['valuation_date','code'] + columns]
            except:
                type_list=df.columns.tolist()
                print(f"输入的{columns}需要在{type_list}列里面")
                df = pd.DataFrame()
            return df
# ============= 期货数据处理函数 =============
    def futureData_withdraw_sql_daily(self,start_date=None,end_date=None,columns=list):
        inputpath_futuredata = glv('input_futuredata')
        inputpath_futuredata= inputpath_futuredata  + f" WHERE valuation_date Between'{start_date}' AND '{end_date}'"
        df_final = data_getting_glb(inputpath_futuredata)
        # 按股票代码分组，计算每个股票前一天的复权因子
        df_final['code'] = df_final['code'].apply(lambda x: get_string_before_last_dot(x))
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
    def futureData_withdraw_sql_realtime(self,columns=list):
        inputpath_futuredata = glv('input_futuredata_realtime')
        inputpath_futuredata = inputpath_futuredata+ f" WHERE type='future'"
        df = data_getting_glb(inputpath_futuredata)
        df.rename(columns={'ret':'pct_chg'},inplace=True)
        df['code'] = df['code'].apply(lambda x: get_string_before_last_dot(x))
        date=datetime.today()
        date=strdate_transfer(date)
        df['valuation_date']=date
        if not columns:
            return df
        else:
            try:
                df = df[['valuation_date','code'] + columns]
            except:
                type_list=df.columns.tolist()
                print(f"输入的{columns}需要在{type_list}列里面")
                df = pd.DataFrame()
            return df


