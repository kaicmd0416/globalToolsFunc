import pandas as pd
import numpy as np
import os
import warnings
from datetime import time, datetime, timedelta, date
# 导入全局配置
from global_dic import get as glv
from utils import index_mapping, data_getting_glb
from time_utils import strdate_transfer
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
        try:
            df_final = df_final[['valuation_date'] + columns]
        except:
            type_list = df_final.columns.tolist()
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