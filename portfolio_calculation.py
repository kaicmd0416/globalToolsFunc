import pandas as pd 
import re
import numpy as np
from utils import index_mapping
# pd.set_option('display.max_columns', None)
# pd.set_option('display.max_rows', None)

class portfolio_calculation:
    """
    投资组合计算类
    
    提供投资组合分析功能，支持股票、ETF、期权、期货、可转债等多种金融工具的计算
    
    Input file formats:
    - df_holding: DataFrame with two columns
        - code: Security code
        - weight/quantity: Either weight (portfolio allocation) or quantity (number of units held)

    Market data formats (applies to df_stock, df_etf, df_option, df_future, df_convertible_bond):
    Each DataFrame contains three columns:
        - code: Security code
        - close: Current closing price
        - pre_close: Previous day's closing price
    """
    
    def __init__(self, df_stock=pd.DataFrame(), df_etf=pd.DataFrame(), df_option=pd.DataFrame(),
                 df_future=pd.DataFrame(), df_convertible_bond=pd.DataFrame(), df_index=pd.DataFrame(), 
                 account_money=None, cost_stock=None, cost_etf=None, cost_future=None, cost_option=None, 
                 cost_cb=None, realtime=False):
        """
        初始化投资组合计算对象
        
        Args:
            df_stock (pandas.DataFrame, optional): 股票市场数据，默认为空DataFrame
            df_etf (pandas.DataFrame, optional): ETF市场数据，默认为空DataFrame
            df_option (pandas.DataFrame, optional): 期权市场数据，默认为空DataFrame
            df_future (pandas.DataFrame, optional): 期货市场数据，默认为空DataFrame
            df_convertible_bond (pandas.DataFrame, optional): 可转债市场数据，默认为空DataFrame
            df_index (pandas.DataFrame, optional): 指数数据，默认为空DataFrame
            account_money (float, optional): 账户资金，默认为None
            cost_stock (float, optional): 股票交易成本，默认为None
            cost_etf (float, optional): ETF交易成本，默认为None
            cost_future (float, optional): 期货交易成本，默认为None
            cost_option (float, optional): 期权交易成本，默认为None
            cost_cb (float, optional): 可转债交易成本，默认为None
            realtime (bool, optional): 是否为实时数据，默认为False
        """
        self.df_stock = df_stock
        self.df_etf = df_etf
        self.df_option = df_option
        self.df_future = df_future
        self.df_convertible_bond = df_convertible_bond
        self.df_index = df_index
        self.account_money = account_money
        self.check_input_format(realtime)
        self.cost_stock = cost_stock
        self.cost_etf = cost_etf
        self.cost_future = cost_future
        self.cost_option = cost_option
        self.cost_cb = cost_cb
        self.realtime = realtime
        self.df_mkt = self.mktdata_data_processing()
    
    def df_processing(self, df):
        """
        数据处理函数
        
        对DataFrame进行标准化处理，包括缺失值处理、数据类型转换等
        
        Args:
            df (pandas.DataFrame): 要处理的数据框
            
        Returns:
            pandas.DataFrame: 处理后的数据框
        """
        df = df.replace('None', np.nan)
        df.dropna(inplace=True)
        for col in df.columns.tolist():
            # Skip date columns from float conversion
            if col == 'valuation_date':
                continue
            try:
                df[col] = df[col].astype(float)
            except:
                df[col] = df[col]
        return df
    
    def check_input_format(self, realtime):
        """
        检查输入格式
        
        验证所有输入的DataFrame是否符合要求的格式，包括必需的列名
        
        Args:
            realtime (bool): 是否为实时数据
            
        Raises:
            ValueError: 当输入格式不符合要求时抛出异常
        """
        invalid_inputs = []
        # Check market data formats
        market_dfs = {
            'stock': self.df_stock,
        }
        required_columns = ['valuation_date', 'code', 'close', 'pre_close']
        for df_name, df in market_dfs.items():
            if not all(col in df.columns for col in required_columns):
                invalid_inputs.append(df_name)
        etf_required_columns = ['valuation_date', 'code', 'close', 'pre_close']
        if not all(col in self.df_etf.columns for col in etf_required_columns):
            invalid_inputs.append('etf')
        # Special check for futures data
        if realtime == True:
             futures_required_columns = ['valuation_date', 'code', 'close', 'pre_settle', 'multiplier']
        else:
             futures_required_columns = ['valuation_date', 'code', 'settle', 'pre_settle', 'multiplier']
        if not all(col in self.df_future.columns for col in futures_required_columns):
            invalid_inputs.append('future')
        if realtime == True:
             oc_required_columns = ['valuation_date', 'code', 'close', 'pre_settle', 'delta', 'delta_yes']
        else:
            oc_required_columns = ['valuation_date', 'code', 'settle', 'pre_settle', 'delta', 'delta_yes']
        cvb_required_columns = ['valuation_date', 'code', 'close', 'pre_close', 'delta', 'delta_yes']
        if not all(col in self.df_option.columns for col in oc_required_columns):
            invalid_inputs.append('option')
        if not all(col in self.df_convertible_bond.columns for col in cvb_required_columns):
            invalid_inputs.append('convertible_bond')
        if invalid_inputs:
            print(f"以下输入文件格式不符合要求: {', '.join(invalid_inputs)}")
            raise ValueError
    
    def future_option_mapping(self, x):
        """
        期货期权映射函数
        
        将期权代码映射到对应的期货代码
        
        Args:
            x (str): 期权代码
            
        Returns:
            str: 对应的期货代码
            
        Raises:
            ValueError: 当无法识别期权代码时抛出异常
        """
        if str(x)[:2] == 'HO':
            return 'IH' + str(x)[2:6]
        elif str(x)[:2] == 'IO':
            return 'IF' + str(x)[2:6]
        elif str(x)[:2] == 'MO':
            return 'IM' + str(x)[2:6]
        else:
            print('qnmd')
            raise ValueError
    
    def option_mkt_calculate(self, df_option2, df_future2):
        """
        计算期权市场价值
        
        对时间序列数据进行期权市场价值计算，支持每日独立处理
        
        Args:
            df_option2 (pandas.DataFrame): 期权数据，包含valuation_date列
            df_future2 (pandas.DataFrame): 期货数据，包含valuation_date列
            
        Returns:
            pandas.DataFrame: 计算后的期权数据
            
        Raises:
            ValueError: 当缺少valuation_date列时抛出异常
        """
        df_option = df_option2.copy()
        df_future = df_future2.copy()

        # Check if valuation_date column exists
        if 'valuation_date' not in df_option.columns or 'valuation_date' not in df_future.columns:
            print("Error: valuation_date column is required for time series processing")
            raise ValueError

        # Get unique dates
        option_dates = df_option['valuation_date'].unique()
        future_dates = df_future['valuation_date'].unique()

        # Process each date separately
        processed_options = []

        for date in option_dates:
            # Get data for current date
            df_option_date = df_option[df_option['valuation_date'] == date].copy()
            df_future_date = df_future[df_future['valuation_date'] == date].copy()

            if df_future_date.empty:
                print(f"Warning: No future data for date {date}")
                # Add default values for missing future data
                df_option_date['risk_mkt_value'] = df_option_date['multiplier'] * df_option_date['close'] * df_option_date['delta']
                df_option_date['risk_mkt_value_yes'] = df_option_date['multiplier'] * df_option_date['pre_close'] * df_option_date['delta_yes']
                processed_options.append(df_option_date)
                continue

            # Process current date data
            df_future_processed = df_future_date[['code', 'mkt_value', 'mkt_value_yes', 'multiplier']].copy()
            df_future_processed.columns = ['code', 'mkt_value_future', 'mkt_value_future_yes', 'multiplier_future']
            df_future_processed.rename(columns={'code': 'future_type'}, inplace=True)

            # Add future_type mapping
            df_option_date['future_type'] = df_option_date['code'].apply(lambda x: self.future_option_mapping(x))

            # Merge with future data
            df_option_date = df_option_date.merge(df_future_processed, on='future_type', how='left')

            # Calculate proportions and market values
            df_option_date['proportion'] = df_option_date['delta'] / (df_option_date['multiplier_future'] / 100)
            df_option_date['proportion_yes'] = df_option_date['delta_yes'] / (df_option_date['multiplier_future'] / 100)
            df_option_date['risk_mkt_value'] = df_option_date['proportion'] * df_option_date['mkt_value_future']
            df_option_date['risk_mkt_value_yes'] = df_option_date['proportion_yes'] * df_option_date['mkt_value_future_yes']

            # Remove temporary columns
            df_option_date = df_option_date.drop(['future_type', 'mkt_value_future', 'mkt_value_future_yes', 'multiplier_future', 'proportion', 'proportion_yes'], axis=1)

            processed_options.append(df_option_date)

        # Combine all processed data
        if processed_options:
            df_option_final = pd.concat(processed_options, ignore_index=True)
            return df_option_final
        else:
            return df_option2
    
    def mktdata_data_processing(self):
        """
        市场数据处理
        
        处理各种类型的市场数据，统一格式并计算市场价值和风险价值
        
        Returns:
            pandas.DataFrame: 处理后的统一格式市场数据
        """
        # Initialize list to store non-empty processed DataFrames
        processed_dfs = []
        required_column = ['valuation_date', 'code', 'close', 'pre_close']
        if self.realtime == True:
             required_column2 = ['valuation_date', 'code', 'close', 'pre_settle']
        else:
             required_column2 = ['valuation_date', 'code', 'settle', 'pre_settle']
        # Process convertible bond if not empty
        if not self.df_convertible_bond.empty:
            df_convertible_bond = self.df_convertible_bond.copy()
            df_convertible_bond = df_convertible_bond[required_column + ['delta', 'delta_yes']]
            df_convertible_bond['class'] = 'convertible_bond'
            df_convertible_bond['multiplier'] = 10
            df_convertible_bond = self.df_processing(df_convertible_bond)
            df_convertible_bond['risk_mkt_value'] = df_convertible_bond['multiplier'] * df_convertible_bond['close'] * df_convertible_bond['delta']
            df_convertible_bond['risk_mkt_value_yes'] = df_convertible_bond['multiplier'] * df_convertible_bond['pre_close'] * df_convertible_bond['delta_yes']
            df_convertible_bond['mkt_value'] = df_convertible_bond['multiplier'] * df_convertible_bond['close']
            df_convertible_bond['mkt_value_yes'] = df_convertible_bond['multiplier'] * df_convertible_bond['pre_close']
            processed_dfs.append(df_convertible_bond)

        # Process ETF if not empty
        if not self.df_etf.empty:
            df_etf = self.df_etf.copy()
            df_etf = df_etf[required_column]
            df_etf['class'] = 'etf'
            df_etf['multiplier'] = 100
            df_etf['delta'] = 1
            df_etf['delta_yes'] = 1
            df_etf = self.df_processing(df_etf)
            df_etf['risk_mkt_value'] = df_etf['close'] * df_etf['multiplier'] * df_etf['delta']
            df_etf['risk_mkt_value_yes'] = df_etf['pre_close'] * df_etf['multiplier'] * df_etf['delta_yes']
            df_etf['mkt_value'] = df_etf['risk_mkt_value']
            df_etf['mkt_value_yes'] = df_etf['risk_mkt_value_yes']
            processed_dfs.append(df_etf)
        # Process future if not empty
        if not self.df_future.empty:
            df_future = self.df_future.copy()
            df_future = df_future[required_column2 + ['multiplier']]
            df_future.rename(columns={'pre_settle': 'pre_close'}, inplace=True)
            if self.realtime == False:
                df_future.rename(columns={'settle': 'close'}, inplace=True)
            df_future['class'] = 'future'
            df_future['delta'] = 1
            df_future['delta_yes'] = 1
            df_future = self.df_processing(df_future)
            df_future['risk_mkt_value'] = df_future['close'] * df_future['multiplier'] * df_future['delta']
            df_future['risk_mkt_value_yes'] = df_future['pre_close'] * df_future['multiplier'] * df_future['delta_yes']
            df_future['mkt_value'] = df_future['risk_mkt_value']
            df_future['mkt_value_yes'] = df_future['risk_mkt_value_yes']
            processed_dfs.append(df_future)
        else:
            df_future = pd.DataFrame()
        # Process option if not empty
        if not self.df_option.empty:
            df_option = self.df_option.copy()
            df_option = df_option[required_column2 + ['delta', 'delta_yes']]
            df_option.rename(columns={'pre_settle': 'pre_close'}, inplace=True)
            if self.realtime == False:
                df_option.rename(columns={'settle': 'close'}, inplace=True)
            df_option['class'] = 'option'
            df_option['multiplier'] = 100
            df_option = self.df_processing(df_option)
            if not df_future.empty:
                df_option = self.option_mkt_calculate(df_option, df_future)
            df_option['mkt_value'] = df_option['multiplier'] * df_option['close']
            df_option['mkt_value_yes'] = df_option['multiplier'] * df_option['pre_close']
            processed_dfs.append(df_option)
        # Process stock if not empty
        if not self.df_stock.empty:
            df_stock = self.df_stock.copy()
            df_stock = df_stock[required_column]
            df_stock['class'] = 'stock'
            df_stock['multiplier'] = 100
            df_stock['delta'] = 1
            df_stock['delta_yes'] = 1
            df_stock = self.df_processing(df_stock)
            df_stock['risk_mkt_value'] = df_stock['close'] * df_stock['multiplier'] * df_stock['delta']
            df_stock['risk_mkt_value_yes'] = df_stock['pre_close'] * df_stock['multiplier'] * df_stock['delta_yes']
            df_stock['mkt_value'] = df_stock['risk_mkt_value']
            df_stock['mkt_value_yes'] = df_stock['risk_mkt_value_yes']
            processed_dfs.append(df_stock)
        # If no DataFrames were processed, return empty DataFrame
        if not processed_dfs:
            return pd.DataFrame()
        # Concatenate all processed DataFrames
        df = pd.concat(processed_dfs)
        df = df.replace(0, np.nan)
        df = df.replace('None', np.nan)
        df.dropna(inplace=True)
        return df
    
    def df_holding_processing(self, df_holding, account_money):
        """
        持仓数据处理
        
        处理投资组合持仓数据，支持权重和数量两种输入格式的转换
        
        Args:
            df_holding (pandas.DataFrame): 持仓数据
            account_money (float): 账户资金
            
        Returns:
            pandas.DataFrame: 处理后的持仓数据
            
        Raises:
            ValueError: 当输入格式不正确时抛出异常
        """
        if 'weight' in df_holding.columns.tolist() and 'quantity' not in df_holding.columns.tolist():
            df_missing = df_holding[df_holding.isna().any(axis=1)]
            if len(df_missing) > 0:
                valuation_date = df_holding['valuation_date'].unique().tolist()
                if 'portfolio_name' in df_holding.columns.tolist():
                    portfolio_name = df_holding['portfolio_name'].unique().tolist()
                    print(f"{portfolio_name}在{valuation_date}中以下数据存在缺失，将weight做映射")
                else:
                    print(f"在{valuation_date}以下数据存在缺失，将weight做映射")
                print(df_missing['code'].tolist())
                code_list = df_missing['code'].tolist()
                df_holding = df_holding[~(df_holding['code'].isin(code_list))]
                df_holding['weight'] = df_holding['weight'] / df_holding['weight'].sum()
            if account_money == None:
                print('因为输入的holding为weight,所以account_money不能为空')
                raise ValueError
            else:
                df_holding['money'] = df_holding['weight'] * account_money
                df_holding['quantity'] = df_holding['money'] / df_holding['mkt_value_yes']
                df_holding['quantity'] = df_holding['quantity'].round(0)
        if 'quantity' in df_holding.columns.tolist() and 'weight' not in df_holding.columns.tolist():
            df_holding.loc[df_holding['class'].isin(['stock', 'etf', 'convertible_bond']), 'quantity'] = \
            df_holding[df_holding['class'].isin(['stock', 'etf', 'convertible_bond'])]['quantity'] / \
            df_holding[df_holding['class'].isin(['stock', 'etf', 'convertible_bond'])]['multiplier']
            df_holding['weight'] = (df_holding['quantity'] * df_holding['mkt_value']) / abs(df_holding['mkt_value'] * df_holding['quantity']).sum()
        return df_holding
    
    def turnover_calculate(self, df_holding_initial, df_holding_ori):
        """
        计算换手率
        
        计算投资组合的换手率、换手成本和收益成本
        
        Args:
            df_holding_initial (pandas.DataFrame): 初始持仓数据
            df_holding_ori (pandas.DataFrame): 当前持仓数据
            
        Returns:
            tuple: (turnover_ratio, turnover_return_cost, turnover_cost) - 换手率、收益成本、换手成本
        """
        df_holding = df_holding_ori[['code', 'quantity', 'weight', 'risk_mkt_value_yes', 'class']]
        if len(df_holding_initial) == 0:
            df_holding_initial = pd.DataFrame(columns=['code', 'quantity_yes', 'weight_yes', 'risk_mkt_value_yes_2', 'class_2'])
        else:
            df_holding_initial.columns = ['code', 'quantity_yes', 'weight_yes', 'risk_mkt_value_yes_2', 'class_2']
        df_final = df_holding.merge(df_holding_initial, on='code', how='outer')
        df_final.fillna(0, inplace=True)
        df_final['quantity_difference'] = df_final['quantity'] - df_final['quantity_yes']
        df_final['weight_difference'] = df_final['weight'] - df_final['weight_yes']
        turnover_ratio = abs(df_final['weight_difference']).sum()
        df_final.loc[df_final['risk_mkt_value_yes'].isna(), ['risk_mkt_value_yes']] = df_final[df_final['risk_mkt_value_yes'].isna()]['risk_mkt_value_yes_2']
        df_final.loc[df_final['class'].isna(), ['class']] = \
        df_final[df_final['class'].isna()]['class_2']
        # Calculate costs based on class
        df_final['cost'] = 0.0
        df_final['return_cost'] = 0.0

        # Calculate turnover cost and return cost for each class
        df_final.loc[df_final['class'] == 'stock', 'cost'] = abs(df_final[df_final['class'] == 'stock']['quantity_difference']) * df_final[df_final['class'] == 'stock']['risk_mkt_value_yes'] * self.cost_stock
        df_final.loc[df_final['class'] == 'stock', 'return_cost'] = abs(df_final[df_final['class'] == 'stock']['weight_difference']) * self.cost_stock

        df_final.loc[df_final['class'] == 'etf', 'cost'] = abs(df_final[df_final['class'] == 'etf']['quantity_difference']) * df_final[df_final['class'] == 'etf']['risk_mkt_value_yes'] * self.cost_etf
        df_final.loc[df_final['class'] == 'etf', 'return_cost'] = abs(df_final[df_final['class'] == 'etf']['weight_difference']) * self.cost_etf

        df_final.loc[df_final['class'] == 'option', 'cost'] = abs(df_final[df_final['class'] == 'option']['quantity_difference']) * df_final[df_final['class'] == 'option']['risk_mkt_value_yes'] * self.cost_option
        df_final.loc[df_final['class'] == 'option', 'return_cost'] = abs(df_final[df_final['class'] == 'option']['weight_difference']) * self.cost_option

        df_final.loc[df_final['class'] == 'future', 'cost'] = abs(df_final[df_final['class'] == 'future']['quantity_difference']) * df_final[df_final['class'] == 'future']['risk_mkt_value_yes'] * self.cost_future
        df_final.loc[df_final['class'] == 'future', 'return_cost'] = abs(df_final[df_final['class'] == 'future']['weight_difference']) * self.cost_future

        df_final.loc[df_final['class'] == 'convertible_bond', 'cost'] = abs(df_final[df_final['class'] == 'convertible_bond']['quantity_difference']) * df_final[df_final['class'] == 'convertible_bond']['risk_mkt_value_yes'] * self.cost_cb
        df_final.loc[df_final['class'] == 'convertible_bond', 'return_cost'] = abs(df_final[df_final['class'] == 'convertible_bond']['weight_difference']) * self.cost_cb

        turnover_cost = df_final['cost'].sum()
        turnover_return_cost = df_final['return_cost'].sum()

        return turnover_ratio, turnover_return_cost, turnover_cost
    
    def portfolio_calculation_main(self, df_holding_ori):
        """
        投资组合计算主函数
        
        执行完整的投资组合计算流程，包括每日收益计算、风险分析等
        
        Args:
            df_holding_ori (pandas.DataFrame): 原始持仓数据
            
        Returns:
            tuple: (df_info, df_detail) - 投资组合汇总信息和详细数据
        """
        df_holding_final = df_holding_ori.merge(self.df_mkt, on=['valuation_date', 'code'], how='left')
        date_list = df_holding_final['valuation_date'].unique().tolist()
        account_money = self.account_money
        df_info = pd.DataFrame()
        df_detail = pd.DataFrame()
        df_initial_holding = pd.DataFrame()
        for i in range(len(date_list)):
            date = date_list[i]
            df_holding = df_holding_final[df_holding_final['valuation_date'] == date]
            df_index = self.df_index[self.df_index['valuation_date'] == date]
            if 'index_type' not in df_holding.columns.tolist():
                index_type = None
            else:
                index_type = df_holding['index_type'].unique().tolist()[0]
            df_holding = self.df_holding_processing(df_holding, account_money)
            turnover_ratio, turnover_return_cost, turnover_cost = self.turnover_calculate(df_initial_holding, df_holding)
            df_missing = df_holding[df_holding.isna().any(axis=1)]
            if len(df_missing) > 0:
                df_holding.dropna(inplace=True)
                print(df_missing['code'].tolist())
            df_holding['price_difference'] = df_holding['close'] - df_holding['pre_close']
            df_holding['pct_chg'] = df_holding['price_difference'] / df_holding['pre_close']
            df_holding['profit'] = df_holding['price_difference'] * df_holding['quantity'] * df_holding['multiplier']
            df_holding['risk_mkt_value'] = df_holding['risk_mkt_value'] * df_holding['quantity']
            df_holding['risk_mkt_value_yes'] = df_holding['risk_mkt_value_yes'] * df_holding['quantity']
            df_holding['mkt_value'] = df_holding['mkt_value'] * df_holding['quantity']
            df_holding['paper_return'] = df_holding['pct_chg'] * df_holding['weight']
            df_holding.sort_values(by='profit', ascending=False, inplace=True)
            paper_return = df_holding['paper_return'].sum() - turnover_return_cost
            portfolio_profit = df_holding['profit'].sum()
            portfolio_profit = portfolio_profit - turnover_cost
            portfolio_mktvalue_yes = abs(df_holding['risk_mkt_value_yes']).sum()
            portfolio_return = portfolio_profit / portfolio_mktvalue_yes
            portfolio_riskvalue = df_holding['risk_mkt_value'].sum()
            if index_type != None:
                    code = index_mapping(index_type, 'code')
                    index_return = df_index[df_index['code'] == code]['pct_chg'].tolist()[0]
            else:
                    index_return = 0
            excess_paper_return = paper_return - index_return
            excess_portfolio_return = portfolio_return - index_return
            account_money = account_money + portfolio_profit
            portfolio_dic = {'valuation_date': [date], 'portfolio_profit': [portfolio_profit],
                             'portfolio_return': [portfolio_return], 'paper_return': [paper_return],
                             'portfolio_mktvalue': [portfolio_riskvalue], 'turnover_ratio': [turnover_ratio],
                             'turnover_return_cost': [turnover_return_cost], 'turnover_cost': [turnover_cost],
                             'index_return': [index_return], 'excess_paper_return': [excess_paper_return],
                             'excess_portfolio_return': [excess_portfolio_return], 'account_money': [account_money]}
            df_portfolio = pd.DataFrame(portfolio_dic)
            df_info = pd.concat([df_info, df_portfolio])
            df_detail = pd.concat([df_detail, df_holding])
            df_initial_holding = df_holding[['code', 'quantity', 'weight', 'risk_mkt_value', 'class']]
        return df_info, df_detail




