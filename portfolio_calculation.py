import pandas as pd 
import re
import numpy as np
# pd.set_option('display.max_columns', None)
# pd.set_option('display.max_rows', None)
class portfolio_calculation:
    """
    Portfolio analysis class that processes and analyzes different types of financial instruments.

    Input file formats:
    - df_holding: DataFrame with two columns
        - code: Security code
        - weight/quantity: Either weight (portfolio allocation) or quantity (number of units held)
    
    Market data formats (applies to df_stock, df_etf, df_option, df_future, df_convertible_bond):
    Each DataFrame contains three columns:
        - code: Security code
        - close: Current closing price
        - close_yes: Previous day's closing price
    """
    def __init__(self,df_initial_holding=pd.DataFrame(),df_holding=pd.DataFrame(),df_stock=pd.DataFrame(),df_etf=pd.DataFrame(),df_option=pd.DataFrame(),
                 df_future=pd.DataFrame(),df_convertible_bond=pd.DataFrame(),df_adj_factor=pd.DataFrame(),account_money=None,cost=None):
        self.df_initial_holding=df_initial_holding
        if self.df_initial_holding.empty:
            self.df_initial_holding=pd.DataFrame(columns=['code','quantity'])
        self.df_holding=df_holding
        self.df_stock=df_stock
        self.df_etf=df_etf
        self.df_option=df_option
        self.df_future=df_future
        self.df_convertible_bond=df_convertible_bond
        self.df_adjfactor=df_adj_factor
        self.account_money=account_money
        self.check_input_format()
        self.cost=cost
    def df_processing(self,df):
        df=df.replace('None',np.nan)
        df.dropna(inplace=True)
        for col in df.columns.tolist():
            try:
                df[col]=df[col].astype(float)
            except:
                df[col]=df[col]
        return df
    def check_input_format(self):
        """
        Check if all input DataFrames follow the required format.
        Returns a list of DataFrames that don't meet the format requirements.
        """
        invalid_inputs = []
        # Check df_holding format
        if not all(col in self.df_holding.columns for col in ['code']):
            invalid_inputs.append('holding')
        if not any(col in self.df_holding.columns for col in ['weight', 'quantity']):
            invalid_inputs.append('holding')
            
        # Check market data formats
        market_dfs = {
            'stock': self.df_stock,
            'etf': self.df_etf,
        }
        required_columns = ['code', 'close', 'close_yes']
        for df_name, df in market_dfs.items():
            if not all(col in df.columns for col in required_columns):
                invalid_inputs.append(df_name)
        adj_required_columns=['code', 'adjfactor', 'adjfactor_yes']
        if not all(col in self.df_adjfactor.columns for col in adj_required_columns):
            invalid_inputs.append('adjfactor')
        # Special check for futures data
        futures_required_columns = ['code', 'close', 'close_yes', 'multiplier']
        if not all(col in self.df_future.columns for col in futures_required_columns):
            invalid_inputs.append('future')
        oc_required_columns = ['code', 'close', 'close_yes', 'delta','delta_yes']
        if not all(col in self.df_option.columns for col in oc_required_columns):
            invalid_inputs.append('option')
        if not all(col in self.df_convertible_bond.columns for col in oc_required_columns):
            invalid_inputs.append('option')
        if invalid_inputs:
            print(f"以下输入文件格式不符合要求: {', '.join(invalid_inputs)}")
            raise ValueError
    def future_option_mapping(self,x):
        if str(x)[:2] == 'HO':
            return 'IH' + str(x)[2:6]
        elif str(x)[:2] == 'IO':
            return 'IF' + str(x)[2:6]
        elif str(x)[:2] == 'MO':
            return 'IM' + str(x)[2:6]
        else:
            print('qnmd')
            raise ValueError
    def option_mkt_calculate(self,df_option2,df_future2):
        df_option=df_option2.copy()
        df_future=df_future2
        df_future=df_future[['code','mkt_value','mkt_value_yes','multiplier']]
        df_future.columns=['code','mkt_value_future','mkt_value_future_yes','multiplier_future']
        df_future.rename(columns={'code':'future_type'},inplace=True)
        df_option['future_type']=df_option['code'].apply(lambda x: self.future_option_mapping(x))
        df_option=df_option.merge(df_future,on='future_type',how='left')

        df_option['proportion']=df_option['delta']/(df_option['multiplier_future']/100)
        df_option['proportion_yes'] = df_option['delta_yes'] / (df_option['multiplier_future']/100)
        df_option['mkt_value']=df_option['proportion']*df_option['mkt_value_future']
        df_option['mkt_value_yes']=df_option['proportion_yes']*df_option['mkt_value_future_yes']
        df_option2['mkt_value']=df_option['mkt_value'].tolist()
        df_option2['mkt_value_yes']=df_option['mkt_value_yes'].tolist()
        return df_option2
    def mktdata_data_processing(self):
        # Initialize list to store non-empty processed DataFrames
        processed_dfs = []
        
        # Process convertible bond if not empty
        if not self.df_convertible_bond.empty:
            df_convertible_bond=self.df_convertible_bond.copy()
            df_convertible_bond['class']='convertible_bond'
            df_convertible_bond['adjfactor']=1
            df_convertible_bond['adjfactor_yes'] = 1
            df_convertible_bond['multiplier']=10
            df_convertible_bond=self.df_processing(df_convertible_bond)
            df_convertible_bond['mkt_value']=df_convertible_bond['multiplier']*df_convertible_bond['close']
            df_convertible_bond['mkt_value_yes']=df_convertible_bond['multiplier']*df_convertible_bond['close_yes']
            processed_dfs.append(df_convertible_bond)
        # Process ETF if not empty
        if not self.df_etf.empty:
            df_etf=self.df_etf.copy()
            df_etf['class']='etf'
            df_etf['multiplier']=100
            df_etf['delta']=1
            df_etf['delta_yes'] = 1
            df_etf['adjfactor']=1
            df_etf['adjfactor_yes']=1
            df_etf=self.df_processing(df_etf)
            df_etf['mkt_value']=df_etf['close']*df_etf['multiplier']*df_etf['delta']
            df_etf['mkt_value_yes']=df_etf['close_yes']*df_etf['multiplier']*df_etf['delta']
            processed_dfs.append(df_etf)
            
        # Process future if not empty
        if not self.df_future.empty:
            df_future=self.df_future.copy()
            df_future['class']='future'
            df_future['delta']=1
            df_future['delta_yes'] = 1
            df_future['adjfactor']=1
            df_future['adjfactor_yes']=1
            df_future=self.df_processing(df_future)
            df_future['mkt_value']=df_future['close']*df_future['multiplier']*df_future['delta']
            df_future['mkt_value_yes']=df_future['close_yes']*df_future['multiplier']*df_future['delta']
            processed_dfs.append(df_future)
        else:
            df_future=pd.DataFrame()
            
        # Process option if not empty
        if not self.df_option.empty:
            df_option=self.df_option.copy()
            df_option['class']='option'
            df_option['multiplier']=100
            df_option['adjfactor']=1
            df_option['adjfactor_yes']=1
            df_option=self.df_processing(df_option)
            if not df_future.empty:
                df_option=self.option_mkt_calculate(df_option,df_future)

            processed_dfs.append(df_option)
        # Process stock if not empty
        if not self.df_stock.empty:
            df_stock=self.df_stock.copy()
            df_stock['class']='stock'
            df_stock['multiplier']=100
            df_stock['delta']=1
            df_stock['delta_yes'] = 1
            df_stock=self.df_processing(df_stock)
            df_adjfactor=self.df_processing(self.df_adjfactor)
            df_stock['mkt_value']=df_stock['close']*df_stock['multiplier']*df_stock['delta']
            df_stock['mkt_value_yes']=df_stock['close_yes']*df_stock['multiplier']*df_stock['delta']
            if not self.df_adjfactor.empty:
                df_stock=df_stock.merge(df_adjfactor,on='code',how='left')
            else:
                df_stock['adjfactor']=1
                df_stock['adjfactor_yes']=1
            processed_dfs.append(df_stock)
        # If no DataFrames were processed, return empty DataFrame
        if not processed_dfs:
            return pd.DataFrame()
            
        # Concatenate all processed DataFrames
        df = pd.concat(processed_dfs)
        df=df.replace('None',np.nan)
        df.dropna(inplace=True)
        return df
    def df_holding_processing(self,yes,turnover):
        if yes==False:
             df_holding=self.df_holding.copy()
        else:
            df_holding=self.df_initial_holding.copy()
        df_close=self.mktdata_data_processing()
        if 'weight' in self.df_holding.columns.tolist():
            df = df_holding.merge(df_close, on='code', how='left')
            df_missing = df[df.isna().any(axis=1)]
            if len(df_missing) > 0:
                print('以下数据存在缺失，将weight做映射')
                print(df_missing)
                code_list = df_missing['code'].tolist()
                df_holding=df_holding[~(df_holding['code'].isin(code_list))]
                df_holding['weight']=df_holding['weight']/df_holding['weight'].sum()
            if self.account_money==None:
                print('因为输入的holding为weight,所以account_money不能为空')
                raise ValueError
            else:
                df_holding['money']=df_holding['weight']*self.account_money
                df_holding=df_holding.merge(df_close,on='code',how='left')
                df_holding['quantity']=df_holding['money']/df_holding['mkt_value_yes']
                df_holding['quantity']=df_holding['quantity'].round(0)
        if 'quantity' in self.df_holding.columns.tolist():
            df_holding=df_holding.merge(df_close,on='code',how='left')
            df_holding.loc[df_holding['class'].isin(['stock','etf','convertible_bond']),'quantity']=df_holding[df_holding['class'].isin(['stock','etf','convertible_bond'])]['quantity']/df_holding[df_holding['class'].isin(['stock','etf','convertible_bond'])]['multiplier']
            df_holding['weight']=(df_holding['quantity']*df_holding['mkt_value'])/abs(df_holding['mkt_value']*df_holding['quantity']).sum()
        if turnover==True:
              df_holding=df_holding[['code','quantity','weight','mkt_value']]
        else:
              df_holding=df_holding[['code','quantity','weight']]
        return df_holding
    def turnover_calculate(self):
        df_holding_initial=self.df_holding_processing(yes=True,turnover=True)
        df_holding_initial.columns=['code','quantity_yes','weight_yes','mkt_value_yes']
        df_holding=self.df_holding_processing(yes=False,turnover=True)
        df_final=df_holding.merge(df_holding_initial,on='code',how='outer')
        df_final.fillna(0,inplace=True)
        df_final['quantity_difference']=df_final['quantity']-df_final['quantity_yes']
        df_final['weight_difference'] = df_final['weight'] - df_final['weight_yes']
        turnover_ratio=abs(df_final['weight_difference']).sum()
        df_final.loc[df_final['mkt_value'].isna(),['mkt_value']]=df_final[df_final['mkt_value'].isna()]['mkt_value_yes']
        df_final['cost']=abs(df_final['quantity_difference'])*df_final['mkt_value']*self.cost
        turnover_cost=df_final['cost'].sum()
        return turnover_ratio,turnover_cost
    def portfolio_calculation_main(self):
        df_holding=self.df_holding_processing(yes=False,turnover=False)
        df_mkt=self.mktdata_data_processing()
        df=df_holding.merge(df_mkt,on='code',how='left')
        df_missing = df[df.isna().any(axis=1)]
        turnover_ratio, turnover_cost=self.turnover_calculate()
        if len(df_missing) > 0:
            print('以下数据存在缺失，将按照0处理')
            print(df_missing)
        df['return'] = ((df['close'] * df['adjfactor']) - (
                    df['close_yes'] * df['adjfactor_yes'])) / (
                                         df['close_yes'] * df['adjfactor_yes'])
        df['price_difference']=(df['close']*df['adjfactor']-df['close_yes']*df['adjfactor_yes'])/df['adjfactor_yes']
        df['profit']=df['price_difference']*df['quantity']*df['multiplier']
        df['risk_mktvalue']=df['close']*df['multiplier']*df['quantity']*df['delta']
        df['mkt_value']=df['mkt_value']*df['quantity']
        df['mkt_value_yes']=df['mkt_value_yes']*df['quantity']
        df['paper_return']=df['return']*df['weight']
        paper_return=df['paper_return'].sum()-self.cost*turnover_ratio
        portfolio_profit=df['profit'].sum()
        portfolio_profit=portfolio_profit-turnover_cost
        portfolio_mktvalue_yes=abs(df['mkt_value_yes']).sum()
        portfolio_return=portfolio_profit/portfolio_mktvalue_yes
        portfolio_riskvalue=df['risk_mktvalue'].sum()
        portfolio_dic={'portfolio_profit':[portfolio_profit],'portfolio_return':[portfolio_return],'paper_return':[paper_return],
                       'portfolio_riskvalue':[portfolio_riskvalue],'turnover_ratio':[turnover_ratio],
                       'turnover_cost':[turnover_cost]}
        df_portfolio=pd.DataFrame(portfolio_dic)
        return df_portfolio



