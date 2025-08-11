#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
生成函数字典文档
包含所有函数的详细信息
"""

def create_function_dictionary():
    """创建函数字典"""
    
    function_dict = {
        "utils.py": {
            "source_getting": {
                "函数名": "source_getting",
                "输入": "无",
                "输出": "str - 数据源模式（'local' 或 'sql'）",
                "说明": "获取数据源配置，从tools_path_config.json文件中读取数据源模式"
            },
            "source_getting2": {
                "函数名": "source_getting2",
                "输入": "config_path (str) - 配置文件路径",
                "输出": "str - 数据源模式（'local' 或 'sql'）",
                "说明": "从指定配置文件路径获取数据源配置"
            },
            "get_db_connection": {
                "函数名": "get_db_connection",
                "输入": "config_path (str, optional) - 配置文件路径\nuse_database2 (bool, optional) - 是否使用第二个数据库\nuse_database3 (bool, optional) - 是否使用第三个数据库\nmax_retries (int, optional) - 最大重试次数",
                "输出": "pymysql.connections.Connection - 数据库连接对象",
                "说明": "获取数据库连接，使用连接池管理，支持多数据库切换"
            },
            "close_all_connections": {
                "函数名": "close_all_connections",
                "输入": "无",
                "输出": "无",
                "说明": "关闭所有数据库连接池"
            },
            "data_reader": {
                "函数名": "data_reader",
                "输入": "filepath (str) - 文件路径\ndtype (dict, optional) - 数据类型\nindex_col (int, optional) - 索引列\nsheet_name (str, optional) - Excel工作表名称",
                "输出": "pandas.DataFrame - 读取的数据框",
                "说明": "读取数据文件，支持CSV、Excel和MAT格式"
            },
            "data_getting_glb": {
                "函数名": "data_getting_glb",
                "输入": "path (str) - 数据路径或SQL查询\nconfig_path (str, optional) - 配置文件路径\nsheet_name (str, optional) - Excel工作表名称\nupdate_time (bool, optional) - 是否保留update_time列",
                "输出": "pandas.DataFrame - 获取的数据",
                "说明": "全局数据获取函数，支持本地文件和数据库查询"
            },
            "data_getting": {
                "函数名": "data_getting",
                "输入": "path (str) - 数据路径或SQL查询\nconfig_path (str, optional) - 配置文件路径\nsheet_name (str, optional) - Excel工作表名称\nupdate_time (bool, optional) - 是否保留update_time列",
                "输出": "pandas.DataFrame - 获取的数据",
                "说明": "数据获取函数，支持本地文件和数据库查询"
            },
            "contains_chinese": {
                "函数名": "contains_chinese",
                "输入": "text (str) - 要检测的文本",
                "输出": "bool - 如果包含中文字符返回True，否则返回False",
                "说明": "检测文本是否包含中文字符"
            },
            "index_mapping": {
                "函数名": "index_mapping",
                "输入": "index_name (str) - 指数中文名称或代码\ntype (str, optional) - 返回类型，仅在中文名称输入时有效",
                "输出": "str - 指数代码、简称或中文名称",
                "说明": "指数名称映射 - 支持双向映射"
            },
            "readcsv": {
                "函数名": "readcsv",
                "输入": "filepath (str) - CSV文件路径\ndtype (dict, optional) - 指定列的数据类型\nindex_col (int, optional) - 指定索引列",
                "输出": "pandas.DataFrame - 读取的数据框",
                "说明": "读取CSV文件，支持多种编码格式"
            },
            "chunks": {
                "函数名": "chunks",
                "输入": "lst (list) - 要等分的列表\nn (int) - 等分数量",
                "输出": "list - 等分后的列表",
                "说明": "等分列表"
            },
            "file_withdraw": {
                "函数名": "file_withdraw",
                "输入": "inputpath (str) - 输入路径\navailable_date (str) - 日期",
                "输出": "str - 文件路径",
                "说明": "提取指定日期的文件"
            },
            "file_withdraw2": {
                "函数名": "file_withdraw2",
                "输入": "inputpath (str) - 输入路径\navailable_date (str) - 日期",
                "输出": "pandas.DataFrame - 读取的数据框",
                "说明": "提取指定日期的文件并读取数据"
            },
            "folder_creator": {
                "函数名": "folder_creator",
                "输入": "inputpath (str) - 文件夹路径",
                "输出": "无",
                "说明": "创建文件夹"
            },
            "folder_creator2": {
                "函数名": "folder_creator2",
                "输入": "path (str) - 目录路径",
                "输出": "无",
                "说明": "创建多级目录"
            },
            "folder_creator3": {
                "函数名": "folder_creator3",
                "输入": "file_path (str) - 文件路径",
                "输出": "无",
                "说明": "创建文件的路径"
            },
            "move_specific_files": {
                "函数名": "move_specific_files",
                "输入": "old_path (str) - 源目录\nnew_path (str) - 目标目录\nfiles_to_move (list, optional) - 要移动的文件列表",
                "输出": "无",
                "说明": "移动特定文件"
            },
            "move_specific_files2": {
                "函数名": "move_specific_files2",
                "输入": "old_path (str) - 源目录\nnew_path (str) - 目标目录",
                "输出": "无",
                "说明": "复制整个目录"
            },
            "get_string_before_last_dot": {
                "函数名": "get_string_before_last_dot",
                "输入": "s (str) - 字符串",
                "输出": "str - 最后一个点之前的字符串",
                "说明": "获取字符串中最后一个点之前的部分"
            },
            "optiondata_greeksprocessing": {
                "函数名": "optiondata_greeksprocessing",
                "输入": "df (pandas.DataFrame) - 包含delta, delta_wind, impliedvol, implied_vol_wind列的数据框",
                "输出": "pandas.DataFrame - 处理后的数据框",
                "说明": "处理期权Greeks数据，将delta_wind和implied_vol_wind的缺失值用delta和impliedvol补充"
            }
        },
        
        "time_utils.py": {
            "Chinese_valuation_date": {
                "函数名": "Chinese_valuation_date",
                "输入": "无",
                "输出": "pandas.DataFrame - 交易日期数据",
                "说明": "获取中国交易日期数据"
            },
            "next_workday_auto": {
                "函数名": "next_workday_auto",
                "输入": "无",
                "输出": "str - 下一个工作日",
                "说明": "获取下一个工作日"
            },
            "last_workday_auto": {
                "函数名": "last_workday_auto",
                "输入": "无",
                "输出": "str - 上一个工作日",
                "说明": "获取上一个工作日"
            },
            "last_workday_calculate": {
                "函数名": "last_workday_calculate",
                "输入": "x (str/datetime) - 指定日期",
                "输出": "str - 上一个工作日",
                "说明": "计算指定日期的上一个工作日"
            },
            "next_workday_calculate": {
                "函数名": "next_workday_calculate",
                "输入": "x (str/datetime) - 指定日期",
                "输出": "str - 下一个工作日",
                "说明": "计算指定日期的下一个工作日"
            },
            "last_workday_calculate2": {
                "函数名": "last_workday_calculate2",
                "输入": "df_score (pandas.DataFrame) - 包含date列的数据框",
                "输出": "pandas.DataFrame - 处理后的数据框",
                "说明": "批量计算上一个工作日"
            },
            "is_workday": {
                "函数名": "is_workday",
                "输入": "today (str) - 日期",
                "输出": "bool - 是否为工作日",
                "说明": "判断是否为工作日"
            },
            "working_days": {
                "函数名": "working_days",
                "输入": "df (pandas.DataFrame) - 包含date列的数据框",
                "输出": "pandas.DataFrame - 处理后的数据框",
                "说明": "筛选工作日数据"
            },
            "is_workday_auto": {
                "函数名": "is_workday_auto",
                "输入": "无",
                "输出": "bool - 是否为工作日",
                "说明": "判断今天是否为工作日"
            },
            "intdate_transfer": {
                "函数名": "intdate_transfer",
                "输入": "date (str/datetime) - 日期",
                "输出": "str - 整数格式日期",
                "说明": "日期转整数格式"
            },
            "strdate_transfer": {
                "函数名": "strdate_transfer",
                "输入": "date (str/datetime) - 日期",
                "输出": "str - 字符串格式日期",
                "说明": "日期转字符串格式"
            },
            "working_days_list": {
                "函数名": "working_days_list",
                "输入": "start_date (str) - 开始日期\nend_date (str) - 结束日期",
                "输出": "list - 工作日列表",
                "说明": "获取工作日列表"
            },
            "working_day_count": {
                "函数名": "working_day_count",
                "输入": "start_date (str) - 开始日期\nend_date (str) - 结束日期",
                "输出": "int - 工作日天数",
                "说明": "计算工作日天数"
            },
            "month_lastday_df": {
                "函数名": "month_lastday_df",
                "输入": "无",
                "输出": "list - 每月最后工作日列表",
                "说明": "获取每月最后工作日"
            },
            "last_weeks_lastday_df": {
                "函数名": "last_weeks_lastday_df",
                "输入": "无",
                "输出": "str - 上周最后工作日",
                "说明": "获取上周最后工作日"
            },
            "last_weeks_lastday": {
                "函数名": "last_weeks_lastday",
                "输入": "date (str) - 指定日期",
                "输出": "str - 上周最后工作日",
                "说明": "获取指定日期的上周最后工作日"
            },
            "weeks_firstday": {
                "函数名": "weeks_firstday",
                "输入": "date (str) - 日期",
                "输出": "str - 周第一个工作日",
                "说明": "获取周第一个工作日"
            },
            "next_weeks_lastday": {
                "函数名": "next_weeks_lastday",
                "输入": "date (str) - 日期",
                "输出": "str - 下周最后工作日",
                "说明": "获取下周最后工作日"
            }
        },
        
        "global_tools.py": {
            "sql_to_timeseries": {
                "函数名": "sql_to_timeseries",
                "输入": "df (pandas.DataFrame) - 包含valuation_date, code, value列的数据框",
                "输出": "pandas.DataFrame - 转换后的时间序列数据框",
                "说明": "将SQL查询结果转换为时间序列格式"
            },
            "rank_score_processing": {
                "函数名": "rank_score_processing",
                "输入": "df_score (pandas.DataFrame) - 包含valuation_date和code列的数据框",
                "输出": "pandas.DataFrame - 处理后的数据框",
                "说明": "标准化分数生成"
            },
            "code_transfer": {
                "函数名": "code_transfer",
                "输入": "df (pandas.DataFrame) - 包含code列的数据框",
                "输出": "pandas.DataFrame - 处理后的数据框",
                "说明": "股票代码格式转换"
            },
            "factor_name_old": {
                "函数名": "factor_name_old",
                "输入": "无",
                "输出": "tuple - (barra_name, industry_name)",
                "说明": "获取旧版因子名称"
            },
            "factor_name_new": {
                "函数名": "factor_name_new",
                "输入": "无",
                "输出": "tuple - (barra_name, industry_name)",
                "说明": "获取新版因子名称"
            },
            "factor_name": {
                "函数名": "factor_name",
                "输入": "inputpath_factor (str) - 因子文件路径",
                "输出": "tuple - (barra_name, industry_name)",
                "说明": "从因子文件中提取因子名称"
            },
            "weight_sum_check": {
                "函数名": "weight_sum_check",
                "输入": "df (pandas.DataFrame) - 包含weight列的数据框",
                "输出": "pandas.DataFrame - 处理后的数据框",
                "说明": "检查权重和"
            },
            "weight_sum_warning": {
                "函数名": "weight_sum_warning",
                "输入": "df (pandas.DataFrame) - 包含weight列的数据框",
                "输出": "无",
                "说明": "权重和警告"
            },
            "stock_volatility_calculate": {
                "函数名": "stock_volatility_calculate",
                "输入": "df (pandas.DataFrame) - 包含valuation_date列的数据框\navailable_date (str) - 日期",
                "输出": "pandas.DataFrame - 计算后的数据框",
                "说明": "计算股票波动率"
            },
            "factor_universe_withdraw": {
                "函数名": "factor_universe_withdraw",
                "输入": "type (str, optional) - 类型（'new'或'old'）",
                "输出": "pandas.DataFrame - 股票池数据",
                "说明": "获取股票池数据"
            },
            "index_weight_withdraw": {
                "函数名": "index_weight_withdraw",
                "输入": "index_type (str) - 指数类型\navailable_date (str) - 日期",
                "输出": "pandas.DataFrame - 权重股数据",
                "说明": "提取指数权重股数据"
            },
            "indexData_withdraw": {
                "函数名": "indexData_withdraw",
                "输入": "index_type (str) - 指数类型\nstart_date (str, optional) - 开始日期\nend_date (str, optional) - 结束日期\ncolumns (list, optional) - 列名列表\nrealtime (bool, optional) - 是否实时数据",
                "输出": "pandas.DataFrame - 指数数据",
                "说明": "提取指数收益率数据"
            },
            "indexFactor_withdraw": {
                "函数名": "indexFactor_withdraw",
                "输入": "index_type (str) - 指数类型\nstart_date (str, optional) - 开始日期\nend_date (str, optional) - 结束日期",
                "输出": "pandas.DataFrame - 因子暴露数据",
                "说明": "提取指数因子暴露数据"
            },
            "stockData_withdraw": {
                "函数名": "stockData_withdraw",
                "输入": "start_date (str, optional) - 开始日期\nend_date (str, optional) - 结束日期\ncolumns (list, optional) - 列名列表\nrealtime (bool, optional) - 是否实时数据",
                "输出": "pandas.DataFrame - 股票数据",
                "说明": "提取股票数据"
            },
            "etfData_withdraw": {
                "函数名": "etfData_withdraw",
                "输入": "start_date (str, optional) - 开始日期\nend_date (str, optional) - 结束日期\ncolumns (list, optional) - 列名列表\nrealtime (bool, optional) - 是否实时数据",
                "输出": "pandas.DataFrame - ETF数据",
                "说明": "提取ETF数据"
            },
            "cbData_withdraw": {
                "函数名": "cbData_withdraw",
                "输入": "start_date (str, optional) - 开始日期\nend_date (str, optional) - 结束日期\ncolumns (list, optional) - 列名列表\nrealtime (bool, optional) - 是否实时数据",
                "输出": "pandas.DataFrame - 可转债数据",
                "说明": "提取可转债数据"
            },
            "optionData_withdraw": {
                "函数名": "optionData_withdraw",
                "输入": "start_date (str, optional) - 开始日期\nend_date (str, optional) - 结束日期\ncolumns (list, optional) - 列名列表\nrealtime (bool, optional) - 是否实时数据",
                "输出": "pandas.DataFrame - 期权数据",
                "说明": "提取期权数据"
            },
            "futureData_withdraw": {
                "函数名": "futureData_withdraw",
                "输入": "start_date (str, optional) - 开始日期\nend_date (str, optional) - 结束日期\ncolumns (list, optional) - 列名列表\nrealtime (bool, optional) - 是否实时数据",
                "输出": "pandas.DataFrame - 期货数据",
                "说明": "提取期货数据"
            },
            "weight_df_standardization": {
                "函数名": "weight_df_standardization",
                "输入": "df (pandas.DataFrame) - 包含code列的数据框",
                "输出": "pandas.DataFrame - 标准化后的数据框",
                "说明": "标准化权重数据"
            },
            "weight_df_datecheck": {
                "函数名": "weight_df_datecheck",
                "输入": "df (pandas.DataFrame) - 包含valuation_date列的数据框",
                "输出": "无",
                "说明": "检查权重数据的日期完整性"
            },
            "portfolio_analyse": {
                "函数名": "portfolio_analyse",
                "输入": "df_holding (pandas.DataFrame, optional) - 持仓数据\naccount_money (float, optional) - 账户资金\ncost_stock (float, optional) - 股票成本\ncost_etf (float, optional) - ETF成本\ncost_future (float, optional) - 期货成本\ncost_option (float, optional) - 期权成本\ncost_convertiblebond (float, optional) - 可转债成本\nrealtime (bool, optional) - 是否实时数据\nweight_standardize (bool, optional) - 是否标准化权重",
                "输出": "tuple - (df_info, df_detail)",
                "说明": "投资组合分析"
            },
            "backtesting_report": {
                "函数名": "backtesting_report",
                "输入": "df_portfolio (pandas.DataFrame) - 投资组合数据\noutputpath (str, optional) - 输出路径\nindex_type (str, optional) - 指数类型\nsignal_name (str, optional) - 信号名称",
                "输出": "无",
                "说明": "回测报告生成"
            },
            "table_manager": {
                "函数名": "table_manager",
                "输入": "config_path (str) - 配置文件路径\ntable_name (str) - 要删除的表名",
                "输出": "bool - 操作是否成功",
                "说明": "删除指定的数据库表"
            }
        }
    }
    
    return function_dict

def save_function_dictionary():
    """保存函数字典到文件"""
    function_dict = create_function_dictionary()
    
    # 保存为Python字典文件
    with open('function_dictionary.py', 'w', encoding='utf-8') as f:
        f.write('# -*- coding: utf-8 -*-\n')
        f.write('"""\n')
        f.write('金融数据处理工具函数字典\n')
        f.write('包含所有函数的详细信息\n')
        f.write('"""\n\n')
        f.write('FUNCTION_DICTIONARY = ')
        f.write(str(function_dict))
        f.write('\n')
    
    # 保存为文本文件
    with open('function_dictionary.txt', 'w', encoding='utf-8') as f:
        f.write('金融数据处理工具函数字典\n')
        f.write('=' * 50 + '\n\n')
        
        for file_name, functions in function_dict.items():
            f.write(f'{file_name}\n')
            f.write('-' * 30 + '\n\n')
            
            for func_name, func_info in functions.items():
                f.write(f'函数名: {func_info["函数名"]}\n')
                f.write(f'输入: {func_info["输入"]}\n')
                f.write(f'输出: {func_info["输出"]}\n')
                f.write(f'说明: {func_info["说明"]}\n')
                f.write('-' * 20 + '\n\n')
    
    print("函数字典已保存到 function_dictionary.py 和 function_dictionary.txt")

if __name__ == "__main__":
    save_function_dictionary()

