import pandas as pd
import numpy as np
import os
import shutil
import warnings
import json
import pymysql
import subprocess
import sys
from dbutils.pooled_db import PooledDB
from datetime import time
from scipy.io import loadmat
global source, db_pools
# 初始化数据库连接池字典
db_pools = {}
# ============= 获取数据源模式 =============
def source_getting():
    """
    获取数据源配置

    Returns:
        str: 数据源模式（'local' 或 'sql'）
    """
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(current_dir, 'tools_path_config.json')
        with open(config_path, 'r', encoding='utf-8') as f:
            config_data = json.load(f)
        source = config_data['components']['data_source']['mode']
    except Exception as e:
        print(f"获取配置出错: {str(e)}")
        source = 'local'
    return source
def source_getting2(config_path):
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config_data = json.load(f)
        source = config_data['components']['data_source']['mode']
    except Exception as e:
        print(f"获取配置出错: {str(e)}")
        source = 'local'
    return source
source=source_getting()
# ============= 连接数据库 =============
def get_db_connection(config_path=None, use_database2=False, use_database3=False, max_retries=3):
    """
    获取数据库连接，使用连接池管理

    Args:
        config_path (str, optional): 配置文件路径
        use_database2 (bool, optional): 是否使用第二个数据库。默认为False。
        use_database3 (bool, optional): 是否使用第三个数据库。默认为False。
        max_retries (int, optional): 最大重试次数。默认为3。

    Returns:
        pymysql.connections.Connection: 数据库连接对象
    """
    if config_path == None:
        source2 = source
    else:
        source2 = source_getting2(config_path)

    if source2 == 'local':
        return None

    try:
        if config_path == None:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            config_path = os.path.join(current_dir, 'tools_path_config.json')

        # 生成连接池的唯一键
        pool_key = f"{config_path}_{use_database2}_{use_database3}"

        # 如果连接池不存在，创建新的连接池
        if pool_key not in db_pools:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)

            # 选择数据库配置
            if use_database3:
                db_key = 'database3'
            elif use_database2:
                db_key = 'database2'
            else:
                db_key = 'database1'
            db_config = config['components']['database'][db_key]

            # 创建连接池
            db_pools[pool_key] = PooledDB(
                creator=pymysql,  # 使用pymysql作为数据库连接器
                maxconnections=6,  # 连接池最大连接数
                mincached=2,  # 初始化时，连接池中至少创建的空闲的链接
                maxcached=5,  # 连接池中最多闲置的链接
                maxshared=3,  # 链接最大共享数
                blocking=True,  # 连接池中如果没有可用连接后，是否阻塞等待
                maxusage=None,  # 一个链接最多被重复使用的次数，None表示无限制
                setsession=[],  # 开始会话前执行的命令列表
                ping=1,  # ping MySQL服务端，检查是否服务可用
                host=db_config['host'],
                port=db_config['port'],
                user=db_config['user'],
                password=db_config['password'],
                charset=db_config['charset'],
                connect_timeout=100,  # 连接超时时间（秒）
                read_timeout=300,  # 读取超时时间（秒）
                write_timeout=300,  # 写入超时时间（秒）
                autocommit=True  # 自动提交
            )

        # 重试机制
        for attempt in range(max_retries):
            try:
                # 从连接池获取连接
                conn = db_pools[pool_key].connection()
                # 测试连接是否有效
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                return conn
            except (pymysql.err.OperationalError, pymysql.err.TimeoutError) as e:
                if attempt == max_retries - 1:  # 最后一次尝试
                    print(f"数据库连接失败，已重试{max_retries}次: {str(e)}")
                    raise
                print(f"数据库连接失败，正在重试 ({attempt + 1}/{max_retries}): {str(e)}")
                time.sleep(1)  # 等待1秒后重试

    except Exception as e:
        print(f"数据库连接失败: {str(e)}")
        return None
def close_all_connections():
    """
    关闭所有数据库连接池
    """
    global db_pools
    for pool in db_pools.values():
        try:
            pool.close()
        except:
            pass
    db_pools.clear()
# 在程序退出时关闭所有连接
import atexit
atexit.register(close_all_connections)
# ============= 数据读取=============
def data_reader(filepath, dtype=None, index_col=None, sheet_name=None):
    """
    读取数据文件，支持CSV、Excel和MAT格式

    Args:
        filepath (str): 文件路径
        dtype (dict, optional): 数据类型
        index_col (int, optional): 索引列

    Returns:
        pandas.DataFrame: 读取的数据框
    """
    if filepath == None:
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
            if sheet_name != None:
                df = pd.read_excel(filepath, dtype=dtype, index_col=index_col, sheet_name=sheet_name)
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


def data_getting_glb(path, config_path=None, sheet_name=None,update_time=False):
    """
    获取数据

    Args:
        path (str): 数据路径或SQL查询

    Returns:
        pandas.DataFrame: 获取的数据
    """
    df = pd.DataFrame()
    if source == 'local':
        df = data_reader(path, sheet_name=sheet_name)
    else:
        try:
            conn1 = get_db_connection(config_path)
            if conn1 is not None:
                try:
                    df = pd.read_sql(path, con=conn1)
                    conn1.close()
                    if not df.empty:
                        pass  # 已找到数据
                except Exception as e:
                    pass
            if df.empty:
                conn2 = get_db_connection(config_path, use_database2=True)
                if conn2 is not None:
                    try:
                        df = pd.read_sql(path, con=conn2)
                        conn2.close()
                        if not df.empty:
                            pass
                    except Exception as e:
                        pass
            if df.empty:
                conn3 = get_db_connection(config_path, use_database3=True)
                if conn3 is not None:
                    try:
                        df = pd.read_sql(path, con=conn3)
                        conn3.close()
                    except Exception as e:
                        pass
        except Exception as e:
            if df.empty:
                print(f"数据获取失败: {str(e)}")
    if df.empty:
        print(f"未找到数据: {path}")
    if update_time==False:
        try:
            df.drop(columns='update_time',inplace=True)
        except:
            pass
    return df
def data_getting(path, config_path=None, sheet_name=None,update_time=False):
    """
    获取数据

    Args:
        path (str): 数据路径或SQL查询

    Returns:
        pandas.DataFrame: 获取的数据
    """
    source2 = source_getting2(config_path)
    df = pd.DataFrame()
    if source2 == 'local':
        df = data_reader(path, sheet_name=sheet_name)
    else:
        conn1 = get_db_connection(config_path)
        if conn1 is not None:
            try:
                df = pd.read_sql(path, con=conn1)
                conn1.close()
                if not df.empty:
                    pass  # 已找到数据
            except Exception as e:
                pass
        if df.empty:
            conn2 = get_db_connection(config_path, use_database2=True)
            if conn2 is not None:
                try:
                    df = pd.read_sql(path, con=conn2)
                    conn2.close()
                    if not df.empty:
                        pass
                except Exception as e:
                    pass
        if df.empty:
            conn3 = get_db_connection(config_path, use_database3=True)
            if conn3 is not None:
                try:
                    df = pd.read_sql(path, con=conn3)
                    conn3.close()
                except Exception as e:
                    pass
    if df.empty:
        print(f"未找到数据: {path}")
    for column in df.columns.tolist():
        if column != 'valuation_date':
            try:
                df[column] = df[column].astype(float)
            except:
                pass
    if update_time==False:
        try:
            df.drop(columns='update_time',inplace=True)
        except:
            pass
    return df
# =============指数通用函数=============
def contains_chinese(text):
    """
    检测文本是否包含中文字符

    Args:
        text (str): 要检测的文本

    Returns:
        bool: 如果包含中文字符返回True，否则返回False
    """
    if text is None:
        return False
    for char in text:
        if '\u4e00' <= char <= '\u9fff':
            return True
    return False
def index_mapping(index_name, type='code'):
    """
    指数名称映射 - 支持双向映射

    Args:
        index_name (str): 指数中文名称或代码
        type (str, optional): 返回类型，仅在中文名称输入时有效

    Returns:
        str: 指数代码、简称或中文名称
    """
    # 如果输入包含中文，执行原有逻辑（中文名称 -> 代码/简称）
    if contains_chinese(index_name):
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
                return '000510.CSI'
        else:
            print(f'{index_name} 不存在')
            return None
    # 如果输入不包含中文，根据type参数决定返回类型
    else:
        # 当type='code'时，返回shortname；当type='shortname'时，返回中文名称
        if type == 'shortname':
            # 返回shortname
            if index_name == '000016.SH':
                return 'sz50'
            elif index_name == '000300.SH':
                return 'hs300'
            elif index_name == '000905.SH':
                return 'zz500'
            elif index_name == '000852.SH':
                return 'zz1000'
            elif index_name == '932000.CSI':
                return 'zz2000'
            elif index_name == '399303.SZ':
                return 'gz2000'
            elif index_name == '000510.CSI':
                return 'zzA500'
            else:
                print(f'{index_name} 不存在')
                return None
        else:
            return index_name

# ============= 文件操作函数 =============
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
        inputpath = os.path.join(inputpath, file_name)
        df = data_reader(inputpath)
    else:
        df = pd.DataFrame()
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