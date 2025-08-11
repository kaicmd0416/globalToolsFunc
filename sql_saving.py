import yaml
import os
import sys
from typing import Dict, Any, Optional
import pandas as pd
from sqlalchemy import inspect, create_engine, MetaData, Table, Column, Float, String, DateTime, text, Integer, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, text
from sqlalchemy import bindparam
from sqlalchemy.exc import OperationalError, SQLAlchemyError
Base = declarative_base()

class ConfigReader:
    """
    配置读取器类
    
    负责读取和管理YAML格式的配置文件，提供任务配置和参数获取功能
    
    支持从指定路径或脚本目录自动查找配置文件
    """

    def __init__(self, config_path: str = "task_config.yaml"):
        """
        初始化配置读取器
        
        Args:
            config_path (str): 配置文件路径，默认为"task_config.yaml"
        """
        self.config_path = config_path
        self.config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        """
        加载配置文件
        
        从指定路径加载YAML配置文件，如果文件不存在会尝试在脚本目录查找
        
        Returns:
            Dict[str, Any]: 配置字典，加载失败时返回空字典
        """
        try:
            if not os.path.exists(self.config_path):
                print(f"配置文件不存在: {self.config_path}")

                # 尝试在脚本目录查找
                script_dir = os.path.dirname(os.path.abspath(__file__))
                alt_path = os.path.join(script_dir, os.path.basename(self.config_path))
                if os.path.exists(alt_path):
                    self.config_path = alt_path
                else:
                    return {}

            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)

            return config if config else {}
        except Exception as e:
            print(f"加载配置文件失败: {str(e)}")
            return {}

    def get_task_config(self, task_name: str) -> Dict[str, Any]:
        """
        获取任务配置
        
        根据任务名称获取完整的任务配置信息
        
        Args:
            task_name (str): 任务名称
            
        Returns:
            Dict[str, Any]: 任务配置字典，未找到时返回空字典
        """
        if not self.config:
            print("配置文件为空")
            return {}

        task_config = self.config.get(task_name, {})
        if not task_config:
            print(f"未找到任务配置: {task_name}")

        return task_config

    def get_task_param(self, task_name: str, param_name: str, default: Any = None) -> Any:
        """
        获取任务参数
        
        根据任务名称和参数名称获取特定的参数值
        
        Args:
            task_name (str): 任务名称
            param_name (str): 参数名称
            default (Any, optional): 默认值，当参数不存在时返回
            
        Returns:
            Any: 参数值或默认值
        """
        task_config = self.get_task_config(task_name)
        return task_config.get(param_name, default)


class TableManager:
    """
    表管理器类
    
    负责数据库表的创建、存在性检查和结构更新
    
    提供表结构管理和维护功能，支持动态添加列
    """

    def __init__(self, engine):
        """
        初始化表管理器
        
        Args:
            engine: SQLAlchemy引擎对象
        """
        self.engine = engine
        self.inspector = inspect(engine)
        self.metadata = MetaData()
        # 确保元数据表存在

    def table_exists(self, table_name: str) -> bool:
        """
        检查表是否存在
        
        检查数据库中是否存在指定的表
        
        Args:
            table_name (str): 表名
            
        Returns:
            bool: 表是否存在
        """
        try:
            # 转换表名为小写
            table_name = table_name.lower()
            return self.engine.dialect.has_table(self.engine.connect(), table_name)
        except Exception as e:
            print(f"Error checking table existence: {str(e)}")
            return False
        finally:
            # 确保连接被正确关闭
            self.engine.dispose()

    def create_data_table(self, table_name: str, df: pd.DataFrame, schema: dict, private_keys: list[str] = None) -> None:
        """
        创建数据表
        
        根据DataFrame和schema定义创建数据库表结构
        
        Args:
            table_name (str): 表名
            df (pd.DataFrame): 数据框，用于确定列名
            schema (dict): 表结构定义字典
            private_keys (list[str], optional): 主键列名列表，默认为None
            
        Raises:
            ValueError: 当schema中包含未知的数据类型时抛出异常
        """
        columns = []
        for col_name, col_info in schema.items():
            col_type = col_info['type']
            # 处理长度
            if col_type == 'String':
                length = col_info.get('length', 50)
                coltype_obj = String(length)
            elif col_type == 'Text':
                coltype_obj = Text
            elif col_type == 'Integer':
                coltype_obj = Integer
            elif col_type == 'SmallInteger':
                coltype_obj = SmallInteger
            elif col_type == 'BigInteger':
                coltype_obj = BigInteger
            elif col_type == 'Float':
                coltype_obj = Float
            elif col_type == 'Numeric':
                precision = col_info.get('precision', 10)
                scale = col_info.get('scale', 2)
                coltype_obj = Numeric(precision, scale)
            elif col_type == 'Boolean':
                coltype_obj = Boolean
            elif col_type == 'DateTime':
                coltype_obj = DateTime
            elif col_type == 'Date':
                coltype_obj = Date
            elif col_type == 'Time':
                coltype_obj = Time
            elif col_type == 'JSON':
                coltype_obj = JSON
            else:
                raise ValueError(f'Unknown type: {col_type}')
            is_pk = col_name in private_keys
            columns.append(Column(col_name, coltype_obj, primary_key=is_pk))
        Table(table_name, self.metadata, *columns)
        # 创建所有表
        self.metadata.create_all(self.engine)
        # 确保连接被正确关闭
        self.engine.dispose()

    def update_table_if_needed(self, table_name: str, df: pd.DataFrame) -> None:
        """
        如果需要则更新表结构
        
        检查DataFrame中的列是否在表中存在，如果不存在则添加新列
        
        Args:
            table_name (str): 表名
            df (pd.DataFrame): 数据框
            
        Raises:
            Exception: 更新表结构时发生错误
        """
        try:
            # 转换表名为小写
            table_name = table_name.lower()

            with self.engine.connect() as conn:
                # 获取现有列
                result = conn.execute(text(f"SHOW COLUMNS FROM {table_name}"))
                existing_columns = {row[0] for row in result}

                # 检查新列
                for col in df.columns:
                    if col not in existing_columns:
                        dtype = df[col].dtype
                        if pd.api.types.is_integer_dtype(dtype):
                            col_type = "INTEGER"
                        elif pd.api.types.is_float_dtype(dtype):
                            col_type = "FLOAT"
                        else:
                            col_type = "VARCHAR(255)"

                        # 添加新列
                        conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {col} {col_type}"))
                        print(f"Added column {col} to table {table_name}")

        except Exception as e:
            print(f"Error updating table structure: {str(e)}")
            raise
        finally:
            # 确保连接被正确关闭
            self.engine.dispose()

    def _get_sql_type(self, dtype) -> str:
        """
        根据pandas数据类型获取SQL类型
        
        将pandas的数据类型映射到对应的SQL数据类型
        
        Args:
            dtype: pandas数据类型
            
        Returns:
            str: SQL类型字符串
        """
        if pd.api.types.is_integer_dtype(dtype):
            return "BIGINT"
        elif pd.api.types.is_float_dtype(dtype):
            return "DOUBLE"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return "DATETIME"
        else:
            return "VARCHAR(255)"


class DatabaseWriter:
    """
    数据库写入器类
    
    负责将DataFrame数据写入数据库，支持批量写入和REPLACE INTO操作
    
    提供连接池管理和错误处理功能
    """

    def __init__(self, db_url: str):
        """
        初始化数据库写入器
        
        Args:
            db_url (str): 数据库连接URL
            
        Raises:
            Exception: 创建数据库引擎失败时抛出异常
        """
        try:
            self.engine = create_engine(
                db_url,
                pool_size=5,
                max_overflow=10,
                pool_timeout=30,
                pool_recycle=3600,
                echo=False
            )
        except Exception as e:
            print(f"Failed to create database engine: {str(e)}")
            raise

    def write(self, df: pd.DataFrame, table_name: str, if_exists: str = 'append', delete=False, delet_name=None, delet_key=None) -> None:
        """
        写入数据到数据库
        
        将DataFrame数据写入指定的数据库表，支持删除指定日期的数据
        
        Args:
            df (pd.DataFrame): 要写入的数据框
            table_name (str): 目标表名
            if_exists (str, optional): 如果表存在时的处理方式，默认为'append'
                - 'fail': 如果表存在则失败
                - 'replace': 如果表存在则替换
                - 'append': 如果表存在则追加
            delete (bool, optional): 是否删除指定valuation_date的数据，默认为False
            delet_name (str, optional): 删除条件列名，默认为None
            delet_key (str, optional): 删除条件值，默认为None
            
        Raises:
            Exception: 写入数据时发生错误
        """
        try:
            # Convert table name to lowercase to avoid case sensitivity issues
            table_name_lower = table_name.lower()
            
            # 如果delete=True，先删除指定valuation_date的数据
            if delete:
                valuation_list = df['valuation_date'].unique().tolist()
                with self.engine.connect() as conn:
                    # 根据delet_name和delet_key是否为空决定SQL
                    if delet_name is not None and delet_key is not None:
                        delete_sql = f"DELETE FROM {table_name_lower} WHERE valuation_date IN :val_list AND {delet_name} = :delet_key"
                        params = {'val_list': valuation_list, 'delet_key': delet_key}
                    else:
                        delete_sql = f"DELETE FROM {table_name_lower} WHERE valuation_date IN :val_list"
                        params = {'val_list': valuation_list}
                    try:
                        conn.execute(
                            text(delete_sql).bindparams(bindparam('val_list', expanding=True)),
                            params
                        )
                        conn.commit()
                        print(f"Successfully deleted {len(valuation_list)} valuation_date records from table {table_name_lower}{f' where {delet_name} = {delet_key}' if delet_name and delet_key else ''}")
                    except Exception as e:
                        print(f"Failed to delete from table {table_name_lower}: {str(e)}")
                        conn.rollback()
                        raise
            # 写入新数据
            df.to_sql(
                name=table_name_lower,
                con=self.engine,
                if_exists=if_exists,
                index=False,
                chunksize=10000,
                method=self.replace_into_method
            )
            print(f"Successfully wrote {len(df)} rows to table {table_name_lower}")
        except Exception as e:
            print(f"Failed to write to table {table_name}: {str(e)}")
            failed_batch = df.iloc[0:1000]
            failed_batch.to_csv("error_batch.csv")
            raise
        finally:
            # 确保连接被正确关闭
            self.engine.dispose()

    def replace_into_method(self, table, conn, keys, data_iter):
        """
        自定义批量 REPLACE INTO 实现
        
        适用于 pandas.to_sql(method=replace_into_method)，提供REPLACE INTO功能
        
        Args:
            table: 目标表对象
            conn: 数据库连接
            keys: 列名列表
            data_iter: 数据迭代器
            
        Returns:
            int: 插入的行数
            
        Raises:
            Exception: 插入数据时发生错误
        """
        try:
            # 构建列名和占位符
            l_columns = [f'`{col}`' for col in keys]
            columns = ', '.join(l_columns)
            placeholders = ', '.join([':{}'.format(k) for k in keys])

            # 构建 REPLACE 语句
            sql = f"REPLACE INTO {table.name} ({columns}) VALUES ({placeholders})"

            # 处理数据
            data = []
            for row in data_iter:
                processed_row = {}
                for i, val in enumerate(row):
                    if val is None:
                        processed_row[keys[i]] = None
                    else:
                        processed_row[keys[i]] = str(val)
                data.append(processed_row)

            # 执行 SQL
            stmt = text(sql)
            result = conn.execute(stmt, data)

            print(f"Successfully inserted {len(data)} rows into {table.name}")
            return result.rowcount
        except Exception as e:
            print(f"Error inserting data into {table.name}")
            print(f"Data sample: {data[0] if data else 'No data'}")
            raise

    def execute_query(self, query: str) -> None:
        """
        执行SQL查询
        
        执行指定的SQL查询语句
        
        Args:
            query (str): SQL查询语句
            
        Raises:
            Exception: 执行查询时发生错误
        """
        try:
            with self.engine.begin() as connection:
                connection.execute(text(query))
            print(f"Successfully executed query")
        except Exception as e:
            print(f"Failed to execute query: {str(e)}")
            raise
        finally:
            # 确保连接被正确关闭
            self.engine.dispose()


class SqlSaving:
    """
    SQL数据保存主类
    
    提供完整的数据库保存功能，包括配置管理、表结构管理和数据写入
    
    支持数据标准化、批量处理和错误恢复
    """

    def __init__(self, config_path, task_name, delete):
        """
        初始化SQL数据保存对象
        
        Args:
            config_path (str): 配置文件路径
            task_name (str): 任务名称
            delete (bool): 是否启用删除功能
        """
        # 创建配置读取器
        config_reader = ConfigReader(config_path)

        # 从配置读取器获取所有参数
        self.db_url = config_reader.get_task_param(task_name, "db_url")
        # 转为小写，确保表名一致性
        self.table_name = config_reader.get_task_param(task_name, "table_name").lower()
        print(f"Using lowercase table name: {self.table_name}")

        self.chunk_size = config_reader.get_task_param(task_name, "chunk_size")
        self.workers = config_reader.get_task_param(task_name, "workers")
        self.private_keys = config_reader.get_task_param(task_name, "private_keys")
        self.schema = config_reader.get_task_param(task_name, 'schema')
        # 初始化其他组件
        self.writer = DatabaseWriter(self.db_url)
        self.table_manager = TableManager(self.writer.engine)
        self.processed_tables = set()  # 跟踪已处理的表
        self.delete = delete

    def process_file(self, df: pd.DataFrame, delet_name, delet_key) -> None:
        """
        处理单个文件
        
        将DataFrame数据标准化后写入数据库，包括表结构管理和数据写入
        
        Args:
            df (pd.DataFrame): 要处理的数据框
            delet_name (str): 删除条件列名
            delet_key (str): 删除条件值
            
        Raises:
            Exception: 处理文件时发生错误
        """
        df = self.df_standardize(df)
        try:
            # 检查点：创建或更新表结构
            print("步骤: 创建或更新表结构")
            self._create_or_update_table(df)
            print("表结构创建或更新完成")
            # 写入数据
            self.writer.write(df, self.table_name, 'append', self.delete, delet_name, delet_key)
            print(f"数据已写入表: {self.table_name}")
        except Exception as e:
            error_msg = f"处理文件时发生错误: {str(e)}"
            print(error_msg)
            raise Exception(error_msg)

    def _create_or_update_table(self, df: pd.DataFrame) -> None:
        """
        创建或更新表结构
        
        根据DataFrame和配置的schema创建新表或更新现有表结构
        
        Args:
            df (pd.DataFrame): 数据框
        """
        df = self.df_standardize(df)
        if not self.table_manager.table_exists(self.table_name):
            self.table_manager.create_data_table(self.table_name, df, self.schema, self.private_keys)
        else:
            self.table_manager.update_table_if_needed(self.table_name, df)

    def df_standardize(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        数据标准化处理
        
        根据配置文件schema将DataFrame的每一列转换为指定类型
        
        Args:
            df (pd.DataFrame): 原始DataFrame
            
        Returns:
            pd.DataFrame: 类型标准化后的DataFrame
        """
        import numpy as np
        schema = self.schema
        df = df.copy()
        try:
            if 'valuation_date' in df.columns:
                df['valuation_date'] = pd.to_datetime(df['valuation_date'].astype(str))
                df['valuation_date'] = df['valuation_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        except:
            pass
        for col, col_info in schema.items():
            if col not in df.columns:
                continue
            col_type = col_info.get('type')
            try:
                if col_type == 'String':
                    df[col] = df[col].astype(str)
                elif col_type == 'Float':
                    df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce').astype(float)
                elif col_type == 'Integer':
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
                elif col_type == 'Boolean':
                    df[col] = df[col].astype(bool)
                elif col_type == 'DateTime':
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                elif col_type == 'Date':
                    df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
                # 可扩展更多类型
            except Exception as e:
                print(f"列 {col} 类型转换为 {col_type} 失败: {e}")
        return df