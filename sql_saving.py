import yaml
import os
import sys
from typing import Dict, Any, Optional
import pandas as pd
from sqlalchemy import inspect, Column, String, Integer, DateTime, Float, text, Table, MetaData, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, text

Base = declarative_base()
class ConfigReader:
    """配置读取器"""

    def __init__(self, config_path: str = "task_config.yaml"):
        """
        初始化配置读取器
        :param config_path: 配置文件路径
        """
        self.config_path = config_path
        self.config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        """
        加载配置文件
        :return: 配置字典
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
        :param task_name: 任务名称
        :return: 任务配置字典
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
        :param task_name: 任务名称
        :param param_name: 参数名称
        :param default: 默认值
        :return: 参数值
        """
        task_config = self.get_task_config(task_name)
        return task_config.get(param_name, default)
class TableManager:
    """表管理器"""

    def __init__(self, engine):
        """
        初始化表管理器
        :param engine: SQLAlchemy引擎
        """
        self.engine = engine
        self.inspector = inspect(engine)
        self.metadata = MetaData()
        # 确保元数据表存在

    def table_exists(self, table_name: str) -> bool:
        """
        检查表是否存在
        :param table_name: 表名
        :return: 是否存在
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


    def create_data_table(self, table_name: str, df: pd.DataFrame, private_keys: list[str] = None) -> None:
        """
        创建数据表
        :param table_name: 表名
        :param df: 数据框
        :param private_keys list[str] 联合索引集合
        """
        try:
            # 转换表名为小写
            table_name = table_name.lower()

            # 创建数据表
            columns = []
            exist_columns = []
            if private_keys:
                for private_key in private_keys:
                    exist_columns.append(private_key)
                    columns.append(Column(private_key, String(255), nullable=False))
            # 根据DataFrame的列类型创建表结构

            for col in df.columns:
                if col not in exist_columns:  # 跳过已添加的列
                    dtype = df[col].dtype
                    if pd.api.types.is_integer_dtype(dtype):
                        columns.append(Column(col, Integer))
                    elif pd.api.types.is_float_dtype(dtype):
                        columns.append(Column(col, Float))
                    else:
                        columns.append(Column(col, String(255)))

            # 创建表
            table = Table(table_name, self.metadata, *columns,
                          mysql_engine='InnoDB',
                          mysql_charset='utf8mb4',
                          mysql_collate='utf8mb4_unicode_ci')
            if private_keys:
                unique_constraint = UniqueConstraint(*private_keys, name=f'uk_{table_name}_private_keys')
                table.append_constraint(unique_constraint)
            table.create(self.engine)

            print(f"Created table: {table_name}")

        except Exception as e:
            print(f"Error creating table {table_name}: {str(e)}")
            raise
        finally:
            # 确保连接被正确关闭
            self.engine.dispose()


    def update_table_if_needed(self, table_name: str, df: pd.DataFrame) -> None:
        """
        如果需要则更新表结构
        :param table_name: 表名
        :param df: 数据框
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
        :param dtype: pandas数据类型
        :return: SQL类型字符串
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
    """数据库写入器"""

    def __init__(self, db_url: str):
        """
        初始化数据库写入器
        :param db_url: 数据库连接URL
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

    def write(self, df: pd.DataFrame, table_name: str, if_exists: str = 'append') -> None:
        """
        写入数据到数据库
        :param df: 数据框
        :param table_name: 表名
        :param if_exists: 如果表存在时的处理方式 ('fail', 'replace', 'append')
        """
        try:
            # Convert table name to lowercase to avoid case sensitivity issues
            table_name_lower = table_name.lower()
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
        适用于 pandas.to_sql(method=replace_into_method)
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
        :param query: SQL查询语句
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
    """目录处理器基类"""

    def __init__(self, config_path,task_name):
        """
        初始化目录处理器
        :param task_name: 任务名称
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

        # 初始化其他组件
        self.writer = DatabaseWriter(self.db_url)
        self.table_manager = TableManager(self.writer.engine)
        self.processed_tables = set()  # 跟踪已处理的表

    def process_file(self, df: pd.DataFrame) -> None:
        """
        处理单个文件
        :param file_path: 文件路径
        :param extra_columns: 额外的列信息
        """
        try:
            # 检查点：创建或更新表结构
            print("步骤: 创建或更新表结构")
            self._create_or_update_table(df)
            print("表结构创建或更新完成")
            # 写入数据
            self.writer.write(df, self.table_name)
            print(f"数据已写入表: {self.table_name}")
        except Exception as e:
            error_msg = f"处理文件时发生错误: {str(e)}"
            print(error_msg)
            raise Exception(error_msg)

    def _create_or_update_table(self, df: pd.DataFrame) -> None:
        """创建或更新表结构"""
        if not self.table_manager.table_exists(self.table_name):
            self.table_manager.create_data_table(self.table_name, df, self.private_keys)
        else:
            self.table_manager.update_table_if_needed(self.table_name, df)