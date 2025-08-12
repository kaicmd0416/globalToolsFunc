# globalToolsFunc

## 项目简介
`globalToolsFunc` 是一个功能全面的金融工具函数库，主要用于金融数据处理、投资组合计算、日期处理以及各类金融产品（股票、期货、期权、ETF等）的数据获取与分析。

## 主要功能
- 金融数据读取与处理（支持本地文件和数据库）
- 日期与工作日计算
- 指数数据处理
- 证券数据处理
- 期权和期货数据处理
- 投资组合收益计算
- 回测分析报告生成

## 目录
- [utils.py 函数](#utils-py-函数)
- [time_utils.py 函数](#time_utils-py-函数)
- [global_tools.py 函数](#global_tools-py-函数)
- [依赖包](#依赖包)
- [配置说明](#配置说明)
- [使用示例](#使用示例)
- [注意事项](#注意事项)
- [版本历史](#版本历史)

## 函数详细说明

### utils.py 函数

#### source_getting
**函数名**: source_getting
**输入**: 无
**输出**: str - 数据源模式（'local' 或 'sql'）
**说明**: 获取数据源配置，从tools_path_config.json文件中读取数据源模式

#### source_getting2
**函数名**: source_getting2
**输入**: config_path (str) - 配置文件路径
**输出**: str - 数据源模式（'local' 或 'sql'）
**说明**: 从指定配置文件路径获取数据源配置

#### get_db_connection
**函数名**: get_db_connection
**输入**:
- config_path (str, optional) - 配置文件路径
- use_database2 (bool, optional) - 是否使用第二个数据库
- use_database3 (bool, optional) - 是否使用第三个数据库
- max_retries (int, optional) - 最大重试次数
**输出**: pymysql.connections.Connection - 数据库连接对象
**说明**: 获取数据库连接，使用连接池管理，支持多数据库切换

#### close_all_connections
**函数名**: close_all_connections
**输入**: 无
**输出**: 无
**说明**: 关闭所有数据库连接池

#### data_reader
**函数名**: data_reader
**输入**:
- filepath (str) - 文件路径
- dtype (dict, optional) - 数据类型
- index_col (int, optional) - 索引列
- sheet_name (str, optional) - Excel工作表名称
**输出**: pandas.DataFrame - 读取的数据框
**说明**: 读取数据文件，支持CSV、Excel和MAT格式

// ... 其他utils.py函数 ...

### time_utils.py 函数

#### Chinese_valuation_date
**函数名**: Chinese_valuation_date
**输入**: 无
**输出**: pandas.DataFrame - 交易日期数据
**说明**: 获取中国交易日期数据

#### next_workday_auto
**函数名**: next_workday_auto
**输入**: 无
**输出**: str - 下一个工作日
**说明**: 获取下一个工作日

#### last_workday_auto
**函数名**: last_workday_auto
**输入**: 无
**输出**: str - 上一个工作日
**说明**: 获取上一个工作日

// ... 其他time_utils.py函数 ...

### global_tools.py 函数

#### sql_to_timeseries
**函数名**: sql_to_timeseries
**输入**: 未完全显示
**输出**: 未完全显示
**说明**: 未完全显示

#### rank_score_processing
**函数名**: rank_score_processing
**输入**: 未完全显示
**输出**: 未完全显示
**说明**: 未完全显示

// ... 其他global_tools.py函数 ...

## 依赖包
- pandas
- numpy
- scipy
- pymysql
- warnings
- json
- os
- datetime
- shutil
- docx (用于生成文档)

## 配置说明
代码使用配置文件 'tools_path_config.json' 进行配置，包含以下主要设置：
- 数据源模式（local/SQL）
- 数据库连接信息
- 文件路径配置

## 使用示例

### 读取日期数据
```python
import sys
import os
path = os.getenv('GLOBAL_TOOLSFUNC')
sys.path.append(path)
import global_tools as gt

gt.Chinese_valuation_date()
```


## 注意事项
- 确保配置文件正确设置
- 使用数据库模式时需要正确配置数据库连接信息
- 日期格式统一使用 'YYYY-MM-DD'
- 代码格式标准化处理支持多种金融产品代码

## 版本历史
- v1.0: 初始版本
- v1.1: 添加数据库支持
- v1.2: 优化代码结构和错误处理
- v1.3: 完善函数注释和文档