# tools_path_config.json 配置文件说明

## 概述
`tools_path_config.json` 是项目的核心配置文件，用于管理数据源、数据库连接、文件路径等配置信息。

## 配置文件结构

### 1. components 组件配置

#### 1.1 data_source 数据源配置
```json
"data_source": {
    "mode": "local"
}
```
- **mode**: 数据源模式
  - `"local"`: 使用本地文件系统作为数据源
  - `"sql"`: 使用SQL数据库作为数据源

#### 1.2 database 数据库配置

##### database1 - 阿里云RDS数据库
```json
"database1": {
    "host": "rm-bp1o6we7s3o1h76x1to.mysql.rds.aliyuncs.com",
    "port": 3306,
    "user": "kai",
    "password": "Abcd1234#",
    "charset": "utf8mb4"
}
```
- **host**: 数据库服务器地址
- **port**: 数据库端口号
- **user**: 数据库用户名
- **password**: 数据库密码
- **charset**: 字符编码格式

##### database2 - 本地数据库
```json
"database2": {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "root",
    "charset": "utf8mb4"
}
```
- **host**: 本地数据库服务器地址
- **port**: 数据库端口号
- **user**: 数据库用户名
- **password**: 数据库密码
- **charset**: 字符编码格式

### 2. main_folder 主文件夹配置

#### 2.1 input_folder - 主要输入文件夹
```json
{
    "folder_type": "input_folder",
    "path": "OneDrive\\Data_prepared",
    "disk": "D"
}
```
- **folder_type**: 文件夹类型标识
- **path**: 文件夹相对路径
- **disk**: 磁盘驱动器盘符

#### 2.2 input_folder2 - 第二个主要输入文件夹
```json
{
    "folder_type": "input_folder2",
    "path": "OneDrive\\Data_Original",
    "disk": "D"
}
```
- **folder_type**: 文件夹类型标识
- **path**: 文件夹相对路径
- **disk**: 磁盘驱动器盘符

### 3. sub_folder 子文件夹配置

#### 3.1 指数相关数据

##### input_indexcomponent - 指数成分股数据
```json
{
    "data_type": "input_indexcomponent",
    "folder_name": "data_index\\index_component",
    "folder_type": "input_folder",
    "sql_sheet": "data_indexcomponent",
    "database": "data_prepared"
}
```

##### index_data - 指数数据
```json
{
    "data_type": "index_data",
    "folder_name": "data_index\\index_data",
    "folder_type": "input_folder",
    "sql_sheet": "data_index",
    "database": "data_prepared"
}
```

##### input_indexreturn_realtime - 实时指数收益率数据
```json
{
    "data_type": "input_indexreturn_realtime",
    "folder_name": "realtime_data\\realtime_data.xlsx",
    "folder_type": "input_folder",
    "sql_sheet": "realtime_data",
    "database": "data_prepared"
}
```

##### input_index_exposure - 指数因子暴露数据
```json
{
    "data_type": "input_index_exposure",
    "folder_name": "data_index\\index_exposure",
    "folder_type": "input_folder",
    "sql_sheet": "data_factorindexexposure",
    "database": "data_prepared"
}
```

#### 3.2 股票相关数据

##### input_stockdata - 股票数据
```json
{
    "data_type": "input_stockdata",
    "folder_name": "data_stock",
    "folder_type": "input_folder",
    "sql_sheet": "data_stock",
    "database": "data_prepared"
}
```

##### input_stockclose_realtime - 实时股票收盘价数据
```json
{
    "data_type": "input_stockclose_realtime",
    "folder_name": "realtime_data\\realtime_data.xlsx",
    "folder_type": "input_folder",
    "sql_sheet": "realtime_data",
    "database": "data_prepared"
}
```

#### 3.3 ETF相关数据

##### input_etfdata - ETF数据
```json
{
    "data_type": "input_etfdata",
    "folder_name": "data_etf",
    "folder_type": "input_folder",
    "sql_sheet": "data_etf",
    "database": "data_prepared"
}
```

##### input_etfdata_realtime - 实时ETF数据
```json
{
    "data_type": "input_etfdata_realtime",
    "folder_name": "realtime_data\\realtime_data.xlsx",
    "folder_type": "input_folder",
    "sql_sheet": "realtime_data",
    "database": "data_prepared"
}
```

#### 3.4 可转债数据

##### input_cbdata - 可转债数据
```json
{
    "data_type": "input_cbdata",
    "folder_name": "data_convertiblebond",
    "folder_type": "input_folder",
    "sql_sheet": "data_convertiblebond",
    "database": "data_prepared"
}
```

#### 3.5 期权相关数据

##### input_optiondata - 期权数据
```json
{
    "data_type": "input_optiondata",
    "folder_name": "data_option",
    "folder_type": "input_folder",
    "sql_sheet": "data_option",
    "database": "data_prepared"
}
```

##### input_optiondata_realtime - 实时期权数据
```json
{
    "data_type": "input_optiondata_realtime",
    "folder_name": "realtime_data\\realtime_data.xlsx",
    "folder_type": "input_folder",
    "sql_sheet": "realtime_data",
    "database": "data_prepared"
}
```

#### 3.6 期货相关数据

##### input_futuredata - 期货数据
```json
{
    "data_type": "input_futuredata",
    "folder_name": "data_future",
    "folder_type": "input_folder",
    "sql_sheet": "data_future",
    "database": "data_prepared"
}
```

##### input_futuredata_realtime - 实时期货数据
```json
{
    "data_type": "input_futuredata_realtime",
    "folder_name": "realtime_data\\realtime_data.xlsx",
    "folder_type": "input_folder",
    "sql_sheet": "realtime_data",
    "database": "data_prepared"
}
```

#### 3.7 其他数据

##### valuation_date - 中国交易日历数据
```json
{
    "data_type": "valuation_date",
    "folder_name": "data_other\\chinese_valuation_date.xlsx",
    "folder_type": "input_folder",
    "sql_sheet": "chinesevaluationdate",
    "database": "data_prepared"
}
```

##### stock_universe_new - 新版股票池数据
```json
{
    "data_type": "stock_universe_new",
    "folder_name": "data_other\\StockUniverse_new.csv",
    "folder_type": "input_folder",
    "sql_sheet": "stockuniverse",
    "database": "data_prepared"
}
```

##### stock_universe_old - 旧版股票池数据
```json
{
    "data_type": "stock_universe_old",
    "folder_name": "data_other\\StockUniverse_old.csv",
    "folder_type": "input_folder",
    "sql_sheet": "stockuniverse",
    "database": "data_prepared"
}
```

##### weeks_lastday - 周最后交易日数据
```json
{
    "data_type": "weeks_lastday",
    "folder_name": "data_other\\weeks_lastday.xlsx",
    "folder_type": "input_folder",
    "sql_sheet": "specialday",
    "database": "data_prepared"
}
```

##### weeks_firstday - 周第一个交易日数据
```json
{
    "data_type": "weeks_firstday",
    "folder_name": "data_other\\weeks_firstday.xlsx",
    "folder_type": "input_folder",
    "sql_sheet": "specialday",
    "database": "data_prepared"
}
```

##### timeseires_indexReturn - 时间序列指数收益率数据
```json
{
    "data_type": "timeseires_indexReturn",
    "folder_name": "data_timeSeries\\index_data\\IndexReturn.csv",
    "folder_type": "input_folder",
    "sql_sheet": "data_index",
    "database": "data_prepared"
}
```

## 子文件夹配置参数说明

每个子文件夹配置都包含以下参数：

- **data_type**: 数据类型标识，用于在代码中识别不同的数据类型
- **folder_name**: 子文件夹名称或文件路径
- **folder_type**: 关联的主文件夹类型，指向main_folder中的配置
- **sql_sheet**: 对应的数据库表名，用于数据存储和查询
- **database**: 目标数据库名称，指定数据存储的目标数据库

## 使用说明

1. **数据源切换**: 通过修改 `data_source.mode` 可以在本地文件和SQL数据库之间切换
2. **数据库配置**: 可以配置多个数据库连接，支持阿里云RDS和本地数据库
3. **文件路径管理**: 通过main_folder和sub_folder的配置，统一管理所有数据文件的路径
4. **数据类型映射**: 每个数据类型都有对应的文件路径和数据库表名，便于数据读写操作

## 注意事项

1. 确保所有配置的路径都存在且可访问
2. 数据库连接信息需要根据实际环境进行调整
3. 文件路径使用Windows格式的反斜杠 `\`
4. 实时数据通常存储在Excel文件中，而历史数据存储在对应的子文件夹中
