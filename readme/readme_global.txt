global_tools.py 使用说明

1. 简介
global_tools.py 是一个用于金融数据处理和投资组合计算的工具包，提供了丰富的功能用于处理股票、期货、期权、ETF等金融数据，以及进行投资组合收益计算。

2. 主要功能
   - 数据读取和标准化
   - 日期处理和工作日计算
   - 指数数据处理
   - 证券数据处理
   - 期权和期货数据处理
   - 投资组合收益计算

3. 依赖包
   - pandas
   - numpy
   - scipy
   - pymysql
   - warnings
   - json
   - os
   - datetime
   - shutil

4. 配置说明
   代码使用配置文件 'config_path/tools_path_config.json' 进行配置，包含以下主要设置：
   - 数据源模式（local/SQL）
   - 数据库连接信息
   - 文件路径配置

5. 使用示例

   a. 读取数据
   ```python
   from global_tools import data_getting
   
   # 读取本地数据
   df = data_getting('path/to/your/file.csv')
   
   # 从数据库读取数据
   df = data_getting('SELECT * FROM your_table')
   ```

   b. 日期处理
   ```python
   from global_tools import next_workday, last_workday
   
   # 获取下一个工作日
   next_day = next_workday()
   
   # 获取上一个工作日
   last_day = last_workday()
   ```

   c. 投资组合计算
   ```python
   from global_tools import portfolio_return_calculate_daily
   
   # 计算投资组合收益
   portfolio_return, excess_return = portfolio_return_calculate_daily(
       df, 
       target_date='2024-03-25',
       index_type='沪深300'
   )
   ```

6. 注意事项
   - 确保配置文件正确设置
   - 使用数据库模式时需要正确配置数据库连接信息
   - 日期格式统一使用 'YYYY-MM-DD'
   - 代码格式标准化处理支持多种金融产品代码

7. 错误处理
   - 文件不存在时会返回空DataFrame
   - 数据库连接失败时会打印错误信息
   - 权重和异常时会发出警告

8. 维护说明
   - 定期检查配置文件的有效性
   - 确保数据源路径正确
   - 及时更新数据库连接信息

9. 版本历史
   - v1.0: 初始版本
   - v1.1: 添加数据库支持
   - v1.2: 优化代码结构和错误处理

10. 联系方式
    如有问题或建议，请联系技术支持。 