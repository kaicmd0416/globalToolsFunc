# MySQL

## 主要功能
- 创建表
- 输出df到数据库，并按照主键更新数据

## 使用示例

### 初始化数据库类
```python
import sys
import os
path = os.getenv('GLOBAL_TOOLSFUNC')
sys.path.append(path)
import global_tools as gt

sm=gt.sqlSaving_main(config_path,task_name,delete)
#config_path 配置文件路径
#task_name 任务名字
#delete 是否先删数据再更新 （默认是False）
sm.df_to_sql(df, delete_name, delet_key)
# delete_name和delet_key是默认值为None
# delete_name表示表中的列名
# delet_key表示列名中的元素值


场景一
delete,delete_name和delet_key都不填写,只用默认值
正常更新数据,逻辑为replace,不会进行删除

场景二
delete为True,delete_name和delet_key为None
删除你输入df的所有天数的数据

场景三
delete为True,delete_name和delet_key为对应的列名和单元值
先对表中符合列名和单元值的数据进行删除,再更新数据

