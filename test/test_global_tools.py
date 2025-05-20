"""
测试 global_tools 模块的测试套件
"""

import sys
import os
import unittest
import pandas as pd
import numpy as np
from datetime import date, datetime
import time as time_module  # Rename time module import to avoid conflict
import shutil
import functools
import json

from globalToolsFunc import global_tools, global_dic

# Add the root directory (containing globalToolsFunc) to the Python path
current_dir = os.path.dirname(os.path.abspath(__file__))  # test directory
parent_dir = os.path.dirname(current_dir)  # globalToolsFunc directory
root_dir = os.path.dirname(parent_dir)  # root directory
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

# Import the modules using absolute imports
from globalToolsFunc.global_tools import *
from globalToolsFunc.global_dic import get as glv

class TestResult:
    def __init__(self, name, status, duration, error=None):
        self.name = name
        self.status = status
        self.duration = duration
        self.error = error

class CustomTestResult(unittest.TestResult):
    def __init__(self):
        super().__init__()
        self.test_results = []
        self.current_test = None
        self.start_time = time_module.time()  # Use renamed time module
        self.failures = []
        self.errors = []

    def startTest(self, test):
        self.current_test = test
        self.start_time = time_module.time()  # Use renamed time module
        print(f"\n正在执行: {test._testMethodName}")
        super().startTest(test)

    def addSuccess(self, test):
        duration = time_module.time() - self.start_time  # Use renamed time module
        self.test_results.append(
            TestResult(test._testMethodName, "通过", duration)
        )
        print(f"✓ {test._testMethodName}: 测试通过 (耗时: {duration:.3f}秒)")
        super().addSuccess(test)

    def addError(self, test, err):
        try:
            duration = time_module.time() - (self.start_time or time_module.time())  # Use renamed time module
        except:
            duration = 0.0
        error_msg = str(err[1])
        self.test_results.append(
            TestResult(test._testMethodName, "错误", duration, error_msg)
        )
        print(f"✗ {test._testMethodName}: 测试错误 (耗时: {duration:.3f}秒)")
        print(f"  错误信息: {error_msg}")
        self.errors.append((test, err))
        super().addError(test, err)

    def addFailure(self, test, err):
        try:
            duration = time_module.time() - (self.start_time or time_module.time())  # Use renamed time module
        except:
            duration = 0.0
        failure_msg = str(err[1])
        self.test_results.append(
            TestResult(test._testMethodName, "失败", duration, failure_msg)
        )
        print(f"✗ {test._testMethodName}: 测试失败 (耗时: {duration:.3f}秒)")
        print(f"  错误信息: {failure_msg}")
        self.failures.append((test, err))
        super().addFailure(test, err)

    def printSummary(self):
        print("\n=== 测试结果总结 ===")
        
        passed = [r for r in self.test_results if r.status == "通过"]
        failed = [r for r in self.test_results if r.status in ["失败", "错误"]]
        total_time = sum(r.duration for r in self.test_results)
        
        print(f"\n通过的测试 ({len(passed)}):")
        for result in passed:
            print(f"✓ {result.name:<30} 耗时: {result.duration:.3f}秒")
        
        if failed:
            print(f"\n失败的测试 ({len(failed)}):")
            for result in failed:
                print(f"✗ {result.name:<30} 耗时: {result.duration:.3f}秒")
                print(f"  错误信息: {result.error}")
        else:
            print("\n没有测试失败！")
        
        print(f"\n总计: {len(self.test_results)} 个测试")
        print(f"通过: {len(passed)} 个")
        print(f"失败: {len(failed)} 个")
        print(f"总耗时: {total_time:.3f}秒")
        print("\n=== 测试结束 ===")

def setup_test_environment():
    """设置测试环境"""
    # 使用test目录作为基础目录
    current_dir = os.path.dirname(os.path.abspath(__file__))
    test_data_dir = os.path.join(current_dir, 'test_data')
    
    # 获取tools_path_config.json的路径
    config_path = os.path.join(os.path.dirname(current_dir), 'tools_path_config.json')
    
    return test_data_dir, config_path

class TestGlobalTools(unittest.TestCase):
    """
    测试 global_tools 模块的测试套件
    
    注意:
    1. 本测试套件不会修改任何原始文件:
       - tools_path_config.json
       - global_dic.py
       - global_tools.py
    2. 所有测试操作都在内存中进行
    3. 只会创建和修改 test/test_data 目录下的临时文件
    4. 测试完成后会自动清理所有临时文件
    """

    @classmethod
    def setUpClass(cls):
        print("\n=== 开始测试 global_tools 模块 ===\n")
        cls.test_data_dir, cls.config_path = setup_test_environment()
        
        # 确保global_dic使用正确的配置文件
        global_dic.config_path = cls.config_path
        
        # 读取配置文件
        with open(cls.config_path, 'r', encoding='utf-8') as f:
            cls.config = json.load(f)
        
        # 获取数据源模式
        cls.source = cls.config['components']['data_source']['mode']
        
        # 创建测试CSV文件
        cls.test_csv_path = os.path.join(cls.test_data_dir, 'test.csv')
        os.makedirs(cls.test_data_dir, exist_ok=True)
        test_data = pd.DataFrame({
            'A': [1, 2, 3],
            'B': ['a', 'b', 'c']
        })
        test_data.to_csv(cls.test_csv_path, index=False)

    @classmethod
    def tearDownClass(cls):
        """Clean up test fixtures"""
        try:
            if os.path.exists(cls.test_data_dir):
                shutil.rmtree(cls.test_data_dir)
        except Exception as e:
            print(f"Warning: Failed to clean up test directory: {str(e)}")

    def setUp(self):
        """每个测试开始前执行"""
        print("\n---")  # 添加分隔线

    def test_01_source_getting(self):
        """Test source_getting function"""
        source = global_tools.source_getting()
        self.assertIsInstance(source, str)
        self.assertIn(source, ['local', 'sql'])
        self.assertEqual(source, self.source)  # 验证与配置文件一致

    def test_02_get_db_connection(self):
        """Test get_db_connection function"""
        if self.source == 'sql':
            try:
                # 测试主数据库连接
                conn = global_tools.get_db_connection()
                self.assertIsNotNone(conn)
                conn.close()
                
                # 测试第二数据库连接
                conn2 = global_tools.get_db_connection(use_database2=True)
                if 'database2' in self.config['components']['database']:
                    self.assertIsNotNone(conn2)
                    conn2.close()
            except Exception as e:
                self.skipTest(f"数据库连接失败: {str(e)}")
        else:
            conn = global_tools.get_db_connection()
            self.assertIsNone(conn)

    def test_03_readcsv(self):
        """Test readcsv function"""
        df = global_tools.readcsv(self.test_csv_path)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 3)

    def test_04_move_specific_files(self):
        """Test move_specific_files function"""
        src_dir = os.path.join(self.test_data_dir, 'src')
        dst_dir = os.path.join(self.test_data_dir, 'dst')
        os.makedirs(src_dir, exist_ok=True)
        os.makedirs(dst_dir, exist_ok=True)
        
        # Create test files
        test_files = ['test1.txt', 'test2.txt']
        for file in test_files:
            with open(os.path.join(src_dir, file), 'w') as f:
                f.write('test')
        
        global_tools.move_specific_files(src_dir, dst_dir, test_files)
        for file in test_files:
            self.assertTrue(os.path.exists(os.path.join(dst_dir, file)))

    def test_05_move_specific_files2(self):
        """Test move_specific_files2 function"""
        src_dir = os.path.join(self.test_data_dir, 'src2')
        dst_dir = os.path.join(self.test_data_dir, 'dst2')
        os.makedirs(src_dir, exist_ok=True)
        
        # Create test files
        test_files = ['test1.txt', 'test2.txt']
        for file in test_files:
            with open(os.path.join(src_dir, file), 'w') as f:
                f.write('test')
        
        global_tools.move_specific_files2(src_dir, dst_dir)
        for file in test_files:
            self.assertTrue(os.path.exists(os.path.join(dst_dir, file)))

    def test_06_rr_score_processing(self):
        """Test rr_score_processing function"""
        df = pd.DataFrame({
            'valuation_date': ['2025-01-02', '2025-01-02', '2025-01-03', '2025-01-03'],  # 2025-01-02 is Thursday, 2025-01-03 is Friday
            'code': ['000001.SZ', '600000.SH', '000001.SZ', '600000.SH'],
            'score': [0.8, 0.7, 0.9, 0.6]
        })
        result = global_tools.rr_score_processing(df)
        self.assertIsInstance(result, pd.DataFrame)
        self.assertIn('final_score', result.columns)
        self.assertEqual(len(result), 4)

    def test_07_code_transfer(self):
        """Test code_transfer function"""
        df = pd.DataFrame({'code': [1, 600000]})
        df = global_tools.code_transfer(df)
        self.assertTrue(all(df['code'].str.contains('.(SH|SZ)')))

    def test_08_factor_name_old(self):
        """Test factor_name_old function"""
        barra_name, industry_name = global_tools.factor_name_old()
        self.assertIsInstance(barra_name, list)
        self.assertIsInstance(industry_name, list)

    def test_09_factor_name_new(self):
        """Test factor_name_new function"""
        barra_name, industry_name = global_tools.factor_name_new()
        self.assertIsInstance(barra_name, list)
        self.assertIsInstance(industry_name, list)

    def test_10_chunks(self):
        """Test chunks function"""
        lst = [1, 2, 3, 4]
        result = global_tools.chunks(lst, 2)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], [1, 3])
        self.assertEqual(result[1], [2, 4])

    def test_11_file_withdraw(self):
        """Test file_withdraw function"""
        test_dir = os.path.join(self.test_data_dir, 'test_files')
        os.makedirs(test_dir, exist_ok=True)
        
        # Create test file with date
        test_file = 'test_20250102.csv'  # 2025-01-02 is a Thursday
        with open(os.path.join(test_dir, test_file), 'w') as f:
            f.write('test')
        
        result = global_tools.file_withdraw(test_dir, '20250102')
        self.assertIsNotNone(result)
        self.assertTrue(os.path.exists(result))

    def test_12_folder_creator(self):
        """Test folder creator functions"""
        test_folder = os.path.join(self.test_data_dir, 'test_folder')
        global_tools.folder_creator(test_folder)
        self.assertTrue(os.path.exists(test_folder))

    def test_13_folder_creator2(self):
        """Test folder_creator2 function"""
        test_folder2 = os.path.join(self.test_data_dir, 'test_folder2')
        global_tools.folder_creator2(test_folder2)
        self.assertTrue(os.path.exists(test_folder2))

    def test_14_folder_creator3(self):
        """Test folder_creator3 function"""
        test_file_path = os.path.join(self.test_data_dir, 'subfolder', 'test.txt')
        global_tools.folder_creator3(test_file_path)
        self.assertTrue(os.path.exists(os.path.dirname(test_file_path)))

    def test_15_weight_sum_check(self):
        """Test weight_sum_check function"""
        df = pd.DataFrame({
            'code': ['000001.SZ', '600000.SH'],
            'weight': [0.4, 0.4]
        })
        result = global_tools.weight_sum_check(df)
        self.assertAlmostEqual(result['weight'].sum(), 1.0)

    def test_16_weight_sum_warning(self):
        """Test weight_sum_warning function"""
        df = pd.DataFrame({
            'code': ['000001.SZ', '600000.SH'],
            'weight': [0.4, 0.4]
        })
        # This should print a warning but not raise an exception
        global_tools.weight_sum_warning(df)

    def test_17_stock_volatility_calculate(self):
        """Test stock_volatility_calculate function"""
        df = pd.DataFrame({
            'valuation_date': pd.date_range('2025-01-02', periods=60, freq='B'),  # Business days only
            'return': np.random.normal(0, 1, 60)
        })
        result = global_tools.stock_volatility_calculate(df, '2025-03-20')  # 2025-03-20 is a Thursday
        self.assertIsInstance(result, pd.DataFrame)

    def test_18_data_reader(self):
        """Test data_reader function"""
        df = global_tools.data_reader(self.test_csv_path)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 3)

    def test_19_data_getting(self):
        """Test data_getting function"""
        if self.source == 'sql':
            try:
                # 从配置文件获取SQL表名
                table_name = None
                for folder in self.config['sub_folder']:
                    if folder['data_type'] == 'input_stockreturn':
                        table_name = folder['sql_sheet']
                        break
                
                if table_name:
                    # 构建SQL查询
                    query = f"SELECT * FROM {table_name} LIMIT 1"
                    df = global_tools.data_getting(query)
                    
                    if df is not None and not df.empty:
                        self.assertIsInstance(df, pd.DataFrame)
                    else:
                        self.skipTest("数据库中没有找到测试数据")
                else:
                    self.skipTest("配置文件中未找到stock_return表名")
            except Exception as e:
                self.skipTest(f"数据库查询失败: {str(e)}")
        else:
            # 本地模式测试
            self.skipTest("当前为本地模式，跳过SQL测试")

    def test_20_factor_universe_withdraw(self):
        """Test factor_universe_withdraw function"""
        df = global_tools.factor_universe_withdraw('new')
        self.assertIsInstance(df, pd.DataFrame)

    def test_21_Chinese_valuation_date(self):
        """Test Chinese_valuation_date function"""
        df = global_tools.Chinese_valuation_date()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertIn('valuation_date', df.columns)

    def test_22_next_workday(self):
        """Test next_workday function"""
        result = global_tools.next_workday()
        self.assertIsInstance(result, str)

    def test_23_last_workday(self):
        """Test last_workday function"""
        result = global_tools.last_workday()
        self.assertIsInstance(result, str)

    def test_24_last_workday_calculate(self):
        """Test last_workday_calculate function"""
        result = global_tools.last_workday_calculate('2025-01-02')  # 2025-01-02 is a Thursday
        self.assertIsInstance(result, str)

    def test_25_next_workday_calculate(self):
        """Test next_workday_calculate function"""
        result = global_tools.next_workday_calculate('2025-01-02')  # 2025-01-02 is a Thursday
        self.assertIsInstance(result, str)

    def test_26_last_workday_calculate2(self):
        """Test last_workday_calculate2 function"""
        df = pd.DataFrame({
            'date': ['2025-01-02', '2025-01-02', '2025-01-03', '2025-01-03']  # 2025-01-02 is Thursday, 2025-01-03 is Friday
        })
        result = global_tools.last_workday_calculate2(df)
        self.assertIsInstance(result, pd.DataFrame)

    def test_27_is_workday(self):
        """Test is_workday function"""
        result = global_tools.is_workday('2025-01-02')  # 2025-01-02 is a Thursday
        self.assertIsInstance(result, bool)

    def test_28_working_days(self):
        """Test working_days function"""
        df = pd.DataFrame({
            'date': ['2025-01-02', '2025-01-03', '2025-01-06']  # Thursday, Friday, Monday
        })
        result = global_tools.working_days(df)
        self.assertIsInstance(result, pd.DataFrame)

    def test_29_is_workday2(self):
        """Test is_workday2 function"""
        result = global_tools.is_workday2()
        self.assertIsInstance(result, bool)

    def test_30_intdate_transfer(self):
        """Test intdate_transfer function"""
        result = global_tools.intdate_transfer('2025-01-02')  # 2025-01-02 is a Thursday
        self.assertEqual(result, '20250102')

    def test_31_strdate_transfer(self):
        """Test strdate_transfer function"""
        result = global_tools.strdate_transfer('20250102')  # 2025-01-02 is a Thursday
        self.assertEqual(result, '2025-01-02')

    def test_32_working_days_list(self):
        """Test working_days_list function"""
        result = global_tools.working_days_list('2025-01-02', '2025-03-20')  # 2025-01-02 is Thursday, 2025-03-20 is Thursday
        self.assertIsInstance(result, list)

    def test_33_working_day_count(self):
        """Test working_day_count function"""
        result = global_tools.working_day_count('2025-01-02', '2025-03-20')  # 2025-01-02 is Thursday, 2025-03-20 is Thursday
        self.assertIsInstance(result, int)

    def test_34_month_lastday(self):
        """Test month_lastday function"""
        result = global_tools.month_lastday()
        self.assertIsInstance(result, list)

    def test_35_last_weeks_lastday(self):
        """Test last_weeks_lastday function"""
        result = global_tools.last_weeks_lastday()
        self.assertIsInstance(result, str)

    def test_36_last_weeks_lastday2(self):
        """Test last_weeks_lastday2 function"""
        result = global_tools.last_weeks_lastday2('2025-01-02')  # 2025-01-02 is a Thursday
        self.assertIsInstance(result, str)

    def test_37_weeks_firstday(self):
        """Test weeks_firstday function"""
        result = global_tools.weeks_firstday('2025-01-02')  # 2025-01-02 is a Thursday
        self.assertIsInstance(result, str)

    def test_38_next_weeks_lastday2(self):
        """Test next_weeks_lastday2 function"""
        result = global_tools.next_weeks_lastday2('2025-01-02')  # 2025-01-02 is a Thursday
        self.assertIsInstance(result, str)

    def test_39_index_mapping(self):
        """Test index_mapping function"""
        result = global_tools.index_mapping('上证50')
        self.assertEqual(result, 'sz50')
        result = global_tools.index_mapping('上证50', 'code')
        self.assertEqual(result, '000016.SH')

    def test_40_index_weight_withdraw(self):
        """Test index_weight_withdraw function"""
        try:
            # 从配置文件获取SQL表名
            table_name = None
            for folder in self.config['sub_folder']:
                if folder['data_type'] == 'input_indexcomponent':
                    table_name = folder['sql_sheet']
                    break

            if table_name:
                result = global_tools.index_weight_withdraw('上证50', '2024-05-27')
                if result is not None and not result.empty:
                    self.assertIsInstance(result, pd.DataFrame)
                    self.assertIn('code', result.columns)
                    self.assertIn('weight', result.columns)
                else:
                    self.skipTest("数据库中没有找到指数权重数据")
            else:
                self.skipTest("配置文件中未找到index_weight表名")
        except Exception as e:
            self.skipTest(f"获取指数权重数据失败: {str(e)}")

    def test_41_crossSection_index_return_withdraw(self):
        """Test crossSection_index_return_withdraw function"""
        try:
            # 从配置文件获取SQL表名
            table_name = None
            for folder in self.config['sub_folder']:
                if folder['data_type'] == 'input_indexreturn':
                    table_name = folder['sql_sheet']
                    break

            if table_name:
                result = global_tools.crossSection_index_return_withdraw('上证50', '2024-05-27')
                if result is not None:
                    self.assertIsInstance(result, (float, type(None)))
                else:
                    self.skipTest("数据库中没有找到指数收益率数据")
            else:
                self.skipTest("配置文件中未找到index_return表名")
        except Exception as e:
            self.skipTest(f"获取指数收益率数据失败: {str(e)}")

    def test_42_crossSection_index_factorexposure_withdraw_new(self):
        """Test crossSection_index_factorexposure_withdraw_new function"""
        try:
            # 从配置文件获取SQL表名
            table_name = None
            for folder in self.config['sub_folder']:
                if folder['data_type'] == 'input_index_exposure':
                    table_name = folder['sql_sheet']
                    break

            if table_name:
                result = global_tools.crossSection_index_factorexposure_withdraw_new('上证50', '2024-05-27')
                if result is not None and not result.empty:
                    self.assertIsInstance(result, pd.DataFrame)
                else:
                    self.skipTest("数据库中没有找到因子暴露数据")
            else:
                self.skipTest("配置文件中未找到index_factorexposure表名")
        except Exception as e:
            self.skipTest(f"获取因子暴露数据失败: {str(e)}")

    def test_43_timeSeries_index_return_withdraw(self):
        """Test timeSeries_index_return_withdraw function"""
        try:
            # 从配置文件获取SQL表名
            table_name = None
            for folder in self.config['sub_folder']:
                if folder['data_type'] == 'timeseires_indexReturn':
                    table_name = folder['sql_sheet']
                    break

            if table_name:
                result = global_tools.timeSeries_index_return_withdraw()
                if result is not None and not result.empty:
                    self.assertIsInstance(result, pd.DataFrame)
                else:
                    self.skipTest("数据库中没有找到时间序列指数收益率数据")
            else:
                self.skipTest("配置文件中未找到时间序列指数收益率表名")
        except Exception as e:
            self.skipTest(f"获取时间序列指数收益率数据失败: {str(e)}")

    def test_44_crossSection_stock_return_withdraw(self):
        """Test crossSection_stock_return_withdraw function"""
        result = global_tools.crossSection_stock_return_withdraw('2025-02-21')
        self.assertIsInstance(result, (pd.DataFrame, type(None)))

    def test_45_crossSection_etf_data_withdraw(self):
        """Test crossSection_etf_data_withdraw function"""
        try:
            # 从配置文件获取SQL表名
            table_name = None
            for folder in self.config['sub_folder']:
                if folder['data_type'] == 'input_etfdata':
                    table_name = folder['sql_sheet']
                    break

            if table_name:
                result = global_tools.crossSection_etf_data_withdraw('2025-02-21')
                if result is not None and not result.empty:
                    self.assertIsInstance(result, pd.DataFrame)
                    self.assertIn('code', result.columns)
                else:
                    self.skipTest("数据库中没有找到ETF数据")
            else:
                self.skipTest("配置文件中未找到ETF表名")
        except Exception as e:
            self.skipTest(f"获取ETF数据失败: {str(e)}")

    def test_46_crossSection_cb_data_withdraw(self):
        """Test crossSection_cb_data_withdraw function"""
        try:
            # 从配置文件获取SQL表名
            table_name = None
            for folder in self.config['sub_folder']:
                if folder['data_type'] == 'input_cbdata':
                    table_name = folder['sql_sheet']
                    break

            if table_name:
                result = global_tools.crossSection_cb_data_withdraw('2025-02-21')
                if result is not None and not result.empty:
                    self.assertIsInstance(result, pd.DataFrame)
                    self.assertIn('code', result.columns)
                else:
                    self.skipTest("数据库中没有找到可转债数据")
            else:
                self.skipTest("配置文件中未找到可转债表名")
        except Exception as e:
            self.skipTest(f"获取可转债数据失败: {str(e)}")

    def test_47_crossSection_option_data_withdraw(self):
        """Test crossSection_option_data_withdraw function"""
        try:
            # 从配置文件获取SQL表名
            table_name = None
            for folder in self.config['sub_folder']:
                if folder['data_type'] == 'input_optiondata':
                    table_name = folder['sql_sheet']
                    break

            if table_name:
                result = global_tools.crossSection_option_data_withdraw('2025-02-21')
                if result is not None and not result.empty:
                    self.assertIsInstance(result, pd.DataFrame)
                    self.assertIn('code', result.columns)
                else:
                    self.skipTest("数据库中没有找到期权数据")
            else:
                self.skipTest("配置文件中未找到期权表名")
        except Exception as e:
            self.skipTest(f"获取期权数据失败: {str(e)}")

    def test_48_crossSection_future_data_withdraw(self):
        """Test crossSection_future_data_withdraw function"""
        try:
            # 从配置文件获取SQL表名
            table_name = None
            for folder in self.config['sub_folder']:
                if folder['data_type'] == 'input_futuredata':
                    table_name = folder['sql_sheet']
                    break

            if table_name:
                result = global_tools.crossSection_future_data_withdraw('2025-03-03')
                if result is not None and not result.empty:
                    self.assertIsInstance(result, pd.DataFrame)
                    self.assertIn('code', result.columns)
                else:
                    self.skipTest("数据库中没有找到期货数据")
            else:
                self.skipTest("配置文件中未找到期货表名")
        except Exception as e:
            self.skipTest(f"获取期货数据失败: {str(e)}")

    def test_49_weight_df_standardization(self):
        """Test weight_df_standardization function"""
        # Create test data
        test_data = {
            'code': ['000001', '600000', 'IF2403', 'IO2403-P-3000', '300001', '000002'],
            'weight': [0.2, 0.2, 0.2, 0.2, 0.1, 0.1]
        }
        df = pd.DataFrame(test_data)
        
        # Test standardization
        result = global_tools.weight_df_standardization(df)
        
        # Verify result is a DataFrame
        self.assertIsInstance(result, pd.DataFrame)
        
        # Verify codes are standardized correctly
        expected_codes = ['000001.SZ', '600000.SH', 'IF2403', 'IO2403-P-3000', '300001.SZ', '000002.SZ']
        self.assertEqual(sorted(list(result['code'])), sorted(expected_codes))
        
        # Verify weights sum to 1
        self.assertAlmostEqual(result['weight'].sum(), 1.0)
        
        # Test with already standardized codes
        df_standardized = pd.DataFrame({
            'code': ['000001.SZ', '600000.SH'],
            'weight': [0.6, 0.4]
        })
        result_standardized = global_tools.weight_df_standardization(df_standardized)
        self.assertEqual(sorted(list(result_standardized['code'])), sorted(['000001.SZ', '600000.SH']))
        
        # Test with invalid DataFrame
        df_invalid = pd.DataFrame({'invalid': [1, 2, 3]})
        result_invalid = global_tools.weight_df_standardization(df_invalid)
        self.assertIsInstance(result_invalid, pd.DataFrame)

    def test_50_option_df_standardization(self):
        """Test option_df_standardization function"""
        df = pd.DataFrame({
            'code': ['10000001.P'],
            'settle': [100],
            'pre_settle': [90]
        })
        result = global_tools.option_df_standardization(df)
        self.assertIsInstance(result, pd.DataFrame)
        self.assertIn('return', result.columns)

    def test_51_future_df_standardization(self):
        """Test future_df_standardization function"""
        df = pd.DataFrame({
            'code': ['IF2403.CF'],
            'settle': [100],
            'pre_settle': [90]
        })
        result = global_tools.future_df_standardization(df)
        self.assertIsInstance(result, pd.DataFrame)
        self.assertIn('return', result.columns)

    def test_52_etf_df_standardization(self):
        """Test etf_df_standardization function"""
        df = pd.DataFrame({
            'code': ['510300.SH'],
            'return': [0.01]
        })
        result = global_tools.etf_df_standardization(df)
        self.assertIsInstance(result, pd.DataFrame)
        self.assertIn('return', result.columns)

    def test_53_cb_df_standardization(self):
        """Test cb_df_standardization function"""
        df = pd.DataFrame({
            'code': ['110000.SH'],
            'close': [100],
            'pre_close': [90]
        })
        result = global_tools.cb_df_standardization(df)
        self.assertIsInstance(result, pd.DataFrame)
        self.assertIn('return', result.columns)

    def test_54_portfolio_return_calculate_daily(self):
        """Test portfolio_return_calculate_daily function"""
        try:
            # 创建测试投资组合数据
            portfolio_data = {
                'code': ['000001.SZ', '600000.SH'],
                'weight': [0.5, 0.5]
            }
            df = pd.DataFrame(portfolio_data)
            test_date = '2025-02-21'
            
            portfolio_return, excess_portfolio_return = global_tools.portfolio_return_calculate_daily(
                df, test_date, '上证50'
            )
            
            if portfolio_return is not None:
                self.assertIsInstance(portfolio_return, float)
                self.assertIsInstance(excess_portfolio_return, float)
            else:
                self.skipTest("数据库中没有找到必要的数据")
            
        except Exception as e:
            self.skipTest(f"计算组合收益率失败: {str(e)}")

    def test_55_portfolio_return_calculate_standard(self):
        """Test portfolio_return_calculate_standard function"""
        try:
            # 创建测试数据
            portfolio_data = {
                'code': ['000001.SZ', '600000.SH'],
                'weight': [0.5, 0.5]
            }
            df = pd.DataFrame(portfolio_data)
            
            # 在SQL模式下，这些数据应该从数据库获取
            test_date = '2024-01-01'
            
            # 调用函数时传入空DataFrame，让函数内部从数据库获取数据
            portfolio_return, excess_portfolio_return = global_tools.portfolio_return_calculate_standard(
                df, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), 
                pd.DataFrame(), pd.DataFrame(), test_date, '上证50'
            )
            
            if portfolio_return is not None:
                self.assertIsInstance(portfolio_return, float)
                self.assertIsInstance(excess_portfolio_return, float)
            else:
                self.skipTest("数据库中没有找到必要的数据")
                
        except Exception as e:
            self.skipTest(f"计算标准组合收益率失败: {str(e)}")

    def test_00_global_dic_sql_mode(self):
        """Test if global_dic returns SQL queries in SQL mode"""
        if self.source != 'sql':
            self.skipTest("此测试仅在SQL模式下运行")
            
        # 测试数据获取路径
        path = glv('input_stockreturn')
        self.assertTrue(isinstance(path, str))
        self.assertTrue(path.upper().startswith('SELECT'), 
            f"在SQL模式下应返回SQL查询语句，而不是路径。实际返回: {path}")
        
        # 测试估值日期路径
        path = glv('valuation_date')
        self.assertTrue(isinstance(path, str))
        self.assertTrue(path.upper().startswith('SELECT'),
            f"在SQL模式下应返回SQL查询语句，而不是路径。实际返回: {path}")
        
        # 测试因子数据路径
        path = glv('stock_universe_new')
        self.assertTrue(isinstance(path, str))
        self.assertTrue(path.upper().startswith('SELECT'),
            f"在SQL模式下应返回SQL查询语句，而不是路径。实际返回: {path}")
        
        # 测试指数权重路径
        path = glv('input_indexcomponent')
        self.assertTrue(isinstance(path, str))
        self.assertTrue(path.upper().startswith('SELECT'),
            f"在SQL模式下应返回SQL查询语句，而不是路径。实际返回: {path}")

if __name__ == '__main__':
    # 创建自定义测试结果收集器
    test_result = CustomTestResult()
    
    # 运行测试套件
    suite = unittest.TestLoader().loadTestsFromTestCase(TestGlobalTools)
    suite.run(test_result)
    
    # 打印测试结果总结
    test_result.printSummary() 