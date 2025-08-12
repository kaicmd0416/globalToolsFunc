
import logging
from io import StringIO
from contextlib import contextmanager
import sys
import os
path = os.getenv('GLOBAL_TOOLSFUNC')
sys.path.append(path)
import global_tools as gt

class PrintCapture:
    """捕获标准输出并转发到日志系统的工具类"""

    def __init__(self, logger: logging.Logger, log_level: int = logging.INFO):
        """
        初始化捕获器

        :param logger: 日志记录器实例
        :param log_level: 转换后的日志级别，默认INFO
        """
        self.logger = logger
        self.log_level = log_level
        self.original_stdout = sys.stdout

    def write(self, message: str):
        """重定向输出到日志"""
        # 过滤空消息（避免print()产生的空行）
        if message.strip():
            self.logger.log(self.log_level, message.strip())
        # 同时保留原始输出（可选）
        self.original_stdout.write(message)

    def flush(self):
        """刷新输出缓冲区"""
        self.original_stdout.flush()


@contextmanager
def capture_prints_to_log(logger: logging.Logger, log_level: int = logging.INFO):
    """
    上下文管理器：在上下文范围内捕获print输出并转为日志

    :param logger: 日志记录器实例
    :param log_level: 转换后的日志级别
    """
    # 保存原始stdout
    original_stdout = sys.stdout
    try:
        # 替换stdout为我们的捕获器
        sys.stdout = PrintCapture(logger, log_level)
        yield
    finally:
        # 恢复原始stdout
        sys.stdout = original_stdout


# 使用示例
if __name__ == "__main__":
    from logger_utils import get_logger  # 导入之前定义的日志工具

    # 创建日志实例
    logger = get_logger("capture_example", "logs/capture.log")

    # 普通print不会被捕获
    print("这条消息不会被捕获到日志")

    # 在上下文管理器中调用通用模块
    with capture_prints_to_log(logger):
        print("这条消息会被捕获为日志")


        # 模拟调用外部通用模块
        gt.data_reader("D:\windsurf_project\SQLCheck\data\combine_20250806.csv")


        # external_module_function()

    # 上下文之外的print恢复正常
    print("这条消息也不会被捕获到日志")
