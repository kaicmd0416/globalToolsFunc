import logging
import os
import logging
from logging.handlers import RotatingFileHandler
import time
import uuid
from typing import Optional


class StandardLogger:
    """标准化日志工具类，提供统一的日志输出格式和便捷的日志操作接口"""

    def __init__(self,
                 logger_name: str = __name__,
                 log_file: Optional[str] = None,
                 log_level: int = logging.INFO,
                 max_bytes: int = 10 * 1024 * 1024,  # 10MB
                 backup_count: int = 5):
        """
        初始化日志工具

        :param logger_name: 日志名称，通常使用模块名
        :param log_file: 日志文件路径，None则不写入文件
        :param log_level: 日志级别，默认INFO
        :param max_bytes: 单个日志文件最大大小
        :param backup_count: 日志文件备份数量
        """
        # 创建logger
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(log_level)
        self.logger_name = logger_name
        self.run_id = str(uuid.uuid4())[:8]  # 生成短UUID作为运行标识
        self.start_time = None

        # 避免重复添加处理器
        if self.logger.handlers:
            return

        # 定义日志格式
        # 格式包含：时间、日志级别、模块名、行号、消息内容
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(module)s:%(lineno)d - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # 添加控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

        # 添加文件处理器（如果指定了日志文件）
        if log_file:
            # 确保日志目录存在
            log_dir = os.path.dirname(log_file)
            print(f"日志目录: {log_dir}")
            if log_dir and not os.path.exists(log_dir):
                print(f"创建日志目录: {log_dir}")
                os.makedirs(log_dir, exist_ok=True)
            else:
                print(f"日志目录已存在: {log_dir}")

            # 使用RotatingFileHandler，支持日志轮转
            file_handler = RotatingFileHandler(
                log_file,
                maxBytes=max_bytes,
                backupCount=backup_count,
                encoding='utf-8'
            )
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)

        # 使用LoggerAdapter添加run_id属性
        class RunIdAdapter(logging.LoggerAdapter):
            def process(self, msg, kwargs):
                return f'[RUN:{self.extra["run_id"]}] - {msg}', kwargs

        self.logger = RunIdAdapter(self.logger, {'run_id': self.run_id})

    def start(self, message: str = "程序开始运行"):
        """
        记录程序开始运行的日志，并标记开始时间

        :param message: 开始运行的消息
        :return: 开始时间戳
        """
        self.start_time = datetime.now()
        start_time_str = self.start_time.strftime('%Y-%m-%d %H:%M:%S')
        self.logger.info(f"{message} - 开始时间: {start_time_str} - 运行ID: {self.run_id}")
        return self.start_time

    def end(self, message: str = "程序运行结束"):
        """
        记录程序运行结束的日志，并计算运行时间

        :param message: 结束运行的消息
        :return: 结束时间戳
        """
        end_time = datetime.now()
        end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
        if self.start_time:
            duration = end_time - self.start_time
            self.logger.info(f"{message} - 结束时间: {end_time_str} - 运行ID: {self.run_id} - 运行时长: {duration}")
        else:
            self.logger.info(f"{message} - 结束时间: {end_time_str} - 运行ID: {self.run_id}")
        return end_time

    def debug(self, message: str):
        """输出DEBUG级别的日志"""
        self.logger.debug(message)

    def info(self, message: str):
        """输出INFO级别的日志"""
        self.logger.info(message)

    def warning(self, message: str):
        """输出WARNING级别的日志"""
        self.logger.warning(message)

    def error(self, message: str, exc_info: bool = False):
        """
        输出ERROR级别的日志

        :param message: 日志消息
        :param exc_info: 是否记录异常信息，默认为False
        """
        self.logger.error(message, exc_info=exc_info)

    def critical(self, message: str, exc_info: bool = False):
        """
        输出CRITICAL级别的日志

        :param message: 日志消息
        :param exc_info: 是否记录异常信息，默认为False
        """
        self.logger.critical(message, exc_info=exc_info)

    def log(self, level: int, message: str, exc_info: bool = False):
        """
        输出指定级别的日志

        :param level: 日志级别
        :param message: 日志消息
        :param exc_info: 是否记录异常信息，默认为False
        """
        self.logger.log(level, message, exc_info=exc_info)


from datetime import datetime

# 提供一个便捷的方式获取默认日志实例
def get_logger(logger_name: str = __name__,
               log_file: Optional[str] = None,
               log_level: int = logging.INFO) -> StandardLogger:
    """
    获取日志实例

    :param logger_name: 日志名称
    :param log_file: 日志文件路径
    :param log_level: 日志级别
    :return: 日志工具实例
    """
    return StandardLogger(logger_name, log_file, log_level)


# 示例用法
if __name__ == "__main__":

    # 创建日志实例，日志文件将保存在当前目录的logs文件夹下
    logger = get_logger(
        logger_name="example_logger",
        log_file="logs/example.log",
        log_level=logging.DEBUG # CRITICAL ERROR WARNING INFO DEBUG

    )


    # 输出不同级别的日志
    logger.debug("这是一条调试信息")
    logger.info("这是一条普通信息")
    logger.warning("这是一条警告信息")



    try:
        1 / 0
    except Exception:
        logger.error("这是一条错误信息，不包含堆栈", exc_info=False)
        logger.error("这是一条错误信息，包含堆栈", exc_info=True)
        logger.critical("这是一条严重错误信息，包含堆栈", exc_info=True)
