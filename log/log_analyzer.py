import re
import os
from collections import defaultdict
from datetime import datetime
from dotenv import load_dotenv
from feishu_bot.feishu_sender import FeishuSender

# 加载.env文件中的配置
# load_dotenv()
#
# sender = FeishuSender(os.getenv('FEISHU_WEBHOOK_URL'))

class LogAnalyzer:
    """日志分析工具类，用于解析和分析logger_utils.py生成的日志文件"""

    # 日志行的正则表达式模式
    LOG_PATTERN = r'^(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) - (?P<level>[A-Z]+) - (?P<module>[\w_]+):(?P<line>\d+) - \[RUN:(?P<run_id>\w+)\] - (?P<message>.*)$'

    def __init__(self, log_file_path,FEISHU_WEBHOOK_URL):
        """
        初始化日志分析器

        :param log_file_path: 日志文件路径
        """
        self.log_file_path = log_file_path
        self.logs = []
        self.pattern = re.compile(self.LOG_PATTERN)
        if not self.parse_file():  # 初始化时自动解析日志文件
            print(f"警告: 无法解析日志文件 {self.log_file_path}")
        self.sender = FeishuSender(FEISHU_WEBHOOK_URL)

    def parse_file(self):
        """解析指定的日志文件"""
        if not os.path.exists(self.log_file_path):
            print(f"错误: 文件 {self.log_file_path} 不存在")
            return False

        self.logs = []
        current_log = None

        with open(self.log_file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                # 尝试匹配标准日志行
                match = self.pattern.match(line)
                if match:
                    # 如果有未完成的日志，先添加到列表
                    if current_log:
                        self.logs.append(current_log)

                    # 创建新的日志条目
                    timestamp_str = match.group('timestamp')
                    timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')

                    current_log = {
                        'timestamp': timestamp,
                        'level': match.group('level'),
                        'module': match.group('module'),
                        'line': int(match.group('line')),
                        'run_id': match.group('run_id'),
                        'message': match.group('message'),
                        'exception': None
                    }
                elif current_log and line.startswith('Traceback'):
                    # 开始捕获异常信息
                    current_log['exception'] = [line]
                elif current_log and current_log['exception'] is not None:
                    # 继续捕获异常信息
                    current_log['exception'].append(line)

            # 添加最后一个日志条目
            if current_log:
                self.logs.append(current_log)

        return True

    def get_latest_run_logs(self):
        """获取最新运行的所有日志

        Returns:
            list: 最新运行的日志列表
        """
        if not self.logs:
            return []
        
        # 按运行ID分组
        runs = defaultdict(list)
        for log in self.logs:
            runs[log['run_id']].append(log)
        
        # 找出最新的运行ID（基于该运行的第一条日志时间）
        latest_run_id = None
        latest_run_time = None
        for run_id, run_logs in runs.items():
            # 获取该运行的第一条日志时间
            run_time = min(log['timestamp'] for log in run_logs)
            if latest_run_time is None or run_time > latest_run_time:
                latest_run_time = run_time
                latest_run_id = run_id
        
        return runs[latest_run_id] if latest_run_id else []

    def _convert_to_str(self, data):
        """
        递归将数据结构中的所有非字符串类型转换为字符串

        Args:
            data: 任意数据结构

        Returns:
            转换后的数据集
        """
        if isinstance(data, dict):
            return {k: self._convert_to_str(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._convert_to_str(item) for item in data]
        elif isinstance(data, datetime):
            return data.strftime('%Y-%m-%d %H:%M:%S')
        elif not isinstance(data, str):
            return str(data)
        return data

    def get_formatted_latest_run_logs(self):
        """获取最新运行的所有日志并转换为可序列化的字符串格式

        Returns:
            list: 最新运行的日志列表（所有值均为字符串类型）
        """
        latest_run_logs = self.get_latest_run_logs()
        return self._convert_to_str(latest_run_logs)

    def send_feishu_notification(self, project_name):
        """
        发送飞书通知，包含项目名称、运行结果和异常信息

        Args:
            project_name: 项目名称
        """
        # 获取最新运行的所有日志
        latest_run_logs = self.get_formatted_latest_run_logs()
        
        # 定义日志级别优先级映射
        log_level_priority = {
            'DEBUG': 1,
            'INFO': 2,
            'WARNING': 3,
            'ERROR': 4,
            'CRITICAL': 5
        }
        
        # 判断运行结果是否正常
        is_normal = True
        error_logs = []
        
        for log in latest_run_logs:
            # 检查是否有高于WARNING级别的日志或有效的异常信息
            log_level = log.get('level', '')
            level_priority = log_level_priority.get(log_level, 0)
            exception = log.get('exception')
            
            # 只有当日志级别高于WARNING或者异常信息存在且不为None/空时，才视为异常
            has_valid_exception = exception is not None and exception != '' and exception != 'None'
            
            if level_priority > log_level_priority['WARNING'] or has_valid_exception:
                is_normal = False
                error_logs.append(log)
        
        # 生成飞书消息
        message = f"项目名称：{project_name}\n"
        message += f"运行结果：{'正常' if is_normal else '异常'}\n"
        
        if not is_normal:
            message += f"异常数量：{len(error_logs)}\n"
            message += "异常信息：\n"
            for i, error_log in enumerate(error_logs, 1):
                message += f"{i}. [{error_log['timestamp']}] [{error_log['level']}] {error_log['message']}\n"
                exception = error_log.get('exception')
                if exception and exception != 'None':
                    # 安全地处理异常信息
                    if isinstance(exception, list):
                        formatted_exception = '\n   '.join(str(line) for line in exception)
                        message += f"   异常堆栈：{formatted_exception}\n"
                    else:
                        message += f"   异常堆栈：{str(exception)}\n"
        
        # 发送消息
        self.sender.send_message(message)
        return message


# 保留命令行接口，便于直接使用
if __name__ == "__main__":
    # 项目名称（用户可自定义）
    PROJECT_NAME = "测试"  # 请替换为实际项目名称
    
    # 初始化类实例，传入日志文件路径
    log_analyzer = LogAnalyzer('D:\windsurf_project\log\logs\example.log')

    # 获取最新运行的所有日志（已转换为可序列化格式）
    latest_run_logs = log_analyzer.get_formatted_latest_run_logs()
    print('获取最新运行的所有日志')
    print(latest_run_logs)

    # 发送飞书通知
    notification_message = log_analyzer.send_feishu_notification(PROJECT_NAME)
    print('飞书通知已发送，内容如下：')
    print(notification_message)