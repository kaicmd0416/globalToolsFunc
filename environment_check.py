"""
environment_check.py
环境检查工具包
提供Python环境检查和依赖安装功能
"""

import os
import sys
import subprocess

def check_environment():
    """
    检查环境并安装必要的依赖
    
    Returns:
        bool: 环境检查是否通过
    """
    try:
        # 获取当前文件所在目录
        current_dir = os.path.dirname(os.path.abspath(__file__))
        requirements_path = os.path.join(current_dir, 'requirements.txt')
        
        if not os.path.exists(requirements_path):
            print("错误: 未找到 requirements.txt 文件")
            return False
            
        # 检查是否安装了pip
        try:
            import pip
        except ImportError:
            print("错误: 未安装pip，请先安装pip")
            return False
            
        # 检查并安装依赖
        print("正在检查环境依赖...")
        try:
            # 首先尝试使用 --user 选项安装
            subprocess.check_call([sys.executable, "-m", "pip", "install", "--user", "-r", requirements_path])
        except subprocess.CalledProcessError:
            try:
                # 如果 --user 安装失败，尝试不使用 --user 选项
                subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", requirements_path])
            except subprocess.CalledProcessError as e:
                print(f"安装依赖失败: {str(e)}")
                return False
                
        print("环境检查完成，所有依赖已安装")
        return True
        
    except Exception as e:
        print(f"环境检查失败: {str(e)}")
        return False

if __name__ == "__main__":
    print("开始环境检查...")
    if check_environment():
        print("环境检查通过！")
    else:
        print("环境检查失败！")
        sys.exit(1) 