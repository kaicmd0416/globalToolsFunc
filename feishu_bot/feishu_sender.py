#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
飞书消息发送模块
用于发送数据质量检查报告到飞书
"""

import os
import json
import requests
from dotenv import load_dotenv
import sys
path = os.getenv('GLOBAL_TOOLSFUNC')
sys.path.append(path)
import global_tools as gt
# 加载.env文件中的配置
# load_dotenv()

class FeishuSender:
    def __init__(self,webhook_url):
        # self.webhook_url = os.getenv('FEISHU_WEBHOOK_URL')
        self.webhook_url = webhook_url

        if not self.webhook_url:
            raise ValueError("未配置飞书 Webhook URL，请在 .env 文件中设置 FEISHU_WEBHOOK_URL")
        
        # 打印配置信息
        print("\n=== 配置信息 ===")
        print(f"Webhook URL: {self.webhook_url}")
        print("===============\n")

    def send_message(self, content):
        """
        发送消息到飞书
        
        Args:
            content (str): 要发送的消息内容
        """
        try:
            # 构建消息内容
            message = {
                "msg_type": "text",
                "content": {
                    "text": content
                }
            }
            
            print("\n=== 请求信息 ===")
            print(f"请求体: {json.dumps(message, ensure_ascii=False)}")
            
            # 发送请求
            headers = {
                'Content-Type': 'application/json'
            }
            
            response = requests.post(
                self.webhook_url,
                headers=headers,
                json=message
            )

            # 检查响应
            print("\n=== 响应信息 ===")
            print(f"状态码: {response.status_code}")
            print(f"响应内容: {response.text}")
            
            if response.status_code == 200:
                result = response.json()
                if result.get('code') == 0:
                    print("消息发送成功")
                else:
                    print(f"消息发送失败: {result.get('msg')}")
            else:
                print(f"消息发送失败: HTTP {response.status_code}")
                print(f"Response: {response.text}")

        except Exception as e:
            print(f"发送消息时发生错误: {str(e)}")
            import traceback
            print(traceback.format_exc())

if __name__ == "__main__":
    # 测试发送
    sender = FeishuSender('https://open.feishu.cn/open-apis/bot/v2/hook/43599c85-25fc-486c-a00d-0525687dabb9')
    valuation_date = gt.data_reader("D:\windsurf_project\SQLCheck\data\combine_20250805.csv")
    valuation_date = valuation_date.to_string(index=False)
    sender.send_message(str(valuation_date))


    pass