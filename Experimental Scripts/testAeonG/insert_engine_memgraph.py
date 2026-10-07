#!/usr/bin/env python3
# insert_engine_memgraph.py [scale]

import sys
import time
import subprocess
import os
from datetime import datetime
import mgclient

def create_constraint_with_retry(host='localhost', port=7687, max_retries=10, retry_delay=5):
    """创建唯一性约束，失败时重试"""
    for i in range(1, max_retries + 1):
        try:
            print(f"尝试创建约束 (第 {i} 次尝试)...")
            
            # 连接 Memgraph
            conn = mgclient.connect(
                host=host,
                port=port,
                username='',
                password=''
            )
            
            # 方法1：使用自动提交模式
            cursor = conn.cursor()
            # 设置自动提交为 True
            conn.autocommit = True
            cursor.execute("CREATE CONSTRAINT ON (n:Node) ASSERT n.id IS UNIQUE;")
            
            conn.close()
            
            print("约束创建成功！")
            return True
            
        except Exception as e:
            print(f"约束创建失败: {e}")
            
            if i < max_retries:
                print(f"等待 {retry_delay} 秒后重试...")
                time.sleep(retry_delay)
            else:
                print("❌ 已达到最大重试次数")
                return False
    
    return False


def main():
    # 配置
    bolt_host = "localhost"
    bolt_port = 7687

    if not create_constraint_with_retry(host=bolt_host, port=bolt_port):
        sys.exit(1)

if __name__ == "__main__":
    main()