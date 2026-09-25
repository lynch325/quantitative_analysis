"""loguru 日志初始化（控制台 + 轮转文件）。

供**独立脚本/离线作业**使用；web 与服务层的日志配置在 app/__init__.py，
两边互不干涉。

注意：setup_logger 先 `logger.remove()` 清空**全部** handler 再重建，
因此重复调用是幂等的（不会叠加管道），但它也会把调用方先前添加的
自定义 sink 一并清掉——在已被其他模块配置过日志的进程里慎用。

文件默认 `logs/stock_analysis.log`，10 MB 轮转、保留 30 天、zip 压缩、UTF-8。
"""

import os
from loguru import logger

def setup_logger(log_level='INFO', log_file='logs/stock_analysis.log'):
    """设置日志配置"""
    
    # 确保日志目录存在
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # 移除默认处理器
    logger.remove()
    
    # 添加控制台输出
    logger.add(
        sink=lambda msg: print(msg, end=''),
        level=log_level,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        colorize=True
    )
    
    # 添加文件输出
    logger.add(
        sink=log_file,
        level=log_level,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        rotation="10 MB",
        retention="30 days",
        compression="zip",
        encoding="utf-8"
    )
    
    return logger 