"""Tushare 客户端初始化：从 .env 读 token 并建 pro_api。

token 兼容 `TUSHARE_TOKEN` 与 `tushare_token` 两种写法；未配置时直接抛
ValueError（快速失败，避免带着空 token 跑完整轮抓取才在 401 上报错）。

被 app/utils 下的作业脚本以**顶层模块名** `db_utils` 导入——脚本由
data_jobs/runner.py 以子进程方式执行，其所在目录在 sys.path 上（见该模块说明）。
"""

import os
from dotenv import load_dotenv
import tushare as ts


load_dotenv()


class DatabaseUtils:
    # Tushare API token
    _tushare_token = os.getenv('TUSHARE_TOKEN') or os.getenv('tushare_token')

    @classmethod
    def init_tushare_api(cls):
        """
        初始化Tushare API
        :return: Tushare pro API对象
        """
        if not cls._tushare_token:
            raise ValueError('未配置TUSHARE_TOKEN，请在.env中设置后重试')
        return ts.pro_api(cls._tushare_token)
