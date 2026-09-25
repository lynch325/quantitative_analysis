"""API 蓝图包：主蓝图 `api_bp`（url_prefix=/api）及其路由模块。

两处结构约束，改动前先看：
- 本文件只 import 挂在 `api_bp` 上的模块（stock_api / analysis_api / text2sql_api /
  trial_api），且 **import 必须放在 Blueprint 定义之后**——子模块要
  `from app.api import api_bp`，放到前面会循环导入；
- 其他蓝图（ml_factor / realtime_* / websocket / data-jobs / ai-assistant /
  market / datasources / stock-pool / pattern-screen）在 app/__init__.py 的
  工厂里各自按 url_prefix 注册，避免把所有路由的导入都堆到这里。
"""

from flask import Blueprint

api_bp = Blueprint('api', __name__)

from . import stock_api, analysis_api, text2sql_api, trial_api  # noqa: F401