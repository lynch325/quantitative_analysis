"""应用工厂入口：创建 Flask app、装扩展、注册蓝图与前端托管。

执行顺序上有两处强约束：
1. **首行** 的 `ensure_click_parameter_source()`（runtime_compat）必须在 import
   Flask 之前跑：老版本 Click 缺 `ParameterSource` 会让 Flask CLI 直接起不来；
2. 蓝图与 websocket 事件在工厂内部**延迟导入**，否则子模块 `from app.api import api_bp`
   会在工厂定义前触发循环导入。

配置保护：config_name 不存在直接抛 ValueError；配置对象若实现 `validate`（生产配置）
则调用它做启动前校验——配置不完整宁可起不来，也不带病启动。

前端托管：frontend/dist 存在时由 frontend_spa 挂到根路径（/api、/socket.io、/static
不接管）；dist 缺失时自动跳过，纯 API 仍可用。
"""

from runtime_compat import ensure_click_parameter_source

ensure_click_parameter_source()

from flask import Flask
from flask_cors import CORS
from config import config  # noqa: F401
from app.extensions import db, socketio
from app.utils.logger import setup_logger

def create_app(config_name='default'):
    """应用工厂函数"""
    from config import config as config_map

    if config_name not in config_map:
        raise ValueError(
            f"未知的配置名: {config_name!r}，可选值: {sorted(config_map.keys())}"
        )

    app = Flask(__name__)

    # 加载配置
    app.config.from_object(config_map[config_name])

    # 生产环境强制校验（SECRET_KEY 等），配置不完整直接失败而不是带病启动
    validator = getattr(config_map[config_name], 'validate', None)
    if callable(validator):
        validator(app.config)

    # 初始化扩展
    db.init_app(app)
    cors_origins = app.config.get('CORS_ORIGINS', '*')
    socketio.init_app(app, cors_allowed_origins=cors_origins)
    CORS(app, origins=cors_origins)
    
    # 设置日志
    setup_logger(app.config['LOG_LEVEL'], app.config['LOG_FILE'])
    
    # 注册蓝图
    from app.api import api_bp
    from app.api.ml_factor_api import ml_factor_bp
    from app.api.text2sql_api import text2sql_bp
    from app.api.realtime_analysis import realtime_analysis_bp
    from app.api.realtime_indicators import realtime_indicators_bp
    from app.api.realtime_signals import realtime_signals_bp
    from app.api.realtime_monitor import realtime_monitor_bp
    from app.api.realtime_risk import realtime_risk_bp
    from app.api.realtime_report import realtime_report_bp
    from app.api.websocket_api import websocket_api_bp
    from app.api.data_jobs_api import data_jobs_bp
    from app.api.ai_assistant_api import ai_assistant_bp
    from app.api.pattern_screen_api import pattern_screen_api
    from app.api.market_api import market_bp, datasources_bp
    from app.api.stock_pool_api import stock_pool_bp
    app.register_blueprint(api_bp, url_prefix='/api')
    app.register_blueprint(ml_factor_bp)
    app.register_blueprint(text2sql_bp)
    app.register_blueprint(realtime_analysis_bp)
    app.register_blueprint(realtime_indicators_bp, url_prefix='/api/realtime-analysis/indicators')
    app.register_blueprint(realtime_signals_bp, url_prefix='/api/realtime-analysis/signals')
    app.register_blueprint(realtime_monitor_bp, url_prefix='/api/realtime-analysis/monitor')
    app.register_blueprint(realtime_risk_bp, url_prefix='/api/realtime-analysis/risk')
    app.register_blueprint(realtime_report_bp, url_prefix='/api/realtime-analysis/reports')
    app.register_blueprint(websocket_api_bp, url_prefix='/api/websocket')
    app.register_blueprint(data_jobs_bp)
    app.register_blueprint(ai_assistant_bp)
    app.register_blueprint(pattern_screen_api)
    app.register_blueprint(market_bp)
    app.register_blueprint(datasources_bp)
    app.register_blueprint(stock_pool_bp)

    # 注册WebSocket事件处理器
    from app.websocket import websocket_events  # noqa: F401

    # 托管前端构建产物（frontend/dist）：日常使用只需 python run.py 单进程，
    # 访问 http://127.0.0.1:5000 即为完整界面；dist 缺失时自动跳过
    from app.frontend_spa import register_frontend_spa
    register_frontend_spa(app)

    return app 
