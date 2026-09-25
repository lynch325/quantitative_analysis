"""Flask 扩展单例：SQLAlchemy(db) 与 SocketIO。

两个与部署强相关的点：
- **SQLite PRAGMA**：在 Engine 全局 connect 事件里给 sqlite3 连接设
  `journal_mode=WAL` + `synchronous=NORMAL`——多进程/多线程共写同一库文件时
  避免 `database is locked`；等待超时由 config.py 的 connect_args(timeout=30) 兜底；
- **SocketIO async_mode**：默认 threading；仅当 SOCKETIO_ASYNC_MODE=eventlet 时
  要求入口先 monkey_patch（见 run.py），否则阻塞调用会卡死事件循环。

`db` / `socketio` 是模块级单例，由 create_app 里 init_app 绑定到具体 app；
不要在模块导入期做 init_app。
"""

from flask_sqlalchemy import SQLAlchemy
import os
import sqlite3
from flask_socketio import SocketIO
from sqlalchemy import event
from sqlalchemy.engine import Engine


# SQLite 并发防护：多个工作进程/线程共写同一库文件。WAL 允许读写并行，
# synchronous=NORMAL 是 WAL 的推荐搭配；busy_timeout 由 config.py 的
# connect_args timeout 提供，两层配合避免 database is locked
@event.listens_for(Engine, "connect")
def _set_sqlite_pragmas(dbapi_connection, connection_record):
    """SQLite 连接建立时设置 PRAGMA：WAL 日志模式 + synchronous=NORMAL。

    WAL 让读写互不阻塞（实时轮询与写入并存时很关键）；
    非 SQLite 连接直接返回，保证将来换数据库不需要改这里。
    """
    if not isinstance(dbapi_connection, sqlite3.Connection):
        return
    cursor = dbapi_connection.cursor()
    try:
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")
    finally:
        cursor.close()

# 数据库实例
db = SQLAlchemy()
# eventlet 模式必须在应用入口最先 monkey_patch，否则所有阻塞调用会卡死
# 事件循环；默认 threading 模式无需补丁，本地开发更稳
socketio = SocketIO(
    cors_allowed_origins="*",
    async_mode=os.getenv('SOCKETIO_ASYNC_MODE', 'threading'),
)