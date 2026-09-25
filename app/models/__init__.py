"""ORM 模型包（SQLite）：text2sql 元数据、数据任务记录、AI 会话与股票池。

ORM 只承载少量需要事务/关系的小表（`instance/stock_cursor.sqlite3`）；
行情、因子、任务状态等大数据一律走 Parquet（见 app/services/parquet_state_store.py）。

注意：`DataJobRun` 是 ORM 时代的任务记录模型，**当前除本文件的导出外已无任何引用**
（状态存储实际是 ParquetDataJobStateStore），保留导出仅作兼容。
"""

from .text2sql_metadata import TableMetadata, FieldMetadata, QueryTemplate, QueryHistory, BusinessDictionary
from .data_job_run import DataJobRun
from .ai_chat import AiChatSession, AiChatMessage
from .stock_pool import StockPool, StockPoolItem

__all__ = [
    'DataJobRun',
    'TableMetadata',
    'FieldMetadata',
    'QueryTemplate',
    'QueryHistory',
    'BusinessDictionary',
    'AiChatSession',
    'AiChatMessage',
    'StockPool',
    'StockPoolItem',
]
