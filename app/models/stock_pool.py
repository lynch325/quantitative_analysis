"""股票池模型（迁移自 PyQt/C# 桌面版股票池 `user/股票池`）。

与原桌面版 schema 的对应关系：
- `pool(id, name UNIQUE, note, created_at)`      → `StockPool`
- `pool_item(pool_id, code, add_at, source)`     → `StockPoolItem`

差异（为对齐本项目的 SQLAlchemy 约定，语义不变）：
- 用自增主键 + `(pool_id, ts_code)` 唯一约束，替代原复合主键；
  "同一池内代码唯一" 的语义由唯一约束保证，与 `INSERT OR REPLACE` 等价格式一致。
- 字段名 `code` → `ts_code`，与项目内其余模型（持仓/信号等）统一。
"""

from sqlalchemy import Index

from app.extensions import db
from app.utils.time_utils import now_local


class StockPool(db.Model):
    """宏观股票池（一个命名分组）。"""

    __tablename__ = 'stock_pool'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(64), nullable=False, unique=True, comment='股票池名称')
    note = db.Column(db.String(500), default='', comment='备注')
    created_at = db.Column(db.DateTime, default=now_local, comment='创建时间')

    __table_args__ = (
        Index('idx_stock_pool_name', 'name'),
    )

    def to_dict(self):
        """自选池的 API 形态（note 兜底空串，created_at 转 ISO 串）。
        """
        return {
            'id': self.id,
            'name': self.name,
            'note': self.note or '',
            'created_at': self.created_at.isoformat() if self.created_at else None,
        }


class StockPoolItem(db.Model):
    """股票池成分：带 source（导入来源），供页面按来源筛选。"""

    __tablename__ = 'stock_pool_item'

    id = db.Column(db.Integer, primary_key=True)
    pool_id = db.Column(
        db.Integer, db.ForeignKey('stock_pool.id'), nullable=False, comment='所属股票池')
    ts_code = db.Column(db.String(20), nullable=False, comment='股票代码，如 600519.SH')
    source = db.Column(db.String(128), default='', comment='导入来源（板块名/条件/SQL/粘贴）')
    add_at = db.Column(db.DateTime, default=now_local, comment='加入时间')

    __table_args__ = (
        db.UniqueConstraint('pool_id', 'ts_code', name='uq_stock_pool_item'),
        Index('idx_stock_pool_item_pool', 'pool_id'),
        Index('idx_stock_pool_item_code', 'ts_code'),
    )

    def to_dict(self):
        """池成分的 API 形态（source 兜底空串，add_at 转 ISO 串）。
        """
        return {
            'id': self.id,
            'pool_id': self.pool_id,
            'ts_code': self.ts_code,
            'source': self.source or '',
            'add_at': self.add_at.isoformat() if self.add_at else None,
        }
