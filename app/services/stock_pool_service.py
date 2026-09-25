"""股票池服务：池与成分的持久化 + 数仓导入源。

迁移自桌面版股票池（`user/股票池/stock_pool_app.py` 及其 C# 版 StockPoolWpf），
把原软件的 `PoolStore`（池/成分 CRUD）与 `Warehouse`（数仓导入源）搬到 Web 服务层。

数仓（DuckDB）是**外部只读依赖**：默认 `PYPlugins/user/数据/tdx_data.duckdb`，
可用环境变量 `TDX_DUCKDB_PATH` 覆盖。它由数仓脚本（tdx_db.py）独立维护，本模块
只做只读查询；被 DBeaver 等占用时可能打开失败，故查询失败会重建连接重试一次。

数仓 schema 依赖（与桌面版一致）：
- `dim_stock(stock_code, stock_name)`        → 名称
- `bridge_stock_sector` + `dim_sector`       → 行业 / 板块与成分
- `fact_daily_kline(trade_date, pct_change, amount, is_stale)`  → 涨跌幅/成交额条件
- `fact_daily_gpjy(GP15_1)`                  → 涨停(2)/曾涨停(1)/跌停(-2)/曾跌停(-1)
"""

from __future__ import annotations

import csv
import io
import os
import re
import threading
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import duckdb
from loguru import logger
from sqlalchemy import or_

from app.extensions import db
from app.models.stock_pool import StockPool, StockPoolItem
from app.services.persistence import persist_changes, persist_new
from app.utils.time_utils import now_local

#: 数仓路径（环境变量优先，缺省为项目同级 user/数据/tdx_data.duckdb）
def warehouse_path() -> Path:
    override = os.getenv("TDX_DUCKDB_PATH")
    if override:
        return Path(override)
    # app/services/stock_pool_service.py → parents[3] = PYPlugins/
    return Path(__file__).resolve().parents[3] / "user" / "数据" / "tdx_data.duckdb"


#: 「未标注来源」哨兵值：空字符串在 items() 里语义是「不过滤」，
#: 无法表达「只看未标注」，故单独约定一个值（前端同名常量 '__none__'）
UNLABELED_SOURCE = "__none__"

#: 条件导入支持的种类及参数含义（extra 为阈值）
CONDITIONS = {
    "涨停": "当日涨停（GP15_1=2）",
    "曾涨停": "当日曾涨停（GP15_1=1）",
    "跌停": "当日跌停（GP15_1=-2）",
    "曾跌停": "当日曾跌停（GP15_1=-1）",
    "涨幅≥%": "当日涨幅≥阈值，需 extra",
    "跌幅≥%": "当日跌幅≥阈值，需 extra",
    "成交额≥亿": "当日成交额≥阈值（亿元），需 extra",
}


def norm_code(text: Any) -> Optional[str]:
    """任意文本 → `600519.SH`；非个股（板块 88 开头）返回 None。"""
    m = re.search(r"(\d{6})", str(text).strip())
    if not m:
        return None
    bare = m.group(1)
    if bare.startswith("88"):  # 板块指数不是个股
        return None
    raw = str(text)
    suffix = raw.split(".")[-1].upper() if "." in raw else ""
    if suffix not in ("SH", "SZ", "BJ"):
        suffix = "BJ" if bare.startswith("920") else (
            "SH" if bare.startswith(("6", "5", "9")) else "SZ")
    return f"{bare}.{suffix}"


def parse_paste(text: str) -> List[str]:
    """从粘贴文本中解析去重的代码列表。"""
    out: List[str] = []
    seen = set()
    for tok in re.findall(r"\d{6}", text or ""):
        code = norm_code(tok)
        if code and code not in seen:
            seen.add(code)
            out.append(code)
    return out


class Warehouse:
    """DuckDB 数仓只读查询（股票池的导入源）。"""

    _TABLE_HINT = "数仓不可用（路径或占用问题），导入源相关功能暂时不可用"

    def __init__(self, path: Optional[Path] = None):
        self.path = Path(path) if path else warehouse_path()
        self._con = None
        self._names: Optional[Dict[str, str]] = None
        self._industries: Optional[Dict[str, str]] = None

    # ---- 连接 ----

    def _open(self):
        """打开只读连接。1.1GB 的库文件打开有成本，故进程内复用。"""
        if self._con is None:
            if not self.path.is_file():
                raise RuntimeError(f"数仓文件不存在: {self.path}")
            self._con = duckdb.connect(str(self.path), read_only=True)
        return self._con

    def _reset(self):
        if self._con is not None:
            try:
                self._con.close()
            except Exception:  # noqa: BLE001 - 关闭失败无碍
                pass
            self._con = None

    def query(self, sql: str, params: Optional[Sequence] = None):
        """执行查询；连接异常时重建后重试一次（应对被占用/文件替换）。"""
        try:
            return self._open().execute(sql, list(params or [])).fetchall()
        except Exception as exc:  # noqa: BLE001 - 统一兜底并在上层转成提示
            logger.warning(f"[stock-pool] 数仓查询失败，重建连接重试: {exc}")
            self._reset()
            try:
                return self._open().execute(sql, list(params or [])).fetchall()
            except Exception as exc2:  # noqa: BLE001
                raise RuntimeError(f"{self._TABLE_HINT}: {exc2}") from exc2

    def available(self) -> bool:
        try:
            self._open()
            return True
        except Exception:  # noqa: BLE001
            return False

    # ---- 元数据 ----

    @property
    def names(self) -> Dict[str, str]:
        """code -> 名称。"""
        if self._names is None:
            rows = self.query("SELECT stock_code, stock_name FROM dim_stock")
            self._names = {str(r[0]): str(r[1]) for r in rows}
        return self._names

    @property
    def industries(self) -> Dict[str, str]:
        """code -> 子行业（缺省行业，多个用 / 连接）。"""
        if self._industries is None:
            rows = self.query("""
                SELECT b.stock_code, string_agg(s.sector_name, '/')
                FROM bridge_stock_sector b
                JOIN dim_sector s ON b.sector_code = s.sector_code
                WHERE s.sector_type = '行业' GROUP BY b.stock_code""")
            self._industries = {str(r[0]): str(r[1]) for r in rows}
        return self._industries

    def sectors(self, stype: Optional[str] = None) -> List[Dict[str, Any]]:
        """板块列表 [{code, name, type, count}]。

        count 为成分数：一次 GROUP BY 聚合即可（实测 744 个板块 0.02s），
        供前端按热度排序。stype 为空或「全部」时返回全部类型
        （概念 269 / 自定义 159 / 风格 152 / 行业 110 / 地区 32）。
        """
        sql = (
            "SELECT s.sector_code, s.sector_name, s.sector_type, "
            "count(b.stock_code) AS cnt "
            "FROM dim_sector s "
            "LEFT JOIN bridge_stock_sector b ON s.sector_code = b.sector_code"
        )
        params: List[Any] = []
        if stype and stype != "全部":
            sql += " WHERE s.sector_type = ?"
            params.append(stype)
        rows = self.query(sql + " GROUP BY 1, 2, 3 ORDER BY s.sector_name", params)
        return [{
            "code": str(r[0]),
            "name": str(r[1]),
            "type": str(r[2]) if r[2] is not None else "未分类",
            "count": int(r[3] or 0),
        } for r in rows]

    def sector_members(self, sector_code: str) -> List[str]:
        rows = self.query(
            "SELECT stock_code FROM bridge_stock_sector WHERE sector_code = ?", [sector_code])
        return [str(r[0]) for r in rows if r[0]]

    def trade_dates(self, limit: int = 30) -> List[str]:
        rows = self.query(
            "SELECT DISTINCT CAST(trade_date AS DATE) FROM fact_daily_kline "
            "ORDER BY 1 DESC LIMIT ?", [int(limit)])
        return [str(r[0])[:10] for r in rows if r[0]]

    # ---- 条件/自定义 SQL ----

    def by_condition(self, kind: str, date: str, extra: Optional[float] = None) -> List[str]:
        """按预置条件取代码；涨停类走 GP15_1，其余走 kline。参数均做绑定。"""
        d = f"{date}"
        if kind == "涨停":
            sql = ("SELECT stock_code FROM fact_daily_gpjy "
                   "WHERE CAST(trade_date AS DATE)=CAST(? AS DATE) AND CAST(GP15_1 AS INT)=2")
        elif kind == "曾涨停":
            sql = ("SELECT stock_code FROM fact_daily_gpjy "
                   "WHERE CAST(trade_date AS DATE)=CAST(? AS DATE) AND CAST(GP15_1 AS INT)=1")
        elif kind == "跌停":
            sql = ("SELECT stock_code FROM fact_daily_gpjy "
                   "WHERE CAST(trade_date AS DATE)=CAST(? AS DATE) AND CAST(GP15_1 AS INT)=-2")
        elif kind == "曾跌停":
            sql = ("SELECT stock_code FROM fact_daily_gpjy "
                   "WHERE CAST(trade_date AS DATE)=CAST(? AS DATE) AND CAST(GP15_1 AS INT)=-1")
        elif kind == "涨幅≥%":
            sql = ("SELECT stock_code FROM fact_daily_kline "
                   "WHERE CAST(trade_date AS DATE)=CAST(? AS DATE) "
                   "AND COALESCE(is_stale, FALSE)=FALSE AND pct_change >= ?")
            return [str(r[0]) for r in self.query(sql, [d, float(extra or 5)]) if r[0]]
        elif kind == "跌幅≥%":
            sql = ("SELECT stock_code FROM fact_daily_kline "
                   "WHERE CAST(trade_date AS DATE)=CAST(? AS DATE) "
                   "AND COALESCE(is_stale, FALSE)=FALSE AND pct_change <= ?")
            return [str(r[0]) for r in self.query(sql, [d, -abs(float(extra or 5))]) if r[0]]
        elif kind == "成交额≥亿":
            sql = ("SELECT stock_code FROM fact_daily_kline "
                   "WHERE CAST(trade_date AS DATE)=CAST(? AS DATE) AND amount >= ?")
            # kline 的 amount 单位为千元，亿元阈值 ×10000 得千元
            return [str(r[0]) for r in self.query(sql, [d, float(extra or 10) * 10000]) if r[0]]
        else:
            raise ValueError(f"不支持的条件: {kind}")
        return [str(r[0]) for r in self.query(sql, [d]) if r[0]]

    def by_sql(self, sql: str) -> List[str]:
        """自定义 SQL：取结果里的 stock_code 列（无该列时取第一列）。"""
        con = self._open()
        result = con.execute(sql)
        cols = [d[0] for d in result.description]
        idx = next((i for i, c in enumerate(cols) if str(c).lower() == "stock_code"), 0)
        return [str(r[idx]) for r in result.fetchall() if r[idx]]


class StockPoolError(RuntimeError):
    """股票池业务错误（池不存在、参数非法等）。"""


class StockPoolService:
    """股票池：池与成分的 CRUD、导入、导出、推送通达信。"""

    #: 建表是否已完成（双检锁，幂等）
    _tables_ready = False
    _tables_lock = threading.Lock()

    def __init__(self, warehouse: Optional[Warehouse] = None):
        self.warehouse = warehouse or Warehouse()

    @classmethod
    def ensure_tables(cls) -> None:
        """首次访问时幂等补建股票池表。

        项目只在 `run_system.py` 手动初始化时 `db.create_all()`，正常启动不建表；
        这里沿用 AI 助手模块（`assistant_service._ensure_tables`）的既有做法，
        免得用户为新增模块再跑一次初始化。
        """
        if cls._tables_ready:
            return
        with cls._tables_lock:
            if cls._tables_ready:
                return
            db.create_all()
            cls._tables_ready = True
            logger.info("[stock-pool] 股票池表就绪")

    # ---- 池 ----

    def list_pools(self) -> List[Dict[str, Any]]:
        """列出股票池（按名称排序），每条附带成分数量。

        成分数逐池单独 count：池数量有限时代价可接受，
        但若池数量涨到成百上千，这里会退化成 N+1 查询。
        """
        pools = StockPool.query.order_by(StockPool.name).all()
        result = []
        for pool in pools:
            item = pool.to_dict()
            item["count"] = StockPoolItem.query.filter_by(pool_id=pool.id).count()
            result.append(item)
        return result

    def _get_pool(self, pool_id: int) -> StockPool:
        pool = StockPool.query.get(int(pool_id))
        if pool is None:
            raise StockPoolError(f"股票池不存在: {pool_id}")
        return pool

    def create_pool(self, name: str, note: str = "") -> Dict[str, Any]:
        """新建股票池；名称为空或已存在同名池时抛 StockPoolError（接口层转业务错误）。
        """
        name = (name or "").strip()
        if not name:
            raise StockPoolError("股票池名称不能为空")
        if StockPool.query.filter_by(name=name).first():
            raise StockPoolError(f"股票池已存在: {name}")
        pool = StockPool(name=name, note=note or "", created_at=now_local())
        persist_new(pool)
        item = pool.to_dict()
        item["count"] = 0
        return item

    def rename_pool(self, pool_id: int, name: str) -> Dict[str, Any]:
        """重命名股票池；与**其他池**同名时抛 StockPoolError（查重排除自身 id）。
        """
        name = (name or "").strip()
        if not name:
            raise StockPoolError("股票池名称不能为空")
        pool = self._get_pool(pool_id)
        dup = StockPool.query.filter(StockPool.name == name, StockPool.id != pool.id).first()
        if dup:
            raise StockPoolError(f"股票池已存在: {name}")
        pool.name = name
        persist_changes(pool)
        return pool.to_dict()

    def delete_pool(self, pool_id: int) -> bool:
        pool = self._get_pool(pool_id)
        StockPoolItem.query.filter_by(pool_id=pool.id).delete()
        db.session.delete(pool)
        db.session.commit()
        return True

    # ---- 成分 ----

    def items(self, pool_id: int, keyword: str = "", source: str = "",
              with_quote: bool = False) -> List[Dict[str, Any]]:
        """池成分；可按关键字（代码/名称）与来源过滤，可选富化实时行情。"""
        self._get_pool(pool_id)
        query = StockPoolItem.query.filter_by(pool_id=pool_id)
        if source == UNLABELED_SOURCE:
            query = query.filter(
                or_(StockPoolItem.source == "", StockPoolItem.source.is_(None)))
        elif source:
            query = query.filter(StockPoolItem.source == source)
        rows = query.order_by(StockPoolItem.add_at, StockPoolItem.ts_code).all()

        names = industries = {}
        if self.warehouse.available():
            try:
                names, industries = self.warehouse.names, self.warehouse.industries
            except Exception as exc:  # noqa: BLE001 - 元数据缺失不影响返回代码
                logger.warning(f"[stock-pool] 读取数仓名称/行业失败: {exc}")

        quotes: Dict[str, Dict[str, Any]] = {}
        if with_quote:
            quotes = self._quote_index()

        keyword = (keyword or "").strip().lower()
        result = []
        for row in rows:
            name = names.get(row.ts_code, "")
            industry = industries.get(row.ts_code, "")
            if keyword and not (keyword in row.ts_code.lower()
                                or keyword in name.lower()
                                or keyword in industry.lower()):
                continue
            item = row.to_dict()
            item["name"] = name
            item["industry"] = industry
            if with_quote:
                quote = quotes.get(row.ts_code)
                item["last_price"] = quote.get("last_price") if quote else None
                item["pct_chg"] = quote.get("pct_chg") if quote else None
            result.append(item)
        return result

    def sources(self, pool_id: int) -> List[Dict[str, Any]]:
        """该池出现过的导入来源及各自成分数，按数量降序（供左侧来源筛选）。

        未标注来源（source 为空/NULL）以 UNLABELED_SOURCE 返回，使「全部来源」
        的计数等于池内总数——原实现直接丢掉空来源，会少算。
        """
        self._get_pool(pool_id)
        rows = (StockPoolItem.query.filter_by(pool_id=pool_id)
                .with_entities(StockPoolItem.source).all())
        counter = Counter((r[0] or UNLABELED_SOURCE) for r in rows)
        return [{"name": k, "count": v}
                for k, v in sorted(counter.items(), key=lambda kv: (-kv[1], kv[0]))]

    def add_items(self, pool_id: int, codes: Sequence[str], source: str = "") -> int:
        """批量加入成分；已存在则覆盖来源与加入时间（与原 INSERT OR REPLACE 一致）。"""
        self._get_pool(pool_id)
        wanted = []
        for raw in codes or []:
            code = norm_code(raw)
            if code:
                wanted.append(code)
        if not wanted:
            return 0
        now = now_local()
        existed = {r.ts_code: r for r in StockPoolItem.query.filter(
            StockPoolItem.pool_id == pool_id,
            StockPoolItem.ts_code.in_(wanted)).all()}
        for code in wanted:
            row = existed.get(code)
            if row is None:
                db.session.add(StockPoolItem(
                    pool_id=pool_id, ts_code=code, source=source or "", add_at=now))
            else:
                row.source = source or ""
                row.add_at = now
        db.session.commit()
        return len(wanted)

    def remove_items(self, pool_id: int, codes: Sequence[str]) -> int:
        """从池中移除成分，返回实际删除行数。

        代码先经 norm_code 归一（容忍多种写法）；归一后为空直接返回 0，不执行删除。
        """
        self._get_pool(pool_id)
        wanted = [c for c in (norm_code(x) for x in codes or []) if c]
        if not wanted:
            return 0
        removed = StockPoolItem.query.filter(
            StockPoolItem.pool_id == pool_id,
            StockPoolItem.ts_code.in_(wanted)).delete(synchronize_session=False)
        db.session.commit()
        return int(removed)

    # ---- 导入源 ----

    def preview(self, kind: str, **kwargs) -> List[str]:
        """预览导入结果（返回代码列表）。

        kind: sector | condition | sql | paste
        """
        if kind == "sector":
            code = kwargs.get("sector_code")
            if not code:
                raise StockPoolError("缺少板块代码")
            return self.warehouse.sector_members(code)
        if kind == "condition":
            cond = kwargs.get("condition")
            date = kwargs.get("date")
            extra = kwargs.get("extra")
            if not cond or not date:
                raise StockPoolError("缺少条件或交易日")
            return self.warehouse.by_condition(cond, date, extra)
        if kind == "sql":
            sql = (kwargs.get("sql") or "").strip()
            if not sql:
                raise StockPoolError("SQL 不能为空")
            return self.warehouse.by_sql(sql)
        if kind == "paste":
            return parse_paste(kwargs.get("text") or "")
        raise StockPoolError(f"不支持的导入方式: {kind}")

    # ---- 导出 / 推送 ----

    def export_csv(self, pool_id: int) -> str:
        """把池成分导出为 CSV 文本（ts_code / name / industry / source / add_at）。

        返回字符串而不是文件：响应头与编码由接口层决定。
        """
        rows = self.items(pool_id)
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(["ts_code", "name", "industry", "source", "add_at"])
        for row in rows:
            writer.writerow([row["ts_code"], row["name"], row["industry"],
                             row["source"], row["add_at"]])
        return buf.getvalue()

    # ---- 辅助 ----

    @staticmethod
    def _quote_index() -> Dict[str, Dict[str, Any]]:
        """全市场行情索引（code -> {last_price, pct_chg}）；不可用返回空。"""
        try:
            from app.services.market_snapshot_service import get_market_snapshot_service

            frame = get_market_snapshot_service().get_quote_frame()
            if frame is None or frame.empty:
                return {}
            cols = frame.columns
            name_col = "last_price" if "last_price" in cols else None
            pct_col = "pct_chg" if "pct_chg" in cols else None
            if not name_col:
                return {}
            return {
                str(r["ts_code"]): {
                    "last_price": r.get(name_col),
                    "pct_chg": r.get(pct_col) if pct_col else None,
                }
                for _, r in frame.iterrows()
            }
        except Exception as exc:  # noqa: BLE001 - 行情缺失不影响池数据
            logger.warning(f"[stock-pool] 行情富化不可用: {exc}")
            return {}


_service: Optional[StockPoolService] = None


def get_stock_pool_service() -> StockPoolService:
    """进程内单例。"""
    global _service
    if _service is None:
        _service = StockPoolService()
    return _service
