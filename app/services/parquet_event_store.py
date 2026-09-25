"""实时事件存储：指标（indicators）与交易信号（signals）的 Parquet 追加/查询层。

落盘结构：`{DATA_DIR}/realtime_events/{event_type}/year=/month=/day=/data.parquet`，
写入时按 datetime 的日期分组、与当天既有分区合并后**原子替换**该分区文件。

读写的三条关键约定（改动前必读）：
- 读 = 该事件类型全部分区扫描 + concat，没有按时间窗做分区裁剪，
  因此 get_* 类查询的成本随历史天数线性增长，属"低频事件、可接受"的取舍；
- 写 = 整分区读-改-写，必须走 locked(event_type)（flock）；
  已在锁内时必须调用 *_unlocked 变体，重入同一 event_type 会死锁；
- 分区文件损坏不能静默返回空表：上层读-改-写会把空表当基线，
  concat 后覆盖掉该分区既有数据，故 _read_partition 先隔离坏文件（quarantine）。

典型调用方：realtime_* 系列引擎（写指标/信号）、推送服务与相关 API 路由（读）。
"""

from __future__ import annotations

import os
from datetime import datetime
from app.utils.time_utils import now_local
from app.utils.parquet_writer import (
    atomic_write_parquet,
    parquet_file_lock,
    quarantine_corrupt_parquet,
)
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Sequence

import pandas as pd
from loguru import logger


class ParquetEventStore:
    """Lightweight Parquet-backed append/query store for realtime events."""

    def __init__(self, base_dir: Optional[str] = None):
        if base_dir is None:
            base_dir = os.getenv(
                "DATA_DIR",
                os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data"),
            )
        self.base_dir = Path(base_dir) / "realtime_events"
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def locked(self, event_type: str):
        """以独占文件锁包裹一段 read-modify-write。

        信号/指标引擎、推送服务、API 路由各自持有独立的 store 实例，
        可能并发读改写同一事件分区；flock 保证同一时刻只有一个
        持有者在写（flock 按打开的文件描述互斥，进程内多线程同样生效）。
        注意：不要在 locked 内再进入同一 event_type 的 locked（同进程会重入死锁），
        内部路径一律调用 *_unlocked 变体。
        """
        lock_path = self._event_dir(event_type) / f"{event_type}.lock"
        return parquet_file_lock(lock_path)

    def _event_dir(self, event_type: str) -> Path:
        path = self.base_dir / event_type
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _partition_path(self, event_type: str, day: datetime) -> Path:
        """事件分区路径：事件目录 / year= / month= / day= / data.parquet。
        """
        return (
            self._event_dir(event_type)
            / f"year={day.year:04d}"
            / f"month={day.month:02d}"
            / f"day={day.day:02d}"
            / "data.parquet"
        )

    def _ensure_frame(self, rows: Iterable[Dict[str, Any]] | pd.DataFrame) -> pd.DataFrame:
        """把 dict 列表或 DataFrame 统一成 DataFrame，并归一已知时间列。

        时间列（datetime/created_at/updated_at/expiry_time/resolved_at）统一转
        datetime，无法解析的置 NaT（随后在写入路径被 dropna 丢弃）。
        """
        if isinstance(rows, pd.DataFrame):
            frame = rows.copy()
        else:
            frame = pd.DataFrame(list(rows))
        if frame.empty:
            return frame
        if "datetime" in frame.columns:
            frame["datetime"] = pd.to_datetime(frame["datetime"], errors="coerce")
        if "created_at" in frame.columns:
            frame["created_at"] = pd.to_datetime(frame["created_at"], errors="coerce")
        if "updated_at" in frame.columns:
            frame["updated_at"] = pd.to_datetime(frame["updated_at"], errors="coerce")
        if "expiry_time" in frame.columns:
            frame["expiry_time"] = pd.to_datetime(frame["expiry_time"], errors="coerce")
        if "resolved_at" in frame.columns:
            frame["resolved_at"] = pd.to_datetime(frame["resolved_at"], errors="coerce")
        return frame

    def _read_partition(self, path: Path) -> pd.DataFrame:
        """读取单个分区文件；不存在返回空表。

        读到损坏文件时先把该文件隔离（quarantine_corrupt_parquet）再返回空表——
        若只吞异常返回空表，上层读-改-写会把空表当基线，覆盖掉分区既有数据。
        """
        if not path.is_file():
            return pd.DataFrame()
        try:
            return pd.read_parquet(path)
        except Exception as exc:
            # 关键：坏文件不能既吞掉异常又返回空表——上层的读改写会把
            # 空表当真，concat 后把这个分区的既有数据整个覆盖掉
            quarantine_corrupt_parquet(
                path,
                lambda msg, err=exc: logger.error(f"实时事件 {msg}: {err}"),
            )
            return pd.DataFrame()

    def _read_event_frame(self, event_type: str) -> pd.DataFrame:
        """读取某事件类型的全部分区并合并（无分区时返回空表）。

        时间列统一转 datetime、id 统一转数值；并发写入期间可能读到
        上次原子替换前的完整旧分区，不会读到半个文件。
        """
        root = self._event_dir(event_type)
        paths = sorted(root.glob("year=*/month=*/day=*/data.parquet"))
        if not paths:
            return pd.DataFrame()

        frames = [self._read_partition(path) for path in paths]
        frames = [frame for frame in frames if not frame.empty]
        if not frames:
            return pd.DataFrame()

        frame = pd.concat(frames, ignore_index=True)
        for column in ["datetime", "created_at", "updated_at", "expiry_time", "resolved_at"]:
            if column in frame.columns:
                frame[column] = pd.to_datetime(frame[column], errors="coerce")
        if "id" in frame.columns:
            frame["id"] = pd.to_numeric(frame["id"], errors="coerce")
        return frame

    def _write_event_frame(self, event_type: str, frame: pd.DataFrame) -> int:
        """写入事件表（自动加锁），返回**入参行数**（去重前的本次提交量）。"""
        with self.locked(event_type):
            return self._write_event_frame_unlocked(event_type, frame, merge_existing=True)

    def _write_event_frame_unlocked(
        self, event_type: str, frame: pd.DataFrame, merge_existing: bool = True
    ) -> int:
        """按日期分区写入（调用方必须已持锁），返回入参行数。

        每个日期分区内的处理顺序：
        1. merge_existing=True 时先读当天既有分区做 concat，否则整分区覆盖；
        2. 缺 id 列时从全表（_read_event_frame）最大 id 起分配自增 id；
        3. 按业务键（id/ts_code/datetime/period_type/指标或策略名）去重，keep=last，
           故**重复提交同一根指标会覆盖旧值而非追加**；
        4. 排序后 atomic_write_parquet 原子替换分区文件。

        注意：datetime 无法解析的行会被静默丢弃（返回行数仍计入原始条数）。
        """
        if frame is None or frame.empty:
            return 0

        frame = frame.copy()
        frame["datetime"] = pd.to_datetime(frame["datetime"], errors="coerce")
        frame = frame.dropna(subset=["datetime"])
        if frame.empty:
            return 0

        total_rows = 0
        for date_value, day_frame in frame.groupby(frame["datetime"].dt.date):
            path = self._partition_path(event_type, datetime.combine(date_value, datetime.min.time()))
            path.parent.mkdir(parents=True, exist_ok=True)
            if merge_existing and path.is_file():
                existing = self._read_partition(path)
                combined = pd.concat([existing, day_frame], ignore_index=True)
            else:
                combined = day_frame.copy()

            if "id" in combined.columns:
                combined["id"] = pd.to_numeric(combined["id"], errors="coerce")
                if combined["id"].isna().all():
                    combined = combined.drop(columns=["id"])

            if "id" not in combined.columns:
                existing = self._read_event_frame(event_type)
                next_id = 1
                if not existing.empty and "id" in existing.columns:
                    numeric = pd.to_numeric(existing["id"], errors="coerce").dropna()
                    if not numeric.empty:
                        next_id = int(numeric.max()) + 1
                combined = combined.copy()
                combined["id"] = range(next_id, next_id + len(combined))

            dedupe_cols = [
                col
                for col in [
                    "id",
                    "ts_code",
                    "datetime",
                    "period_type",
                    "indicator_name",
                    "sub_name",
                    "strategy_name",
                ]
                if col in combined.columns
            ]
            if dedupe_cols:
                combined = combined.drop_duplicates(subset=dedupe_cols, keep="last")

            sort_cols = [
                col
                for col in [
                    "datetime",
                    "ts_code",
                    "indicator_name",
                    "sub_name",
                    "strategy_name",
                    "id",
                ]
                if col in combined.columns
            ]
            if sort_cols:
                combined = combined.sort_values(sort_cols).reset_index(drop=True)

            # 原子替换：写一半崩溃时读者要么看到旧文件要么看到完整新文件，
            # 不会留下半个 parquet 再被 _read_partition 当损坏文件隔离
            atomic_write_parquet(combined, str(path))
            total_rows += len(day_frame)
        return total_rows

    def _filter_frame(
        self,
        event_type: str,
        ts_code: Optional[str] = None,
        period_type: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> pd.DataFrame:
        """读取某类事件并按 ts_code / period_type / 时间区间过滤。

        先解析 datetime 并丢弃无法解析的行，脏数据不参与范围比较；
        各过滤条件只在对应列存在时生效，缺列不报错（历史分区字段不齐）。
        """
        frame = self._read_event_frame(event_type)
        if frame.empty:
            return frame

        if "datetime" in frame.columns:
            frame["datetime"] = pd.to_datetime(frame["datetime"], errors="coerce")
            frame = frame.dropna(subset=["datetime"])
        if ts_code and "ts_code" in frame.columns:
            frame = frame[frame["ts_code"] == ts_code]
        if period_type and "period_type" in frame.columns:
            frame = frame[frame["period_type"] == period_type]
        if start_time is not None and "datetime" in frame.columns:
            frame = frame[frame["datetime"] >= start_time]
        if end_time is not None and "datetime" in frame.columns:
            frame = frame[frame["datetime"] <= end_time]
        if frame.empty:
            return frame
        sort_cols = [
            col
            for col in [
                "datetime",
                "ts_code",
                "indicator_name",
                "sub_name",
                "strategy_name",
                "id",
            ]
            if col in frame.columns
        ]
        if sort_cols:
            frame = frame.sort_values(sort_cols).reset_index(drop=True)
        return frame

    def append_indicators(self, rows: Iterable[Dict[str, Any]] | pd.DataFrame) -> int:
        """追加/更新指标记录（event_type=indicators），返回提交行数。"""
        frame = self._ensure_frame(rows)
        return self._write_event_frame("indicators", frame)

    def append_signals(self, rows: Iterable[Dict[str, Any]] | pd.DataFrame) -> int:
        """追加/更新交易信号（event_type=signals），返回提交行数。"""
        frame = self._ensure_frame(rows)
        return self._write_event_frame("signals", frame)

    def get_latest_indicators(
        self,
        ts_code: str,
        period_type: str,
        indicator_names: Optional[Sequence[str]] = None,
        limit: int = 100,
    ) -> pd.DataFrame:
        """取某股某周期的最新指标，**按 datetime 倒序**（最新在前）截断 limit 条。

        indicator_names 省略时返回全部指标；先取最新再过滤名称
        （过滤发生在取数之后，因此 limit 是"每指标各若干"的近似语义）。
        """
        frame = self._filter_frame("indicators", ts_code=ts_code, period_type=period_type)
        if frame.empty:
            return frame
        if indicator_names and "indicator_name" in frame.columns:
            frame = frame[frame["indicator_name"].isin(set(indicator_names))]
        if frame.empty:
            return frame
        return frame.sort_values("datetime", ascending=False).head(limit).reset_index(drop=True)

    def get_indicator_history(
        self,
        ts_code: str,
        period_type: str,
        indicator_name: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> pd.DataFrame:
        """取单只股票单个指标的时间序列，**按 datetime 升序**返回全量。

        与 get_latest_indicators 相反：这里不截断，供绘图/因子计算使用。
        """
        frame = self._filter_frame(
            "indicators",
            ts_code=ts_code,
            period_type=period_type,
            start_time=start_time,
            end_time=end_time,
        )
        if frame.empty or "indicator_name" not in frame.columns:
            return frame
        frame = frame[frame["indicator_name"] == indicator_name]
        if frame.empty:
            return frame
        return frame.sort_values("datetime").reset_index(drop=True)

    def get_indicators_by_time_range(
        self,
        ts_code: Optional[str],
        period_type: Optional[str],
        start_time: datetime,
        end_time: datetime,
    ) -> pd.DataFrame:
        """时间窗内的指标（可按代码/周期可选过滤），升序返回，不做条数截断。"""
        return self._filter_frame(
            "indicators",
            ts_code=ts_code,
            period_type=period_type,
            start_time=start_time,
            end_time=end_time,
        )

    def get_indicator_stats(self) -> Dict[str, Any]:
        """指标表整体统计（总条数/股票数/按指标名与周期计数/时间范围）。

        每次调用都全量扫描并读入内存，只适合管理页低频调用，勿放入高频路径。
        """
        frame = self._read_event_frame("indicators")
        if frame.empty:
            return {
                "total_records": 0,
                "total_stocks": 0,
                "indicator_stats": {},
                "period_stats": {},
                "earliest_time": None,
                "latest_time": None,
            }

        indicator_stats = {}
        if "indicator_name" in frame.columns:
            indicator_stats = frame["indicator_name"].fillna("UNKNOWN").value_counts().to_dict()
        period_stats = {}
        if "period_type" in frame.columns:
            period_stats = frame["period_type"].fillna("UNKNOWN").value_counts().to_dict()

        earliest = pd.to_datetime(frame["datetime"], errors="coerce").min() if "datetime" in frame.columns else None
        latest = pd.to_datetime(frame["datetime"], errors="coerce").max() if "datetime" in frame.columns else None
        stock_count = int(frame["ts_code"].dropna().astype(str).nunique()) if "ts_code" in frame.columns else 0
        return {
            "total_records": int(len(frame)),
            "total_stocks": stock_count,
            "indicator_stats": {str(key): int(value) for key, value in indicator_stats.items()},
            "period_stats": {str(key): int(value) for key, value in period_stats.items()},
            "earliest_time": earliest.isoformat() if pd.notna(earliest) else None,
            "latest_time": latest.isoformat() if pd.notna(latest) else None,
        }

    def cleanup_old_indicators(self, days_to_keep: int = 30) -> int:
        """删除早于 `days_to_keep` 天的指标记录，返回删除条数。

        持锁执行；按 datetime 过滤后整表重写（_rewrite_event_frame_unlocked
        会顺带 unlink 不再有数据的旧分区），因此会真实释放磁盘。
        """
        cutoff = now_local() - pd.Timedelta(days=days_to_keep)
        with self.locked("indicators"):
            frame = self._read_event_frame("indicators")
            if frame.empty or "datetime" not in frame.columns:
                return 0
            keep = frame[pd.to_datetime(frame["datetime"], errors="coerce") >= cutoff]
            removed = len(frame) - len(keep)
            self._rewrite_event_frame_unlocked("indicators", keep)
        return removed

    def get_active_signals(
        self,
        ts_code: Optional[str] = None,
        strategy_name: Optional[str] = None,
        limit: int = 100,
    ) -> pd.DataFrame:
        """取当前活跃信号（status == 'ACTIVE'，大小写不敏感），按时间倒序取 limit 条。

        读的是全部分区；策略名过滤在读取后执行，故 limit 为近似语义。
        """
        frame = self._read_event_frame("signals")
        if frame.empty:
            return frame
        if "status" in frame.columns:
            frame = frame[frame["status"].fillna("").str.upper() == "ACTIVE"]
        if ts_code and "ts_code" in frame.columns:
            frame = frame[frame["ts_code"] == ts_code]
        if strategy_name and "strategy_name" in frame.columns:
            frame = frame[frame["strategy_name"] == strategy_name]
        if frame.empty:
            return frame
        return frame.sort_values("datetime", ascending=False).head(limit).reset_index(drop=True)

    def get_signals_by_time_range(
        self,
        start_time: datetime,
        end_time: datetime,
        ts_code: Optional[str] = None,
        strategy_name: Optional[str] = None,
    ) -> pd.DataFrame:
        """时间窗内的信号（不限状态），升序返回全量，用于绩效统计与回放。"""
        frame = self._filter_frame("signals", ts_code=ts_code, start_time=start_time, end_time=end_time)
        if frame.empty:
            return frame
        if strategy_name and "strategy_name" in frame.columns:
            frame = frame[frame["strategy_name"] == strategy_name]
        if frame.empty:
            return frame
        return frame.sort_values("datetime").reset_index(drop=True)

    def get_recent_signals(
        self,
        since: datetime,
        limit: int = 20,
        status: Optional[str] = "ACTIVE",
    ) -> pd.DataFrame:
        """取 since 至当前时刻的信号（默认只要 ACTIVE），按时间倒序取 limit 条。

        status=None 表示不过滤状态。
        """
        frame = self._filter_frame("signals", start_time=since, end_time=now_local())
        if frame.empty:
            return frame
        if status and "status" in frame.columns:
            frame = frame[frame["status"].fillna("").str.upper() == status.upper()]
        if frame.empty:
            return frame
        return frame.sort_values("datetime", ascending=False).head(limit).reset_index(drop=True)

    def get_signal_performance(self, strategy_name: Optional[str] = None, days: int = 30) -> Dict[str, Any]:
        """近 days 天信号绩效：胜率、平均/累计盈亏、最大盈利与最大亏损。

        口径提醒（易误读）：统计前先把窗口内的信号裁到
        status ∈ {EXECUTED, EXPIRED}，所以返回的 `total_signals` 是
        「已执行 + 已过期」条数，**不等于**窗口内产生的全部信号数；
        胜率分母为 EXECUTED 且 profit_loss 非空的条数。
        另：frame 为空或过滤后为空时统一返回全零结构，调用方无需判 None。
        """
        end_time = now_local()
        start_time = end_time - pd.Timedelta(days=days)
        frame = self.get_signals_by_time_range(start_time, end_time, strategy_name=strategy_name)
        if frame.empty:
            return {
                "total_signals": 0,
                "executed_signals": 0,
                "win_rate": 0.0,
                "avg_profit_loss": 0.0,
                "total_profit_loss": 0.0,
                "max_profit": 0.0,
                "max_loss": 0.0,
            }
        if "status" in frame.columns:
            frame = frame[frame["status"].fillna("").isin(["EXECUTED", "EXPIRED"])]
        if frame.empty:
            return {
                "total_signals": 0,
                "executed_signals": 0,
                "win_rate": 0.0,
                "avg_profit_loss": 0.0,
                "total_profit_loss": 0.0,
                "max_profit": 0.0,
                "max_loss": 0.0,
            }

        executed = frame[(frame["status"].fillna("") == "EXECUTED") & frame["profit_loss"].notna()] if "profit_loss" in frame.columns else pd.DataFrame()
        if executed.empty:
            return {
                "total_signals": int(len(frame)),
                "executed_signals": 0,
                "win_rate": 0.0,
                "avg_profit_loss": 0.0,
                "total_profit_loss": 0.0,
                "max_profit": 0.0,
                "max_loss": 0.0,
            }
        profit_losses = pd.to_numeric(executed["profit_loss"], errors="coerce").dropna()
        if profit_losses.empty:
            return {
                "total_signals": int(len(frame)),
                "executed_signals": int(len(executed)),
                "win_rate": 0.0,
                "avg_profit_loss": 0.0,
                "total_profit_loss": 0.0,
                "max_profit": 0.0,
                "max_loss": 0.0,
            }
        winning = executed[pd.to_numeric(executed["profit_loss"], errors="coerce") > 0]
        return {
            "total_signals": int(len(frame)),
            "executed_signals": int(len(executed)),
            "win_rate": len(winning) / len(executed) * 100 if len(executed) else 0.0,
            "avg_profit_loss": float(profit_losses.mean()),
            "total_profit_loss": float(profit_losses.sum()),
            "max_profit": float(profit_losses.max()),
            "max_loss": float(profit_losses.min()),
        }

    def get_signal_stats(self) -> Dict[str, Any]:
        """信号表整体统计（总数/股票数/按状态、策略、类型计数/时间范围）。

        与 get_indicator_stats 同样每次全量扫描，仅用于低频管理视图。
        """
        frame = self._read_event_frame("signals")
        if frame.empty:
            return {
                "total_signals": 0,
                "total_stocks": 0,
                "status_stats": {},
                "strategy_stats": {},
                "type_stats": {},
                "earliest_time": None,
                "latest_time": None,
            }

        status_stats = frame["status"].fillna("UNKNOWN").value_counts().to_dict() if "status" in frame.columns else {}
        strategy_stats = frame["strategy_name"].fillna("UNKNOWN").value_counts().to_dict() if "strategy_name" in frame.columns else {}
        type_stats = frame["signal_type"].fillna("UNKNOWN").value_counts().to_dict() if "signal_type" in frame.columns else {}
        earliest = pd.to_datetime(frame["datetime"], errors="coerce").min() if "datetime" in frame.columns else None
        latest = pd.to_datetime(frame["datetime"], errors="coerce").max() if "datetime" in frame.columns else None
        stock_count = int(frame["ts_code"].dropna().astype(str).nunique()) if "ts_code" in frame.columns else 0
        return {
            "total_signals": int(len(frame)),
            "total_stocks": stock_count,
            "status_stats": {str(key): int(value) for key, value in status_stats.items()},
            "strategy_stats": {str(key): int(value) for key, value in strategy_stats.items()},
            "type_stats": {str(key): int(value) for key, value in type_stats.items()},
            "earliest_time": earliest.isoformat() if pd.notna(earliest) else None,
            "latest_time": latest.isoformat() if pd.notna(latest) else None,
        }

    def update_signal_status(
        self,
        signal_id: int,
        status: str,
        executed_price: Optional[float] = None,
        profit_loss: Optional[float] = None,
    ) -> bool:
        """按 id 更新信号状态，返回是否找到并更新（False = 表空/无 id 列/该 id 不存在）。

        副作用：传 executed_price 会同时写入 executed_time；
        传 profit_loss 且表内有 trigger_price 时按触发价算出 profit_loss_pct。
        持锁整表读改写，会触发分区重写。
        """
        with self.locked("signals"):
            frame = self._read_event_frame("signals")
            if frame.empty or "id" not in frame.columns:
                return False
            mask = frame["id"] == signal_id
            if not mask.any():
                return False
            now = now_local()
            frame.loc[mask, "status"] = status
            frame.loc[mask, "updated_at"] = now
            if executed_price is not None:
                frame.loc[mask, "executed_price"] = executed_price
                frame.loc[mask, "executed_time"] = now
            if profit_loss is not None:
                frame.loc[mask, "profit_loss"] = profit_loss
                if "trigger_price" in frame.columns:
                    trigger = pd.to_numeric(frame.loc[mask, "trigger_price"], errors="coerce")
                    frame.loc[mask, "profit_loss_pct"] = (profit_loss / trigger) * 100
            self._rewrite_event_frame_unlocked("signals", frame)
        return True

    def expire_old_signals(self, hours: int = 24) -> int:
        """把超过 hours 小时仍为 ACTIVE 的信号置为 EXPIRED，返回条数。

        只有 status 列存在时才限定 ACTIVE；无该列则匹配全部超时信号。
        无命中时不写盘。
        """
        with self.locked("signals"):
            frame = self._read_event_frame("signals")
            if frame.empty or "datetime" not in frame.columns:
                return 0
            cutoff = now_local() - pd.Timedelta(hours=hours)
            mask = pd.to_datetime(frame["datetime"], errors="coerce") < cutoff
            if "status" in frame.columns:
                mask = mask & (frame["status"].fillna("") == "ACTIVE")
            expired_count = int(mask.sum())
            if expired_count:
                frame.loc[mask, "status"] = "EXPIRED"
                frame.loc[mask, "updated_at"] = now_local()
                self._rewrite_event_frame_unlocked("signals", frame)
        return expired_count

    def _rewrite_event_frame_unlocked(self, event_type: str, frame: pd.DataFrame) -> None:
        """整表重写：先按日期整分区替换，再删除多余分区。

        旧实现先 unlink 全部分区再重写，中途崩溃会把该事件类型的全部
        历史清空。改为"先写新、后删旧"后，任意时刻崩溃最多留下
        新旧混合的完整分区，不会丢数据。
        """
        root = self._event_dir(event_type)

        keep_days = set()
        if frame is not None and not frame.empty:
            normalized = frame.copy()
            normalized["datetime"] = pd.to_datetime(normalized["datetime"], errors="coerce")
            normalized = normalized.dropna(subset=["datetime"])
            if not normalized.empty:
                keep_days = set(normalized["datetime"].dt.date)
                self._write_event_frame_unlocked(event_type, normalized, merge_existing=False)

        # 只清理本次重写范围内不再存在的旧分区；重写为空（全部过期）时才全删
        for path in root.glob("year=*/month=*/day=*/data.parquet"):
            parts = {p.split("=")[0]: p.split("=")[1] for p in path.parts if "=" in p}
            try:
                day = datetime(
                    int(parts["year"]), int(parts["month"]), int(parts["day"])
                ).date()
            except (KeyError, ValueError):
                continue
            if day not in keep_days:
                try:
                    path.unlink()
                except FileNotFoundError:
                    pass
