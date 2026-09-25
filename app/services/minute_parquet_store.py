"""分钟线写盘：按 (日期, period_type) 分区写入 `stock_minute/{period}/year=/month=/day=/data.parquet`。

写入语义（与读取端 minute_parquet_reader 对称）：
- 先按「日期 + period_type」分组再落盘——一天内混入多周期时必须分组，
  否则会按第一行的 period_type 把其余周期写进错误目录；
- 每个分区是**读-合并-写**：合并既有分区后按 (ts_code, datetime, period_type)
  去重 keep=last，再用 atomic_write_parquet 原子替换，崩溃不会留半个文件；
- 全程持分区级文件锁（同步任务与聚合任务可能并发写同一天）。

其它约定：`datetime` 列必填（缺失抛 ValueError），解析失败的行被丢弃；
Windows 无 fcntl，锁退化为空操作，单机单进程场景仍安全。
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
from loguru import logger

try:
    import fcntl
except ImportError:  # Windows 无 fcntl，锁退化为无操作（单机单进程场景仍安全）
    fcntl = None

from app.utils.parquet_writer import (
    atomic_write_parquet,
    parquet_file_lock,
    quarantine_corrupt_parquet,
)


class MinuteParquetStore:
    """Persist minute-level rows to partitioned parquet files."""

    def __init__(self, data_dir: str | None = None):
        if data_dir is None:
            data_dir = os.getenv(
                "DATA_DIR",
                os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data"),
            )
        self.data_dir = data_dir

    def write_frame(self, frame: pd.DataFrame, period_type: str) -> int:
        """写入分钟数据，按 (日期, period_type) 分组落到各自分区，返回写入行数。

        **按 period_type 一起分组是刻意的**：一天内混入多周期数据时，
        旧实现用 iloc[0] 的 period_type 决定分区路径，其余周期的数据会被写进错误目录。
        datetime 缺失或不可解析的行直接丢弃。
        """
        if frame is None or frame.empty:
            return 0

        if "datetime" not in frame.columns:
            raise ValueError("minute parquet frame requires datetime column")

        df = frame.copy()
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
        df = df.dropna(subset=["datetime"])
        if df.empty:
            return 0

        if "period_type" not in df.columns:
            df["period_type"] = period_type

        total_rows = 0
        # 按 (日期, period_type) 分组：一天内混入多周期数据时，
        # 旧实现取 iloc[0] 的 period_type 决定分区路径，其余周期会被写错目录
        group_keys = ["period_type"] if "period_type" in df.columns else []
        for (date_value, *_), day_df in df.groupby([df["datetime"].dt.date] + group_keys):
            total_rows += self._write_day_frame(day_df, date_value)
        return total_rows

    def _write_day_frame(self, day_df: pd.DataFrame, date_value) -> int:
        """写入单个交易日的分区（读-合并-写 + 原子替换）。

        - 全程持**分区级文件锁**：同步任务与聚合任务可能并发写同一天；
        - 按 (ts_code, datetime, period_type) 去重并保留最后一条；
        - 先写临时文件再原子替换，避免写一半崩溃留下坏分区。
        """
        year = f"{date_value.year:04d}"
        month = f"{date_value.month:02d}"
        day = f"{date_value.day:02d}"

        partition_dir = Path(self.data_dir) / "stock_minute" / str(day_df.iloc[0]["period_type"]) / f"year={year}" / f"month={month}" / f"day={day}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        parquet_path = partition_dir / "data.parquet"

        # 同步任务与聚合任务可能并发写同一日分区：读-合并-写全程持锁，
        # 避免后写者覆盖先写者
        with self._partition_lock(parquet_path):
            if parquet_path.is_file():
                existing = self._read_existing_partition(parquet_path)
                combined = pd.concat([existing, day_df], ignore_index=True)
            else:
                combined = day_df.copy()

            if "datetime" in combined.columns:
                combined["datetime"] = pd.to_datetime(combined["datetime"], errors="coerce")
            dedup_keys = [key for key in ["ts_code", "datetime", "period_type"] if key in combined.columns]
            if dedup_keys:
                combined = combined.drop_duplicates(subset=dedup_keys, keep="last")
            combined = combined.sort_values([col for col in ["ts_code", "datetime"] if col in combined.columns]).reset_index(drop=True)
            # 原子替换：直接 to_parquet 写一半崩溃会留下坏文件，
            # 下次读取时该日既有数据被当成不存在
            atomic_write_parquet(combined, str(parquet_path))
        logger.info(f"写入分钟 parquet: {parquet_path} ({len(combined)} 行)")
        return len(combined)

    def _read_existing_partition(self, parquet_path: Path) -> pd.DataFrame:
        """读取待合并的既有分区。

        **坏文件不能静默当空表**：合并写入会把该分区已有数据整体覆盖掉；
        因此先隔离（quarantine）坏文件再返回空表 —— 数据可以人工恢复，静默丢失不可恢复。
        """
        try:
            return pd.read_parquet(parquet_path)
        except Exception as exc:
            # 坏文件不能静默当空表：合并写入会把该分区既有数据覆盖掉
            quarantine_corrupt_parquet(
                parquet_path,
                lambda msg, err=exc: logger.error(f"分钟 {msg}: {err}"),
            )
            return pd.DataFrame()

    def _partition_lock(self, parquet_path: Path):
        lock_path = parquet_path.with_name(f"{parquet_path.name}.lock")
        return parquet_file_lock(lock_path)
