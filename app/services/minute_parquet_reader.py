"""分钟线 Parquet 读取器：data/stock_minute/{period}/year=/month=/day=/data.parquet。

与日线读取器（app.services.data_reader）是两套独立实现，差异点：
- 时间粒度到分钟，datetime 列统一去时区（见 _normalize_dt）后再比较；
- 按 period_type（1min/5min/...）分目录，读取时逐分区整读后内存过滤，
  未做列裁剪 / 谓词下推——分钟分区体量远小于日线（单分区百 KB 级），
  暂未构成瓶颈，若数据量增长需按 data_reader 的做法改造；
- 支持「最新分区回落」：非交易时段或当日尚未同步时用
  get_latest_partition_date 找到最后一份数据，避免实时模块整体返回空。
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, Optional
from datetime import timezone

import pandas as pd
from loguru import logger


class MinuteParquetReader:
    """Read minute-level stock data from partitioned parquet files."""

    def __init__(self, data_dir: str | None = None):
        if data_dir is None:
            data_dir = os.getenv(
                "DATA_DIR",
                os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data"),
            )
        self.data_dir = data_dir

    def get_data(
        self,
        ts_code: str | None = None,
        period_type: str | None = None,
        start_time: str | datetime | None = None,
        end_time: str | datetime | None = None,
    ) -> pd.DataFrame:
        """按代码 / 周期 / 时间窗读取分钟线，返回升序 DataFrame（无数据返回空表）。

        ts_code 兼容多种写法（600000.SH / sh.600000 / 6 位纯数字），由
        _minute_code_aliases 展开别名后匹配；period_type 为 None 时扫描全部
        周期目录。单个分区读取失败只记 warning 并跳过，不影响其余分区。
        读取顺序固定为 (datetime, ts_code) 升序，调用方无需再排序。
        """
        frames: list[pd.DataFrame] = []
        for parquet_path in self._walk_parquet_files(period_type, start_time, end_time):
            try:
                df = pd.read_parquet(parquet_path)
            except Exception as exc:
                logger.warning(f"读取分钟 parquet 失败 {parquet_path}: {exc}")
                continue
            if not df.empty:
                frames.append(df)

        if not frames:
            return pd.DataFrame()

        result = pd.concat(frames, ignore_index=True)
        if "datetime" in result.columns:
            result["datetime"] = pd.to_datetime(result["datetime"], errors="coerce")
            if getattr(result["datetime"].dt, "tz", None) is not None:
                result["datetime"] = result["datetime"].dt.tz_localize(None)
            result = result.dropna(subset=["datetime"])
        if ts_code is not None and "ts_code" in result.columns:
            candidate_codes = _minute_code_aliases(ts_code)
            result = result[result["ts_code"].astype(str).isin(candidate_codes)]
        if period_type is not None and "period_type" in result.columns:
            result = result[result["period_type"] == period_type]
        if start_time is not None:
            start_dt = _normalize_dt(start_time)
            result = result[result["datetime"] >= start_dt]
        if end_time is not None:
            end_dt = _normalize_dt(end_time)
            result = result[result["datetime"] <= end_dt]
        if "datetime" in result.columns:
            result = result.sort_values(["datetime", "ts_code"], kind="stable").reset_index(drop=True)
        return result

    def get_latest_partition_date(self, period_type: Optional[str] = None) -> Optional[datetime]:
        """返回最新可用分钟分区的日期（当日 00:00）；无任何分区时返回 None。

        用途：调用方以「当前时间」为窗口查不到数据时（非交易时段，或分钟线
        尚未同步到今天），可回落到「最新一份数据」，避免整个实时模块返回空。
        """
        latest: Optional[datetime] = None
        for parquet_path in self._walk_parquet_files(period_type, None, None):
            day_dir = parquet_path.parent
            y = _partition_value(day_dir.parent.parent.name, "year")
            m = _partition_value(day_dir.parent.name, "month")
            d = _partition_value(day_dir.name, "day")
            if not (y and m and d):
                continue
            try:
                current = datetime.strptime(f"{y}-{m}-{d}", "%Y-%m-%d")
            except ValueError:
                continue
            if latest is None or current > latest:
                latest = current
        return latest

    def get_latest_data(self, ts_code: str, period_type: str = "1min", limit: int = 100) -> pd.DataFrame:
        """取最近 limit 根分钟线；返回结果按时间**倒序**（最新在前）。"""
        df = self.get_data(ts_code=ts_code, period_type=period_type)
        if df.empty:
            return df
        return df.sort_values("datetime", ascending=False).head(limit).reset_index(drop=True)

    def get_summary(self, ts_code: str, period_type: str = "1min", hours: int = 24) -> dict[str, object]:
        """过去 hours 小时的分钟数据概况，供实时模块展示「数据是否就绪」。

        返回字段：has_data / data_count / latest_time / earliest_time /
        missing_count / completeness / status / message。
        注意 missing_count 与 completeness 目前是固定值（0 与 100.0），
        并未按交易日历推算真实缺口，只表示「窗口内有数据」，勿当作完整性指标使用。
        """
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours)
        df = self.get_data(ts_code=ts_code, period_type=period_type, start_time=start_time, end_time=end_time)
        if df.empty:
            return {
                "has_data": False,
                "data_count": 0,
                "latest_time": None,
                "earliest_time": None,
                "missing_count": 0,
                "completeness": 0.0,
                "status": "no_data",
                "message": f"没有找到 {ts_code} 在过去 {hours} 小时的 {period_type} 数据",
            }

        latest_time = pd.to_datetime(df["datetime"]).max().to_pydatetime().isoformat()
        earliest_time = pd.to_datetime(df["datetime"]).min().to_pydatetime().isoformat()
        return {
            "has_data": True,
            "data_count": int(len(df)),
            "latest_time": latest_time,
            "earliest_time": earliest_time,
            "missing_count": 0,
            "completeness": 100.0,
            "status": "ok",
            "message": f"数据完整性: {100.0:.1f}%",
        }

    def _walk_parquet_files(
        self,
        period_type: str | None,
        start_time: str | datetime | None,
        end_time: str | datetime | None,
    ) -> Iterable[Path]:
        """生成窗口内的分钟分区文件路径。

        日期比较只到「天」粒度（分区目录本身按天），窗口内的时分精度由
        调用方在读取结果上再过滤；period_type 为 None 时遍历全部周期子目录。
        """
        base = Path(self.data_dir) / "stock_minute"
        if period_type:
            bases = [base / period_type]
        else:
            bases = [path for path in base.iterdir() if path.is_dir()] if base.exists() else []

        start_dt = _normalize_dt(start_time) if start_time is not None else None
        end_dt = _normalize_dt(end_time) if end_time is not None else None

        for period_base in bases:
            if not period_base.exists():
                continue
            for year_dir in sorted(_partition_dirs(period_base, "year")):
                y = _partition_value(year_dir.name, "year")
                if y is None:
                    continue
                for month_dir in sorted(_partition_dirs(year_dir, "month")):
                    m = _partition_value(month_dir.name, "month")
                    if m is None:
                        continue
                    for day_dir in sorted(_partition_dirs(month_dir, "day")):
                        d = _partition_value(day_dir.name, "day")
                        if d is None:
                            continue
                        day_str = f"{y}-{m}-{d}"
                        if start_dt and day_str < start_dt.strftime("%Y-%m-%d"):
                            continue
                        if end_dt and day_str > end_dt.strftime("%Y-%m-%d"):
                            continue
                        parquet_path = day_dir / "data.parquet"
                        if parquet_path.is_file():
                            yield parquet_path


def _partition_dirs(parent: Path, key: str) -> list[Path]:
    """列出 parent 下形如 `{key}=...` 的分区子目录；目录不存在返回空列表。"""
    if not parent.exists():
        return []
    return [path for path in parent.iterdir() if path.is_dir() and path.name.startswith(f"{key}=")]


def _partition_value(dir_name: str, key: str) -> Optional[str]:
    """从分区目录名解析分量值；month/day 单数字补零（`day=1` → `'01'`）。

    非该 key 的目录名返回 None，调用方据此跳过无关目录。
    """
    prefix = f"{key}="
    if not dir_name.startswith(prefix):
        return None
    value = dir_name[len(prefix):]
    if key in {"month", "day"} and len(value) == 1:
        value = f"0{value}"
    return value


def _normalize_dt(value: str | datetime) -> datetime:
    """把多种时间输入统一成**无时区** datetime，便于与 parquet 列直接比较。

    接受 datetime、YYYY-MM-DD、YYYYMMDD 与 ISO 串；带时区的一律转 UTC 后
    去掉 tzinfo（与 get_data 中去时区的处理保持同一口径）。
    """
    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo is not None else value
    text = str(value)
    if len(text) == 10 and text[4] == "-":
        dt = datetime.fromisoformat(text)
        return dt.replace(tzinfo=None) if dt.tzinfo is not None else dt
    if len(text) == 8 and text.isdigit():
        return datetime.strptime(text, "%Y%m%d")
    dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    return dt.astimezone(timezone.utc).replace(tzinfo=None) if dt.tzinfo is not None else dt


def _minute_code_aliases(value: str) -> set[str]:
    """展开分钟表里可能出现的代码写法，供 ts_code 精确匹配使用。

    分钟表历史数据同时存在 `600000.SH` 与 `sh.600000` 两种写法；前端还会直接传
    6 位纯数字（按 6 开头判 SH，其余判 SZ，不含北交所）。返回全部候选别名，
    调用方用 isin 过滤；无法识别的写法只返回原值（大小写两种）。
    """
    text = str(value).strip()
    lower_text = text.lower()
    aliases = {text, lower_text}

    # 兼容前端直接传入的 6 位纯数字代码（例如 300502）
    if lower_text.isdigit() and len(lower_text) == 6:
        market = 'SH' if lower_text.startswith('6') else 'SZ'
        aliases.add(f"{lower_text}.{market}")
        aliases.add(f"{market.lower()}.{lower_text}")

    if lower_text.startswith(("sh.", "sz.")):
        market, symbol = lower_text.split(".", 1)
        aliases.add(f"{symbol}.{market.upper()}")
    elif lower_text.endswith(".sh") or lower_text.endswith(".sz"):
        symbol, market = lower_text.split(".", 1)
        aliases.add(f"{market}.{symbol}")
        aliases.add(f"{symbol}.{market.upper()}")

    return aliases
