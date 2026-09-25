"""扩展技术因子（stk_factor/daily 表的 derived 生产者）。

原 tushare 版（stk_factor.py）把 MACD/KDJ/RSI/BOLL/CCI 交给服务端算；
本脚本不依赖任何外部行情源，直接用已经落盘的日线 Parquet 自算：

    daily_history/daily（fuyao 已落盘）→ 本脚本 → stk_factor/daily

复权因子优先取本地数仓 tdx_data.duckdb 的 forward_factor；数仓被占用
（例如 tdx_db.py 正在入库）或不存在时退化为 1.0，此时 *_hfq/*_qfq 等于
未复权价，adj_factor 列写 1.0，不会报错。

指标口径按通达信/同花顺习惯（SMA(X,N,1) 等价于 alpha=1/N 的 EWM）：
    MACD  EMA12/EMA26 → DIF → DEA=EWM(DIF,9) → MACD=2*(DIF-DEA)
    KDJ   RSV=(C-L9)/(H9-L9)*100 → K=EWM(RSV,3) → D=EWM(K,3) → J=3K-2D
    RSI   Wilder 平滑（EWM alpha=1/N）
    BOLL  MA20 ± 2*STD20（总体标准差 ddof=0）
    CCI   TP=(H+L+C)/3 → MA14 → MD=MA14(|TP-MA|) → (TP-MA)/(0.015*MD)
"""

import os
import sys
from pathlib import Path

# 兼容直接运行（python app/utils/stk_factor_derived.py）
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import numpy as np
import pandas as pd
from loguru import logger

from app.utils.parquet_job_helpers import (
    _default_data_root,
    existing_partition_dates,
    resolve_trade_dates_with_gap_fill,
)
from app.utils.parquet_writer import save_to_parquet

REL_SOURCE = "daily_history/daily"
REL_TABLE = "stk_factor/daily"

# 指标预热窗口：MACD 需要 26+9，RSI24 需要 24，取 120 个交易日足够
PREHEAT_TRADING_DAYS = 120

DUCKDB_PATH = Path(
    os.getenv(
        "TDX_DB_PATH",
        os.path.join(os.path.dirname(_PROJECT_ROOT), "user", "数据", "tdx_data.duckdb"),
    )
)

OUTPUT_COLUMNS = [
    "ts_code", "trade_date", "close", "open", "high", "low", "pre_close",
    "change", "pct_change", "vol", "amount", "adj_factor",
    "open_hfq", "open_qfq", "close_hfq", "close_qfq",
    "high_hfq", "high_qfq", "low_hfq", "low_qfq",
    "pre_close_hfq", "pre_close_qfq",
    "macd_dif", "macd_dea", "macd",
    "kdj_k", "kdj_d", "kdj_j",
    "rsi_6", "rsi_12", "rsi_24",
    "boll_upper", "boll_mid", "boll_lower",
    "cci",
]


def _read_local_daily(data_dir: str | None = None) -> pd.DataFrame:
    """把 daily_history/daily 的全部已有分区读成一张长表。"""
    root = Path(data_dir or _default_data_root()) / REL_SOURCE
    if not root.exists():
        return pd.DataFrame()
    frames = []
    for parquet_file in sorted(root.rglob("data.parquet")):
        try:
            frames.append(pd.read_parquet(parquet_file))
        except Exception as exc:  # noqa: BLE001 - 坏分区跳过，不阻塞其它日期
            logger.warning(f"[stk_factor_derived] 分区读取失败，跳过 {parquet_file}: {exc}")
    if not frames:
        return pd.DataFrame()
    frame = pd.concat(frames, ignore_index=True)
    frame["trade_date"] = frame["trade_date"].astype(str)
    return frame


def _load_adjust_factors() -> dict[str, pd.Series]:
    """从本地数仓读取**全历史**前复权因子：{ts_code: Series(trade_date -> forward_factor)}。

    数仓 `lake_kline.forward_factor` 的语义是**前复权因子**：最新交易日为 1.0，
    越早的日期越小（实测 600519.SH 2021-08-02 = 0.864284）。
    因此：
        前复权价 P_qfq = P × forward_factor
        后复权价 P_hfq = P × (forward_factor / 该股序列最早一天的因子)
    必须取全历史序列（不能只取目标窗口），否则后复权的基准会落在窗口首日而不是上市至今。

    数仓不可用（被 tdx_db.py 独占、文件不存在、表缺失）时返回空 dict，
    调用方退化为 adj_factor = 1.0（等价不复权）。
    """
    if not DUCKDB_PATH.exists():
        logger.warning(f"[stk_factor_derived] 数仓不存在，adj_factor 退化为 1.0: {DUCKDB_PATH}")
        return {}
    try:
        import duckdb

        con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    except Exception as exc:  # noqa: BLE001 - 被占用/无权限都退化
        logger.warning(f"[stk_factor_derived] 数仓打开失败，adj_factor 退化为 1.0: {exc}")
        return {}
    try:
        frame = con.execute(
            "SELECT stock_code, trade_date, forward_factor FROM lake_kline "
            "WHERE forward_factor IS NOT NULL ORDER BY stock_code, trade_date"
        ).df()
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"[stk_factor_derived] 复权因子查询失败，退化为 1.0: {exc}")
        return {}
    finally:
        con.close()

    if frame.empty:
        return {}
    frame["trade_date"] = pd.to_datetime(frame["trade_date"]).dt.strftime("%Y%m%d")
    frame["forward_factor"] = pd.to_numeric(frame["forward_factor"], errors="coerce")
    frame = frame.dropna(subset=["forward_factor"])
    return {
        code: group.set_index("trade_date")["forward_factor"]
        for code, group in frame.groupby("stock_code", sort=False)
    }


def _compute_indicators(group: pd.DataFrame) -> pd.DataFrame:
    """对单只股票的时间序列（按 trade_date 升序）计算全部技术指标。"""
    frame = group.sort_values("trade_date").copy()
    close = frame["close"].astype(float)
    high = frame["high"].astype(float)
    low = frame["low"].astype(float)

    # ---- MACD ----
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    frame["macd_dif"] = ema12 - ema26
    frame["macd_dea"] = frame["macd_dif"].ewm(span=9, adjust=False).mean()
    frame["macd"] = (frame["macd_dif"] - frame["macd_dea"]) * 2

    # ---- KDJ（SMA(X,N,1) == ewm(alpha=1/N, adjust=False)）----
    low9 = low.rolling(9, min_periods=1).min()
    high9 = high.rolling(9, min_periods=1).max()
    span = (high9 - low9).replace(0, np.nan)
    rsv = ((close - low9) / span * 100).fillna(50.0)
    frame["kdj_k"] = rsv.ewm(alpha=1 / 3, adjust=False).mean()
    frame["kdj_d"] = frame["kdj_k"].ewm(alpha=1 / 3, adjust=False).mean()
    frame["kdj_j"] = 3 * frame["kdj_k"] - 2 * frame["kdj_d"]

    # ---- RSI（Wilder 平滑）----
    delta = close.diff()
    for period in (6, 12, 24):
        gain = delta.clip(lower=0).ewm(alpha=1 / period, adjust=False).mean()
        loss = (-delta.clip(upper=0)).ewm(alpha=1 / period, adjust=False).mean()
        rs = gain / loss.replace(0, np.nan)
        frame[f"rsi_{period}"] = (100 - 100 / (1 + rs)).fillna(100.0)

    # ---- BOLL ----
    mid = close.rolling(20, min_periods=1).mean()
    std = close.rolling(20, min_periods=1).std(ddof=0)
    frame["boll_mid"] = mid
    frame["boll_upper"] = mid + 2 * std
    frame["boll_lower"] = mid - 2 * std

    # ---- CCI ----
    tp = (high + low + close) / 3
    ma_tp = tp.rolling(14, min_periods=1).mean()
    md = (tp - ma_tp).abs().rolling(14, min_periods=1).mean()
    frame["cci"] = (tp - ma_tp) / (0.015 * md.replace(0, np.nan))

    return frame


def main() -> int:
    """作业入口：计算技术指标因子（stk_factor）并写分区。

    除目标日期外还会**往前多读一段做指标预热**：MA / MACD 等需要前置历史窗口，
    只读目标日会得到错误值。daily_history/daily 没有任何分区时返回 1。
    """
    data_dir = os.getenv("DATA_DIR")

    trade_dates, _ = resolve_trade_dates_with_gap_fill(REL_TABLE)
    if not trade_dates:
        print("[stk_factor_derived] 没有需要计算的交易日")
        return 0

    # 目标日期之外还要往前多读一段做指标预热
    all_dates = sorted(existing_partition_dates(REL_SOURCE, data_dir))
    if not all_dates:
        print("[stk_factor_derived] daily_history/daily 没有任何分区，请先跑 daily_history_fuyao")
        return 1

    targets = [d for d in trade_dates if d in set(all_dates)]
    if not targets:
        print(
            f"[stk_factor_derived] 目标交易日 {trade_dates[0]}~{trade_dates[-1]} "
            "在 daily_history 中不存在，跳过"
        )
        return 0

    first_needed = min(targets)
    keep = [d for d in all_dates if d <= max(targets)]
    warmup_start = keep[max(0, keep.index(first_needed) - PREHEAT_TRADING_DAYS)]

    raw = _read_local_daily(data_dir)
    if raw.empty:
        print("[stk_factor_derived] 日线为空，作业标记失败")
        return 1
    raw = raw[(raw["trade_date"] >= warmup_start) & (raw["trade_date"] <= max(targets))]
    if raw.empty:
        print("[stk_factor_derived] 预热窗口内无日线数据，作业标记失败")
        return 1

    adjust_map = _load_adjust_factors()

    parts = []
    for code, group in raw.groupby("ts_code", sort=False):
        computed = _compute_indicators(group)
        factors = adjust_map.get(code)
        if factors is not None:
            forward = computed["trade_date"].map(factors).astype(float)
        else:
            forward = pd.Series(1.0, index=computed.index)
        forward = forward.ffill().bfill().fillna(1.0)
        computed["_forward_factor"] = forward
        # 后复权因子 = 前复权因子 / 该股最早一天的前复权因子（基准归一到上市至今）
        base = float(factors.iloc[0]) if factors is not None and len(factors) else 1.0
        if not np.isfinite(base) or base == 0:
            base = 1.0
        computed["adj_factor"] = forward / base
        parts.append(computed)

    frame = pd.concat(parts, ignore_index=True)

    # 前复权（基准=最新交易日）与后复权（基准=最早）
    for column in ("open", "high", "low", "close", "pre_close"):
        price = frame[column].astype(float)
        frame[f"{column}_qfq"] = price * frame["_forward_factor"]
        frame[f"{column}_hfq"] = price * frame["adj_factor"]

    frame["pct_change"] = pd.to_numeric(frame.get("pct_chg"), errors="coerce")
    frame = frame[frame["trade_date"].isin(set(targets))]

    written = 0
    for trade_date, day_frame in frame.groupby("trade_date", sort=True):
        out = day_frame[OUTPUT_COLUMNS].copy()
        out["trade_date"] = trade_date
        written += save_to_parquet(out, trade_date, REL_TABLE, data_dir=data_dir)

    print(
        f"[stk_factor_derived] 完成，trade_days={len(targets)}, "
        f"total_upsert={written}, adj_source={'duckdb' if adjust_map else 'none(1.0)'}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
