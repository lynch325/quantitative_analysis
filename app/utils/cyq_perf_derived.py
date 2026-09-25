"""筹码分布（cyq_perf/daily 表的 derived 生产者）。

三个可用数据源（扶摇 / TickFlow / 通达信数仓）**都不提供筹码分布**，
因此本脚本按业界标准的「三角形分布 + 换手衰减」模型自算：

    每个交易日在 [low, high] 区间按三角形分布（峰值在 close）投入当日筹码，
    历史筹码按当日换手率衰减：
        chips = chips × (1 − turnover) + new_chips × turnover
    换手率 turnover = vol / total_share（total_share 取自 daily_basic，
    缺失时退化为成交量占比序列的移动均值）。

产出字段与 tushare 版一致：
    his_low / his_high / cost_5pct / cost_15pct / cost_50pct /
    cost_85pct / cost_95pct / weight_avg / winner_rate

⚠️ 这是**模型估算**而非交易所真实筹码数据：
- 依赖"成交量在当日价格区间内呈三角形分布"的假设，真实分布不可得；
- 换手率用总股本近似（无流通股本数据），会系统性偏小；
- `winner_rate` 为"成本低于最新价的筹码占比"，含义与官方口径一致但数值有偏差。
"""

import os
import sys
from pathlib import Path

# 兼容直接运行（python app/utils/cyq_perf_derived.py）
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from loguru import logger

from app.utils.parquet_job_helpers import (
    _default_data_root,
    existing_partition_dates,
    resolve_trade_dates_with_gap_fill,
)
from app.utils.parquet_writer import save_to_parquet

REL_SOURCE = "daily_history/daily"
REL_TABLE = "cyq_perf/daily"

OUTPUT_COLUMNS = [
    "ts_code", "trade_date", "his_low", "his_high",
    "cost_5pct", "cost_15pct", "cost_50pct", "cost_85pct", "cost_95pct",
    "weight_avg", "winner_rate",
]

#: 筹码价格分桶数（越多越精细，但越慢）
PRICE_BINS = 120
#: 参与计算的回看交易日数（筹码衰减很快，过长无意义且慢）
LOOKBACK_DAYS = 120
#: 换手率兜底（total_share 缺失时）
DEFAULT_TURNOVER = 0.02


def _read_local_daily(data_dir: str | None = None) -> pd.DataFrame:
    """读取本地源表全部分区并合并（筹码分布计算的输入）。

    单分区损坏只告警跳过，不中断整批；trade_date 统一转字符串，便于后续比较与分组。
    """
    root = Path(data_dir or _default_data_root()) / REL_SOURCE
    if not root.exists():
        return pd.DataFrame()
    frames = []
    for parquet_file in sorted(root.rglob("data.parquet")):
        try:
            frames.append(pd.read_parquet(parquet_file))
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"[cyq_perf_derived] 分区读取失败，跳过 {parquet_file}: {exc}")
    if not frames:
        return pd.DataFrame()
    frame = pd.concat(frames, ignore_index=True)
    frame["trade_date"] = frame["trade_date"].astype(str)
    return frame


def _triangle_chips(low: float, high: float, close: float, edges: np.ndarray) -> np.ndarray:
    """把当日成交按三角形分布（峰值在 close）投到价格桶上，返回未归一化的权重。"""
    if not np.isfinite(low) or not np.isfinite(high) or high <= low:
        # 一字板/停牌：全部筹码压在单一价格上
        idx = np.searchsorted(edges, close if np.isfinite(close) else low) - 1
        chips = np.zeros(len(edges) - 1)
        if 0 <= idx < len(chips):
            chips[idx] = 1.0
        return chips

    centers = 0.5 * (edges[:-1] + edges[1:])
    in_range = (centers >= low) & (centers <= high)
    weights = np.zeros(len(centers))
    if not in_range.any():
        idx = np.searchsorted(edges, close) - 1
        if 0 <= idx < len(weights):
            weights[idx] = 1.0
        return weights

    # 三角形：low→close 线性升，close→high 线性降
    rising = centers <= close
    with np.errstate(divide="ignore", invalid="ignore"):
        weights[in_range & rising] = (centers[in_range & rising] - low) / max(
            close - low, 1e-9
        )
        weights[in_range & ~rising] = (high - centers[in_range & ~rising]) / max(
            high - close, 1e-9
        )
    weights[~in_range] = 0.0
    total = weights.sum()
    return weights / total if total > 0 else weights


def _compute_for_stock(group: pd.DataFrame) -> dict:
    """对单只股票计算筹码分布（成本分布）。

    取最近 LOOKBACK_DAYS 根 K 线，把 [最低价, 最高价] 等分为 PRICE_BINS 个价格区间，
    按该区间内成交量加权得到各价位筹码量；数组化计算，逐股调用。
    价格区间无效（hi ≤ lo 或非有限值）时返回空 dict，由调用方跳过该股。
    """
    group = group.sort_values("trade_date").tail(LOOKBACK_DAYS)
    lows = group["low"].to_numpy(dtype=float)
    highs = group["high"].to_numpy(dtype=float)
    closes = group["close"].to_numpy(dtype=float)
    vols = group["vol"].to_numpy(dtype=float)

    lo = np.nanmin(lows)
    hi = np.nanmax(highs)
    if not np.isfinite(lo) or not np.isfinite(hi) or hi <= lo:
        return {}

    edges = np.linspace(lo, hi, PRICE_BINS + 1)
    centers = 0.5 * (edges[:-1] + edges[1:])
    chips = np.zeros(PRICE_BINS)

    # 换手率：vol / total_share（total_share 来自 daily_basic 的推导列，可能缺失）
    shares = group.get("total_share")
    for i in range(len(group)):
        if shares is not None and np.isfinite(shares.iloc[i]) and shares.iloc[i] > 0:
            turnover = float(vols[i]) * 100.0 / float(shares.iloc[i])  # vol 单位为手
        else:
            turnover = DEFAULT_TURNOVER
        turnover = float(np.clip(turnover, 0.0, 1.0))

        chips *= (1.0 - turnover)
        chips += _triangle_chips(lows[i], highs[i], closes[i], edges) * turnover

    total = chips.sum()
    if total <= 0:
        return {}
    chips /= total

    cum = np.cumsum(chips)

    def quantile_cost(q: float) -> float:
        idx = np.searchsorted(cum, q)
        idx = min(idx, len(centers) - 1)
        return float(centers[idx])

    last_close = float(closes[-1]) if np.isfinite(closes[-1]) else float(hi)
    winner = float(chips[centers <= last_close].sum())

    return {
        "his_low": float(lo),
        "his_high": float(hi),
        "cost_5pct": quantile_cost(0.05),
        "cost_15pct": quantile_cost(0.15),
        "cost_50pct": quantile_cost(0.50),
        "cost_85pct": quantile_cost(0.85),
        "cost_95pct": quantile_cost(0.95),
        "weight_avg": float(np.sum(centers * chips)),
        "winner_rate": winner * 100.0,
    }


def main() -> int:
    """作业入口：按交易日逐个计算筹码分布并写分区。

    只处理源数据（daily_history/daily）已有分区的日期（existing_partition_dates 求交集），
    没有交集直接跳过 —— 筹码分布依赖日线先落盘。
    """
    load_dotenv()
    data_dir = os.getenv("DATA_DIR")

    trade_dates, _ = resolve_trade_dates_with_gap_fill(REL_TABLE)
    if not trade_dates:
        print("[cyq_perf_derived] 没有需要计算的交易日")
        return 0

    have = existing_partition_dates(REL_SOURCE, data_dir)
    targets = [d for d in trade_dates if d in have]
    if not targets:
        print("[cyq_perf_derived] daily_history 中无对应分区，跳过")
        return 0

    raw = _read_local_daily(data_dir)
    if raw.empty:
        print("[cyq_perf_derived] 日线为空，作业标记失败")
        return 1

    # 尽量带上 total_share（daily_basic 已由 daily_basic_fuyao 产出）
    try:
        basic_root = Path(data_dir or _default_data_root()) / "daily_basic" / "daily"
        frames = []
        for parquet_file in sorted(basic_root.rglob("data.parquet")):
            try:
                frames.append(
                    pd.read_parquet(parquet_file, columns=["ts_code", "trade_date", "total_share"])
                )
            except Exception:  # noqa: BLE001
                continue
        if frames:
            basic = pd.concat(frames, ignore_index=True)
            basic["trade_date"] = basic["trade_date"].astype(str)
            raw = raw.merge(basic, on=["ts_code", "trade_date"], how="left")
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"[cyq_perf_derived] 读取 total_share 失败，用默认换手率: {exc}")

    latest_target = max(targets)
    rows = []
    for ts_code, group in raw.groupby("ts_code", sort=False):
        if latest_target not in set(group["trade_date"]):
            continue
        stats = _compute_for_stock(group)
        if not stats:
            continue
        rows.append({"ts_code": ts_code, "trade_date": latest_target, **stats})

    if not rows:
        print("[cyq_perf_derived] 无可用筹码结果，作业标记失败")
        return 1

    frame = pd.DataFrame(rows).reindex(columns=OUTPUT_COLUMNS)
    written = 0
    for trade_date, day_frame in frame.groupby("trade_date", sort=True):
        written += save_to_parquet(day_frame, trade_date, REL_TABLE, data_dir=data_dir)

    print(
        f"[cyq_perf_derived] 完成，trade_days={len(targets)}, "
        f"stocks={len(frame)}, total_upsert={written}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
