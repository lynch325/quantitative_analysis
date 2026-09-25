"""日线基本指标（daily_basic/daily 表的 fuyao + 数仓 + 自算生产者）。

与 tushare 版（daily_basic.py）写入同一张 daily_basic/daily、同一套 schema。

字段来源（按可获得性拼装，拿不到的列留 NaN，不伪造）：

| 列 | 来源 |
|---|---|
| `ts_code` / `trade_date` / `close` | 已落盘的 `daily_history/daily`（扶摇日线） |
| `vol` → `volume_ratio` | 同上，量比 = 当日量 / 前 5 个交易日均量 |
| `total_mv` | 本地数仓 `lake_gpjy.GP16_1`（总市值，万元 → 元） |
| `total_share` | `total_mv / close` 推导 |
| `pe_ttm` / `pb` / `ps_ttm` | 扶摇 `valuations_snapshot`（**仅最新交易日**有值） |

⚠️ 与 tushare 版的差异（重要）：
- `turnover_rate` / `turnover_rate_f`（换手率）需要**流通股本**，三源均无 → 留 NaN；
- `float_share` / `free_share` / `circ_mv`（流通股本与流通市值）同上 → 留 NaN；
- `pe` / `ps` / `dv_ratio` / `dv_ttm`（静态市盈率、市销率、股息率）扶摇不提供 → 留 NaN；
- 估值类字段只有**最新交易日**有值，历史日期为 NaN（扶摇估值接口是实时快照）。
"""

import os
import sys
from pathlib import Path

# 兼容直接运行（python app/utils/daily_basic_fuyao.py）
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
REL_TABLE = "daily_basic/daily"

OUTPUT_COLUMNS = [
    "ts_code", "trade_date", "close", "turnover_rate", "turnover_rate_f",
    "volume_ratio", "pe", "pe_ttm", "pb", "ps", "ps_ttm",
    "dv_ratio", "dv_ttm", "total_share", "float_share", "free_share",
    "total_mv", "circ_mv",
]

#: 量比回看窗口（交易日）
VOLUME_RATIO_WINDOW = 5

DUCKDB_PATH = Path(
    os.getenv(
        "TDX_DB_PATH",
        os.path.join(os.path.dirname(_PROJECT_ROOT), "user", "数据", "tdx_data.duckdb"),
    )
)


def _read_local_daily(data_dir: str | None = None) -> pd.DataFrame:
    """读取本地源表全部分区并合并，作为每日指标计算的输入。

    单分区读取失败只告警跳过；trade_date 统一转字符串。
    """
    root = Path(data_dir or _default_data_root()) / REL_SOURCE
    if not root.exists():
        return pd.DataFrame()
    frames = []
    for parquet_file in sorted(root.rglob("data.parquet")):
        try:
            frames.append(pd.read_parquet(parquet_file))
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"[daily_basic_fuyao] 分区读取失败，跳过 {parquet_file}: {exc}")
    if not frames:
        return pd.DataFrame()
    frame = pd.concat(frames, ignore_index=True)
    frame["trade_date"] = frame["trade_date"].astype(str)
    return frame


def _load_total_mv() -> pd.DataFrame:
    """从本地数仓取**全历史**总市值：列 ts_code / trade_date / total_mv（元）。

    数仓 GP16_1 单位是万元，这里 ×10000 转成元。数仓不可用返回空表。

    ⚠️ 数仓 GP16 比日线**滞后数个交易日**（实测日线到 09-11 时 GP16 只到 09-09），
    因此调用方须用「截至目标日最近一次」的方式对齐（merge_asof backward），
    不能要求目标日当天必须有值。
    """
    if not DUCKDB_PATH.exists():
        logger.warning(f"[daily_basic_fuyao] 数仓不存在，total_mv 留空: {DUCKDB_PATH}")
        return pd.DataFrame()
    try:
        import duckdb

        con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"[daily_basic_fuyao] 数仓打开失败，total_mv 留空: {exc}")
        return pd.DataFrame()
    try:
        frame = con.execute(
            "SELECT stock_code, trade_date, GP16_1 FROM lake_gpjy "
            "WHERE GP16_1 IS NOT NULL"
        ).df()
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"[daily_basic_fuyao] GP16 查询失败，total_mv 留空: {exc}")
        return pd.DataFrame()
    finally:
        con.close()

    if frame.empty:
        return pd.DataFrame()
    # DuckDB 返回的列名保留原始大小写（GP16_1），这里不区分大小写地定位
    mv_col = next((c for c in frame.columns if str(c).lower() == "gp16_1"), None)
    if mv_col is None:
        logger.warning("[daily_basic_fuyao] lake_gpjy 缺少 GP16_1 列，total_mv 留空")
        return pd.DataFrame()
    frame["trade_date"] = pd.to_datetime(frame["trade_date"]).dt.strftime("%Y%m%d")
    frame = frame.rename(columns={"stock_code": "ts_code"})
    frame[mv_col] = pd.to_numeric(frame[mv_col], errors="coerce")
    frame = frame.dropna(subset=[mv_col])
    if frame.empty:
        return pd.DataFrame()
    frame["total_mv"] = frame[mv_col] * 10000.0
    frame = frame[["ts_code", "trade_date", "total_mv"]].sort_values(
        ["ts_code", "trade_date"]
    )
    return frame


def _load_fuyao_valuations(ts_codes: list[str]) -> pd.DataFrame:
    """拉扶摇估值快照（单次 ≤100 只，分批）。失败返回空表。"""
    try:
        from app.utils.data_sources.fuyao_client import FuyaoClient
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"[daily_basic_fuyao] 扶摇客户端不可用: {exc}")
        return pd.DataFrame()

    client = FuyaoClient()
    rows = []
    for start in range(0, len(ts_codes), 100):
        batch = ts_codes[start:start + 100]
        try:
            rows.extend(client.valuations_snapshot(batch) or [])
        except Exception as exc:  # noqa: BLE001 - 单批失败不阻断
            logger.warning(f"[daily_basic_fuyao] 估值批次失败: {exc}")
    if not rows:
        return pd.DataFrame()
    frame = pd.DataFrame(rows)
    if "thscode" not in frame.columns:
        return pd.DataFrame()
    renamed = frame.rename(columns={"thscode": "ts_code"})
    keep = [c for c in ("ts_code", "pe_ttm", "pb_mrq", "ps_ttm") if c in renamed.columns]
    return renamed[keep]


def main() -> int:
    """作业入口：计算每日指标（换手率 / 总市值等）并按日分区写入。

    交易日由 resolve_trade_dates_with_gap_fill 决定（含缺口回补）；
    仅对源数据已有分区的日期计算，无则跳过。
    """
    load_dotenv()
    data_dir = os.getenv("DATA_DIR")

    trade_dates, _ = resolve_trade_dates_with_gap_fill(REL_TABLE)
    if not trade_dates:
        print("[daily_basic_fuyao] 没有需要计算的交易日")
        return 0

    have = existing_partition_dates(REL_SOURCE, data_dir)
    targets = [d for d in trade_dates if d in have]
    if not targets:
        print("[daily_basic_fuyao] daily_history 中无对应分区，跳过")
        return 0

    raw = _read_local_daily(data_dir)
    if raw.empty:
        print("[daily_basic_fuyao] 日线为空，作业标记失败")
        return 1

    # 量比需要历史窗口，多读一段
    all_dates = sorted(have)
    first_idx = all_dates.index(min(targets))
    warm_start = all_dates[max(0, first_idx - VOLUME_RATIO_WINDOW)]
    raw = raw[raw["trade_date"] >= warm_start]
    raw = raw.sort_values(["ts_code", "trade_date"])

    # 量比：当日量 / 前 VOLUME_RATIO_WINDOW 日均量（不含当日）
    raw["_prev_mean_vol"] = (
        raw.groupby("ts_code", sort=False)["vol"]
        .transform(lambda s: s.shift(1).rolling(VOLUME_RATIO_WINDOW, min_periods=1).mean())
    )
    raw["volume_ratio"] = raw["vol"] / raw["_prev_mean_vol"].replace(0, np.nan)

    frame = raw[raw["trade_date"].isin(set(targets))].copy()
    frame = frame[["ts_code", "trade_date", "close", "vol", "volume_ratio"]]

    # 总市值（数仓 GP16）：按股票取「截至该日最近一次」的值
    # （GP16 比日线滞后数日，目标日当天常无数据，直接 merge 会全空）
    mv = _load_total_mv()
    if not mv.empty:
        # merge_asof 的排序列必须是数值/时间类型，YYYYMMDD 字符串不行 → 转整数键
        left = frame[["ts_code", "trade_date"]].drop_duplicates().copy()
        left["_sort_key"] = left["trade_date"].astype(int)
        # 右侧不能带 trade_date：会与左侧同名列冲突（merge_asof 会产生 _x/_y 后缀）
        right = mv[["ts_code", "trade_date", "total_mv"]].copy()
        right["_sort_key"] = right["trade_date"].astype(int)
        right = right.drop(columns=["trade_date"])
        asof = pd.merge_asof(
            left.sort_values("_sort_key"),
            right.sort_values("_sort_key"),
            on="_sort_key",
            by="ts_code",
            direction="backward",
        )
        # 必须保留 ts_code + trade_date 双键再合并：
        # 只按 ts_code 合并会让单行目标匹配到 asof 中的多天记录，行数成倍膨胀
        asof = asof.drop(columns=["_sort_key"], errors="ignore")
        frame = frame.merge(asof, on=["ts_code", "trade_date"], how="left")
    else:
        frame["total_mv"] = np.nan
    frame["total_share"] = frame["total_mv"] / frame["close"].replace(0, np.nan)

    # 估值：仅对最新交易日拉扶摇快照
    latest_target = max(targets)
    frame["pe_ttm"] = np.nan
    frame["pb"] = np.nan
    frame["ps_ttm"] = np.nan
    try:
        codes = sorted(frame.loc[frame["trade_date"] == latest_target, "ts_code"].unique())
        val = _load_fuyao_valuations(codes)
        if not val.empty:
            mask = frame["trade_date"] == latest_target
            merged = frame.loc[mask, ["ts_code"]].merge(val, on="ts_code", how="left")
            for src, dst in (("pe_ttm", "pe_ttm"), ("pb_mrq", "pb"), ("ps_ttm", "ps_ttm")):
                if src in merged.columns:
                    frame.loc[mask, dst] = merged[src].to_numpy()
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"[daily_basic_fuyao] 估值获取失败，留空: {exc}")

    # 无源的列保持 NaN
    for column in (
        "turnover_rate", "turnover_rate_f", "pe", "ps",
        "dv_ratio", "dv_ttm", "float_share", "free_share", "circ_mv",
    ):
        frame[column] = np.nan

    written = 0
    for trade_date, day_frame in frame.groupby("trade_date", sort=True):
        out = day_frame.reindex(columns=OUTPUT_COLUMNS).copy()
        out["ts_code"] = day_frame["ts_code"].to_numpy()
        out["trade_date"] = trade_date
        written += save_to_parquet(out, trade_date, REL_TABLE, data_dir=data_dir)

    print(f"[daily_basic_fuyao] 完成，trade_days={len(targets)}, total_upsert={written}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
