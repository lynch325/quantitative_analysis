"""板块热力图数据源 —— 生成 data/data.parquet。

背景：HeatmapService（app/services/heatmap_service.py:24）读取 `data/data.parquet`，
但该文件历史上没有任何生产者，导致板块热力图页面一直报
"数据加载失败，请确认 data/data.parquet 是否存在"。

本脚本按 HeatmapService 需要的 schema（见 tests/services/test_heatmap_service.py
的 sample_df fixture）拼装这 9 列，全部取自本地已有的 Parquet：

| 列 | 来源 |
|---|---|
| `ts_code` / `close` / `pct_chg` / `trade_date` | `daily_history/daily` 最新分区 |
| `total_mv` / `turnover_rate` | `daily_basic/daily` 最新分区 |
| `net_mf_amount` | `moneyflow/daily` 最新分区 |
| `name` / `industry` | `stock_basic.parquet`（industry 由数仓板块体系回填） |

只产出**最新一个交易日**（HeatmapService 取 df['trade_date'].iloc[0]），整文件覆盖。

⚠️ 已知：turnover_rate 目前恒为 NaN（三源无流通股本，见问题清单 A7），
热力图会显示为 0/空，不影响板块聚合（聚合只用 pct_chg + total_mv + net_mf_amount）。
"""

import os
import sys
from pathlib import Path

# 兼容直接运行（python app/utils/heatmap_data_builder.py）
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import pandas as pd
from dotenv import load_dotenv
from loguru import logger

from app.utils.parquet_writer import save_single_parquet

REL_FILENAME = "data.parquet"

OUTPUT_COLUMNS = [
    "ts_code", "name", "industry", "pct_chg", "close",
    "total_mv", "net_mf_amount", "turnover_rate", "trade_date",
]


def _read_latest_partition(rel_table: str, data_dir: str | None = None) -> pd.DataFrame:
    """读取某张日频表的**最新分区**；不存在返回空表。"""
    from app.utils.parquet_job_helpers import _default_data_root

    root = Path(data_dir or _default_data_root()) / rel_table
    if not root.exists():
        return pd.DataFrame()
    files = sorted(root.rglob("data.parquet"))
    if not files:
        return pd.DataFrame()
    try:
        frame = pd.read_parquet(files[-1])
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"[heatmap_data_builder] {rel_table} 最新分区读取失败: {exc}")
        return pd.DataFrame()
    frame["trade_date"] = frame["trade_date"].astype(str)
    return frame


def main() -> int:
    """作业入口：构建板块热力图数据并落盘。

    以最新日线分区为基准（日线为空则作业失败返回 1），
    再合并每日指标（总市值 / 换手率）；指标缺失不阻断，只是少几列。
    """
    load_dotenv()
    data_dir = os.getenv("DATA_DIR")

    daily = _read_latest_partition("daily_history/daily", data_dir)
    if daily.empty:
        print("[heatmap_data_builder] 日线为空，作业标记失败")
        return 1

    trade_date = str(daily["trade_date"].iloc[0])
    frame = daily[["ts_code", "trade_date", "close", "pct_chg"]].copy()

    # 基本面：总市值 / 换手率
    basic = _read_latest_partition("daily_basic/daily", data_dir)
    if not basic.empty:
        basic = basic[basic["trade_date"] == trade_date]
        cols = [c for c in ("ts_code", "total_mv", "turnover_rate") if c in basic.columns]
        frame = frame.merge(basic[cols], on="ts_code", how="left")
    for column in ("total_mv", "turnover_rate"):
        if column not in frame.columns:
            frame[column] = pd.NA

    # 资金流净额
    flow = _read_latest_partition("moneyflow/daily", data_dir)
    if not flow.empty and "net_mf_amount" in flow.columns:
        flow = flow[flow["trade_date"] == trade_date]
        frame = frame.merge(flow[["ts_code", "net_mf_amount"]], on="ts_code", how="left")
    if "net_mf_amount" not in frame.columns:
        frame["net_mf_amount"] = pd.NA

    # 名称与行业
    try:
        from app.utils.parquet_job_helpers import _default_data_root

        basic_info = pd.read_parquet(
            Path(data_dir or _default_data_root()) / "stock_basic.parquet"
        )
        cols = [c for c in ("ts_code", "name", "industry") if c in basic_info.columns]
        frame = frame.merge(basic_info[cols], on="ts_code", how="left")
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"[heatmap_data_builder] stock_basic 读取失败: {exc}")
    for column in ("name", "industry"):
        if column not in frame.columns:
            frame[column] = None

    # 没有行业的股票无法参与板块聚合，剔除（避免聚合成名为 None 的板块）
    before = len(frame)
    frame = frame[frame["industry"].notna() & (frame["industry"].astype(str).str.strip() != "")]
    dropped = before - len(frame)

    frame = frame.reindex(columns=OUTPUT_COLUMNS)
    written = save_single_parquet(frame, REL_FILENAME, data_dir=data_dir)
    if not written:
        print("[heatmap_data_builder] 写入 0 行，作业标记失败")
        return 1

    print(
        f"[heatmap_data_builder] 完成: trade_date={trade_date}, rows={written}, "
        f"行业数={frame['industry'].nunique()}, 无行业剔除={dropped}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
