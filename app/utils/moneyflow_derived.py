"""资金流向（moneyflow/daily 表的 derived 生产者）。

⚠️⚠️ 重要说明（请务必先看）：
**三个可用数据源（扶摇 / TickFlow / 通达信数仓）都不提供分笔/分单资金流**：
- 扶摇 `capital-flow/*` 接口官方文档明确标注「当前不可用于外部调用」；
- TickFlow 只有 K线/行情/复权因子/标的维表，无资金流；
- 通达信数仓的 GP 系列只有涨停/封单/总市值等，无大中小单拆分。

因此本脚本**只能给出价格位置法的净额估算**，填不了买卖分层：

| 列 | 是否填充 | 说明 |
|---|---|---|
| `net_mf_amount` | ✅ 估算 | Chaikin 单日资金流：((C−L)−(H−C))/(H−L) × 成交额 |
| `net_mf_vol` | ✅ 估算 | 净额按当日均价折算成手数 |
| `buy/sell_{sm,md,lg,elg}_{vol,amount}` | ❌ 留空 | 需要逐笔分单数据，无源可得 |

下游影响：因子引擎的资金面因子 `money_flow_strength`（大单净流入/总成交）、
`big_order_ratio` 依赖分层列，**在缺少真实资金流时会算出空值**；
`money_flow_momentum` 仅用 `net_mf_amount`，可正常工作。
"""

import os
import sys
from pathlib import Path

# 兼容直接运行（python app/utils/moneyflow_derived.py）
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import numpy as np
import pandas as pd
from dotenv import load_dotenv

from app.utils.parquet_job_helpers import (
    _default_data_root,
    existing_partition_dates,
    resolve_trade_dates_with_gap_fill,
)
from app.utils.parquet_writer import save_to_parquet

REL_SOURCE = "daily_history/daily"
REL_TABLE = "moneyflow/daily"

OUTPUT_COLUMNS = [
    "ts_code", "trade_date",
    "buy_sm_vol", "buy_sm_amount", "sell_sm_vol", "sell_sm_amount",
    "buy_md_vol", "buy_md_amount", "sell_md_vol", "sell_md_amount",
    "buy_lg_vol", "buy_lg_amount", "sell_lg_vol", "sell_lg_amount",
    "buy_elg_vol", "buy_elg_amount", "sell_elg_vol", "sell_elg_amount",
    "net_mf_vol", "net_mf_amount",
]

#: daily 的 amount 单位是千元，tushare moneyflow 的金额单位是万元
_KILO_TO_WAN = 0.1


def _read_local_daily(data_dir: str | None = None) -> pd.DataFrame:
    """读取本地源表全部分区并合并（资金流计算的输入）。

    单分区读取失败静默跳过（资金流是从日线派生，个别分区坏了不该整批失败）。
    """
    root = Path(data_dir or _default_data_root()) / REL_SOURCE
    if not root.exists():
        return pd.DataFrame()
    frames = []
    for parquet_file in sorted(root.rglob("data.parquet")):
        try:
            frames.append(pd.read_parquet(parquet_file))
        except Exception:  # noqa: BLE001
            continue
    if not frames:
        return pd.DataFrame()
    frame = pd.concat(frames, ignore_index=True)
    frame["trade_date"] = frame["trade_date"].astype(str)
    return frame


def main() -> int:
    """作业入口：按交易日计算资金流派生指标并写分区。

    只处理源数据已有分区的日期，无交集则跳过；交易日含缺口回补。
    """
    load_dotenv()
    data_dir = os.getenv("DATA_DIR")

    trade_dates, _ = resolve_trade_dates_with_gap_fill(REL_TABLE)
    if not trade_dates:
        print("[moneyflow_derived] 没有需要计算的交易日")
        return 0

    have = existing_partition_dates(REL_SOURCE, data_dir)
    targets = [d for d in trade_dates if d in have]
    if not targets:
        print("[moneyflow_derived] daily_history 中无对应分区，跳过")
        return 0

    raw = _read_local_daily(data_dir)
    if raw.empty:
        print("[moneyflow_derived] 日线为空，作业标记失败")
        return 1

    frame = raw[raw["trade_date"].isin(set(targets))].copy()

    high = frame["high"].astype(float)
    low = frame["low"].astype(float)
    close = frame["close"].astype(float)
    amount = frame["amount"].astype(float)
    vol = frame["vol"].astype(float)

    span = (high - low).replace(0, np.nan)
    # Chaikin 资金流：+1 表示收在最高（买盘主导），−1 表示收在最低
    mf_ratio = (((close - low) - (high - close)) / span).fillna(0.0)
    frame["net_mf_amount"] = mf_ratio * amount * _KILO_TO_WAN

    # 净量：按当日均价（amount 千元 → 元；vol 手 → 股）折算
    avg_price = (amount * 1000.0) / (vol * 100.0).replace(0, np.nan)
    frame["net_mf_vol"] = (
        frame["net_mf_amount"] * 10000.0 / (avg_price * 100.0).replace(0, np.nan)
    )

    # 分层买卖列：无数据源可得，保持 NaN
    for column in OUTPUT_COLUMNS:
        if column not in frame.columns:
            frame[column] = np.nan

    written = 0
    for trade_date, day_frame in frame.groupby("trade_date", sort=True):
        out = day_frame.reindex(columns=OUTPUT_COLUMNS).copy()
        out["ts_code"] = day_frame["ts_code"].to_numpy()
        out["trade_date"] = trade_date
        written += save_to_parquet(out, trade_date, REL_TABLE, data_dir=data_dir)

    print(
        f"[moneyflow_derived] 完成，trade_days={len(targets)}, total_upsert={written} "
        "（仅净额为模型估算，分层列无数据源留空）"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
