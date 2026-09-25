"""按成交额取前 N 只活跃股，同步其分钟线。

全量 5571 只按 0.15s 节流约需 1 小时，不现实；而"实时监控"真正需要的是
活跃股（板块表现 / 市场情绪 / 异动检测都基于活跃股截面），故取 Top N。
"""
import os
import sys
from pathlib import Path

ROOT = Path(r"d:\new_tdx64\PYPlugins\quantitative_analysis")
sys.path.insert(0, str(ROOT))

TOP_N = int(os.getenv("SYNC_TOP_N", "300"))

import pandas as pd  # noqa: E402
from app.services.data_reader import ParquetDataReader  # noqa: E402

reader = ParquetDataReader()
daily = reader._read_latest_partition("daily")
if daily is None or daily.empty:
    print("无日线数据，无法挑选活跃股")
    sys.exit(1)

daily = daily.copy()
daily["_amt"] = pd.to_numeric(daily.get("amount"), errors="coerce")
codes = (
    daily.dropna(subset=["_amt"])
    .sort_values("_amt", ascending=False)["ts_code"]
    .astype(str).drop_duplicates().head(TOP_N).tolist()
)
print(f"[sync_active] 活跃股 TOP{TOP_N}: {len(codes)} 只，样例 {codes[:5]}")

os.environ["DATA_JOB_PARAM_SYMBOLS"] = ",".join(codes)
os.environ["DATA_JOB_PERIOD"] = os.getenv("SYNC_PERIOD", "5m")

from app.utils.minute_sync_tickflow import main  # noqa: E402

sys.exit(main())
