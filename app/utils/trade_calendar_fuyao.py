"""交易日历下载（stock_trade_calendar.parquet 的 fuyao 生产者）。

与 tushare 版（trade_calendar.py）写入同一个 stock_trade_calendar.parquet、
同一套 schema（exchange / cal_date / is_open / pretrade_date），读取侧无感知。

数据来源：扶摇 `GET /api/a-share/calendar/trading-days`
（FuyaoClient.trading_days()，返回 {date_ms, date}）。

⚠️ 已知限制：扶摇该接口是**固定近一年窗口**（实测 242 个交易日），
无法像 tushare 那样取 2005 年至今的完整历史。因此：
- 日频增量任务（依赖"最近交易日/缺口回补"）完全够用；
- 需要把财务因子公告日对齐到很早期交易日的场景，历史段会缺失。

pretrade_date（前一交易日）由本脚本按日期升序自行推导，首行为空。
"""

import sys
from pathlib import Path

# 兼容直接运行（python app/utils/trade_calendar_fuyao.py）
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import pandas as pd
from dotenv import load_dotenv
from loguru import logger

from app.utils.data_sources.fuyao_client import FuyaoClient
from app.utils.parquet_writer import save_single_parquet

REL_FILENAME = "stock_trade_calendar.parquet"
#: 扶摇日历覆盖沪/深/北全市场，exchange 记 SSE（与 tushare 空 exchange 查询等价）
EXCHANGE = "SSE"


def main() -> int:
    """作业入口：用扶摇交易日历重建 stock_trade_calendar。

    交易日列表为空视为失败返回 1；只写 is_open=1 的行（休市日不落库）。
    """
    load_dotenv()

    days = FuyaoClient().trading_days()
    if not days:
        print("[trade_calendar_fuyao] 交易日历为空，作业标记失败")
        return 1

    ordered = sorted(days, key=lambda item: item.get("date") or "")
    frame = pd.DataFrame(
        {
            "exchange": EXCHANGE,
            "cal_date": [item.get("date") for item in ordered],
            "is_open": 1,
        }
    )
    # 前一交易日：整体下移一行，首行无前一交易日
    frame["pretrade_date"] = frame["cal_date"].shift(1)

    saved = save_single_parquet(frame, REL_FILENAME)
    if not saved:
        print("[trade_calendar_fuyao] 写入 0 行，作业标记失败")
        return 1

    logger.info(
        f"[trade_calendar_fuyao] 完成: {saved} 个交易日 "
        f"({frame['cal_date'].iloc[0]} ~ {frame['cal_date'].iloc[-1]})，"
        "窗口为扶摇固定近一年"
    )
    print(
        f"[trade_calendar_fuyao] 完成: total={saved} "
        f"({frame['cal_date'].iloc[0]}~{frame['cal_date'].iloc[-1]})"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
