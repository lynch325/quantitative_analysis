"""分钟线同步（TickFlow 日内分时 → stock_minute/）—— 问题的 A9 方案之一。

背景：`data/stock_minute/` 缺失会让整个"实时分析"模块为空
（实时行情/板块/异动/情绪与 WebSocket 行情推送都依赖它）。
而现有两个分钟源都有硬伤：
- 通达信 pytdx：**不支持 1min**（`bars.py:16-19` 直接抛 ValueError）
- Baostock：1min 会被**静默降级成 5min**（`minute_data_sync_service.py:104-107`）

TickFlow 的 `GET /v1/klines/intraday`（`TickflowClient.intraday()`）能直接给出
**最新交易日的 1 分钟线**（1m/5m/15m/30m/60m），正好补这个缺口。

落盘走项目既有的 `MinuteParquetStore`，路径与其它分钟源一致：
    {DATA_DIR}/stock_minute/{period_type}/year=/month=/day=/data.parquet

参数（沿用 data_jobs 环境变量约定）：
- `DATA_JOB_PARAM_SYMBOLS`：逗号分隔的标的白名单；缺省取 `stock_basic` 全量
- `DATA_JOB_PERIOD`：周期，取值 1m/5m/15m/30m/60m（默认 1m）
- `TF_THROTTLE_SECONDS`：单次请求间隔（默认 0.15s，防触发限频）

⚠️ 单位口径：tickflow 的 `volume` 是**手**、`amount` 是**元**，与 TDX 源一致。
"""

import os
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

# 兼容直接运行（python app/utils/minute_sync_tickflow.py）
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from loguru import logger

from app.services.minute_parquet_store import MinuteParquetStore
from app.utils.data_sources.tickflow_client import TickflowClient, TickflowError

#: API 周期 → 项目内部的 period_type（落盘目录名）
PERIOD_MAP = {
    "1m": "1min",
    "5m": "5min",
    "15m": "15min",
    "30m": "30min",
    "60m": "60min",
}

#: 落盘列（与其它分钟源保持一致）
OUTPUT_COLUMNS = [
    "ts_code", "datetime", "period_type",
    "open", "high", "low", "close", "volume", "amount",
    "pre_close", "change", "pct_chg",
]

MAX_RETRIES = 3
DEFAULT_THROTTLE = 0.15


def _resolve_symbols() -> List[str]:
    """标的清单：DATA_JOB_PARAM_SYMBOLS 优先，否则取 stock_basic 全量。"""
    raw = (os.getenv("DATA_JOB_PARAM_SYMBOLS") or "").strip()
    if raw:
        return [s.strip().upper() for s in raw.split(",") if s.strip()]
    from app.utils.parquet_job_helpers import get_stock_codes

    return get_stock_codes()


def _daily_pre_close() -> Dict:
    """{trade_date: {ts_code: pre_close}}，用于给分钟线补 pre_close。"""
    from app.utils.parquet_job_helpers import _default_data_root

    root = Path(_default_data_root()) / "daily_history" / "daily"
    if not root.exists():
        return {}
    files = sorted(root.rglob("data.parquet"))
    if not files:
        return {}
    try:
        frame = pd.read_parquet(files[-1], columns=["ts_code", "trade_date", "pre_close"])
    except Exception as exc:  # noqa: BLE001 - 补列失败不影响主流程
        logger.warning(f"[minute_sync_tickflow] 读取日线昨收失败: {exc}")
        return {}
    frame["trade_date"] = frame["trade_date"].astype(str)
    trade_date = str(frame["trade_date"].iloc[0])
    mapping = dict(zip(frame["ts_code"].astype(str), pd.to_numeric(frame["pre_close"], errors="coerce")))
    return {trade_date: mapping}


def _rows_from_intraday(payload: Dict, ts_code: str, period_type: str) -> pd.DataFrame:
    """把 tickflow 的列式紧凑数据转成行式 DataFrame。"""
    if not payload:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    times = payload.get("trade_time") or payload.get("datetime") or []
    if not times:
        # 退化：用 timestamp(ms) 还原北京时间
        stamps = payload.get("timestamp") or []
        times = [
            pd.to_datetime(int(t), unit="ms", utc=True).tz_convert("Asia/Shanghai").strftime("%Y-%m-%d %H:%M:%S")
            for t in stamps
        ]
    if not times:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    frame = pd.DataFrame(
        {
            "ts_code": ts_code,
            "datetime": times,
            "period_type": period_type,
            "open": payload.get("open"),
            "high": payload.get("high"),
            "low": payload.get("low"),
            "close": payload.get("close"),
            "volume": payload.get("volume"),
            "amount": payload.get("amount"),
        }
    )
    return frame


def _fetch_with_retry(client: TickflowClient, symbol: str, api_period: str, throttle: float):
    """带限频解析的重试拉取。"""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return client.intraday(symbol, period=api_period)
        except TickflowError as exc:
            message = str(exc.message or "")
            # 限频：tickflow 会在消息里给出"请 Nms 后重试"，可直接解析
            if "ms" in message and "重试" in message:
                digits = "".join(ch for ch in message if ch.isdigit())
                wait = (int(digits) / 1000.0) if digits else 1.0
                logger.warning(f"[minute_sync_tickflow] {symbol} 限频，等待 {wait:.2f}s 后重试")
                time.sleep(min(wait, 5.0))
                continue
            if exc.is_permission_denied:
                raise
            logger.warning(f"[minute_sync_tickflow] {symbol} 第{attempt}次失败: {exc}")
            time.sleep(throttle * attempt * 4)
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"[minute_sync_tickflow] {symbol} 第{attempt}次异常: {exc}")
            time.sleep(throttle * attempt * 4)
    return None


def main() -> int:
    """作业入口：用 TickFlow 同步分钟线并写本地分区。

    周期取值优先级：DATA_JOB_PARAM_PERIOD（data_jobs runner 会把提交的 params
    转成 DATA_JOB_PARAM_<KEY>）> DATA_JOB_PERIOD > 默认 1m；
    不在 PERIOD_MAP 里的周期直接返回 1。请求间隔由 TF_THROTTLE_SECONDS 控制。
    """
    load_dotenv()

    # data_jobs 提交的任意 params 会被 runner 转成 DATA_JOB_PARAM_<KEY>，优先读它
    api_period = (
        os.getenv("DATA_JOB_PARAM_PERIOD") or os.getenv("DATA_JOB_PERIOD") or "1m"
    ).strip()
    if api_period not in PERIOD_MAP:
        print(f"[minute_sync_tickflow] 不支持的周期: {api_period}（可选 {'/'.join(PERIOD_MAP)}）")
        return 1
    period_type = PERIOD_MAP[api_period]
    throttle = float(os.getenv("TF_THROTTLE_SECONDS", DEFAULT_THROTTLE))

    symbols = _resolve_symbols()
    if not symbols:
        print("[minute_sync_tickflow] 无可用标的清单")
        return 1

    client = TickflowClient()
    store = MinuteParquetStore()
    pre_close_map = _daily_pre_close()

    print(
        f"[minute_sync_tickflow] 开始: symbols={len(symbols)}, period={api_period}→{period_type}, "
        f"throttle={throttle}s"
    )

    ok = 0
    empty = 0
    failed = 0
    written_rows = 0

    for index, symbol in enumerate(symbols, start=1):
        payload = _fetch_with_retry(client, symbol, api_period, throttle)
        if payload is None:
            failed += 1
        else:
            frame = _rows_from_intraday(payload, symbol, period_type)
            if frame.empty:
                empty += 1
            else:
                # 补 pre_close / change / pct_chg（取该股票当日昨收）
                # ⚠️ client.intraday() 的原始返回**只有** timestamp/open/high/low/close/
                # volume/amount 七个键 —— symbol/name/trade_date/trade_time 都是 tickflow
                # **SDK 自己派生**的列，REST 层并不返回。因此交易日只能从已解析出的
                # datetime 推导（datetime 本身已由 timestamp 兜底还原），
                # 再去匹配本地日线的 "YYYYMMDD" 键。
                trade_date = str(frame["datetime"].iloc[0])[:10].replace("-", "")
                pre_close = (pre_close_map.get(trade_date) or {}).get(symbol)
                if pre_close is not None and pd.notna(pre_close):
                    frame["pre_close"] = float(pre_close)
                    close_num = pd.to_numeric(frame["close"], errors="coerce")
                    frame["change"] = close_num - float(pre_close)
                    frame["pct_chg"] = np.where(
                        float(pre_close) > 0, frame["change"] / float(pre_close) * 100.0, np.nan
                    )
                else:
                    frame["pre_close"] = pd.NA
                    frame["change"] = pd.NA
                    frame["pct_chg"] = pd.NA

                written_rows += store.write_frame(frame, period_type)
                ok += 1

        if index % 200 == 0:
            print(f"[minute_sync_tickflow] 进度 {index}/{len(symbols)} ok={ok} empty={empty} failed={failed}")
        time.sleep(throttle)

    print(
        f"[minute_sync_tickflow] 完成: 成功={ok}, 无数据={empty}, 失败={failed}, "
        f"累计写入行数={written_rows}"
    )
    # 全部失败才算作业失败；部分标的无数据（新股/停牌）是正常现象
    return 0 if ok > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
