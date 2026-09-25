"""均线衍生计算：为全市场股票生成 MA5~MA120 与 EMA5~EMA120 的最新值。

数据流：data/daily_history 日线（ParquetDataReader）→ 每只股票取最近 250 个
交易日 → 取各周期均线尾值 → 落 data/stock_ma_data.parquet（单文件表）。

运行方式：由 app/services/data_jobs/registry.py 注册为「均线衍生计算」任务
（dangerous=True，会整表覆盖 stock_ma_data.parquet），不是常驻服务；
下面 import 的 parquet_job_helpers / parquet_writer 是同目录模块，
因此必须在该脚本所在目录位于 sys.path 的方式下运行。

口径注意：EMA5~EMA30 走 pandas ewm(adjust=False)，EMA60/EMA120 走
calculate_ema_manually —— 两者种子不同（pandas 以首值为种子，本文件的
函数以前 period 个的 SMA 为种子），250 根样本下长周期的种子残差未完全
衰减；若要对齐这两个口径，需先与下游消费方确认。
"""

import numpy as np
import pandas as pd

from app.services.data_reader import ParquetDataReader
from parquet_job_helpers import get_stock_codes
from parquet_writer import save_single_parquet


def calculate_ema_manually(prices, period):
    """手算 EMA 尾值（SMA 种子递推），样本不足 period 根时返回 None。

    只返回最后一个值，供落库用；与 pandas ewm(adjust=False) 的差别仅在
    种子选择（见模块头「口径注意」），递推公式一致。

    Parameters
    ----------
    prices : array-like
        按时间升序的收盘价序列。
    period : int
        EMA 周期，同时决定 SMA 种子长度。
    """
    if len(prices) < period:
        return None

    sma = np.mean(prices[:period])
    ema_values = [sma]
    multiplier = 2 / (period + 1)

    for i in range(period, len(prices)):
        ema = (prices[i] * multiplier) + (ema_values[-1] * (1 - multiplier))
        ema_values.append(ema)

    return ema_values[-1]


def main():
    """批量计算全市场均线尾值并落盘（无返回值，供数据任务直接调用）。

    逐只股票取最近 250 个交易日；样本不足某周期时该列留空 None，
    因此 stock_ma_data.parquet 各列允许出现空值。落盘走 parquet_writer
    的整表覆盖写入；计算过程按股票串行，属离线批处理路径。
    """
    reader = ParquetDataReader()
    stock_codes = get_stock_codes()
    rows = []

    for ts_code in stock_codes:
        df = reader.get_daily(ts_codes=[ts_code])
        if df.empty:
            continue

        df = df.sort_values("trade_date").tail(250).copy()
        if len(df) < 5:
            continue
        df["close"] = pd.to_numeric(df["close"], errors="coerce")

        ma5 = df["close"].rolling(window=5).mean().iloc[-1] if len(df) >= 5 else None
        ma10 = df["close"].rolling(window=10).mean().iloc[-1] if len(df) >= 10 else None
        ma20 = df["close"].rolling(window=20).mean().iloc[-1] if len(df) >= 20 else None
        ma30 = df["close"].rolling(window=30).mean().iloc[-1] if len(df) >= 30 else None
        ma60 = df["close"].rolling(window=60).mean().iloc[-1] if len(df) >= 60 else None
        ma120 = df["close"].rolling(window=120).mean().iloc[-1] if len(df) >= 120 else None

        close_values = df["close"].values
        ema5 = df["close"].ewm(span=5, adjust=False).mean().iloc[-1] if len(df) >= 5 else None
        ema10 = df["close"].ewm(span=10, adjust=False).mean().iloc[-1] if len(df) >= 10 else None
        ema20 = df["close"].ewm(span=20, adjust=False).mean().iloc[-1] if len(df) >= 20 else None
        ema30 = df["close"].ewm(span=30, adjust=False).mean().iloc[-1] if len(df) >= 30 else None
        ema60 = calculate_ema_manually(close_values, 60) if len(df) >= 60 else None
        ema120 = calculate_ema_manually(close_values, 120) if len(df) >= 120 else None

        rows.append(
            {
                "ts_code": ts_code,
                "ma5": float(ma5) if ma5 is not None else None,
                "ma10": float(ma10) if ma10 is not None else None,
                "ma20": float(ma20) if ma20 is not None else None,
                "ma30": float(ma30) if ma30 is not None else None,
                "ma60": float(ma60) if ma60 is not None else None,
                "ma120": float(ma120) if ma120 is not None else None,
                "ema5": float(ema5) if ema5 is not None else None,
                "ema10": float(ema10) if ema10 is not None else None,
                "ema20": float(ema20) if ema20 is not None else None,
                "ema30": float(ema30) if ema30 is not None else None,
                "ema60": float(ema60) if ema60 is not None else None,
                "ema120": float(ema120) if ema120 is not None else None,
            }
        )

    if rows:
        save_single_parquet(pd.DataFrame(rows), "stock_ma_data.parquet")
        print("MA和EMA计算完成!")


if __name__ == "__main__":
    main()
