"""通达信分钟线规范化：把 pytdx 返回的裸结构转成本项目的标准分钟表。

输出列固定为
`ts_code, datetime, period_type, open, high, low, close, volume, amount, pre_close, change, pct_chg`：
- `volume` 直接复用 pytdx 的 `vol`；
- `pre_close` 用**前一根 bar 的收盘价**推算（首根用自身收盘价），
  因此跨交易日/跨停牌的首根 bar 的涨跌幅天然为 0——这是刻意的保守口径，
  真实前收需外部日线补齐；
- 入参为空时返回**同结构的空表**（而非空 DataFrame），避免下游缺列报错。

周期仅支持 5/15/30/60 分钟（TDX_CATEGORY_MAP），其他周期直接抛 ValueError。
"""

from __future__ import annotations

import pandas as pd

from app.services.tongdaxin.code_mapping import tdx_market_code_to_bs_style


TDX_CATEGORY_MAP = {
    "5min": 0,
    "15min": 1,
    "30min": 2,
    "60min": 3,
}


def period_type_to_tdx_category(period_type: str) -> int:
    if period_type not in TDX_CATEGORY_MAP:
        raise ValueError(f"unsupported tongdaxin period_type: {period_type}")
    return TDX_CATEGORY_MAP[period_type]


def normalize_tdx_minute_bars(payload, market: int, code: str, period_type: str) -> pd.DataFrame:
    """把通达信原始分钟 K 线转成项目标准列。

    空输入也返回**带完整列的空表**：列契约稳定，下游不必再判列；
    datetime 解析失败的行丢弃后排序；周期非法时由 period_type_to_tdx_category 直接抛错。
    """
    period_type_to_tdx_category(period_type)
    df = pd.DataFrame(list(payload or []))
    if df.empty:
        return pd.DataFrame(
            columns=[
                "ts_code",
                "datetime",
                "period_type",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "amount",
                "pre_close",
                "change",
                "pct_chg",
            ]
        )

    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df = df.dropna(subset=["datetime"]).sort_values("datetime").reset_index(drop=True)
    for column in ["open", "high", "low", "close", "vol", "amount"]:
        df[column] = pd.to_numeric(df[column], errors="coerce")
    df["ts_code"] = tdx_market_code_to_bs_style(market, code)
    df["period_type"] = period_type
    df["volume"] = df["vol"]
    df["pre_close"] = df["close"].shift(1)
    if not df.empty:
        df.loc[df.index[0], "pre_close"] = df.loc[df.index[0], "close"]
    df["change"] = df["close"] - df["pre_close"]
    df["pct_chg"] = ((df["change"] / df["pre_close"]) * 100).fillna(0).round(4)
    if not df.empty:
        df.loc[df.index[0], ["change", "pct_chg"]] = [0, 0]
    return df[
        [
            "ts_code",
            "datetime",
            "period_type",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amount",
            "pre_close",
            "change",
            "pct_chg",
        ]
    ]
