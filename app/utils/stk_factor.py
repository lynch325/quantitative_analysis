"""数据作业：拉 Tushare 技术因子（复权价 + MACD/KDJ/RSI/BOLL/CCI）→ `data/stk_factor/daily/`。

`FIELDS` 是落盘白名单，列必须与 data_reader.STANDARD_COLUMNS["stk_factor"] 对齐，
否则下游读表时列会被裁掉。骨架与调度约定见 parquet_job_helpers.DailyFetchJob。

注意：registry 中 job_type `stk_factor` 现指向 `stk_factor_derived.py`
（由本地日线自算同一组字段，不依赖外部源）；本脚本是 Tushare 版本，
未在注册表内，作为对照/备用保留。
"""

from db_utils import DatabaseUtils
from parquet_job_helpers import DailyFetchJob


FIELDS = [
    "ts_code",
    "trade_date",
    "close",
    "open",
    "high",
    "low",
    "pre_close",
    "change",
    "pct_change",
    "vol",
    "amount",
    "adj_factor",
    "open_hfq",
    "open_qfq",
    "close_hfq",
    "close_qfq",
    "high_hfq",
    "high_qfq",
    "low_hfq",
    "low_qfq",
    "pre_close_hfq",
    "pre_close_qfq",
    "macd_dif",
    "macd_dea",
    "macd",
    "kdj_k",
    "kdj_d",
    "kdj_j",
    "rsi_6",
    "rsi_12",
    "rsi_24",
    "boll_upper",
    "boll_mid",
    "boll_lower",
    "cci",
]


class StkFactorJob(DailyFetchJob):
    job_name = "stk_factor"
    rel_table = "stk_factor/daily"

    def fetch_one(self, trade_date):
        return self.api.stk_factor(trade_date=trade_date, fields=FIELDS)


def main():
    StkFactorJob(api=DatabaseUtils.init_tushare_api()).run()


if __name__ == "__main__":
    main()
