"""数据作业：拉 Tushare 每日指标（daily_basic）→ `data/daily_basic/daily/`。

`FIELDS` 为落盘白名单（换手率、量比、估值、股本与市值等），表由 data_reader
以 TABLE_DIRS["daily_basic"] 读取，是估值类因子与股票业务大宽表的基础。

注意：registry 中 job_type `daily_basic` 现指向 `daily_basic_fuyao.py`；
本脚本是 Tushare 版本，未在注册表内（写同一张表）。
"""

from db_utils import DatabaseUtils
from parquet_job_helpers import DailyFetchJob


FIELDS = [
    "ts_code",
    "trade_date",
    "close",
    "turnover_rate",
    "turnover_rate_f",
    "volume_ratio",
    "pe",
    "pe_ttm",
    "pb",
    "ps",
    "ps_ttm",
    "dv_ratio",
    "dv_ttm",
    "total_share",
    "float_share",
    "free_share",
    "total_mv",
    "circ_mv",
]


class DailyBasicJob(DailyFetchJob):
    job_name = "daily_basic"
    rel_table = "daily_basic/daily"

    def fetch_one(self, trade_date):
        return self.api.daily_basic(trade_date=trade_date, fields=FIELDS)


def main():
    DailyBasicJob(api=DatabaseUtils.init_tushare_api()).run()


if __name__ == "__main__":
    main()
