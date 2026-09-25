"""数据作业：拉 Tushare 筹码分布（cyq_perf）→ `data/cyq_perf/daily/`。

`FIELDS` 为落盘白名单（获利比例、平均成本、90/70 分位成本与集中度等）。
骨架与调度约定见 parquet_job_helpers.DailyFetchJob。

注意：registry 中 job_type `cyq_perf` 现指向 `cyq_perf_derived.py`
（三角形分布 + 换手衰减模型本地自算，因数据源均不提供真实筹码）；
本脚本是 Tushare 版本，未在注册表内，作为对照/备用保留。
"""

from db_utils import DatabaseUtils
from parquet_job_helpers import DailyFetchJob


FIELDS = [
    "ts_code",
    "trade_date",
    "his_low",
    "his_high",
    "cost_5pct",
    "cost_15pct",
    "cost_50pct",
    "cost_85pct",
    "cost_95pct",
    "weight_avg",
    "winner_rate",
]


class CyqPerfJob(DailyFetchJob):
    job_name = "cyq_perf"
    rel_table = "cyq_perf/daily"

    def fetch_one(self, trade_date):
        return self.api.cyq_perf(trade_date=trade_date, fields=FIELDS)


def main():
    CyqPerfJob(api=DatabaseUtils.init_tushare_api()).run()


if __name__ == "__main__":
    main()
