"""数据作业：按交易日拉全市场日线 → `data/daily_history/daily/`。

Tushare `daily` 接口按 trade_date 一次返回全市场，是 O(交易日数) 的主路径
（按代码逐只拉的备用路径见 daily_history_by_code.py）。

骨架继承 parquet_job_helpers.DailyFetchJob：日期窗口由 DATA_JOB_* 环境变量
决定，缺省「只拉最新一天」，并在区间内自动回补本地缺失分区（单次有上限）；
接口限速 + 指数退避重试，重试仍失败的交易日让进程以退出码 1 结束，
由调度侧记为 failed（漏掉的日子下一轮 gap_fill 自动补）。

注意：`data_jobs/registry.py` 中 job_type `daily_history_by_date` 现已指向
`daily_history_fuyao.py`，本脚本是 Tushare 版本的备用/对照实现。
"""

from db_utils import DatabaseUtils
from parquet_job_helpers import DailyFetchJob


class DailyHistoryByDateJob(DailyFetchJob):
    job_name = "daily_history_by_date"
    rel_table = "daily_history/daily"

    def fetch_one(self, trade_date):
        return self.api.daily(trade_date=trade_date)


def main():
    DailyHistoryByDateJob(api=DatabaseUtils.init_tushare_api()).run()


if __name__ == "__main__":
    main()
