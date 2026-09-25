"""数据作业：拉 Tushare 资金流向（moneyflow）→ `data/moneyflow/daily/`。

`FIELDS` 为落盘白名单（大中小单买卖金额/量），表由 data_reader 以
TABLE_DIRS["moneyflow"] 读取（get_moneyflow），并参与大宽表构建。

注意：registry 中 job_type `moneyflow` 现指向 `moneyflow_derived.py`
（按价格位置法本地估算净额，因数据源不提供逐笔分层）；本脚本是 Tushare 版本，
未在注册表内。两者写同一张表、口径不同，切换前需确认下游一致性。
"""

from db_utils import DatabaseUtils
from parquet_job_helpers import DailyFetchJob


FIELDS = [
    "ts_code",
    "trade_date",
    "buy_sm_vol",
    "buy_sm_amount",
    "sell_sm_vol",
    "sell_sm_amount",
    "buy_md_vol",
    "buy_md_amount",
    "sell_md_vol",
    "sell_md_amount",
    "buy_lg_vol",
    "buy_lg_amount",
    "sell_lg_vol",
    "sell_lg_amount",
    "buy_elg_vol",
    "buy_elg_amount",
    "sell_elg_vol",
    "sell_elg_amount",
    "net_mf_vol",
    "net_mf_amount",
]


class MoneyflowJob(DailyFetchJob):
    job_name = "moneyflow"
    rel_table = "moneyflow/daily"

    def fetch_one(self, trade_date):
        return self.api.moneyflow(trade_date=trade_date, fields=FIELDS)


def main():
    MoneyflowJob(api=DatabaseUtils.init_tushare_api()).run()


if __name__ == "__main__":
    main()
