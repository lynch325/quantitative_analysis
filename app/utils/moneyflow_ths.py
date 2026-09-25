"""数据作业：拉同花顺口径资金流向（moneyflow_ths）→ `data/moneyflow_ths/daily/`。

与 moneyflow.py（Tushare 口径）并存但**落到不同表**，避免口径混淆。

注意：该表未登记在 data_reader.TABLE_DIRS 中，目前没有读表入口，
写入结果只供人工/外部核对；本脚本也未在 registry 注册。
"""

from db_utils import DatabaseUtils
from parquet_job_helpers import DailyFetchJob


FIELDS = [
    "ts_code",
    "trade_date",
    "name",
    "pct_change",
    "latest",
    "net_amount",
    "net_d5_amount",
    "buy_lg_amount",
    "buy_lg_amount_rate",
    "buy_md_amount",
    "buy_md_amount_rate",
    "buy_sm_amount",
    "buy_sm_amount_rate",
]


class MoneyflowThsJob(DailyFetchJob):
    job_name = "moneyflow_ths"
    rel_table = "moneyflow_ths/daily"

    def fetch_one(self, trade_date):
        return self.api.moneyflow_ths(trade_date=trade_date, fields=FIELDS)


def main():
    MoneyflowThsJob(api=DatabaseUtils.init_tushare_api()).run()


if __name__ == "__main__":
    main()
