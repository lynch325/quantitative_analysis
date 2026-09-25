"""数据作业：按「股票 × 交易日」逐只拉日线 → `data/daily_history/daily/`。

与 daily_history_by_date.py 写同一张表，但调用次数是 O(股票数 × 交易日数)，
且每只股票一次请求、无分页——属**手工补数的备用路径**，日常增量请用
by_date 版本或扶摇版本。

日期窗口由 parquet_job_helpers.resolve_trade_dates 解析 DATA_JOB_* 环境变量；
股票清单取本地 stock_basic（get_stock_codes），因此必须先跑基础资料作业。

注意：registry 中 job_type `daily_history_by_code` 现已指向
`daily_history_fuyao.py`，本脚本未在注册表内。
"""

from db_utils import DatabaseUtils
from parquet_job_helpers import get_stock_codes, resolve_trade_dates
from parquet_writer import save_partitioned_parquet


def main():
    """作业入口：逐日 × 逐标的调用 Tushare daily 拉日线并汇总落盘。

    注意这是**按标的循环**（标的数 × 天数 次请求），比按日拉慢得多，
    也更容易撞 Tushare 的频次限制。
    """
    pro = DatabaseUtils.init_tushare_api()
    stock_list = get_stock_codes()
    trade_dates, _ = resolve_trade_dates()

    total_saved = 0
    for trade_date in trade_dates:
        frames = []
        for ts_code in stock_list:
            df = pro.daily(ts_code=ts_code, trade_date=trade_date)
            if df is not None and not df.empty:
                frames.append(df)
        if frames:
            import pandas as pd

            total_saved += save_partitioned_parquet(
                pd.concat(frames, ignore_index=True), "trade_date", "daily_history/daily"
            )
        print(f"[daily_history_by_code] trade_date={trade_date}")

    print(
        f"[daily_history_by_code] 完成，trade_days={len(trade_dates)}, total_upsert={total_saved}"
    )


if __name__ == "__main__":
    main()
