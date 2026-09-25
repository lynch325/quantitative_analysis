"""数据作业：下载交易日历 → 单文件表 `data/stock_trade_calendar.parquet`。

字段：exchange / cal_date / is_open / pretrade_date。

日期区间**硬编码**在 main 内（2005-01-01 ~ 2026-12-31，见其中注释）：
财务因子按公告日打点后需向后对齐到交易日，只下载近年会让早期公告日无处可对；
区间到期后需同步放宽，否则新一年的交易日会缺失、公告日对齐会退化。

注意：registry 中 job_type `trade_calendar` 现指向 `trade_calendar_fuyao.py`，
本脚本是 Tushare 版本；它写的是同一个单文件表，两者择一即可。
"""

from db_utils import DatabaseUtils
from parquet_writer import save_single_parquet


def main():
    """作业入口：下载交易日历并落盘。

    **从 2005 年起覆盖完整历史**：财务因子要把公告日对齐到交易日，
    只下载近年会让早期快照落在周末且无法对齐，精确匹配永远查不到。
    改起始日期前先确认因子侧的对齐需求。
    """
    pro = DatabaseUtils.init_tushare_api()
    # 从 2005 年起覆盖完整历史：财务因子公告日对齐交易日需要历史日历，
    # 只下载近年会让早期快照落在周末且无法对齐，精确匹配永远查不到
    data = pro.trade_cal(
        exchange="",
        start_date="20050101",
        end_date="20261231",
        fields="exchange,cal_date,is_open,pretrade_date",
    )
    save_single_parquet(data, "stock_trade_calendar.parquet")


if __name__ == "__main__":
    main()
