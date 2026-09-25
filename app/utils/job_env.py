"""作业参数环境变量的解析工具（含 MySQL 时代遗留函数）。

**当前仍在用的**只有 `normalize_ymd` 与 `env_bool`——财务三表脚本
（app/utils/financial_fuyao.py）复用它们解析 DATA_JOB_* 参数。

**已废弃**的是下面几个基于 MySQL cursor 的函数（latest_open_trade_date /
resolve_date_window / fetch_open_trade_dates / delete_trade_date_range）：
它们依赖 `stock_trade_calendar` 表与 MySQL 的 DATE_FORMAT/CURDATE 语法，
是去 MySQL 之前的实现；当前链路统一走 Parquet（见 parquet_job_helpers.py）。
新代码不要再用这些函数，日期解析一律用 parquet_job_helpers。
"""

import os
from datetime import datetime, timedelta
from typing import Optional


def normalize_ymd(date_str: Optional[str]) -> Optional[str]:
    """把日期规整成 YYYYMMDD；空值返回 None。

    已是 8 位数字则原样返回，否则按 %Y-%m-%d 解析。
    格式不符会抛 ValueError —— 刻意不静默返回 None，避免把「参数写错」当成「没传」。
    """
    if not date_str:
        return None
    text = str(date_str).strip()
    if not text:
        return None
    if len(text) == 8 and text.isdigit():
        return text
    return datetime.strptime(text, "%Y-%m-%d").strftime("%Y%m%d")


def env_bool(key: str, default: bool = False) -> bool:
    raw = os.getenv(key)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def latest_open_trade_date(cursor) -> Optional[str]:
    """取交易日历里不晚于今天的最后一个开市日（YYYYMMDD）。

    比较放在 MySQL 侧做，避免把整张日历取回本地再算。
    """
    cursor.execute(
        """
        SELECT DATE_FORMAT(MAX(cal_date), '%Y%m%d')
        FROM stock_trade_calendar
        WHERE is_open = 1 AND cal_date <= CURDATE()
        """
    )
    return cursor.fetchone()[0]


def next_date_ymd(date_ymd: str) -> str:
    return (datetime.strptime(date_ymd, "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")


def resolve_date_window(cursor, target_table: str) -> tuple[Optional[str], Optional[str], bool]:
    """解析作业的日期窗口，返回 (start_date, end_date, full_refresh)。

    优先级：DATA_JOB_TRADE_DATE（单日窗口）> DATA_JOB_START_DATE / DATA_JOB_END_DATE；
    end 缺省取最近开市日，start 缺省取目标表已有的最大交易日（即默认增量续跑）。
    full_refresh 来自 DATA_JOB_FULL_REFRESH，与窗口一起回给调用方。
    """
    start_date = normalize_ymd(os.getenv("DATA_JOB_START_DATE"))
    end_date = normalize_ymd(os.getenv("DATA_JOB_END_DATE"))
    trade_date = normalize_ymd(os.getenv("DATA_JOB_TRADE_DATE"))
    full_refresh = env_bool("DATA_JOB_FULL_REFRESH", default=False)

    if trade_date:
        return trade_date, trade_date, full_refresh

    if not end_date:
        end_date = latest_open_trade_date(cursor)

    if not start_date:
        cursor.execute(f"SELECT DATE_FORMAT(MAX(trade_date), '%Y%m%d') FROM {target_table}")
        max_trade_date = cursor.fetchone()[0]
        if max_trade_date:
            start_date = next_date_ymd(max_trade_date)
        else:
            start_date = end_date

    return start_date, end_date, full_refresh


def fetch_open_trade_dates(cursor, start_date: str, end_date: str) -> list[str]:
    """取 [start_date, end_date] 内全部开市日（YYYYMMDD 升序）。

    参数缺失或 start > end 直接返回空列表。
    注意 SQL 字符串里的 % 要写成 %%：格式化模板与参数化占位符都会吃掉单个 %。
    """
    if not start_date or not end_date or start_date > end_date:
        return []

    cursor.execute(
        """
        SELECT DATE_FORMAT(cal_date, '%%Y%%m%%d')
        FROM stock_trade_calendar
        WHERE is_open = 1
          AND cal_date >= STR_TO_DATE(%s, '%%Y%%m%%d')
          AND cal_date <= STR_TO_DATE(%s, '%%Y%%m%%d')
        ORDER BY cal_date
        """,
        (start_date, end_date),
    )
    return [row[0] for row in cursor.fetchall()]


def delete_trade_date_range(cursor, table_name: str, start_date: str, end_date: str) -> None:
    """删除目标表在 [start_date, end_date] 内的数据（全量刷新前清窗口）。

    **表名是拼进 SQL 的**（不能参数化）：调用方只能传内部常量表名，
    绝不能传外部输入，否则是注入面。
    """
    cursor.execute(
        f"""
        DELETE FROM {table_name}
        WHERE trade_date >= STR_TO_DATE(%s, '%%Y%%m%%d')
          AND trade_date <= STR_TO_DATE(%s, '%%Y%%m%%d')
        """,
        (start_date, end_date),
    )
