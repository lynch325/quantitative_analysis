# 工具包 
"""工具层包（脚本 + 读取/写盘助手 + 数据源适配）。

三类内容，注意区分：
- **数据作业脚本**（由 data_jobs 以子进程执行，`main()` 入口）：
  `stock_basic_*`、`daily_history_*`、`daily_basic_*`、`moneyflow*`、
  `stk_factor*`、`cyq_perf*`、`financial_*`、`minute_sync_tickflow`、
  `ma_calculator`、`wide_table_builder`、`factor_compute`；
- **作业公共骨架**：`parquet_job_helpers.py`（日期解析/缺口回补/限速重试/
  分区落盘）、`parquet_writer.py`（原子写与文件锁）、`job_env.py`（仅少量函数仍在用）；
- **被服务层复用的工具**：`cache.py`、`time_utils.py`、`data_sources/`（扶摇/TickFlow/
  通达信等外部源客户端）、`stock_name_registry.py` 等。

注意：作业脚本内部用**顶层模块名**互相导入（如 `from db_utils import ...`），
因为子进程运行时本目录在 sys.path 上；而服务层引用这些工具时走 `app.utils.*`。
"""
