
# 服务层 
"""服务层包。

按领域分子包与模块：
- 数据读取/落盘：`data_reader`、`parquet_state_store`、`parquet_event_store`、
  `minute_parquet_reader` / `minute_parquet_store`、`wide_table_status`；
- 因子与模型：`factor_engine`、`factor_expression_engine`、`factor_analyzer`、
  `stock_scoring`、`ml_models`、`model_training_job_service`；
- 回测与组合：`backtest_engine`、`single_stock_backtest`、`portfolio_optimizer`；
- 行情与看板：`market_snapshot_service`、`board_market_service`、`market_dashboard`、
  `heatmap_service`、`stock_service`、`stock_pool_service`；
- 实时链路：`realtime_*` 系列 + `websocket_push_service`；
- 数据任务：`data_jobs/`（注册表、执行器、状态库、门面）；
- 对话与文本：`ai/`、`llm_service`、`nlp_processor`、`text2sql_engine`、`sql_generator`。

约定：本包不 import `app.api`，依赖方向只能是 API → services → (utils / models / data)。
"""
