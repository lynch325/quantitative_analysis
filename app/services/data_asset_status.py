"""数据资产初始化状态：Parquet 健康检查 + 推荐下一步。

迁移说明：本函数原先定义在 `app/main/views.py`（Jinja 页面模块），随 Jinja
前端一并删除。它是纯业务逻辑（不含页面渲染），故迁到 services 层，
供 `/api/data-jobs/initialization-status` 使用。
"""
from flask import current_app
from startup_runtime import build_health_report, inspect_parquet_data_assets


def inspect_data_management_status():
    """返回数据资产健康报告（连接状态、已存在表、非空表）。"""
    connected, existing_tables, non_empty_tables = inspect_parquet_data_assets()

    return build_health_report(
        current_app.config,
        connected=connected,
        existing_tables=existing_tables,
        non_empty_tables=non_empty_tables,
    )
