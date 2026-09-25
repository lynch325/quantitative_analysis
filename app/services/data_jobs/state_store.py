"""数据任务状态存储的兼容包装。

历史代码按 `DataJobStateStore` 的方法名调用，实际实现已统一到
`ParquetDataJobStateStore`（Parquet 两表：data_job_runs / data_job_cursors）。
本类只做方法转发、不承载任何逻辑；新代码请直接依赖 ParquetDataJobStateStore。
"""

from typing import Any, Dict, Optional

from app.services.data_jobs.parquet_state_store import ParquetDataJobStateStore


class DataJobStateStore:
    """Compatibility wrapper for data job state backed by Parquet."""

    def __init__(self, state_store: Optional[Any] = None):
        self.state_store = state_store or ParquetDataJobStateStore()

    def create_run(
        self,
        job_type: str,
        params: Dict[str, Any],
        source_name: Optional[str] = None,
        source_mode: Optional[str] = None,
        snapshot_tag: Optional[str] = None,
        progress_message: Optional[str] = None,
    ):
        """建 run 的薄封装：转发给底层 state_store，便于替换实现或注入测试替身。
        """
        return self.state_store.create_run(
            job_type=job_type,
            params=params,
            source_name=source_name,
            source_mode=source_mode,
            snapshot_tag=snapshot_tag,
            progress_message=progress_message,
        )

    def find_active_duplicate(self, job_type: str, params: Dict[str, Any]):
        return self.state_store.find_active_duplicate(job_type, params)

    def get_run(self, run_id: int):
        return self.state_store.get_run(run_id)

    def update_run_status(
        self,
        run,
        status: str,
        progress: Optional[float] = None,
        error_message: Optional[str] = None,
        progress_message: Optional[str] = None,
    ):
        """更新 run 状态的薄封装：转发给底层 state_store，便于替换实现或注入测试替身。
        """
        return self.state_store.update_run_status(
            run,
            status,
            progress=progress,
            error_message=error_message,
            progress_message=progress_message,
        )

    def save_run(self, run):
        return self.state_store.save_run(run)

    def upsert_cursor(self, job_type: str, cursor_key: str, cursor_value: str):
        return self.state_store.upsert_cursor(job_type, cursor_key, cursor_value)

    def list_runs(self, limit: int = 50, status: Optional[str] = None):
        return self.state_store.list_runs(limit=limit, status=status)
