"""数据任务服务门面：提交、去重、重试与查询。

提交流程：清理僵尸 run → find_active_duplicate 去重 → create_run(pending)
→ update_run_status(queued) → 按 execution_mode 派发。

执行模式：去 Redis/Celery 后只剩 **inline** 一种（见 _resolve_execution_mode），
即用后台线程执行 run_data_job；线程内自建 app context，状态推进与失败落盘
全部由它负责，提交侧立即返回 queued——同步跑会把提交请求挂住最长
DATA_JOB_TIMEOUT，浏览器超时重试还会被去重逻辑拒绝。
"""

import threading
from app.utils.time_utils import now_local
from typing import Any, Dict, Optional

from app.services.data_jobs.parquet_state_store import ParquetDataJobStateStore
from app.services.data_jobs.registry import JobRegistry
from app.tasks.data_jobs_tasks import run_data_job

try:
    from flask import current_app
except Exception:  # pragma: no cover
    current_app = None


def _resolve_execution_mode(explicit_mode: Optional[str] = None) -> str:
    """解析任务执行模式：优先入参，其次 Flask 配置 DATA_JOB_EXECUTION_MODE。

    **去 Redis/Celery 后只剩 inline 一种实现**，app context 不可用时也按 inline 处理，
    因此这里的兜底不是「降级」而是当前唯一模式。
    """
    if explicit_mode:
        return explicit_mode

    try:
        if current_app:
            mode = current_app.config.get("DATA_JOB_EXECUTION_MODE")
            if mode:
                return str(mode).lower()
    except Exception:
        pass

    # 去 Redis/Celery 后只有本地执行一种模式；app context 不可用时也按本地处理
    return "inline"


class DataJobService:
    """Facade for data job submission and querying."""

    def __init__(
        self,
        registry: Optional[JobRegistry] = None,
        state_store: Optional[Any] = None,
        execution_mode: Optional[str] = None,
    ):
        self.registry = registry or JobRegistry()
        self.state_store = state_store or ParquetDataJobStateStore()
        self.execution_mode = _resolve_execution_mode(execution_mode)

    def submit(self, job_type: str, params: Optional[Dict[str, Any]] = None):
        """提交数据任务：先清僵尸、再查重、最后建 run。

        顺序是刻意的：
        1. **先 reap_stale_runs** —— worker 被 kill 后 run 会永远停在 running，
           不清理的话查重会永久拒绝该作业再次提交；
        2. find_active_duplicate 命中即抛 ValueError（幂等保护）；
        3. 建 run 抛 TypeError 时回退到精简参数调用，兼容 state_store 的签名差异。
        """
        definition = self.registry.get_job(job_type)
        params = params or {}
        # 提交前先清理僵尸 run：worker 被 kill 后 run 永远停在 running，
        # 不清理的话 find_active_duplicate 会永久拒绝该作业再次提交
        reap_stale = getattr(self.state_store, "reap_stale_runs", None)
        if callable(reap_stale):
            reap_stale()
        find_active_duplicate = getattr(self.state_store, "find_active_duplicate", None)
        if callable(find_active_duplicate):
            duplicate_run = find_active_duplicate(job_type, params)
            if duplicate_run is not None:
                raise ValueError(f"duplicate running job: {duplicate_run.id}")

        snapshot_tag = now_local().strftime("%Y-%m-%d")
        try:
            run = self.state_store.create_run(
                job_type,
                params,
                source_name=definition.source_name,
                source_mode=definition.source_mode,
                snapshot_tag=snapshot_tag,
                progress_message="已创建任务，等待调度",
            )
        except TypeError:
            run = self.state_store.create_run(job_type, params)
            for field_name, field_value in {
                "source_name": definition.source_name,
                "source_mode": definition.source_mode,
                "snapshot_tag": snapshot_tag,
                "progress_message": "已创建任务，等待调度",
            }.items():
                if hasattr(run, field_name):
                    setattr(run, field_name, field_value)

        try:
            run = self.state_store.update_run_status(
                run,
                "queued",
                progress=0.0,
                progress_message="任务已入队",
            )
        except TypeError:
            run = self.state_store.update_run_status(run, "queued", progress=0.0)

        if self.execution_mode == "inline":
            # inline 任务在后台线程执行：同步跑会把提交请求挂住最长
            # DATA_JOB_TIMEOUT（默认 1 小时），浏览器超时后重试还会撞上
            # find_active_duplicate 被拒。run_data_job 自建 app context，
            # 状态推进/失败落盘全部由它负责，提交侧立即返回 queued
            thread = threading.Thread(
                target=lambda: run_data_job(run.id),
                name=f"data-job-{run.job_type}-{run.id}",
                daemon=True,
            )
            thread.start()
            return run

        run_data_job.delay(run.id)
        return run

    def retry(self, run_id: int):
        run = self.get_run(run_id)
        if run is None:
            raise ValueError(f"job run not found: {run_id}")
        return self.submit(run.job_type, run.params_json or {})

    def list_job_definitions(self, visible_only: bool = True):
        if visible_only:
            return self.registry.list_visible_jobs()
        return self.registry.list_jobs()

    def list_runs(self, limit: int = 50, status: Optional[str] = None):
        return self.state_store.list_runs(limit=limit, status=status)

    def get_run(self, run_id: int):
        return self.state_store.get_run(run_id)
