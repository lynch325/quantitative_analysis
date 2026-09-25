"""模型训练任务的进程内追踪：提交、进度快照与结果供前端轮询。

定位：这是**轻量进程内**实现（无 Celery/Redis），训练跑在
ThreadPoolExecutor(max_workers=2) 里，任务状态存在 self.job_store 字典中——
- 进程重启即丢历史任务，多 worker 部署时各进程看到的状态不同；
- job_store 只进不出会缓慢泄漏，超过 MAX_TRACKED_JOBS(200) 时按 created_at
  从旧到新淘汰**已结束**的任务（运行中的不淘汰）。

提交时会先用 MLModelManager.resolve_training_date_range 校正日期区间（保证
尾部样本能算出未来收益标签），校正结果与原因写入任务日志，前端轮询可见。
"""

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from app.utils.time_utils import now_local_iso
from threading import Lock
from typing import Any, Dict, Optional
from uuid import uuid4

from app.services.ml_models import MLModelManager


class ModelTrainingJobService:
    """Lightweight in-process training job tracker for UI polling."""

    # job_store 只进不出会随长期运行缓慢泄漏；超过上限时按 created_at
    # 从旧到新淘汰已结束的任务（运行中的不淘汰）
    MAX_TRACKED_JOBS = 200

    def __init__(self, manager: Optional[MLModelManager] = None, job_store: Optional[Dict[str, Dict[str, Any]]] = None):
        self.manager = manager or MLModelManager()
        self.job_store = job_store if job_store is not None else {}
        self._lock = Lock()
        self._executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="model-train")

    def _evict_finished_jobs_locked(self) -> None:
        """任务数超过上限时淘汰**已结束**的任务（success / failed / cancelled）。

        按 created_at 字符串排序近似 FIFO；运行中的任务永不淘汰。
        **调用方必须已持有 self._lock**（函数名以 _locked 结尾即为此约定）。
        """
        if len(self.job_store) <= self.MAX_TRACKED_JOBS:
            return
        ordered = sorted(
            self.job_store.items(),
            key=lambda item: str(item[1].get("created_at") or ""),
        )
        finished_states = {"success", "failed", "cancelled"}
        for stale_id, snapshot in ordered:
            if len(self.job_store) <= self.MAX_TRACKED_JOBS:
                break
            if snapshot.get("status") in finished_states:
                del self.job_store[stale_id]

    def submit_job(self, model_id: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """提交训练任务并入队，返回任务快照。

        日期窗口先经 resolve_training_date_range 按可用数据裁剪，
        date_range_adjusted 标记是否被调整，调整原因写进 logs ——
        用户能直接看到「为什么我传的窗口变了」。提交后需轮询查进度。
        """
        resolved = self.manager.resolve_training_date_range(model_id, start_date, end_date)
        resolved_start_date = resolved["start_date"]
        resolved_end_date = resolved["end_date"]
        job_id = str(uuid4())
        logs = [f"已提交训练任务: {model_id}"]
        if resolved.get("adjusted") and resolved.get("message"):
            logs.append(resolved["message"])
        snapshot = {
            "job_id": job_id,
            "model_id": model_id,
            "start_date": resolved_start_date,
            "end_date": resolved_end_date,
            "requested_start_date": resolved.get("requested_start_date", start_date),
            "requested_end_date": resolved.get("requested_end_date", end_date),
            "date_range_adjusted": bool(resolved.get("adjusted")),
            "status": "queued",
            "progress": 0.0,
            "step": "已加入训练队列",
            "logs": logs,
            "result": None,
            "error": None,
            "created_at": now_local_iso(),
            "started_at": None,
            "finished_at": None,
        }
        with self._lock:
            self.job_store[job_id] = snapshot
            self._evict_finished_jobs_locked()
        self._executor.submit(self._run_job, job_id, model_id, resolved_start_date, resolved_end_date)
        return deepcopy(snapshot)

    def get_job_snapshot(self, job_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            snapshot = self.job_store.get(job_id)
            return deepcopy(snapshot) if snapshot is not None else None

    def _run_job(self, job_id: str, model_id: str, start_date: str, end_date: str) -> None:
        """训练任务的实际执行体（跑在线程里）。

        通过 progress_callback 回写进度 / 步骤 / 日志，每次写入都持锁；
        首次回调顺带补 started_at。异常在内部转成 failed 状态而不外抛 ——
        线程里的异常没人接收，只有落进任务快照才可见。
        """
        def progress_callback(progress: float, step: str, log_message: Optional[str] = None) -> None:
            with self._lock:
                snapshot = self.job_store[job_id]
                snapshot["status"] = "running"
                snapshot["progress"] = progress
                snapshot["step"] = step
                if snapshot["started_at"] is None:
                    snapshot["started_at"] = now_local_iso()
                if log_message:
                    snapshot["logs"].append(log_message)

        try:
            progress_callback(5.0, "初始化训练任务", f"开始训练模型: {model_id}")
            result = self.manager.train_model(
                model_id,
                start_date,
                end_date,
                progress_callback=progress_callback,
            )

            with self._lock:
                snapshot = self.job_store[job_id]
                if result.get("success"):
                    snapshot["status"] = "success"
                    snapshot["progress"] = 100.0
                    snapshot["step"] = "训练完成"
                    snapshot["result"] = result
                    snapshot["logs"].append("模型训练完成")
                else:
                    snapshot["status"] = "failed"
                    snapshot["progress"] = 100.0
                    snapshot["step"] = "训练失败"
                    snapshot["error"] = result.get("error", "训练失败")
                    snapshot["logs"].append(f"训练失败: {snapshot['error']}")
                snapshot["finished_at"] = now_local_iso()
        except Exception as exc:
            with self._lock:
                snapshot = self.job_store[job_id]
                snapshot["status"] = "failed"
                snapshot["progress"] = 100.0
                snapshot["step"] = "训练失败"
                snapshot["error"] = str(exc)
                snapshot["logs"].append(f"训练异常: {exc}")
                snapshot["finished_at"] = now_local_iso()
