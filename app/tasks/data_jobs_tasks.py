"""数据任务后台入口：`run_data_job(run_id)` 的完整生命周期管理。

**这是任务执行的唯一入口**（`@celery.task` 装饰只是形式——去 Celery 后
app/celery_app.py 是本地桩，真实调用走 service 里的后台线程）。

状态机约定（务必守住）：任何阶段的异常都必须把 run 落盘为 `failed`，
否则它会永远停在 running，界面上看不出任务已死、`find_active_duplicate`
还会永久拒绝同作业再次提交。为此最外层有兜底 except，连「failed 落盘本身
失败」也留日志。

两条联动：任务成功后按作业类型清缓存——`wide_table_builder` 成功会
失效 data_reader 的宽表缓存与 text2sql 的 SQLite 临时表缓存，保证后续请求
读到新数据；子进程 stdout/stderr 各截取末尾写入 result_json（日志有上限）。
"""

from loguru import logger

from app import create_app
from app.celery_app import celery
from app.services.data_jobs.parquet_state_store import ParquetDataJobStateStore
from app.services.data_jobs.registry import JobRegistry
from app.services.data_jobs.runner import ScriptRunner


def _build_app():
    return create_app("development")


def _build_registry():
    return JobRegistry()


def _build_state_store():
    return ParquetDataJobStateStore()


def _build_runner():
    return ScriptRunner()


@celery.task(name="data_jobs.run")
def run_data_job(run_id: int):
    """Background task entrypoint for data jobs.

    任何阶段的异常都必须把 run 落盘为 failed，否则状态会永远停在
    running，界面上无从得知任务已经死了。
    """

    app = _build_app()
    with app.app_context():
        store = _build_state_store()
        try:
            run = store.get_run(run_id)
            if run is None:
                return {"run_id": run_id, "status": "missing"}

            store.update_run_status(run, "running", progress=5.0, progress_message="任务开始执行")

            registry = _build_registry()
            try:
                definition = registry.get_job(run.job_type)
            except KeyError:
                run.error_message = f"未注册的任务类型: {run.job_type}"
                store.update_run_status(run, "failed", progress=100.0,
                                        error_message=run.error_message,
                                        progress_message="任务类型不存在")
                return {"run_id": run_id, "status": "failed", "error": run.error_message}

            runner = _build_runner()

            run.source_name = getattr(run, "source_name", None) or definition.source_name
            run.source_mode = getattr(run, "source_mode", None) or definition.source_mode
            store.save_run(run)

            runner_params = dict(run.params_json or {})
            runner_params.setdefault("source_name", run.source_name or definition.source_name)
            runner_params.setdefault("source_mode", run.source_mode or definition.source_mode)
            snapshot_tag = getattr(run, "snapshot_tag", None)
            if snapshot_tag:
                runner_params.setdefault("snapshot_tag", snapshot_tag)

            completed = runner.run_script(definition.script_path, params=runner_params)
            run.result_json = {
                "returncode": completed.returncode,
                "stdout": completed.stdout[-4000:] if completed.stdout else "",
                "stderr": completed.stderr[-4000:] if completed.stderr else "",
            }

            if completed.returncode == 0:
                store.update_run_status(run, "success", progress=100.0, progress_message="任务执行完成")
                # 大宽表构建成功后清除缓存，使后续请求读取新数据
                if run.job_type == "wide_table_builder":
                    from app.services.data_reader import ParquetDataReader
                    ParquetDataReader.invalidate_stock_business_cache()
                    # 同时清除 text2sql 的 SQLite 临时表缓存
                    from app.services.text2sql_engine import get_text2sql_engine
                    engine = get_text2sql_engine()
                    engine.query_executor.invalidate_cache()
                store.save_run(run)
                return {"run_id": run_id, "status": "success"}

            run.error_message = completed.stderr[-1000:] if completed.stderr else "task failed"
            store.update_run_status(
                run,
                "failed",
                progress=100.0,
                error_message=run.error_message,
                progress_message="任务执行失败",
            )
            store.save_run(run)
            return {"run_id": run_id, "status": "failed", "error": run.error_message}

        except Exception as exc:  # noqa: BLE001 - 任务入口必须兜底，保证状态落盘
            try:
                run = store.get_run(run_id)
                if run is not None:
                    run.error_message = f"任务异常: {exc}"[:1000]
                    store.update_run_status(run, "failed", progress=100.0,
                                            error_message=run.error_message,
                                            progress_message="任务异常终止")
                    store.save_run(run)
            except Exception as cleanup_exc:
                # 状态落盘本身失败时 run 会卡在 running 且无现场，至少留日志
                logger.error(f"任务 {run_id} 异常后的 failed 落盘也失败: {cleanup_exc}（原始异常: {exc}）")
            return {"run_id": run_id, "status": "failed", "error": str(exc)}
