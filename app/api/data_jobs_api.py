"""数据任务接口（url_prefix=/api/data-jobs）。

- `POST /submit`：提交作业。提交前对 `wide_table_builder` 做 **18:00 校验**
  （当日数据源可能尚未下载完，过早构建会得到残缺宽表）；
  `KeyError/ValueError` 视为参数错误返回 400，其他异常回 500；
- `GET /jobs`：作业定义列表（`?include_hidden=1` 时含未在 `_visible_job_types` 中的作业）；
- `GET /list`：运行记录列表；`GET /<run_id>`：单条详情；
  `POST /<run_id>/retry`：按原参数重新提交；
- `GET /init-status`：数据资产健康检查；`GET /wide-table/status` 与
  `POST /wide-table/build`：宽表状态与手动构建（构建同样受 18:00 校验约束）。

服务实例是模块级单例（get_data_job_service），DataJobService 内部持有
Parquet 状态库与注册表，每次请求重建代价高。
"""

from flask import Blueprint, jsonify, request

from app.services.data_jobs.service import DataJobService
from app.services.wide_table_status import get_wide_table_status


data_jobs_bp = Blueprint("data_jobs", __name__, url_prefix="/api/data-jobs")

_service = None


def get_data_job_service() -> DataJobService:
    global _service
    if _service is None:
        _service = DataJobService()
    return _service


@data_jobs_bp.route("/submit", methods=["POST"])
def submit_job():
    """POST 提交数据任务，返回 run_id 供前端轮询进度。

    **大宽表构建有 18:00 前置校验**：未过 18:00 时数据源可能还没下载完，直接 400，
    避免生成半截宽表。参数错误 → 400，执行异常 → 500。
    """
    payload = request.get_json(silent=True) or {}
    job_type = payload.get("job_type")
    if not job_type:
        return jsonify({"success": False, "error": "missing job_type"}), 400

    # 大宽表构建任务需通过 18:00 校验
    if job_type == "wide_table_builder":
        from flask import current_app
        status = get_wide_table_status(current_app.config.get("DATA_DIR"))
        if not status["past_cutoff"]:
            return jsonify({
                "success": False,
                "error": "当前时间未过 18:00，数据源可能尚未下载完毕，请稍后再试",
            }), 400

    params = payload.get("params", {})
    try:
        run = get_data_job_service().submit(job_type, params)
    except (KeyError, ValueError) as exc:
        return jsonify({"success": False, "error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"success": False, "error": str(exc)}), 500

    return jsonify(
        {
            "success": True,
            "run_id": run.id,
            "job_type": run.job_type,
            "status": run.status,
        }
    )


@data_jobs_bp.route("/jobs", methods=["GET"])
def list_jobs():
    include_hidden = (request.args.get("include_hidden") or "").lower() in {"1", "true", "yes"}
    jobs = [
        j.__dict__
        for j in get_data_job_service().list_job_definitions(visible_only=not include_hidden)
    ]
    return jsonify({"success": True, "jobs": jobs, "count": len(jobs)})


@data_jobs_bp.route("/list", methods=["GET"])
def list_runs():
    limit = int(request.args.get("limit", 50))
    status = request.args.get("status")
    runs = [run.to_dict() for run in get_data_job_service().list_runs(limit=limit, status=status)]
    return jsonify({"success": True, "runs": runs, "count": len(runs)})


@data_jobs_bp.route("/<int:run_id>", methods=["GET"])
def get_run(run_id: int):
    run = get_data_job_service().get_run(run_id)
    if run is None:
        return jsonify({"success": False, "error": "run not found"}), 404
    return jsonify({"success": True, "run": run.to_dict()})


@data_jobs_bp.route("/<int:run_id>/retry", methods=["POST"])
def retry_run(run_id: int):
    """POST 重跑历史任务；run_id 不存在返回 404，成功返回新的 run_id 与状态。
    """
    try:
        run = get_data_job_service().retry(run_id)
    except ValueError as exc:
        return jsonify({"success": False, "error": str(exc)}), 404
    except Exception as exc:
        return jsonify({"success": False, "error": str(exc)}), 500

    return jsonify({"success": True, "run_id": run.id, "status": run.status})


@data_jobs_bp.route("/init-status", methods=["GET"])
def init_status():
    """数据资产初始化状态（Parquet 健康检查 + 推荐下一步），供新前端数据管理页使用。"""
    from app.services.data_asset_status import inspect_data_management_status

    try:
        return jsonify({"success": True, "status": inspect_data_management_status()})
    except Exception as exc:
        return jsonify({"success": False, "error": str(exc)}), 500


@data_jobs_bp.route("/wide-table/status", methods=["GET"])
def wide_table_status():
    """返回大宽表状态：是否存在、日期、是否需要更新、是否过了 18:00。"""
    try:
        from flask import current_app
        data_dir = current_app.config.get("DATA_DIR")
        status = get_wide_table_status(data_dir)
        return jsonify({"success": True, "status": status})
    except Exception as exc:
        return jsonify({"success": False, "error": str(exc)}), 500


@data_jobs_bp.route("/wide-table/build", methods=["POST"])
def build_wide_table():
    """提交大宽表构建任务。需通过 18:00 校验。"""
    try:
        # 后端强制 18:00 校验，防止绕过 UI 直接调 API
        from flask import current_app
        status = get_wide_table_status(current_app.config.get("DATA_DIR"))
        if not status["past_cutoff"]:
            return jsonify({
                "success": False,
                "error": "当前时间未过 18:00，数据源可能尚未下载完毕，请稍后再试",
            }), 400

        run = get_data_job_service().submit("wide_table_builder", {})
        # 构建成功后清缓存（inline 模式立即生效，celery 模式在 task 完成后清）
        if run.status == "success":
            from app.services.data_reader import ParquetDataReader
            ParquetDataReader.invalidate_stock_business_cache()
            from app.services.text2sql_engine import get_text2sql_engine
            get_text2sql_engine().query_executor.invalidate_cache()
        return jsonify({
            "success": True,
            "run_id": run.id,
            "job_type": run.job_type,
            "status": run.status,
        })
    except (KeyError, ValueError) as exc:
        return jsonify({"success": False, "error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"success": False, "error": str(exc)}), 500
