"""数据任务运行记录（ORM 版，表 `data_job_run`）。

**已无实际引用**：任务状态落盘走 ParquetDataJobStateStore（data_job_runs /
data_job_cursors 两张 Parquet 表），本模型只在 app/models/__init__.py 里导出兼容。

口径提醒：`updated_at` 的 onupdate 用 `datetime.utcnow`，而 default 与其它时间列
用 `now_local`（本地时间）——同一行里两种时区口径混用，比对时间时需注意。
"""

from datetime import datetime

from app.extensions import db
from app.utils.time_utils import now_local


class DataJobRun(db.Model):
    """Data download job run record."""

    __tablename__ = "data_job_run"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    job_type = db.Column(db.String(64), nullable=False, index=True)
    status = db.Column(db.String(32), nullable=False, default="pending", index=True)
    progress = db.Column(db.Float, nullable=False, default=0.0)
    progress_message = db.Column(db.String(255))
    params_json = db.Column(db.JSON, nullable=False, default=dict)
    source_name = db.Column(db.String(64), index=True)
    source_mode = db.Column(db.String(32))
    snapshot_tag = db.Column(db.String(64))
    result_json = db.Column(db.JSON)
    error_message = db.Column(db.Text)
    log_text = db.Column(db.Text)
    queued_at = db.Column(db.DateTime, nullable=False, default=now_local)
    started_at = db.Column(db.DateTime)
    finished_at = db.Column(db.DateTime)
    created_at = db.Column(db.DateTime, nullable=False, default=now_local)
    updated_at = db.Column(
        db.DateTime, nullable=False, default=now_local, onupdate=datetime.utcnow
    )

    def to_dict(self):
        """任务运行的 API 形态（供前端轮询进度）：params_json 与 result_json 已是对象，
        三个时间戳（queued_at / started_at / finished_at）转 ISO 串，缺失为 None。
        """
        return {
            "id": self.id,
            "job_type": self.job_type,
            "status": self.status,
            "progress": self.progress,
            "progress_message": self.progress_message,
            "params_json": self.params_json or {},
            "source_name": self.source_name,
            "source_mode": self.source_mode,
            "snapshot_tag": self.snapshot_tag,
            "result_json": self.result_json,
            "error_message": self.error_message,
            "queued_at": self.queued_at.isoformat() if self.queued_at else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
        }
