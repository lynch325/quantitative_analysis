"""数据任务（data_jobs）的运行状态与游标持久化。

两张 Parquet 表（落在 `{DATA_DIR}/data_job_state/`）：
- `data_job_runs`：每次作业运行一行，承载 status/progress/params/结果/错误；
- `data_job_cursors`：增量同步游标（job_type + cursor_key 唯一）。

实现要点：
- 底层复用通用 ParquetStateStore（见 app.services.parquet_state_store），
  但所有「读-改-写」都在 `store.locked()` 内完成——web 进程提交与 worker
  写状态会并发写同一张表，不持锁会丢更新；
- JSON 字段（params_json/result_json）统一以稳定序列化文本落盘
  （sort_keys，见 _json_text），避免同义 JSON 被判为不同参数；
- 兼容两个历史包袱：旧 `ml_factor_state/` 目录迁移、旧表里 JSON 列的
  二次规范化（见 _migrate_previous_state_if_needed / _normalize_runs_file）。

运行前提：本模块的锁是文件级独占锁，持有期间禁止嵌套调用另一个 locked()，
同进程重入会死锁（见 ParquetStateStore.locked 的说明）。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from app.utils.time_utils import now_local, now_local_iso
import json
import os
from pathlib import Path
import shutil
from typing import Any, Dict, List, Optional

import pandas as pd

from app.services.parquet_state_store import ParquetStateStore


def _now_iso() -> str:
    """当前本地时间的 ISO 字符串（项目统一时间源，见 app.utils.time_utils）。"""
    return now_local_iso()


def _to_python(value: Any) -> Any:
    """把 pandas/numpy 标量转成可 JSON 化的 Python 原生值；缺失值统一为 None。

    parquet 读出的 np.int64 / pd.Timestamp 直接进 json.dumps 会抛错，
    故所有出库字段都先过这里。
    """
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return value
    return value


def _normalize_json_payload(value: Any) -> Any:
    """把 JSON 字段值规整为 dict/list：字符串尝试反序列化，失败则原样返回。

    历史表里该列可能存的是序列化文本、也可能是真对象；调用方据此统一取值。
    """
    value = _to_python(value)
    if value is None or value == "":
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return value
    return value


def _json_text(value: Any) -> str:
    """把 JSON 字段序列化为稳定文本（sort_keys）用于落盘与参数比对。

    必须稳定：`find_active_duplicate` 与 `prune_superseded_failures` 都以
    该文本（或其等价 dict）判断「同一作业同一参数」，键序不同的同义 JSON
    会被判成两次不同提交。空值统一落成 `{}` 而非空串。
    """
    normalized = _normalize_json_payload(value)
    if normalized is None:
        normalized = {}
    if isinstance(normalized, str):
        return normalized
    return json.dumps(normalized, ensure_ascii=False, sort_keys=True)


@dataclass
class DataJobRunRecord:
    """一次数据任务运行的完整状态（data_job_runs 表的一行）。

    字段语义以状态机为准：
    - status: pending → queued → running → success / failed / cancelled；
    - progress: 0~1；progress_message 为面向用户的阶段文案；
    - params_json: 提交时的作业参数（用于重复提交判定与历史追溯）；
    - queued_at/started_at/finished_at 由状态流转自动补齐（见 update_run_status），
      started_at 缺失时僵尸判定退化为用 created_at 计时。
    """

    id: int
    job_type: str
    status: str
    progress: float
    progress_message: Optional[str]
    params_json: Dict[str, Any]
    source_name: Optional[str]
    source_mode: Optional[str]
    snapshot_tag: Optional[str]
    result_json: Optional[Dict[str, Any]]
    error_message: Optional[str]
    log_text: Optional[str]
    queued_at: Optional[str]
    started_at: Optional[str]
    finished_at: Optional[str]
    created_at: Optional[str]
    updated_at: Optional[str]

    def to_dict(self) -> Dict[str, Any]:
        """转成 API 返回用的 dict；params_json 缺失时补 `{}` 保证前端结构稳定。"""
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
            "queued_at": self.queued_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


class DataJobCursorRecord:
    """增量同步游标：某作业在某维度（如某股票）上「已同步到哪」的断点。

    这是 task 断点续传的依据，不是运行记录；updated_at 缺省取当前时间。
    """

    def __init__(self, job_type: str, cursor_key: str, cursor_value: str, updated_at: Optional[str] = None):
        """构造游标记录；updated_at 省略时用当前本地时间。"""
        self.job_type = job_type
        self.cursor_key = cursor_key
        self.cursor_value = cursor_value
        self.updated_at = updated_at or _now_iso()


class ParquetDataJobStateStore:
    """数据任务状态库：runs（运行记录）+ cursors（增量游标）两张 Parquet 表。

    所有写方法都在文件锁内做「读-改-写」，因此：
    - 单表数据量增长会直接放大写延迟（每次写都是整表重写）；
    - 禁止在 locked() 内调用本类的其他写方法（同进程重入死锁）。
    """

    TABLE_RUNS = "data_job_runs"
    TABLE_CURSORS = "data_job_cursors"

    def __init__(self, base_dir: Optional[str] = None):
        """定位状态目录并做一次自愈：先迁移历史目录，再规范化 JSON 列。

        base_dir 省略时取 `{DATA_DIR}/data_job_state`；构造期会读表并可能
        重写（仅在内容确实需要规范化时），因此不要在持锁上下文中构造。
        """
        if base_dir is None:
            data_dir = os.getenv(
                "DATA_DIR",
                str(Path(__file__).resolve().parents[3] / "data"),
            )
            base_dir = os.path.join(data_dir, "data_job_state")
        self.store = ParquetStateStore(base_dir=base_dir)
        self._migrate_previous_state_if_needed()
        self._normalize_state_files()

    def create_run(
        self,
        job_type: str,
        params: Dict[str, Any],
        source_name: Optional[str] = None,
        source_mode: Optional[str] = None,
        snapshot_tag: Optional[str] = None,
        progress_message: Optional[str] = None,
    ) -> DataJobRunRecord:
        """新建一条 pending 运行记录并返回（id 由表内自增分配）。

        整个「读表 → 取 id → 追加 → 写表」持表锁执行；调用方拿到返回值后
        应保留该对象并把后续状态流转交给 update_run_status / save_run。
        """
        # web 进程提交与 worker 写状态会并发竞争这张表，整个读-改-写持锁执行
        with self.store.locked(self.TABLE_RUNS):
            df = self.store.read_frame(self.TABLE_RUNS)
            now = _now_iso()
            run_id = int(self.store.next_integer_id(self.TABLE_RUNS))
            record = {
                "id": run_id,
                "job_type": job_type,
                "status": "pending",
                "progress": 0.0,
                "progress_message": progress_message,
                "params_json": _json_text(params or {}),
                "source_name": source_name,
                "source_mode": source_mode,
                "snapshot_tag": snapshot_tag,
                "result_json": None,
                "error_message": None,
                "log_text": None,
                "queued_at": now,
                "started_at": None,
                "finished_at": None,
                "created_at": now,
                "updated_at": now,
            }
            df = pd.concat([df, pd.DataFrame([record])], ignore_index=True) if not df.empty else pd.DataFrame([record])
            self.store.write_frame(self.TABLE_RUNS, df)
            return self._row_to_record(pd.Series(record))

    def find_active_duplicate(self, job_type: str, params: Dict[str, Any]) -> Optional[DataJobRunRecord]:
        """查找同作业同参数的进行中记录（pending/queued/running），用于去重提交。

        只在最近 1000 条内从新往旧找；因为僵尸 run 也会持续命中，
        调用方通常先跑 reap_stale_runs() 清理，否则同一作业会被永久拒绝提交。
        """
        runs = self.list_runs(limit=1000)
        active_status = {"pending", "queued", "running"}
        for run in reversed(runs):
            if run.job_type == job_type and run.status in active_status and (run.params_json or {}) == (params or {}):
                return run
        return None

    # 与 ScriptRunner 的子进程超时保持同一量级：超过它还没终态的 run，
    # 只可能是 worker 被杀/掉电留下的僵尸
    STALE_RUN_DEFAULT_TIMEOUT_SECONDS = 3600

    @classmethod
    def _stale_timeout_seconds(cls) -> float:
        """僵尸判定阈值：读 DATA_JOB_TIMEOUT（秒），非法或非正数回退默认 3600。"""
        raw = os.getenv("DATA_JOB_TIMEOUT", "")
        try:
            value = float(raw)
        except (TypeError, ValueError):
            return float(cls.STALE_RUN_DEFAULT_TIMEOUT_SECONDS)
        return value if value > 0 else float(cls.STALE_RUN_DEFAULT_TIMEOUT_SECONDS)

    def reap_stale_runs(self, timeout_seconds: Optional[float] = None) -> List[DataJobRunRecord]:
        """把长期停留在 pending/queued/running 的僵尸 run 标记为 failed。

        worker 被 kill -9 / 掉电后 run 永远停在 running，
        find_active_duplicate 会据此永久拒绝同一作业再次提交。
        以 started_at（未开始则 created_at）+ 超时判定，超时与
        子进程 DATA_JOB_TIMEOUT 对齐。返回本次清理的 run 列表。
        """
        limit = float(timeout_seconds) if timeout_seconds else self._stale_timeout_seconds()
        now = now_local()
        reaped: List[DataJobRunRecord] = []
        for run in self.list_runs(limit=1000):
            if run.status not in {"pending", "queued", "running"}:
                continue
            reference = run.started_at or run.created_at
            try:
                started = datetime.fromisoformat(str(reference))
            except (TypeError, ValueError):
                started = None
            if started is None or (now - started).total_seconds() <= limit:
                continue
            reaped.append(
                self.update_run_status(
                    run,
                    "failed",
                    error_message=(
                        f"run 超过 {int(limit)} 秒仍处于 {run.status}，"
                        "判定为 worker 中断留下的僵尸任务并强制失败"
                    ),
                    progress_message="已超时强制失败",
                )
            )
        return reaped

    def get_run(self, run_id: int) -> Optional[DataJobRunRecord]:
        """按 id 读取单条运行记录；表为空或无此 id 时返回 None。

        id 存在同表多条时取最后一行（写入总是追加/覆盖，理论上不应重复）。
        """
        df = self.store.read_frame(self.TABLE_RUNS)
        if df.empty or "id" not in df.columns:
            return None
        match = df[pd.to_numeric(df["id"], errors="coerce") == int(run_id)]
        if match.empty:
            return None
        return self._row_to_record(match.iloc[-1])

    def list_runs(self, limit: int = 50, status: Optional[str] = None) -> List[DataJobRunRecord]:
        """按 id 倒序返回运行记录（最新在前），可按 status 过滤后截断 limit 条。

        每次调用都整表读入内存，故 limit 只在读取后生效，不减少 IO。
        """
        df = self.store.read_frame(self.TABLE_RUNS)
        if df.empty:
            return []
        if status and "status" in df.columns:
            df = df[df["status"] == status]
        if "id" in df.columns:
            df = df.sort_values("id", ascending=False)
        if limit:
            df = df.head(limit)
        return [self._row_to_record(row) for _, row in df.iterrows()]

    def update_run_status(
        self,
        run: DataJobRunRecord,
        status: str,
        progress: Optional[float] = None,
        error_message: Optional[str] = None,
        progress_message: Optional[str] = None,
    ) -> DataJobRunRecord:
        """更新状态（并按需更新进度/文案/错误），返回落库后的最新记录。

        传参为 None 的字段保持原值不动，避免调用方无意清空已有信息；
        时间戳自动维护：status=running 首次写 started_at，进入终态写 finished_at。
        表或该 id 不存在时原样返回入参对象（不抛异常）。
        """
        with self.store.locked(self.TABLE_RUNS):
            df = self.store.read_frame(self.TABLE_RUNS)
            if df.empty or "id" not in df.columns:
                return run
            mask = pd.to_numeric(df["id"], errors="coerce") == int(run.id)
            if not mask.any():
                return run
            now = _now_iso()
            df.loc[mask, "status"] = status
            if progress is not None:
                df.loc[mask, "progress"] = float(progress)
                run.progress = float(progress)
            if error_message is not None:
                df.loc[mask, "error_message"] = error_message
                run.error_message = error_message
            if progress_message is not None:
                df.loc[mask, "progress_message"] = progress_message
                run.progress_message = progress_message
            if status == "running" and not run.started_at:
                df.loc[mask, "started_at"] = now
                run.started_at = now
            if status in {"success", "failed", "cancelled"}:
                df.loc[mask, "finished_at"] = now
                run.finished_at = now
            df.loc[mask, "updated_at"] = now
            run.status = status
            run.updated_at = now
            self.store.write_frame(self.TABLE_RUNS, df)
        return self.get_run(run.id) or run

    def save_run(self, run: DataJobRunRecord) -> DataJobRunRecord:
        """整行覆盖保存（update_run_status 之外的字段走这里）。

        写前会刷新 updated_at 并把 params_json/result_json 重新稳定序列化，
        避免调用方传入的 dict 直接落盘导致文本形态不一致。
        """
        with self.store.locked(self.TABLE_RUNS):
            df = self.store.read_frame(self.TABLE_RUNS)
            if df.empty or "id" not in df.columns:
                return run
            mask = pd.to_numeric(df["id"], errors="coerce") == int(run.id)
            if not mask.any():
                return run
            payload = run.to_dict()
            payload["updated_at"] = _now_iso()
            payload["params_json"] = _json_text(payload.get("params_json"))
            if payload.get("result_json") is not None:
                payload["result_json"] = _json_text(payload.get("result_json"))
            for key, value in payload.items():
                df.loc[mask, key] = [value]
            self.store.write_frame(self.TABLE_RUNS, df)
        return self.get_run(run.id) or run

    def upsert_cursor(self, job_type: str, cursor_key: str, cursor_value: str) -> DataJobCursorRecord:
        """写入/更新 (job_type, cursor_key) 游标；旧行先删再加，保证唯一。

        整表加锁重写，属低频调用（每次增量同步推进时一次）。
        """
        with self.store.locked(self.TABLE_CURSORS):
            df = self.store.read_frame(self.TABLE_CURSORS)
            now = _now_iso()
            record = {
                "job_type": job_type,
                "cursor_key": cursor_key,
                "cursor_value": cursor_value,
                "updated_at": now,
            }
            if df.empty:
                df = pd.DataFrame([record])
            else:
                mask = (df["job_type"] == job_type) & (df["cursor_key"] == cursor_key)
                df = df[~mask]
                df = pd.concat([df, pd.DataFrame([record])], ignore_index=True)
            self.store.write_frame(self.TABLE_CURSORS, df)
            return DataJobCursorRecord(**record)

    def _row_to_record(self, row: pd.Series) -> DataJobRunRecord:
        """把表行转成 DataJobRunRecord，并做缺列/脏值兜底。

        params_json 保证是 dict；result_json 若是纯文本会包成 `{"raw": ...}`，
        status 缺失退化为 'pending'，id/progress 走数值强转。
        """
        params = _to_python(row.get("params_json")) or {}
        result_json = _to_python(row.get("result_json"))
        params = _normalize_json_payload(params) or {}
        result_json = _normalize_json_payload(result_json)
        if isinstance(result_json, str):
            result_json = {"raw": result_json}
        return DataJobRunRecord(
            id=int(_to_python(row.get("id")) or 0),
            job_type=str(_to_python(row.get("job_type")) or ""),
            status=str(_to_python(row.get("status")) or "pending"),
            progress=float(_to_python(row.get("progress")) or 0.0),
            progress_message=_to_python(row.get("progress_message")),
            params_json=params,
            source_name=_to_python(row.get("source_name")),
            source_mode=_to_python(row.get("source_mode")),
            snapshot_tag=_to_python(row.get("snapshot_tag")),
            result_json=result_json,
            error_message=_to_python(row.get("error_message")),
            log_text=_to_python(row.get("log_text")),
            queued_at=_to_python(row.get("queued_at")),
            started_at=_to_python(row.get("started_at")),
            finished_at=_to_python(row.get("finished_at")),
            created_at=_to_python(row.get("created_at")),
            updated_at=_to_python(row.get("updated_at")),
        )

    def _migrate_previous_state_if_needed(self) -> None:
        """一次性迁移：把旧 `ml_factor_state/` 下的两张表搬到当前目录。

        仅在目标文件不存在且源文件存在时用 shutil.move 搬迁；构造期调用，
        幂等，迁移后旧路径不再引用。
        """
        current_runs = self.store.path_for(self.TABLE_RUNS)
        current_cursors = self.store.path_for(self.TABLE_CURSORS)
        current_dir = self.store.base_dir
        previous_dir = current_dir.parent / "ml_factor_state"

        if previous_dir == current_dir:
            return

        migrations = [
            (previous_dir / f"{self.TABLE_RUNS}.parquet", current_runs),
            (previous_dir / f"{self.TABLE_CURSORS}.parquet", current_cursors),
        ]
        for previous_path, current_path in migrations:
            if current_path.exists() or not previous_path.exists():
                continue
            current_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(previous_path), str(current_path))

    def _normalize_state_files(self) -> None:
        """构造期自愈入口：当前只规范化 runs 表，游标表无需处理。"""
        self._normalize_runs_file()

    def _normalize_runs_file(self) -> None:
        """把历史 runs 表的 JSON 列统一成稳定文本，并清掉中间列 `__params_key`。

        针对早期写入形态的兼容：当时 params_json/result_json 可能落成 dict。
        内容无变化时直接返回，避免每次启动都无谓重写整表。
        """
        path = self.store.path_for(self.TABLE_RUNS)
        if not path.exists():
            return
        df = self.store.read_frame(self.TABLE_RUNS)
        if df.empty:
            return

        normalized = df.copy()
        if "params_json" in normalized.columns:
            normalized["params_json"] = normalized["params_json"].apply(_json_text)
        if "result_json" in normalized.columns:
            normalized["result_json"] = normalized["result_json"].apply(
                lambda value: None if _normalize_json_payload(value) is None else _json_text(value)
            )
        if "__params_key" in normalized.columns:
            normalized = normalized.drop(columns=["__params_key"])

        if normalized.equals(df):
            return
        self.store.write_frame(self.TABLE_RUNS, normalized)

    def prune_superseded_failures(self) -> int:
        """清理「已被后续成功覆盖」的历史失败记录，返回删除条数。

        判定口径：(job_type, 稳定化 params) 相同、且 id 小于该组最新成功 id 的
        failed 行会被删除——重试成功后旧的失败记录只有噪声价值。
        持锁整表重写；无删除项时不写盘。
        """
        with self.store.locked(self.TABLE_RUNS):
            df = self.store.read_frame(self.TABLE_RUNS)
            if df.empty or "id" not in df.columns or "job_type" not in df.columns:
                return 0

            work_df = df.copy()
            work_df["id"] = pd.to_numeric(work_df["id"], errors="coerce")
            if "status" not in work_df.columns:
                return 0
            if "params_json" in work_df.columns:
                work_df["__params_key"] = work_df["params_json"].apply(
                    lambda value: json.dumps(_normalize_json_payload(value) or {}, ensure_ascii=True, sort_keys=True)
                )
            else:
                work_df["__params_key"] = "{}"

            drop_ids: list[int] = []
            success_groups = work_df[work_df["status"] == "success"].groupby(["job_type", "__params_key"], dropna=False)
            for (job_type, params_key), group in success_groups:
                latest_success_id = group["id"].max()
                failed_mask = (
                    (work_df["job_type"] == job_type)
                    & (work_df["status"] == "failed")
                    & (work_df["__params_key"] == params_key)
                    & (work_df["id"] < latest_success_id)
                )
                drop_ids.extend(work_df.loc[failed_mask, "id"].dropna().astype(int).tolist())

            if not drop_ids:
                return 0

            filtered = work_df[~work_df["id"].isin(set(drop_ids))].reset_index(drop=True)
            filtered = filtered.drop(columns=["__params_key"])
            self.store.write_frame(self.TABLE_RUNS, filtered)
            return len(drop_ids)
