"""Parquet-backed state repositories for ml-factor business objects.

This module provides a lightweight filesystem-backed persistence layer for the
stateful parts of the ml-factor feature set:

- factor definitions
- factor values
- model definitions
- model predictions
- portfolio positions
- backtest runs

The implementation intentionally keeps the API simple and pandas-friendly so it
can be used as a drop-in replacement for ORM-backed storage during the
migration.
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from app.utils.parquet_writer import parquet_file_lock
from app.utils.time_utils import now_local_iso
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence

import pandas as pd
from loguru import logger



class StateStoreError(RuntimeError):
    """状态存储读写失败（文件损坏/IO错误）。"""


def _now_iso() -> str:
    return now_local_iso()


def _as_iso(value: Any) -> Optional[str]:
    """把值规整成 ISO 字符串（None / 空串 → None）。

    落盘前统一时间列格式：Timestamp、datetime 或任何带 isoformat 的对象都能转，
    保证库里 created_at / updated_at 是可比较的字符串而不是对象。
    """
    if value is None or value == "":
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _to_python_scalar(value: Any) -> Any:
    """把 pandas / numpy 标量还原成 Python 原生值；缺失值 → None。

    parquet 读出的 np.int64、pd.Timestamp 直接进 json.dumps 会抛错，
    所有出库字段都要先过这里。
    """
    if pd.isna(value):
        return None
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return value


def _normalize_json(value: Any) -> Any:
    """把 JSON 列的值规整成 dict / list：字符串尝试反序列化，失败则原样返回。

    历史写入形态不一（有的落 JSON 文本、有的落对象），读取侧统一在此归一。
    """
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return value
    return value


def _jsonify_object_columns(frame: pd.DataFrame) -> pd.DataFrame:
    """把 object 列中的 dict/list 单元格序列化为 JSON 字符串。

    Parquet 无法写入"没有子字段的空 struct"（例如 params={}），
    pyarrow 会抛 ArrowNotImplementedError 让整表写入失败；
    统一转成字符串后由读取侧的 _normalize_json 还原。
    """
    for column in frame.columns:
        if frame[column].dtype != object:
            continue
        frame[column] = frame[column].map(
            lambda value: json.dumps(value, ensure_ascii=False)
            if isinstance(value, (dict, list))
            else value
        )
    return frame


class ParquetStateStore:
    """Filesystem-backed store for Parquet state tables."""

    def __init__(self, base_dir: Optional[str] = None):
        if base_dir is None:
            base_dir = os.getenv(
                "DATA_DIR",
                os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data"),
            )
            base_dir = os.path.join(base_dir, "ml_factor_state")
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def path_for(self, name: str) -> Path:
        return self.base_dir / f"{name}.parquet"

    def read_frame(self, name: str) -> pd.DataFrame:
        """读取整张状态表；表文件不存在时返回空 DataFrame。

        读取失败一律抛 StateStoreError —— **绝不能返回空表**：上层读-改-写会把
        「空表」当成当前状态，随后的写回会把已有数据静默清空。
        """
        path = self.path_for(name)
        if not path.is_file():
            return pd.DataFrame()
        try:
            df = pd.read_parquet(path)
            return df
        except Exception as exc:
            # 读损坏时必须抛错：若返回空表，随后的 read-modify-write 会把
            # 已有数据全部静默清空（比读失败严重得多）
            raise StateStoreError(f"读取 Parquet 状态表失败 {path}: {exc}") from exc

    def write_frame(self, name: str, df: pd.DataFrame) -> None:
        """原子写入：先写临时文件再 rename，读方要么看到旧表要么看到新表，
        不会观察到写了一半的 parquet。"""
        path = self.path_for(name)
        path.parent.mkdir(parents=True, exist_ok=True)
        frame = df.copy()
        if not frame.empty:
            frame = frame.reset_index(drop=True)
        frame = _jsonify_object_columns(frame)
        tmp_path = path.with_name(f"{path.name}.tmp.{os.getpid()}")
        try:
            frame.to_parquet(tmp_path, index=False)
            os.replace(tmp_path, path)
        finally:
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)

    def locked(self, name: str):
        """以独占文件锁包裹一段 read-modify-write，跨进程互斥。

        web 进程与 Celery worker 会并发更新同一状态表；
        flock 保证同一时刻只有一个进程在做读-改-写。
        注意：不要在 locked 内嵌套调用另一个 locked（同进程会重入死锁）。
        """
        path = self.path_for(name)
        return parquet_file_lock(path.with_suffix(f"{path.suffix}.lock"))

    # ------------------------------------------------------------------
    # Hive 风格分区表支持：{base_dir}/{name}/{key}={value}/data.parquet
    # 适用于按交易日增长的大表（如 factor_values），读写只触碰涉及的分区
    # ------------------------------------------------------------------

    def _partition_path(self, name: str, partition_key: str,
                        partition_value: str) -> Path:
        """分区文件路径：`{base_dir}/{name}/{partition_column}={value}/data.parquet`。

        value 不做转义，调用方传入的日期/键值必须已是文件系统安全的形式。
        """
        return self.base_dir / name / f"{partition_key}={partition_value}" / "data.parquet"

    def list_partitions(self, name: str, partition_key: str = "trade_date") -> List[str]:
        """列出分区值（按名称排序）。表不存在时返回空列表。"""
        base = self.base_dir / name
        if not base.is_dir():
            return []
        prefix = f"{partition_key}="
        return sorted(
            d.name[len(prefix):]
            for d in base.iterdir()
            if d.is_dir() and d.name.startswith(prefix) and (d / "data.parquet").is_file()
        )

    def read_partition(self, name: str, partition_key: str,
                       partition_value: str) -> pd.DataFrame:
        """读取单个分区；分区不存在返回空表，读损坏抛 StateStoreError。"""
        path = self._partition_path(name, partition_key, partition_value)
        if not path.is_file():
            return pd.DataFrame()
        try:
            return pd.read_parquet(path)
        except Exception as exc:
            raise StateStoreError(f"读取 Parquet 分区失败 {path}: {exc}") from exc

    def write_partition(self, name: str, partition_key: str,
                        partition_value: str, df: pd.DataFrame) -> None:
        """原子写入单个分区：先写临时文件再 rename。"""
        path = self._partition_path(name, partition_key, partition_value)
        path.parent.mkdir(parents=True, exist_ok=True)
        frame = df.copy()
        if not frame.empty:
            frame = frame.reset_index(drop=True)
        frame = _jsonify_object_columns(frame)
        tmp_path = path.with_name(f"{path.name}.tmp.{os.getpid()}")
        try:
            frame.to_parquet(tmp_path, index=False)
            os.replace(tmp_path, path)
        finally:
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)

    def next_integer_id(self, name: str, column: str = "id") -> int:
        """取该表的下一个自增 id（现有最大值 + 1；空表或列缺失时从 1 起）。

        **必须在 store.locked() 内调用**：并发创建时两个进程会拿到同一个 id。
        """
        df = self.read_frame(name)
        if df.empty or column not in df.columns:
            return 1
        numeric = pd.to_numeric(df[column], errors="coerce").dropna()
        if numeric.empty:
            return 1
        return int(numeric.max()) + 1


class FactorRepository:
    TABLE_DEFINITIONS = "factor_definitions"
    TABLE_VALUES = "factor_values"

    def __init__(self, store: ParquetStateStore):
        self.store = store

    def upsert_definition(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """写入或更新因子定义（按 factor_id 唯一，已存在则整行替换）。

        持表锁读-改-写；is_active 缺省 True，created_at 缺失时补当前时间，
        写入时刷新 updated_at。返回落库后的记录。
        """
        now = _now_iso()
        record = {
            **record,
            "is_active": bool(record.get("is_active", True)),
            "created_at": _as_iso(record.get("created_at")) or now,
            "updated_at": now,
        }
        with self.store.locked(self.TABLE_DEFINITIONS):
            df = self.store.read_frame(self.TABLE_DEFINITIONS)
            if df.empty:
                df = pd.DataFrame([record])
            else:
                if "factor_id" not in df.columns:
                    df = pd.DataFrame(columns=list(record.keys()))
                df = df[df["factor_id"] != record["factor_id"]]
                df = pd.concat([df, pd.DataFrame([record])], ignore_index=True)
            self.store.write_frame(self.TABLE_DEFINITIONS, df)
        return record

    def list_definitions(self, include_inactive: bool = False) -> List[Dict[str, Any]]:
        """列出因子定义（默认只含启用项），按 factor_id 排序。

        每行经 _record_to_dict 归一；include_inactive=True 时含已停用（软删除）的定义。
        """
        df = self.store.read_frame(self.TABLE_DEFINITIONS)
        if df.empty:
            return []
        if not include_inactive and "is_active" in df.columns:
            df = df[df["is_active"].fillna(True).astype(bool)]
        if "factor_id" in df.columns:
            df = df.sort_values(["factor_id"]).reset_index(drop=True)
        return [_record_to_dict(row) for _, row in df.iterrows()]

    def get_definition(self, factor_id: str) -> Optional[Dict[str, Any]]:
        """按 factor_id 取单条定义；不存在返回 None。

        同一 factor_id 出现多行时取最后一行（upsert 保证唯一，正常不会重复）。
        """
        df = self.store.read_frame(self.TABLE_DEFINITIONS)
        if df.empty or "factor_id" not in df.columns:
            return None
        match = df[df["factor_id"] == factor_id]
        if match.empty:
            return None
        return _record_to_dict(match.iloc[-1])

    def deactivate_definition(self, factor_id: str) -> bool:
        """停用因子定义（软删除：置 is_active=False），返回是否命中。

        持锁读-改-写；已停用或不存在时返回 False，不抛异常。
        取值侧默认过滤 is_active，因此停用后历史因子值仍在库中，只是不再参与打分。
        """
        with self.store.locked(self.TABLE_DEFINITIONS):
            df = self.store.read_frame(self.TABLE_DEFINITIONS)
            if df.empty or "factor_id" not in df.columns:
                return False
            mask = df["factor_id"] == factor_id
            if not mask.any():
                return False
            df.loc[mask, "is_active"] = False
            df.loc[mask, "updated_at"] = _now_iso()
            self.store.write_frame(self.TABLE_DEFINITIONS, df)
        return True

    def save_values(self, frame: pd.DataFrame) -> int:
        """写入因子值（按 trade_date 分区、按业务键去重后 upsert），返回写入行数。

        口径与实现要点：
        - 必填列 ts_code / trade_date / factor_id / factor_value，缺列直接抛 ValueError；
        - trade_date 先按 format='mixed' 归一（库里两种格式混存），解析失败的行丢弃并告警；
        - factor_value / z_score / percentile_rank 统一转 float32：因子精度足够，
          库体积与读取内存约省一半；
        - 只读-改-写涉及的分区（不是整表），单次写入成本与表总规模无关。
        """
        if frame is None or frame.empty:
            return 0
        required = {"ts_code", "trade_date", "factor_id", "factor_value"}
        missing = required - set(frame.columns)
        if missing:
            raise ValueError(f"factor values missing columns: {sorted(missing)}")

        self._migrate_legacy_values()

        df = frame.copy()
        # trade_date 归一化到天：它同时是分区键和业务键的一部分
        df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce", format="mixed")
        total = len(df)
        df = df.dropna(subset=["trade_date"])
        if len(df) < total:
            logger.warning(f"因子值有 {total - len(df)} 行 trade_date 无效，写入时丢弃")
        if df.empty:
            return 0
        # 数值列统一 float32：因子值精度足够，库体积与读取内存约省一半
        for column in ("factor_value", "z_score", "percentile_rank"):
            if column in df.columns:
                df[column] = pd.to_numeric(df[column], errors="coerce").astype("float32")

        # 按交易日分区写入：trade_date 是业务键的一部分，同一记录只会
        # 落入一个分区，跨分区不会产生重复；每次只读-改-写涉及的分区，
        # 而不是整表重写
        written = 0
        for date_value, partition_frame in df.groupby(df["trade_date"].dt.normalize()):
            partition_key = pd.Timestamp(date_value).strftime("%Y-%m-%d")
            with self.store.locked(self.TABLE_VALUES):
                existing = self.store.read_partition(
                    self.TABLE_VALUES, "trade_date", partition_key
                )
                if existing.empty:
                    combined = partition_frame
                else:
                    combined = pd.concat([existing, partition_frame], ignore_index=True)
                combined = self._dedupe_values(combined)
                self.store.write_partition(
                    self.TABLE_VALUES, "trade_date", partition_key, combined
                )
            written += len(partition_frame)
        return written

    def _migrate_legacy_values(self) -> None:
        """旧单文件 factor_values.parquet 一次性迁移到按日分区。

        幂等：迁移完成后旧文件被改名为 .migrated 备份，不会重复迁移；
        迁移在文件锁内进行并二次检查，多进程并发安全。
        """
        legacy_path = self.store.path_for(self.TABLE_VALUES)
        if not legacy_path.is_file():
            return
        with self.store.locked(self.TABLE_VALUES):
            if not legacy_path.is_file():
                return  # 另一个进程已完成迁移
            legacy = self.store.read_frame(self.TABLE_VALUES)
            migrated_rows = 0
            if not legacy.empty:
                normalized = legacy.copy()
                normalized["trade_date"] = pd.to_datetime(
                    normalized["trade_date"], errors="coerce", format="mixed"
                )
                normalized = normalized.dropna(subset=["trade_date"])
                for date_value, partition_frame in normalized.groupby(
                    normalized["trade_date"].dt.normalize()
                ):
                    partition_key = pd.Timestamp(date_value).strftime("%Y-%m-%d")
                    existing = self.store.read_partition(
                        self.TABLE_VALUES, "trade_date", partition_key
                    )
                    if existing.empty:
                        combined = partition_frame
                    else:
                        combined = pd.concat([existing, partition_frame], ignore_index=True)
                    combined = self._dedupe_values(combined)
                    self.store.write_partition(
                        self.TABLE_VALUES, "trade_date", partition_key, combined
                    )
                    migrated_rows += len(partition_frame)
            backup_path = legacy_path.with_name(f"{legacy_path.name}.migrated")
            legacy_path.replace(backup_path)
            logger.info(
                f"factor_values 已迁移到按交易日分区存储"
                f"（{migrated_rows} 行），旧文件备份为 {backup_path.name}"
            )

    def get_values(
        self,
        factor_ids: Optional[Sequence[str]] = None,
        trade_date: Optional[str] = None,
        ts_codes: Optional[Sequence[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """按因子 / 代码 / 日期窗口读因子值，返回升序 DataFrame。

        性能与口径：
        - 先按日期做**分区裁剪**，只读涉及的分区；
        - 库内 trade_date 混存 YYYYMMDD 与 YYYY-MM-DD，必须 `format='mixed'`：
          默认格式推断会把第二种格式静默变成 NaT 再被 dropna 丢掉（历史数据凭空消失）；
        - end_date 为闭区间且含当日全天（内部加到 23:59:59.999999）；
        - 结果按 (trade_date, ts_code, factor_id) 升序。
        """
        self._migrate_legacy_values()

        # 分区裁剪：按日期过滤时只读取涉及的分区
        partitions = self.store.list_partitions(self.TABLE_VALUES)
        if not partitions:
            return pd.DataFrame()
        wanted = self._select_partitions(partitions, trade_date, start_date, end_date)
        frames = [
            self.store.read_partition(self.TABLE_VALUES, "trade_date", partition)
            for partition in wanted
        ]
        df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        if df.empty:
            return df

        if "trade_date" in df.columns:
            # 库内 trade_date 是 YYYYMMDD 字符串、YYYY-MM-DD 字符串与
            # Timestamp 混存（历史写入口径不一），必须 format='mixed'：
            # 默认格式推断会把第二种格式静默变成 NaT 然后被 dropna 丢掉
            df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce", format="mixed")
            df = df.dropna(subset=["trade_date"])

        start_dt = pd.to_datetime(start_date, errors="coerce") if start_date is not None else None
        end_dt = pd.to_datetime(end_date, errors="coerce") if end_date is not None else None
        trade_dt = pd.to_datetime(trade_date, errors="coerce") if trade_date is not None else None

        if factor_ids is not None:
            df = df[df["factor_id"].isin(set(factor_ids))]
        if trade_dt is not None and "trade_date" in df.columns:
            df = df[df["trade_date"].dt.normalize() == trade_dt.normalize()]
        if ts_codes is not None:
            df = df[df["ts_code"].isin(set(ts_codes))]
        if start_dt is not None and "trade_date" in df.columns:
            df = df[df["trade_date"] >= start_dt]
        if end_dt is not None and "trade_date" in df.columns:
            df = df[df["trade_date"] <= end_dt + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)]

        if df.empty:
            return df

        for column in ["trade_date", "created_at"]:
            if column in df.columns:
                converted = pd.to_datetime(df[column], errors="coerce")
                if converted.notna().any():
                    df[column] = converted
        sort_cols = [c for c in ["trade_date", "ts_code", "factor_id"] if c in df.columns]
        if sort_cols:
            df = df.sort_values(sort_cols).reset_index(drop=True)
        return df

    @staticmethod
    def _select_partitions(partitions: List[str], trade_date: Optional[str],
                           start_date: Optional[str],
                           end_date: Optional[str]) -> List[str]:
        """根据查询条件裁剪分区。trade_date 精确匹配优先于区间。"""
        parsed = {p: pd.to_datetime(p, errors="coerce") for p in partitions}
        if trade_date is not None:
            trade_dt = pd.to_datetime(trade_date, errors="coerce")
            if pd.isna(trade_dt):
                return []
            return [p for p, ts in parsed.items() if ts == trade_dt]

        lo = pd.to_datetime(start_date, errors="coerce") if start_date else None
        hi = pd.to_datetime(end_date, errors="coerce") if end_date else None
        return [
            p for p, ts in parsed.items()
            if not pd.isna(ts)
            and (lo is None or ts >= lo)
            and (hi is None or ts <= hi)
        ]

    def _dedupe_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """因子值去重：按 (ts_code, trade_date, factor_id) 保留最新一条（keep=last）。

        去重前先把 trade_date 归一为 Timestamp —— 混格式字符串直接比较会让同一天的
        两条记录都留下，读取端出现重复行。
        """
        if df.empty:
            return df
        key_cols = [c for c in ["ts_code", "trade_date", "factor_id"] if c in df.columns]
        if not key_cols:
            return df
        # trade_date 归一化为 Timestamp 后再去重：库内 YYYYMMDD 与
        # YYYY-MM-DD 两种字符串并存时按字符串去重会让同一天的记录都保留，
        # 读取端出现重复行
        if "trade_date" in df.columns:
            normalized = pd.to_datetime(df["trade_date"], errors="coerce", format="mixed")
            if normalized.notna().any():
                df = df.assign(trade_date=normalized.where(normalized.notna(), df["trade_date"]))
        for column in ["created_at"]:
            if column in df.columns:
                df[column] = df[column].astype(str)
        df = df.sort_values(key_cols + [c for c in ["created_at"] if c in df.columns]).reset_index(drop=True)
        return df.drop_duplicates(subset=key_cols, keep="last")


class ModelRepository:
    TABLE_DEFINITIONS = "model_definitions"
    TABLE_PREDICTIONS = "ml_predictions"

    def __init__(self, store: ParquetStateStore):
        self.store = store

    def upsert_definition(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """写入或更新模型定义（按 model_id 唯一）。

        factor_list / model_params / training_config 三个结构化字段统一序列化成 JSON
        文本落库，读取侧由 _record_to_dict 的 json_columns 反序列化。
        """
        now = _now_iso()
        record = {
            **record,
            "factor_list": json.dumps(record.get("factor_list", [])),
            "model_params": json.dumps(record.get("model_params", {})),
            "training_config": json.dumps(record.get("training_config", {})),
            "is_active": bool(record.get("is_active", True)),
            "created_at": _as_iso(record.get("created_at")) or now,
            "updated_at": now,
        }
        with self.store.locked(self.TABLE_DEFINITIONS):
            df = self.store.read_frame(self.TABLE_DEFINITIONS)
            if df.empty:
                df = pd.DataFrame([record])
            else:
                df = df[df["model_id"] != record["model_id"]]
                df = pd.concat([df, pd.DataFrame([record])], ignore_index=True)
            self.store.write_frame(self.TABLE_DEFINITIONS, df)
        return record

    def list_definitions(self, include_inactive: bool = False) -> List[Dict[str, Any]]:
        """列出模型定义（默认只含启用项），按 model_id 排序；结构化字段已反序列化。
        """
        df = self.store.read_frame(self.TABLE_DEFINITIONS)
        if df.empty:
            return []
        if not include_inactive and "is_active" in df.columns:
            df = df[df["is_active"].fillna(True).astype(bool)]
        df = df.sort_values(["model_id"]).reset_index(drop=True)
        return [_record_to_dict(row, json_columns={"factor_list", "model_params", "training_config"}) for _, row in df.iterrows()]

    def get_definition(self, model_id: str) -> Optional[Dict[str, Any]]:
        """按 model_id 取单条定义；不存在返回 None（结构化字段已反序列化）。
        """
        df = self.store.read_frame(self.TABLE_DEFINITIONS)
        if df.empty or "model_id" not in df.columns:
            return None
        match = df[df["model_id"] == model_id]
        if match.empty:
            return None
        return _record_to_dict(match.iloc[-1], json_columns={"factor_list", "model_params", "training_config"})

    def delete_definition(self, model_id: str) -> bool:
        """删除模型定义：软删定义（is_active=False）并**物理删除该模型的全部预测结果**。

        先在定义表锁内软删，再在预测表锁内删除该 model_id 的预测行——预测数据
        没有保留价值且体积大。任一步未命中返回 False。
        """
        with self.store.locked(self.TABLE_DEFINITIONS):
            df = self.store.read_frame(self.TABLE_DEFINITIONS)
            if df.empty or "model_id" not in df.columns:
                return False
            mask = df["model_id"] == model_id
            if not mask.any():
                return False
            df.loc[mask, "is_active"] = False
            df.loc[mask, "updated_at"] = _now_iso()
            self.store.write_frame(self.TABLE_DEFINITIONS, df)
        with self.store.locked(self.TABLE_PREDICTIONS):
            pred_df = self.store.read_frame(self.TABLE_PREDICTIONS)
            if not pred_df.empty and "model_id" in pred_df.columns:
                pred_df = pred_df[pred_df["model_id"] != model_id]
                self.store.write_frame(self.TABLE_PREDICTIONS, pred_df)
        return True

    def save_predictions(self, frame: pd.DataFrame) -> int:
        """写入模型预测（必填 ts_code / trade_date / model_id，缺列抛 ValueError）。

        整表读-改-写 + 按 (ts_code, trade_date, model_id) 去重 keep=last，
        因此重复预测是覆盖而非追加。返回入参行数。
        """
        if frame is None or frame.empty:
            return 0
        required = {"ts_code", "trade_date", "model_id"}
        missing = required - set(frame.columns)
        if missing:
            raise ValueError(f"predictions missing columns: {sorted(missing)}")

        with self.store.locked(self.TABLE_PREDICTIONS):
            df = self.store.read_frame(self.TABLE_PREDICTIONS)
            if df.empty:
                combined = frame.copy()
            else:
                combined = pd.concat([df, frame], ignore_index=True)
            combined = self._dedupe_predictions(combined)
            self.store.write_frame(self.TABLE_PREDICTIONS, combined)
        return len(frame)

    def get_predictions(
        self,
        model_id: Optional[str] = None,
        trade_date: Optional[str] = None,
        ts_codes: Optional[Sequence[str]] = None,
    ) -> pd.DataFrame:
        """按模型 / 交易日 / 代码过滤预测结果，按 (trade_date, ts_code, model_id) 升序返回。

        trade_date 在库内是字符串，过滤用 astype(str) 等值比较（不做日期解析）；
        因此调用方传 '2026-09-25' 与库内 '20260925' 不会匹配。
        """
        df = self.store.read_frame(self.TABLE_PREDICTIONS)
        if df.empty:
            return df
        if model_id is not None:
            df = df[df["model_id"] == model_id]
        if trade_date is not None:
            df = df[df["trade_date"].astype(str) == str(trade_date)]
        if ts_codes is not None:
            df = df[df["ts_code"].isin(set(ts_codes))]
        if df.empty:
            return df
        for column in ["trade_date", "created_at"]:
            if column in df.columns:
                df[column] = df[column].astype(str)
        sort_cols = [c for c in ["trade_date", "ts_code", "model_id"] if c in df.columns]
        if sort_cols:
            df = df.sort_values(sort_cols).reset_index(drop=True)
        return df

    def _dedupe_predictions(self, df: pd.DataFrame) -> pd.DataFrame:
        """预测去重：按 (ts_code, trade_date, model_id) 保留最新一条（keep=last）。

        与 _dedupe_values 同理，先归一 trade_date 再比较，避免混格式字符串留下旧行。
        """
        if df.empty:
            return df
        key_cols = [c for c in ["ts_code", "trade_date", "model_id"] if c in df.columns]
        if not key_cols:
            return df
        # 与 _dedupe_values 同理：trade_date 归一化后再去重，
        # 混格式字符串比较会保留旧行
        if "trade_date" in df.columns:
            normalized = pd.to_datetime(df["trade_date"], errors="coerce", format="mixed")
            if normalized.notna().any():
                df = df.assign(trade_date=normalized.where(normalized.notna(), df["trade_date"]))
        for column in ["created_at"]:
            if column in df.columns:
                df[column] = df[column].astype(str)
        df = df.sort_values(key_cols + [c for c in ["created_at"] if c in df.columns]).reset_index(drop=True)
        return df.drop_duplicates(subset=key_cols, keep="last")


class PortfolioRepository:
    TABLE_POSITIONS = "portfolio_positions"

    def __init__(self, store: ParquetStateStore):
        self.store = store

    def create_position(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """新建持仓记录（id 在锁内分配），返回落库后的记录。

        id 分配必须在表锁内，否则并发创建会拿到同一个 id；
        is_active 缺省 True，created_at / updated_at 缺失时补当前时间。
        """
        now = _now_iso()
        with self.store.locked(self.TABLE_POSITIONS):
            # id 分配必须在锁内：并发创建时两个进程会拿到同一个 id
            record = {
                **record,
                "id": int(record.get("id") or self.store.next_integer_id(self.TABLE_POSITIONS)),
                "is_active": bool(record.get("is_active", True)),
                "created_at": _as_iso(record.get("created_at")) or now,
                "updated_at": _as_iso(record.get("updated_at")) or now,
            }
            df = self.store.read_frame(self.TABLE_POSITIONS)
            if df.empty:
                df = pd.DataFrame([record])
            else:
                df = pd.concat([df, pd.DataFrame([record])], ignore_index=True)
            self.store.write_frame(self.TABLE_POSITIONS, df)
        return record

    def list_positions(self, portfolio_id: str, active_only: bool = True) -> List[Dict[str, Any]]:
        """列出某组合的持仓（默认只含生效中），按 (created_at, id) 升序。
        """
        df = self.store.read_frame(self.TABLE_POSITIONS)
        if df.empty or "portfolio_id" not in df.columns:
            return []
        df = df[df["portfolio_id"] == portfolio_id]
        if active_only and "is_active" in df.columns:
            df = df[df["is_active"].fillna(True).astype(bool)]
        if df.empty:
            return []
        sort_cols = [c for c in ["created_at", "id"] if c in df.columns]
        if sort_cols:
            df = df.sort_values(sort_cols).reset_index(drop=True)
        return [_record_to_dict(row) for _, row in df.iterrows()]

    def list_portfolio_ids(self, active_only: bool = True) -> List[str]:
        """列出库中出现过的组合 id（去重排序）；active_only 时只看生效持仓。

        注意这是从持仓行反推出来的集合：某组合的持仓被全部停用/删除后，
        它的 id 也会从结果里消失。
        """
        df = self.store.read_frame(self.TABLE_POSITIONS)
        if df.empty or "portfolio_id" not in df.columns:
            return []
        if active_only and "is_active" in df.columns:
            df = df[df["is_active"].fillna(True).astype(bool)]
        return sorted(df["portfolio_id"].dropna().astype(str).unique().tolist())

    def get_position_by_stock(self, portfolio_id: str, ts_code: str) -> Optional[Dict[str, Any]]:
        """取某组合下某只股票的生效持仓；不存在返回 None。
        """
        df = self.store.read_frame(self.TABLE_POSITIONS)
        if df.empty:
            return None
        mask = (df["portfolio_id"] == portfolio_id) & (df["ts_code"] == ts_code)
        if "is_active" in df.columns:
            mask &= df["is_active"].fillna(True).astype(bool)
        match = df[mask]
        if match.empty:
            return None
        return _record_to_dict(match.iloc[-1])

    def deactivate_portfolio(self, portfolio_id: str) -> int:
        """停用整个组合（把其下所有生效持仓置 is_active=False），返回影响行数。

        软删除：行仍留在库中，只是不再参与组合指标计算。
        """
        with self.store.locked(self.TABLE_POSITIONS):
            df = self.store.read_frame(self.TABLE_POSITIONS)
            if df.empty or "portfolio_id" not in df.columns:
                return 0
            mask = df["portfolio_id"] == portfolio_id
            if "is_active" in df.columns:
                mask &= df["is_active"].fillna(True).astype(bool)
            count = int(mask.sum())
            if count == 0:
                return 0
            df.loc[mask, "is_active"] = False
            df.loc[mask, "updated_at"] = _now_iso()
            self.store.write_frame(self.TABLE_POSITIONS, df)
        return count

    def upsert_position(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """按 id 覆盖写入持仓记录（无 id 时在锁内分配），返回落库记录。

        与 create_position 的差别在语义：会先剔除同 id 的旧行再追加，属覆盖写入。
        """
        now = _now_iso()
        with self.store.locked(self.TABLE_POSITIONS):
            record = {
                **record,
                "id": int(record.get("id") or self.store.next_integer_id(self.TABLE_POSITIONS)),
                "is_active": bool(record.get("is_active", True)),
                "created_at": _as_iso(record.get("created_at")) or now,
                "updated_at": _as_iso(record.get("updated_at")) or now,
            }
            df = self.store.read_frame(self.TABLE_POSITIONS)
            if df.empty:
                df = pd.DataFrame([record])
            else:
                if "id" in df.columns:
                    df = df[df["id"] != record["id"]]
                df = pd.concat([df, pd.DataFrame([record])], ignore_index=True)
            self.store.write_frame(self.TABLE_POSITIONS, df)
        return record

    def refresh_prices(self, portfolio_id: str) -> Dict[str, Any]:
        """从通达信获取实时报价并更新所有持仓的当前价格。"""
        positions = self.list_positions(portfolio_id, active_only=True)
        if not positions:
            return {"updated": 0, "total": 0}

        # 构建 (market, code) 列表
        from app.services.tongdaxin.code_mapping import any_style_code_to_tdx
        stock_params = []
        ts_code_list = []
        for pos in positions:
            ts_code = pos.get("ts_code", "")
            try:
                market, code = any_style_code_to_tdx(ts_code)
                stock_params.append((market, code))
                ts_code_list.append(ts_code)
            except ValueError:
                continue

        if not stock_params:
            return {"updated": 0, "total": len(positions)}

        # 调用通达信批量获取实时报价
        from app.services.tongdaxin.client import create_hq_api, connected_session
        price_map: Dict[str, float] = {}
        try:
            api = create_hq_api()
            with connected_session(api):
                quotes = api.get_security_quotes(stock_params)
                if quotes:
                    for quote in quotes:
                        c = quote.get("code", "")
                        price = quote.get("price")
                        if price and price > 0:
                            # 反查 ts_code
                            matched = [tc for tc in ts_code_list if tc.startswith(c[:6])]
                            if matched:
                                price_map[matched[0]] = float(price)
        except Exception as exc:
            logger.warning(f"通达信实时报价获取失败: {exc}")
            return {"updated": 0, "total": len(positions), "error": str(exc)}

        if not price_map:
            return {"updated": 0, "total": len(positions)}

        # 更新 Parquet（网络报价获取已在锁外完成，锁只包住读改写）
        with self.store.locked(self.TABLE_POSITIONS):
            df = self.store.read_frame(self.TABLE_POSITIONS)
            if df.empty or "portfolio_id" not in df.columns:
                return {"updated": 0, "total": len(positions)}

            now = _now_iso()
            updated = 0
            for ts_code, new_price in price_map.items():
                mask = (df["portfolio_id"] == portfolio_id) & (df["ts_code"] == ts_code)
                if "is_active" in df.columns:
                    mask &= df["is_active"].fillna(True).astype(bool)
                if not mask.any():
                    continue
                df.loc[mask, "current_price"] = new_price
                pos_size = pd.to_numeric(df.loc[mask, "position_size"], errors="coerce").fillna(0)
                avg_cost = pd.to_numeric(df.loc[mask, "avg_cost"], errors="coerce").fillna(0)
                df.loc[mask, "market_value"] = pos_size * new_price
                df.loc[mask, "unrealized_pnl"] = (new_price - avg_cost) * pos_size
                df.loc[mask, "updated_at"] = now
                updated += int(mask.sum())

            self.store.write_frame(self.TABLE_POSITIONS, df)
        return {"updated": updated, "total": len(positions)}

    def calculate_metrics(self, portfolio_id: str) -> Dict[str, Any]:
        """按组合当前生效持仓重算汇总指标（不落库，返回 dict）。

        口径：权重按持仓市值占比重算（无市值时退回行内 weight）；收益与风险指标
        由各行**已算好的字段**累加（market_value / unrealized_pnl / var_1d）。
        因此前提是入库前已按最新价刷新过持仓，否则拿到的是上次刷新时的快照。
        没有生效持仓时返回空 dict。
        """
        positions = self.list_positions(portfolio_id, active_only=True)
        if not positions:
            return {}
        total_market_value = sum(float(pos.get("market_value") or 0) for pos in positions)
        total_unrealized_pnl = sum(float(pos.get("unrealized_pnl") or 0) for pos in positions)
        enriched = []
        sector_distribution: Dict[str, float] = {}
        for pos in positions:
            weight = float(pos.get("weight") or 0)
            if total_market_value > 0:
                weight = float(pos.get("market_value") or 0) / total_market_value * 100
            sector = pos.get("sector") or "未知"
            sector_distribution[sector] = sector_distribution.get(sector, 0) + weight
            enriched.append({**pos, "weight": weight})
        portfolio_var_1d = sum(float(pos.get("var_1d") or 0) * (float(pos.get("weight") or 0) / 100) for pos in enriched)
        portfolio_var_5d = sum(float(pos.get("var_5d") or 0) * (float(pos.get("weight") or 0) / 100) for pos in enriched)
        return {
            "total_positions": len(enriched),
            "total_market_value": total_market_value,
            "total_unrealized_pnl": total_unrealized_pnl,
            "total_pnl_percentage": (total_unrealized_pnl / (total_market_value - total_unrealized_pnl) * 100) if total_market_value > total_unrealized_pnl else 0,
            "sector_distribution": sector_distribution,
            "portfolio_var_1d": portfolio_var_1d,
            "portfolio_var_5d": portfolio_var_5d,
            "max_position_weight": max((float(pos.get("weight") or 0) for pos in enriched), default=0),
            "positions": enriched,
        }


class BacktestRepository:
    TABLE_RUNS = "backtest_runs"

    def __init__(self, store: ParquetStateStore):
        self.store = store

    def create_run(
        self,
        strategy_config: Dict[str, Any],
        start_date: str,
        end_date: str,
        initial_capital: float,
        rebalance_frequency: str,
    ) -> Dict[str, Any]:
        """创建回测运行记录（id 在锁内分配），返回面向前端的 dict。

        strategy_config 与 summary 以 JSON 文本落库；summary 初始为空 dict，
        回测完成后由 update_summary 补写。返回值的字段名与读接口一致（不是库表列名），
        前端无需区分读写两种形态。
        """
        record = {
            "strategy_config_json": json.dumps(strategy_config or {}),
            "start_date": start_date,
            "end_date": end_date,
            "initial_capital": float(initial_capital),
            "rebalance_frequency": rebalance_frequency,
            "summary_json": None,
            "created_at": _now_iso(),
        }
        with self.store.locked(self.TABLE_RUNS):
            # id 分配必须在锁内，避免并发提交拿到同一个 id
            df = self.store.read_frame(self.TABLE_RUNS)
            run_id = int(self.store.next_integer_id(self.TABLE_RUNS))
            record = {"id": run_id, **record}
            if df.empty:
                df = pd.DataFrame([record])
            else:
                df = pd.concat([df, pd.DataFrame([record])], ignore_index=True)
            self.store.write_frame(self.TABLE_RUNS, df)
        return {
            "id": run_id,
            "strategy_config": strategy_config or {},
            "start_date": start_date,
            "end_date": end_date,
            "initial_capital": float(initial_capital),
            "rebalance_frequency": rebalance_frequency,
            "summary": {},
            "created_at": record["created_at"],
        }

    def get_run(self, run_id: int) -> Optional[Dict[str, Any]]:
        """按 run_id 取运行记录；不存在返回 None。

        出参把库表列名翻译成前端字段（strategy_config_json → strategy_config 等），
        并对数值列做类型兜底（initial_capital 缺失记 0）。
        """
        df = self.store.read_frame(self.TABLE_RUNS)
        if df.empty or "id" not in df.columns:
            return None
        match = df[pd.to_numeric(df["id"], errors="coerce") == int(run_id)]
        if match.empty:
            return None
        row = match.iloc[-1]
        return {
            "id": int(_to_python_scalar(row["id"])),
            "strategy_config": _normalize_json(row.get("strategy_config_json")) or {},
            "start_date": _to_python_scalar(row.get("start_date")),
            "end_date": _to_python_scalar(row.get("end_date")),
            "initial_capital": float(_to_python_scalar(row.get("initial_capital")) or 0),
            "rebalance_frequency": _to_python_scalar(row.get("rebalance_frequency")),
            "summary": _normalize_json(row.get("summary_json")) or {},
            "created_at": _as_iso(row.get("created_at")),
        }

    def update_summary(self, run_id: int, summary: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """覆盖写入回测汇总（summary_json），返回更新后的记录；未命中返回 None。

        这是**回测进度与终态的唯一写入点**：轮询接口读的 status / error 都在 summary 里，
        改这里的结构要同步前端轮询解析。
        """
        with self.store.locked(self.TABLE_RUNS):
            df = self.store.read_frame(self.TABLE_RUNS)
            if df.empty or "id" not in df.columns:
                return None
            mask = pd.to_numeric(df["id"], errors="coerce") == int(run_id)
            if not mask.any():
                return None
            df.loc[mask, "summary_json"] = json.dumps(summary or {})
            self.store.write_frame(self.TABLE_RUNS, df)
        return self.get_run(run_id)

    def reap_stale_runs(self, is_active: Callable[[int], bool]) -> List[Dict[str, Any]]:
        """把状态停在 queued/running 但已无活跃线程的孤儿 run 标记为 failed。

        is_active: 判活回调（生产方传 backtest_tasks.is_run_active，即进程
        内注册表）。回测线程是 daemon 线程、只随进程退出，因此"在册 =
        存活"是精确判活：在册的一律跳过（跑几小时都不会误杀）；不在册
        且仍处于 queued/running 的只可能是进程中断留下的孤儿。
        必填且在表锁内对每个候选逐个复查——调用方先快照再扫描的话，
        快照之后才提交并在册的 run 会被误清（TOCTOU）。
        """
        reaped: List[Dict[str, Any]] = []
        with self.store.locked(self.TABLE_RUNS):
            df = self.store.read_frame(self.TABLE_RUNS)
            if df.empty or "id" not in df.columns:
                return []
            changed = False
            for idx, row in df.iterrows():
                run_id = int(_to_python_scalar(row["id"]))
                if is_active(run_id):
                    continue
                summary = _normalize_json(row.get("summary_json")) or {}
                status = summary.get("status")
                if status not in {"queued", "running"}:
                    continue
                summary.update({
                    "status": "failed",
                    "error": (
                        "回测线程已不存在（进程可能中断或重启），"
                        "判定为孤儿任务并强制失败"
                    ),
                })
                df.loc[df.index == idx, "summary_json"] = json.dumps(summary)
                reaped.append({"id": run_id, "summary": summary})
                changed = True
            if changed:
                self.store.write_frame(self.TABLE_RUNS, df)
        return reaped

    def list_runs(self) -> List[Dict[str, Any]]:
        """列出全部回测运行记录，按 (created_at, id) 升序（与前端提交顺序一致）。
        """
        df = self.store.read_frame(self.TABLE_RUNS)
        if df.empty:
            return []
        df = df.sort_values(["created_at", "id"]).reset_index(drop=True)
        return [
            {
                "id": int(_to_python_scalar(row["id"])),
                "strategy_config": _normalize_json(row.get("strategy_config_json")) or {},
                "start_date": _to_python_scalar(row.get("start_date")),
                "end_date": _to_python_scalar(row.get("end_date")),
                "initial_capital": float(_to_python_scalar(row.get("initial_capital")) or 0),
                "rebalance_frequency": _to_python_scalar(row.get("rebalance_frequency")),
                "summary": _normalize_json(row.get("summary_json")) or {},
                "created_at": _as_iso(row.get("created_at")),
            }
            for _, row in df.iterrows()
        ]

    # ------------------------------------------------------------------
    # 回测完整结果（异步任务落盘，API 轮询读取）
    # ------------------------------------------------------------------

    TABLE_RESULTS = "backtest_results"

    def save_result(self, run_id: int, result: Dict[str, Any]) -> None:
        """保存回测完整结果（同一 run_id 覆盖写入）。"""
        with self.store.locked(self.TABLE_RESULTS):
            df = self.store.read_frame(self.TABLE_RESULTS)
            row = {
                "run_id": int(run_id),
                "result_json": json.dumps(result or {}, ensure_ascii=False, default=str),
                "created_at": _now_iso(),
            }
            if not df.empty and "run_id" in df.columns:
                df = df[pd.to_numeric(df["run_id"], errors="coerce") != int(run_id)]
            df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
            self.store.write_frame(self.TABLE_RESULTS, df)

    def get_result(self, run_id: int) -> Optional[Dict[str, Any]]:
        """取回测完整结果（大 JSON）；不存在返回 None。

        与 summary 分离存储：summary 供轮询轻量读取，result 只在需要图表数据时按需拉取，
        避免轮询把 MB 级结果反复搬进内存。
        """
        df = self.store.read_frame(self.TABLE_RESULTS)
        if df.empty or "run_id" not in df.columns:
            return None
        match = df[pd.to_numeric(df["run_id"], errors="coerce") == int(run_id)]
        if match.empty:
            return None
        return _normalize_json(match.iloc[-1].get("result_json"))


def _record_to_dict(row: Any, json_columns: Optional[Iterable[str]] = None) -> Dict[str, Any]:
    """表行 → dict，并按列名做类型归一。

    json_columns 中的列走 _normalize_json（反序列化）；created_at / updated_at /
    trade_date 走 _as_iso（时间串）；其余走 _to_python_scalar。
    各 Repository 的出参都经这里，改口径会同时影响所有下游 API 响应。
    """
    json_columns = set(json_columns or [])
    if isinstance(row, pd.Series):
        data = row.to_dict()
    else:
        data = dict(row)

    result: Dict[str, Any] = {}
    for key, value in data.items():
        if key in json_columns:
            result[key] = _normalize_json(value)
            continue
        if key in {"created_at", "updated_at", "trade_date"}:
            result[key] = _as_iso(value)
            continue
        result[key] = _to_python_scalar(value)
    return result
