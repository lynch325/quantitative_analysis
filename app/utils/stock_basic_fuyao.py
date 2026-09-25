"""扶摇股票清单刷新（stock_basic.parquet 的 fuyao 生产者）。

与 tushare 版（stock_basic.py）写入同一个 stock_basic.parquet。定位差异：
- 扶摇全市场快照一次拉齐（免费 key），适合日常刷新代码清单/发现新上市代码
- 快照没有股票名称字段（名称靠下游维表关联），也没有退市股（D/P 状态）
  和行业/地域/上市日期等元数据

合并策略：已有记录整体保留（tushare 元数据与退市记录不动），快照中新出现
的代码追加（list_status=L，名称待 tushare 版补齐）。名称刷新与完整元数据
仍以 tushare 版为准。
"""

import sys
from pathlib import Path

# 兼容直接运行（python app/utils/stock_basic_fuyao.py）
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import pandas as pd
from dotenv import load_dotenv
from loguru import logger

from app.utils.data_sources.fuyao_client import FuyaoClient
from app.utils.data_sources.fuyao_normalize import snapshot_rows_to_stock_basic
from app.utils.parquet_writer import save_single_parquet

REL_FILENAME = "stock_basic.parquet"


def _read_existing(data_dir) -> pd.DataFrame:
    """读取既有 stock_basic 文件；文件不存在或损坏都返回空表。

    损坏时按空处理是刻意的：后续走重建逻辑，好过让整个作业失败。
    """
    from app.utils.parquet_job_helpers import _default_data_root

    path = Path(data_dir or _default_data_root()) / REL_FILENAME
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_parquet(path)
    except Exception as exc:  # noqa: BLE001 - 坏文件按空处理重建
        logger.warning(f"[stock_basic_fuyao] 现有文件读取失败，将重建: {exc}")
        return pd.DataFrame()


#: 数仓 dim_stock 的字段 → stock_basic 列名（扶摇快照不带这些元数据）
#: 注意：数仓里名称列叫 `stock_name`，不是 `name`
_DIM_FIELD_MAP = {"stock_name": "name", "list_date": "list_date", "status": "status"}

#: 数仓板块类型 → stock_basic 列名。
#: 扶摇全市场快照不带行业/地域，但本地通达信数仓有完整板块体系
#: （实测：行业 110 个 / 地区 32 个 / 概念 269 个，覆盖率约 99.9%）。
#: 地区的值形如"贵州板块"，去掉"板块"后缀以与原有 tushare 口径（"贵州"）保持一致。
_SECTOR_FILLS = {
    "行业": ("industry", True),
    "地区": ("area", True),
}
_AREA_SUFFIX = "板块"

#: 概念是多归属（一只股票可属多个概念），单独成一列，分号分隔
_CONCEPT_SECTOR_TYPE = "概念"
_CONCEPT_COLUMN = "concepts"
_CONCEPT_SEPARATOR = ";"


def _apply_lookup(
    frame: pd.DataFrame,
    out_col: str,
    lookup: dict,
    treat_empty_as_missing: bool = True,
) -> int:
    """用 {ts_code: value} 填充 frame[out_col] 的空值（不覆盖已有值），返回填充条数。"""
    if not lookup:
        return 0
    if out_col not in frame.columns:
        frame[out_col] = None
    series = frame[out_col]
    if treat_empty_as_missing:
        mask = series.isna() | (series.astype(str).str.strip() == "")
    else:
        mask = series.isna()
    if not mask.any():
        return 0
    frame.loc[mask, out_col] = frame.loc[mask, "ts_code"].map(lookup)
    return int(frame.loc[mask, out_col].notna().sum())


def _load_sector_lookup(con, sector_type: str, strip_suffix: bool = False, multi: bool = False) -> dict:
    """从 bridge_stock_sector JOIN dim_sector 取「股票代码 → 板块名」。

    sector_type 取 行业 / 地区 / 概念 / 风格 / 自定义（dim_sector 的取值）。
    multi=True 时一只股票可属多个板块，返回分号连接的字符串。
    """
    rows = con.execute(
        "SELECT b.stock_code, s.sector_name "
        "FROM bridge_stock_sector b JOIN dim_sector s ON b.sector_code = s.sector_code "
        "WHERE b.sector_type = ?",
        [sector_type],
    ).fetchall()

    if multi:
        grouped: dict = {}
        for code, name in rows:
            if code and name:
                grouped.setdefault(str(code), []).append(str(name))
        return {k: _CONCEPT_SEPARATOR.join(v) for k, v in grouped.items()}

    lookup: dict = {}
    for code, name in rows:
        if not code or not name:
            continue
        text = str(name)
        if strip_suffix:
            text = text.replace(_AREA_SUFFIX, "")
        # 实测仅极个别股票多归属，取第一个保持稳定
        lookup.setdefault(str(code), text)
    return lookup


def _enrich_from_duckdb(frame: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """用本地数仓回填 name / list_date / status / industry / area / concepts。

    扶摇全市场快照只有代码，**没有股票名称，也没有行业/地域**；
    缺了 name 会让选股与 AI 问数显示空值，缺了 industry 会让板块映射
    退化成 3 个板块共 12 只股（见 realtime_monitor_service._initialize_sector_mapping）。
    名称/上市日/状态来自 dim_stock，行业/地区/概念来自 bridge_stock_sector + dim_sector。

    只补空值、不覆盖已有值；数仓不可用时静默返回原表，不影响主流程。
    """
    import os

    db_path = os.getenv(
        "TDX_DB_PATH",
        str(_PROJECT_ROOT.parent / "user" / "数据" / "tdx_data.duckdb"),
    )
    if not os.path.exists(db_path) or "ts_code" not in frame.columns:
        return frame, 0

    try:
        import duckdb

        con = duckdb.connect(db_path, read_only=True)
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"[stock_basic_fuyao] 数仓不可用，跳过名称回填: {exc}")
        return frame, 0

    filled = 0
    try:
        # ① dim_stock：名称 / 上市日 / 状态
        try:
            dim = con.execute(
                "SELECT stock_code, stock_name, list_date, status FROM dim_stock"
            ).df()
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"[stock_basic_fuyao] dim_stock 读取失败: {exc}")
            dim = None

        if dim is not None:
            dim = dim.rename(columns={"stock_code": "ts_code"})
            for dim_col, out_col in _DIM_FIELD_MAP.items():
                if dim_col not in dim.columns:
                    continue
                filled += _apply_lookup(
                    frame,
                    out_col,
                    dim.set_index("ts_code")[dim_col].to_dict(),
                    treat_empty_as_missing=(dim_col == "stock_name"),
                )

        # ② 板块：行业 / 地区（扶摇快照没有，本地数仓有完整体系）
        for sector_type, (out_col, strip_suffix) in _SECTOR_FILLS.items():
            try:
                lookup = _load_sector_lookup(con, sector_type, strip_suffix=strip_suffix)
            except Exception as exc:  # noqa: BLE001
                logger.warning(f"[stock_basic_fuyao] 板块[{sector_type}]读取失败: {exc}")
                continue
            filled += _apply_lookup(frame, out_col, lookup)

        # ③ 概念：多归属，分号连接
        try:
            concepts = _load_sector_lookup(con, _CONCEPT_SECTOR_TYPE, multi=True)
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"[stock_basic_fuyao] 概念读取失败: {exc}")
            concepts = {}
        filled += _apply_lookup(frame, _CONCEPT_COLUMN, concepts)
    finally:
        con.close()

    return frame, filled


def main() -> int:
    """作业入口：用扶摇快照重建 stock_basic（与既有数据合并后再补全字段）。

    快照为空视为失败返回 1；补全计数用于日志观测。
    """
    load_dotenv()
    import os

    data_dir = os.getenv("DATA_DIR")

    rows, _ = FuyaoClient().snapshot_all()
    if not rows:
        print("[stock_basic_fuyao] 快照为空，作业标记失败")
        return 1

    existing = _read_existing(data_dir)
    merged = snapshot_rows_to_stock_basic(rows, existing)
    merged, filled = _enrich_from_duckdb(merged)
    if filled:
        print(f"[stock_basic_fuyao] 由数仓 dim_stock 回填 {filled} 个字段值（name/list_date/status）")
    saved = save_single_parquet(merged, REL_FILENAME, data_dir=data_dir)
    print(
        f"[stock_basic_fuyao] 完成: total={saved} (此前 {len(existing)}, "
        f"新增 {max(saved - len(existing), 0)})"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
