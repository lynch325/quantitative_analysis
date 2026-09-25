"""扶摇全市场日K dump 的缓存管理与三档取数策略。

取数三档（逐级降级，参考 tick-stock-panel 同名实现的策略分层）：
- 近端窗口（≤12 天）：daily-k-10d dump（约 1MB），一次下载覆盖全市场 10 个交易日
- 深窗口：daily-k 10 年全量 dump（约 172MB），缓存覆盖起点即复用、不追新 release
  （旧 release 的中段历史不会变，尾部新鲜度由 10d dump 补）
- 兜底：单标的 historical 接口（仅当未覆盖交易日 ≤5 天时启用，
  5567 只 × 0.12s 节流 ≈ 11 分钟/天，超过 5 天直接报错而不是烧两小时）

dump 缓存目录：``{DATA_DIR}/cache/fuyao/``，文件名 ``{prefix}__{release}.parquet``。
release 号取自预签名 URL 的 releases/<date>/ 路径；小 dump 下载新 release 后
清理旧版；10 年大 dump 只要覆盖请求起点就继续复用。
"""

from __future__ import annotations

import os
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from loguru import logger

from app.utils.data_sources.fuyao_client import (
    FuyaoClient,
    FuyaoError,
    beijing_ms_to_ymd,
)
from app.utils.data_sources.fuyao_normalize import (
    DAILY_COLUMNS,
    daily_frame_from_dump,
    daily_frame_from_kline_rows,
)

RECENT_DUMP_KIND = "daily-k-10d"
FULL_DUMP_KIND = "daily-k"

#: 10 年 dump 单次可覆盖的最大年限（服务端约束）
MAX_HISTORY_YEARS = 10
#: 兜底单标的接口允许的最大未覆盖交易日数
SYMBOL_FALLBACK_MAX_DATES = 5

#: 大 dump 读取列：只取推导 daily 所需，跳过 currency/interval 等常量列
FULL_DUMP_COLUMNS = [
    "thscode", "date_ms", "open_price", "high_price", "low_price",
    "close_price", "volume", "turnover", "adjusted",
]
#: 窗口前置缓冲（自然日）。pre_close 靠「前一交易日」推导，必须把目标日之前的
#: 交易日一并读进来；取 30 天以保证跨春节/国庆等连续休市后仍能取到上一交易日。
PRE_CLOSE_LOOKBACK_DAYS = 30


def load_dump_window(path: Path, start_ms: int, end_ms: int,
                     columns: Optional[List[str]] = None) -> pd.DataFrame:
    """按 date_ms 窗口读大 dump：谓词下推 + 列裁剪，避免整表进内存。

    实测（daily_k：172MB / 1028 万行 / 仅 2 个 row group，按 thscode 排序，
    读 2026-09-04~09-11 窗口）：
    - 整表 ``pd.read_parquet``       峰值 2039MB，1.76s
    - ``pd.read_parquet(filters=)``  峰值 1229MB，0.61s
    - duckdb 谓词下推                峰值  247MB，0.89s

    文件按 thscode 排序、row group 横跨全时段（每个 row group 的 date_ms 统计
    都是全域），所以 pyarrow 的 filters **跳不过任何 row group**，只压掉 40%；
    duckdb 是分块流式扫描，峰值降约 88%，故优先用 duckdb，不可用时退化到
    pyarrow filters（仍比整表读省）。
    """
    cols = ", ".join(columns or FULL_DUMP_COLUMNS)
    try:
        import duckdb

        con = duckdb.connect()
        try:
            # 路径转义后直接内联：read_parquet 是表函数，参数化支持因版本而异
            safe_path = str(path).replace("'", "''")
            return con.execute(
                f"SELECT {cols} FROM read_parquet('{safe_path}') "
                f"WHERE date_ms >= ? AND date_ms <= ?",
                [int(start_ms), int(end_ms)],
            ).df()
        finally:
            con.close()
    except Exception as exc:  # noqa: BLE001 - duckdb 缺失/查询失败都退化
        logger.warning(f"[fuyao] duckdb 窗口读取不可用，退化 pyarrow filters: {exc}")
        return pd.read_parquet(
            path,
            columns=columns or FULL_DUMP_COLUMNS,
            filters=[("date_ms", ">=", int(start_ms)), ("date_ms", "<=", int(end_ms))],
        )


def default_cache_dir() -> Path:
    data_dir = os.getenv(
        "DATA_DIR",
        os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), "data"),
    )
    return Path(data_dir) / "cache" / "fuyao"


def _ymd_to_date(ymd: str) -> date:
    return datetime.strptime(str(ymd), "%Y%m%d").date()


def _date_to_ms_end(ymd: str) -> int:
    """YYYYMMDD → 当天（北京时间）23:59:59.999 的 epoch ms。"""
    dt = datetime.strptime(str(ymd), "%Y%m%d").replace(hour=23, minute=59, second=59, tzinfo=None)
    epoch = datetime(1970, 1, 1)
    return int((dt - epoch).total_seconds() * 1000) - 8 * 3600 * 1000


class DumpStore:
    """dump 文件缓存：按 release 落盘、按需下载、内存 memo。"""

    def __init__(self, client: FuyaoClient, cache_dir: Optional[Path] = None):
        self.client = client
        self.cache_dir = Path(cache_dir) if cache_dir else default_cache_dir()
        # memo 仅限单进程生命周期：当前数据作业均经 subprocess 执行，每次新进程
        # 冷启动自然拿到新 release。若将来把 DumpStore 引入长驻服务，这里必须
        # 改为按 release 失效（或加 TTL），否则会钉死在进程首次加载的版本。
        self._path_memo: Dict[str, Path] = {}
        self._frame_memo: Dict[str, pd.DataFrame] = {}

    def _release_re(self) -> "re.Pattern[str]":
        return re.compile(r"releases/(\d{8})/")

    def ensure_path(self, kind: str, prefix: str, reuse_old_if_covers: Optional[str] = None) -> Path:
        """确保 dump 已落盘并返回路径。

        reuse_old_if_covers: YYYYMMDD。已有旧缓存覆盖该日期时直接复用，
        不追新 release（避免深窗口高频触发时日日重下 172MB）。
        """
        memo = self._path_memo.get(kind)
        if memo is not None and memo.exists():
            return memo

        if reuse_old_if_covers:
            old = self._cached_covering(prefix, reuse_old_if_covers)
            if old is not None:
                self._path_memo[kind] = old
                return old

        info = self.client.dump_download_url(kind)
        release = self._release_re().search(str(info.get("presigned_url") or ""))
        release_tag = release.group(1) if release else "unknown"
        dest = self.cache_dir / f"{prefix}__{release_tag}.parquet"
        if not dest.exists():
            logger.info(f"[fuyao] 下载 dump {kind} (release {release_tag}) -> {dest}")
            self.client.download_dump(kind, dest)
            for old_file in self.cache_dir.glob(f"{prefix}__*.parquet"):
                if old_file.name != dest.name:
                    old_file.unlink(missing_ok=True)
        self._path_memo[kind] = dest
        return dest

    def _cached_covering(self, prefix: str, ymd: str) -> Optional[Path]:
        """返回缓存中 date_ms 覆盖 ymd 的最新文件；坏文件跳过。"""
        for path in sorted(self.cache_dir.glob(f"{prefix}__*.parquet"), reverse=True):
            try:
                dmin, _ = dump_date_range(path)
            except Exception as exc:  # noqa: BLE001 - 缓存损坏不致命
                logger.warning(f"[fuyao] 缓存文件不可读，跳过: {path} ({exc})")
                continue
            if dmin is not None and _ymd_to_date(ymd) >= dmin:
                return path
        return None

    def load_frame(self, kind: str, prefix: str, reuse_old_if_covers: Optional[str] = None) -> pd.DataFrame:
        """小 dump 整读 + 进程内 memo（10d dump/因子表体量小）。"""
        memo = self._frame_memo.get(kind)
        if memo is not None:
            return memo
        path = self.ensure_path(kind, prefix, reuse_old_if_covers=reuse_old_if_covers)
        frame = pd.read_parquet(path)
        self._frame_memo[kind] = frame
        return frame


def dump_date_range(path: Path) -> Tuple[Optional[date], Optional[date]]:
    """读 dump 的 date_ms 边界。

    优先让 duckdb 做 min/max 聚合（1028 万行单列整读约 82MB，聚合只需几 MB；
    该函数在 _cached_covering 里会对每个缓存文件调用，聚合收益明显）；
    duckdb 不可用时退化到只读单列。坏文件仍会抛异常，调用方按原语义跳过。
    """
    try:
        import duckdb

        con = duckdb.connect()
        try:
            safe_path = str(path).replace("'", "''")
            row = con.execute(
                f"SELECT min(date_ms), max(date_ms) FROM read_parquet('{safe_path}')"
            ).fetchone()
        finally:
            con.close()
        dmin_ms, dmax_ms = (row[0], row[1]) if row else (None, None)
    except Exception as exc:  # noqa: BLE001 - duckdb 不可用时退化
        logger.warning(f"[fuyao] duckdb 读 dump 边界失败，退化单列读: {exc}")
        series = pd.read_parquet(path, columns=["date_ms"])["date_ms"]
        if series.empty:
            return None, None
        dmin_ms, dmax_ms = series.min(), series.max()

    if dmin_ms is None or dmax_ms is None:
        return None, None
    dmin = beijing_ms_to_ymd(dmin_ms)
    dmax = beijing_ms_to_ymd(dmax_ms)
    return (
        datetime.strptime(dmin, "%Y%m%d").date() if dmin else None,
        datetime.strptime(dmax, "%Y%m%d").date() if dmax else None,
    )


class FuyaoDailyFetcher:
    """按交易日列表取全市场日K，三档策略自动降级。

    返回 {trade_date: DataFrame(tushare 口径)}；无法覆盖的日期会抛
    FuyaoError（调用方按作业失败处理，缺口由下一轮 gap_fill 回补）。
    """

    def __init__(
        self,
        client: Optional[FuyaoClient] = None,
        store: Optional[DumpStore] = None,
    ):
        self.client = client or FuyaoClient()
        self.store = store or DumpStore(self.client)

    def fetch_dates(self, trade_dates: List[str]) -> Dict[str, pd.DataFrame]:
        """批量取指定交易日的日线，返回 {交易日: DataFrame}。

        **三级回退**：先查近 10 日 dump；未覆盖的转 10 年 dump；仍缺的才逐标的走单标的接口。
        每级都只处理上一级缺的日期，最后按请求顺序过滤返回。
        """
        wanted = sorted({str(d) for d in trade_dates})
        if not wanted:
            return {}

        result = self._fetch_from_recent_dump(wanted)
        missing = [d for d in wanted if d not in result]
        if missing:
            result.update(self._fetch_from_full_dump(missing, result))
        missing = [d for d in wanted if d not in result]
        if missing:
            self._fetch_from_symbol_api(missing, result)
        return {d: result[d] for d in wanted if d in result}

    # ---- 档 1：10d dump ----

    def _fetch_from_recent_dump(self, wanted: List[str]) -> Dict[str, pd.DataFrame]:
        """从近 10 日 dump 取窗口数据。

        跨度超过 12 天、dump 不可用、或窗口超出 dump 覆盖范围时都返回空 dict，
        交给下一级回退 —— 这里不抛错，属正常降级路径。
        """
        span_days = (_ymd_to_date(wanted[-1]) - _ymd_to_date(wanted[0])).days
        if span_days > 12:
            return {}
        try:
            dump = self.store.load_frame(RECENT_DUMP_KIND, "daily_k_10d")
        except FuyaoError as exc:
            logger.warning(f"[fuyao] 10d dump 不可用，尝试 10 年 dump: {exc}")
            return {}
        dmin = beijing_ms_to_ymd(dump["date_ms"].min())
        dmax = beijing_ms_to_ymd(dump["date_ms"].max())
        if dmin is None or dmax is None or wanted[0] < dmin or wanted[-1] > dmax:
            logger.info(f"[fuyao] 10d dump 覆盖 [{dmin}~{dmax}]，不满足窗口 [{wanted[0]}~{wanted[-1]}]")
            return {}
        frame = daily_frame_from_dump(dump, wanted)
        return self._split_by_date(frame)

    # ---- 档 2：10 年全量 dump（+ 10d dump 补尾）----

    def _fetch_from_full_dump(
        self, missing: List[str], already: Dict[str, pd.DataFrame]
    ) -> Dict[str, pd.DataFrame]:
        """从 10 年 dump 补拉缺失交易日。

        窗口早于 dump 覆盖范围（> MAX_HISTORY_YEARS 年）时抛 window_too_long：
        这是调用方的窗口问题，不能靠降级掩盖；dump 本身不可用则只告警降级，由单标的兜底接手。
        """
        start_ymd = missing[0]
        if (_ymd_to_date(missing[-1]) - _ymd_to_date(start_ymd)).days > MAX_HISTORY_YEARS * 365:
            raise FuyaoError(
                "window_too_long",
                f"回补窗口早于扶摇 dump 覆盖范围（>10 年）: {start_ymd}~{missing[-1]}",
            )
        try:
            path = self.store.ensure_path(
                FULL_DUMP_KIND, "daily_k", reuse_old_if_covers=start_ymd
            )
        except FuyaoError as exc:
            logger.warning(f"[fuyao] 10 年 dump 不可用: {exc}")
            return {}

        dmin, dmax = dump_date_range(path)
        result: Dict[str, pd.DataFrame] = {}
        covered = [d for d in missing if dmin and dmax and dmin <= _ymd_to_date(d) <= dmax]
        if covered:
            # 只读窗口而不是整表：窗口 = 目标日区间 + 前置缓冲，
            # 缓冲区用于推导首日 pre_close（见 PRE_CLOSE_LOOKBACK_DAYS）
            window_start_ms = _window_start_ms(
                _backoff_ymd(covered[0], days=PRE_CLOSE_LOOKBACK_DAYS)
            )
            big = load_dump_window(path, window_start_ms, _date_to_ms_end(covered[-1]))
            frame = daily_frame_from_dump(big, covered)
            result = self._split_by_date(frame)

        # 大 dump 末端之后的日期由 10d dump 补尾。10d dump 覆盖最近 10 个交易日，
        # 其窗口起点即含大 dump 尾日，补尾日的 pre_close 在小 dump 内推导即为正确值。
        dmax_ymd = dmax.strftime("%Y%m%d") if dmax else ""
        tail = [d for d in missing if not dmax_ymd or d > dmax_ymd]
        if tail:
            try:
                small = self.store.load_frame(RECENT_DUMP_KIND, "daily_k_10d")
            except FuyaoError as exc:
                logger.warning(f"[fuyao] 补尾失败，10d dump 不可用: {exc}")
                return result
            small_dmin = beijing_ms_to_ymd(small["date_ms"].min())
            small_dmax = beijing_ms_to_ymd(small["date_ms"].max())
            tail_ok = [
                d for d in tail
                if small_dmin and small_dmax and small_dmin <= d <= small_dmax
            ]
            if tail_ok:
                tail_frame = daily_frame_from_dump(small, tail_ok)
                result.update(self._split_by_date(tail_frame))
        return result

    # ---- 档 3：单标的接口兜底 ----

    def _fetch_from_symbol_api(self, missing: List[str], result: Dict[str, pd.DataFrame]) -> None:
        """最后一级兜底：逐标的走单标的接口补缺失交易日。

        **代价极高**（标的数 × 天数 次请求，日志会估算预计分钟数），
        因此缺失交易日数超过 SYMBOL_FALLBACK_MAX_DATES 直接抛错，避免一次作业跑成几小时；
        stock_basic 为空同样抛错（压根没有标的可拉）。
        """
        if len(missing) > SYMBOL_FALLBACK_MAX_DATES:
            raise FuyaoError(
                "dates_not_covered",
                f"{len(missing)} 个交易日超出 dump 覆盖且超过兜底上限 "
                f"{SYMBOL_FALLBACK_MAX_DATES} 天，请检查 dump 缓存或改用显式窗口: {missing[:5]}...",
            )
        symbols = _all_stock_codes()
        if not symbols:
            raise FuyaoError("no_symbols", "stock_basic 为空，无法执行单标的兜底拉取")
        logger.warning(
            f"[fuyao] {len(missing)} 个交易日 dump 未覆盖，走单标的接口兜底"
            f"（{len(symbols)} 只 × {len(missing)} 天，预计约 "
            f"{len(symbols) * len(missing) * 0.12 / 60:.0f} 分钟）"
        )
        start_ms = _window_start_ms(_backoff_ymd(missing[0], days=10))
        end_ms = _date_to_ms_end(missing[-1])
        total = len(symbols)
        for index, ts_code in enumerate(symbols, start=1):
            try:
                rows = self.client.historical_kline(ts_code, start_ms=start_ms, end_ms=end_ms)
            except FuyaoError as exc:
                logger.warning(f"[fuyao] {ts_code} 日K拉取失败，跳过: {exc}")
                continue
            frame = daily_frame_from_kline_rows(rows, ts_code)
            frame = frame[frame["trade_date"].isin(missing)]
            for trade_date, group in frame.groupby("trade_date"):
                existing = result.get(trade_date)
                result[trade_date] = (
                    pd.concat([existing, group], ignore_index=True) if existing is not None else group
                )
            if index % 500 == 0:
                logger.info(f"[fuyao] 单标的兜底进度: {index}/{total}")
        for trade_date in missing:
            frame = result.get(trade_date)
            if frame is not None:
                result[trade_date] = frame[DAILY_COLUMNS].reset_index(drop=True)

    @staticmethod
    def _split_by_date(frame: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        if frame.empty:
            return {}
        return {
            str(trade_date): group.reset_index(drop=True)
            for trade_date, group in frame.groupby("trade_date", sort=True)
        }


def _backoff_ymd(ymd: str, days: int) -> str:
    """回退若干自然日，给单标的接口的窗口留出前一交易日上下文（推导 pre_close）。"""
    return (_ymd_to_date(ymd) - timedelta(days=days)).strftime("%Y%m%d")


def _window_start_ms(ymd: str) -> int:
    """YYYYMMDD → 当天（北京时间）00:00 的 epoch ms。"""
    dt = datetime.strptime(str(ymd), "%Y%m%d")
    epoch = datetime(1970, 1, 1)
    return int((dt - epoch).total_seconds() * 1000) - 8 * 3600 * 1000


def _all_stock_codes() -> List[str]:
    """从本地 stock_basic 读全量代码（含退市股，兜底口径与 tushare 对齐）。"""
    from app.services.data_reader import ParquetDataReader

    df = ParquetDataReader().get_stock_basic()
    if df.empty or "ts_code" not in df.columns:
        return []
    return df["ts_code"].dropna().astype(str).tolist()
