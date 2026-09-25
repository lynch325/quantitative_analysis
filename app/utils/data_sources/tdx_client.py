"""通达信行情适配器：经 tdx-master 的 HTTP 服务取行情。

数据源：tdx-master（`github.com/injoyai/tdx` 的 Go 实现）自带的 HTTP 服务
（`extend/httpserver`），直连通达信行情服务器，**无需任何凭证/费用**。
跨语言边界：Go 服务是独立进程，本模块只走 HTTP，不引入 Python 侧的原生依赖。

服务：`tdx-master/tdx-master/output/tdx-httpserver.exe`（默认监听 :8080）。
地址与可执行文件位置可用 `TDX_SERVER_URL` / `TDX_SERVER_EXE` 覆盖；
`ensure_server()` 可在服务未启动时自动拉起。

实测（2026-09-23，与扶摇快照逐只交叉验证 5/5 一致，含 -21.011% / -34.527% 这类
无涨跌幅限制的新股）：

- `GET /code/stocks` 返回 5578 只，约 3.3s（本模块进程内缓存 12h）
- `GET /quote?codes=...` **单次上限 80 只**（超过返回「预期N个，实际80个」）；
  全市场 70 片，串行 3.1s、并发 4~16 均 2.13s（Go 侧连接池默认 1，并发提升有限），
  实测零失败片
- 单位：价格与昨收 ÷1000 得元；`Kline.Amount` ÷1000 得元；`Kline.Volume` 已是手

⚠️ 两个反直觉点（均已实测确认）：
1. `Kline.Close` 是**现价**、`Kline.Last` 是**昨收** —— 与 K 线里的语义相反，
   源码里的注释“昨收盘好像不太对”其实是虚惊。
2. `Kline.Time` 填的是**服务端当前时间**而非行情时间，`/quote` 也不返回证券名称；
   行情时间须由调用方按交易时段推断（见实时监控的 `_snapshot_data_time`），
   名称须用名称注册表补齐。
"""

from __future__ import annotations

import json
import os
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
from loguru import logger

#: /quote 单次请求的协议上限（实测超过 80 会被服务端截断并报错）
QUOTE_BATCH = 80
#: 分片并发度。Go 侧连接池默认 1，实测 4/8/16 耗时几乎相同，取 4 即够
QUOTE_WORKERS = int(os.getenv("TDX_QUOTE_WORKERS", "4"))
#: 单片失败的重试次数
QUOTE_RETRY = int(os.getenv("TDX_QUOTE_RETRY", "1"))
HTTP_TIMEOUT = float(os.getenv("TDX_HTTP_TIMEOUT", "20"))
#: 服务地址
DEFAULT_BASE_URL = os.getenv("TDX_SERVER_URL", "http://127.0.0.1:8080")
#: 代码列表的进程内缓存时长（日内基本不变）
CODES_TTL_SECONDS = 12 * 3600.0
#: tdx 的 Exchange 编码 → 项目内部代码后缀
_EXCHANGE_SUFFIX = {0: "SZ", 1: "SH", 2: "BJ"}
#: 项目内部代码后缀 → tdx 的代码前缀
_SUFFIX_PREFIX = {"SZ": "sz", "SH": "sh", "BJ": "bj"}


def default_server_exe() -> Path:
    """推导 tdx-master 编译产物的默认位置（项目根同级目录）。"""
    # app/utils/data_sources/tdx_client.py → parents[4] = PYPlugins/
    project_root = Path(__file__).resolve().parents[4]
    return project_root / "tdx-master" / "tdx-master" / "output" / "tdx-httpserver.exe"


class TdxError(RuntimeError):
    """tdx 服务不可用、或返回业务错误码。"""


class TdxClient:
    """tdx-master HTTP 服务客户端（全市场行情快照 + 代码池）。线程安全。"""

    def __init__(self, base_url: Optional[str] = None, timeout: float = HTTP_TIMEOUT):
        self.base_url = (base_url or DEFAULT_BASE_URL).rstrip("/")
        self.timeout = timeout
        self._lock = threading.Lock()
        self._codes_cache: Optional[tuple] = None  # (monotonic, [tdx代码])
        self._exe = Path(os.getenv("TDX_SERVER_EXE") or default_server_exe())

    # ---- 基础 ----

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None, timeout: Optional[float] = None) -> Any:
        """调用本地通达信 HTTP 服务的 GET，把异常统一翻译成 TdxError。

        两层校验：URL / JSON 解析失败 → 请求失败；返回体 code 非 0 或缺失 → 业务错误。
        最终返回信封里的 data 字段（不是整个响应体）。
        """
        url = self.base_url + path
        if params:
            url += "?" + urllib.parse.urlencode(params)
        try:
            with urllib.request.urlopen(url, timeout=timeout or self.timeout) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
        except (urllib.error.URLError, OSError, ValueError) as exc:
            raise TdxError(f"tdx 服务请求失败 {path}: {exc}") from exc
        if not isinstance(payload, dict):
            raise TdxError(f"tdx 服务返回格式异常 {path}")
        if payload.get("code") not in (0, None):
            raise TdxError(f"tdx 服务业务错误 {path}: {payload.get('msg')}")
        return payload.get("data")

    def is_alive(self) -> bool:
        """服务健康检查（不抛异常）。"""
        try:
            self._get("/", timeout=3)
            return True
        except TdxError:
            return False

    def ensure_server(self, wait_seconds: float = 20.0) -> bool:
        """确保服务在跑；未跑则拉起可执行文件并等待就绪。

        已就绪直接返回 True（不重复拉起）。可执行文件缺失或超时返回 False，
        由调用方决定降级——本模块不在降级路径上抛异常。
        """
        if self.is_alive():
            return True
        if not self._exe.is_file():
            logger.warning(f"[tdx] 服务未启动且找不到可执行文件: {self._exe}")
            return False
        try:
            subprocess.Popen(
                [str(self._exe)],
                cwd=str(self._exe.parent),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            logger.info(f"[tdx] 已拉起行情服务: {self._exe}")
        except OSError as exc:
            logger.warning(f"[tdx] 拉起行情服务失败: {exc}")
            return False
        deadline = time.monotonic() + wait_seconds
        while time.monotonic() < deadline:
            if self.is_alive():
                logger.info("[tdx] 行情服务就绪")
                return True
            time.sleep(0.5)
        logger.warning(f"[tdx] 行情服务 {wait_seconds:.0f}s 内未就绪")
        return False

    # ---- 代码池 ----

    def stock_codes(self, force: bool = False) -> List[str]:
        """全市场股票代码（tdx 形式，如 `sh600000`），进程内缓存 12h。"""
        with self._lock:
            cached = self._codes_cache
            if not force and cached and time.monotonic() - cached[0] < CODES_TTL_SECONDS:
                return cached[1]
        codes = [str(c) for c in (self._get("/code/stocks", timeout=120) or []) if c]
        if not codes:
            raise TdxError("tdx 代码列表为空")
        with self._lock:
            self._codes_cache = (time.monotonic(), codes)
        return codes

    # ---- 行情 ----

    @staticmethod
    def to_project_code(tdx_code: str) -> str:
        """`sh600000` → `600000.SH`（与项目内部代码口径一致）。"""
        text = str(tdx_code).strip()
        if len(text) < 7:
            return text
        prefix, body = text[:2].lower(), text[2:]
        suffix = {"sh": "SH", "sz": "SZ", "bj": "BJ"}.get(prefix)
        return f"{body}.{suffix}" if suffix else text

    @staticmethod
    def to_tdx_code(project_code: str) -> str:
        """`600000.SH` → `sh600000`；已是 tdx 形式则原样返回。"""
        text = str(project_code).strip()
        if "." in text:
            body, _, suffix = text.partition(".")
            prefix = _SUFFIX_PREFIX.get(suffix.upper())
            if prefix:
                return f"{prefix}{body}"
        return text

    def _quote_batch(self, codes: List[str]) -> List[Dict[str, Any]]:
        """取一片行情，失败按 QUOTE_RETRY 重试。"""
        last: Optional[Exception] = None
        for attempt in range(QUOTE_RETRY + 1):
            try:
                return self._get("/quote", {"codes": ",".join(codes)}, timeout=60) or []
            except TdxError as exc:
                last = exc
                if attempt < QUOTE_RETRY:
                    time.sleep(0.3)
        raise TdxError(f"取行情失败({len(codes)}只): {last}")

    def quotes_raw(self, codes: Optional[Iterable[str]] = None) -> List[Dict[str, Any]]:
        """全市场（或指定代码）原始行情明细，自动按 80 只分片并发拉取。"""
        pool = [self.to_tdx_code(c) for c in codes] if codes else self.stock_codes()
        batches = [pool[i:i + QUOTE_BATCH] for i in range(0, len(pool), QUOTE_BATCH)]
        if not batches:
            return []
        rows: List[Dict[str, Any]] = []
        failures: List[str] = []
        with ThreadPoolExecutor(max_workers=max(1, QUOTE_WORKERS)) as pool_exec:
            for batch, result in zip(batches, pool_exec.map(self._safe_batch, batches)):
                if result is None:
                    failures.append(f"{batch[0]}..{batch[-1]}")
                else:
                    rows.extend(result)
        if failures and len(failures) == len(batches):
            raise TdxError(f"tdx 全部 {len(batches)} 片行情均失败")
        if failures:
            logger.warning(f"[tdx] {len(failures)}/{len(batches)} 片失败，已跳过: {failures[:3]}")
        return rows

    def _safe_batch(self, batch: List[str]) -> Optional[List[Dict[str, Any]]]:
        try:
            return self._quote_batch(batch)
        except TdxError as exc:
            logger.warning(f"[tdx] 分片失败: {exc}")
            return None

    @staticmethod
    def normalize(rows: Iterable[Dict[str, Any]]) -> pd.DataFrame:
        """原始明细 → 与扶摇快照**同 schema** 的 DataFrame。

        列与口径刻意对齐 `fuyao_normalize.snapshot_rows_to_quote_frame`，
        这样实时监控的 `_snapshot_frame` 无需为换源改动取列逻辑。

        ⚠️ 字段语义（反直觉，见模块 docstring）：`Kline.Close`=现价、
        `Kline.Last`=昨收；价格与金额需 ÷1000 得元。
        `Volume` 原本是手，这里 ×100 转成股，与扶摇的 `volume` 口径一致
        （下游 `_snapshot_frame` 会再 ÷100 还原成手）。
        """
        records: List[Dict[str, Any]] = []
        for row in rows or []:
            kline = row.get("Kline") or {}
            try:
                last_price = float(kline.get("Close")) / 1000.0
                prev_close = float(kline.get("Last")) / 1000.0
            except (TypeError, ValueError):
                continue
            suffix = _EXCHANGE_SUFFIX.get(row.get("Exchange"))
            code = str(row.get("Code") or "")
            if not suffix or not code:
                continue
            if prev_close > 0:
                change = last_price - prev_close
                pct_chg = change / prev_close * 100.0
            else:
                change = None
                pct_chg = None
            try:
                volume_hands = float(kline.get("Volume") or 0.0)
            except (TypeError, ValueError):
                volume_hands = 0.0
            try:
                amount_yuan = float(kline.get("Amount") or 0.0) / 1000.0
            except (TypeError, ValueError):
                amount_yuan = 0.0
            records.append({
                "ts_code": f"{code}.{suffix}",
                "name": None,  # /quote 不返回名称，由 merge_names 补齐
                "last_price": last_price,
                "open": TdxClient._price(kline.get("Open")),
                "high": TdxClient._price(kline.get("High")),
                "low": TdxClient._price(kline.get("Low")),
                "prev_close": prev_close,
                "change": change,
                "pct_chg": pct_chg,
                "volume": volume_hands * 100.0,
                "turnover": amount_yuan,
            })
        return pd.DataFrame(records)

    @staticmethod
    def _price(value: Any) -> Optional[float]:
        try:
            return float(value) / 1000.0
        except (TypeError, ValueError):
            return None

    def quote_frame(self, codes: Optional[Iterable[str]] = None,
                    with_names: bool = True) -> pd.DataFrame:
        """全市场（或指定池）行情帧，schema 对齐扶摇快照。

        名称用名称注册表补齐（`/quote` 不返回名称）；补齐失败不影响行情主数据。
        """
        frame = self.normalize(self.quotes_raw(codes))
        if frame.empty:
            return frame
        if with_names:
            try:
                from app.services.stock_name_registry import get_stock_name_registry

                merged = get_stock_name_registry().merge_names(frame.to_dict("records"))
                frame = pd.DataFrame(merged)
            except Exception as exc:  # noqa: BLE001 - 名称缺失不阻断行情
                logger.debug(f"[tdx] 名称补齐跳过: {exc}")
        return frame


_client: Optional[TdxClient] = None
_client_lock = threading.Lock()


def get_tdx_client() -> TdxClient:
    """进程内单例（线程安全）。"""
    global _client
    with _client_lock:
        if _client is None:
            _client = TdxClient()
        return _client
