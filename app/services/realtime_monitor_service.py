"""
实时监控服务
提供实时行情监控、热点板块监控、异动股票监控和市场情绪监控功能
"""

import pandas as pd
import numpy as np
from datetime import datetime, time as dt_time, timedelta
from typing import Any, Dict, List, Optional, Tuple
import logging
import threading
import time

from app.services.data_reader import ParquetDataReader

logger = logging.getLogger(__name__)


#: 上午收盘 / 下午开盘 / 全天收盘时刻
_MORNING_CLOSE = dt_time(11, 30)
_AFTERNOON_OPEN = dt_time(13, 0)
_CLOSE_TIME = dt_time(15, 0)
#: A 股连续竞价时段；快照落在这些时段内才算「实时跳动」的数据
_REALTIME_SESSIONS = ((dt_time(9, 30), _MORNING_CLOSE), (_AFTERNOON_OPEN, _CLOSE_TIME))


class RealtimeMonitorService:
    """实时监控服务"""
    DEFAULT_PERIOD_TYPE = "5min"
    #: 数据源标识。快照路径有两条来源，列口径完全一致（见各自 data_sources 适配器），
    #: 因此共用同一套取数/统计逻辑，只有降级顺序与对外展示的 source 值不同：
    #: tdx（通达信协议，免费无凭证，全市场约 2.1s）优先，扶摇（需 API key）兜底。
    TDX_SNAPSHOT_SOURCE = "tdx_snapshot"
    FUYAO_SNAPSHOT_SOURCE = "fuyao_snapshot"
    #: 兼容既有引用（前端/日志里出现过该字面量）
    SNAPSHOT_SOURCE = FUYAO_SNAPSHOT_SOURCE
    MINUTE_SOURCE = "local_minute"
    #: 全市场行情帧的 TTL 缓存秒数。单次 API 请求会多次调用 _snapshot_frame
    #: （活跃股池 + 监控帧），不缓存会把 2.1s 的上游耗时成倍放大。
    SNAPSHOT_TTL_SECONDS = 5.0

    @classmethod
    def _is_snapshot_source(cls, source: Optional[str]) -> bool:
        """是否为快照路径（tdx 或 扶摇）。两者列口径一致，逻辑共用。"""
        return source in (cls.TDX_SNAPSHOT_SOURCE, cls.FUYAO_SNAPSHOT_SOURCE)

    def __init__(self):
        self.data_reader = ParquetDataReader()
        self.minute_reader = self.data_reader.get_minute_reader()
        # 板块映射依赖 data_reader 的 stock_basic，必须在它初始化之后构建
        self.sector_mapping = self._initialize_sector_mapping()
        # (写入时刻, 全市场帧, 数据源标识)
        self._snapshot_lock = threading.Lock()
        self._snapshot_cache: Optional[Tuple[float, pd.DataFrame, str]] = None
    
    def _initialize_sector_mapping(self):
        """初始化板块映射：优先用 stock_basic 的真实行业字段。

        旧实现是 28 个手工挑选的 4 股列表，存在大量错分
        （如五粮液在"医药"、海康威司重复在"电子"和"通信"），
        计算出的"板块表现"不可信。
        """
        fallback = {
            '银行': ['000001.SZ', '600000.SH', '600036.SH', '601988.SH'],
            '食品饮料': ['000568.SZ', '600519.SH', '000596.SZ', '600887.SH'],
            '电子': ['000725.SZ', '002415.SH', '600584.SH', '000021.SZ'],
        }
        try:
            stock_basic = self.data_reader.get_stock_basic()
            if stock_basic.empty or "industry" not in stock_basic.columns:
                logger.warning("stock_basic 缺少行业字段，板块映射退化为默认列表")
                return fallback
            df = stock_basic.dropna(subset=["industry", "ts_code"])
            mapping = {}
            for industry, group in df.groupby("industry"):
                mapping[str(industry)] = group["ts_code"].astype(str).tolist()
            if not mapping:
                return fallback
            return mapping
        except Exception as e:
            logger.error(f"构建板块映射失败，退化为默认列表: {e}")
            return fallback

    def _minute_frame(
        self,
        period_type: str = DEFAULT_PERIOD_TYPE,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        ts_codes: Optional[List[str]] = None,
        fallback_latest: bool = False,
    ) -> pd.DataFrame:
        """Load minute rows from parquet and optionally filter by codes.

        fallback_latest=True：窗口内取不到数据时（非交易时段、或分钟线还没
        同步到今天），自动回落到「最新可用分区当日」，避免整个实时模块返回空。
        """
        df = self.minute_reader.get_data(
            period_type=period_type,
            start_time=start_time,
            end_time=end_time,
        )
        if df.empty and fallback_latest:
            df = self._fallback_latest_frame(period_type)
        if df.empty:
            return df
        if ts_codes:
            df = df[df["ts_code"].isin(set(ts_codes))]
        return df

    def _fallback_latest_frame(self, period_type: str) -> pd.DataFrame:
        """回落读取：最新可用分区「当日」的全部分钟数据。"""
        try:
            latest_date = self.minute_reader.get_latest_partition_date(period_type)
        except Exception as e:  # noqa: BLE001 - 回落失败不应打断主流程
            logger.error(f"探测最新分钟分区失败: {e}")
            return pd.DataFrame()
        if latest_date is None:
            return pd.DataFrame()
        logger.warning(
            f"实时窗口内无分钟数据，回落到最新分区 {latest_date:%Y-%m-%d}（{period_type}）"
        )
        # 右端取当日 23:59:59，确保只命中当天分区（reader 按 YYYY-MM-DD 剪枝）
        return self.minute_reader.get_data(
            period_type=period_type,
            start_time=latest_date,
            end_time=latest_date + timedelta(hours=23, minutes=59, seconds=59),
        )

    @staticmethod
    def _latest_rows(df: pd.DataFrame) -> pd.DataFrame:
        """按 ts_code 取每个标的的最新一行（实时监控只关心最新状态）。

        **必须先按 datetime 排序再 groupby().tail(1)**：顺序反了会取到最早那条。
        缺 datetime / ts_code 列时返回空表而不是报错。
        """
        if df.empty or "datetime" not in df.columns or "ts_code" not in df.columns:
            return pd.DataFrame()
        return (
            df.sort_values("datetime")
            .groupby("ts_code", as_index=False, sort=False)
            .tail(1)
            .reset_index(drop=True)
        )

    @staticmethod
    def _frame_quote_time(frame: pd.DataFrame) -> datetime:
        """取监控帧的**行情时间**：快照=服务端时间戳，分钟线=最新 bar 时间。

        接口的 update_time 必须用它，而不是 datetime.now() —— 否则收盘后
        调用会把 15:00 的数据标成"此刻更新"。
        """
        if frame is not None and not frame.empty and "datetime" in frame.columns:
            latest = pd.to_datetime(frame["datetime"], errors="coerce").max()
            if not pd.isna(latest):
                return latest.to_pydatetime()
        return datetime.now()

    @staticmethod
    def _display_name(raw: Any, ts_code: str) -> str:
        """证券名称缺失时退回代码。

        不能用 `raw or ts_code` 兜底：NaN 是**真值**，`nan or code` 会返回 nan，
        序列化成 JSON 就是 null，前端显示 None。另外 `_snapshot_frame` 用
        `astype(str)` 统一列类型，会把缺失值转成 "None"/"nan" 字面量，一并识别。
        """
        if raw is None:
            return ts_code
        try:
            if pd.isna(raw):
                return ts_code
        except (TypeError, ValueError):  # 非标量交给 str() 处理
            pass
        text = str(raw).strip()
        if not text or text.lower() in ("none", "nan", "nat", "null"):
            return ts_code
        return text

    # ---- 全市场快照（首选数据源）----

    def _snapshot_data_time(self, now: Optional[datetime] = None) -> datetime:
        """推断快照这份数据代表哪一刻（接口不提供行情时间）。

        扶摇 prices/snapshot 的 timestamp 是**响应生成时刻**（实测与请求时刻同秒），
        item 的 11 个字段里也没有任何行情时间字段，所以不能直接拿来当数据时间。
        改为按「最近交易日 + 连续竞价时段」推断：
        - 最近交易日就是今天、且当前处于竞价时段 → now（数据在实时跳动）
        - 其余情况（盘前/盘后/非交易日）→ 最近交易日的 15:00 收盘

        交易日历走 BoardMarketService.latest_trade_date()（自带 6 小时缓存与自然日
        回退），不在这里重复实现一份日历缓存。日历不可用时退回 now()，
        宁可显示得乐观也不阻断行情主流程。
        """
        moment = now or datetime.now()
        try:
            from app.services.board_market_service import get_board_market_service

            last_trade_date = get_board_market_service().latest_trade_date()
        except Exception as e:  # noqa: BLE001 - 日历不可用不影响行情本身
            logger.warning(f"最近交易日获取失败，快照时间退回当前时刻: {e}")
            return moment

        if not last_trade_date:
            return moment
        try:
            day = datetime.strptime(str(last_trade_date), '%Y%m%d').date()
        except ValueError:
            logger.warning(f"最近交易日格式异常: {last_trade_date!r}，快照时间退回当前时刻")
            return moment

        clock = moment.time()
        if day == moment.date():
            if any(start <= clock <= end for start, end in _REALTIME_SESSIONS):
                return moment  # 竞价时段内：数据在实时跳动
            if _MORNING_CLOSE < clock < _AFTERNOON_OPEN:
                return datetime.combine(day, _MORNING_CLOSE)  # 午休：停在上午收盘
        # 数据不可能来自未来：盘前时段最近交易日仍是今天，但当天行情还没出，
        # 这里由 min() 退回当前时刻，而不是给出「今天 15:00」这种未来时间
        return min(datetime.combine(day, _CLOSE_TIME), moment)

    def _raw_snapshot_frame(self) -> Optional[pd.DataFrame]:
        """全市场行情帧（带 TTL 缓存）；None 表示两条快照来源都不可用。"""
        now = time.monotonic()
        with self._snapshot_lock:
            cached = self._snapshot_cache
        if cached and now - cached[0] < self.SNAPSHOT_TTL_SECONDS:
            return cached[1]

        frame = self._tdx_snapshot_frame()
        source = self.TDX_SNAPSHOT_SOURCE
        if frame is None or frame.empty:
            frame = self._fuyao_snapshot_frame()
            source = self.FUYAO_SNAPSHOT_SOURCE
        if frame is None or frame.empty:
            return None
        with self._snapshot_lock:
            self._snapshot_cache = (now, frame, source)
        return frame

    def _snapshot_source(self) -> str:
        """最近一次快照命中的数据源标识（缓存为空时按优先序推定）。"""
        with self._snapshot_lock:
            cached = self._snapshot_cache
        return cached[2] if cached else self.TDX_SNAPSHOT_SOURCE

    def _tdx_snapshot_frame(self) -> Optional[pd.DataFrame]:
        """tdx（通达信协议）全市场行情帧；不可用返回 None 让调用方降级。"""
        try:
            from app.utils.data_sources.tdx_client import get_tdx_client

            client = get_tdx_client()
            if not client.is_alive() and not client.ensure_server():
                return None
            return client.quote_frame()
        except Exception as e:  # noqa: BLE001 - 换源失败必须静默降级到扶摇
            logger.warning(f"[monitor] tdx 快照不可用，降级扶摇: {e}")
            return None

    def _fuyao_snapshot_frame(self) -> Optional[pd.DataFrame]:
        """扶摇全市场快照帧（兜底来源，需要 API key）。"""
        try:
            from app.services.market_snapshot_service import get_market_snapshot_service

            return get_market_snapshot_service().get_quote_frame()
        except Exception as e:  # noqa: BLE001 - 快照不可用必须静默回退
            logger.warning(f"[monitor] 扶摇快照不可用: {e}")
            return None

    def _snapshot_frame(self, ts_codes: Optional[List[str]] = None) -> pd.DataFrame:
        """取全市场行情快照并归一化为监控内部契约（每只一行）。

        来源优先级：tdx（通达信协议，免费无凭证，实测全市场约 2.1s）→ 扶摇
        （一次请求覆盖全市场，需 API key）。两条来源的列口径一致（见各自
        data_sources 适配器），所以下方取列逻辑共用；命中哪条见 `_snapshot_source`。

        单位换算：快照 volume 是「股」→ ÷100 换成与分钟线一致的「手」；
        turnover 本身是「元」，与分钟线 amount 一致。

        datetime 列填**推断出的数据时刻**（非请求时刻），下游据此算
        update_time / data_delay，见 _snapshot_data_time。
        """
        raw = self._raw_snapshot_frame()

        if raw is None or raw.empty or "ts_code" not in raw.columns:
            return pd.DataFrame()

        df = raw.copy()
        df["ts_code"] = df["ts_code"].astype(str)
        if ts_codes:
            df = df[df["ts_code"].isin(set(ts_codes))]
        if df.empty:
            return df

        def _num(col: str) -> pd.Series:
            return (
                pd.to_numeric(df[col], errors="coerce")
                if col in df.columns
                else pd.Series(np.nan, index=df.index)
            )

        out = pd.DataFrame(
            {
                "ts_code": df["ts_code"],
                "name": df["name"].astype(str) if "name" in df.columns else df["ts_code"],
                "datetime": self._snapshot_data_time(),
                "close": _num("last_price"),
                "open": _num("open"),
                "high": _num("high"),
                "low": _num("low"),
                "volume": _num("volume") / 100.0,   # 股 → 手
                "amount": _num("turnover"),         # 元
                "prev_close": _num("prev_close"),
                "change_pct": _num("pct_chg"),
            }
        )
        # 剔除价格无效（NaN / 0）的行：停牌、退市、配股缴款（072913.SZ 这类非交易
        # 代码）等无行情标的若混进来，会在涨跌幅榜上伪造成 -100%
        return out[out["close"].notna() & (out["close"] > 0)]

    def _monitor_frame(
        self,
        ts_codes: Optional[List[str]] = None,
        period_type: str = DEFAULT_PERIOD_TYPE,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> Tuple[pd.DataFrame, str]:
        """监控数据统一入口：优先全市场快照，不可用时回退分钟线。

        返回 (frame, source)。快照已是「每只一行」的最新值，无需再聚合。
        """
        snap = self._snapshot_frame(ts_codes)
        if not snap.empty:
            return snap, self._snapshot_source()
        df = self._minute_frame(
            period_type=period_type,
            start_time=start_time,
            end_time=end_time,
            ts_codes=ts_codes,
            fallback_latest=True,
        )
        return self._latest_rows(df), self.MINUTE_SOURCE

    def _enrich_quotes(
        self,
        frame: pd.DataFrame,
        source: str,
        period_type: str,
        volume_avg_map: Optional[Dict[str, float]] = None,
        prev_close_map: Optional[Dict[str, float]] = None,
        turnover_map: Optional[Dict[str, float]] = None,
    ) -> List[Dict]:
        """把监控帧逐行富化成对外 quote 契约（名称/量比/换手率/行情时间）。

        实时行情与全市场排行共用：快照路径的口径变更（换源、量比、行情时间）
        只需改这一处。快照自带 change_pct，不做二次计算。
        """
        volume_avg_map = volume_avg_map or {}
        turnover_map = turnover_map or {}
        quotes: List[Dict] = []
        for _, row in frame.iterrows():
            ts_code = row["ts_code"]
            try:
                current_time = pd.to_datetime(row["datetime"]).to_pydatetime()
                if self._is_snapshot_source(source):
                    raw_pct = row.get("change_pct")
                    change_pct = 0.0 if pd.isna(raw_pct) else float(raw_pct)
                    name = self._display_name(row.get("name"), ts_code)
                    volume_ratio = self._snapshot_volume_ratio(
                        row.get("volume"), volume_avg_map.get(ts_code)
                    )
                else:
                    prev_close = self._get_previous_close(
                        ts_code, current_time, period_type, prev_close_map
                    )
                    change_pct = 0.0
                    if prev_close and prev_close > 0:
                        change_pct = (row["close"] - prev_close) / prev_close * 100
                    name = self._get_stock_name(ts_code)
                    volume_ratio = self._calculate_volume_ratio(
                        ts_code, current_time, period_type
                    )

                quotes.append({
                    'ts_code': ts_code,
                    'name': name,
                    'current_price': row["close"],
                    'open_price': row["open"],
                    'high_price': row["high"],
                    'low_price': row["low"],
                    'volume': row["volume"],
                    'amount': row["amount"],
                    'change_pct': change_pct,
                    'volume_ratio': volume_ratio,
                    'update_time': current_time.isoformat(),
                    'turnover_rate': self._calculate_turnover_rate(
                        ts_code, row["volume"], turnover_map
                    ),
                })
            except Exception as e:
                logger.error(f"富化 {ts_code} 行情数据失败: {str(e)}")
                continue
        return quotes

    def get_realtime_quotes(self, stock_codes: List[str] = None, 
                           period_type: str = DEFAULT_PERIOD_TYPE, limit: int = 50) -> Dict:
        """获取实时行情数据"""
        try:
            # 如果没有指定股票代码，获取活跃股票
            if not stock_codes:
                stock_codes = self._get_active_stocks(limit)

            end_time = datetime.now()
            start_time = end_time - timedelta(hours=1)
            frame, source = self._monitor_frame(
                ts_codes=stock_codes, period_type=period_type,
                start_time=start_time, end_time=end_time,
            )

            # 量比与换手率都必须批量预取：`_calculate_turnover_rate` 未命中映射时
            # 会**逐只**读日线（100 只 = 100 次 parquet 访问，接口会卡到十秒级）；
            # 而这两张表按**日期**分区，预取全市场与预取候选集的 IO 成本几乎相同。
            prev_close_map: Dict[str, float] = {}
            turnover_map: Dict[str, float] = {}
            volume_avg_map: Dict[str, float] = {}
            current_date = end_time.strftime('%Y%m%d')
            picked_codes = frame["ts_code"].astype(str).tolist() if not frame.empty else list(stock_codes or [])
            if self._is_snapshot_source(source):
                volume_avg_map = self._daily_volume_avg_map(picked_codes, current_date)
                turnover_map = self._daily_turnover_map(picked_codes, current_date)
            else:
                prev_close_map = self._daily_prev_close_map(stock_codes, current_date)
                turnover_map = self._daily_turnover_map(stock_codes, current_date)

            quotes = self._enrich_quotes(
                frame, source, period_type,
                volume_avg_map=volume_avg_map,
                prev_close_map=prev_close_map,
                turnover_map=turnover_map,
            )

            return {
                'success': True,
                'data': {
                    'quotes': quotes,
                    'total_count': len(quotes),
                    'source': source,
                    'update_time': datetime.now().isoformat()
                },
                'message': f'成功获取 {len(quotes)} 只股票的实时行情'
            }
            
        except Exception as e:
            logger.error(f"获取实时行情失败: {str(e)}")
            return {'success': False, 'message': str(e)}

    def get_top_movers(self, limit: int = 20,
                       period_type: str = DEFAULT_PERIOD_TYPE) -> Dict:
        """全市场涨跌幅 / 成交额排行（每类各 limit 条）。

        候选池是**全市场**快照帧。旧实现取数走 `get_realtime_quotes(limit=100)`，
        而那个 limit 的语义是「按成交额取最活跃 N 只」（见 `_get_active_stocks`），
        于是榜单只在活跃股内部排名 —— 实测全市场跌幅前 15 名里有 12 只
        （成交额不在前列）从未出现。

        富化只针对最终入选的 3×limit 只：全市场逐只读日线是 5000+ 次 parquet
        访问；而这些表按日期分区，批量预取入选集即可。
        """
        try:
            frame, source = self._monitor_frame(
                period_type=period_type,
                start_time=datetime.now() - timedelta(hours=1),
                end_time=datetime.now(),
            )
            if frame.empty:
                return {
                    'success': True,
                    'data': {'top_gainers': [], 'top_losers': [], 'most_active': [],
                             'source': source, 'total_stocks': 0, 'update_time': None},
                    'message': '当前无行情数据',
                }

            rows = frame.copy()
            rows['_pct'] = pd.to_numeric(rows['change_pct'], errors='coerce')
            rows['_amt'] = pd.to_numeric(rows['amount'], errors='coerce').fillna(0.0)

            ranked = rows.dropna(subset=['_pct'])  # 排序必须排除无涨跌幅的行
            gainers = ranked.nlargest(limit, '_pct')
            losers = ranked.nsmallest(limit, '_pct')
            active = rows.nlargest(limit, '_amt')

            picked = pd.concat([gainers, losers, active]).drop_duplicates(subset=['ts_code'])
            picked_codes = picked['ts_code'].astype(str).tolist()
            current_date = datetime.now().strftime('%Y%m%d')
            quotes = self._enrich_quotes(
                picked, source, period_type,
                volume_avg_map=(self._daily_volume_avg_map(picked_codes, current_date)
                                if self._is_snapshot_source(source) else None),
                turnover_map=self._daily_turnover_map(picked_codes, current_date),
            )
            by_code = {q['ts_code']: q for q in quotes}

            def _collect(group: pd.DataFrame) -> List[Dict]:
                # nlargest/nsmallest 已排好序，这里按原顺序挑回富化结果
                return [by_code[c] for c in group['ts_code'].astype(str) if c in by_code]

            return {
                'success': True,
                'data': {
                    'top_gainers': _collect(gainers),
                    'top_losers': _collect(losers),
                    'most_active': _collect(active),
                    'source': source,
                    'total_stocks': int(len(rows)),
                    'update_time': self._frame_quote_time(frame).isoformat(),
                },
                'message': f'全市场 {len(rows)} 只中取涨跌幅/成交额各 {limit} 名',
            }
        except Exception as e:
            logger.error(f"获取涨跌幅排行失败: {str(e)}")
            return {'success': False, 'message': str(e)}

    def get_sector_performance(self, period_hours: int = 1) -> Dict:
        """获取板块表现"""
        try:
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=period_hours)
            latest_rows, source = self._monitor_frame(
                period_type=self.DEFAULT_PERIOD_TYPE,
                start_time=start_time, end_time=end_time,
            )

            if latest_rows.empty:
                return {
                    'success': True,
                    'data': {
                        'sectors': [],
                        'total_sectors': 0,
                        'period_hours': period_hours,
                        'update_time': datetime.now().isoformat()
                    },
                    'message': '当前时段无分钟数据'
                }

            sector_performance = []

            # 批量预取昨收，避免逐股逐板块重复读全量数据
            sector_codes = list(set(latest_rows["ts_code"].astype(str)))
            prev_close_map = self._daily_prev_close_map(sector_codes, end_time.strftime('%Y%m%d'))

            for sector_name, stock_codes in self.sector_mapping.items():
                try:
                    sector_rows = latest_rows[latest_rows["ts_code"].isin(stock_codes)].copy()
                    if sector_rows.empty:
                        continue

                    sector_changes = []
                    sector_volumes = []
                    sector_amounts = []

                    for _, row in sector_rows.iterrows():
                        if self._is_snapshot_source(source):
                            raw_pct = row.get("change_pct")
                            if pd.isna(raw_pct):
                                continue
                            change_pct = float(raw_pct)
                        else:
                            current_time = pd.to_datetime(row["datetime"]).to_pydatetime()
                            prev_close = self._get_previous_close(
                                row["ts_code"], current_time, self.DEFAULT_PERIOD_TYPE, prev_close_map
                            )
                            if not prev_close or prev_close <= 0:
                                continue
                            change_pct = (row["close"] - prev_close) / prev_close * 100
                        sector_changes.append(change_pct)
                        sector_volumes.append(row["volume"])
                        sector_amounts.append(row["amount"])
                    
                    if sector_changes:
                        # 计算板块平均涨跌幅（等权重）
                        avg_change = np.mean(sector_changes)
                        total_volume = sum(sector_volumes)
                        total_amount = sum(sector_amounts)
                        
                        # 计算上涨股票数量
                        rising_count = sum(1 for change in sector_changes if change > 0)
                        falling_count = sum(1 for change in sector_changes if change < 0)
                        
                        sector_performance.append({
                            'sector_name': sector_name,
                            'avg_change_pct': avg_change,
                            'total_volume': total_volume,
                            'total_amount': total_amount,
                            'stock_count': len(sector_changes),
                            'rising_count': rising_count,
                            'falling_count': falling_count,
                            'rising_ratio': rising_count / len(sector_changes) * 100 if sector_changes else 0
                        })
                        
                except Exception as e:
                    logger.error(f"计算板块 {sector_name} 表现失败: {str(e)}")
                    continue
            
            # 按涨跌幅排序
            sector_performance.sort(key=lambda x: x['avg_change_pct'], reverse=True)
            
            return {
                'success': True,
                'data': {
                    'sectors': sector_performance,
                    'total_sectors': len(sector_performance),
                    'period_hours': period_hours,
                    'update_time': self._frame_quote_time(latest_rows).isoformat()
                },
                'message': f'成功获取 {len(sector_performance)} 个板块的表现数据'
            }
            
        except Exception as e:
            logger.error(f"获取板块表现失败: {str(e)}")
            return {'success': False, 'message': str(e)}
    
    def detect_anomalies(self, change_threshold: float = 5.0, 
                        volume_threshold: float = 3.0, 
                        period_hours: int = 1) -> Dict:
        """检测异动股票"""
        try:
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=period_hours)
            # 全市场扫描：不再受「活跃股上限」约束，否则会漏掉异动个股
            latest_rows, source = self._monitor_frame(
                period_type=self.DEFAULT_PERIOD_TYPE,
                start_time=start_time, end_time=end_time,
            )

            # 量比基准一次性批量预取：get_daily 按日期分区读取，
            # 预取全市场与预取候选集的 IO 成本几乎相同，不必留到循环里逐只查
            volume_avg_map: Dict[str, float] = {}
            if self._is_snapshot_source(source) and not latest_rows.empty:
                volume_avg_map = self._daily_volume_avg_map(
                    latest_rows["ts_code"].astype(str).tolist(),
                    end_time.strftime('%Y%m%d'),
                )

            anomalies = []

            for _, row in latest_rows.iterrows():
                ts_code = row["ts_code"]
                try:
                    if self._is_snapshot_source(source):
                        raw_pct = row.get("change_pct")
                        if pd.isna(raw_pct):
                            continue
                        change_pct = float(raw_pct)
                        # 快照无历史均量，"量比"需逐只查本地分钟线（全市场查询极慢）：
                        # 先按涨跌幅粗筛，只对候选计算量比
                        if abs(change_pct) < change_threshold:
                            continue
                        current_time = end_time
                        name = row.get("name") or ts_code
                    else:
                        current_time = pd.to_datetime(row["datetime"]).to_pydatetime()
                        prev_close = self._get_previous_close(ts_code, current_time, self.DEFAULT_PERIOD_TYPE)
                        if not prev_close or prev_close <= 0:
                            continue
                        change_pct = (row["close"] - prev_close) / prev_close * 100
                        name = self._get_stock_name(ts_code)

                    if self._is_snapshot_source(source):
                        # 快照自带当日成交量，配批量预取的 N 日均量算真量比
                        volume_ratio = self._snapshot_volume_ratio(
                            row.get("volume"), volume_avg_map.get(ts_code)
                        )
                    else:
                        volume_ratio = self._calculate_volume_ratio(
                            ts_code, current_time, self.DEFAULT_PERIOD_TYPE
                        )

                    anomaly_types = []
                    if abs(change_pct) >= change_threshold:
                        anomaly_types.append('急涨' if change_pct > 0 else '急跌')
                    # 量比无基准时为 None，None 参与比较会抛 TypeError
                    if volume_ratio is not None and volume_ratio >= volume_threshold:
                        anomaly_types.append('放量')
                    # 「突破」需要历史高低点：快照只有当日，仅分钟线路径可判
                    if not self._is_snapshot_source(source) and self._check_price_breakout(ts_code, row):
                        anomaly_types.append('突破')

                    if anomaly_types:
                        anomalies.append({
                            'ts_code': ts_code,
                            'name': name,
                            'current_price': row["close"],
                            'change_pct': change_pct,
                            'volume_ratio': volume_ratio,
                            'anomaly_types': anomaly_types,
                            'anomaly_score': self._calculate_anomaly_score(change_pct, volume_ratio),
                            'update_time': current_time.isoformat()
                        })
                except Exception as e:
                    logger.error(f"检测 {ts_code} 异动失败: {str(e)}")
                    continue
            
            # 同分时按 |涨跌幅| 降序：只按 score 排会让同分条目顺序随机
            anomalies.sort(
                key=lambda x: (x['anomaly_score'], abs(x['change_pct'])),
                reverse=True,
            )
            
            return {
                'success': True,
                'data': {
                    'anomalies': anomalies[:50],  # 返回前50个异动股票
                    'total_count': len(anomalies),
                    'change_threshold': change_threshold,
                    'volume_threshold': volume_threshold,
                    'period_hours': period_hours,
                    'update_time': self._frame_quote_time(latest_rows).isoformat()
                },
                'message': f'检测到 {len(anomalies)} 只异动股票'
            }
            
        except Exception as e:
            logger.error(f"检测异动股票失败: {str(e)}")
            return {'success': False, 'message': str(e)}
    
    def get_market_sentiment(self, period_hours: int = 1) -> Dict:
        """获取市场情绪指标"""
        try:
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=period_hours)
            # 全市场口径：快照覆盖全部标的，不再受「活跃股上限」约束
            frame, source = self._monitor_frame(
                period_type=self.DEFAULT_PERIOD_TYPE,
                start_time=start_time, end_time=end_time,
            )

            if self._is_snapshot_source(source):
                # 快照自带涨跌幅：向量化统计，全市场 5575 只也能秒算
                pct = pd.to_numeric(frame["change_pct"], errors="coerce").dropna()
                total_volume = float(pd.to_numeric(frame["volume"], errors="coerce").fillna(0).sum())
                total_amount = float(pd.to_numeric(frame["amount"], errors="coerce").fillna(0).sum())
                changes = pct.tolist()
                rising_stocks = int((pct > 0.1).sum())
                falling_stocks = int((pct < -0.1).sum())
                unchanged_stocks = int(len(pct) - rising_stocks - falling_stocks)
            else:
                # 回落路径：分钟线需逐只用昨收计算涨跌幅
                rising_stocks = 0
                falling_stocks = 0
                unchanged_stocks = 0
                total_volume = 0.0
                total_amount = 0.0
                changes = []
                prev_close_map = self._daily_prev_close_map(
                    frame["ts_code"].astype(str).tolist(), end_time.strftime('%Y%m%d')
                )
                for _, row in frame.iterrows():
                    try:
                        current_time = pd.to_datetime(row["datetime"]).to_pydatetime()
                        prev_close = self._get_previous_close(
                            row["ts_code"], current_time, self.DEFAULT_PERIOD_TYPE, prev_close_map
                        )
                        if not prev_close or prev_close <= 0:
                            continue
                        change_pct = (float(row["close"]) - prev_close) / prev_close * 100
                        changes.append(change_pct)
                        if change_pct > 0.1:
                            rising_stocks += 1
                        elif change_pct < -0.1:
                            falling_stocks += 1
                        else:
                            unchanged_stocks += 1
                        total_volume += float(row["volume"] or 0)
                        total_amount += float(row["amount"] or 0)
                    except Exception as e:
                        logger.error(f"处理 {row.get('ts_code')} 市场情绪数据失败: {str(e)}")
                        continue

            total_stocks = rising_stocks + falling_stocks + unchanged_stocks
            
            if total_stocks == 0:
                return {
                    'success': False,
                    'message': '没有足够的数据计算市场情绪'
                }
            
            # 计算市场情绪指标
            rising_ratio = rising_stocks / total_stocks * 100
            falling_ratio = falling_stocks / total_stocks * 100
            
            # 计算市场强度指标
            avg_change = np.mean(changes) if changes else 0
            change_std = np.std(changes) if changes else 0
            
            # 计算情绪评分 (0-100)
            sentiment_score = min(100, max(0, 50 + avg_change * 5 + (rising_ratio - 50)))
            
            # 确定市场状态
            if sentiment_score >= 70:
                market_status = '强势'
                status_color = 'success'
            elif sentiment_score >= 55:
                market_status = '偏强'
                status_color = 'info'
            elif sentiment_score >= 45:
                market_status = '震荡'
                status_color = 'warning'
            elif sentiment_score >= 30:
                market_status = '偏弱'
                status_color = 'secondary'
            else:
                market_status = '弱势'
                status_color = 'danger'
            
            return {
                'success': True,
                'data': {
                    'sentiment_score': sentiment_score,
                    'market_status': market_status,
                    'status_color': status_color,
                    'rising_stocks': rising_stocks,
                    'falling_stocks': falling_stocks,
                    'unchanged_stocks': unchanged_stocks,
                    'total_stocks': total_stocks,
                    'rising_ratio': rising_ratio,
                    'falling_ratio': falling_ratio,
                    'avg_change_pct': avg_change,
                    'volatility': change_std,
                    'total_volume': total_volume,
                    'total_amount': total_amount,
                    'period_hours': period_hours,
                    'update_time': self._frame_quote_time(frame).isoformat()
                },
                'message': f'成功计算市场情绪，涉及 {total_stocks} 只股票'
            }
            
        except Exception as e:
            logger.error(f"获取市场情绪失败: {str(e)}")
            return {'success': False, 'message': str(e)}
    
    def get_monitor_overview(self) -> Dict:
        """获取监控概览"""
        try:
            # 快照优先（全市场），回落分钟线（含最新分区），避免非交易时段全空
            end_time = datetime.now()
            minute_df, _source = self._monitor_frame(
                period_type=self.DEFAULT_PERIOD_TYPE,
                start_time=end_time - timedelta(hours=1),
                end_time=end_time,
            )
            if minute_df.empty:
                return {
                    'success': True,
                    'data': {
                        'total_stocks': 0,
                        'active_stocks': 0,
                        'today_records': 0,
                        'latest_update': None,
                        'system_status': 'running',
                        'data_delay': None
                    },
                    'message': '监控概览获取成功'
                }

            minute_df["datetime"] = pd.to_datetime(minute_df["datetime"], errors="coerce")
            total_stocks = int(minute_df["ts_code"].dropna().astype(str).nunique()) if "ts_code" in minute_df.columns else 0
            latest_time = minute_df["datetime"].max()
            latest_is_valid = latest_time is not None and not pd.isna(latest_time)
            # 「今天」以数据最新交易日为准：否则回落到历史分区时 today_records 恒为 0
            data_date = latest_time.date() if latest_is_valid else datetime.now().date()
            today_records = int((minute_df["datetime"].dt.date == data_date).sum())
            # 「活跃」相对数据最新时刻衡量，而非墙钟时间
            active_anchor = latest_time if latest_is_valid else datetime.now()
            active_stocks = int(
                minute_df[minute_df["datetime"] >= (active_anchor - timedelta(hours=1))]["ts_code"]
                .dropna().astype(str).nunique()
            )
            
            return {
                'success': True,
                'data': {
                    'total_stocks': total_stocks,
                    'active_stocks': active_stocks,
                    'today_records': today_records,
                    'latest_update': latest_time.isoformat() if latest_time else None,
                    'system_status': 'running',
                    'data_delay': self._calculate_data_delay(latest_time) if latest_time else None
                },
                'message': '监控概览获取成功'
            }
            
        except Exception as e:
            logger.error(f"获取监控概览失败: {str(e)}")
            return {'success': False, 'message': str(e)}
    
    def _get_active_stocks(self, limit: int = 100) -> List[str]:
        """获取活跃股票列表。

        优先级：① 全市场快照（按成交额降序取最活跃的 N 只，覆盖全市场）
        ② 分钟线（窗口无数据时回落最新分区）③ stock_basic 真实标的池。
        绝不返回硬编码的 5 只股票 —— 那会让下游"活跃股"变成假数据。
        """
        try:
            snap = self._snapshot_frame()
            if not snap.empty and "ts_code" in snap.columns:
                if "amount" in snap.columns:
                    snap = snap.sort_values("amount", ascending=False)
                codes = snap["ts_code"].dropna().astype(str).drop_duplicates().head(limit).tolist()
                if codes:
                    return codes
        except Exception as e:
            logger.error(f"从快照获取活跃股票失败: {str(e)}")

        try:
            recent_time = datetime.now() - timedelta(hours=1)
            minute_df = self._minute_frame(
                period_type=self.DEFAULT_PERIOD_TYPE,
                start_time=recent_time,
                end_time=datetime.now(),
                fallback_latest=True,
            )
            if not minute_df.empty and "ts_code" in minute_df.columns:
                codes = (
                    minute_df["ts_code"].dropna().astype(str)
                    .drop_duplicates().head(limit).tolist()
                )
                if codes:
                    return codes
        except Exception as e:
            logger.error(f"获取活跃股票失败: {str(e)}")

        # 无分钟数据：退回股票基础表（真实标的池）
        try:
            basic = self.data_reader.get_stock_basic()
            if not basic.empty and "ts_code" in basic.columns:
                codes = (
                    basic["ts_code"].dropna().astype(str)
                    .drop_duplicates().head(limit).tolist()
                )
                if codes:
                    logger.warning("分钟线无数据，活跃股清单退回 stock_basic 前 N 只")
                    return codes
        except Exception as e:
            logger.error(f"读取股票基础表失败: {str(e)}")

        return ['000001.SZ', '000002.SZ', '600000.SH', '600036.SH', '000858.SZ']
    
    def _daily_prev_close_map(self, ts_codes: List[str], current_date: str,
                              lookback_days: int = 20) -> Dict[str, float]:
        """批量获取昨收：一次读日线，返回 {ts_code: 昨收}。

        当日 bar 的 pre_close 就是昨收；当日尚无日线（盘中）时，
        取最近一根前日 bar 的 close。
        """
        result: Dict[str, float] = {}
        try:
            if not ts_codes:
                return result
            start = (datetime.strptime(current_date, '%Y%m%d') - timedelta(days=lookback_days)).strftime('%Y%m%d')
            daily = self.data_reader.get_daily(ts_codes=list(ts_codes), start_date=start)
            if daily.empty or "ts_code" not in daily.columns:
                return result
            daily = daily.copy()
            if "trade_date" in daily.columns:
                daily["td"] = daily["trade_date"].astype(str).str.replace("-", "", regex=False)
            else:
                return result
            daily = daily.sort_values(["ts_code", "td"])
            for ts_code, group in daily.groupby("ts_code"):
                prior_or_same = group[group["td"] <= current_date]
                if prior_or_same.empty:
                    continue
                last = prior_or_same.iloc[-1]
                if last["td"] == current_date and "pre_close" in group.columns:
                    pc = last.get("pre_close")
                    if pc is not None and not pd.isna(pc) and float(pc) > 0:
                        result[str(ts_code)] = float(pc)
                        continue
                close = last.get("close")
                if close is not None and not pd.isna(close) and float(close) > 0:
                    result[str(ts_code)] = float(close)
            return result
        except Exception as e:
            logger.error(f"批量获取昨收失败: {e}")
            return result

    def _daily_turnover_map(self, ts_codes: List[str], current_date: str,
                            lookback_days: int = 10) -> Dict[str, float]:
        """批量获取最近可得的真实换手率（来自 daily_basic），缺数据返回空映射。"""
        result: Dict[str, float] = {}
        try:
            if not ts_codes:
                return result
            start = (datetime.strptime(current_date, '%Y%m%d') - timedelta(days=lookback_days)).strftime('%Y%m%d')
            basic = self.data_reader.get_daily_basic(ts_codes=list(ts_codes), start_date=start)
            if basic.empty or "ts_code" not in basic.columns or "turnover_rate" not in basic.columns:
                return result
            basic = basic.copy()
            basic["td"] = basic["trade_date"].astype(str).str.replace("-", "", regex=False)
            basic = basic[basic["td"] <= current_date].sort_values(["ts_code", "td"])
            for ts_code, group in basic.groupby("ts_code"):
                last = group.iloc[-1]
                value = last.get("turnover_rate")
                if value is not None and not pd.isna(value):
                    result[str(ts_code)] = float(value)
            return result
        except Exception as e:
            logger.error(f"批量获取换手率失败: {e}")
            return result

    def _daily_volume_avg_map(self, ts_codes: List[str], current_date: str,
                              n_days: int = 5, lookback_days: int = 20) -> Dict[str, float]:
        """批量获取最近 n_days 个交易日的成交量均值（手），作量比分母。

        只纳入 current_date **之前**的交易日：当日 bar 一旦入库（收盘后同步），
        把当日算进分母会让量比恒接近 1。缺数据的标的不会出现在结果里，
        调用方据此返回 None，不编造 1.0。
        """
        result: Dict[str, float] = {}
        try:
            if not ts_codes:
                return result
            start = (datetime.strptime(current_date, '%Y%m%d') - timedelta(days=lookback_days)).strftime('%Y%m%d')
            daily = self.data_reader.get_daily(ts_codes=list(ts_codes), start_date=start)
            if daily.empty or "ts_code" not in daily.columns \
                    or "vol" not in daily.columns or "trade_date" not in daily.columns:
                return result
            daily = daily.copy()
            daily["td"] = daily["trade_date"].astype(str).str.replace("-", "", regex=False)
            daily = daily[(daily["td"] < current_date) & daily["vol"].notna()]
            if daily.empty:
                return result
            daily = daily.sort_values(["ts_code", "td"]).groupby("ts_code").tail(n_days)
            for ts_code, group in daily.groupby("ts_code"):
                avg = float(group["vol"].mean())
                if avg > 0:  # NaN > 0 为 False，天然跳过缺值标的
                    result[str(ts_code)] = avg
            return result
        except Exception as e:
            logger.error(f"批量获取均量失败: {e}")
            return result

    @staticmethod
    def _snapshot_volume_ratio(volume, avg_volume: Optional[float]) -> Optional[float]:
        """量比 = 当日成交量 ÷ 最近 N 日全天成交量均量；基准缺失返回 None。

        口径说明：分母用「全天均量」而非「同一时刻的累计均量」——本地没有
        分钟级历史，无法做同期对齐，故盘中量比会系统性偏低，按全天口径解读。
        无基准时返回 None 而非 1.0，让前端显示「--」而不是假数据。
        """
        if avg_volume is None or pd.isna(avg_volume) or float(avg_volume) <= 0:
            return None
        if volume is None or pd.isna(volume):
            return None
        return float(volume) / float(avg_volume)

    def _get_previous_close(self, ts_code: str, current_time: datetime, period_type: str,
                            prev_close_map: Optional[Dict[str, float]] = None) -> Optional[float]:
        """获取昨收价（前一交易日收盘价）。

        优先用预取的日线 昨收 map（当日 pre_close 或最近前日 close）；
        无日线数据时退回分钟序列的最近收盘（此时返回值语义是
        "最近一次分钟收盘"而非严格昨收，调用方需容忍）。
        """
        try:
            if prev_close_map and ts_code in prev_close_map:
                return prev_close_map[ts_code]

            current_date = current_time.strftime('%Y%m%d')
            daily_map = self._daily_prev_close_map([ts_code], current_date)
            if ts_code in daily_map:
                return daily_map[ts_code]

            # 日线缺失时的退化路径：最近一根分钟 bar 的 close
            df = self._minute_frame(period_type=period_type, end_time=current_time, ts_codes=[ts_code])
            if df.empty:
                return None
            latest = self._latest_rows(df)
            if latest.empty:
                return None
            return float(latest.iloc[0]["close"])

        except Exception as e:
            logger.error(f"获取 {ts_code} 前收盘价失败: {str(e)}")
            return None
    
    def _calculate_volume_ratio(self, ts_code: str, current_time: datetime, period_type: str) -> float:
        """计算成交量比"""
        try:
            start_time = current_time - timedelta(hours=20)
            df = self._minute_frame(period_type=period_type, start_time=start_time, end_time=current_time, ts_codes=[ts_code])
            if df.empty:
                return 1.0

            avg_volume = df["volume"].dropna().mean()
            if pd.isna(avg_volume) or avg_volume == 0:
                return 1.0

            current_rows = df[df["datetime"] == current_time]
            if current_rows.empty:
                latest = self._latest_rows(df)
                if latest.empty:
                    return 1.0
                current_volume = latest.iloc[0]["volume"]
            else:
                current_volume = current_rows.iloc[0]["volume"]

            return float(current_volume) / float(avg_volume)
            
        except Exception as e:
            logger.error(f"计算 {ts_code} 成交量比失败: {str(e)}")
            return 1.0
    
    def _calculate_turnover_rate(self, ts_code: str, volume: float,
                                 turnover_map: Optional[Dict[str, float]] = None) -> Optional[float]:
        """换手率：来自 daily_basic 的真实值；无数据时返回 None 而不是编造。

        旧实现 `min(20.0, volume/1000000*0.1)` 是拍脑袋的估算值，
        却以真实数据的形态展示给用户，对量化产品不可接受。
        """
        if turnover_map and ts_code in turnover_map:
            return turnover_map[ts_code]

        try:
            current_date = datetime.now().strftime('%Y%m%d')
            daily_map = self._daily_turnover_map([ts_code], current_date)
            return daily_map.get(ts_code)
        except Exception as e:
            logger.error(f"获取 {ts_code} 换手率失败: {str(e)}")
            return None
    
    def _get_stock_name(self, ts_code: str) -> str:
        """获取股票名称"""
        try:
            stock_basic = self.data_reader.get_stock_basic(ts_code)
            if stock_basic.empty or "name" not in stock_basic.columns:
                return ts_code
            return str(stock_basic.iloc[0]["name"])
        except Exception as e:
            logger.error(f"获取 {ts_code} 股票名称失败: {str(e)}")
            return ts_code
    
    def _check_price_breakout(self, ts_code: str, latest_data) -> bool:
        """检查价格突破（简化版本）"""
        try:
            latest_time = pd.to_datetime(latest_data["datetime"]).to_pydatetime()
            start_time = latest_time - timedelta(hours=20)
            price_df = self._minute_frame(period_type=self.DEFAULT_PERIOD_TYPE, start_time=start_time, end_time=latest_time, ts_codes=[ts_code])

            if price_df.empty:
                return False

            price_df = price_df[price_df["datetime"] < latest_time]
            if price_df.empty:
                return False

            max_high = price_df["high"].max()
            min_low = price_df["low"].min()
            if pd.isna(max_high) or pd.isna(min_low):
                return False

            return (latest_data["high"] > max_high * 1.01 or latest_data["low"] < min_low * 0.99)
            
        except Exception as e:
            logger.error(f"检查 {ts_code} 价格突破失败: {str(e)}")
            return False
    
    def _calculate_anomaly_score(self, change_pct: float, volume_ratio: Optional[float]) -> float:
        """计算异动评分（价格分 + 量能分，各上限 50）。

        量比缺失（None）时量能分记 0，评分退化为纯价格分，而不是整体失分；
        量比 < 1（缩量）用 max(0, ...) 截断而非扣分，否则缩量股会被负分
        压到榜尾，掩盖其价格异动。
        """
        try:
            price_score = min(50, abs(change_pct) * 5)  # 价格变动评分
            if volume_ratio is None or pd.isna(volume_ratio):
                volume_score = 0.0
            else:
                volume_score = min(50, max(0.0, (float(volume_ratio) - 1) * 10))  # 成交量变动评分
            return price_score + volume_score
        except Exception as e:
            logger.error(f"计算异动评分失败: {str(e)}")
            return 0.0
    
    def _calculate_data_delay(self, latest_time: datetime) -> int:
        """计算数据延迟（分钟）"""
        try:
            now = datetime.now()
            delay = (now - latest_time).total_seconds() / 60
            return int(delay)
        except Exception as e:
            logger.error(f"计算数据延迟失败: {str(e)}")
            return 0 
