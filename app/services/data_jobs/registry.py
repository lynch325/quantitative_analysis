"""数据作业注册表：`job_type` → JobDefinition（脚本路径 / 分组 / 依赖 / 数据源）。

唯一真相源，新增或下线作业只改这里：
- `_jobs` 是全集（含仅供内部调用的作业）；
- `_visible_job_types` 控制「页面与 AI/API 可见」的作业集合——
  两者需同步维护，只加 `_jobs` 不会出现在前端下拉里；
- `recommended_order` 决定推荐执行顺序（list_visible_jobs 按它排序），
  例：交易日历 → 基础资料 → 日线 → 衍生/宽表，顺序错会让下游算到空数据。

历史上已摘除 `baostock_daily` 与 `min5/min15/min30/min60`：它们硬编码 2025 年
日期区间、无视 DATA_JOB_* 参数，且写入的表无人读取（分钟线现由通达信同步
服务维护 `stock_minute/`）——保留注册项只会让 AI/API 误提交无效作业。

`get_job` 对未知 job_type 直接抛 KeyError（不做默认兜底）。
"""

from collections import defaultdict
from typing import Dict, List, Set

from app.services.data_jobs.schemas import JobDefinition


class JobRegistry:
    """Central registry for data job definitions mapped from app/utils scripts."""

    def __init__(self) -> None:
        # 仅向页面暴露的任务（按用户确认保留）
        self._visible_job_types: Set[str] = {
            "stock_basic",            # 1
            "stock_basic_fuyao",      # 1b（扶摇源股票清单刷新）
            "trade_calendar",         # 2
            "daily_history_by_date",  # 5
            "daily_history_fuyao",    # 5b（扶摇源日线）
            "daily_basic",            # 6
            "moneyflow",              # 15
            "stk_factor",             # 17
            "stk_factor_derived",     # 17b（本地日线自算，不依赖外部源）
            "cyq_perf",               # 18
            "minute_sync_tickflow",   # 19（分钟线：TickFlow 日内分时）
            "wide_table_builder",     # 20
            "factor_compute",         # 因子计算（打分/回测的前置作业）
        }

        # 注意：baostock_daily / min5 / min15 / min30 / min60 已从注册表摘除：
        # 脚本硬编码 2025 年日期区间、无视 DATA_JOB_* 参数，且写入的表没有任何
        # 读取方（实时分钟线走 stock_minute/，由通达信同步服务维护），保留
        # 注册项只会让 AI/API 误提交无效作业。
        self._jobs: Dict[str, JobDefinition] = {
            "stock_basic": JobDefinition(
                "stock_basic",
                "基础资料",
                "app/utils/stock_basic_fuyao.py",
                display_name="股票基础资料",
                description="刷新股票代码与名称清单（扶摇全市场快照）。",
                recommended_order=2,
                source_name="fuyao",
                source_mode="incremental",
                supports_incremental=True,
            ),
            "stock_basic_fuyao": JobDefinition(
                "stock_basic_fuyao",
                "基础资料",
                "app/utils/stock_basic_fuyao.py",
                display_name="股票清单（扶摇源）",
                description="从扶摇快照刷新股票名称并追加新上市代码；行业/退市股等元数据仍由 tushare 版维护。",
                recommended_order=2,
                source_name="fuyao",
                source_mode="incremental",
                supports_incremental=True,
            ),
            "trade_calendar": JobDefinition(
                "trade_calendar",
                "基础资料",
                "app/utils/trade_calendar_fuyao.py",
                display_name="交易日历",
                description="下载交易日、开市状态和前一交易日，是日频任务的基础依赖（扶摇固定近一年窗口）。",
                recommended_order=1,
                source_name="fuyao",
                source_mode="incremental",
                supports_incremental=True,
            ),
            # 注：原 stock_company（上市公司资料：董事长/总经理/注册资本等）
            # 三个可用数据源均不提供，已移除注册，避免 AI/API 提交无源作业。
            "daily_history_by_code": JobDefinition(
                "daily_history_by_code",
                "日频行情与基本面",
                "app/utils/daily_history_fuyao.py",
                display_name="日线行情（按股票代码）",
                description="按股票逐只下载日线行情，依赖股票基础资料。",
                dependencies=["stock_basic"],
                recommended_order=5,
                source_name="fuyao",
                source_mode="incremental",
                supports_incremental=True,
            ),
            "daily_history_by_date": JobDefinition(
                "daily_history_by_date",
                "日频行情与基本面",
                "app/utils/daily_history_fuyao.py",
                display_name="日线行情（按交易日）",
                description="按交易日批量下载日线行情，适合初始化全市场日线数据。",
                dependencies=["trade_calendar"],
                recommended_order=4,
                source_name="fuyao",
                source_mode="incremental",
                supports_incremental=True,
            ),
            "daily_basic": JobDefinition(
                "daily_basic",
                "日频行情与基本面",
                "app/utils/daily_basic_fuyao.py",
                display_name="日线基本指标",
                description=(
                    "日线基本面指标：收盘价与量比取自本地日线，总市值取自通达信数仓 GP16，"
                    "pe_ttm/pb/ps_ttm 取自扶摇估值快照（仅最新交易日）。"
                    "换手率/流通股本/股息率等无可用数据源，保持缺失。"
                ),
                recommended_order=6,
                source_name="fuyao",
                source_mode="incremental",
                supports_incremental=True,
            ),
            "daily_history_fuyao": JobDefinition(
                "daily_history_fuyao",
                "日频行情与基本面",
                "app/utils/daily_history_fuyao.py",
                display_name="日线行情（扶摇源）",
                description="从扶摇数据源下载全市场日线行情（dump 一次覆盖全市场），与 tushare 版写入同一张表。",
                dependencies=["trade_calendar"],
                recommended_order=5,
                source_name="fuyao",
                source_mode="incremental",
                supports_incremental=True,
            ),
            "income_statement": JobDefinition(
                "income_statement", "财务三表", "app/utils/financial_fuyao.py", display_name="利润表", description="下载上市公司利润表（扶摇单标的接口，按报告期增量）。", dependencies=["stock_basic"], source_name="fuyao", source_mode="incremental", supports_incremental=True
            ),
            "balance_sheet": JobDefinition(
                "balance_sheet", "财务三表", "app/utils/financial_fuyao.py", display_name="资产负债表", description="下载上市公司资产负债表（扶摇单标的接口，按报告期增量）。", dependencies=["stock_basic"], source_name="fuyao", source_mode="incremental", supports_incremental=True
            ),
            "cash_flow": JobDefinition(
                "cash_flow", "财务三表", "app/utils/financial_fuyao.py", display_name="现金流量表", description="下载上市公司现金流量表（扶摇单标的接口，按报告期增量）。", dependencies=["stock_basic"], source_name="fuyao", source_mode="incremental", supports_incremental=True
            ),
            "financial_fuyao": JobDefinition(
                "financial_fuyao", "财务三表", "app/utils/financial_fuyao.py", display_name="财务三表（扶摇源）",
                description="从扶摇数据源下载利润表/资产负债表/现金流量表（单标的接口逐只拉取，免费 key 可用），与 tushare VIP 版写入同一组表。",
                dependencies=["stock_basic"], source_name="fuyao", source_mode="incremental", supports_incremental=True
            ),
            "moneyflow": JobDefinition("moneyflow", "资金流与扩展因子", "app/utils/moneyflow_derived.py", display_name="资金流向（本地估算）", description="按价格位置法估算每日资金净额；大中小单分层需逐笔数据，现有数据源均不提供，保持缺失。", recommended_order=7, source_name="derived", source_mode="derived"),
            "stk_factor": JobDefinition("stk_factor", "资金流与扩展因子", "app/utils/stk_factor_derived.py", display_name="扩展技术因子", description="由本地日线自算 MACD/KDJ/RSI/BOLL/CCI 与复权价，不依赖外部行情源。", recommended_order=8, source_name="derived", source_mode="derived", dependencies=["daily_history_fuyao"]),
            "stk_factor_derived": JobDefinition(
                "stk_factor_derived",
                "资金流与扩展因子",
                "app/utils/stk_factor_derived.py",
                display_name="扩展技术因子（本地自算）",
                description=(
                    "不依赖任何外部行情源：直接读已落盘的 daily_history/daily，"
                    "自算 MACD/KDJ/RSI/BOLL/CCI 与复权价，写入与 tushare 版同一张 "
                    "stk_factor/daily 表（复权因子取本地数仓 forward_factor，"
                    "取不到时退化为未复权）。"
                ),
                recommended_order=8,
                source_name="derived",
                source_mode="derived",
                dependencies=["daily_history_fuyao"],
            ),
            "cyq_perf": JobDefinition("cyq_perf", "资金流与扩展因子", "app/utils/cyq_perf_derived.py", display_name="筹码分布（本地估算）", description="按三角形分布+换手衰减模型自算筹码成本分布与胜率，数据源均不提供真实筹码。", recommended_order=9, source_name="derived", source_mode="derived", dependencies=["daily_history_fuyao"]),
            # 分钟线：TickFlow 日内分时（pro+ 档，Beta）。填补 stock_minute 缺口 ——
            # 通达信 pytdx 不支持 1min（bars.py 直接抛错）、Baostock 会把 1min 静默降级为 5min。
            # 逐只请求（全市场约 5500 次），脚本内置限频重试与节流，建议盘中/盘后单次执行。
            "minute_sync_tickflow": JobDefinition(
                "minute_sync_tickflow",
                "分钟线",
                "app/utils/minute_sync_tickflow.py",
                display_name="分钟线同步（TickFlow 日内分时）",
                description=(
                    "同步最新交易日分钟线到 stock_minute/，供实时监控/异动/情绪与 WebSocket 推送使用。"
                    "参数 period=1m|5m|15m|30m|60m（默认 1m，与监控默认的 5min 分属不同分区）。"
                ),
                recommended_order=19,
                source_name="tickflow",
                source_mode="incremental",
                supports_incremental=True,
                dependencies=["stock_basic"],
            ),
            "ma_calculator": JobDefinition(
                "ma_calculator",
                "衍生计算",
                "app/utils/ma_calculator.py",
                display_name="均线衍生计算",
                description="基于日线行情生成均线结果，属于衍生计算任务。",
                dangerous=True,
                dependencies=["daily_history_by_code"],
                source_name="derived",
                source_mode="derived",
            ),
            "wide_table_builder": JobDefinition(
                "wide_table_builder",
                "衍生计算",
                "app/utils/wide_table_builder.py",
                display_name="大宽表构建",
                description="合并日线基本指标、技术因子、资金流向和股票基础资料为最新交易日大宽表。",
                recommended_order=10,
                source_name="derived",
                source_mode="derived",
                dependencies=["daily_basic", "stk_factor", "moneyflow", "stock_basic"],
            ),
            "factor_compute": JobDefinition(
                "factor_compute",
                "衍生计算",
                "app/utils/factor_compute.py",
                display_name="因子计算",
                description=(
                    "基于行情/资金流/筹码/财务数据计算内置因子与自定义表达式因子，"
                    "写入 factor_values 供打分与回测使用。"
                ),
                recommended_order=11,
                source_name="derived",
                source_mode="derived",
                dependencies=[
                    "daily_history_by_code", "daily_basic", "stk_factor",
                    "moneyflow", "cyq_perf", "income_statement", "balance_sheet",
                ],
            ),
        }

    def get_job(self, job_type: str) -> JobDefinition:
        if job_type not in self._jobs:
            raise KeyError(f"unknown job type: {job_type}")
        return self._jobs[job_type]

    def list_jobs(self) -> List[JobDefinition]:
        return list(self._jobs.values())

    def list_visible_jobs(self) -> List[JobDefinition]:
        jobs = [job for job in self._jobs.values() if job.job_type in self._visible_job_types]
        return sorted(jobs, key=lambda job: (job.recommended_order, job.group, job.job_type))

    def grouped_jobs(self) -> Dict[str, List[JobDefinition]]:
        groups: Dict[str, List[JobDefinition]] = defaultdict(list)
        for job in self._jobs.values():
            groups[job.group].append(job)
        return dict(groups)
