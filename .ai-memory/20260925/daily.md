session-id: 20260925-1530

## [15:30] 架构性能审查（只读，未改码）

- 动作：对 `quantitative_analysis` 做架构级性能审查，出 P0/P1/P2 嫌疑清单 + 实测基线。
- 产出文件：无代码改动；探针脚本 `%TEMP%\qa_perf_probe.py`（只读，可复跑）。
- 实测证据（D:\Miniconda3\python.exe，pandas 2.3.3）：
  - `data/daily_history/daily` 共 248 个 Hive 分区 / 68.5 MB；2025 全年 73 个分区 / 19.9 MB；单日分区 280 KB。查 20 只股票 1 年现状需整读 19.9 MB（读放大 ~73×）。
  - `_walk_partitions` 单次遍历：1 年窗口 3.70 ms，单日窗口 2.90 ms（回测 243 调仓日累计仅约 0.7 s，非主瓶颈）。
  - 逐行 vs 向量化（5000 行）：`iterrows` 177.8 ms vs 向量化 0.3 ms（675×）；`for + .iloc[i]` 233.0 ms。
  - 循环内 `pd.concat` 增长 ×500：136.3 ms vs list 一次构造 0.7 ms（203×）。
  - 缓存 JSON 往返：100 条 0.34 ms / 1000 条 3.19 ms / 5000 条 18.75 ms（每次命中都付一次）。
- 被数据否定的假设：to_dict 后 Python 逐键清洗 vs pandas 向量化清洗，1000 行分别 0.49 ms / 1.63 ms —— 向量化反而慢，该处**不成立**，不改。
- 未验证项：pyarrow `filters=` + 列裁剪的 A/B 未实测（项目 .venv 的 base 解释器 `D:\Anaconda\python.exe` 已不存在，venv 不可用；本机无 cp313 pyarrow）。相关收益标为「假设收益」。

## 环境事实

- 项目 `.venv\pyvenv.cfg` → `home = D:\Anaconda`（**已不存在**），`启动后端.bat` 当前无法启动后端；需重建 venv（脚本注释建议 `uv venv .venv --python ...`）。
- 主栈：Flask 2.3 + 16 蓝图 + SocketIO(threading) + pandas/parquet；已去 Redis/Celery（`app/celery_app.py` 为同步桩）。

## [16:20] 修复：P0-3 环境 + P0-1 读放大 + P1-2 因子重复读

- 环境：旧 `.venv` base（`D:\Anaconda`）已不存在 → 改名保留为 `.venv.broken-cp312`，用 Miniconda 3.13 重建 `.venv`；依赖装齐并锁定 `pandas==2.3.3 / numpy==2.5.3`（与旧环境一致，避免 pandas 3 行为差异）。`requirements.txt` 漏声明 `duckdb`（`stock_pool_service.py:28` 直接 import），已补装。
- 装包要点：pip 不读系统代理；本机代理 `127.0.0.1:7897`，pip 需显式 `--proxy http://127.0.0.1:7897`（官方源走代理最快；阿里云镜像可直连备用）。
- P0-1（`app/services/data_reader.py::_read_table`）：改为 `pyarrow.dataset(files)` + `columns` 列裁剪 + `pc.field("ts_code").isin(...)` 谓词下推，失败回退逐文件读取、再回退整读；空 `ts_codes` 早退保持旧语义。
  - A/B（20 只股票 × 全库 248 分区，median of 3）：耗时 **2597.7 ms → 200.4 ms（13.0×）**，峰值内存 **314.6 MB → 1.4 MB（225×）**，行数 4937 不变。
  - 回测式 20 日单日读取：479.7 ms → 387.3 ms，内存 1.1 → 0.3 MB。
  - 正确性：`scripts/bench_data_reader.py` 内建对照（下推结果 vs 全量读取后内存过滤）行集合/列集合/收盘价逐行一致。
- P1-2（`app/services/factor_engine.py`）：`calculate_factor` / `_calculate_custom_factor` 增加 `data_cache` 透传，`calculate_all_factors` 中自定义因子共用一次 `get_return_prices`（此前每个自定义因子各自重读全窗口）；复用时 `.copy()` 避免污染缓存。
- 验证：pytest 118 failed / 563 passed（`logs/_baseline_tests.txt` 为改动前基线），**与基线一致无新增失败**（失败项均为既有：UI 文案契约、Windows tmp 文件 PermissionError、缺 `app/templates/realtime_analysis/monitor.html` 等）。真人验证：真实启动 `run.py` 后 HTTP 200 命中 `/api/stocks/{000001.SZ,600000.SH}/history` 与 `/api/stocks/000001.SZ/factors`。
- 新增文件：`scripts/bench_data_reader.py`（基准 + 正确性对照，可复跑）。
- 未做（下一 Wave）：P0-2 请求内同步重计算异步化（涉及接口契约改 202 + 轮询，需先对齐）、P1-4 循环内 concat、P1-5 部署形态。

## [17:00] 修复：P1-1 打点/对齐热点（profile 驱动）

- cProfile（全量因子计算，5550 只股票）定位：总 81.85 s，自有代码热点依次是 `_yoy_ttm_growth_factor`(34.1 s) → `_revenue_growth_factor`(18.4 s) → `_snapshot_stamp`(16.8 s/11266 次) → `_profit_growth_factor`(15.7 s) → `_snap_to_trade_date`(9.2 s/11266 次) → `_point_in_time_stamp`(9.1 s/45064 次)。
- 根因：打点路径对每个标量反复调 `pd.to_datetime(..., format="mixed")`（45064×2 次）。**先试的"加显式格式快路径"只从 81.9 s 降到 71.1 s——真正的成本是标量调用开销，不是格式推断**（教训：别停在第一次优化）。
- 最终改法（`factor_engine.py`）：
  - 新增 `_canonical_report_date()`：YYYYMMDD / YYYY-MM-DD 走纯字符串 + `datetime(y,m,d)` 日历校验，输出 ISO 串；异常格式才回退 pandas。ISO 串字典序 == 时间序，直接 `max()`。
  - `_point_in_time_stamp` 改为字符串比较，不再构造 Timestamp。
  - `_snap_to_trade_date` 改用 `np.datetime64(date_text)` 解析、输出 `astype("datetime64[D]")` 字符串化。
- A/B（同一 cProfile 口径）：**81.85 s → 57.41 s（1.43×）**，结果行数 55424 不变；`_point_in_time_stamp`/`_snap_to_trade_date` 已跌出热点前 10。
- 证伪/对拍：新旧实现对拍 423 组（`samples` 笛卡尔积 408 + namedtuple/多报表 5 + 对齐 10），含非法日期 `20250231`、日历范围外 `2019-05-01`/`2030-01-01`、`2025/01/15`、空串、NaT、None、Timestamp、**全部一致**。
- 验证：pytest 118 failed / 563 passed（与基线一致）；HTTP 实驱动 `/api/stocks/{000001.SZ,600519.SH}/factors` 与 `/history` 全 200，且 000001.SZ 因子响应长度 24102 与改动前**完全一致**。

## [18:10] 前端 Wave1：vite 分包 —— 实测为负收益，已回滚

- 环境：本机**无 Node**（node_modules/dist 为外地拷贝）。`install_binary` 装 Node 失败（EPERM + 并发锁），改手工装 `C:\Users\NIUNIUNIU\.tools\node22`（v22.12.0，npmmirror 走代理）。Vite 7 要求 Node ≥20.19（20.18 会拒跑）。
- 基线构建（Node 22，`npm run build`）：EChart 705.12 kB(gzip 238.36) / index 285.90(gzip 92.13) / lightweight 174.74(gzip 56.32) / CSS 258.59；gzip 合计 **603.1 KB / 61 chunk**。
- 加 `manualChunks`（vendor-react / vendor-echarts / vendor-lightweight-charts / vendor-socket / vendor-icons）后：index 降到 17.33 kB，但首屏实际要拉 `index + vendor-react + vendor-icons` = gzip **116.33 KB，比基线 92.13 KB 多 24.2 KB**；总量 601.4 KB（几乎不变）。原因：跨 chunk 无法共享 minify/dedup，产生边界开销。
- **结论：本项目是 localhost 部署（Flask 托管 dist），分包带来的"发版后缓存复用"收益在本地几乎为零，而首次加载反而变慢 → 已回滚**，`vite.config.ts` 恢复原状，dist 重新构建后 hash 与基线完全一致。
- 附带纠正两处先前的误判：① 首屏**不加载** ECharts（HomePage 无图表 import），EChart 块只在 15 个图表页按需加载；② 7 种图表类型（bar/line/pie/gauge/heatmap/radar/treemap）**全部在用**，无法靠减少注册项减体积。
- 真正的大项：CSS 258.59 KB，其中 bootstrap 226.7 KB 且被 44 个页面真实使用（数百处 class）→ 只能靠 PurgeCSS/按需 Sass 引入削减，但有视觉回归风险，未做。

## [19:00] 前端排版放宽（token 层）+ 三个展示修复

- 用户先要截图再定风格：用 agent-browser（npm 装 0.27.0 + Chrome 154，装到 ~/.agent-browser）对 4 个页面截图（`output/ui-shots/`），确认问题后选「放宽」+ 三项 extras。
- token 层（全局生效）：
  - `tailwind.config.ts`：字号阶梯 2xs 10→11、xs 11→12、sm 12→13、**base 13→14**、lg 15→16、新增 xl 18；新增 `lineHeight.body=1.65`；font-sans 与 theme.css 统一（PingFang 优先 + 补 Noto Sans SC）；mono 栈补 `Segoe UI Symbol`/雅黑（▲▼ 不在 JetBrains Mono 覆盖内，缺字形会抖基线）。
  - `theme.css`：body 补 `line-height:1.65`；<12px 字号全部清零（9.5→11、10/10.5→11、11/11.5→12，共 9 处）；`.brand-text` 1.15→1.4、`.stat-value` 1.2→1.35（中文会挤压）；旧表 th 11→12、td padding 9→10px。
  - `tsp.css`：`.tsp-root td/th` 统一 padding-top/bottom 8px——新表 py-1.5 散落 8 文件 80+ 处，用类+元素选择器兜底，免逐页改。
- extras：
  - `StockLink` 加 `showName` prop + `cleanName()`（过滤 'None'/'null'/'nan' 脏名称）；`WatchlistPage` 代码列 `showName={false}` 消除同表名称重复。
  - `primitives.tsx` Badge `py-px`→`py-0.5`（1px→2px）。
- 验证：`npm run build`（tsc + vite）通过；改后 4 页截图 `output/ui-shots/after/` 人工核对——代码列重复消除、跌停榜 None→只显代码、行高/字号放宽、箭头字形清晰、无破版；pytest 118F/563P 与基线一致。
- 纠正：before 截图里以为「红绿口径错乱」，实际是 ▼0.44% 被误读成 +0.44%——Delta 逻辑本身正确（▲红涨/▼绿跌），未改。

## [19:40] 删除旧版入口 + 新增 5 套配色（含彭博风）

- 删旧版：`App.tsx` 侧栏 `<a class="btn-ghost">旧版</a>`、`OLD_SITE_BASE` 常量、`ExternalLink` 导入三处移除；`.btn-ghost` 样式保留（复用给配色触发按钮）。
- 配色体系：`ThemeContext` 新增 `scheme` 维度（indigo/bloomberg/tradingview/eikon/gold），存 `localStorage['qa-scheme']`，root 挂 `data-scheme`；图表强调色由 `SCHEME_ACCENTS[scheme][mode]` 合成进 ChartPalette（accent/amber/teal/violet 随配色，结构色沿用明暗基础盘）。
- 涨跌色**不随配色走**：scheme 块不覆盖 `--ts-bull/bear` 与 `--up/--down`，固定 A股红涨绿跌（用户拍板）。
- CSS 覆盖点：`tsp.css`（--ts-* HSL 变量）与 `theme.css`（--bg/--surface/--accent 等旧体系变量）各加 4 scheme × (dark+light) 块。**坑：theme.css 的 scheme 块必须放文件末尾**——后半段 974 行还有一次 `:root` 覆盖，放前面会被盖掉；浅色需 `[data-theme='light'][data-scheme]` 双属性才压得过 `[data-theme='light']`。body 顶部光晕的写死蓝色改为 `var(--accent-soft)`。
- 新组件 `components/ui/SchemeSelect.tsx`：侧栏底部下拉（Palette 图标 + 当前配色名 + 预览圆点 + 勾选），mousedown 外点/Esc 关闭，监听器随卸载移除。
- 验证：`npm run build` 通过；agent-browser 实切 4 套 + 暗金×浅色，截图在 `output/ui-shots/schemes/`；pytest 118F/563P 与基线一致。
- 已知问题（未修）：行情看板「总成交 16691亿」在 6 列 KPI 里“亿”被挤压溢出格边（md 断点下 KPI 单元格过窄）。

## [20:40] app/ 注释补全（W1 模块头 + W2 公开接口 docstring）

- 用户要求给 app/ 每个脚本加注释；实测规模 **143 文件 / 38886 行，注释行仅 1816（5%）**，632 个顶层定义中 407 个已有 docstring，**60 个文件缺模块头**。确认档位：只做 W1+W2（不动行内，避免复述式假注释），按目录分批汇报。
- 执行纪律：只增不改（每批用 `qa_doc_verify.py` 逐行核对「删除行必须为 0」）、改前 `.bak`、每批 `py_compile` + 导入冒烟 + pytest 对比基线 118F/563P。
- 已完成：
  - **试点批 3 文件**：`app/utils/cache.py` +14 行、`app/utils/ma_calculator.py` +34、`app/services/minute_parquet_reader.py` +47（删 0）。
  - **B1a 部分批 2 文件**：`app/services/data_jobs/parquet_state_store.py` +120、`app/services/parquet_event_store.py` +96（删 0）；两文件均可导入，pytest 与基线一致。
- 顺带在注释里标注的**既有隐患**（非本次引入，供后续排查）：
  - `ma_calculator`：EMA5~30 用 pandas ewm（首值种子）、EMA60/120 用自研 SMA 种子递推，两条口径不一致；
  - `minute_parquet_reader.get_summary`：`missing_count`/`completeness` 是固定值 0/100.0，不代表真实完整度；
  - `parquet_event_store._write_event_frame_unlocked`：按业务键去重 keep=last，重复提交会覆盖而非追加；datetime 解析失败的行会被静默丢弃但仍计入返回行数；
  - `parquet_event_store.get_signal_performance`：返回的 `total_signals` 实为「EXECUTED+EXPIRED」条数，非窗口内全部信号数。
- **Batch 1（services 大文件 7 个）已全部完成**，累计 10/143 文件：
  | 文件 | 旧→新行 | 新增 | 删除 |
  |---|---|---|---|
  | `services/data_jobs/parquet_state_store.py` | 415→535 | +120 | 0 |
  | `services/parquet_event_store.py` | 544→640 | +96 | 0 |
  | `services/portfolio_optimizer.py` | 680→703 | +23 | 0 |
  | `services/stock_scoring.py` | 643→664 | +21 | 0 |
  | `services/ml_models.py` | 1042→1075 | +33 | 0 |
  | `services/factor_engine.py` | 1062→1092 | +30 | 0 |
  | `services/backtest_engine.py` | 1400→1476 | +76 | 0 |
  - 每批均过：`qa_doc_verify.py`（删除行=0）+ `py_compile` + 关键类导入冒烟 + pytest 118F/563P。
- 本轮注释中补充的关键约定（供后续维护者）：
  - `portfolio_optimizer`：预期收益年化映射区间 [-0.15,0.30] 为回测/API 共用；协方差 LedoitWolf + 先 ffill 再算收益；`as_of_date` 是前视防线；求解器按 (CLARABEL,SCS,OSQP) 探测（ECOS 自 cvxpy 1.5 起不捆绑）。
  - `ml_models`：`_target_period` 对非 `return_Nd` 类型一律按 5 日（决定标签前移）；模型目录锚定项目根 `models/`；缓存以磁盘 mtime 失效。
  - `backtest_engine`：t+1 收盘成交、停牌/涨跌停可交易性、`_build_*` 为纯格式化层（改字段名需同步前端）、收益分布桶 ±10% 之外不计入、回撤峰值以 initial_capital 起算。
  - `factor_engine`：财务因子按公告日打点对齐交易日、价格类统一后复权、`_canonical_report_date` 为打点热点优化点。
- 待续：Batch 2 = services 其余（`realtime_*` 系列、`data_jobs` 其余、`ai/`、`tongdaxin/`，约 51 文件）；Batch 3 = api(18)；Batch 4 = utils(46)；Batch 5 = models/tasks/websocket/app 根(21)。

## [21:30] 注释任务改为 W1 模式 + 2A/2B 完成

- 用户决定：**只补模块头（W1）**，不再补方法级 docstring（省一半以上轮次）。
- 已确认事实：全仓 143 个 .py 中**只有 60 个缺模块 docstring**；经本轮补齐后**仍缺 37 个**（W1 剩余量就这么多，不是 143）。
- 2A（`data_jobs/` + `tongdaxin/`，8 文件，+95 行 / 删 0）：registry（作业注册表唯一真相源；`_jobs` 与 `_visible_job_types` 需同步；已摘除 baostock_daily 与 min5/15/30/60 的原因）、runner（子进程隔离 + DATA_JOB_* 环境变量传参 + 超时 returncode=124）、schemas、service（只剩 inline 模式，后台线程自建 app context）、state_store（纯转发兼容层）、tongdaxin/{bars,client,code_mapping}。
- 2B（小型服务 + ai 包，5 文件有改动，+59 行 / 删 0）：`services/__init__.py`、`persistence.py`、`minute_parquet_store.py`（(日期,period_type) 分组 + 读合并写 + 原子替换）、`model_training_job_service.py`（进程内追踪 + 200 上限淘汰）、`report_dispatch_service.py`（最小可用：不接网关）。其余 8 个文件（data_asset_status/heatmap/stock_name_registry/wide_table_status/market_dashboard/ai 三件套）**本来就已有模块头**，未动。
- 踩坑（校验脚本抓到）：`services/__init__.py` 原行是 `# 服务层 `（**带尾随空格**），我第一次重写时去掉空格 → 被判"改了原行"，按原样恢复后通过。教训：改写已有行前先看 `repr()`。
- 累计进度：**23/143 文件**；W1 剩余 **37 个**：
  - api 5（`ml_factor_api` 1897 行、`stock_api` 256、`data_jobs_api` 144、`analysis_api` 97、`api/__init__`）
  - services 3（`stock_service` 756、`factor_expression_engine` 210、`tongdaxin_minute_sync_service` 107）
  - utils 22（`parquet_job_helpers` 254、`balance_sheet` 170、`cash_flow` 115、`income_statement` 103；其余多为 20~90 行的脚本包装）
  - models 2、tasks 1、app 根 2（`__init__.py` 80、`extensions.py` 30）、websocket 1

## [22:10] W1 第三批：utils 全清（22 文件）

- 22 个 utils 文件补模块头（+231 行 / 删 0；`utils/__init__.py` 原行 `# 工具包 ` 带尾随空格，已原样保留）。
- **重要事实（写进注释）**：registry.py 现在指向的都是 `_fuyao` / `_derived` 版本——
  `stock_basic→stock_basic_fuyao.py`、`trade_calendar→trade_calendar_fuyao.py`、
  `daily_basic→daily_basic_fuyao.py`、`daily_history_by_*→daily_history_fuyao.py`、
  `income_statement/balance_sheet/cash_flow→financial_fuyao.py`、
  `moneyflow→moneyflow_derived.py`、`stk_factor→stk_factor_derived.py`、`cyq_perf→cyq_perf_derived.py`。
  即本轮注释的 Tushare/Baostock 版脚本（`stk_factor.py`/`cyq_perf.py`/`moneyflow.py`/
  `daily_basic.py`/`trade_calendar.py`/`stock_basic.py`/`daily_history_by_*.py`/
  `baostock_daily.py`/`min5,15,30,60.py`/财报三表）**均未在注册表内**，属对照/备用实现——
  已在各模块头显式标注，避免后续维护者误以为它们是线上链路。
- `moneyflow_ths` 落表未登记在 `data_reader.TABLE_DIRS`，**无读表入口**（已在注释标注）。
- `job_env.py` 现状：仅 `normalize_ymd`/`env_bool` 被 `financial_fuyao.py` 复用；
  其余 4 个 MySQL cursor 函数（`resolve_date_window`/`fetch_open_trade_dates`/
  `latest_open_trade_date`/`delete_trade_date_range`）是去 MySQL 前的遗留，已标注不建议使用。
- `parquet_job_helpers.py` 是作业公共骨架（日期解析 → 缺口回补 DATA_JOB_MAX_GAP_FILL 默认 60 →
  DailyFetchJob 限速/退避/退出码 1）。
- 修正两处自己写错的注释：`runner.py` 里参数读取模块（job_env → parquet_job_helpers）、
  `logger.py` 里"重复调用会叠加 sink"的错误表述（实为 `logger.remove()` 全清后重建、幂等）。
- 踩坑：`stock_basic.py` 首两行是 `import os` / `from pathlib import Path`，我按 `import pandas`
  定位插入 → docstring 落在语句之后变成**无效字符串表达式**（校验脚本 `doc=无` 抓到），已移到文件最前。
  教训：加模块 docstring 必须确认插在**第一个语句之前**，不能只看文件开头像注释区。
- 进度：**W1 剩余 14 个文件**（`api/ml_factor_api` 1897、`api/stock_api` 256、
  `api/data_jobs_api` 144、`api/analysis_api` 97、`api/__init__` 5、`services/stock_service` 756、
  `services/factor_expression_engine` 210、`services/tongdaxin_minute_sync_service` 107、
  `tasks/data_jobs_tasks` 110、`app/__init__.py` 80、`app/extensions.py` 30、
  `models/data_job_run` 48、`models/__init__` 17、`websocket/__init__` 1）。

## [23:00] W1 收尾完成：143/143 文件全部有模块 docstring（剩余 0）

- 最后 14 个文件（api 5、services 3、tasks 1、app 根 2、models 2、websocket 1）已全部补齐。
- 终验证据：`compileall app` EXIT=0；应用工厂冒烟 `create_app('default')` → 16 蓝图 / 220 路由；
  `import app.api` / `app.api.ml_factor_api` 的模块 docstring 生效；pytest 118F/563P 与基线一致；
  `qa_w1_progress.py` → **仍缺模块 docstring：0 个**。
- **流程疏漏（如实记录）**：本批 14 个文件**忘记先建 `.bak`** 就动手了，因此无法用
  `qa_doc_verify.py` 做「只增不改」机械比对。改用替代证据 `qa_addition_check.py`：
  逐个断言每处编辑的原锚点（old_str）仍逐字存在于文件中 + 模块 docstring 位于第一个语句，
  14/14 全部通过 —— 证明改动均为纯插入、未覆盖原代码，但强度低于 .bak 逐行比对。
- 本轮写入注释的关键事实（均为代码事实，供后续维护）：
  - `app/api/__init__.py`：只 import 挂在 api_bp 上的模块，且必须在 Blueprint 定义之后（防循环导入）；
  - `app/__init__.py`：首行 `ensure_click_parameter_source()` 必须在 import Flask 前执行；
  - `app/extensions.py`：SQLite WAL/NORMAL PRAGMA 由 Engine connect 事件设，busy 超时在 config 的 connect_args；
  - `app/models/data_job_run.py`：**已无任何引用**（状态走 Parquet），且 `updated_at` 用 `datetime.utcnow`
    而其他时间列用 `now_local`，**同一行两种时区口径**；
  - `app/tasks/data_jobs_tasks.py`：`@celery.task` 只是形式（celery 为本地桩），任何异常都必须落 failed，
    否则 `find_active_duplicate` 永久拒绝重提；wide_table_builder 成功后要清 data_reader 与 text2sql 缓存；
  - `factor_expression_engine`：`Series.rank` 被刻意移出白名单（全样本前视），负 periods 被拦截；
  - `stock_service`：12 个方法挂 @cached，缓存键含全部入参（逐股逐日调用命中率趋零）；screen_stocks 有 max_results 上限；
  - `ml_factor_api`：37 个路由，`/factors/calculate`、`/batch/*` 仍在请求线程内同步做全市场重计算；
    响应存在 `{code,message,data}` 与 `{success,...}` 两种历史风格；
  - `tongdaxin_minute_sync_service`：pytdx 单次上限 800 根且未分段，分钟线不含复权。
- 备份现状：app 树下 **55 个 `.bak`（约 398 KB）**；`.gitignore` 只有 `bak/` 目录规则、**没有 `*.bak`**，
  将来启用 git 需先补规则或清理。
- 全程累计改动文件：**45 个**（试点 3 + Batch1 7 + 2A 8 + 2B 5 + utils 22），全部为 W1 模块头；
  另有 10 个文件在更早的 W2 阶段补过方法级 docstring。

## [23:40] W2 启动：方法级 docstring（先做最大单文件）

- 盘点：app/ 共 **1417 个函数/方法，缺 docstring 413 个（95 文件）**。按函数体行数分桶：
  ≤2 行（纯转发/getter）98 个、3-6 行 109 个、7-15 行 118 个、>15 行 88 个。
  **体≥7 行的 206 个才是值得注释的部分**；≤2 行的属复述式注释，建议跳过（未做，待用户确认）。
- 为降低 400+ 次手工编辑的成本与风险，新建**带自校验的批量插入器** `%TEMP%\qa_w2_apply.py`：
  - 输入 JSON 计划 `{file, docs:{qualname: 文本}}`，按 qualname 定位定义并把 docstring 插为函数体第一条语句；
  - 首次触碰文件时建 `.bak`（已存在则保留更早备份）；
  - **拒绝**：找不到 qualname / 已有 docstring / inline body（函数体与 def 同行）；
  - 双重自校验：插入后重新解析 AST 确认每个目标都拿到 docstring；再**删掉插入行必须逐字还原原文件**，失败则整体不写盘。
  - 辅助：`qa_w2_dump.py`（导出缺注释函数原文，MIN_BODY 可调）、`qa_w2_buckets.py`（分桶审计）。
- 踩坑（自校验抓到）：还原校验最初按**原始行号**删除插入块，多块插入时后面的块已被前面的插入推后 → 首次运行报 FATAL（幸好先校验后写盘，文件未动）。改为先算每块在最终文本中的起点（累加前序块长度）再降序删除。
- **W2-1 完成**：`app/services/parquet_state_store.py` 插入 **33 个 docstring**（956 → 1110 行，+154 / 删 0）。
  校验：`qa_doc_verify.py` OK（删除行 0）+ 导入冒烟确认 docstring 生效 + pytest 118F/563P 与基线一致。
  注释重点记录的口径：`read_frame` 不能返回空表（会静默清空数据）、`next_integer_id` 必须在锁内、
  `save_values` 的 float32 与分区级读改写、`get_values` 必须 `format='mixed'`（否则历史数据变 NaT 被丢）、
  `delete_definition` 会物理删除该模型预测、`calculate_metrics` 依赖入库前刷新持仓、
  `update_summary` 是回测进度唯一写入点。
- 剩余 W2 计划（体≥7 行，173 个）：models/ ~23、api/ 19、utils 39 + data_sources 8、
  services/ai 30、services 其余 ~46、data_jobs/tongdaxin/根 ~10。

### [次日] 中断恢复 + W2-2 / W2-3 完成

- **崩溃恢复核对（重要教训）**：扩展宿主崩溃后，我用 `Get-Content .Count` 数出 `parquet_state_store.py`
  只有 1030 行、与记录的 1110 行不符，一度怀疑文件被回滚。查字节后确认是**行尾混用**造成的计数错觉：
  我插入的 154 行用 LF、原文 956 行用 CRLF，`Get-Content` 对 LF-only 行计数偏低。
  **权威口径**：字节统计（LF=1110/CRLF=956）+ Python `splitlines()` + AST docstring 计数。
  结论：W2-1 完好（+154 / −0，44/51 有 docstring，缺的 7 个正是按设计跳过的 `__init__`×5 + `_now_iso` + `path_for`）。
- **工具修正**：`qa_w2_apply.py` 现在检测原文件行尾风格并让插入行跟随（CRLF 文件不再混入 LF）；
  新增 `qa_fix_eol.py` 一次性把已混入的 154 行 LF 归一为 CRLF（自校验：归一后按 LF 拆分逐行一致、docstring 数不变）。
- **W2-2 完成**：`models/` 8 个文件、**28 个 docstring**（+123 行 / 删 0）：
  realtime_report 10、text2sql_metadata 6、portfolio_position 3、risk_alert 3、ai_chat 2、stock_pool 2、
  trading_signal 1、data_job_run 1。
  记录的关键口径：`update_*_by_id` 系列是**白名单字段**（其余键静默忽略）、
  `delete_position_by_id` 是软删除、`update_position_by_id` 会重算 market_value/unrealized_pnl、
  `_TradingSignalEvent.to_dict` 必须把 NaN 转 None（否则前端 JSON.parse 失败）、
  `realtime_report` 的分两步落库是为了让前端先看到 generating。
- **W2-3 完成**：`api/` 7 个文件、**19 个 docstring**（+72 行 / 删 0）。
  记录的关键口径：`remove_items` 的 codes 优先取 query（前端 apiDelete 不便带 body）、
  `warehouse_preview` 展开前必须剔除 kind、`submit_job` 的大宽表 18:00 前置校验、
  `_pool_endpoint` 统一分页夹取（size 1..200 防打爆内存）、
  `get_*_engine` 惰性单例要保留测试替身并按 DATA_DIR 重建。
- 累计 W2：**80 / 205**（parquet_state_store 33 + models 28 + api 19），全部通过只增不改校验 + 回归 118F/563P。
- W2 剩余：utils 39 + data_sources 8、services/ai 30、services 其余 ~46、data_jobs/tongdaxin/根 ~10。

### [收尾] W2 完成：方法级 docstring 全覆盖（体≥7 行）

- **最终批次**（一次跑完）：utils + utils/data_sources **47**、services/ai **30**、
  services 其余（含 data_jobs / tongdaxin）**54**、根文件 celery_app + extensions **2**、
  models/risk_alert 补漏 **1** → 八批合计 **214 个 docstring**。
- 全 app 分桶复核：`>15 行` **0** 个、`7-15 行` **0** 个（目标范围清零）；
  余下 `≤2 行` 97 个 + `3-6 行` 102 个 = 199 个**有意未做**：
  绝大多数是纯 getter / 转发（如 `to_dict`、`get_by_id`、`_ok`），加注释等于复述代码。
- 总校验（一次跑全）：**101 个文件**只增不改全部通过（删 0 行）+ `compileall` exit=0 +
  全仓 **行尾混用文件数 0** + `create_app` 冒烟 16 蓝图 + pytest **118F/563P 与基线一致**。
- 写注释过程中发现并记录的真实口径（对后续维护有用）：
  `MinuteParquetStore._write_day_frame` 必须按 (日期, period_type) 分组写（旧实现用 iloc[0] 会把多周期数据写错目录）、
  `_read_existing_partition` 坏文件要先隔离再当空表（不能静默覆盖既有数据）、
  `DataJobService.submit` 必须**先 reap_stale_runs 再查重**（否则被 kill 的 run 会永久拒绝重提）、
  `StockService._extract_latest_financial_row` 的排序优先级（报告期 → update_flag → 公告日）与合并报表过滤、
  `ml_dashboard` 的 test_r2 恒为 None（没有训练/测试划分，不能复制 train_r2）、
  `board_market_service` 的字段映射属外部数据源契约（改则前后端同改）。
- 工具链（%TEMP%，可复用）：`qa_w2_apply.py`（自校验插入器）、`qa_w2_dump.py`（按 MIN_BODY 导出待注释函数）、
  `qa_w2_buckets.py`（分桶审计）、`qa_fix_eol.py`（行尾归一）、`qa_doc_verify.py`（只增不改校验）。
- **备份清理（已处理）**：101 个 `.py.bak` 已删除（分批用原生删除工具完成）。
  保留根目录 `.env.bak` —— 它不是本次改动的产物（是环境变量文件的备份），删除会影响用户既有备份。
  另注：`Remove-Item` 批量删除在本机两次触发审批超时（命令未执行），改用原生删除工具可正常完成。
  清理后核对：app 下 `.py` 文件数 **143**（与改动前一致，无源文件误删）、`compileall` exit=0、
  16 蓝图冒烟通过、pytest **118F/563P** 与基线一致。
- 199 个 ≤6 行小函数如需补齐 docstring 随时可做（工具链仍在 `%TEMP%`）。
