## [22:05] - 性能优化: fuyao_dump 10 年 dump 改窗口读取，读取峰值内存降 88%

- **文件**: `app/utils/data_sources/fuyao_dump.py`
- **决策**: 用 duckdb 谓词下推替代 `pd.read_parquet` 整表读取。**pyarrow `filters=` 在这个文件上无效** —— 该 parquet 只有 2 个 row group 且按 thscode 排序，每个 row group 的 `date_ms` 统计都横跨全域，跳不过任何 row group（实测只压掉 40%）；duckdb 分块流式扫描才真正降下来。duckdb 缺失/失败时退化 pyarrow filters。新增 `load_dump_window()`、`FULL_DUMP_COLUMNS`（9 列，跳过常量列）、`PRE_CLOSE_LOOKBACK_DAYS = 30`（pre_close 靠前一交易日推导，30 天覆盖春节/国庆连续休市）；`dump_date_range()` 改 duckdb `min/max` 聚合（原单列整读 82MB）。
- **验证**: 子进程精确峰值 —— 整表 2039MB / 1.76s → duckdb 247MB / 0.89s。6 个 case（dump 末日、近端多日、跨国庆隔 9 天、跨春节、dump 首日无前置数据、跨年）与旧实现 `assert_frame_equal(check_exact=True)` **逐值一致**，`pre_close` 的 NaN 计数一致。lint 0 错误。

## [22:45] - Bug 修复完成: 实时监控量比 / 异动评分 / 数据时间三处口径修复

- **文件**: `app/services/realtime_monitor_service.py`（`market_snapshot_service.py` 的试探改动已全部回退）
- **决策**:
  1. 量比改走全市场快照：原 `_calculate_volume_ratio` **只查分钟线**，而分钟线里没有这些标的 → 全市场恒 1.0。新增 `_daily_volume_avg_map()` 批量取近 5 个**交易日**（严格 `< current_date`，防当日 bar 入库污染分母）均量 + `_snapshot_volume_ratio()`（无基准返回 **None** 而非 1.0）。
  2. 异动评分 `Optional` 化：量比 None 时量能分记 0；`max(0.0, (vr-1)*10)` 防缩量股被**负分**拉低；排序加二级键 `(score, abs(change_pct))`，否则同分条目顺序随机。
  3. 数据时间改「最近交易日 + 竞价时段」推断：扶摇 `prices/snapshot` 的 `timestamp` 是**响应生成时刻**（实测与本机 now 同秒），`item` 11 个字段无任何行情时间 → 不能当数据时间。日历**复用** `get_board_market_service().latest_trade_date()`（自带 6h 缓存 + 自然日回退），禁用滞后的本地 `stock_trade_calendar.parquet`（实测只到 2026-09-11）。盘前用 `min(…, now)` 兜底，保证不产生未来时间。
- **验证**: 量比 `0.64 / 0.65 / 0.86 / 0.85 / 0.93`（修复前恒 1.0）；异动 `score ∈ [72.1, 100.0]`、43 个不同分值（修复前 50 条全 50.0）；`anomaly_types = {急涨:42, 放量:50, 急跌:8}`（修复前无"放量"）；量比非空 50/50；`update_time = 2026-09-23T15:00:00`、`data_delay = 452`（修复前为 now 时刻 + delay 0）；9 个时段模拟用例**未来时间 0 个**；lint 0 错误。
- **未修（已披露）**: 快照路径 `change_pct` 缺失时仍写 `0.0`；`period_hours` 在快照路径下仍是死参数；`turnover_rate` 恒 null（后端有意不编造）。改动在 web 进程内，**需重启后端**生效。

## [23:15] - 功能实现: 实时监控行情源切换为 tdx-master（通达信协议，免费无凭证）

- **文件**: 新增 `app/utils/data_sources/tdx_client.py`；改 `app/services/realtime_monitor_service.py`、`app/utils/data_sources/__init__.py`
- **决策**: tdx-master 是 Go 库，跨语言**用其自带 HTTP 服务**（`extend/httpserver` → 编译 `output/tdx-httpserver.exe`），Python 只走 HTTP，不引入 ctypes/c-shared。适配器输出列**刻意对齐扶摇快照 schema**，使 `_snapshot_frame` 的取列逻辑零改动复用；`volume` 由手 ×100 转"股"以匹配下游 `/100` 得手。降级链：tdx → 扶摇 → 本地分钟线。`source` 改动态值 + 新增 `_is_snapshot_source()` 统一 10 处判断点。
- **实测关键参数**: `/quote` **单次上限 80 只**；全市场 70 片，并发 4/8/16 均 **2.13s**（Go 侧 poolSize=1，并发无提升）、串行 3.1s；`/code/stocks` 5578 只（缓存 12h）。
- **字段坑（实测确认）**: `Kline.Close`=**现价**、`Kline.Last`=**昨收**（与 K 线语义相反）；价格与 `Kline.Amount` 均 **÷1000 得元**；`Kline.Volume` 已是手；`Kline.Time` 是服务端当前时间；`/quote` 不返回名称。
- **验证**: 与扶摇逐只交叉比对 **5/5 一致**（含 -21.011% / -34.527% 新股）；自动拉起 exe 2.6s；全市场帧 `(5578,11)` 2.09s、name/pct_chg 非空 99.9%；监控 `source=tdx_snapshot`、量比 0.857 正常、时间 15:00:00；TTL 缓存后 overview/sentiment 0.01s；lint 0 错误。

## [23:40] - Bug 修复完成: 涨跌幅榜改为全市场口径 + 修掉 3 个被掩盖的数据质量缺陷

- **文件**: `app/services/realtime_monitor_service.py`、`app/api/realtime_monitor.py`
- **决策**: ① 新增 `get_top_movers()`，候选池从「成交额前 100 活跃股」换成**全市场快照帧**，且**只对入选的 3×limit 只富化**（全市场逐只富化是 5000+ 次 parquet 访问）；② 抽出 `_enrich_quotes()` 供实时行情与排行共用；③ `get_realtime_quotes` 快照路径**也批量预取 turnover_map**（原先逐只读日线，100 只 = 100 次访问、接口 10s）；④ `_snapshot_frame` 过滤 `close > 0`（停牌/退市/配股缴款代码如 `072913.SZ` 的 `last_price=0` 会在榜上伪造成 **-100%**）；⑤ 新增 `_display_name()`（`nan or code` 会返回 nan；`astype(str)` 会把缺失值变成 "None" 字面量，两者都要识别）。
- **验证**: 全市场真实跌幅前 15 **覆盖率 15/15**（修复前 3/15）；三榜排序单调；5578→5562 只；`-100%` 假数据清零；`920025.BJ +580.751%` 核实为**真实**（北交所新股首日，发行价 4.26 / 开盘 29.0 / 成交 493507 手）；耗时 4.1s（修复前 10s）；lint 0 错误。

## [00:05] - 重构: 启动方式简化为单进程（Flask 托管前端 dist）

- **文件**: 新增 `app/frontend_spa.py`、`启动.bat`；改 `app/__init__.py`
- **决策**: 根因是 `app/__init__.py` **没有托管前端构建产物的代码**，而 `frontend/dist/` 早已构建好（2026-09-13），导致启动必须额外跑 Vite（Node）并开两个窗口。新增 `register_frontend_spa(app)`：`/` 与 `/<path:path>` catch-all，未命中静态文件回 index.html（React Router history），`api`/`socket.io`/`static` 前缀 `abort(404)` 不接管，dist 缺失时静默跳过。`启动系统.bat` 保留给改前端时用（Vite 热更新）。
- **验证**: 独立实例 :5055 **8/8 通过** —— `/`、`/assets/*.js`、`/assets/*.css`、SPA 深链均 200 且 MIME 正确；`/api/*` 仍返回 JSON 未被抢；`/api/不存在` 404；`/socket.io` 握手 200 text/plain；lint 0 错误。
- **注意**: 改前端源码后必须 `cd frontend && npm run build` 重新生成 dist。

## [23:55] - 功能实现: 桌面版股票池（user/股票池）迁移为本项目模块

- **文件**: 新增 `app/models/stock_pool.py`、`app/services/stock_pool_service.py`、`app/api/stock_pool_api.py`、`frontend/src/api/stockPool.ts`、`frontend/src/pages/StockPoolPage.tsx`、`scripts/migrate_stock_pool.py`；改 `app/models/__init__.py`、`app/__init__.py`、`frontend/src/App.tsx`、`frontend/src/styles/theme.css`
- **决策**: ① 持久化并入项目 SQLite（自增主键 + `(pool_id, ts_code)` 唯一约束替代原复合主键），配 `scripts/migrate_stock_pool.py` 幂等迁移；② 数仓走可配 DuckDB 只读连接（`TDX_DUCKDB_PATH`，缺省 `PYPlugins/user/数据/tdx_data.duckdb`），查询失败重建连接重试一次；③ 推送通达信保留，subprocess 调桌面版同款 `push_tdx.py`；④ 建表沿用 AI 助手模块「首次访问幂等补建」（项目只在 run_system.py 手动 create_all）
- **验证**: 后端 14 项全通过；前端 `npm run build` 通过（8.53s）；迁移 **4 池 1123 条全导入**、复跑幂等全跳过；清理冒烟池后剩 AI/地产链/消费/电力 共 1123 条
- **坑**: 蓝图只 import 不 register 会被 SPA catch-all 兜成 404；`apiDelete` 不支持 body（删除改走 query）；`preview(kind, **body)` 会重复传 kind；前端组件 props（`PageHeader.subtitle` / `Card.title` 必填 / `EmptyState.description`）与直觉不同
