"""data_reader 日线读取基准 + 正确性对照（只读，不写数据）。

用法:
    .venv\\Scripts\\python.exe scripts/bench_data_reader.py

输出:
    - 三个场景的中位耗时与峰值内存
    - 正确性对照：下推读取结果 == 全量读取后内存过滤的结果（行集合 + 收盘价）
"""
import os
import sys
import time
import tracemalloc

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.data_reader import ParquetDataReader  # noqa: E402


def measure(fn, n=3):
    """返回 (中位耗时秒, 峰值内存 MB, 返回值)。"""
    times = []
    peak = 0.0
    out = None
    for _ in range(n):
        tracemalloc.start()
        t0 = time.perf_counter()
        out = fn()
        times.append(time.perf_counter() - t0)
        peak = max(peak, tracemalloc.get_traced_memory()[1] / 1024 / 1024)
        tracemalloc.stop()
    times.sort()
    return times[len(times) // 2], peak, out


def partition_dates(r: "ParquetDataReader"):
    """列出 daily 表全部分区日期（升序），避免硬编码不存在的日期。"""
    base = os.path.join(r.data_dir, r.TABLE_DIRS["daily"])
    out = []
    for y in sorted(os.listdir(base)):
        yp = os.path.join(base, y)
        if not os.path.isdir(yp) or "=" not in y:
            continue
        for m in sorted(os.listdir(yp)):
            mp = os.path.join(yp, m)
            if not os.path.isdir(mp) or "=" not in m:
                continue
            for d in sorted(os.listdir(mp)):
                if not os.path.isdir(os.path.join(mp, d)) or "=" not in d:
                    continue
                out.append("-".join([y.split("=")[1], m.split("=")[1], d.split("=")[1]]))
    out.sort()
    return out


def main():
    r = ParquetDataReader()
    print("data_dir:", r.data_dir)

    ds = partition_dates(r)
    print(f"分区日期: {len(ds)} 个, {ds[0]} ~ {ds[-1]}")

    # 取真实代码样本
    probe_day = ds[len(ds) // 2]
    probe = r.get_daily(start_date=probe_day, end_date=probe_day)
    if probe.empty:
        print("样本日无数据，退出")
        return 1
    codes = probe["ts_code"].dropna().unique().tolist()[:20]
    print(f"样本日: {probe_day} | 样本代码数: {len(codes)}")

    print("\n== 场景1: 20 只股票 × 全库区间（最坏情况） ==")
    t1, m1, df1 = measure(
        lambda: r.get_daily(ts_codes=codes, start_date=ds[0], end_date=ds[-1])
    )
    print(f"中位耗时 {t1*1000:8.1f} ms | 峰值内存 {m1:7.1f} MB | 行数 {len(df1)}")

    print("\n== 场景2: 全市场单日（无代码过滤） ==")
    t2, m2, df2 = measure(lambda: r.get_daily(start_date=probe_day, end_date=probe_day))
    print(f"中位耗时 {t2*1000:8.1f} ms | 峰值内存 {m2:7.1f} MB | 行数 {len(df2)}")

    print("\n== 场景3: 回测式 20 个调仓日单日读取 ==")
    days = ds[len(ds) // 2: len(ds) // 2 + 20]
    t3, m3, _ = measure(
        lambda: [r.get_daily(ts_codes=codes, start_date=d, end_date=d) for d in days], n=2
    )
    print(f"中位耗时 {t3*1000:8.1f} ms（20 次合计，单次 {t3/20*1000:.1f} ms）| 峰值内存 {m3:7.1f} MB")

    print("\n== 正确性对照: 下推结果 vs 全量读取后内存过滤 ==")
    small = codes[:5]
    win = (ds[40], ds[60])
    pushed = r.get_daily(ts_codes=small, start_date=win[0], end_date=win[1])
    full = r.get_daily(start_date=win[0], end_date=win[1])
    ref = full[full["ts_code"].isin(set(small))]

    ok_rows = set(zip(pushed["ts_code"], pushed["trade_date"])) == set(
        zip(ref["ts_code"], ref["trade_date"])
    )
    cols_ok = list(pushed.columns) == list(ref.columns)
    print(f"行集合一致: {ok_rows} | 列集合一致: {cols_ok} "
          f"| pushed={len(pushed)} ref={len(ref)}")
    if not (ok_rows and cols_ok):
        print("!! 对照失败，下推读取与全量读取结果不一致")
        return 2

    # 收盘价逐行比对（防止列错位）
    p = pushed.set_index(["ts_code", "trade_date"])["close"].sort_index()
    q = ref.set_index(["ts_code", "trade_date"])["close"].sort_index()
    same = p.round(6).equals(q.round(6))
    print(f"收盘价逐行一致: {same}")
    if not same:
        return 2

    print("\n对照全部通过")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
