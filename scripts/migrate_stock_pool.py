"""把桌面版股票池（`user/股票池/pools.db`）迁移到本项目的 SQLite。

用法（项目根目录执行）:
    .venv\\Scripts\\python.exe scripts\\migrate_stock_pool.py
    .venv\\Scripts\\python.exe scripts\\migrate_stock_pool.py --source <pools.db> --dry-run

行为:
- 幂等：已存在同名池就复用，成分按 `ts_code` 去重跳过，重复执行不会翻倍。
- 保留每条成分的原始 `source`（导入来源）与 `add_at`。
- 代码统一经 `norm_code` 规范化（补全 SH/SZ/BJ 后缀，剔除板块代码）。
"""

import argparse
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# Windows 控制台为 GBK 时中文会抛 UnicodeEncodeError
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding='utf-8', errors='replace')
    except Exception:  # noqa: BLE001
        pass

from app import create_app  # noqa: E402
from app.extensions import db  # noqa: E402
from app.models.stock_pool import StockPool, StockPoolItem  # noqa: E402
from app.services.stock_pool_service import StockPoolService, norm_code  # noqa: E402
from app.utils.time_utils import now_local  # noqa: E402


def _parse_add_at(text):
    try:
        return datetime.strptime(str(text)[:19], '%Y-%m-%d %H:%M:%S')
    except (ValueError, TypeError):
        return now_local()


def main():
    default_src = ROOT.parent / 'user' / '股票池' / 'pools.db'
    ap = argparse.ArgumentParser()
    ap.add_argument('--source', default=str(default_src), help='源 pools.db 路径')
    ap.add_argument('--dry-run', action='store_true', help='只统计不写入')
    ap.add_argument('--env', default='default', help="FLASK_ENV 配置名（default/development/production）")
    args = ap.parse_args()

    src = Path(args.source)
    if not src.is_file():
        print(f'❌ 源文件不存在: {src}')
        return 1

    con = sqlite3.connect(str(src))
    pools = con.execute('SELECT id, name, note FROM pool ORDER BY name').fetchall()
    print(f'源库: {src}')
    print(f'共 {len(pools)} 个池\n')
    if not pools:
        return 0

    app = create_app(args.env)
    moved_pools = moved_items = skipped = 0
    with app.app_context():
        StockPoolService.ensure_tables()

        for pid, name, note in pools:
            rows = con.execute(
                'SELECT code, add_at, source FROM pool_item WHERE pool_id=?', (pid,)).fetchall()
            print(f'  池「{name}」 源成分 {len(rows)} 条')
            if args.dry_run:
                moved_items += len(rows)
                moved_pools += 1
                continue

            pool = StockPool.query.filter_by(name=name).first()
            if pool is None:
                pool = StockPool(name=name, note=note or '', created_at=now_local())
                db.session.add(pool)
                db.session.commit()
                moved_pools += 1
            exist = {i.ts_code for i in StockPoolItem.query.filter_by(pool_id=pool.id).all()}
            fresh = 0
            for code, add_at, source in rows:
                normalized = norm_code(code)
                if not normalized or normalized in exist:
                    skipped += 1
                    continue
                db.session.add(StockPoolItem(
                    pool_id=pool.id, ts_code=normalized,
                    source=source or '', add_at=_parse_add_at(add_at)))
                exist.add(normalized)
                fresh += 1
            db.session.commit()
            moved_items += fresh
            print(f'      → 新增 {fresh} 条（跳过 {len(rows) - fresh} 条重复/无效）')

    print(f'\n{"（dry-run 未写入）" if args.dry_run else ""}'
          f'完成: 池 {moved_pools}/{len(pools)}，成分 {moved_items} 条，跳过 {skipped} 条')
    return 0


if __name__ == '__main__':
    sys.exit(main())
