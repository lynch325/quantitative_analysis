"""把前端构建产物（frontend/dist）托管到 Flask 根路径下。

为什么需要：前端原本只能经 Vite dev server（:5173）访问，导致启动必须同时跑
Python 与 Node 两套运行时、开两个控制台窗口。而 `npm run build` 产出的 dist
本身是可静态托管的，挂到 Flask 上之后日常使用只需 `python run.py` 一个进程，
直接访问 http://127.0.0.1:5000 即可。

约定：
- dist 不存在时静默跳过（只提供 API，不报错）。
- `/api`、`/socket.io`、`/static` 前缀由蓝图与 SocketIO 处理，不接受 SPA 兜底。
- 其余未命中静态文件的路径一律回 index.html，交给 React Router（history 模式）。
- 前端源码有改动后需重新构建：`cd frontend && npm run build`；
  改前端期间仍可用 `启动前端.bat`（Vite 热更新，:5173 代理到 :5000）。
"""

from __future__ import annotations

import os
from pathlib import Path

from flask import Flask, abort, send_from_directory

#: 由蓝图 / SocketIO 处理的前缀，SPA 兜底不得接管
_RESERVED_PREFIXES = ("api", "socket.io", "static")

#: 静态资源目录（Vite 产物都在 assets/ 下）
_STATIC_DIR = "assets"

#: 静态资源后缀白名单：缺失时返回 404，**绝不能**回 index.html。
#: 不用「路径含点就算静态」这种宽判据——前端存在 /stock/000001.SZ 这类真实路由。
_STATIC_SUFFIXES = (
    ".js", ".mjs", ".css", ".map", ".png", ".jpg", ".jpeg", ".gif", ".svg",
    ".ico", ".webp", ".woff", ".woff2", ".ttf", ".eot", ".wasm",
)


def _is_static_asset(path: str) -> bool:
    """是否静态资源请求（缺失时必须给 404，而不是回 index.html）。"""
    return path.split("/", 1)[0] == _STATIC_DIR or path.lower().endswith(_STATIC_SUFFIXES)


def frontend_dist_dir(app: Flask) -> Path:
    """前端构建产物目录（可用 FRONTEND_DIST_DIR 环境变量覆盖）。"""
    override = os.getenv("FRONTEND_DIST_DIR")
    if override:
        return Path(override)
    return Path(app.root_path).parent / "frontend" / "dist"


def register_frontend_spa(app: Flask) -> bool:
    """注册 SPA 静态托管。返回是否注册成功（dist 缺失时为 False）。"""
    dist = frontend_dist_dir(app)
    if not (dist / "index.html").is_file():
        app.logger.info(f"[frontend] 未找到构建产物，跳过托管: {dist}")
        return False

    @app.route("/", defaults={"path": ""})
    @app.route("/<path:path>")
    def frontend_spa(path: str):
        # 保留前缀交给原有路由处理（正常情况 Flask 的规则特异性已保证，
        # 这里显式拦一道，避免将来新增蓝图时被兜底悄悄接管）
        if path and path.split("/", 1)[0] in _RESERVED_PREFIXES:
            abort(404)
        if path and (dist / path).is_file():
            return send_from_directory(dist, path)
        if path and _is_static_asset(path):
            # 重新构建后旧 chunk 会被清掉，而仍停留在旧页面的标签页还会来取它。
            # 若这里回 index.html，浏览器会把 HTML 当 JS 解析并抛 SyntaxError，
            # 表现为整块页面/组件加载不出来（例如「股票池无法导入」）。必须 404。
            abort(404)
        return send_from_directory(dist, "index.html")

    app.logger.info(f"[frontend] 已托管前端构建产物: {dist}")
    return True
