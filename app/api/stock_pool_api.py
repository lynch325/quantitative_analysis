"""股票池 API（迁移自桌面版股票池 user/股票池）。

路由前缀 `/api/stock-pool`：
- 池：列表/新建/改名/删除
- 成分：查询（关键字/来源过滤、可选行情富化）、批量加入、批量移除、来源清单
- 导入源：数仓状态、板块列表、交易日、预览（板块/条件/SQL/粘贴）
- 导出 CSV、推送通达信自定义板块
"""

from flask import Blueprint, jsonify, request
from loguru import logger

from app.services.stock_pool_service import (
    CONDITIONS,
    StockPoolError,
    get_stock_pool_service,
    warehouse_path,
)

stock_pool_bp = Blueprint("stock_pool_api", __name__, url_prefix="/api/stock-pool")


def _ok(data):
    return jsonify({"code": 200, "message": "成功", "data": data})


def _error(message: str, status: int = 400):
    return jsonify({"code": status, "message": message, "data": None}), status


def _json():
    return request.get_json(silent=True) or {}


@stock_pool_bp.before_request
def _ensure_tables():
    """首次访问本模块时幂等补建股票池表（与 AI 助手模块同款做法）。"""
    get_stock_pool_service().ensure_tables()


# ---------------- 池 ----------------

@stock_pool_bp.route("/pools", methods=["GET"])
def list_pools():
    try:
        return _ok(get_stock_pool_service().list_pools())
    except Exception as exc:  # noqa: BLE001
        logger.error(f"股票池列表失败: {exc}")
        return _error(f"股票池列表失败: {exc}", 500)


@stock_pool_bp.route("/pools", methods=["POST"])
def create_pool():
    """POST 新建股票池：body 取 name / note。

    StockPoolError（业务校验失败，如重名）→ 业务错误响应；
    其他异常记日志后返回 500，避免把堆栈暴露给前端。
    """
    body = _json()
    try:
        pool = get_stock_pool_service().create_pool(body.get("name", ""), body.get("note", ""))
        return _ok(pool)
    except StockPoolError as exc:
        return _error(str(exc))
    except Exception as exc:  # noqa: BLE001
        logger.error(f"新建股票池失败: {exc}")
        return _error(f"新建股票池失败: {exc}", 500)


@stock_pool_bp.route("/pools/<int:pool_id>", methods=["PUT"])
def rename_pool(pool_id):
    """POST 重命名股票池：body 取 name（缺失按空串处理，校验交给服务层）。
    """
    try:
        return _ok(get_stock_pool_service().rename_pool(pool_id, (_json().get("name") or "")))
    except StockPoolError as exc:
        return _error(str(exc))
    except Exception as exc:  # noqa: BLE001
        logger.error(f"重命名股票池失败: {exc}")
        return _error(f"重命名股票池失败: {exc}", 500)


@stock_pool_bp.route("/pools/<int:pool_id>", methods=["DELETE"])
def delete_pool(pool_id):
    """POST 删除股票池，成功返回被删的 pool_id。
    """
    try:
        get_stock_pool_service().delete_pool(pool_id)
        return _ok({"deleted": pool_id})
    except StockPoolError as exc:
        return _error(str(exc))
    except Exception as exc:  # noqa: BLE001
        logger.error(f"删除股票池失败: {exc}")
        return _error(f"删除股票池失败: {exc}", 500)


# ---------------- 成分 ----------------

@stock_pool_bp.route("/pools/<int:pool_id>/items", methods=["GET"])
def pool_items(pool_id):
    """GET 池成分列表，返回 {items, total}。

    query 参数：keyword（代码/名称模糊）、source（来源过滤）、
    quote=1/true/yes 时附带实时行情 —— 会逐只取价，明显更慢，前端按需开启。
    """
    try:
        rows = get_stock_pool_service().items(
            pool_id,
            keyword=request.args.get("keyword", ""),
            source=request.args.get("source", ""),
            with_quote=request.args.get("quote", "") in ("1", "true", "yes"),
        )
        return _ok({"items": rows, "total": len(rows)})
    except StockPoolError as exc:
        return _error(str(exc))
    except Exception as exc:  # noqa: BLE001
        logger.error(f"查询池成分失败: {exc}")
        return _error(f"查询池成分失败: {exc}", 500)


@stock_pool_bp.route("/pools/<int:pool_id>/items", methods=["POST"])
def add_items(pool_id):
    """POST 批量加入成分：body 取 codes 列表与 source 标签，返回实际新增数。

    已存在的成分由服务层跳过，因此 added 可能小于传入条数（前端据此提示「已存在」）。
    """
    body = _json()
    try:
        added = get_stock_pool_service().add_items(
            pool_id, body.get("codes") or [], body.get("source", ""))
        return _ok({"added": added})
    except StockPoolError as exc:
        return _error(str(exc))
    except Exception as exc:  # noqa: BLE001
        logger.error(f"加入成分失败: {exc}")
        return _error(f"加入成分失败: {exc}", 500)


@stock_pool_bp.route("/pools/<int:pool_id>/items", methods=["DELETE"])
def remove_items(pool_id):
    # 优先取 query（前端 apiDelete 封装不便带 body），同时兼容 body 形式
    """POST 批量移除成分，返回实际移除数。

    **codes 优先取 query 参数**（逗号分隔）：前端的 apiDelete 封装不便带 body；
    query 为空时才回退读 body 的 codes。
    """
    raw = request.args.get("codes", "")
    codes = [c.strip() for c in raw.split(",") if c.strip()] or (_json().get("codes") or [])
    try:
        removed = get_stock_pool_service().remove_items(pool_id, codes)
        return _ok({"removed": removed})
    except StockPoolError as exc:
        return _error(str(exc))
    except Exception as exc:  # noqa: BLE001
        logger.error(f"移除成分失败: {exc}")
        return _error(f"移除成分失败: {exc}", 500)


@stock_pool_bp.route("/pools/<int:pool_id>/sources", methods=["GET"])
def pool_sources(pool_id):
    """GET 池内成分的来源分布，供前端渲染来源筛选下拉。
    """
    try:
        return _ok(get_stock_pool_service().sources(pool_id))
    except StockPoolError as exc:
        return _error(str(exc))
    except Exception as exc:  # noqa: BLE001
        logger.error(f"查询来源失败: {exc}")
        return _error(f"查询来源失败: {exc}", 500)


# ---------------- 导入源（数仓） ----------------

@stock_pool_bp.route("/warehouse/status", methods=["GET"])
def warehouse_status():
    service = get_stock_pool_service()
    return _ok({
        "path": str(warehouse_path()),
        "available": service.warehouse.available(),
    })


@stock_pool_bp.route("/warehouse/sectors", methods=["GET"])
def warehouse_sectors():
    try:
        return _ok(get_stock_pool_service().warehouse.sectors(
            request.args.get("type") or None))
    except Exception as exc:  # noqa: BLE001 - 多为数仓不可用
        return _error(f"读取板块列表失败: {exc}", 503)


@stock_pool_bp.route("/warehouse/trade-dates", methods=["GET"])
def warehouse_trade_dates():
    try:
        limit = int(request.args.get("limit", 30))
        return _ok(get_stock_pool_service().warehouse.trade_dates(limit))
    except Exception as exc:  # noqa: BLE001
        return _error(f"读取交易日失败: {exc}", 503)


@stock_pool_bp.route("/conditions", methods=["GET"])
def conditions():
    return _ok([{"name": k, "desc": v, "need_extra": "%" in k or "亿" in k} for k, v in CONDITIONS.items()])


@stock_pool_bp.route("/warehouse/preview", methods=["POST"])
def warehouse_preview():
    """POST 预览仓库取数结果：body 的 kind 指定取数方式，其余键作为参数展开。

    **展开前必须剔除 kind** —— 它已由 preview(kind, **kwargs) 形参接收，再展开会重复传参。
    失败返回 503（数据源不可用）而非 400。
    """
    body = _json()
    kind = body.get("kind", "")
    # 注意剔除 kind：preview(kind, **kwargs) 已接收它，再展开 body 会重复传参
    params = {k: v for k, v in body.items() if k != "kind"}
    try:
        codes = get_stock_pool_service().preview(kind, **params)
        return _ok({"codes": codes, "total": len(codes)})
    except StockPoolError as exc:
        return _error(str(exc))
    except Exception as exc:  # noqa: BLE001
        return _error(f"预览失败: {exc}", 503)


# ---------------- 导出 / 推送 ----------------

@stock_pool_bp.route("/pools/<int:pool_id>/export", methods=["GET"])
def export_csv(pool_id):
    """GET 把池成分导出为 CSV 附件。

    文件名用池名拼装（含中文，故显式声明 charset=utf-8）；
    这里直接调服务层 _get_pool 只为取名称。
    """
    try:
        service = get_stock_pool_service()
        pool = service._get_pool(pool_id)  # noqa: SLF001 - 接口内取名称用
        csv_text = service.export_csv(pool_id)
        from flask import Response

        return Response(
            csv_text,
            mimetype="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="pool_{pool_id}_{pool.name}.csv"'},
        )
    except StockPoolError as exc:
        return _error(str(exc))
    except Exception as exc:  # noqa: BLE001
        logger.error(f"导出失败: {exc}")
        return _error(f"导出失败: {exc}", 500)



