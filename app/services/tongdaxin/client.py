"""pytdx 行情连接管理：多主机探测与连接生命周期收口。

- `DEFAULT_HOSTS` 是内置行情服务器候选（7709 标准行情端口）；
- `connect_first_available` 依次尝试直到连通，全部失败抛 ConnectionError；
- `connected_session` 提供 with 语义：进入时连接、退出时**一定** disconnect，
  避免批量拉取循环里漏断开把连接句柄耗光。

pytdx 在 create_hq_api 内部延迟导入，未安装 pytdx 的环境仍可导入本模块
（只在真正建连时报错），便于按部署环境裁剪依赖。
"""

from __future__ import annotations

from collections.abc import Iterable
from contextlib import contextmanager


DEFAULT_HOSTS = (
    ("default", "119.147.212.81", 7709),
    ("server_1", "115.238.56.198", 7709),
    ("server_2", "115.238.90.165", 7709),
    ("server_3", "180.153.18.170", 7709),
)


def create_hq_api(**overrides):
    """创建 pytdx 行情 API 实例。

    默认 heartbeat + auto_retry + raise_exception=False（失败返回错误码而非抛异常），
    overrides 可覆盖任意选项，便于测试注入。
    """
    from pytdx.hq import TdxHq_API

    options = {
        "heartbeat": True,
        "auto_retry": True,
        "raise_exception": False,
    }
    options.update(overrides)
    return TdxHq_API(**options)


def normalize_hosts(hosts: Iterable[tuple[str, str, int]] | None = None) -> list[tuple[str, str, int]]:
    normalized = []
    for name, host, port in (hosts or DEFAULT_HOSTS):
        normalized.append((str(name), str(host), int(port)))
    return normalized


def connect_first_available(api, hosts: Iterable[tuple[str, str, int]] | None = None) -> tuple[str, str, int]:
    """按顺序尝试多个行情主机，返回第一个连通的 (名称, host, port)。

    全部失败抛 ConnectionError，并把最后一次的异常作为 cause，便于定位。
    """
    last_error = None
    for name, host, port in normalize_hosts(hosts):
        try:
            if api.connect(host, port):
                return name, host, port
        except Exception as exc:
            last_error = exc
    if last_error is not None:
        raise ConnectionError("No available pytdx hosts") from last_error
    raise ConnectionError("No available pytdx hosts")


@contextmanager
def connected_session(api, hosts: Iterable[tuple[str, str, int]] | None = None):
    connect_first_available(api, hosts)
    try:
        yield api
    finally:
        api.disconnect()
