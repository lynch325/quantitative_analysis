"""数据任务的接口数据结构（dataclass）。

- `JobSubmitRequest`：提交请求体（job_type / params / full_refresh）；
- `JobDefinition`（frozen，不可变）：作业定义——脚本相对路径、分组、
  是否 `dangerous`（会整表覆盖或删除既有数据，前端需二次确认）、
  依赖作业、默认参数、推荐执行顺序与数据源标记（source_name/source_mode、
  是否支持增量 supports_incremental）。

定义清单见 registry.py；本模块只放结构，不含行为。
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class JobSubmitRequest:
    """Request payload for submitting a data job."""

    job_type: str
    params: Dict[str, Any] = field(default_factory=dict)
    full_refresh: bool = False


@dataclass(frozen=True)
class JobDefinition:
    """Metadata for a supported data job."""

    job_type: str
    group: str
    script_path: str
    display_name: str = ""
    description: str = ""
    dangerous: bool = False
    dependencies: List[str] = field(default_factory=list)
    default_params: Dict[str, Any] = field(default_factory=dict)
    recommended_order: int = 999
    source_name: str = "unknown"
    source_mode: str = "full"
    supports_incremental: bool = False
