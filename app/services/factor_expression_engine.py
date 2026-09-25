"""自定义因子表达式引擎：在单只股票的时间序列 DataFrame 上安全求值。

设计要点（**白名单 + 因果性**，改动前务必读完整）：
- 用 ast 解析后只允许白名单内的列、Series 方法、窗口聚合与二元/一元运算，
  不做 eval，避免表达式注入；
- `allowed_series_methods` 只放**时间因果**原语：pct_change / shift / diff / rolling。
  注意 rank 被刻意移除——`Series.rank()` 是整条时间序列上的排名，会把未来价格
  纳入分母（全样本前视）；截面排名必须在评分层按单日截面做；
- `causal_period_methods` 会拦截负参数（如 `close.shift(-1)` 取的是明天价格）；
- `max_rolling_window` 上限 10000，防止构造超大窗口拖垮计算；
- 求值单位是**单个 ts_code 的序列**（调用方按 ts_code groupby 后逐只传入），
  不负责横截面运算。

使用方：factor_engine._calculate_custom_factor（预热窗口按其 rolling 窗口自动扩容）。
"""

import ast
import operator
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd


class FactorExpressionEngine:
    """Safely evaluate custom factor expressions on a stock dataframe."""

    def __init__(self, allowed_columns: Optional[set[str]] = None):
        self.allowed_columns = allowed_columns or {
            "open",
            "high",
            "low",
            "close",
            "pre_close",
            "change_c",
            "pct_chg",
            "vol",
            "amount",
        }
        self.allowed_series_methods = {
            "pct_change",
            "shift",
            "diff",
            # 注意：rank 已被移出白名单。Series.rank() 是"整条时间序列"上的
            # 排名，close.rank() 会把未来价格纳入分母（全样本前视）。
            # 截面排名请在评分层做（factor_engine._calculate_factor_stats
            # 按单日截面分组），表达式层只允许时间因果的原语
            "rolling",
        }
        # 这些方法取负 periods 即引用未来数据（如 close.shift(-1) 是明天的价格）
        self.causal_period_methods = {"pct_change", "shift", "diff"}
        self.max_rolling_window = 10000
        self.allowed_window_methods = {
            "mean",
            "std",
            "max",
            "min",
            "sum",
        }
        self.allowed_functions = {
            "abs": abs,
        }
        self.bin_ops = {
            ast.Add: operator.add,
            ast.Sub: operator.sub,
            ast.Mult: operator.mul,
            ast.Div: operator.truediv,
            ast.Pow: operator.pow,
            ast.Mod: operator.mod,
        }
        self.unary_ops = {
            ast.UAdd: operator.pos,
            ast.USub: operator.neg,
        }

    def extract_max_rolling_window(self, expression: str) -> Optional[int]:
        """静态解析公式，返回其中 rolling() 的最大窗口；无 rolling 返回 None。

        只识别整数字面量窗口（位置参数 rolling(250) 与关键字参数
        rolling(window=250) 两种写法）；非字面量参数无法静态定尺寸，
        不识别——与 evaluate 的数值标量约束一致。
        """
        try:
            tree = ast.parse(expression, mode="eval")
        except (SyntaxError, ValueError):
            return None
        max_window: Optional[int] = None

        def _record(value: Any) -> None:
            nonlocal max_window
            window = int(value)
            max_window = window if max_window is None else max(max_window, window)

        for node in ast.walk(tree):
            if not (isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "rolling"):
                continue
            if node.args and isinstance(node.args[0], ast.Constant) \
                    and isinstance(node.args[0].value, (int, float)):
                _record(node.args[0].value)
            for kw in node.keywords:
                if kw.arg == "window" and isinstance(kw.value, ast.Constant) \
                        and isinstance(kw.value.value, (int, float)):
                    _record(kw.value.value)
        return max_window

    def evaluate(self, expression: str, df: pd.DataFrame) -> pd.DataFrame:
        """求值因子表达式，返回带 factor_value 列的新 DataFrame。

        入口三件事：空表达式抛错、标量结果广播成整列、非 Series 结果拒绝。
        **入参 df 不被修改**（copy 后加列），调用方可以安全复用。
        """
        if df is None or df.empty:
            return pd.DataFrame(columns=["factor_value"])
        if not expression or not str(expression).strip():
            raise ValueError("empty expression")

        parsed = ast.parse(expression, mode="eval")
        value = self._eval_node(parsed.body, df)

        if np.isscalar(value):
            value = pd.Series([float(value)] * len(df), index=df.index)

        if not isinstance(value, pd.Series):
            raise ValueError("expression must evaluate to a pandas Series")

        result = df.copy()
        result["factor_value"] = pd.to_numeric(value, errors="coerce")
        return result

    def _eval_node(self, node: ast.AST, df: pd.DataFrame):
        """递归求值 AST 节点（白名单式递归下降）。

        安全边界都在这里：
        - 只允许登记过的运算符；
        - 变量名必须「不以 __ 开头」且在 allowed_columns 内，否则报 column not allowed ——
          这是阻止表达式触达任意属性的关键；
        - 列不存在报 column not found，把「不许用」与「没这列」分开，便于排错。
        """
        if isinstance(node, ast.BinOp):
            left = self._eval_node(node.left, df)
            right = self._eval_node(node.right, df)
            op = self.bin_ops.get(type(node.op))
            if op is None:
                raise ValueError(f"unsupported operator: {type(node.op).__name__}")
            return op(left, right)

        if isinstance(node, ast.UnaryOp):
            op = self.unary_ops.get(type(node.op))
            if op is None:
                raise ValueError(f"unsupported unary operator: {type(node.op).__name__}")
            return op(self._eval_node(node.operand, df))

        if isinstance(node, ast.Name):
            if node.id.startswith("__"):
                raise ValueError("unsafe expression")
            if node.id not in self.allowed_columns:
                raise ValueError(f"column not allowed: {node.id}")
            if node.id not in df.columns:
                raise ValueError(f"column not found: {node.id}")
            return pd.to_numeric(df[node.id], errors="coerce")

        if isinstance(node, ast.Constant):
            if isinstance(node.value, (int, float)):
                return node.value
            raise ValueError("only numeric constants are allowed")

        if isinstance(node, ast.Call):
            return self._eval_call(node, df)

        raise ValueError(f"unsupported expression node: {type(node).__name__}")

    def _eval_call(self, node: ast.Call, df: pd.DataFrame):
        """求值函数调用节点：函数名必须在 allowed_functions 白名单内。

        两种形态：普通函数调用（参数先降为标量）与方法调用（如 rolling(5).mean()）。
        方法名同样拒绝 __ 开头的属性（如 __class__），防止借属性链逃出白名单。
        """
        if isinstance(node.func, ast.Name):
            func_name = node.func.id
            if func_name not in self.allowed_functions:
                raise ValueError(f"function not allowed: {func_name}")
            args = [self._as_scalar(self._eval_node(arg, df), "arg") for arg in node.args]
            kwargs = {
                kw.arg: self._as_scalar(self._eval_node(kw.value, df), kw.arg or "kwarg")
                for kw in node.keywords
            }
            return self.allowed_functions[func_name](*args, **kwargs)

        if not isinstance(node.func, ast.Attribute):
            raise ValueError("unsupported callable")

        method_name = node.func.attr
        if method_name.startswith("__"):
            raise ValueError("unsafe expression")

        target = self._eval_node(node.func.value, df)
        raw_args = [self._eval_node(arg, df) for arg in node.args]
        raw_kwargs: Dict[str, Any] = {
            kw.arg: self._eval_node(kw.value, df)
            for kw in node.keywords
        }

        if method_name == "rolling":
            window = int(self._as_scalar(raw_args[0], "window")) if raw_args else None
            if window is None or window <= 0:
                raise ValueError("rolling window must be positive")
            if window > self.max_rolling_window:
                raise ValueError(
                    f"rolling window too large: {window} (max {self.max_rolling_window})"
                )

        if isinstance(target, pd.Series):
            if method_name not in self.allowed_series_methods:
                raise ValueError(f"series method not allowed: {method_name}")
            args = [self._as_scalar(arg, "arg") for arg in raw_args]
            kwargs = {k: self._as_scalar(v, k) for k, v in raw_kwargs.items()}
            # shift/pct_change/diff 的负 periods 引用未来数据，一律拒绝。
            # periods 既可能是位置参数也可能是关键字参数
            # （close.shift(periods=-1)），两条路都要拦
            if method_name in self.causal_period_methods:
                periods = args[0] if args else kwargs.get("periods")
                if periods is not None and periods < 0:
                    raise ValueError(
                        f"{method_name}() 不允许负 periods：引用未来数据（前视偏差）"
                    )
            return getattr(target, method_name)(*args, **kwargs)

        if hasattr(target, method_name):
            if method_name not in self.allowed_window_methods:
                raise ValueError(f"window method not allowed: {method_name}")
            args = [self._as_scalar(arg, "arg") for arg in raw_args]
            kwargs = {k: self._as_scalar(v, k) for k, v in raw_kwargs.items()}
            return getattr(target, method_name)(*args, **kwargs)

        raise ValueError(f"unsupported method target: {method_name}")

    def _as_scalar(self, value: Any, name: str):
        if isinstance(value, (int, np.integer)):
            return int(value)
        if isinstance(value, (float, np.floating)):
            return float(value)
        raise ValueError(f"{name} must be a numeric scalar")
