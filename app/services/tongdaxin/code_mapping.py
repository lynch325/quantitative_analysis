"""股票代码格式互转：本项目/baostock 风格 `sz.000001` ↔ pytdx 的 (market, code)。

- market 约定：0 = 深市，1 = 沪市（pytdx 口径，**不含北交所**）；
- `any_style_code_to_tdx` 兼容 `sz.000001` / `000001.SZ` / 6 位纯数字三类写法，
  纯数字按首位推断（0/3 → 深市，其余 → 沪市）；
- 无法识别的写法抛 ValueError，不做静默兜底，避免把错误代码带进数据写入。

本项目内部统一使用 `600000.SH` 风格（见 data_reader），转换只发生在
与 pytdx 交互的边界上。
"""

from __future__ import annotations


def bs_style_code_to_tdx(value: str) -> tuple[int, str]:
    text = str(value).strip().lower()
    if text.startswith("sz."):
        return 0, text.split(".", 1)[1]
    if text.startswith("sh."):
        return 1, text.split(".", 1)[1]
    raise ValueError(f"unsupported bs-style code: {value}")


def any_style_code_to_tdx(value: str) -> tuple[int, str]:
    """把各种写法的股票代码转成通达信所需的 (market, code)。

    支持 sz.000001 / 000001.SZ / 纯数字三类；纯数字按首位推断市场
    （0 或 3 开头为深市，其余为沪市）。无法识别的写法直接抛 ValueError，不做猜测。
    """
    text = str(value).strip()
    lower_text = text.lower()
    if lower_text.startswith(("sz.", "sh.")):
        return bs_style_code_to_tdx(lower_text)
    if lower_text.endswith(".sz"):
        return 0, text.split(".", 1)[0]
    if lower_text.endswith(".sh"):
        return 1, text.split(".", 1)[0]
    # 纯数字代码：0/3 开头 → 深市(0)，6 开头 → 沪市(1)
    if text.isdigit():
        return (0, text) if text[0] in ("0", "3") else (1, text)
    raise ValueError(f"unsupported stock code: {value}")


def tdx_market_code_to_bs_style(market: int, code: str) -> str:
    market_prefix = "sz" if int(market) == 0 else "sh"
    return f"{market_prefix}.{str(code).strip()}"
