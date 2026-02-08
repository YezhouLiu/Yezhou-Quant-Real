from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

import pandas as pd


@dataclass(frozen=True)
class SelectionResult:
    """
    选择结果：既给入选列表，也可给一个排序分数列名（便于下游看原因）
    """

    selected: pd.DataFrame  # 至少包含 instrument_id
    ranking_col: str | None = None


@runtime_checkable
class Selector(Protocol):
    """
    Selector: 输入 signals DF，输出 SelectionResult
    """

    def select(self, signals: pd.DataFrame) -> SelectionResult: ...
