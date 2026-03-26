# =============================================================================
# Yezhou Capital Limited  |  Proprietary & Confidential
# =============================================================================
# Copyright (c) 2026 Yezhou Capital Limited. All rights reserved.
#
# Project  : Yezhou Quantitative Trading System
# Author   : Yezhou Liu
# Contact  : yezhoucapital@gmail.com
#
# This source code is the exclusive property of Yezhou Capital Limited.
# Unauthorized copying, modification, distribution, or use of this file,
# via any medium, is strictly prohibited without prior written consent.
# =============================================================================
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
